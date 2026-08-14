#!/usr/bin/env python3
"""
Load a tree of CSV / TSV / XLSX files into DuckDB, one table per file (per sheet
for workbooks), and record every load in meta._load_manifest.

Table names come from the file's path relative to --src, so two files with the
same basename in different folders (CADR/PT index.csv vs zzCADR/PT index.csv)
stay distinct instead of one silently overwriting the other.

Typing: DuckDB's sniffer runs over the whole file first. If it errors -- ragged
rows, a column that is numeric for 100k rows and then "N/A" -- the file is
re-read with every column as VARCHAR rather than dropped, and the manifest says
so. Nothing is skipped for being messy.

Usage:
  load_tabular.py --src DIR --db PATH --schema NAME [--plan] [--force]
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
import time
import zipfile
from pathlib import Path
from xml.etree import ElementTree

import duckdb

CSV_EXT = {".csv", ".tsv", ".txt"}
XLSX_EXT = {".xlsx", ".xlsm"}

MANIFEST_DDL = """
CREATE SCHEMA IF NOT EXISTS meta;
CREATE TABLE IF NOT EXISTS meta._load_manifest (
    duckdb_schema   VARCHAR,
    duckdb_table    VARCHAR,
    source_path     VARCHAR,
    source_bytes    BIGINT,
    source_sha256   VARCHAR,
    source_kind     VARCHAR,
    source_object   VARCHAR,
    n_rows          BIGINT,
    n_columns       INTEGER,
    loaded_at       TIMESTAMP,
    loader          VARCHAR,
    notes           VARCHAR
);
"""


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def slug(text: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z]+", "_", text).strip("_").lower()
    s = re.sub(r"_+", "_", s)
    return s or "unnamed"


def table_name_for(rel: Path, strip: tuple[str, ...], sheet: str | None = None) -> str:
    parts = list(rel.parts[:-1]) + [rel.stem]
    while parts and slug(parts[0]) in strip:
        parts.pop(0)
    if sheet:
        parts.append(sheet)
    name = "_".join(slug(p) for p in parts if slug(p))
    name = re.sub(r"_+", "_", name).strip("_")
    if name and name[0].isdigit():
        name = "t_" + name
    return name[:150] or "unnamed"


def unique(name: str, taken: set[str]) -> str:
    if name not in taken:
        taken.add(name)
        return name
    for i in range(2, 1000):
        cand = f"{name}_{i}"
        if cand not in taken:
            taken.add(cand)
            return cand
    raise RuntimeError(f"cannot uniquify {name}")


def sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def load_csv(con, schema: str, table: str, path: Path, delim: str | None) -> tuple[int, str | None]:
    """Load one delimited file; fall back to all-VARCHAR if the sniffer chokes."""
    d = f", delim = {sql_str(delim)}" if delim else ""
    base = (f"read_csv({sql_str(str(path))}, header = true, sample_size = -1"
            f", union_by_name = false{d}")
    attempts = [
        (base + ")", None),
        (base + ", all_varchar = true)", "sniffer failed; all columns read as VARCHAR"),
        (base + ", all_varchar = true, ignore_errors = true)",
         "sniffer failed AND rows were skipped with ignore_errors = true"),
    ]
    last: Exception | None = None
    for expr, note in attempts:
        try:
            con.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
            con.execute(f'CREATE TABLE "{schema}"."{table}" AS SELECT * FROM {expr}')
            n = con.execute(f'SELECT count(*) FROM "{schema}"."{table}"').fetchone()[0]
            return n, note
        except duckdb.Error as exc:
            last = exc
    raise RuntimeError(f"{path}: {last}")


def xlsx_sheets(path: Path) -> list[str]:
    """Sheet names straight out of the workbook zip.

    DuckDB's excel extension ships read_xlsx but no sheet-listing function, and
    a workbook with several sheets would otherwise silently load only the first.
    """
    with zipfile.ZipFile(path) as zf:
        root = ElementTree.fromstring(zf.read("xl/workbook.xml"))
    ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    return [s.get("name") for s in root.findall(".//m:sheets/m:sheet", ns)
            if s.get("name")]


def load_xlsx(con, schema: str, table: str, path: Path, sheet: str) -> tuple[int, str | None]:
    expr = (f"read_xlsx({sql_str(str(path))}, sheet = {sql_str(sheet)}, "
            f"header = true, all_varchar = true)")
    con.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
    con.execute(f'CREATE TABLE "{schema}"."{table}" AS SELECT * FROM {expr}')
    n = con.execute(f'SELECT count(*) FROM "{schema}"."{table}"').fetchone()[0]
    return n, "workbook cells read as VARCHAR"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True, type=Path)
    ap.add_argument("--db", required=True, type=Path)
    ap.add_argument("--schema", required=True)
    ap.add_argument("--strip", default="", help="comma-separated leading path parts to drop from table names")
    ap.add_argument("--plan", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--min-bytes", type=int, default=1,
                    help="skip files smaller than this (default: skip empty files)")
    ap.add_argument("--match", default=None,
                    help="case-insensitive regex; only paths matching it are loaded")
    args = ap.parse_args()

    strip = tuple(slug(s) for s in args.strip.split(",") if s.strip())
    match = re.compile(args.match, re.I) if args.match else None

    files = sorted(p for p in args.src.rglob("*")
                   if p.is_file()
                   and p.suffix.lower() in (CSV_EXT | XLSX_EXT)
                   and not p.name.startswith("~$")
                   and p.stat().st_size >= args.min_bytes
                   and (match is None or match.search(str(p.relative_to(args.src)))))
    if not files:
        print(f"no tabular files under {args.src}", file=sys.stderr)
        return 1

    con = duckdb.connect(str(args.db))
    con.execute(MANIFEST_DDL)
    con.execute("INSTALL excel; LOAD excel;")
    con.execute(f'CREATE SCHEMA IF NOT EXISTS "{args.schema}"')

    done = {
        r[0] for r in con.execute(
            "SELECT duckdb_table FROM meta._load_manifest WHERE duckdb_schema = ?",
            [args.schema]).fetchall()
    }

    taken: set[str] = set(done)
    jobs: list[tuple[Path, str, str | None]] = []
    for path in files:
        rel = path.relative_to(args.src)
        if path.suffix.lower() in XLSX_EXT:
            try:
                sheets = xlsx_sheets(path)
            except (zipfile.BadZipFile, KeyError, ElementTree.ParseError, OSError) as exc:
                print(f"[warn] cannot read sheets of {rel}: {exc}", flush=True)
                continue
            for sheet in sheets:
                base = table_name_for(rel, strip, sheet if len(sheets) > 1 else None)
                jobs.append((path, unique(base, taken), sheet))
        else:
            jobs.append((path, unique(table_name_for(rel, strip), taken), None))

    if args.plan:
        for path, table, sheet in jobs:
            state = "SKIP" if table in done and not args.force else "load"
            obj = f" :: {sheet}" if sheet else ""
            print(f"{state:5s} {args.schema}.{table:70s} <- "
                  f"{path.relative_to(args.src)}{obj} ({path.stat().st_size/1e6:.1f} MB)")
        print(f"\n{len(jobs)} tables from {len(files)} files")
        return 0

    # The Dropbox tree ships several byte-identical copies of the same export
    # (Outpatient_2004_2014_complete_G1/G2/G3.csv are the same 732 MB file three
    # times). Load one, record the rest as duplicates pointing at it.
    # Checked across every schema, not just this one: the same GOBA.csv appears
    # in the staging folder and again inside the Dropbox pull, and there is no
    # reason to hold two copies of 160 MB under two names.
    hash_seen: dict[str, str] = {
        r[0]: f"{r[1]}.{r[2]}" for r in con.execute(
            "SELECT source_sha256, duckdb_schema, duckdb_table FROM meta._load_manifest "
            "WHERE source_sha256 IS NOT NULL AND duckdb_table IS NOT NULL "
            "AND loader = 'load_tabular.py'").fetchall()
    }

    failures = 0
    for path, table, sheet in jobs:
        if table in done and not args.force:
            continue
        t0 = time.time()
        rel = path.relative_to(args.src)

        digest = sha256_of(path)
        if sheet is None and digest in hash_seen:
            con.execute(
                "DELETE FROM meta._load_manifest WHERE duckdb_schema = ? AND source_path = ? "
                "AND duckdb_table IS NULL", [args.schema, str(path)])
            con.execute(
                """INSERT INTO meta._load_manifest VALUES
                   (?, NULL, ?, ?, ?, ?, NULL, NULL, NULL, now(), 'load_tabular.py', ?)""",
                [args.schema, str(path), path.stat().st_size, digest,
                 path.suffix.lower().lstrip("."),
                 f"NOT LOADED (duplicate): byte-identical to {hash_seen[digest]}"])
            print(f"[dup]  {rel} == {hash_seen[digest]}", flush=True)
            continue

        try:
            if sheet is not None:
                n_rows, note = load_xlsx(con, args.schema, table, path, sheet)
            else:
                delim = "\t" if path.suffix.lower() == ".tsv" else None
                n_rows, note = load_csv(con, args.schema, table, path, delim)
        except RuntimeError as exc:
            failures += 1
            print(f"[FAIL] {args.schema}.{table} <- {rel}: {exc}", flush=True)
            con.execute(
                "DELETE FROM meta._load_manifest WHERE duckdb_schema = ? AND duckdb_table = ?",
                [args.schema, table])
            con.execute(
                """INSERT INTO meta._load_manifest VALUES
                   (?, ?, ?, ?, ?, ?, ?, NULL, NULL, now(), 'load_tabular.py', ?)""",
                [args.schema, table, str(path), path.stat().st_size, digest,
                 path.suffix.lower().lstrip("."), sheet, f"FAILED: {exc}"])
            continue

        n_cols = con.execute(
            f'SELECT count(*) FROM duckdb_columns() WHERE schema_name = ? AND table_name = ?',
            [args.schema, table]).fetchone()[0]
        con.execute(
            "DELETE FROM meta._load_manifest WHERE duckdb_schema = ? AND duckdb_table = ?",
            [args.schema, table])
        con.execute(
            """INSERT INTO meta._load_manifest VALUES
               (?, ?, ?, ?, ?, ?, ?, ?, ?, now(), 'load_tabular.py', ?)""",
            [args.schema, table, str(path), path.stat().st_size, digest,
             path.suffix.lower().lstrip("."), sheet, n_rows, n_cols, note])
        if sheet is None:
            hash_seen.setdefault(digest, f"{args.schema}.{table}")
        print(f"[ok]   {args.schema}.{table}: {n_rows:,} rows, {n_cols} cols "
              f"in {time.time()-t0:.0f}s", flush=True)

    con.close()
    print(f"finished with {failures} failure(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
