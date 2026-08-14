#!/usr/bin/env python3
"""
Load raw CHIA Case Mix Microsoft Access (.mdb) files into DuckDB.

One DuckDB table per (mdb file x mdb table). Columns are carried over exactly
as Access declared them -- Text stays VARCHAR so leading zeros, the CHIA "-"
suppression sentinel, and the encrypted physician IDs survive untouched. Two
provenance columns are appended: _source_file and _data_year.

Every load is recorded in meta._load_manifest, so a re-run skips tables
that already landed with the expected row count.

Usage:
  load_chia_mdb.py --src DIR --db PATH [--plan] [--only PATTERN] [--force]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import duckdb

# ---------------------------------------------------------------- type mapping

# mdb-schema emits Access type names; map them onto DuckDB types. Text and Memo
# deliberately stay VARCHAR: these files encode zip codes, ICD codes and payer
# codes as fixed-width text, and any numeric coercion would silently eat leading
# zeros and turn the "-" suppression marker into a parse failure.
ACCESS_TO_DUCKDB = {
    "Text": "VARCHAR",
    "Memo/Hyperlink": "VARCHAR",
    "Memo": "VARCHAR",
    "Byte": "SMALLINT",
    "Integer": "INTEGER",
    "Long Integer": "BIGINT",
    "Single": "DOUBLE",
    "Double": "DOUBLE",
    "Currency": "DECIMAL(19,4)",
    "DateTime": "TIMESTAMP",
    "DateTime (Short)": "TIMESTAMP",
    "Boolean": "BOOLEAN",
    "Replication ID": "VARCHAR",
    "Numeric": "DOUBLE",
    "OLE": "VARCHAR",
    "Binary": "VARCHAR",
    "Unknown 0x00": "VARCHAR",
}

COL_RE = re.compile(r"^\s*\[(?P<name>[^\]]+)\]\s+(?P<type>.+?)\s*,?\s*$")

DATE_FMT = "%Y-%m-%d"
DATETIME_FMT = "%Y-%m-%d %H:%M:%S"


def normalize_access_type(raw: str) -> tuple[str, str | None]:
    """Split 'Numeric (18, 2) NOT NULL' into ('Numeric', '18, 2')."""
    s = re.sub(r"\s+NOT\s+NULL\s*$", "", raw.strip(), flags=re.I)
    m = re.match(r"^(?P<base>.*?)\s*\(\s*(?P<args>[^)]*)\s*\)\s*$", s)
    if m:
        return m.group("base").strip(), m.group("args").strip()
    return s.strip(), None


def access_type_to_duckdb(raw: str) -> str:
    """Map one mdb-schema type declaration (e.g. 'Text (4)') to a DuckDB type."""
    base, args = normalize_access_type(raw)
    if base in ("Numeric", "Decimal") and args and "," in args:
        precision, scale = (a.strip() for a in args.split(",", 1))
        if precision.isdigit() and scale.isdigit():
            return f"DECIMAL({precision},{scale})"
    if base in ACCESS_TO_DUCKDB:
        return ACCESS_TO_DUCKDB[base]
    # Unrecognised Access types are read as text rather than dropped, so nothing
    # is lost; the manifest note records that it happened.
    return "VARCHAR"


def mdb_tables(mdb: Path) -> list[str]:
    """Table names in an .mdb, or [] if mdbtools cannot open the file at all.

    At least one file in the CHIA pull is a truncated Dropbox artefact that
    mdbtools refuses; an unreadable source is reported, not fatal.
    """
    proc = subprocess.run(
        ["mdb-tables", "-1", str(mdb)], capture_output=True, text=True,
    )
    if proc.returncode != 0:
        return []
    return [t for t in (line.strip() for line in proc.stdout.splitlines()) if t]


def mdb_columns(mdb: Path, table: str) -> list[tuple[str, str, str]]:
    """Return [(column_name, access_type, duckdb_type)] for one table."""
    out = subprocess.run(
        ["mdb-schema", "-T", table, str(mdb)],
        capture_output=True, text=True, check=True,
    ).stdout
    cols: list[tuple[str, str, str]] = []
    inside = False
    for line in out.splitlines():
        stripped = line.strip()
        if not inside:
            if stripped.startswith("CREATE TABLE"):
                inside = True
            continue
        if stripped.startswith("("):
            continue
        if stripped.startswith(");") or stripped == ")":
            break
        m = COL_RE.match(line)
        if m:
            cols.append(
                (m.group("name"), m.group("type"),
                 access_type_to_duckdb(m.group("type")))
            )

    # Access allows two columns to share a name; DuckDB does not, and a repeated
    # key would also collapse silently in the columns spec.
    seen: dict[str, int] = {}
    deduped: list[tuple[str, str, str]] = []
    for name, access, duck in cols:
        seen[name] = seen.get(name, 0) + 1
        deduped.append((name if seen[name] == 1 else f"{name}_{seen[name]}",
                        access, duck))
    return deduped


# ------------------------------------------------------------------- naming

def target_table_name(mdb: Path, table: str) -> str:
    """Derive a stable snake_case DuckDB table name from the file + table name."""
    stem = mdb.stem
    year_m = re.search(r"(19|20)\d{2}", stem)
    year = year_m.group(0) if year_m else None

    # CHIA renamed the inpatient file HDD -> HIDD partway through; both are the
    # same database, so they share the hdd_ prefix and union cleanly by year.
    upper = stem.upper()
    if "_HDD_" in upper or "_HIDD_" in upper:
        dataset = "hdd"
    elif "_OOD_" in upper:
        dataset = "ood"
    else:
        dataset = "chia"

    suffix = ""
    if "WORKING" in stem.upper():
        suffix = "_working"
    dup = re.search(r"\((\d+)\)\s*$", stem)
    if dup:
        suffix = f"_copy{dup.group(1)}"

    tbl = re.sub(r"[^0-9a-zA-Z]+", "_", table).strip("_").lower()
    # Some Access tables already carry the year (e.g. "Discharge2007"); don't
    # repeat it.
    if year and tbl.endswith(year):
        tbl = tbl[: -len(year)].rstrip("_")
    parts = [dataset, tbl] + ([year] if year else [])
    return "_".join(parts) + suffix


def data_year(mdb: Path) -> int | None:
    m = re.search(r"(19|20)\d{2}", mdb.stem)
    return int(m.group(0)) if m else None


# ------------------------------------------------------------------ manifest

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


def sha256_of(path: Path, cache: dict[Path, str]) -> str:
    """Plain SHA-256 of the file.

    Deliberately NOT the content_hash recorded in the Dropbox pull manifests --
    that is Dropbox's own block-wise digest and would be wrong under a column
    named source_sha256.
    """
    if path in cache:
        return cache[path]
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8 << 20), b""):
            h.update(chunk)
    cache[path] = h.hexdigest()
    return cache[path]


# ---------------------------------------------------------------------- main

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True, type=Path)
    ap.add_argument("--db", required=True, type=Path)
    ap.add_argument("--tmp", type=Path, default=Path("/Volumes/MufflySamsung/chia_cadr_build/tmp"))
    ap.add_argument("--schema", default="chia_casemix")
    ap.add_argument("--only", default=None, help="substring filter on the .mdb filename")
    ap.add_argument("--plan", action="store_true", help="print the plan and exit")
    ap.add_argument("--force", action="store_true", help="reload tables already in the manifest")
    args = ap.parse_args()

    args.tmp.mkdir(parents=True, exist_ok=True)
    mdbs = sorted(p for p in args.src.rglob("*.mdb")
                  if args.only is None or args.only.lower() in str(p).lower())
    if not mdbs:
        print(f"no .mdb files under {args.src}", file=sys.stderr)
        return 1

    hash_cache: dict[Path, str] = {}

    con = duckdb.connect(str(args.db))
    con.execute(MANIFEST_DDL)
    con.execute(f'CREATE SCHEMA IF NOT EXISTS "{args.schema}"')

    done = {
        (r[0], r[1])
        for r in con.execute(
            "SELECT duckdb_schema, duckdb_table FROM meta._load_manifest"
        ).fetchall()
    }

    # The Dropbox tree keeps several byte-identical copies of the same .mdb in
    # different folders (CHIA_2016/, Extracted/, and the root). Hash first so a
    # 900 MB table is not loaded three times under three names; the duplicates
    # are still recorded, pointing at the copy that was loaded.
    already = {
        r[0]: r[1] for r in con.execute(
            "SELECT source_sha256, duckdb_table FROM meta._load_manifest "
            "WHERE source_kind = 'access_mdb' AND source_sha256 IS NOT NULL"
        ).fetchall()
    }
    seen_hash: dict[str, Path] = {}
    duplicates: list[tuple[Path, str]] = []
    unique_mdbs: list[Path] = []
    for mdb in mdbs:
        digest = sha256_of(mdb, hash_cache)
        if digest in seen_hash:
            duplicates.append((mdb, f"byte-identical to {seen_hash[digest].name}"))
        elif digest in already and not args.force:
            duplicates.append((mdb, f"byte-identical to already-loaded source of {already[digest]}"))
        else:
            seen_hash[digest] = mdb
            unique_mdbs.append(mdb)
    mdbs = unique_mdbs

    plan: list[tuple[Path, str, str, int]] = []
    unreadable: list[Path] = []
    for mdb in mdbs:
        tables = mdb_tables(mdb)
        if not tables:
            unreadable.append(mdb)
            continue
        for table in tables:
            plan.append((mdb, table, target_table_name(mdb, table), len(mdb_columns(mdb, table))))

    if args.plan:
        for mdb, table, target, ncol in plan:
            state = "SKIP (loaded)" if (args.schema, target) in done and not args.force else "load"
            print(f"{state:14s} {args.schema}.{target:32s} <- {mdb.name} :: {table} "
                  f"({ncol} cols, {mdb.stat().st_size/1e6:.0f} MB file)")
        for mdb in unreadable:
            print(f"{'UNREADABLE':14s} {'-':32s} <- {mdb.name} "
                  f"({mdb.stat().st_size/1e6:.1f} MB file)")
        for mdb, why in duplicates:
            print(f"{'DUPLICATE':14s} {'-':32s} <- {mdb} ({why})")
        return 0

    for mdb, why in duplicates:
        con.execute(
            "DELETE FROM meta._load_manifest WHERE source_path = ? AND duckdb_table IS NULL",
            [str(mdb)],
        )
        con.execute(
            """INSERT INTO meta._load_manifest VALUES
               (NULL, NULL, ?, ?, ?, 'access_mdb', NULL, NULL, NULL, now(),
                'load_chia_mdb.py', ?)""",
            [str(mdb), mdb.stat().st_size, sha256_of(mdb, hash_cache),
             f"NOT LOADED (duplicate): {why}"],
        )
        print(f"[duplicate] {mdb} -- {why}", flush=True)

    for mdb in unreadable:
        con.execute(
            "DELETE FROM meta._load_manifest WHERE source_path = ? AND duckdb_table IS NULL",
            [str(mdb)],
        )
        con.execute(
            """INSERT INTO meta._load_manifest VALUES
               (NULL, NULL, ?, ?, ?, 'access_mdb', NULL, NULL, NULL, now(),
                'load_chia_mdb.py', 'UNREADABLE: mdbtools could not open this file')""",
            [str(mdb), mdb.stat().st_size, sha256_of(mdb, hash_cache)],
        )
        print(f"[unreadable] {mdb.name} -- recorded in meta._load_manifest", flush=True)

    for mdb, table, target, _ in plan:
        if (args.schema, target) in done and not args.force:
            print(f"[skip] {args.schema}.{target} already loaded", flush=True)
            continue

        t0 = time.time()
        cols = mdb_columns(mdb, table)
        if not cols:
            print(f"[warn] no columns parsed for {mdb.name}::{table}, skipping", flush=True)
            continue

        csv_path = args.tmp / f"{target}.csv"
        print(f"[export] {mdb.name} :: {table} -> {csv_path.name}", flush=True)
        with csv_path.open("wb") as out:
            subprocess.run(
                ["mdb-export",
                 "-D", DATE_FMT, "-T", DATETIME_FMT,
                 "-B",                      # TRUE/FALSE for Boolean columns
                 "-H",                      # no header row -- see below
                 str(mdb), table],
                stdout=out, check=True,
            )

        col_spec = ", ".join(f"'{name.replace(chr(39), chr(39)*2)}': '{dtype}'"
                             for name, _, dtype in cols)
        year = data_year(mdb)
        year_sql = str(year) if year is not None else "NULL"

        con.execute(f'DROP TABLE IF EXISTS "{args.schema}"."{target}"')
        con.execute(f"""
            CREATE TABLE "{args.schema}"."{target}" AS
            SELECT *,
                   '{mdb.name.replace("'", "''")}' AS _source_file,
                   CAST({year_sql} AS INTEGER)     AS _data_year
            FROM read_csv(
                '{csv_path.as_posix()}',
                -- Headerless on purpose: mdb-export does not quote the header
                -- row, so a column name containing a comma (Access allows it,
                -- e.g. "age MERGED HG, DOX") would split into two fields and
                -- desync the column count. The names come from mdb-schema.
                header       = false,
                columns      = {{{col_spec}}},
                quote        = '"',
                escape       = '"',
                nullstr      = '',
                sample_size  = -1,
                ignore_errors = false
            )
        """)
        n_rows = con.execute(
            f'SELECT count(*) FROM "{args.schema}"."{target}"').fetchone()[0]

        unknown = sorted({a for _, a, d in cols
                          if d == "VARCHAR"
                          and normalize_access_type(a)[0] not in ACCESS_TO_DUCKDB})
        note = ("unmapped Access types read as VARCHAR: " + ", ".join(unknown)) if unknown else None

        con.execute(
            "DELETE FROM meta._load_manifest WHERE duckdb_schema = ? AND duckdb_table = ?",
            [args.schema, target],
        )
        con.execute(
            """INSERT INTO meta._load_manifest VALUES
               (?, ?, ?, ?, ?, 'access_mdb', ?, ?, ?, now(), 'load_chia_mdb.py', ?)""",
            [args.schema, target, str(mdb), mdb.stat().st_size,
             sha256_of(mdb, hash_cache), table, n_rows, len(cols), note],
        )
        csv_path.unlink(missing_ok=True)
        print(f"[ok]   {args.schema}.{target}: {n_rows:,} rows, {len(cols)} cols "
              f"in {time.time()-t0:.0f}s", flush=True)

    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
