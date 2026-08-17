#!/usr/bin/env python3
"""
Repair tables that lost rows to the CSV loader's ignore_errors fallback.

Cause: 12 source CSVs are Windows-1252, not UTF-8. The high bytes are real
content -- accented physician names (Cote-Sainte-Catherine, Montanita), curly
quotes, en dashes -- sitting in name and address fields. DuckDB rejects the
line as invalid UTF-8, and `ignore_errors = true` then drops it silently, so the
table came up short by exactly the number of non-UTF-8 lines.

Neither encoding='utf-8' nor encoding='latin-1' works: 0x93/0x94 are undefined
in ISO-8859-1, so DuckDB rejects that too. The fix is to transcode cp1252 ->
UTF-8 once, into a sidecar file, and reload from that. Originals are never
modified.

Usage: repair_encoding.py --db PATH [--outdir DIR] [--dry-run]
"""

from __future__ import annotations

import argparse
import hashlib
import shutil
import sys
from pathlib import Path

import duckdb


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def bad_utf8_lines(path: Path) -> int:
    raw = path.read_bytes().split(b"\n")
    if raw and raw[-1] == b"":
        raw = raw[:-1]
    n = 0
    for line in raw:
        try:
            line.decode("utf-8")
        except UnicodeDecodeError:
            n += 1
    return n


def transcode(src: Path, dst: Path) -> str | None:
    """cp1252 -> UTF-8. Returns a note if any byte was not valid cp1252 either."""
    raw = src.read_bytes()
    note = None
    try:
        text = raw.decode("cp1252")
    except UnicodeDecodeError:
        text = raw.decode("cp1252", errors="replace")
        note = "some bytes were not valid cp1252 either and became U+FFFD"
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text(text, encoding="utf-8")
    return note


def sql_str(v: str) -> str:
    return "'" + v.replace("'", "''") + "'"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, type=Path)
    ap.add_argument("--outdir", type=Path,
                    default=Path("/Volumes/MufflySamsung/chia_cadr_build/transcoded"))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    con = duckdb.connect(str(args.db))
    targets = con.execute("""
        SELECT duckdb_schema, duckdb_table, source_path, n_rows, notes
        FROM meta._load_manifest
        WHERE notes LIKE '%ignore_errors%' AND duckdb_table IS NOT NULL
        ORDER BY 1, 2
    """).fetchall()

    print(f"{len(targets)} tables to repair\n", flush=True)
    repaired = fixed_rows = 0

    for schema, table, src_str, old_rows, old_notes in targets:
        src = Path(src_str)
        if not src.exists():
            print(f"[skip] {schema}.{table}: source missing ({src})", flush=True)
            continue

        n_bad = bad_utf8_lines(src)
        if n_bad == 0:
            print(f"[skip] {schema}.{table}: no invalid UTF-8; "
                  f"row loss has another cause -- left alone", flush=True)
            continue

        dst = args.outdir / src.name
        if args.dry_run:
            print(f"[plan] {schema}.{table}: {old_rows} rows, {n_bad} cp1252 lines "
                  f"-> transcode {src.name}", flush=True)
            continue

        tnote = transcode(src, dst)

        # Reload WITHOUT ignore_errors. If it still fails, the table is left as
        # it was rather than replaced by something worse.
        expr = (f"read_csv({sql_str(str(dst))}, header = true, sample_size = -1, "
                f"union_by_name = false, all_varchar = true)")
        staging = f"{table}__repair"
        try:
            con.execute(f'DROP TABLE IF EXISTS "{schema}"."{staging}"')
            con.execute(f'CREATE TABLE "{schema}"."{staging}" AS SELECT * FROM {expr}')
        except duckdb.Error as exc:
            con.execute(f'DROP TABLE IF EXISTS "{schema}"."{staging}"')
            print(f"[FAIL] {schema}.{table}: reload still errors, original kept "
                  f"({str(exc).splitlines()[0][:90]})", flush=True)
            continue

        new_rows = con.execute(
            f'SELECT count(*) FROM "{schema}"."{staging}"').fetchone()[0]
        if new_rows < old_rows:
            con.execute(f'DROP TABLE IF EXISTS "{schema}"."{staging}"')
            print(f"[FAIL] {schema}.{table}: repair had FEWER rows "
                  f"({new_rows} < {old_rows}); original kept", flush=True)
            continue

        con.execute(f'DROP TABLE "{schema}"."{table}"')
        con.execute(f'ALTER TABLE "{schema}"."{staging}" RENAME TO "{table}"')
        n_cols = con.execute(
            "SELECT count(*) FROM duckdb_columns() WHERE schema_name = ? AND table_name = ?",
            [schema, table]).fetchone()[0]

        note = (f"source is Windows-1252; transcoded to UTF-8 at {dst} and reloaded "
                f"without ignore_errors, recovering {new_rows - old_rows} row(s) "
                f"that the first load dropped")
        if tnote:
            note += f"; {tnote}"
        # Carry forward any non-fallback note that was already there.
        keep = [p.strip() for p in (old_notes or "").split(" | ")
                if p.strip() and "ignore_errors" not in p and "sniffer failed" not in p]
        note = " | ".join(keep + [note])

        con.execute(
            "UPDATE meta._load_manifest SET source_path = ?, source_sha256 = ?, "
            "n_rows = ?, n_columns = ?, loaded_at = now(), "
            "loader = 'repair_encoding.py', notes = ? "
            "WHERE duckdb_schema = ? AND duckdb_table = ?",
            [str(dst), sha256_of(dst), new_rows, n_cols, note, schema, table])

        repaired += 1
        fixed_rows += new_rows - old_rows
        print(f"[ok]   {schema}.{table}: {old_rows} -> {new_rows} rows "
              f"(+{new_rows - old_rows}), {n_bad} cp1252 lines", flush=True)

    con.close()
    print(f"\nrepaired {repaired} tables, recovered {fixed_rows} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
