#!/usr/bin/env python3
"""
Post-load pass over chia_cadr.duckdb.

1. Backfills meta._load_manifest.source_sha256 for any row whose hash was taken
   from a Dropbox pull manifest -- Dropbox's content_hash is a block-wise digest,
   not a SHA-256, and it must not sit in a column called source_sha256.
2. Builds year-spanning views over the per-year Case Mix tables. The Access
   schema changes across years (144 / 159 / 179 / 124 columns for Discharge), so
   the views UNION ALL BY NAME: a column missing in one year reads NULL there
   instead of the union failing or silently misaligning.
3. Writes meta._table_catalog, a one-row-per-table index of what is in the
   database and where it came from.

Usage: finalize_db.py --db PATH [--rehash]
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from pathlib import Path

import duckdb


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def rehash(con) -> None:
    rows = con.execute(
        "SELECT DISTINCT source_path FROM meta._load_manifest WHERE source_path IS NOT NULL"
    ).fetchall()
    for (src,) in rows:
        path = Path(src)
        if not path.exists():
            print(f"[skip] missing source, hash left as recorded: {src}", flush=True)
            continue
        digest = sha256_of(path)
        con.execute(
            "UPDATE meta._load_manifest SET source_sha256 = ? WHERE source_path = ?",
            [digest, src])
        print(f"[hash] {digest[:16]}..  {path.name}", flush=True)


# CHIA spells the same Access table differently in different years. Without this
# the 2016 diagnosis file forms its own family and v_hdd_diagnosiscode_all_years
# silently omits 9.5M rows.
FAMILY_ALIASES = {
    "hdd_diagnosescode": "hdd_diagnosiscode",
}


def grouper_views(con) -> None:
    """Views over the CMS-DRG and APR-DRG grouper tables.

    These cannot be unioned as-is: the grouper version is baked into every column
    name (CMS320_DIS_DRG in 2015, CMS330_DIS_DRG in 2016), so UNION ALL BY NAME
    yields a sparse matrix instead of a series. Here the version moves out of the
    column name and into a `grouper_version` column, leaving one stable set of
    field names. This is a rename, not a recode -- no value is altered.
    """
    for prefix, view in (("cms", "v_hdd_cmsdrg_all_years"),
                         ("apr", "v_hdd_aprdrg_all_versions")):
        tables = [r[0] for r in con.execute(f"""
            SELECT table_name FROM duckdb_tables()
            WHERE schema_name = 'chia_casemix'
              AND regexp_matches(table_name, '^hdd_{prefix}[0-9]+_(19|20)[0-9][0-9]$')
            ORDER BY table_name""").fetchall()]
        if not tables:
            continue
        parts = []
        for tbl in tables:
            version = re.fullmatch(rf"hdd_({prefix}\d+)_\d{{4}}", tbl).group(1).upper()
            cols = [r[0] for r in con.execute(
                "SELECT column_name FROM duckdb_columns() "
                "WHERE schema_name='chia_casemix' AND table_name=? ORDER BY column_index",
                [tbl]).fetchall()]
            sel = [f"'{version}' AS grouper_version"]
            for c in cols:
                # CMS330_DIS_DRG -> dis_drg; RecordType20ID / _source_file stay put
                stripped = re.sub(rf"^{version}_", "", c, flags=re.I)
                sel.append(f'"{c}" AS "{stripped.lower()}"' if stripped != c else f'"{c}"')
            parts.append(f'SELECT {", ".join(sel)} FROM chia_casemix."{tbl}"')
        body = "\nUNION ALL BY NAME\n".join(parts)
        con.execute(f'CREATE OR REPLACE VIEW chia_casemix."{view}" AS\n{body}')
        n = con.execute(f'SELECT count(*) FROM chia_casemix."{view}"').fetchone()[0]
        print(f"[view] chia_casemix.{view}: {len(tables)} tables, {n:,} rows", flush=True)


def year_union_views(con) -> list[tuple[str, int, int]]:
    """One view per (schema, family) over the per-year tables in that family."""
    tables = con.execute("""
        SELECT schema_name, table_name
        FROM duckdb_tables()
        WHERE schema_name LIKE 'chia%'
        ORDER BY schema_name, table_name
    """).fetchall()

    families: dict[tuple[str, str], list[tuple[str, int]]] = {}
    for schema, table in tables:
        # hdd_discharge_2007 -> family hdd_discharge, year 2007. Anything with a
        # trailing qualifier (_working, _copy2) is a variant copy, not part of
        # the canonical year series, and stays out of the view.
        m = re.fullmatch(r"(?P<fam>.+?)_(?P<year>(?:19|20)\d{2})", table)
        if not m:
            continue
        fam = FAMILY_ALIASES.get(m.group("fam"), m.group("fam"))
        families.setdefault((schema, fam), []).append(
            (table, int(m.group("year"))))

    made = []
    for (schema, fam), members in sorted(families.items()):
        if len(members) < 2:
            continue
        members.sort(key=lambda t: t[1])
        body = "\nUNION ALL BY NAME\n".join(
            f'SELECT * FROM "{schema}"."{tbl}"' for tbl, _ in members)
        view = f"v_{fam}_all_years"
        con.execute(f'CREATE OR REPLACE VIEW "{schema}"."{view}" AS\n{body}')
        n = con.execute(f'SELECT count(*) FROM "{schema}"."{view}"').fetchone()[0]
        made.append((f"{schema}.{view}", len(members), n))
        print(f"[view] {schema}.{view}: {len(members)} years, {n:,} rows", flush=True)
    return made


def catalog(con) -> None:
    con.execute("DROP TABLE IF EXISTS meta._table_catalog")
    con.execute("""
        CREATE TABLE meta._table_catalog AS
        SELECT
            t.schema_name                                   AS duckdb_schema,
            t.table_name                                    AS duckdb_table,
            t.estimated_size                                AS approx_rows,
            t.column_count                                  AS n_columns,
            m.source_path,
            m.source_kind,
            m.source_object,
            m.n_rows                                        AS loaded_rows,
            m.source_sha256,
            m.notes
        FROM duckdb_tables() t
        LEFT JOIN meta._load_manifest m
               ON m.duckdb_schema = t.schema_name
              AND m.duckdb_table  = t.table_name
        WHERE t.schema_name NOT IN ('main', 'meta')
        ORDER BY t.schema_name, t.table_name
    """)
    n = con.execute("SELECT count(*) FROM meta._table_catalog").fetchone()[0]
    print(f"[catalog] meta._table_catalog: {n} tables", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, type=Path)
    ap.add_argument("--rehash", action="store_true",
                    help="recompute source_sha256 for every source file")
    args = ap.parse_args()

    con = duckdb.connect(str(args.db))
    if args.rehash:
        rehash(con)
    year_union_views(con)
    grouper_views(con)
    catalog(con)
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
