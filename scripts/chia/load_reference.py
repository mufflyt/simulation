#!/usr/bin/env python3
"""
Build the `ref` schema: the code dictionaries CHIA needs but does not ship.

CHIA stores nothing but bare codes -- ICD-9-CM through FY2015, ICD-10-CM/PCS
from FY2016, UB-04 revenue codes, DRGs -- and no descriptions for any of them.
Without this schema every analysis has to decode by hand.

THE JOIN KEY. CHIA writes codes WITHOUT decimal points ('7050', 'N3946'), and so
do the CMS distribution files. Human-facing sources (the urogyn crosswalk) use
dotted form ('70.50', 'N39.46'). Every table here therefore carries BOTH:
  code        as the source publishes it
  code_nodot  punctuation stripped, uppercased -- this is what joins to CHIA
Always join on code_nodot.

Sources are public domain (CMS) or freely published (NUCC). CPT is deliberately
absent: it is AMA-licensed and cannot be redistributed here.

Usage: load_reference.py --db PATH [--plan]
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import re
import sys
import tempfile
from pathlib import Path

import duckdb

DL = Path("/Volumes/MufflySamsung/chia_cadr_build/ref_downloads")

SOURCES = {
    "icd9cm_diagnosis":  "CMS ICD-9-CM v32 master descriptions (public domain)",
    "icd9cm_procedure":  "CMS ICD-9-CM v32 master descriptions (public domain)",
    "icd10cm_diagnosis": "CMS 2019 ICD-10-CM code descriptions, order file (public domain)",
    "icd10pcs_procedure":"CMS 2019 ICD-10-PCS order file (public domain)",
    "nucc_taxonomy":     "NUCC Health Care Provider Taxonomy v25.1 (freely published)",
    "msdrg_fy2019":      "CMS FY2019 IPPS Final Rule Table 5 (MS-DRG list, public domain)",
    "urogyn_icd9_icd10_crosswalk":
                         "Urogynecology ICD-9 to ICD-10 crosswalk (local working file, curated)",
}


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_text(path: Path) -> list[str]:
    """CMS ships at least one file with Windows-1252 bytes; decode accordingly."""
    raw = path.read_bytes()
    try:
        return raw.decode("utf-8").splitlines()
    except UnicodeDecodeError:
        return raw.decode("cp1252").splitlines()


def nodot(code: str) -> str:
    return re.sub(r"[^0-9A-Za-z]", "", code or "").upper()


def dot_icd9_dx(code: str) -> str:
    """0010 -> 001.0 ; V1251 -> V12.51 ; E8497 -> E849.7"""
    c = code.strip().upper()
    if not c:
        return c
    head = 4 if c.startswith("E") else 3
    return c if len(c) <= head else f"{c[:head]}.{c[head:]}"


def dot_icd9_sg(code: str) -> str:
    """7050 -> 70.50"""
    c = code.strip().upper()
    return c if len(c) <= 2 else f"{c[:2]}.{c[2:]}"


def dot_icd10(code: str) -> str:
    """N3946 -> N39.46 ; 0TSD0ZZ (PCS) is never dotted."""
    c = code.strip().upper()
    return c if len(c) <= 3 else f"{c[:3]}.{c[3:]}"


def parse_icd9_pair(long_path: Path, short_path: Path) -> list[tuple[str, str, str]]:
    """CMS ICD-9 files are 'CODE<spaces>DESCRIPTION' per line."""
    def parse(p: Path) -> dict[str, str]:
        out: dict[str, str] = {}
        for line in read_text(p):
            if not line.strip():
                continue
            m = re.match(r"^(\S+)\s+(.*)$", line.rstrip())
            if m:
                out[m.group(1).strip()] = m.group(2).strip()
        return out
    long_d, short_d = parse(long_path), parse(short_path)
    return [(c, long_d[c], short_d.get(c, "")) for c in long_d]


def parse_order_file(path: Path) -> list[tuple[int, str, bool, str, str]]:
    """CMS ICD-10 order-file layout, fixed width:
         1-5 order no | 7-13 code | 15 billable flag | 17-76 short | 78+ long
    """
    rows = []
    for line in read_text(path):
        if len(line) < 17:
            continue
        rows.append((
            int(line[0:5]),
            line[6:13].strip(),
            line[14:15].strip() == "1",
            line[16:76].strip(),
            line[77:].strip(),
        ))
    return rows


def bulk_load(con, schema: str, table: str, columns: list[tuple[str, str]],
              rows: list[tuple]) -> None:
    """Stage through a CSV and let DuckDB read it.

    executemany issues one INSERT per row, which for a 94k-row code dictionary
    takes minutes; a staged CSV read is effectively instantaneous.
    """
    ddl = ", ".join(f'"{n}" {t}' for n, t in columns)
    con.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
    con.execute(f'CREATE TABLE "{schema}"."{table}" ({ddl})')

    # Batched multi-row INSERT. Not a CSV round-trip: some ICD-10 long
    # descriptions contain characters that survive csv.writer but confuse the
    # reader's dialect detection, and not executemany, which issues one
    # statement per row and takes minutes on a 94k-row dictionary.
    n_cols = len(columns)
    row_ph = "(" + ",".join("?" * n_cols) + ")"
    batch = 2000
    con.execute("BEGIN TRANSACTION")
    try:
        for i in range(0, len(rows), batch):
            chunk = rows[i:i + batch]
            sql = (f'INSERT INTO "{schema}"."{table}" VALUES '
                   + ",".join([row_ph] * len(chunk)))
            con.execute(sql, [v for r in chunk for v in r])
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def register(con, schema: str, table: str, source_path: Path, note: str) -> None:
    n_rows = con.execute(f'SELECT count(*) FROM "{schema}"."{table}"').fetchone()[0]
    n_cols = con.execute(
        "SELECT count(*) FROM duckdb_columns() WHERE schema_name = ? AND table_name = ?",
        [schema, table]).fetchone()[0]
    con.execute("DELETE FROM meta._load_manifest WHERE duckdb_schema = ? AND duckdb_table = ?",
                [schema, table])
    con.execute(
        """INSERT INTO meta._load_manifest VALUES
           (?, ?, ?, ?, ?, 'reference', NULL, ?, ?, now(), 'load_reference.py', ?)""",
        [schema, table, str(source_path),
         source_path.stat().st_size if source_path.exists() else None,
         sha256_of(source_path) if source_path.exists() else None,
         n_rows, n_cols, note])
    print(f"[ok]   ref.{table}: {n_rows:,} rows, {n_cols} cols", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, type=Path)
    args = ap.parse_args()

    con = duckdb.connect(str(args.db))
    con.execute("CREATE SCHEMA IF NOT EXISTS ref")
    con.execute("INSTALL excel; LOAD excel;")

    # ---- ICD-9-CM diagnosis + procedure -------------------------------------
    base = DL / "ICD-9-CM-v32-master-descriptions"
    for table, stem, dotter in (
        ("icd9cm_diagnosis", "DX", dot_icd9_dx),
        ("icd9cm_procedure", "SG", dot_icd9_sg),
    ):
        rows = parse_icd9_pair(base / f"CMS32_DESC_LONG_{stem}.txt",
                               base / f"CMS32_DESC_SHORT_{stem}.txt")
        bulk_load(con, "ref", table,
                  [("code", "VARCHAR"), ("code_nodot", "VARCHAR"),
                   ("code_dotted", "VARCHAR"), ("long_desc", "VARCHAR"),
                   ("short_desc", "VARCHAR")],
                  [(c, nodot(c), dotter(c), lg, sh) for c, lg, sh in rows])
        register(con, "ref", table, base / f"CMS32_DESC_LONG_{stem}.txt", SOURCES[table])

    # ---- ICD-10-CM and ICD-10-PCS, unioned across editions ------------------
    # CMS deletes codes between editions. CHIA spans FY2016-2018, so the 2019
    # edition alone misses codes retired before it. Load 2016 + 2017 + 2019 and
    # keep one row per code, described by the newest edition that carries it,
    # with `editions` recording where it appeared.
    EDITIONS = {
        "icd10cm_diagnosis": [
            (2016, DL / "2016-Code-Descriptions-in-Tabular-Order" / "icd10cm_order_2016.txt"),
            (2017, DL / "2017-ICD10-Code-Descriptions" / "icd10cm_order_2017.txt"),
            (2019, DL / "2019-ICD-10-CM-Code-Descriptions" / "icd10cm_order_2019.txt"),
        ],
        "icd10pcs_procedure": [
            (2016, DL / "2016-PCS-Long-Abbrev-Titles" / "icd10pcs_order_2016.txt"),
            (2017, DL / "2017-PCS-Long-Abbrev-Titles" / "icd10pcs_order_2017.txt"),
            (2019, DL / "2019-ICD-10-PCS-Order-File" / "icd10pcs_order_2019.txt"),
        ],
    }
    for table, dotter in (("icd10cm_diagnosis", dot_icd10),
                          ("icd10pcs_procedure", lambda c: c)):
        merged: dict[str, list] = {}
        for fy, path in EDITIONS[table]:
            if not path.exists():
                print(f"[warn] missing edition {fy} for {table}: {path}", flush=True)
                continue
            for o, c, b, s, l in parse_order_file(path):
                key = nodot(c)
                if key in merged:
                    merged[key][0] = max(merged[key][0], fy)
                    merged[key][1].append(fy)
                    if fy >= merged[key][0]:          # newest description wins
                        merged[key][2:] = [o, c, b, s, l]
                else:
                    merged[key] = [fy, [fy], o, c, b, s, l]
        rows = [(o, c, nodot(c), dotter(c), b, s, l,
                 ",".join(str(x) for x in sorted(set(eds))), latest)
                for latest, eds, o, c, b, s, l in merged.values()]
        bulk_load(con, "ref", table,
                  [("order_no", "INTEGER"), ("code", "VARCHAR"),
                   ("code_nodot", "VARCHAR"), ("code_dotted", "VARCHAR"),
                   ("is_billable", "BOOLEAN"), ("short_desc", "VARCHAR"),
                   ("long_desc", "VARCHAR"), ("editions", "VARCHAR"),
                   ("latest_edition", "INTEGER")],
                  rows)
        register(con, "ref", table, EDITIONS[table][-1][1],
                 SOURCES[table] + "; unioned across the FY2016, FY2017 and FY2019 editions")

    # ---- NUCC provider taxonomy ---------------------------------------------
    nucc = Path("/Volumes/MufflySamsung/nucc/nucc_taxonomy_251.csv")
    con.execute("DROP TABLE IF EXISTS ref.nucc_taxonomy")
    con.execute(f"""CREATE TABLE ref.nucc_taxonomy AS
                    SELECT * FROM read_csv('{nucc}', header=true, all_varchar=true, sample_size=-1)""")
    register(con, "ref", "nucc_taxonomy", nucc, SOURCES["nucc_taxonomy"])

    # ---- MS-DRG (FY2019 IPPS Table 5); the title row sits above the header ---
    drg = Path("/Volumes/MufflySamsung/cadr_extracted/Data/DRGs CMS-1694-F Table 5.xlsx")
    con.execute("DROP TABLE IF EXISTS ref.msdrg_fy2019")
    con.execute(f"""CREATE TABLE ref.msdrg_fy2019 AS
                    SELECT * FROM read_xlsx('{drg}', all_varchar=true, range='A2:H800')""")
    register(con, "ref", "msdrg_fy2019", drg, SOURCES["msdrg_fy2019"])

    # ---- urogynecology ICD-9 -> ICD-10 crosswalk ----------------------------
    xw = Path("/Users/tylermuffly/Documents/Documents - TMuff/"
              "Urogynecology_ICD9_to_ICD10_Crosswalks_Final.xlsx")
    if xw.exists():
        con.execute("DROP TABLE IF EXISTS ref.urogyn_icd9_icd10_crosswalk")
        con.execute(f"""CREATE TABLE ref.urogyn_icd9_icd10_crosswalk AS
            SELECT "ICD 9"             AS icd9_dotted,
                   upper(regexp_replace("ICD 9", '[^0-9A-Za-z]', '', 'g'))       AS icd9_nodot,
                   "ICD 9 Description" AS icd9_desc,
                   "ICD 10 Code"       AS icd10_dotted,
                   upper(regexp_replace("ICD 10 Code", '[^0-9A-Za-z]', '', 'g')) AS icd10_nodot,
                   "ICD 10 Description" AS icd10_desc
            FROM read_xlsx('{xw}', all_varchar=true)""")
        register(con, "ref", "urogyn_icd9_icd10_crosswalk", xw,
                 SOURCES["urogyn_icd9_icd10_crosswalk"])

    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
