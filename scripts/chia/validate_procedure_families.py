#!/usr/bin/env python3
"""Validate config/chia_urps_inpatient_codes.yml against the CHIA database.

Three assertions per family:
  1. every listed code exists in the reference tables
  2. the family does not jump implausibly across the FY2015/FY2016 ICD seam
     (the mapping-artefact signature that produced 189 -> 5,710 -> 1,065)
  3. excluded obstetric codes never enter a family's counts

Exit 1 on any structural failure. Seam instability is reported, and families
already declared seam_unstable in the YAML are expected, not failures.
"""
from __future__ import annotations
import argparse, sys
import duckdb, yaml

def q(con, sql, p=None):
    return con.execute(sql, p or []).fetchall()

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--config", default="config/chia_urps_inpatient_codes.yml")
    a = ap.parse_args()
    cfg = yaml.safe_load(open(a.config))
    con = duckdb.connect(a.db, read_only=True)
    fails, warns = [], []

    excluded = set()
    for grp in cfg.get("exclusions", {}).values():
        excluded.update(grp.get("icd10pcs", []))

    for name, fam in cfg["families"].items():
        # --- 1. codes exist ---------------------------------------------------
        retired = set(fam.get("retired_icd9", []))
        for code in fam.get("icd9cm", {}).get("exact", []):
            if not q(con, "SELECT 1 FROM ref.icd9cm_procedure WHERE code_nodot = ?", [code]):
                if code in retired:
                    # ref.icd9cm_procedure is v32, the FINAL revision, so codes
                    # withdrawn before it are legitimately absent. Validate
                    # against the data instead: a retired code must actually
                    # appear, or it is a typo rather than a retirement.
                    n = q(con, """SELECT count(*) FROM chia_casemix.v_cohort_female_adult
                                  WHERE principal_procedure = ?""", [code])[0][0]
                    if n == 0:
                        fails.append(f"{name}: retired ICD-9 {code} appears nowhere in the data")
                    else:
                        warns.append(f"{name}: ICD-9 {code} retired pre-v32, "
                                     f"validated against data ({n:,} cases)")
                else:
                    fails.append(f"{name}: ICD-9 {code} not in ref.icd9cm_procedure")
        for pre in fam.get("icd10pcs", {}).get("prefix", []):
            n = q(con, "SELECT count(*) FROM ref.icd10pcs_procedure WHERE code_nodot LIKE ?||'%'",
                  [pre])[0][0]
            if n == 0:
                fails.append(f"{name}: ICD-10-PCS prefix {pre} matches no code")

        # --- 3. no excluded code reachable -----------------------------------
        for pre in fam.get("icd10pcs", {}).get("prefix", []):
            hit = [e for e in excluded if e.startswith(pre)]
            if hit:
                fails.append(f"{name}: prefix {pre} would capture excluded {hit}")

        # --- 2. seam stability ------------------------------------------------
        i9 = fam.get("icd9cm", {}).get("exact", [])
        i10 = fam.get("icd10pcs", {}).get("prefix", [])
        if not (i9 and i10):
            continue
        i9_list = ",".join(f"'{c}'" for c in i9)
        i10_like = " OR ".join(f"principal_procedure LIKE '{p}%'" for p in i10)
        dxq = fam.get("requires_diagnosis")
        dx_join, dx_where = "", ""
        if dxq:
            spec = cfg["diagnosis_qualifiers"][dxq]
            pats = spec["icd9cm"] + spec["icd10cm"]
            if spec["match"] == "prefix":
                cond = " OR ".join(f"d.code LIKE '{c}%'" for c in pats)
            else:
                cond = "d.code IN (" + ",".join(f"'{c}'" for c in pats) + ")"
            dx_join = ("JOIN (SELECT DISTINCT RecordType20ID, _data_year "
                       f"FROM chia_casemix.hdd_diagnosis_long d WHERE {cond}) dx "
                       "USING (RecordType20ID, _data_year)")
        rows = q(con, f"""
            SELECT _data_year, count(*) FROM chia_casemix.v_cohort_female_adult
            {dx_join}
            WHERE (_data_year <= 2015 AND principal_procedure IN ({i9_list}))
               OR (_data_year >= 2016 AND ({i10_like}))
            GROUP BY 1 ORDER BY 1""")
        by_year = dict(rows)
        # SECOND SEAM. The validator originally checked only the FY2015/16
        # ICD-9 -> ICD-10 boundary and so missed the October 2006 ICD-9 update,
        # which withdrew 3-digit hysterectomy codes. Any family whose members
        # span that revision gets the same ratio test.
        icd9_seam = cfg["validation"].get("icd9_revision_seam_years")
        if icd9_seam:
            a_yr, b_yr = icd9_seam
            ra = q(con, f"""SELECT count(*) FROM chia_casemix.v_cohort_female_adult
                            WHERE _data_year = {a_yr}
                              AND principal_procedure IN ({i9_list})""")[0][0]
            rb = q(con, f"""SELECT count(*) FROM chia_casemix.v_cohort_female_adult
                            WHERE _data_year = {b_yr}
                              AND principal_procedure IN ({i9_list})""")[0][0]
            if ra and rb:
                r2 = max(ra, rb) / min(ra, rb)
                if r2 > cfg["validation"]["max_seam_ratio"]:
                    fails.append(f"{name}: ICD-9 revision seam FY{a_yr}={ra:,} -> "
                                 f"FY{b_yr}={rb:,} (ratio {r2:.1f}x) -- a code "
                                 "withdrawn in the October 2006 update is "
                                 "probably missing from the family")

        pre_yr, post_yr = cfg["validation"]["seam_years"]
        pre, post = by_year.get(pre_yr, 0), by_year.get(post_yr, 0)
        if pre and post:
            ratio = max(pre, post) / min(pre, post)
            if ratio > cfg["validation"]["max_seam_ratio"]:
                msg = (f"{name}: FY{pre_yr}={pre:,} -> FY{post_yr}={post:,} "
                       f"(ratio {ratio:.1f}x)")
                if name in cfg["validation"].get("known_seam_unstable", []):
                    warns.append(msg + "  [declared seam_unstable]")
                else:
                    fails.append(msg + "  -- undeclared seam instability")
        elif not post:
            warns.append(f"{name}: no FY{post_yr} cases -- ICD-10 mapping may be wrong")

    con.close()
    for w in warns:
        print(f"[warn] {w}")
    for f in fails:
        print(f"[FAIL] {f}")
    print(f"\n{len(cfg['families'])} families; {len(fails)} failure(s), {len(warns)} warning(s)")
    return 1 if fails else 0

if __name__ == "__main__":
    sys.exit(main())
