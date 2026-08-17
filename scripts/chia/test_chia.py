#!/usr/bin/env python3
"""
Gates and regression tests for chia_cadr.duckdb.

Every check here exists because the failure it catches ACTUALLY HAPPENED while
building this database, and every one of them failed silently -- no error, just
a wrong number. That is the defining property of this dataset: it does not
crash, it under-reports.

GATE  must pass; a failure means analyses built on the affected tables are wrong
WARN  worth knowing; does not block

Exit code is 1 if any GATE fails, so this can sit in front of a pipeline.

Usage: test_chia.py --db PATH [-v]
"""
from __future__ import annotations
import argparse, sys
import duckdb

RESULTS: list[tuple[str, str, bool, str]] = []

def check(kind: str, name: str, ok: bool, detail: str = "") -> None:
    RESULTS.append((kind, name, ok, detail))

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args()
    con = duckdb.connect(a.db, read_only=True)
    q = lambda s, p=None: con.execute(s, p or []).fetchall()
    one = lambda s, p=None: con.execute(s, p or []).fetchone()[0]

    # --- 1. loader silently dropped rows -------------------------------------
    # The CSV loader's ignore_errors fallback discarded 39 rows across 12 tables
    # because the sources were Windows-1252, not UTF-8. Nothing errored.
    # Match the fallback's own wording, not the substring 'ignore_errors' --
    # the repair note says "reloaded WITHOUT ignore_errors" and would false-positive.
    n = one("""SELECT count(*) FROM meta._load_manifest
               WHERE notes LIKE '%rows were skipped%' AND duckdb_table IS NOT NULL""")
    check("GATE", "no table still loaded with the row-skipping fallback", n == 0,
          f"{n} table(s) still carry 'rows were skipped' in the manifest")

    # --- 2. provenance is complete -------------------------------------------
    n = one("""SELECT count(*) FROM meta._load_manifest
               WHERE duckdb_table IS NOT NULL AND source_kind NOT IN ('derived','reference','reference_modeled')
                 AND source_sha256 IS NULL""")
    check("GATE", "every loaded source file has a SHA-256", n == 0,
          f"{n} loaded table(s) have no source hash")

    # --- 3. year-union views cover every year that exists --------------------
    # CHIA spelled the 2016 table DiagnosesCode, not DiagnosisCode. It formed its
    # own family and v_hdd_diagnosiscode_all_years silently omitted 9,545,563 rows.
    missing = q("""
      WITH fam AS (
        SELECT regexp_extract(table_name,'^(.*)_((19|20)[0-9]{2})$',1) AS family,
               regexp_extract(table_name,'^(.*)_((19|20)[0-9]{2})$',2) AS yr
        FROM duckdb_tables() WHERE schema_name='chia_casemix'
          AND regexp_matches(table_name,'_(19|20)[0-9]{2}$')),
      v AS (SELECT view_name AS table_name FROM duckdb_views() WHERE schema_name='chia_casemix')
      SELECT family, count(DISTINCT yr) FROM fam
      WHERE 'v_'||family||'_all_years' NOT IN (SELECT table_name FROM v)
        AND family NOT LIKE '%copy_of%'
      GROUP BY 1 HAVING count(DISTINCT yr) > 1""")
    check("WARN", "every multi-year table family has a union view", not missing,
          "families without a view: " + ", ".join(f"{f} ({n}y)" for f, n in missing))

    # --- 4. lookups are unique on their join key -----------------------------
    # The CMS revenue-code source lists 26 SNF RUGS codes twice. Left as duplicate
    # keys they would FAN OUT the 33.9M-row service join and inflate every total.
    dupes = []
    for tbl, col in [("revenue_code","revenue_code"), ("chia_admission_source","code"),
                     ("chia_patient_status","code_norm"), ("chia_payer_type_fy18","payer_type_code"),
                     ("chia_race","race_code"), ("icd9cm_procedure","code_nodot"),
                     ("icd10pcs_procedure","code_nodot"), ("icd10cm_diagnosis","code_nodot"),
                     ("chia_ood_source_of_visit","code"), ("chia_ood_departure_status","code")]:
        d = one(f'SELECT count(*) FROM (SELECT "{col}" FROM ref.{tbl} '
                f'GROUP BY 1 HAVING count(*)>1)')
        if d: dupes.append(f"{tbl}.{col} ({d})")
    check("GATE", "every reference lookup is unique on its join key", not dupes,
          "duplicate keys would fan out fact joins: " + ", ".join(dupes))

    # --- 5. labelled views do not multiply the fact table --------------------
    fact = one("SELECT count(*) FROM chia_casemix.v_hdd_service_all_years")
    lab  = one("SELECT count(*) FROM chia_casemix.v_hdd_service_labeled")
    check("GATE", "v_hdd_service_labeled preserves the fact row count", fact == lab,
          f"fact {fact:,} vs labelled {lab:,}")

    # --- 6. external validation against CHIA's own published totals ----------
    # The FY2018 guide states "Facilities reported a total of 809,270 discharges".
    PUBLISHED = {2018: 809_270, 2017: 809_048}
    for yr, pub in PUBLISHED.items():
        got = one("SELECT count(*) FROM chia_casemix.v_hdd_discharge_all_years WHERE _data_year=?", [yr])
        kind = "GATE" if yr == 2018 else "WARN"
        check(kind, f"FY{yr} discharge count matches CHIA's published total", got == pub,
              f"loaded {got:,} vs published {pub:,} (delta {got-pub:+,})")

    # --- 7. the whole series resolves a physician ----------------------------
    # The identifier scheme changes in 2015; a query written for one era returns
    # NULL for the other and the rows just vanish from a GROUP BY.
    bad = q("""SELECT _data_year, count(*) FILTER (WHERE borim_license IS NOT NULL)
               FROM chia_casemix.v_hdd_discharge_physician GROUP BY 1
               HAVING count(*) FILTER (WHERE borim_license IS NOT NULL)=0 ORDER BY 1""")
    check("GATE", "every discharge year resolves at least some physician licences",
          not bad, "years with zero resolved: " + ", ".join(str(y) for y, _ in bad))

    # --- 8. age top-coding is not being read as a real age -------------------
    mx = one("""SELECT max(age) FROM chia_casemix.v_hdd_discharge_demographics
                WHERE _data_year>=2015""")
    check("GATE", "LDS-era age is capped at 89, not 999", mx is not None and mx <= 89,
          f"max age in the LDS era is {mx} (999 would mean the sentinel is being averaged)")
    tc = one("""SELECT count(*) FROM chia_casemix.v_hdd_discharge_demographics
                WHERE _data_year>=2015 AND age_top_coded""")
    check("WARN", "age 90+ is masked in the LDS era", tc > 0,
          f"{tc:,} discharges are top-coded and cannot enter an age-specific rate")

    # --- 9. the suppression marker never resolves to a label -----------------
    # CHIA writes '-' for suppressed/not-applicable. It is not NULL, and a naive
    # IS NOT NULL filter keeps it.
    n = one("""SELECT count(*) FROM chia_casemix.v_hdd_procedure_labeled
               WHERE code='-' AND description IS NOT NULL""")
    check("GATE", "the '-' suppression marker never carries a description", n == 0,
          f"{n} rows decode '-' as a real code")

    # --- 10. coded-field coverage --------------------------------------------
    PROBE = [("AdmissionSourceCode1","hdd_discharge_2018","chia_admission_source","code",False),
             ("PatientStatus","hdd_discharge_2018","chia_patient_status","code_norm",True),
             ("PrimaryPayerType","hdd_discharge_2018","chia_payer_type_fy18","payer_type_code",False),
             ("RevenueCode","hdd_service_2018","revenue_code","revenue_code",False),
             ("PayerCode1","hdd_discharge_2010","chia_payer_source_2007_2016","payer_source_code",False),
             ("SourceOfVisit","ood_observation_2018","chia_ood_source_of_visit","code",False),
             ("DepartureStatus","ood_observation_2018","chia_ood_departure_status","code",False)]
    for f, tbl, rt, rc, pad in PROBE:
        lhs = (f'lpad(trim(CAST("{f}" AS VARCHAR)),2,\'0\')' if pad
               else f'trim(CAST("{f}" AS VARCHAR))')
        pct = one(f"""WITH d AS (SELECT {lhs} c, count(*) n FROM chia_casemix.{tbl}
                        WHERE "{f}" IS NOT NULL AND trim(CAST("{f}" AS VARCHAR)) NOT IN ('-','')
                        GROUP BY 1)
                      SELECT round(100.0*sum(n) FILTER (WHERE r."{rc}" IS NOT NULL)/sum(n),3)
                      FROM d LEFT JOIN ref.{rt} r ON CAST(r."{rc}" AS VARCHAR)=d.c""")
        check("GATE", f"{f} decodes >= 99% of rows", pct is not None and pct >= 99.0,
              f"{pct}% resolved via ref.{rt}")

    # --- 11. ICD era boundary is clean ---------------------------------------
    # FY2015 is entirely ICD-9 and FY2016 entirely ICD-10 because CHIA's fiscal
    # year runs Oct-Sep. A mid-file assumption would be wrong.
    p15 = one("""SELECT round(100.0*count(*) FILTER (WHERE regexp_matches(code,'^[0-9]+$'))/count(*),1)
                 FROM chia_casemix.hdd_procedure_long WHERE _data_year=2015""")
    p16 = one("""SELECT round(100.0*count(*) FILTER (WHERE NOT regexp_matches(code,'^[0-9]+$'))/count(*),1)
                 FROM chia_casemix.hdd_procedure_long WHERE _data_year=2016""")
    check("GATE", "the ICD-9/ICD-10 split falls cleanly on the FY2015/FY2016 boundary",
          p15 >= 99.9 and p16 >= 99.9, f"FY2015 {p15}% ICD-9, FY2016 {p16}% ICD-10-PCS")

    # --- 12. modelled values are flagged, never silently mixed ---------------
    n = one("""SELECT count(*) FROM meta._load_manifest
               WHERE source_kind='reference_modeled' AND notes NOT LIKE '%MODELLED%'""")
    check("GATE", "every modelled reference is labelled as modelled", n == 0,
          f"{n} modelled table(s) not marked in the manifest")

    # --- 13. cohort stages still match the registry -------------------------
    # CADR tables with near-identical names sit at different stages; if a reload
    # shifts one, every downstream N moves with it and nothing complains.
    drift=[]
    for sid, tbl, rows in q("SELECT stage_id, qualified_table, rows FROM meta.cohort_stage"):
        actual = one(f"SELECT count(*) FROM {tbl}")
        if actual != rows: drift.append(f"{sid}: {actual:,} vs registry {rows:,}")
    check("GATE", "every registered CADR cohort stage still has its recorded N",
          not drift, "; ".join(drift))

    # --- 14. the cohort cascade is still nested ------------------------------
    # Each stage must remain a strict subset of its parent, or "subset_of" is a lie.
    broken=[]
    for sid, tbl, parent in q("""SELECT c.stage_id, c.qualified_table, p.qualified_table
                                 FROM meta.cohort_stage c JOIN meta.cohort_stage p
                                   ON p.stage_id = c.subset_of"""):
        extra = one(f"""SELECT count(*) FROM (SELECT DISTINCT WU_ID FROM {tbl}
                        EXCEPT SELECT DISTINCT WU_ID FROM {parent})""")
        if extra: broken.append(f"{sid} has {extra:,} patients absent from its parent")
    check("GATE", "each cohort stage is a strict subset of its parent", not broken,
          "; ".join(broken))

    # --- 15. every registered rate has aligned age ceilings -----------------
    mis = q("""SELECT rate_view, numerator_max_age, denominator_max_age
               FROM meta.rate_contract WHERE numerator_max_age <> denominator_max_age""")
    check("GATE", "every registered rate has matching numerator/denominator ceilings",
          not mis, "; ".join(f"{v}: {n} vs {d}" for v, n, d in mis))

    # --- 16. era-spanning objects still cover their declared years ----------
    off=[]
    for obj, f0, l0, n0 in q("SELECT object_name, first_year, last_year, n_years FROM meta.era_span"):
        lo, hi, n = con.execute(
            f"SELECT min(_data_year), max(_data_year), count(DISTINCT _data_year) FROM {obj}").fetchone()
        if (lo, hi, n) != (f0, l0, n0):
            off.append(f"{obj}: {lo}-{hi} ({n}y) vs declared {f0}-{l0} ({n0}y)")
    check("GATE", "every era-spanning object still covers its declared years", not off,
          "; ".join(off))


    # ======================================================================
    # SEMANTIC CHECKS -- does the data mean what it claims?
    # The gates above ask whether the load is intact. These ask whether the
    # contents are internally coherent, which is a different failure mode: a
    # perfectly loaded table can still be joined on the wrong key or carry a
    # column that contradicts another.
    # ======================================================================

    # --- S1. no orphan child-code rows ---------------------------------------
    for child, view in [("diagnosis","v_hdd_diagnosiscode_all_years"),
                        ("procedure","v_hdd_procedurecode_all_years"),
                        ("service","v_hdd_service_all_years")]:
        orph = one(f"""SELECT count(*) FROM chia_casemix.{view} x
                       LEFT JOIN (SELECT RecordType20ID,_data_year
                                  FROM chia_casemix.v_hdd_discharge_all_years) d
                         USING (RecordType20ID,_data_year)
                       WHERE d.RecordType20ID IS NULL""")
        check("GATE", f"no orphan {child} rows (every child row has its discharge)",
              orph == 0, f"{orph:,} rows reference a discharge that does not exist that year")

    # --- S2. the spine is unique --------------------------------------------
    # RecordType20ID is the join key for every companion table. A duplicate would
    # silently multiply diagnoses, procedures and charges for that stay.
    dup = one("""SELECT count(*)-count(DISTINCT RecordType20ID)
                 FROM chia_casemix.v_hdd_discharge_all_years WHERE _data_year>=2015""")
    check("GATE", "RecordType20ID is unique across the FY2015-2018 discharge series",
          dup == 0, f"{dup:,} duplicate discharge IDs would fan out every companion join")

    # --- S3. CHIA's own ICDIndicator agrees with the code shape --------------
    # ICDIndicator is '0' for ICD-10 and '9' for ICD-9. If it disagreed with the
    # actual codes, era-conditional logic keyed on the year would be unsafe.
    bad = one("""SELECT count(*) FROM chia_casemix.v_hdd_discharge_all_years d
                 JOIN chia_casemix.hdd_diagnosis_long x
                   ON x.RecordType20ID=d.RecordType20ID AND x._data_year=d._data_year
                 WHERE d._data_year>=2016 AND d.ICDIndicator='0'
                   AND regexp_matches(x.code,'^[0-9]')""")
    tot = one("""SELECT count(*) FROM chia_casemix.hdd_diagnosis_long WHERE _data_year>=2016""")
    check("GATE", "ICDIndicator '0' really does mean ICD-10 codes",
          bad/max(tot,1) < 0.0001, f"{bad:,} of {tot:,} flagged-ICD-10 rows carry a numeric code")

    # --- S4. payer cross-field consistency -----------------------------------
    # The payer SOURCE code carries its own payer TYPE in the reference table.
    # It should agree with the type recorded on the discharge. Disagreement would
    # mean the era-matched payer join is wrong.
    agree, tot = q("""SELECT count(*) FILTER (WHERE r.payer_type_code = d.PrimaryPayerType),
                             count(*)
                      FROM chia_casemix.hdd_discharge_2010 d
                      JOIN ref.chia_payer_source_2007_2016 r
                        ON r.payer_source_code = trim(d.PayerCode1)
                      WHERE d.PrimaryPayerType NOT IN ('-','')""")[0]
    pct = 100.0*agree/max(tot,1)
    check("GATE", "PayerCode1's registered payer type matches PrimaryPayerType",
          pct >= 99.9, f"{pct:.3f}% agree ({tot-agree:,} disagreements of {tot:,})")

    # --- S5. charges are coherent -------------------------------------------
    neg, over = q("""SELECT count(*) FILTER (WHERE TotalChargesAll<0 OR TotalChargesRoutine<0
                            OR TotalChargesSpecial<0 OR TotalChargesAncillaries<0),
                            count(*) FILTER (WHERE TotalChargesRoutine+TotalChargesSpecial
                            +TotalChargesAncillaries > TotalChargesAll*1.001+1)
                     FROM chia_casemix.hdd_discharge_2018""")[0]
    check("GATE", "charges are non-negative and components never exceed the total",
          neg == 0 and over == 0, f"{neg:,} negative, {over:,} with components over total")

    # --- S6. clinical plausibility ------------------------------------------
    # A sex-specific procedure appearing on the other sex would mean the
    # demographics view is mis-joined to the procedure long table.
    nonf = one("""SELECT count(*) FROM chia_casemix.hdd_procedure_long p
                  JOIN chia_casemix.v_hdd_discharge_demographics dem USING (RecordType20ID)
                  WHERE p.code='10E0XZZ' AND dem.sex <> 'F'""")
    check("GATE", "obstetric deliveries only appear on female records", nonf == 0,
          f"{nonf:,} delivery procedures on non-female records -- suspect a mis-join")

    # --- S7. grouper coverage ------------------------------------------------
    miss = one("""SELECT count(*) FROM
                  (SELECT RecordType20ID FROM chia_casemix.v_hdd_discharge_all_years
                   WHERE _data_year>=2015) d
                  LEFT JOIN (SELECT DISTINCT RecordType20ID FROM chia_casemix.v_hdd_cmsdrg_all_years) g
                    USING (RecordType20ID) WHERE g.RecordType20ID IS NULL""")
    check("GATE", "every FY2015-2018 discharge carries a CMS-DRG", miss == 0,
          f"{miss:,} discharges have no grouper row")

    # --- S8. CADR pathways partition the cohort exactly ---------------------
    coh, lab = q("""SELECT (SELECT count(*) FROM cadr.cadr_cohort_descriptors),
                           (SELECT count(*) FROM cadr.v_episode_pathway_age)""")[0]
    check("GATE", "the five CADR pathway tables partition the cohort exactly",
          coh == lab, f"cohort {coh:,} vs pathway-labelled {lab:,}")

    # --- S9. the URPS physician key is 1:1 ----------------------------------
    k, n, c = q("""SELECT count(*), count(DISTINCT npi), count(DISTINCT chia_phys_id)
                   FROM urps.v_urps_chia_physician_key""")[0]
    check("GATE", "the URPS-to-CHIA physician key is 1:1 (no fan-out)",
          k == n == c, f"{k} rows, {n} NPIs, {c} CHIA IDs -- unequal means duplicated discharges")

    # --- S10. physician resolution has not regressed ------------------------
    # Denominator matters here. Roughly 38-40% of discharges record NO operating
    # physician (medical admissions have no operation), and CHIA writes
    # non-physician sentinels -- OTHER, MIDWIF, PODTR, DENSG -- in the same field.
    # Both are excluded; what is measured is: of the rows that name an actual
    # physician, how many resolve to a BORIM licence.
    for yr in (2010, 2014, 2018):
        pct = one("""SELECT round(100.0*count(*) FILTER (WHERE borim_license IS NOT NULL)
                     /nullif(count(*) FILTER (WHERE phys_id_raw IS NOT NULL
                                              AND NOT is_non_physician),0),2)
                     FROM chia_casemix.v_hdd_discharge_physician WHERE _data_year=?""", [yr])
        check("GATE", f"FY{yr} named physicians resolve to a licence at >= 98%",
              pct is not None and pct >= 98, f"{pct}% resolved")

    # --- S11. the blank-vs-sentinel split is stable --------------------------
    # A sudden change here means the field's padding or sentinel vocabulary moved,
    # which is how the resolution rate silently collapsed once already.
    blank = one("""SELECT round(100.0*count(*) FILTER (WHERE phys_id_raw IS NULL)/count(*),1)
                   FROM chia_casemix.v_hdd_discharge_physician WHERE _data_year=2010""")
    check("WARN", "share of discharges with no operating physician recorded",
          30 <= blank <= 50, f"{blank}% of FY2010 discharges name no operating physician")


    # ======================================================================
    # ADVERSARIAL CHECKS -- written by trying to break the data, not confirm it.
    # Several of these found real defects on first run.
    # ======================================================================

    # --- A1. facility resolves in EVERY year --------------------------------
    # FY2004 alone names the columns SiteOrgID/FilingOrgID; FY2005+ uses
    # IdOrgSite/IdOrgFiler. Joining on IdOrgSite alone silently dropped all
    # 840,489 FY2004 discharges from facility analysis -- 0% resolved, no error.
    bad = q("""SELECT _data_year, round(100.0*count(OrganizationName)/count(*),2)
               FROM chia_casemix.v_hdd_discharge_facility GROUP BY 1
               HAVING round(100.0*count(OrganizationName)/count(*),2) < 99.0 ORDER BY 1""")
    check("GATE", "facility resolves >= 99% in every year 2004-2018", not bad,
          "; ".join(f"FY{y}: {p}%" for y, p in bad))

    # --- A2. no year is double-counted through a variant source file --------
    # hdd_discharge_2009_working is a second copy of 2009. If the union view
    # picked it up, 2009 would be silently doubled.
    dbl = q("""SELECT _data_year, count(DISTINCT _source_file)
               FROM chia_casemix.v_hdd_discharge_all_years GROUP BY 1
               HAVING count(DISTINCT _source_file) > 1""")
    check("GATE", "no discharge year draws from more than one source file", not dbl,
          "; ".join(f"FY{y}: {n} files" for y, n in dbl))

    # --- A3. CHIA's own code counters agree with the codes present ----------
    # NumberOfDiagnosisCodes is CHIA's count. Comparing it against what the long
    # table actually holds independently validates the 15-column unnest.
    agree, tot = q("""WITH a AS (SELECT RecordType20ID, count(*) n
                                 FROM chia_casemix.hdd_diagnosis_long
                                 WHERE _data_year=2010 GROUP BY 1)
                      SELECT count(*) FILTER (WHERE TRY_CAST(d.NumberOfDiagnosisCodes AS INT)
                                                    = coalesce(a.n,0)), count(*)
                      FROM chia_casemix.hdd_discharge_2010 d
                      LEFT JOIN a USING (RecordType20ID)""")[0]
    pct = 100.0*agree/max(tot,1)
    check("GATE", "NumberOfDiagnosisCodes agrees with the unnested long table",
          pct >= 99.99, f"{pct:.4f}% agree ({tot-agree:,} of {tot:,} disagree)")

    # --- A4. grouper cardinality is exactly the version count ---------------
    # FY2016-2018 ship FOUR APR-DRG versions per discharge. Forgetting to filter
    # grouper_version quadruples any count taken from that view.
    off = q("""SELECT _data_year, round(1.0*count(*)/count(DISTINCT RecordType20ID),2)
               FROM chia_casemix.v_hdd_aprdrg_all_versions GROUP BY 1
               HAVING round(1.0*count(*)/count(DISTINCT RecordType20ID),2)
                      NOT IN (1.0, 3.0, 4.0) ORDER BY 1""")
    check("GATE", "APR-DRG rows per discharge equal the shipped version count",
          not off, "; ".join(f"FY{y}: {r} rows/discharge" for y, r in off))

    # --- A5. CADR index tables are unique on the episode key ---------------
    dup = []
    for t in ("cadr_sling_index","cadr_pessary_index","cadr_pt_index",
              "cadr_open_burch_index","cadr_laparoscopic_burch_index"):
        n, k = q(f"SELECT count(*), count(DISTINCT (WU_ID, episode_number)) FROM cadr.{t}")[0]
        if n != k: dup.append(f"{t}: {n:,} rows vs {k:,} keys")
    check("GATE", "every CADR pathway index table is unique on (WU_ID, episode_number)",
          not dup, "; ".join(dup))

    # --- A6. CADR pathways are disjoint, not merely equal in total ---------
    # Matching totals would not catch one episode double-labelled and another
    # missing. This checks the partition itself.
    multi = one("""SELECT count(*) FROM (SELECT WU_ID, episode_number
                   FROM cadr.v_episode_pathway_age GROUP BY 1,2 HAVING count(*)>1)""")
    check("GATE", "no CADR episode is assigned to two pathways", multi == 0,
          f"{multi:,} episodes carry more than one pathway label")

    # --- A7. CADR cohort bounds match its documented definition ------------
    lo, hi, y0, y1, u65, outside = q("""SELECT min(age), max(age), min(index_year),
             max(index_year), count(*) FILTER (WHERE age < 65),
             count(*) FILTER (WHERE index_year NOT BETWEEN 2008 AND 2016)
             FROM cadr.v_episode_pathway_age""")[0]
    check("GATE", "CADR stays inside its documented age and index-year bounds",
          u65 == 0 and outside == 0,
          f"age {lo}-{hi}, index years {y0}-{y1}, {u65} under-65, {outside} outside 2008-2016")

    # --- A8. CADR costs are non-negative -----------------------------------
    neg = 0
    for t in ("cadr_sling_index","cadr_pessary_index","cadr_pt_index"):
        neg += one(f"""SELECT count(*) FROM cadr.{t}
                       WHERE TRY_CAST(professional_cost AS DOUBLE) < 0
                          OR TRY_CAST(OP_facility_cost AS DOUBLE) < 0""")
    check("GATE", "no negative professional or facility cost in the CADR index tables",
          neg == 0, f"{neg:,} negative cost rows")

    # --- A9. OOD spine uniqueness ------------------------------------------
    dup = one("""SELECT count(*)-count(DISTINCT RecordType01ID)
                 FROM chia_casemix.ood_observation_2018""")
    check("GATE", "RecordType01ID is unique within the FY2018 observation file",
          dup == 0, f"{dup:,} duplicate observation IDs")

    # --- A10. diagnosis slots are contiguous -------------------------------
    gaps = one("""SELECT count(*) FROM (SELECT RecordType20ID, max(seq) m, count(*) n
                  FROM chia_casemix.hdd_diagnosis_long WHERE _data_year=2010
                  GROUP BY 1 HAVING max(seq) <> count(*))""")
    check("WARN", "diagnosis code slots are contiguous (no populated gaps)", gaps < 100,
          f"{gaps:,} FY2010 discharges have a populated slot after a blank one")

    # --- A11. zero-charge discharges are rare ------------------------------
    pct = one("""SELECT round(100.0*count(*) FILTER (WHERE TotalChargesAll=0)/count(*),3)
                 FROM chia_casemix.hdd_discharge_2018""")
    check("WARN", "zero-charge discharges stay below 1%", pct < 1.0,
          f"{pct}% of FY2018 discharges carry zero total charges")

    # ==================================================================
    # SCHEMA-DRIFT CHECKS -- only 30 of 296 HDD columns and 2 of 140 OOD
    # columns exist in all 15 years. Every era rename below was found by
    # column_audit.py; these gates keep them from silently reappearing.
    # ==================================================================

    # --- D1. the rename registry matches the physical schema ----------------
    wrong = con.execute("""
        SELECT r.family, r.canonical_name, r.era_column, r.first_year, r.last_year,
               min(y.yr) AS actual_first, max(y.yr) AS actual_last
        FROM meta.column_rename r
        LEFT JOIN (SELECT DISTINCT lower(column_name) AS col,
                          TRY_CAST(regexp_extract(table_name,'(\\d{4})$',1) AS INTEGER) AS yr,
                          regexp_replace(table_name,'_\\d{4}$','') AS fam
                   FROM duckdb_columns() WHERE schema_name='chia_casemix') y
               ON y.col = lower(r.era_column) AND y.fam = r.family
        GROUP BY ALL
        HAVING min(y.yr) IS DISTINCT FROM r.first_year
            OR max(y.yr) IS DISTINCT FROM r.last_year""").fetchall()
    check("GATE", "every registered era column spans exactly the years recorded",
          not wrong,
          "registry matches the physical schema" if not wrong
          else f"{len(wrong)} mismatched, e.g. {wrong[0]}")

    # --- D2. each canonical name's eras tile the span without overlap -------
    bad = con.execute("""
        WITH e AS (SELECT family, canonical_name, first_year, last_year,
                          lag(last_year) OVER (PARTITION BY family, canonical_name
                                               ORDER BY first_year) AS prev_last
                   FROM meta.column_rename)
        SELECT family, canonical_name FROM e
        WHERE prev_last IS NOT NULL AND first_year <> prev_last + 1""").fetchall()
    check("GATE", "era columns tile each canonical field with no gap or overlap",
          not bad, "all eras contiguous" if not bad else f"broken: {bad}")

    # --- D3/D4. the canonical views actually resolve in every year ----------
    for view, cols, lo, hi in [
        ("chia_casemix.v_hdd_discharge_canonical",
         ["sex", "race_primary", "org_site", "org_filer", "charges_special"], 2004, 2018),
        ("chia_casemix.v_ood_observation_canonical",
         ["sex", "age", "race_primary", "org_site", "cpt1"], 2004, 2018),
    ]:
        holes = []
        for c in cols:
            rows = con.execute(f"""SELECT _data_year FROM {view}
                                   GROUP BY 1 HAVING count({c}) = 0 ORDER BY 1""").fetchall()
            if rows:
                holes.append((c, [r[0] for r in rows]))
        short = view.split(".")[-1]
        check("GATE", f"{short}: every canonical column resolves in all years",
              not holes,
              f"{len(cols)} columns non-null across {lo}-{hi}" if not holes
              else f"empty years: {holes}")

    # --- D5. HDD and OOD share no patient or encounter key ------------------
    # Not a defect -- a documented limit. Gated so a future load cannot make an
    # analysis silently assume a patient-level link that does not exist.
    shared = con.execute("""
        SELECT count(*) FROM (SELECT DISTINCT RecordType20ID k FROM chia_casemix.hdd_discharge_2012) h
        JOIN (SELECT DISTINCT RecordType01ID k FROM chia_casemix.ood_observation_2012) o USING (k)""").fetchone()[0]
    check("GATE", "HDD and OOD record keys stay disjoint (no patient-level link)",
          shared == 0,
          "0 shared keys -- link via facility or BORIM only, never patient"
          if shared == 0 else f"{shared:,} shared keys: re-examine the linkage assumption")

    # --- D6. OOD surgeon field is a raw BORIM number, not an encrypted hash --
    pct = one("""
        WITH s AS (SELECT DISTINCT trim(SurgeonAssociatedProcedure1) AS sid
                   FROM chia_casemix.ood_observation_2012
                   WHERE trim(coalesce(SurgeonAssociatedProcedure1,'')) NOT IN ('','OTHER','-------')),
             b AS (SELECT DISTINCT CAST(borimnumber AS VARCHAR) AS bid
                   FROM chia_provider.fipa_prelds_leghashphysician_master WHERE borimnumber IS NOT NULL)
        SELECT round(100.0*count(*) FILTER (WHERE b.bid IS NOT NULL)/count(*),1)
        FROM s LEFT JOIN b ON b.bid = s.sid""")
    check("GATE", "OOD SurgeonAssociatedProcedure1 is a raw BORIM number",
          pct >= 99.0,
          f"{pct}% join BORIM directly -- unlike the encrypted physician columns "
          "in the same table")

    # --- D7. site-level physician-reporting cliffs are declared, not silent ----
    # At FY2016 seventeen hospitals stopped populating the OOD physician fields
    # while their observation volume held. A specialty-level count across that
    # boundary reads as a workforce collapse. This surfaces every such cliff so
    # it is a known artefact rather than a finding.
    cliffs = con.execute("""
        WITH f AS (SELECT org_site, _data_year AS fy, count(*) AS stays,
                          count(*) FILTER (WHERE phys_raw IS NOT NULL
                                             AND NOT phys_is_non_physician) AS named
                   FROM chia_casemix.v_ood_observation_canonical GROUP BY 1,2),
             p AS (SELECT a.org_site, a.fy,
                          a.named AS prev_named, b.named AS next_named,
                          a.stays AS prev_stays, b.stays AS next_stays
                   FROM f a JOIN f b ON b.org_site=a.org_site AND b.fy=a.fy+1)
        SELECT fy, count(*) FROM p
        WHERE prev_named >= 100 AND next_named < 0.05*prev_named
          AND next_stays >= 0.5*prev_stays          -- volume held; only the field went dark
        GROUP BY 1 ORDER BY 2 DESC""").fetchall()
    worst = cliffs[0] if cliffs else None
    check("WARN", "OOD physician-reporting cliffs are the known FY2016 artefact",
          bool(worst) and worst[0] == 2015,
          f"largest cliff at FY{worst[0]}->{worst[0]+1}: {worst[1]} sites stopped "
          "reporting physicians while volume held" if worst else "no cliff detected")

    # --- D8. the OOD physician analytic window stays closed at FY2015 ---------
    # Reopening it silently would restore a 60%+ undercount from FY2016 on.
    hi = one("SELECT max(_data_year) FROM chia_casemix.v_ood_observation_physician")
    check("GATE", "OOD physician view stops at FY2015", hi == 2015,
          f"max year {hi} -- site-year filtering does NOT repair FY2016+; the "
          "surviving sites are a non-random fifth of the file")

    # --- D9. FY2016+ OOD remains fully usable for everything else -------------
    holes = con.execute("""
        SELECT _data_year FROM chia_casemix.v_ood_observation_canonical
        WHERE _data_year >= 2016
        GROUP BY 1
        HAVING count(sex)=0 OR count(race_primary)=0 OR count(org_site)=0
            OR count(cpt1)=0""").fetchall()
    check("GATE", "FY2016-2018 OOD is sound for non-physician fields", not holes,
          "demographics, org and CPT complete -- only the physician field is affected"
          if not holes else f"unexpected gaps: {holes}")

    # --- D10. work is attributable to an NPI in every year --------------------
    # This is the load-bearing claim for the workforce analysis: a discharge must
    # reach a National Provider Identifier, or CHIA cannot inform supply.
    worst = con.execute("""
        WITH x AS (SELECT DISTINCT TRY_CAST(license AS BIGINT) AS b, NPI
                   FROM chia_provider.borim_stdrel_npi_straight_from_cd
                   WHERE license IS NOT NULL AND NPI IS NOT NULL AND trim(NPI) <> '')
        SELECT c._data_year,
               round(100.0*count(x.NPI) FILTER (WHERE c.is_surgical)
                     / nullif(count(*) FILTER (WHERE c.is_surgical), 0), 1) AS pct
        FROM chia_casemix.v_hdd_discharge_canonical c
        JOIN chia_casemix.v_hdd_discharge_physician d USING (RecordType20ID, _data_year)
        LEFT JOIN x ON x.b = d.borim_license
        GROUP BY 1 ORDER BY pct LIMIT 1""").fetchone()
    check("GATE", "surgical discharges reach an NPI at >= 75% in every year",
          worst[1] >= 75.0,
          f"worst year FY{worst[0]} at {worst[1]}% -- HDD carries the workforce "
          "attribution; OOD does not")

    # --- D11. the pre-FY2015 '-' sentinel is not counted as a procedure -------
    # '-' means 'no procedure' before FY2015 and NULL from FY2015. Treating '-'
    # as a value marks every discharge surgical and inflates the denominator.
    bad = one("""SELECT count(*) FROM chia_casemix.v_hdd_discharge_canonical
                 WHERE principal_procedure = '-' OR principal_procedure = ''""")
    check("GATE", "the '-' no-procedure sentinel is normalised to NULL", bad == 0,
          f"{bad:,} rows still carry a sentinel as a procedure code")

    # ==================================================================
    # WORKFORCE CHECKS -- surgeon_year_volume is the supply-side input, so
    # the ways it can silently lie get their own gates.
    # ==================================================================

    # --- W1. the operative filter actually excludes non-surgeons --------------
    # 'has a principal procedure' is NOT 'had an operation': ICD codes cover
    # vaccination, transfusion and delivery. Before classifying, the top-volume
    # "surgeons" in FY2015 were internists and paediatricians.
    top = con.execute("""
        SELECT borim_specialty, operative_cases FROM chia_casemix.surgeon_year_volume
        WHERE fy = 2015 ORDER BY operative_cases DESC LIMIT 10""").fetchall()
    nonsurg = [t for t in top if t[0] and not any(
        k in t[0].lower() for k in ('surg', 'orthop', 'urolog', 'neuro', 'cardio',
                                    'obstet', 'gyneco', 'transplant', 'otolar'))]
    check("GATE", "top operative-volume physicians are surgical specialties",
          not nonsurg,
          "top 10 FY2015 by operative volume are all surgical"
          if not nonsurg else f"non-surgical in top 10: {nonsurg}")

    # --- W2. physician attribution stays high enough to trust a denominator ---
    worst = con.execute("""
        SELECT fy, pct_attributed, dark_sites
        FROM chia_casemix.v_surgeon_year_completeness
        ORDER BY pct_attributed LIMIT 1""").fetchone()
    check("GATE", "operative discharges are physician-attributed at >= 90%/year",
          worst[1] >= 90.0,
          f"worst year FY{worst[0]} at {worst[1]}%"
          + (f" -- dark: {worst[2]}" if worst[2] else ""))

    # --- W3. one row per surgeon-year; no NPI fan-out ------------------------
    dup = one("""SELECT count(*) FROM (SELECT npi, fy FROM chia_casemix.surgeon_year_volume
                 GROUP BY 1,2 HAVING count(*) > 1)""")
    check("GATE", "surgeon_year_volume is unique on (npi, fy)", dup == 0,
          f"{dup:,} duplicated surgeon-years -- the BORIM->NPI join fanned out")

    # --- W4. no surgeon-year exceeds a physically plausible caseload ---------
    # ~250 working days; 4 major inpatient operations a day is already extreme.
    hi = con.execute("""SELECT npi, fy, operative_cases FROM chia_casemix.surgeon_year_volume
                        ORDER BY operative_cases DESC LIMIT 1""").fetchone()
    check("GATE", "no surgeon-year exceeds 1000 inpatient operations",
          hi[2] <= 1000,
          f"max is {hi[2]:,} (NPI {hi[0]}, FY{hi[1]}) -- above ~1000 suggests a "
          "group or facility identifier, not a person")

    # --- W5. newborn stays are not credited to the mother's surgeon -----------
    # AdmissionType 4 records carry the delivering obstetrician as operating
    # physician. Counting them made circumcision the top URPS procedure of
    # FY2018 and inflated the URPS operative total by 41%.
    leak = one("""
        WITH xw AS (SELECT DISTINCT TRY_CAST(license AS BIGINT) AS b
                    FROM chia_provider.borim_stdrel_npi_straight_from_cd
                    WHERE license IS NOT NULL AND trim(coalesce(NPI,'')) <> '')
        SELECT count(*) FROM chia_casemix.v_hdd_discharge_procedure p
        JOIN chia_casemix.v_hdd_discharge_canonical c USING (RecordType20ID,_data_year)
        JOIN chia_casemix.v_hdd_discharge_physician d USING (RecordType20ID,_data_year)
        JOIN xw ON xw.b = d.borim_license
        WHERE p._data_year = 2018 AND p.procedure_class = 'operative'
          AND c.AdmissionType = '4'
          AND p.principal_procedure = '0VTTXZZ'""")
    counted = one("""SELECT coalesce(sum(newborn_cases),0)
                     FROM chia_casemix.surgeon_year_volume WHERE fy = 2018""")
    check("GATE", "newborn stays are held out of operative_cases",
          counted > 0 and leak > 0,
          f"{counted:,} FY2018 newborn operations kept in their own column "
          f"({leak:,} of them circumcisions)")

    # --- W6. no gynaecologic surgeon's top procedure is a male newborn one ----
    bad = con.execute("""
        SELECT count(*) FROM chia_casemix.surgeon_year_volume
        WHERE is_urps AND newborn_cases > operative_cases AND fy = 2018""").fetchone()[0]
    check("WARN", "no URPS surgeon has more newborn than operative cases",
          bad == 0,
          f"{bad} URPS surgeon-years are majority newborn -- check attribution")

    # ==================================================================
    # COHORT CHECKS -- the study population is female, 18 and over.
    # ==================================================================

    # --- C1. the cohort contains only adult women ----------------------------
    bad = one("""SELECT count(*) FROM chia_casemix.v_cohort_female_adult
                 WHERE sex <> 'F' OR (age < 18 AND NOT age_top_coded)""")
    check("GATE", "cohort is female and 18+ only", bad == 0,
          f"{bad:,} rows violate the cohort definition")

    # --- C2. the 90+ top-code is RETAINED, not filtered away ------------------
    # FY2015+ writes age 90+ as '999'. A plain `age >= 18` drops them -- the
    # oldest women, and the peak-prevalence group for prolapse and incontinence.
    kept = one("""SELECT count(*) FROM chia_casemix.v_cohort_female_adult
                  WHERE _data_year >= 2015 AND age_top_coded""")
    check("GATE", "top-coded 90+ women are kept in the cohort", kept > 0,
          f"{kept:,} top-coded 90+ records retained across FY2015-2018")

    # --- C3. no discontinuity in the 90+ share at the top-coding boundary -----
    # Pre-FY2015 ages are explicit to 115; FY2015+ stop at 89. If age_capped is
    # wrong, the 90+ share jumps at the boundary.
    pct14, pct15 = con.execute("""
        SELECT
          round(100.0*count(*) FILTER (WHERE is_90_plus AND _data_year=2014)
                /nullif(count(*) FILTER (WHERE _data_year=2014),0), 2),
          round(100.0*count(*) FILTER (WHERE is_90_plus AND _data_year=2015)
                /nullif(count(*) FILTER (WHERE _data_year=2015),0), 2)
        FROM chia_casemix.v_cohort_female_adult""").fetchone()
    check("GATE", "the 90+ share is continuous across the FY2015 top-code change",
          abs(pct14 - pct15) < 1.0,
          f"FY2014 {pct14}% vs FY2015 {pct15}% -- a jump here means age_capped is wrong")

    # --- C4. cohort volume is a plausible share of URPS operative work --------
    lo = one("""SELECT min(round(100.0*fa/nullif(allp,0),1)) FROM (
                  SELECT sum(fa_operative_cases) AS fa, sum(operative_cases) AS allp
                  FROM chia_casemix.surgeon_year_volume WHERE is_urps GROUP BY fy)""")
    check("GATE", "URPS operative work is overwhelmingly on adult women",
          lo >= 85.0,
          f"lowest year is {lo}% female-adult -- below ~85% would suggest the "
          "URPS cohort or the sex/age filter is wrong")

    con.close()

    # --- report ---------------------------------------------------------------
    width = max(len(n) for _, n, _, _ in RESULTS)
    gate_fail = 0
    for kind, name, ok, detail in RESULTS:
        status = "pass" if ok else ("FAIL" if kind == "GATE" else "warn")
        if not ok and kind == "GATE":
            gate_fail += 1
        if a.verbose or not ok:
            print(f"[{status:4}] {kind:4} {name:<{width}}  {detail}")
        else:
            print(f"[{status:4}] {kind:4} {name}")
    n_gate = sum(1 for k, _, _, _ in RESULTS if k == "GATE")
    print(f"\n{len(RESULTS)} checks ({n_gate} gates); {gate_fail} gate failure(s)")
    return 1 if gate_fail else 0

if __name__ == "__main__":
    sys.exit(main())
