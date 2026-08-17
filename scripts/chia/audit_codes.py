#!/usr/bin/env python3
"""
Coverage audit for every coded CHIA field that has a reference table.

Answers one question: what share of rows carries a decodable label, and exactly
which values do not. Re-run after adding any dictionary; anything still listed
is a genuine acquisition target, not an oversight.

Usage: audit_codes.py --db PATH
"""
import argparse, duckdb

# field, source table, ref table, ref column, zero-pad the field to 2 chars
# Inpatient (HIDD) and Outpatient Observation (OOD) are separate databases with
# separate documentation; OOD uses SourceOfVisit/DepartureStatus where HIDD uses
# AdmissionSourceCode/PatientStatus, so each gets its own reference table.
PROBE = [
 ("AdmissionSourceCode1","hdd_discharge_2018","chia_admission_source","code",False),
 ("AdmissionSourceCode2","hdd_discharge_2018","chia_admission_source","code",False),
 ("PrimaryConditionPresent","hdd_discharge_2018","chia_condition_present","code",False),
 ("PatientStatus","hdd_discharge_2018","chia_patient_status","code_norm",True),
 ("PrimaryPayerType","hdd_discharge_2018","chia_payer_type_fy18","payer_type_code",False),
 ("SecondaryPayerType","hdd_discharge_2018","chia_payer_type_fy18","payer_type_code",False),
 ("AdmissionType","hdd_discharge_2018","ub04_admission_type","code",False),
 ("Race1","hdd_discharge_2018","chia_race","race_code",False),
 ("Race2","hdd_discharge_2018","chia_race","race_code",False),
 ("PayerCode1","hdd_discharge_2010","chia_payer_source_2007_2016","payer_source_code",False),
 ("PayerCode2","hdd_discharge_2010","chia_payer_source_2007_2016","payer_source_code",False),
 ("Ethnicity1","hdd_discharge_2018","chia_ethnicity","code",False),
 ("Ethnicity2","hdd_discharge_2018","chia_ethnicity","code",False),
 ("RevenueCode","hdd_service_2018","revenue_code","revenue_code",False),
 # --- Outpatient Observation Database ---
 ("SourceOfVisit","ood_observation_2018","chia_ood_source_of_visit","code",False),
 ("SecondarySourceOfVisit","ood_observation_2018","chia_ood_source_of_visit","code",False),
 ("DepartureStatus","ood_observation_2018","chia_ood_departure_status","code",False),
 ("PrincipalConditionPresent","ood_observation_2018","chia_ood_condition_present","code",False),
 ("PrimarySourceOfPayment","ood_observation_2018","chia_payer_source_fy18_fy20","payer_source_code",False),
 ("SecondarySourceOfPayment","ood_observation_2018","chia_payer_source_fy18_fy20","payer_source_code",False),
 ("Ethnicity1","ood_observation_2018","chia_ethnicity","code",False),
 ("Race1","ood_observation_2018","chia_race","race_code",False),
 ("ED_Flag","ood_observation_2018","chia_ood_ed_flag","code",False),
]

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--db", required=True)
    con = duckdb.connect(ap.parse_args().db, read_only=True)
    print(f"{'field':26}{'reference':26}{'codes':>6}{'unres':>7}{'% rows':>9}  unresolved")
    unresolved = 0
    for f, tbl, rt, rc, pad in PROBE:
        lhs = (f'lpad(trim(CAST("{f}" AS VARCHAR)),2,\'0\')' if pad
               else f'trim(CAST("{f}" AS VARCHAR))')
        nc, bad, pct, vals = con.execute(f"""
            WITH d AS (SELECT {lhs} c, count(*) n FROM chia_casemix.{tbl}
                       WHERE "{f}" IS NOT NULL
                         AND trim(CAST("{f}" AS VARCHAR)) NOT IN ('-','') GROUP BY 1)
            SELECT count(*), count(*) FILTER (WHERE r."{rc}" IS NULL),
                   round(100.0*sum(n) FILTER (WHERE r."{rc}" IS NOT NULL)/sum(n),3),
                   string_agg(CASE WHEN r."{rc}" IS NULL THEN d.c||' ('||d.n||')' END, ', ')
            FROM d LEFT JOIN ref.{rt} r ON CAST(r."{rc}" AS VARCHAR) = d.c""").fetchone()
        unresolved += bad
        print(f"{f:26}{rt:26}{nc:>6}{bad:>7}{pct:>9}  {(vals or '')[:60]}")
    print(f"\n{len(PROBE)} fields audited; {unresolved} unresolved values remain")
    con.close()

if __name__ == "__main__":
    main()
