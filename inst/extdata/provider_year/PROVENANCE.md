# Provider-year activity measurement — source provenance

Built by `data-raw/provider_year_activity/build_provider_year_activity.R` and
`build_comparison_and_casemix.R`. Nothing was downloaded for this work; every
source below was already present locally.

## Sources actually used

| # | Source | Local path | Coverage | Granularity | Role |
|---|--------|-----------|----------|-------------|------|
| 1 | URPS roster (ABOG + ABU) | `data-raw/urps_roster/urps_roster_2026-07-22.csv` | 1,100 rows / 1,092 unique NPI | provider | D0 denominator; cert_year, pathway, sex |
| 2 | Medicare Part B by Provider & Service (CMS PUF) | `/Volumes/MufflySamsung 1 1/DuckDB/nber_my_duckdb.duckdb` → `main.medicare_part_b_by_service_{2013..2024}` | 2013–2024, 9.3–10.1M rows/yr | provider × HCPCS × place-of-service × year | D1, D2; case mix |
| 3 | CMS Open Payments, general payments | `/Volumes/MufflySamsung 1 1/open_payments_data/{2015..2023}/unzipped_files/OP_DTL_GNRL_*.csv` | 2015–2023 (~62 GB) | payment record × year, NPI-keyed | second independent source for D3, D4 |
| 4 | URPS certification contract series | `mufflyaccess::urps_counts_long()` v3.0.0 | 2013–2023 national | year | back-test target scaling |

## Sources inventoried and DELIBERATELY NOT USED, with reason

| Source | Local path | Why excluded |
|--------|-----------|--------------|
| `credentials.temporal_nppes_{2013..2024}_fixed` | same DuckDB | **Circular.** Its `activity_validated` flag has `activity_source = "Medicare Part B 2023"` — it is a re-derivation of source 2, so counting it as a second source would count Part B twice. Also covers only ~436/1,092 roster NPIs. |
| `DAC_NationalDownloadableFile.csv` (CMS Doctors & Clinicians) | `/Volumes/MufflySamsung 1 1/nppes_historical_downloads/` | Single undated snapshot (2.6M rows). Carries no program year, so it cannot produce a per-year activity signal. Usable only as a present-day directory check. |
| `credentials.ppef_enrollment_status` (PECOS) | same DuckDB | Interval-censored: first/last enrollment year only. Marking every intervening year "active" is interpolation, not observation. Enrollment also ≠ practising. |
| `credentials.open_payments_activity` | same DuckDB | Rollup (first/last/count) of source 3. The raw per-year files were used instead so per-year presence is observed, not inferred. |
| Open Payments 2013–2014 | `/Volumes/MufflySamsung 1 1/open_payments_data/{2013,2014}/` | CMS published no NPI for those program years (`Physician_Profile_ID` only). Absence is unobserved, not negative — so **D4 is not estimable for 2013–2014** and D3 is a lower bound there. |
| `credentials.abog_providers.clinicallyActive` | same DuckDB | Undated status flag with no year dimension; cannot support a provider-year claim. |
| State licensure | — | Not present locally. |
| Mystery-caller confirmation | — | Not present locally in provider-year form. |

## Known limitations that no definition here removes

1. **Medicare-only.** Every activity signal from source 2 is Medicare fee-for-service. Care to commercial, Medicaid, self-pay, VA/DoD and Medicare Advantage patients is invisible. This is age-selective: a urogynecologist whose practice skews younger looks less active regardless of true workload.
2. **The roster is 83.6% of the 2023 certified population** (1,092 of 1,306). All definitions are computed on the roster and rescaled; representativeness is assumed, not demonstrated.
3. **`age_proxy_from_cert` is exactly `2060 − cert_year`.** The roster carries no independent age. Any "age distribution" is a relabelled certification-cohort distribution and is reported as such.
4. **Sex is missing for 12 of 1,092** roster members.
5. **Drug units are not services.** `HCPCS_Drug_Ind == "Y"` lines report units (J0585 onabotulinumtoxinA is per-unit, ~100–200 per injection). In 2023 they were 58% of raw `Tot_Srvcs` from 0.3% of lines. They are excluded from every service count and reported separately in `provider_year_drug_units.csv`.
6. **CMS suppresses provider-HCPCS cells with <11 beneficiaries**, so low-volume services are systematically absent rather than zero.

## Concepts kept separate (never interconverted here)

`headcount` (persons) · `Medicare billing volume` (services, Medicare only) ·
`total clinical productivity` (**not measured** — would require all-payer volume) ·
`FTE` (**not derived from claims anywhere in these artifacts**).
No claims count is converted to FTE. Doing so needs an explicit calibration model
relating Medicare volume to total clinical time, which does not exist in this repo.
