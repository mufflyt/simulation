# Free external evidence for latent adequacy

This module ingests sources 6, 7, 8, 9, 10, 11, 12, 14, 16, 20, 21, and 25
from the adequacy evidence plan. It does not treat an unavailable source as a
zero. Every source is recorded as `ingested`, `reused`, or `missing` in
`adequacy_source_ingest_audit`.

## Sources and valid roles

| No. | Source | Model contribution | Acquisition |
|---:|---|---|---|
| 6 | Medicaid MCPAR | Plan access and managed-care pressure | CMS DCAT |
| 7 | Medicaid NAAAR | Network time, distance, and availability | CMS DCAT |
| 8 | State Medicaid fee schedules | CPT-specific fee ratio | Manifest |
| 9 | Medicaid enrollment | Population exposed to payer barriers | CMS DCAT |
| 10 | CMS Provider of Services | Facility availability | CMS DCAT |
| 11 | CMS hospital owners | Consolidation and ownership | CMS DCAT |
| 12 | HCRIS | Hospital capacity and financial viability | Manifest |
| 14 | NHIS | Delayed and forgone care | Manifest |
| 16 | BRFSS | Cost barrier and population risk | Existing RDS |
| 20 | ACS PUMS | Insurance and transportation vulnerability | Existing RDS |
| 21 | Census timely access survey | Contemporary care delays | Manifest |
| 25 | State licensing boards | Active-license corroboration | Manifest |

`data_json` sources are resolved from the official CMS or Medicaid DCAT
catalog at run time. State fee schedules and licensing rosters are
manifest-driven because there is no authoritative national bulk endpoint and
state file formats change. HCRIS and survey releases are also manifest-driven
so the analyst must identify the intended year and component rather than let a
script select an arbitrary file from a multi-file archive.

## Manifest contract

Create `data-raw/adequacy_sources/source_manifest.csv` with:

```csv
source_id,local_path
medicaid_fees,/absolute/path/medicaid_fee_ratios.csv
cms_hcris,/absolute/path/hcris_hospital_year.csv
nhis_access,/absolute/path/nhis_access.csv
census_pulse_access,/absolute/path/timely_access.csv
state_license,/absolute/path/state_license_roster.csv
```

BRFSS and ACS PUMS are added automatically when these existing artifacts are
present:

- `data-raw/brfss/brfss_2024_women18plus.rds`
- `data-raw/acs/acs5_2023_pums_women18plus.rds`

## DuckDB load

From R:

```r
base::source(
  base::file.path(
    "scripts",
    "data_acquisition",
    "13_load_adequacy_sources_duckdb.R"
  )
)

loading <- run_adequacy_source_load(
  strict = FALSE
)
```

Set `strict = TRUE` only for a frozen production build in which all selected
sources are expected to exist. Exploratory runs retain unavailable sources as
missing values and show them in the audit.

## Standardizing heterogeneous columns

Raw tables retain source-native column names. A reviewed feature specification
maps those columns into comparable geographic measures:

```r
feature_spec <- tibble::tribble(
  ~table_name, ~geography_col, ~value_col, ~feature_name,
  ~aggregation, ~weight_col,
  "raw_medicaid_fees", "state_fips", "fee_ratio",
  "medicaid_fee_ratio", "weighted_mean", "service_count",
  "raw_brfss_access", "X_STATE", "cost_barrier",
  "brfss_cost_barrier", "weighted_mean", "X_LLCPWT"
)

feature_tbl <- build_adequacy_geographic_features(
  db_path = loading$db_path,
  feature_spec = feature_spec
)
```

The mapping is deliberately explicit. Automatic fuzzy matching of columns can
silently substitute enrollment-processing wait time for clinical appointment
wait time or hospital mailing state for patient residence state.

## Joining the adequacy model

```r
calibration_augmented <- augment_adequacy_from_duckdb(
  calibration_tbl = calibration_tbl,
  db_path = loading$db_path
)
```

`external_evidence_n` counts nonmissing external indicators for each geography.
It is a measurement-coverage field, not an adequacy score. Model code must
retain the missingness masks and must not replace missing evidence with zero or
a national median.

## Interpretation boundary

Claims, BRFSS, NHIS, and ACS measure realized care, barriers, or population
risk. They do not measure unused urogynecology capacity. The URPS practice
survey remains necessary for new-patient capacity, panels, operative access,
and ability to absorb additional referrals.
