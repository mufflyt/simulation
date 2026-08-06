# HCUP NASS — base-year URPS procedure anchors

The **Nationwide Ambulatory Surgery Sample (NASS)** is the right frame for
base-year calibration because most URPS procedures are **outpatient** (NIS is
inpatient/ICD-10-PCS and undercounts slings — see
`config/calibration_targets.yml`). This produces the `sling_procedure_volume` and
`prolapse_procedure_volume` anchors the demand model calibrates against
(`R/calibration-demand_lifecourse.R`).

## Acquire (licensed — no free API)

NASS comes from the **HCUP Central Distributor** after the online HCUP Data Use
Agreement training: <https://hcup-us.ahrq.gov/tech_assist/centdist.jsp>. Read the
raw fixed-width ASCII with the HCUP-supplied load program (SAS/SPSS/Stata) or
file specifications, then export the core file to CSV.

**Free aggregate fallback:** if you only need national totals (not microdata),
HCUP Fast Stats / HCUPnet publish weighted procedure counts you can enter by hand
— <https://datatools.ahrq.gov/hcup-fast-stats> — and Medicare Part B (already in
the repo) anchors the 65+ share.

## Ingest → anchors

`scripts/data_acquisition/10_ingest_hcup_nass.R` filters the core file to the
URPS CPT sets (SUI sling 57288/…; POP repair 57240/57250/57260/57282/57283/
57425/…), weights by `DISCWT` for national estimates, and writes:

```
data-raw/hcup/nass_<year>_urps_anchors.csv   # category, observed, source, year
data/anchors/sling_volume.csv                # matches calibration_targets.yml
data/anchors/prolapse_volume.csv
```

```r
Sys.setenv(NASS_CORE_PATH = "/path/to/nass_core.csv")
source("scripts/data_acquisition/10_ingest_hcup_nass.R")
obs <- readr::read_csv("data-raw/hcup/nass_2021_urps_anchors.csv")
calibrate_lifecourse_demand(fte$service_volumes, obs[, c("category", "observed")])
```

CPT sets are editable at the top of the script — **verify against your NASS
year's coding**. After ingest, refresh the `sha256` fields for the split files in
`config/calibration_targets.yml`.

Once the anchors are in `data/anchors/`, run the whole calibration + back-test in
one command:

```r
Sys.setenv(DEMAND_BASE_YEAR = "2021")   # a year your anchors cover
source("scripts/run_demand_calibration_backtest.R")
# -> artifacts/demand_calibration_scalars.csv + demand_backtest_*.csv
```

It prints the `observed / predicted` scalars and the held-out-year MAPE, and
stamps every output with whether real anchors or the illustrative fallback were
used. For a real back-test, give the anchor CSVs a `year` column with the
`fit_through_year` and `target_year` from `config/calibration_targets.yml`.

## Build note

`data-raw/` is `.Rbuildignore`d; the licensed NASS microdata is **not** committed
— only this README and the derived anchor CSVs' local manifest.
