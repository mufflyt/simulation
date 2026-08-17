# URPS Simulation — Data Download Guide

All external data sources for the URPS workforce microsimulation demand arm.
Run scripts in order. Each is idempotent (re-running skips already-downloaded files).

---

## Quick Start

```bash
cd /Users/tmuffly/simulation   # or wherever the package root is

# 1. BRFSS 2023 (~2 min)
Rscript scripts/data_acquisition/01_download_brfss.R

# 2. ACS 2023 (~15–30 min; requires Census API key)
Rscript scripts/data_acquisition/02_download_acs.R

# 3. MCBS 2022 (~5 min)
Rscript scripts/data_acquisition/03_download_mcbs.R

# 4. NHAMCS/NAMCS ambulatory care (~5 min)
Rscript scripts/data_acquisition/04_download_nhamcs_namcs.R

# 5. CMS PSPS (manual browser download — see section 5 below)
```

Census NPP (population projections) is already bundled in `data-raw/census/`.
CMS PSPS requires a manual browser step (JavaScript redirect; cannot be automated).

---

## Prerequisites

### R packages

```r
install.packages(c("haven", "dplyr", "readr", "purrr", "tidycensus", "here"))
```

### Census API key (ACS only)

Register for a free key at <https://api.census.gov/data/key_signup.html>.
Once you have it:

```r
tidycensus::census_api_key("YOUR_KEY_HERE", install = TRUE)
# installs to ~/.Renviron so it persists across sessions
```

Or set it per-session:

```r
Sys.setenv(CENSUS_API_KEY = "YOUR_KEY_HERE")
```

The key already installed on this machine is in `~/.Renviron` as
`CENSUS_API_KEY=<redacted: the key that was here has been REVOKED>`.

---

## 1. BRFSS 2023 — Behavioral Risk Factor Surveillance System

| Attribute | Value |
|-----------|-------|
| Script | `scripts/data_acquisition/01_download_brfss.R` |
| DUA required | **No** |
| Download size | ~64 MB compressed (ZIP), ~200 MB uncompressed (XPT) |
| Output size | ~9 MB (women 18+ RDS) |
| Time | ~2–3 min |

**What it provides:**  
229,541 women 18+ with age, race/ethnicity, insurance status, income, BMI,
smoking, and (in states that opted in) bladder/bowel control variables
(`BLADCON`, `URINCON`). This is the primary D4 demand prevalence source.

**Source URL:**
```
https://www.cdc.gov/brfss/annual_data/2023/files/LLCP2023XPT.zip
```

**Output files:**
```
data-raw/brfss/LLCP2023XPT.zip
data-raw/brfss/LLCP2023.XPT
data-raw/brfss/brfss_2023_women18plus.rds   ← use this in R
data-raw/brfss/brfss_2023_manifest.txt
```

**Key variables in the RDS:**

| Variable | Description |
|----------|-------------|
| `X_LLCPWT` | Final survey weight (use for all estimates) |
| `X_PSU` | Primary sampling unit |
| `X_STSTR` | Stratum |
| `X_STATE` | State FIPS code |
| `X_SEX` | Sex (2 = female) |
| `X_AGEG5YR` | Age in 5-year groups (1 = 18–24, …, 13 = 80+) |
| `X_PRACE1` | Race/ethnicity (computed) |
| `X_INCOMG1` | Income group |
| `BLADCON` | Bladder/bowel control problem (optional module — not all states) |
| `URINCON` | Urinary incontinence (optional module — not all states) |
| `X_BMI5CAT` | BMI category |
| `X_SMOKER3` | Smoking status |
| `HLTHPLN1` | Health insurance coverage |

**Survey design (for national estimates):**
```r
library(survey)
brfss <- readRDS("data-raw/brfss/brfss_2023_women18plus.rds")
des <- svydesign(ids = ~X_PSU, strata = ~X_STSTR,
                 weights = ~X_LLCPWT, nest = TRUE, data = brfss)
```

**Note on BLADCON/URINCON:**  
The urinary incontinence module is optional; only states that opted in have
non-missing values. For national UI prevalence estimates use NHANES or SWAN
instead. BRFSS is used here for its large sample size and demographic
granularity (insurance × income × age × race).

**Chronic condition variables also in the 2023 BRFSS file:**

| Variable | Condition |
|----------|-----------|
| `BPHIGH6` | Ever told high blood pressure |
| `TOLDHI3` | Ever told high cholesterol |
| `CVDINFR4` | Ever told heart attack |
| `CVDCRHD4` | Ever told angina / coronary heart disease |
| `CVDSTRK3` | Ever told stroke |
| `ASTHMA3` | Ever told asthma |
| `CHCCOPD3` | Ever told COPD / emphysema |
| `CHCSCNC1` | Ever told skin cancer |
| `CHCOCNC1` | Ever told other cancer |
| `DIABETE4` | Ever told diabetes |

These 7+ chronic conditions are already present in the downloaded RDS and are
wired into `R/data-urps_population.R` for comorbidity-stratified demand cells.

---

## 2. ACS 2023 5-Year Estimates — American Community Survey

| Attribute | Value |
|-----------|-------|
| Script | `scripts/data_acquisition/02_download_acs.R` |
| DUA required | **No** |
| Census API key | **Yes** (free, see Prerequisites) |
| Download time | 15–30 min (PUMS downloads state by state) |

**What it provides:**  
Two complementary layers:
- **Summary tables** (B01001, B27001, B17001): state-level female population
  by age, insurance coverage, and poverty status — used as demand denominators
- **PUMS microdata**: person-level records for women 18+ with insurance type,
  income-to-poverty ratio, race/ethnicity — used as the ACS demographic backbone
  for `build_urps_population_cells()`

**Output files:**
```
data-raw/acs/acs5_2023_sex_by_age_state.rds        # B01001 — 1,248 rows
data-raw/acs/acs5_2023_insurance_by_age_state.rds  # B27001 — 2,964 rows
data-raw/acs/acs5_2023_poverty_state.rds           # B17001 — 3,068 rows
data-raw/acs/acs5_2023_pums_women18plus.rds        # PUMS — ~1.6M rows
data-raw/acs/acs_2023_manifest.txt
```

**Key PUMS variables in the RDS:**

| Variable | Description |
|----------|-------------|
| `AGEP` | Age |
| `SEX` | Sex (2 = female) |
| `HINS1`–`HINS7` | Insurance type flags (employer, direct, Medicare, Medicaid, Tricare, VA, IHS) |
| `PINCP` | Personal income |
| `POVPIP` | Income-to-poverty ratio (0–501; 501 = 501%+) |
| `RACBLK`, `RACWHT`, `HISP`, `RACASN`, `RACAIAN` | Race/ethnicity flags |
| `PWGTP` | Person weight |
| `PWGTP1`–`PWGTP80` | Replicate weights (for SE estimation) |
| `age_band_urps` | Derived: "18-39", "40-59", "60-64", "65-79", "80+" |
| `insured`, `medicare`, `medicaid`, `commercial`, `uninsured` | Derived insurance flags |
| `poverty_lt100`, `poverty_lt200` | Derived poverty flags |

**Note on ST variable:**  
`ST` (state FIPS) is automatically included when `state = "all"` — do NOT add it
to the `variables` list in `get_pums()`; it causes an API error.

**Survey design for PUMS (successive-difference replication):**
```r
library(srvyr)
pums <- readRDS("data-raw/acs/acs5_2023_pums_women18plus.rds")
des <- as_survey_rep(pums,
  weights    = PWGTP,
  repweights = matches("PWGTP[0-9]+"),
  type       = "successive-difference",
  mse        = TRUE)
```

---

## 3. MCBS 2022 — Medicare Current Beneficiary Survey

| Attribute | Value |
|-----------|-------|
| Script | `scripts/data_acquisition/03_download_mcbs.R` |
| DUA required | **No** (PUF only) |
| Download size | ~45 MB (Survey File ZIP) + ~10 MB (Cost Supplement ZIP) |
| Output size | ~5.6 MB (women 65+ RDS) |
| Time | ~5 min |

**What it provides:**  
6,879 Medicare women 65+ from the Fall 2022 wave with UI loss, care-seeking,
surgical history, chronic conditions, insurance type (Medicare Advantage vs.
Traditional), and income category. Used to calibrate the Medicare-aged (65+)
demand arm.

**Actual download URLs (2022 data, confirmed working):**

```
Survey File PUF:
  https://data.cms.gov/sites/default/files/2024-10/SFPUF2022_Data.zip

Cost Supplement PUF:
  https://data.cms.gov/sites/default/files/2025-01/13f8f755-6533-4adf-a4cf-5ca29161231f/CSPUF2022_Data.zip
```

> **If the script URLs return 404:** The CMS URL path changes with each release.
> Retrieve the current URL by parsing the CMS data catalog:
> ```bash
> curl -s "https://data.cms.gov/data.json" | \
>   python3 -c "import json,sys; d=json.load(sys.stdin); \
>   [print(r.get('title',''), r.get('downloadURL','')) \
>    for ds in d.get('dataset',[]) for r in ds.get('distribution',[]) \
>    if 'MCBS' in ds.get('title','') and r.get('downloadURL','').endswith('.zip')]"
> ```

**Downloaded and processed files (already on disk):**
```
data-raw/mcbs/SFPUF2022_Data.zip          # Survey File PUF (~45 MB)
data-raw/mcbs/CSPUF2022_Data.zip          # Cost Supplement PUF (~10 MB)
data-raw/mcbs/sfpuf2022/                  # Extracted: fall/winter/summer XPT + CSV
data-raw/mcbs/cspuf2022/                  # Extracted: cspuf2022.xpt + .csv
data-raw/mcbs/mcbs_2022_women65plus.rds   ← use this in R (6,879 rows)
data-raw/mcbs/mcbs_2022_manifest.txt
```

**Key variables in `mcbs_2022_women65plus.rds`:**

| Variable | Description | Non-missing |
|----------|-------------|-------------|
| `age_group` | "65-74" or "75+" | 100% |
| `sex` | Always "Female" | 100% |
| `medicare_adv` | On Medicare Advantage plan (0/1) | 100% |
| `private_medigap` | Has private supplement (0/1) | 100% |
| `ui_loss` | Reports urinary incontinence (43.7% prevalence) | 100% |
| `ui_talked_dr` | Discussed UI with physician | ~40% |
| `ui_had_surgery` | Had surgery for UI | ~40% |
| `had_stroke` | Stroke history | 100% |
| `had_cancer` | Cancer history | 100% |
| `had_depression` | Depression history | 100% |
| `had_osteoporosis` | Osteoporosis history | 100% |
| `income_cat` | "<$25K", "$25-50K", "$50-75K", ">$75K" | ~90% |
| `PUFFWGT` | Survey weight (Fall wave) | 100% |

**Two-tier data access:**

| File | DUA | Notes |
|------|-----|-------|
| Survey File PUF | **No** | No geography below census region |
| Cost Supplement PUF | **No** | Utilization & costs |
| Survey File LDS | **Yes** | Adds state/county FIPS, more vars |
| Cost Supplement LDS | **Yes** | Links to Medicare claims |

**LDS (Limited Data Set) application:**  
See `data-raw/mcbs/DUA_PROCESS.md` for the ResDAC application process.
Timeline: 3–6 months. Cost: ~$1,000–5,000.

---

## 4. NHAMCS / NAMCS — Ambulatory Care Utilization

| Attribute | Value |
|-----------|-------|
| Script | `scripts/data_acquisition/04_download_nhamcs_namcs.R` |
| DUA required | **No** |
| Download size | Variable (~50–200 MB each) |
| Time | ~5 min |

**What it provides:**  
Visit-level data for pelvic floor disorder encounters in emergency departments
(NHAMCS 2022) and physician offices (NAMCS 2019). Used to calibrate the
visit-based demand estimands (D1, D2, D3) and to estimate the share of UI care
delivered in each setting.

> **CRITICAL NOTE:** NHAMCS collected Outpatient Department (OPD) data through
> 2017 — that component is discontinued. For physician office visits, use **NAMCS**,
> not NHAMCS. The script downloads both.

**NAMCS 2019 is used** (not 2020–2022) because:
- 2020–2022: pandemic disruptions reduced response rates to non-representative levels
- 2022: survey redesign; no national visit-weight file available
- 2023: pilot year with provider-only file (no visit microdata)

**Source FTP (no DUA, no authentication required):**
```
NHAMCS: https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/NHAMCS/
NAMCS:  https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/NAMCS/
```

**Pelvic floor ICD-10 codes used for filtering:**

| Code | Condition |
|------|-----------|
| N39.3 | Stress urinary incontinence |
| N39.41 | Urge incontinence |
| N39.46 | Mixed incontinence |
| R32 | Unspecified urinary incontinence |
| N81.x | Pelvic organ prolapse (all subtypes) |
| R15.x | Fecal incontinence |

**Output files:**
```
data-raw/nhamcs/nhamcs_2022_ed_raw.rds
data-raw/nhamcs/nhamcs_2022_ed_pelvic_floor.rds   ← filtered to PFD codes
data-raw/nhamcs/namcs_2019_raw.rds
data-raw/nhamcs/namcs_2019_pelvic_floor.rds        ← women + PFD codes
data-raw/nhamcs/nhamcs_namcs_manifest.txt
```

**Survey design:**
```r
# Both NHAMCS and NAMCS use the same design structure:
library(survey)
namcs <- readRDS("data-raw/nhamcs/namcs_2019_raw.rds")
des <- svydesign(ids = ~CPSUM, strata = ~CSTRATM,
                 weights = ~PATWT, nest = TRUE, data = namcs)
```

---

## 5. CMS PSPS 2022 — Physician/Supplier Procedure Summary

| Attribute | Value |
|-----------|-------|
| Script | None — manual download required |
| DUA required | **No** |
| Download size | ~400 MB compressed, ~2.5 GB uncompressed |

**What it provides:**  
HCPCS-level service counts by facility vs. non-facility setting for every
provider. Used to calibrate the URPS CPT basket setting mix (office vs.
hospital outpatient vs. ASC vs. operative) in `R/supply-urps_settings.R`.

**Why it cannot be automated:**  
CMS data portal requires a JavaScript redirect + session cookie for bulk
downloads. `curl` and `wget` fail without a valid browser session.

**Manual download steps:**

1. Go to:
   <https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service>

2. Select year **2022**

3. Click **"Download"** → **"CSV"**

4. Save the unzipped CSV to:
   ```
   data-raw/cms_psps/MUP_PHY_R24P04_0001_D22_Prov_Svc.csv
   ```

**After downloading:**
```r
pkgload::load_all()
shares <- load_psps_pos_shares("data-raw/cms_psps/MUP_PHY_R24P04_0001_D22_Prov_Svc.csv")
print(shares)
# Copy output to URPS_DEFAULT_SETTING_MIX in R/supply-urps_settings.R
```

For the smaller geography-level aggregate (~20 MB), see `data-raw/cms_psps/DOWNLOAD.md`.

---

## 6. Census NPP — National Population Projections (already bundled)

The 2023 Census National Population Projections are **already included** in
`data-raw/census/`. No download needed.

```
data-raw/census/np2023_d1_mid.csv   # Middle series (primary)
data-raw/census/np2023_d1_low.csv   # Low-immigration series
data-raw/census/np2023_d1_hi.csv    # High-immigration series
```

Each file covers 2022–2100 at single-year-of-age detail
(`SEX, ORIGIN, RACE, YEAR, TOTAL_POP, POP_0…POP_100`).

**Usage:**
```r
# Female population by URPS demand age band (20-39, 40-59, 60-64, 65-79, 80+)
npp_female_by_band("mid", years = 2025:2050)

# Crude single-denominator: women 65+
npp_women_65plus("mid", years = 2025:2050)
```

The SSOT female filter is `SEX == 2 & ORIGIN == 0 & RACE == 0`.

---

## Summary Table

| Dataset | Script | DUA | Key Use | Status |
|---------|--------|-----|---------|--------|
| BRFSS 2023 | `01_download_brfss.R` | No | UI prevalence by age/race/income | ✅ downloaded |
| ACS 5-yr 2023 summary | `02_download_acs.R` | No | Demand denominators by state | ✅ downloaded |
| ACS PUMS 2023 | `02_download_acs.R` | No | Person-level demographic backbone | ⏳ run script |
| MCBS 2022 Survey PUF | `03_download_mcbs.R` | No | Medicare women 65+ UI + comorbidities | ✅ downloaded |
| MCBS 2022 Cost Suppl. | `03_download_mcbs.R` | No | Utilization & cost calibration | ✅ downloaded |
| NHAMCS 2022 (ED) | `04_download_nhamcs_namcs.R` | No | ER PFD visit counts | run script |
| NAMCS 2019 (office) | `04_download_nhamcs_namcs.R` | No | Office visit PFD counts | ✅ downloaded |
| CMS PSPS 2022 | Manual browser | No | CPT setting mix (office vs. ASC) | manual step |
| Census NPP 2023 | Bundled | No | Population projections 2022–2100 | ✅ bundled |

---

## Troubleshooting

**ACS PUMS download hangs or fails:**
- The download is ~1–2 GB and takes 15–30 min on a typical connection.
- If it fails mid-way, re-run the script — tidycensus caches per-state files
  in `~/.cache/tidycensus/` so progress is preserved.
- `ST` must NOT be in the `variables` list — it's automatically included when
  `state = "all"` and causes an API error if explicitly requested.

**MCBS URLs return 404:**
- CMS updates URL paths with each release. Parse `data.cms.gov/data.json` to
  find current URLs (see Section 3 above for the one-liner).

**BRFSS BLADCON/URINCON all missing:**
- These come from an optional state-added module. Only states that opted in
  have non-missing values. For a full 50-state UI prevalence estimate, see
  NHANES (R/43-sandvik.R) or SWAN (R/42-swan.R).

**Census API key not found:**
```r
tidycensus::census_api_key("YOUR_KEY_HERE", install = TRUE)
# Then restart R for ~/.Renviron to reload
```

---

*Last updated: 2026-08-03*
