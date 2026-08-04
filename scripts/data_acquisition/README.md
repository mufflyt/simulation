# Data-acquisition scripts

Each script pulls one real input for the URPS supply/demand microsimulation into
`data-raw/<domain>/`, with a manifest and a clear access note. `data-raw/` is
`.Rbuildignore`d, so raw microdata travels with the git repo (or not, when
licensed) but never ships in the built package.

| Script | Source | Feeds | Access |
|---|---|---|---|
| `01_download_brfss.R`, `02_download_brfss_2024.R` | BRFSS | prevalence / risk | free |
| `02_download_acs.R` | ACS 5-yr (state B01001 + PUMS) | demand denominators, insurance | free API key |
| `03_download_mcbs.R` | Medicare Current Beneficiary Survey | utilization (65+) | free |
| `04_download_nhamcs_namcs.R` | NAMCS/NHAMCS | office-visit anchors | free |
| `05_download_meps_2022.R`, `06_..2023.R` | MEPS | care-seeking, utilization | free |
| `07_download_nhanes_urinary.R` | NHANES | UI prevalence | free |
| **`08_download_acs_tracts.R`** | ACS 5-yr (**tract** B01001 by age band) | **isochrone demand** (`R/32`) | free API key |
| **`09_download_swan_icpsr.R`** | **SWAN** (ICPSR 253) | **DMDM UI hazards** (`R/31`) | ICPSR account (public) / DUA (restricted) |
| **`10_ingest_hcup_nass.R`** | **HCUP NASS** (licensed) | **base-year procedure anchors** (`R/28`) | HCUP Central Distributor |

## Already in the repo (do not re-download)

- **Census 2023 national projections** — `data-raw/census/np2023_d1_{mid,low,hi}.csv`.
- **Tract female-65+ centroids** — `data-raw/spatial/tract_fem65_centroids.csv`
  (join `08`'s age-band file to it on `GEOID` for the full demand age structure).
- **NPPES** provider registry and **Medicare Part B PUF** — maintained outside
  this tree; the Part B PUF anchors the 65+ share of the NASS procedure totals.

## Highest-leverage next pulls

`08` (real tract demand for isochrones) and `10` (flips `calibration_status` off
placeholder) are the two that move the most model components from "illustrative"
to "results"; `09` fits the DMDM UI hazards. See `docs/DEMAND_METHODS.md` §5–§8.
