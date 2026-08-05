# Demand pipeline — operator runbook

How to drive the URPS demand stack from raw data to results. The machinery is
built and wired; turning placeholders into results is a matter of running the
data pulls (each free or licensed as noted) and then the matching runner. Methods
and provenance live in `docs/DEMAND_METHODS.md`; this is the operational map.

## One-command status

```r
source("scripts/run_demand_pipeline.R")     # runs every runner, prints a status table
```

Prints (and writes `artifacts/demand_pipeline_status.csv`) one row per component
with its state — `real_population` / `calibrated` / `fitted` /
`derived_by_analogy` are progressively stronger than `example_only` /
`illustrative` / `placeholder`. Every weak row names the pull that upgrades it.
Nothing below is required to *run* the stack (it degrades to example inputs); the
pulls are what make the numbers real.

## Pull → runner → what it flips

| # | Pull (acquire) | Acquisition script | Runner | Output | Flips |
|---|---|---|---|---|---|
| 1 | **ACS 5-yr tract population** by age band (free Census API key) | `scripts/data_acquisition/08_download_acs_tracts.R` | `isochrone_demand_from_tracts()` / the pipeline's isochrone step | `artifacts/tract_pfd_need.csv` | isochrone demand → `real_population` |
| 2 | **HCUP NASS** procedure volumes (licensed; or free HCUPnet totals) | `scripts/data_acquisition/10_ingest_hcup_nass.R` | `scripts/run_demand_calibration_backtest.R` | `artifacts/demand_calibration_scalars.csv`, `demand_backtest_*.csv` | `calibration_status` → `calibrated`; a real MAPE |
| 3 | **SWAN** longitudinal cohort (ICPSR account) | `scripts/data_acquisition/09_download_swan_icpsr.R` | `scripts/run_swan_dmdm_fit.R` | `artifacts/swan_dmdm_transitions.rds` | DMDM UI hazards → `fitted` |

POP is already `derived_by_analogy` from the cited literature
(`dmdm_transitions_with_pop_literature()`, MOAD/WHI/SWEPOP); SWAN has no POP-Q
staging and does not follow AI, so those stay literature / placeholder until a
POP-Q cohort (MOAD/WHI) and an AI cohort are fitted.

## End-to-end hand-off (fitted → contract → cliff)

```r
source("scripts/demand_contract_end_to_end.R")
```

Takes the SWAN-fitted transitions (or the literature-POP object if SWAN hasn't
been fitted), runs the open-population trajectory, exports the versioned demand
contract with **per-tier provenance**, and round-trips it through cliff's
ingestion — confirming `dmdm_ui` reads `fitted`, `dmdm_pop` `derived_by_analogy`,
`dmdm_ai` `placeholder`, and `tier3` (any-PFD) the weakest of the three, so a
downstream consumer gates on the tier it actually reads.

## Reproducibility notes

- **Anchors by year.** For a genuine back-test (not just structure), give the
  `data/anchors/*.csv` a `year` column covering the `fit_through_year` and
  `target_year` in `config/calibration_targets.yml`; the runner keeps the latest
  year per category and reports whether real or illustrative anchors were used.
- **Local toolchain.** CI runs R CMD check (`error-on: warning`) + a structural
  hygiene gate. After changing exported functions, run `devtools::document()`
  locally to sync `NAMESPACE`/`man/`, then `devtools::test()`. The base-R core is
  always checkable offline with `Rscript scripts/smoke_demand_base_r.R`.
- **cliff side.** The contract is consumed by cliff via
  `read_dpmm_demand_contract()` + `dpmm_tier_status()` /
  `dpmm_alt_d1_index(tier = …)`; see cliff's
  `scripts/urps_demand_denominators_sensitivity.R` (`CLIFF_USE_DMDM_DEMAND=1`).
