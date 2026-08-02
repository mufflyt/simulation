# Workforce Microsimulation Updates

_Ported approaches from the `cliff`, `twostep`, and `isochrones` sister repositories into the urogynecology/FPMRS workforce microsimulation._

## What changed and why

The original supply model (`R/workforce.R`, `R/supply_of_the_urogyn_workforce.R`) was **deterministic and population-level**: `supply[t] = supply[t-1] + entrants - retirements`, with demand from a single denominator (women 65+ × visit rate). That structure cannot express provider-level heterogeneity (age-dependent attrition, productivity decline), demand uncertainty, or geographic access.

These updates add a **stochastic, individual-level microsimulation** in the same per-agent Monte-Carlo style already used by the disease model (`R/01-dppm_setup.R::run_microsimulation_analysis`), plus a three-estimand demand sensitivity, a spatial-access layer, and reproducibility/provenance scaffolding. All new code is **additive** (numbered modules `10`–`15`); nothing in the existing scripts was removed.

## New modules

| File | Purpose | Ported from |
|---|---|---|
| `R/10-repro_provenance.R` | Reproducibility mode (strict/relaxed), seeding, run IDs, fingerprints, and a **fail-closed provenance-sidecar** artifact I/O (content SHA-256 verified before deserialisation). | `isochrones` Non-Negotiables #8/#18, `reproducibility_framework.R`, `cache_repaired_isochrones.R` |
| `R/11-canonical_and_joins.R` | `resolve_canonical()` (one named+checksummed source per input) and `safe_left_join()`/`safe_inner_join()` (block silent row loss / fan-out). | `isochrones` #6/#19, `join_safety.R`, `canonical_sources.yml` |
| `R/12-provider_microsimulation.R` | Individual-provider career microsimulation: age-band departure hazards, fixed-age entrant injection, **effective-FTE** productivity weighting, the strict **active-in-year (`retirement_year > Y`)** predicate, Monte-Carlo replicates, and a deterministic mean-field backbone for validation. | `cliff` `workforce_cliff_engine.R` (`wc_project`), `urps_module_a_effective_supply`; `isochrones` `retirement_filter_utilities.R` |
| `R/13-demand_urps.R` | Three independent demand estimands (D1 prevalent PFD cases, D2 consultations, D3 surgical volume), coverage, cliff **adequacy** ratio, and a **Spearman-ρ concordance** verdict; Wu 2011 age-specific surgery rates; legacy visit-based demand retained. | `cliff` `demand_denominator.R`, `urps_demand_denominators_sensitivity.R` |
| `R/14-spatial_access_e2sfca.R` | Base-R **E2SFCA/M2SFCA** access engine (`diff(W^power)` incremental weights, zero-demand→NA semantics, SPAR, zero-access share, access quintiles) and a **5 km haversine** provider→isochrone matcher with CONUS scoping. | `twostep` `desjardins7_e2sfca.R`, `two_step_floating_catchment.R`; `isochrones` `match_points_to_isochrones.R` |
| `R/15-run_workforce_microsimulation.R` | Orchestrator: seeds a run, executes cliff-style scenarios, computes supply × demand × concordance × outlook, and persists provenance-tagged artifacts. | integrates all of the above |

## Key invariants preserved from the source repos

- **Active-in-year is STRICT `>`** with `retirement_year` stored as first-inactive (`last_active + 1`). `retirement_year == Y` ⇒ not active in `Y`. (isochrones)
- **E2SFCA incremental weights = `diff(W^power)`**, squaring the *cumulative* weights before differencing for M2SFCA (never `diff(W)^2`); step 1 always power 1. (twostep)
- **Zero-demand provider ⇒ `ratio = NA`, contributes 0 access**, supply booked to an audit block — never silently treated as zero capacity. (twostep)
- **Three demand denominators reported by concordance, never blended.** (cliff)
- **Report both headcount and effective FTE**; older providers are down-weighted. (cliff)
- **Fail-closed loading**: a cache/artifact that merely exists is never trusted; content SHA-256 is re-checked before use. (isochrones)
- **CONUS-only** geographic scope for the access layer. (isochrones)

## Running it

```bash
Rscript scripts/run_workforce_microsimulation_example.R
# reproducible / manuscript mode:
REPRODUCIBILITY_MODE=strict Rscript scripts/run_workforce_microsimulation_example.R
```

Tests (pure-function regression guards):

```bash
Rscript -e 'testthat::test_dir("tests/testthat")'
```

## Calibration notes (illustrative vs empirical)

Some defaults are **illustrative and clearly marked for replacement** with empirical values:

- `MICROSIM_REFERENCE_HAZARD` — the age gradient. Replace with empirical person-year/event hazards from `cliff::wc_band_counts()`.
- `PFD_PREVALENCE_65PLUS`, `CONSULT_RATE_PER_WOMAN_65PLUS`, crude D3 rate — replace with the anchored Nygaard 2008 / Kirby 2013 / Wu 2011 series (`anchor_index()` is provided for the two-anchor extrapolations).
- Baseline workforce size: cliff carries **two unreconciled URPS baselines (1,295 frozen SSOT vs 1,339 roster)** — pick one deliberately (PI decision), do not silently average.

Baseline annual retirement rates are the cliff empirical values (FPMRS 4.4%, GO 5.2%, MIGS 3.4%), validated against state boards at 94.4% agreement.
