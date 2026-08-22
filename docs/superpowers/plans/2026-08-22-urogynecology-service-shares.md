# Urogynecology Service Shares Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a provenance-tracked, empirically calibrated provider service-share engine and wire it into the URPS microsimulation without changing legacy behavior.

**Architecture:** Five stacked PRs separate evidence ingestion, CHIA transport evidence, calibration, simulation integration, and validation/CI. Table-first pure R functions are used wherever possible so every evidence calculation can be tested with small fixtures; real-data scripts fail closed when mounted inputs are missing or hashes differ.

**Tech Stack:** R >= 4.1, dplyr, tidyr, purrr, readr, DBI, duckdb, digest, testthat.

**Spec:** `docs/superpowers/specs/2026-08-22-urogynecology-service-shares-design.md`

## Global Constraints

- Preserve the frozen CMS partial-identification estimand and equations in `docs/PRESPEC_URPS_SHARE.md`.
- Never pool CMS Medicare FFS and CHIA Massachusetts all-payer rows as one population.
- Both `207VF0040X` and `2088F0040X` classify as URPS.
- Production mode fails on example/unsourced active mappings and missing required evidence.
- Calibrated shares replace corresponding provider-routing assumptions; no double delegation.
- Legacy simulation behavior remains available and is regression-tested.
- External real-data jobs fail closed; portable unit/fixture tests remain runnable without private data.

---

### Task 1: Canonical registry and CMS evidence builder

**Files:**
- Create: `R/data-urogynecology_service_share_registry.R`
- Create: `R/data-cms_urogynecology_service_shares.R`
- Create: `scripts/calibration/build_cms_urogynecology_service_share_evidence.R`
- Create: `tests/testthat/test-data-cms-urogynecology-service-shares.R`

**Interfaces:**
- Produces: `urogynecology_service_share_registry()`, `urogynecology_provider_taxonomy_registry()`, `validate_service_share_registry()`, `build_cms_service_share_evidence()`.
- `build_cms_service_share_evidence()` returns `service_bounds`, `aggregate_bounds`, `diagnostics`, `provenance`, and `estimand`.

- [ ] Write registry tests for unique HCPCS ownership, source metadata, Tier A/B membership, and both URPS taxonomy codes.
- [ ] Implement the versioned registry and strict validator.
- [ ] Write a synthetic CMS fixture with retained URPS, other-physician, APP, and suppressed volume.
- [ ] Implement `T/U/O/N/M`, `L/H`, observed-cell share, capture, and wRVU-weighted aggregate calculations.
- [ ] Assert `T = U + O + N + M`, nonnegative `M`, complete provider-class mapping, and denominator positivity.
- [ ] Add the real-data calibration script with frozen 2024 roster SHA assertion and timestamped RDS/CSV output.
- [ ] Verify fixture tests and inspect the PR diff.

### Task 2: CHIA evidence and transport diagnostics

**Files:**
- Create: `R/data-chia_urogynecology_service_shares.R`
- Create: `scripts/calibration/build_chia_urogynecology_service_share_evidence.R`
- Create: `tests/testthat/test-data-chia-urogynecology-service-shares.R`

**Interfaces:**
- Consumes canonical registry from Task 1.
- Produces: `read_chia_service_share_events()`, `build_chia_service_share_evidence()`, `compare_chia_to_cms_service_share_evidence()`.

- [ ] Write a temporary DuckDB fixture containing classified pelvic-floor events.
- [ ] Implement fail-closed reading of the classified CHIA event table.
- [ ] Implement provider composition by service/year/payer/setting and conditional URPS-among-physician share.
- [ ] Implement CMS-bound comparison without pooling rows.
- [ ] Add discrepancy and transport-SD diagnostics that increase with source disagreement.
- [ ] Add a real-data script that requires `URPS_CHIA_DUCKDB` and a classified event table.
- [ ] Verify fixture tests and inspect the stacked PR diff.

### Task 3: Held-out calibration and joint composition draws

**Files:**
- Create: `R/calibration-urogynecology_service_shares.R`
- Create: `scripts/calibration/calibrate_urogynecology_service_shares.R`
- Create: `tests/testthat/test-calibration-urogynecology-service-shares.R`

**Interfaces:**
- Consumes classified event counts plus optional CMS and CHIA evidence.
- Produces: `select_service_share_concentration()`, `draw_service_share_composition()`, `calibrate_service_share_model()`, `validate_service_share_bundle()`.

- [ ] Build known-truth multi-year service/provider fixtures.
- [ ] Implement leave-latest-year-out predictive scoring across a declared alpha grid.
- [ ] Select concentration by held-out multinomial log score/cross-entropy rather than a fixed 20.
- [ ] Draw joint Dirichlet compositions with deterministic seeding and exact row normalization.
- [ ] Score CMS interval evidence and CHIA transport evidence separately.
- [ ] Inflate transport discrepancy when sources disagree rather than averaging the source estimates.
- [ ] Save share draws, selected alpha values, validation scores, source fits, hashes, seed, and git SHA in the calibration bundle.
- [ ] Verify known-truth recovery, normalization, reproducibility, and source-disagreement tests.

### Task 4: Simulation and workload integration

**Files:**
- Create: `R/core-service_share_engine.R`
- Modify: `R/core-run_end_to_end_simulation.R`
- Create: `tests/testthat/test-core-service-share-engine.R`
- Modify: `tests/testthat/test-core-run_end_to_end_simulation.R`

**Interfaces:**
- Produces: `service_share_routing_for_year()`, `allocate_urps_service_workload()`.
- Adds runner args: `service_share_engine`, `service_share_bundle`, `service_share_draw`.

- [ ] Write tests that calibrated routing sums to one and maps provider types into URPS/APP/other exactly once.
- [ ] Implement year/draw extraction from a validated calibration bundle.
- [ ] Implement service-level volume and work-RVU conservation checks.
- [ ] Add calibrated routing arguments to `run_end_to_end_simulation()` with legacy as the default.
- [ ] Pass calibrated routing into `pathway_provider_service_volumes()` instead of multiplying a second delegation share afterward.
- [ ] Compute `wrvu_total` from service-specific URPS volume and `urps_service_workload()` in calibrated mode; preserve legacy arithmetic byte-for-byte in legacy mode.
- [ ] Store service-share diagnostics/config in the simulation bundle.
- [ ] Verify missing bundles fail closed and seeded legacy regression remains unchanged.

### Task 5: Validation, source dropout, provenance, and CI

**Files:**
- Create: `R/validation-urogynecology_service_shares.R`
- Create: `scripts/validation/07_service_share_calibration_validation.R`
- Create: `tests/testthat/test-validation-urogynecology-service-shares.R`
- Create or modify: `.github/workflows/service-share-validation.yml`

**Interfaces:**
- Produces: `validate_service_share_accounting()`, `evaluate_service_share_source_dropout()`, `service_share_reproducibility_digest()`.

- [ ] Add exact accounting validation for share sums, volume conservation, work-RVU conservation, inactive-provider zero workload, and CMS `T/U/O/N/M` closure.
- [ ] Add CMS-only, CHIA-only, and combined-source dropout comparisons.
- [ ] Add deterministic digest tests for identical inputs/seeds and changed-digest tests when evidence changes.
- [ ] Add a portable fixture CI job that runs on every PR.
- [ ] Add a guarded real-data validation job that runs only when mounted CMS/CHIA inputs are available and fails rather than substituting examples.
- [ ] Emit timestamped validation tables and a machine-readable provenance manifest.
- [ ] Inspect all five stacked diffs and CI states before declaring completion.
