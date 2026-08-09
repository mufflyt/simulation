# Canonical Sources Audit

**Rule.** Before writing a function here, look for a canonical implementation in
the sibling packages: `mufflyaccess`, `twostep`, `cliff`, `isochrones`,
`mysterymaps`. This repository has re-implemented at least ten functions that
already existed elsewhere, and the re-implementations have since drifted in both
directions.

**Status: audit only.** Nothing below changes behaviour. The purpose is to make
the drift visible so an intentional extension can be told from a stale copy —
today neither is distinguishable, because nothing records where these came from.

Audited 2026-08-09 against `twostep@2ed3907` and `cliff@7c05c87` (both cloned
that day; both had been updated the day before).

---

## Availability

| package | installed? | callable from `simulation` today |
|---|---|---|
| `mufflyaccess` | yes | yes — already a declared dependency, pinned by commit |
| `mysterymaps`, `mysterycall` | yes | yes |
| `twostep` | **no** | no |
| `cliff` | **no** | no |
| `isochrones` | **no** | no — and its verifier is not exported |

So four of the ten duplications below could not be replaced by a call today even
if we wanted to. That is a reason to record provenance now, not a reason to
ignore it.

---

## Two things this audit does NOT conclude

**Line count is not a quality signal.** The first pass flagged six functions as
"shorter in simulation" and it was misleading. `calculate_two_prop_test` is 25
lines here against 63 in `cliff` and is *functionally equivalent* — same
small-sample guard, same zero-denominator guard, same `tryCatch`, same return
shape, just denser formatting. Every classification below is on behaviour.

**A name collision does not say which version should win.** Several divergences
here look like deliberate improvements. One looks like this repository fixed a
bug that is still upstream.

---

## Findings

### Weaker here — ported from `twostep` and stripped

All three are in `R/geography-spatial_access_e2sfca.R`, which already declares
one function as a "Port of twostep::mc_weighted_ci". The rest of the file is
evidently ported too, and the ports lost validation.

| function | what the canonical does that this does not |
|---|---|
| `e2sfca_band_weights` | Validates with `checkmate`; parses weight names as band-minutes and **errors** if they are not; **errors** on non-monotonic weights. Here: sorts by `as.numeric(names(...))` without validating, and only **warns** on non-monotonicity. |
| `gaussian_band_weights` | Validates inputs; routes through `e2sfca_band_weights()` so it inherits those checks; attaches a `decay_meta` attribute declaring the result a *normalized zonal* vector rather than a raw kernel. Here: computes `exp()` inline, no validation, no metadata, does not route through the band-weight check. |
| `e2sfca_incremental_weights` | **Errors when `step2_power > 1` and any cumulative weight exceeds 1**, because squaring a weight above 1 would *increase* access rather than decay it. Also clamps float error. Here: neither. |

**The `e2sfca_incremental_weights` guard is the one that matters.** It is a
correctness guard, not a style check. Severity, stated precisely:

* **Latent under defaults** — `E2SFCA_DEFAULT_WEIGHTS` is `1.00, 0.68, 0.22,
  0.09`; the maximum is exactly 1, so the canonical guard would not fire.
* **Reachable in M2SFCA mode** — `step2_power = 2` is a supported path
  (`compute_access()` labels it `"M2SFCA"`, and `access_weight_sensitivity()`
  sweeps the power). A caller supplying custom weights above 1 in that mode
  would silently get access that *rises* with distance.

### Equivalent — divergence is formatting, not behaviour

| function | note |
|---|---|
| `calculate_two_prop_test` | Same guards, same return shape, same p-value formatting. |
| `calculate_proportion_ci` | Both use the Wilson score interval with the same algebra. This version adds explicit `NA` handling and a `method` field; the canonical wraps in `tryCatch`. |
| `classify_workforce_outlook` | This version handles `NA` explicitly via `case_when`; the canonical's `ifelse` propagates `NA` anyway. Same thresholds. |

### Stronger here — intentional extensions, worth keeping

| function | what this version adds |
|---|---|
| `calculate_replacement_gap` | A `horizon_years` argument and per-subspecialty grouping. This is the function `928df62` tried to delegate to `mufflyaccess` and `72a7e13` reverted — the pinned contract does not export it, but **`cliff` defines it**, which is the likelier canonical home. |
| `npp_total_female` | Requires `YEAR` in addition to `SEX`/`ORIGIN`/`RACE` and errors naming the missing columns. Also adapted from `data.table` to base subsetting, so it is a deliberate port, not a copy. |
| `calculate_state_vulnerability` | Asserts its required input columns, and **ranks by `vulnerability_score`**. |

### One to send upstream

`cliff::calculate_state_vulnerability` computes `vulnerability_score =
pct_loss_if_retire * log10(pmax(1, count_active))` and then sorts by
`pct_loss_if_retire` — not by the score it just computed. A function returning a
"vulnerability ranking" that ranks by a different column than its own score
looks like a defect. This repository sorts by `vulnerability_score`.

Stated as an observation, not a verdict: the upstream author may have intended
the raw-percentage ordering. It is worth one question rather than a silent
divergence in either direction.

### Not duplicated, but unused

`mufflyaccess` is installed and exports `urps_ci_param_draw(retirement_sigma_sd,
entrant_cv, seed)` and `urps_projection_ci(project_fn, scenarios, years, B, ...)`.
Neither is called anywhere here, while `R/calibration-parameter_uncertainty.R`
implements its own retirement-sigma and entrant-CV draws. Unlike `twostep` and
`cliff`, this one is callable **today**. Worth a comparison before the next
change to the parameter-uncertainty module.

`mufflyaccess::mc_weighted_ci()` is deliberately *not* used: `access_moe_ci()` is
named differently to avoid shadowing it, and `ssot_coverage_report()` records
that the contract version "propagates ACS MOE, a different quantity". That is the
pattern the other ten should follow — a declared relationship rather than a
silent copy.

---

## Recommended follow-ups, in order

1. **Restore the `e2sfca_incremental_weights` M2SFCA guard.** The only finding
   with a correctness consequence. Small, and it fails closed.
2. **Annotate each duplicated function with its canonical source**, as
   `access_moe_ci()` already does. Cheap, and it is what makes the next audit
   unnecessary.
3. **Ask about `calculate_state_vulnerability`** upstream.
4. **Compare `calibration-parameter_uncertainty.R` against
   `urps_ci_param_draw()` / `urps_projection_ci()`** before touching it again.
5. **Decide whether `twostep` and `cliff` should become dependencies.** Neither
   is installed, so today the duplication cannot be removed by delegation —
   only documented. That is a project decision, not a code change.
