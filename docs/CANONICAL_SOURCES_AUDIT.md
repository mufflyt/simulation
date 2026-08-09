# Canonical Sources Audit

**Rule.** Before writing a function here, look for a canonical implementation in
the sibling packages: `mufflyaccess`, `twostep`, `cliff`, `isochrones`,
`mysterymaps`. This repository has re-implemented at least twelve functions that
already existed elsewhere, and the re-implementations have since drifted in both
directions. One function is re-implemented **twice inside this repository**.

**Status: audit only.** Nothing below changes behaviour. The purpose is to make
the drift visible so an intentional extension can be told from a stale copy —
today neither is distinguishable, because nothing records where these came from.

Audited 2026-08-09 against `twostep@2ed3907` and `cliff@7c05c87` (both cloned
that day; both had been updated the day before).

**Second pass, 2026-08-09**, against installed `mufflyaccess`, added
`annual_trend`, `calculate_rural_metro_comparison` and `wilson_ci`. The first
pass compared this repository against `twostep` and `cliff` and did not sweep
the `mufflyaccess` export list, which is why two exact copies of installed,
callable functions were missed — and why the third finding, a duplication with
no sibling involved at all, had no section to land in.

---

## Availability

| package | installed? | callable from `simulation` today |
|---|---|---|
| `mufflyaccess` | yes | yes — already a declared dependency, pinned by commit |
| `mysterymaps`, `mysterycall` | yes | yes |
| `twostep` | **no** | no |
| `cliff` | **no** | no |
| `isochrones` | **no** | no — and its verifier is not exported |

So four of the twelve duplications below could not be replaced by a call today
even if we wanted to. That is a reason to record provenance now, not a reason to
ignore it. The two added in the second pass are **not** among those four:
`annual_trend` and `calculate_rural_metro_comparison` both duplicate
`mufflyaccess`, which is installed, declared and pinned — those two could be
delegated today.

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
| `calculate_proportion_ci` | Both use the Wilson score interval with the same algebra. This version adds explicit `NA` handling and a `method` field; the canonical wraps in `tryCatch`. See also `wilson_ci` below — this repository holds a **second** Wilson implementation. |
| `classify_workforce_outlook` | This version handles `NA` explicitly via `case_when`; the canonical's `ifelse` propagates `NA` anyway. Same thresholds. |
| `calculate_rural_metro_comparison` | Character-for-character identical to `mufflyaccess`'s **except the name of one private helper** — `.wf_safe_percentage()` here, `.ws_percentage()` there. Same arguments, same three-part return, same `rate_diff` guard. A copy, with no divergence to defend. |

### Stronger here — intentional extensions, worth keeping

| function | what this version adds |
|---|---|
| `calculate_replacement_gap` | A `horizon_years` argument and per-subspecialty grouping. This is the function `928df62` tried to delegate to `mufflyaccess` and `72a7e13` reverted — the pinned contract does not export it, but **`cliff` defines it**, which is the likelier canonical home. |
| `npp_total_female` | Requires `YEAR` in addition to `SEX`/`ORIGIN`/`RACE` and errors naming the missing columns. Also adapted from `data.table` to base subsetting, so it is a deliberate port, not a copy. |
| `calculate_state_vulnerability` | Asserts its required input columns, and **ranks by `vulnerability_score`**. |
| `annual_trend` | Identical to `mufflyaccess`'s line for line save **one token**: the fewer-than-three-points early return is `c(slope = NA_real_, lo = NA_real_, hi = NA_real_, p = NA_real_)` here and `c(slope = NA, ...)` there. Bare `NA` is *logical*, so the canonical returns a logical vector on short series and a double vector otherwise. Anything that `rbind`s or `vapply(..., numeric(4))`s across a set of series gets a type change that depends on the data. This version is type-stable. |

### Duplicated inside this repository

The audit was built to compare `simulation` against its siblings, so this one had
nowhere to land: **the Wilson score interval is implemented twice here**, in two
files, neither aware of the other.

| | `wilson_ci()` | `calculate_proportion_ci()` |
|---|---|---|
| file | `R/geography-spatial_access_e2sfca.R:546` | `R/reporting-workforce_statistics.R:47` |
| exported | yes | yes |
| algebra | `denom`, `center`, `margin` — identical | identical |
| shape | vectorised; tibble `estimate`/`lo`/`hi` | scalar; list `proportion`/`lower_ci`/`upper_ci`/`method`/`note` |
| guards | `stopifnot` on equal lengths and `successes <= n` | zero-or-`NA` denominator returns all-`NA` with a note |
| `n = 0` | `NA` via `ifelse(n > 0, ...)` | `NA` with `note = "Zero denominator"` |

The algebra agrees, so today they return the same interval. That is the whole
problem: **there is no test asserting they agree**, and each carries a guard the
other lacks — a length check on one side, a zero-denominator branch on the other.
A correction applied to one would not reach the other, and nothing in the
repository would notice.

Neither is a port. `calculate_proportion_ci` matches `mufflyaccess`'s and
`cliff`'s and is recorded above as equivalent; `wilson_ci` appears in **no**
sibling package and was evidently written independently for the spatial-access
module, in a file that is otherwise a `twostep` port.

The resolution is not obvious and is deliberately not asserted here. The two
return shapes serve genuinely different callers — a tibble column-bound into
tract frames, and a list read field-by-field in reporting. Making one call the
other is straightforward; deleting either is not.

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
3. **Ask about `calculate_state_vulnerability`** upstream, and send
   `annual_trend`'s `NA_real_` with it — a one-token type-stability fix that
   costs the canonical nothing.
4. **Decide what to do about the two Wilson implementations.** Not a delete:
   pick which is canonical *here*, have the other call it, and keep both return
   shapes. Until then, the cheap insurance is a test asserting the two agree on
   a shared grid of `(successes, n)`, so a correction to one cannot silently
   fail to reach the other.
5. **`calculate_rural_metro_comparison` is the easiest delegation available** —
   an exact copy of an installed, pinned export, with no divergence to preserve.
   If a first delegation is wanted to establish the pattern after `72a7e13`
   reverted the last attempt, this is the one with nothing to lose.
6. **Compare `calibration-parameter_uncertainty.R` against
   `urps_ci_param_draw()` / `urps_projection_ci()`** before touching it again.
7. **Decide whether `twostep` and `cliff` should become dependencies.** Neither
   is installed, so today the duplication cannot be removed by delegation —
   only documented. That is a project decision, not a code change.
