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

**Third pass, 2026-08-09**, added `isochrones`, `mysterycall` and `mysterymaps`,
and switched method: instead of checking suspected functions one at a time, it
extracts every top-level `name <- function` definition in each repository and
intersects the name sets. Both earlier passes were driven by suspicion, which is
why both missed exact copies sitting in plain sight.

| repo | definitions | names shared with `simulation` |
|---|---:|---:|
| `isochrones` | 6,612 | 15 |
| `mufflyaccess` | 117 | 14 |
| `cliff` | 59 | 7 |
| `twostep` | 40 | 3 |
| `mysterymaps` | 25 | **0** |
| `mysterycall` | 580 | **0** |

`simulation` defines 602.

**What a name intersection does and does not prove.** A shared name is a lead,
not a finding; every entry below was opened and compared. It also under-reports:
the same computation under two different names is invisible to it, which is
exactly how `wilson_ci` and `calculate_proportion_ci` coexisted here. And
`isochrones` needs a discount — it is a project, not a package (6,612
definitions across 1,307 files, one export), so most of its names are
script-local rather than canonical, and a collision there carries less weight
than one against a package export.

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

`calculate_proportion_ci` matches `mufflyaccess`'s and `cliff`'s and is recorded
above as equivalent.

**Correction (third pass).** The second pass asserted that `wilson_ci` "appears
in no sibling package." That was wrong, and wrong in the way this whole audit is
about: it was concluded from a targeted check of `mufflyaccess`, `twostep` and
`cliff` rather than from a sweep. `isochrones` defines its own `wilson_ci` at
`R/integrate_nber_credentials_GO_table2.R:30` — `(x, n, conf)`, returning
`c(lower, upper)`, guarding `n == 0`, `x < 0` and `x > n`. Its half-width is
written `z * sqrt((p(1-p) + z²/4n) / n) / denom` against this repository's
`z * sqrt(p(1-p)/n + z²/(4n²)) / denom`; those are the same expression
rearranged.

So the count is **three Wilson implementations reachable from this workspace**
— two here, one in `isochrones` — or five if `cliff`'s and `mufflyaccess`'s
`calculate_proportion_ci` are counted. Three distinct return shapes
(`tibble`, `list`, named vector) and three distinct guard sets, all computing
one textbook interval.

The resolution is not obvious and is deliberately not asserted here. The two
return shapes serve genuinely different callers — a tibble column-bound into
tract frames, and a list read field-by-field in reporting. Making one call the
other is straightforward; deleting either is not.

### Exact copies of an installed, pinned export

Found by the third pass. `mufflyaccess` is declared, pinned and callable today,
and these are byte-identical to its versions — not ports, not adaptations, no
divergence to preserve.

| function | here | note |
|---|---|---|
| `zero_access_share` | `R/geography-spatial_access_e2sfca.R` | Byte-identical, including the `stopifnot`, the `is.finite(sw)` guard and the `NA_real_` return. |
| `weighted_mean_all` | `R/geography-spatial_access_e2sfca.R` | Byte-identical; six lines. |
| `calculate_rural_metro_comparison` | `R/reporting-workforce_statistics.R` | Identical but for one private helper's name (see *Equivalent* above). |

Both spatial ones sit in the file already annotated as a `twostep` port, and
both are `twostep` exports as well as `mufflyaccess` ones — so the likely history
is a port from `twostep` before `mufflyaccess` re-exported them, which nobody
revisited.

### Same name, different quantity — the dangerous class

The sweep matches on name, and these matched. They are **not** copies, and that
is the problem: a reader who sees a `mufflyaccess` name here will reasonably
assume contract parity, and there is none.

| function | `mufflyaccess` | here |
|---|---|---|
| `urps_p_active` | `(age, sex)`; logistic on age from the LFP parameter table. | `(age, sex, years_certified, scenario_id, coef, registry)`; coerces and recycles all inputs to a common length, validates `sex`, and admits a scenario registry. A **superset**, and `years_certified` means it is not the same function of the same arguments. |
| `urps_survival_curve` | 13 lines. | 27 lines: adds `pathway` (ABOG/ABU), sex-keyed coefficient selection, `scale_shift` and `entry_age`. |

Neither should be deleted. Both need what `ssot_coverage_report()` already does
for other quantities — a recorded row saying local-or-SSOT **and why** — plus,
ideally, a name that does not promise contract parity it does not deliver.

### Shared with `isochrones`

Six utility names collide. Discount them as described under *Third pass*:
`isochrones` is a project, so most are script-local. One is worth reading anyway.

`haversine_km` is defined in both. This repository inlines the great-circle
formula with `EARTH_RADIUS_KM <- 6371.0088` and a `pmin(1, sqrt(a))` clamp.
`isochrones` **already solved this the way this audit recommends**: its
`haversine_km` is a four-line delegation to a canonical `haversine_km_vec()` in
`R/utils/spatial_distance.R`, carrying a comment that records the radius it
moved from (6371 → 6371.0088, "1.4 ppm"), the clamp it gained, and the
conclusion that the two are numerically identical for all real inputs.

Both now use 6371.0088, so the values agree. The point is the pattern, not the
constant: the delegation-with-a-recorded-reason that recommendation 2 asks for
already exists next door, and is worth copying as a house style.

The other five — `isTRUE_vec`, `make_run_id`, `match_points_to_isochrones`,
`safe_inner_join`, `safe_left_join` — are generic utility names whose collision
is weak evidence of a shared lineage. Not compared line by line; listed so the
next audit does not rediscover them as new.

### Swept and clean

`mysterymaps` (25 definitions) and `mysterycall` (580) share **zero** function
names with this repository. Recorded as a result, not an omission: the mapping
and secret-shopper layers are genuinely disjoint from the workforce model, and a
future search for a canonical implementation does not need to look there.

### Matched but not yet compared

`cesarean_rate_for_year`, `cohort_vaginal_exposure`, `completed_parity_for_cohort`
and `.obstetric_extdata` are defined in both this repository and `mufflyaccess`.
The names collide; the implementations were not opened. Listed as open rather
than classified, because guessing at four obstetric-exposure functions would put
unverified rows in a document whose value is that every row was checked.

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
4. **Decide what to do about the Wilson implementations** (two here, a third
   in `isochrones`). Not a delete:
   pick which is canonical *here*, have the other call it, and keep both return
   shapes. Until then, the cheap insurance is a test asserting the two agree on
   a shared grid of `(successes, n)`, so a correction to one cannot silently
   fail to reach the other.
5. **Delegate the three exact copies** — `zero_access_share`,
   `weighted_mean_all`, `calculate_rural_metro_comparison`. All byte-identical
   to installed, pinned `mufflyaccess` exports, with no divergence to preserve.
   If a first delegation is wanted to re-establish the pattern after `72a7e13`
   reverted the last attempt, these are the three with nothing to lose. Copy
   `isochrones`'s `haversine_km` for the house style: delegate, and leave a
   comment recording what changed and why it does not matter.
6. **Rename or record `urps_p_active` and `urps_survival_curve`.** Carrying a
   `mufflyaccess` name for a function with a different signature is worse than
   carrying a different name, because it promises a parity that does not exist.
7. **Open the four obstetric-exposure collisions** and classify them, so the
   *Matched but not yet compared* section can be emptied.
8. **Compare `calibration-parameter_uncertainty.R` against
   `urps_ci_param_draw()` / `urps_projection_ci()`** before touching it again.
9. **Decide whether `twostep` and `cliff` should become dependencies.** Neither
   is installed, so today the duplication cannot be removed by delegation —
   only documented. That is a project decision, not a code change.
