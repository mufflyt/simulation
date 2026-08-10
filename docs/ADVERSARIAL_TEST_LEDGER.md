# Adversarial testing ledger

24 cycles, 10 new tests each, rotating 4/3/3 · 3/4/3 · 3/3/4 across
boundary-value, semantic/contract and adversarial categories.

The objective is not 240 passing tests. It is to find places where plausible
inputs, boundary conditions, hidden state or semantic misunderstandings make the
model produce a scientifically wrong answer. A cycle that finds nothing is a
real result; a cycle that manufactures ten green variants of existing tests is
not.

**Rule carried forward:** when a defect is found, sweep for the same defect
*class* elsewhere before calling it resolved.

---

## Cycle 01 — 2026-08-09

**Mix:** 4 BVA · 3 semantic · 3 adversarial → `tests/testthat/test-adversarial-cycle01.R`

**Targets and why.** Coverage was thinnest exactly where a test passes while the
science is wrong: year indexing (3 test files touched it), cohort aging (5),
FTE-vs-headcount semantics (7).

| # | cat | target | assumption challenged |
|---|---|---|---|
| 1 | BVA | `simulate_provider_career_once` | entrant vector accepted at exactly `ny` and `ny-1`, refused at `ny-2`/`ny+1` |
| 2 | BVA | entrant recycling | the recycled final slot is never read |
| 3 | BVA | `conversion_floor` | open at 0, closed at 1, refused above 1 |
| 4 | BVA | single-year horizon | no transitions ⇒ no entrants |
| 5 | semantic | year indexing | a spike in one slot appears in exactly one year |
| 6 | semantic | FTE vs headcount | clinical FTE never exceeds headcount |
| 7 | semantic | cohort aging | closed cohort ages exactly 1 yr/step |
| 8 | adversarial | RNG | seed determines results; ambient state still leaks in (no internal re-seed) |
| 9 | adversarial | row order | shuffling the agent table does not move the estimand |
| 10 | adversarial | duplicate ids | two rows sharing an id are two clinicians |

**Lead investigated and ruled out.** `effective_entrants <- rep_len(entrants_per_year,
length(years))` recycles, so a length `ny-1` path reuses year 1's value at
position `ny` — which would put the wrong entrant count in the final projection
year. Demonstrated the recycling in isolation (`10 20 30 40 50` → `10 20 30 40
50 10`), then found the loop guards with `if (i < n_years)`, so the recycled slot
is never read. **Latent trap, not an active defect.** Test 2 pins it, so a future
change from `<` to `<=` fails loudly instead of silently shifting entrants.

**Result:** 25 assertions, all passing. **0 defects found.**

**Bug class to sweep in a later cycle:** silent recycling where alignment
matters — `rep_len`/`rep(length.out=)` on any vector indexed by year or age.

**Full suite:** not run this cycle; the repository is being modified
concurrently by another session and suite runs have been killed repeatedly under
machine load. Related files (`test-workforce-microsimulation.R`,
`test-orchestrator-wiring.R`) to be run at the next stable point.

---

## Cycle 02 — 2026-08-09

**Mix:** 3 BVA · 4 semantic · 3 adversarial → `tests/testthat/test-adversarial-cycle02.R`

**Targets and why.** Discharged the bug class cycle 01 carried forward (silent
recycling), then moved to three of the named-but-untouched priorities: scenario
parameter propagation, calibration targets, validation leakage.

| # | cat | target | assumption challenged |
|---|---|---|---|
| 1 | BVA | `fit_calibration_scalars` | flag is strict: exactly `max_scalar` / `1/max_scalar` is not flagged; `predicted == 0` gives NA, not Inf |
| 2 | BVA | `detect_proportional_estimands` | comparison closed at `tol * max\|r\|`; one year cannot establish proportionality |
| 3 | BVA | `assert_no_leakage` | cutoff year itself is legal, cutoff+1 is LEAKAGE, and the message names both years |
| 4 | semantic | `supply_p_active` | a partial covariate is refused, not misaligned |
| 5 | semantic | lifecycle family (7 fns) | the same refusal everywhere the class occurs |
| 6 | semantic | `apply_calibration_scalars` | an uncalibrated category is never returned in a calibrated table |
| 7 | semantic | `.resolve_retirement_shift` | a fractional shift reaches the model at its declared size |
| 8 | adversarial | scenario ids | an unknown id is audible, a known id is silent, the three levers differ |
| 9 | adversarial | leakage audit | an empty audit is a failure; a stale high-water read still trips |
| 10 | adversarial | `assert_estimands_independent` | proportional estimands are refused in strict mode, not merely noted |

### Defects found and fixed — 4

**D1 · silent partial recycling (9 sites).** `rep_len()` was reached on
per-provider covariates in `supply_p_active` (age, sex, years_certified),
`departure_hazard`, `predict_clinical_hours`, `participation_fte`,
`participation_p_no_patient_care`, `.hwsm_hours_offset`, `.hours_offset_scaled`,
`career_state_of` (entered, retired) and `career_departure_by_state`. Unlike base
arithmetic recycling, `rep_len()` does not warn on a non-multiple length, so
`supply_p_active(c(35,50,65,75), "female", c(2,17,32))` scored a 75-year-old as
certified 2 years ago and returned a plausible number in silence.

*Fix:* `.recycle_aligned()` in `R/core-canonical_and_joins.R` — length must be 1
or `n`; a partial vector stops. Generalises the rule
`weighted_interval_score()` already applied to the same failure. All nine sites
converted; the documented scalar contract is unchanged. **Bug class closed.**

**D2 · fractional scenario shift truncated.** `.resolve_retirement_shift()` ended
in `as.integer()`, which truncates toward zero, so a registry declaring
`retirement_shift_years = 0.5` reached the model as `0` and `-1.5` as `-1`.
`validate_scenario_registry()` accepts any numeric within ±10, so the registry
and the model disagreed with no diagnostic. *Fix:* `as.numeric()`. The shift is
a continuous offset on the age axis and the Weibull `scale_shift`; both accept
fractions, so there was nothing to round to.

**D3 · re-calibration silently a no-op.** `apply_calibration_scalars()` on a
table that already carried `calibration_scalar` produced `calibration_scalar.x`
/ `.y`; `out$calibration_scalar` resolved to NULL and `coalesce(NULL, 1)`
multiplied every value by 1. The result still carried calibration columns, so
uncalibrated output reported itself as calibrated. *Fix:* refuse an
already-calibrated `values`.

**D4 · uncalibrated category returned as calibrated.** The same
`coalesce(., 1)` turned a category with no fitted scalar into a silently
unscaled one. Reachable in ordinary use: `min_match_rate = 1.0` only *warns* in
relaxed mode, and a matched key with an NA scalar (what `fit_calibration_scalars`
emits when `predicted == 0`) never trips the join guard at all. *Fix:* `anyNA`
on the joined scalar stops. This is the same defect the workforce runner already
documents for scalars that were "accepted, stored and CHECKED but never
APPLIED".

**Not a defect.** Two of the three initial test failures were mine: a fixture
whose ratio arithmetic was wrong (BVA 2) and an expectation written against the
pre-fix contract (semantic 3). Corrected in the fixture, not in the tolerance.

**Result:** 46 assertions, all passing. **4 defects found, 4 fixed.**

**Bug class to sweep in a later cycle:** guards that only *warn* in relaxed mode
while the code downstream treats them as having *stopped* — `safe_left_join(min_match_rate=)`
was one; look for other `mode`-gated checks whose caller has no fallback branch.

**Related files rerun:** `test-urps-flows.R`, `test-provider-lifecycle.R`,
`test-provider-state-machine.R`, `test-demand-and-validation.R`,
`test-hours-uncertainty-propagation.R`, `test-scientific-benchmarks.R`,
`test-workforce-microsimulation.R`, `test-boundary-values.R`,
`test-adversarial-cycle01.R` — all green under the four fixes.

**Pre-existing failure, not touched:** `test-canonical-overlap.R:49` reports five
stale registry rows (`e2sfca_band_weights`, `e2sfca_incremental_weights`,
`gaussian_band_weights`, `haversine_km`, `isTRUE_vec`, all vs `isochrones`).
Stale means the collision is gone on the SIBLING side; this cycle exported no
new names and removed none, so the cause is the state of the `~/isochrones`
clone on this machine.

---

## Cycle 03 — 2026-08-09

**Mix:** 3 BVA · 3 semantic · 4 adversarial → `tests/testthat/test-adversarial-cycle03.R`

**Targets and why.** Discharged the class cycle 02 carried forward (guards that
only warn in relaxed mode while the caller has no fallback), then denominators,
joins and uncertainty propagation.

| # | cat | target | assumption challenged |
|---|---|---|---|
| 1 | BVA | `provider_concentration` | the unit universe may equal but never fall below the occupied units |
| 2 | BVA | `workforce_top_k_share` | closed at `k = 0`, saturates at `k >= n`, monotone in `k` |
| 3 | BVA | `monte_carlo_se`, `series_mean_se` | MCSE falls as 1/√n; undefined below 2 draws; median/mean ratio is exactly √(π/2) |
| 4 | semantic | concentration family | no metric accepts a negative count |
| 5 | semantic | `supply_per_capita` | density is linear in `per`, undefined (not Inf) at zero population |
| 6 | semantic | 4 concentration metrics | they order two distributions the same way; padding zeros cannot lower Gini and cannot move HHI |
| 7 | adversarial | `as_urps_gap_projection` | a half-covered demand series is refused |
| 8 | adversarial | `validate_urps_gap_projection` | a missing gap is never a gap of zero |
| 9 | adversarial | `safe_left_join` denominators | a duplicated population row is refused, and the guard is not vacuous |
| 10 | adversarial | `monte_carlo_diagnostics` | a degenerate band is not reported as precision |

### Defects found and fixed — 3 (plus one blind spot recorded)

**D5 · the NA gap that validated clean.** `as_urps_gap_projection()` joins the
demand series to the supply years at `min_match_rate = 0.5`. A demand series
covering exactly half the horizon matches at 0.5, which is *not below* 0.5, so
the join emitted **no diagnostic at all** — and the other half of the projection
exported `NA` demand and `NA` gap. `validate_urps_gap_projection(mode="strict")`
passed it: the arithmetic guard uses `na.rm = TRUE`, so `NA - NA` held
vacuously, and nothing else looked at NA. Verified end to end before the fix:
3 of 6 years NA, validator returned clean. *Fix:* a completeness guard ahead of
the arithmetic guard — non-finite values in any of the six contract numeric
columns stop in strict mode and warn in relaxed. This is the class cycle 02
carried forward, in its sharpest form.

**D6 · denominator smaller than its numerator.** `provider_concentration()`
accepted `n_units_total < n_occupied`, returning `pct_units_zero = -100` — a
negative share of empty units — while computing Gini and HHI over *more* units
than `n_units` reported. **This is a port regression:** the canonical
`cliff::concentration_summary()` carries exactly this guard and the port dropped
it. Found by comparing against the canonical source rather than reasoning from
scratch. *Fix:* guard restored, message matched to canonical.

**D7 · negative counts refused in one of four siblings.** `workforce_gini()`
stopped on a negative count; `workforce_hhi()`, `workforce_lorenz()` and
`workforce_top_k_share()` did not, and returned values outside their own
documented `[0, 1]` ranges — a top-k share of **1.2** and a Lorenz curve running
to **-1**. A caller running two of them on the same data got an error from one
and a confident number from the other. *Fix:* one shared
`.assert_nonneg_counts()` across the family, plus a `k >= 0` check.
**Deliberate divergence from canonical**, documented in the source: cliff's
`herfindahl_index()` documents dropping negatives, and this port inherited it.

### Blind spot recorded, not repaired

`test-export-wiring.R` defines "wired" as a bare textual mention outside the
definition line — and a **string literal counts**. Giving three previously
silent functions a self-naming input guard therefore made them read as wired,
which turned their `tests/export-registry.csv` rows stale and failed the gate.
Measured the alternative before acting: stripping string literals from the
haystack takes the orphan list from 53 to **106**, i.e. 53 exports are currently
counted as wired on the strength of their own error messages alone. Triaging
53 exports into api/unwired_gate/dormant is the work, not a side effect of a
test cycle. The three rows were removed (the gate's own contract calls them
stale) and the blind spot is recorded in the registry header so the file's
shrinking is not misread as debt repaid.

**Wrong tests, corrected in the test:** `expect_warning` for `safe_left_join`'s
match-rate guard — it emits a `.msg_warn()` *message*, not an R warning, so it
never reaches `warnings()` and cannot be promoted with `options(warn = 2)`.
Pinned as `expect_message` with that noted. Also `expect_lt(info=)`, which
`testthat` does not accept.

**Result:** 59 assertions, all passing. **3 defects found, 3 fixed.**

**Related files rerun:** `test-workforce-concentration.R` (20),
`test-monte-carlo.R` (32), `test-geographic-demand.R` (26),
`test-export-wiring.R` (10), `test-orchestrator-wiring.R` (56),
`test-demand-and-validation.R` (150), `test-adversarial-cycle01.R` (25),
`test-adversarial-cycle02.R` (46), `test-boundary-values.R` (81) — all green.

**Bug class to sweep in a later cycle:** thresholds written as strict
inequalities where the boundary case is the dangerous one — `min_match_rate`
compared with `<` meant an exactly-50% match passed in silence. Look for other
`<` / `>` guards whose equality case is the failure.

---

## Cycle 04 — 2026-08-09

**Mix:** 4 BVA · 3 semantic · 3 adversarial → `tests/testthat/test-adversarial-cycle04.R`

**Targets and why.** Discharged the class cycle 03 carried forward (thresholds
written as strict inequalities where the boundary is the dangerous case) by
walking the cutpoints of every classification and range guard on the FTE path,
then FTE-vs-headcount semantics and entrant/departure accounting.

| # | cat | target | assumption challenged |
|---|---|---|---|
| 1 | BVA | `classify_workforce_outlook` | cutpoints closed from below; a ratio of exactly 1 is Marginal, not Adequate |
| 2 | BVA | `check_productivity_plausible` | benchmark range closed at both ends; relaxed mode returns FALSE and says so |
| 3 | BVA | `calibrate_wrvu_per_fte` | indirect time closed at 0, open at 1; a zero anchor is refused, not divided by |
| 4 | BVA | `allocate_fte_by_setting` | unit-interval check and the 1e-8 sum tolerance |
| 5 | semantic | five sum-to-one validators | a distribution summing to 1 with a negative part is not a distribution |
| 6 | semantic | `convert_workload_to_fte` | required FTE linear in volume, inverse in productivity, grossed up by exactly 1/(1−indirect) |
| 7 | semantic | `compute_fte_gap` | the gap is an identity; sign of the percentage agrees with the level; a year with no demand is refused |
| 8 | adversarial | `calibrate_wrvu_per_fte` → `convert_workload_to_fte` | the solved denominator round-trips to its own anchor at any indirect share |
| 9 | adversarial | `simulate_provider_career_once` | headcount moves by exactly entrants minus departures; retirement is absorbing |
| 10 | adversarial | `allocate_fte_by_setting` | a partition neither creates nor destroys FTE |

### Defects found and fixed — 3, one family

**D8 · sum-to-one without non-negativity (3 of 6 validators).** The codebase
already records the reasoning, in `validate_migration_matrix()`: *"-0.1 and 1.1
sum to exactly 1.0, so a row-sum test alone accepts a matrix that is not a
probability distribution."* Three siblings had not adopted it.

* `allocate_fte_by_setting()` — the live one. Shares of `1.5 / -0.5` passed the
  sum check and emitted `required_fte = -50` for a setting. Negative clinical
  FTE subtracts from any total it is summed into, so a by-setting demand table
  and a national one would disagree.
* `validate_cpt_basket()` — a negative mix weight subtracts that CPT's work
  RVUs from its service, understating the workload the service represents.
* `psa_discrete()` — a negative probability mass makes the inverse-CDF
  cumulative non-monotone, so a category is drawn never or always depending on
  where the fold lands.

*Fix:* range check before the sum check in all three, each carrying a pointer
to the migration-matrix precedent. `validate_delegation_matrix()`,
`validate_setting_mix()` and `validate_participation_table()` already had it and
are now pinned so they cannot lose it.

**Nothing else found.** The boundary sweep this cycle was otherwise clean: the
outlook cutpoints, the productivity benchmark range, the indirect-time interval
and the sum tolerance all behave exactly as documented, and the conservation
identity (`Δheadcount = entrants − departures`) holds per year with retirement
absorbing. The productivity denominator round-trips to its anchor to 1e-8 at
both a 0% and a 25% indirect share.

**Result:** 59 assertions, all passing. **3 defects found, 3 fixed.**

**Related files rerun:** `test-workload-to-fte.R` (76), `test-psa.R` (28),
`test-psa-adversarial.R` (31), `test-psa-reporting.R` (8),
`test-urps-settings.R` (43), `test-provider-lifecycle.R` (56),
`test-workforce-microsimulation.R` (58), `test-supply-microsim-adversarial.R` (20),
`test-urps-migration.R` (46), plus cycles 01-03 — 496 assertions, 0 problems.

**Bug class to sweep in a later cycle:** cumulative counters read as if they
were per-period rates (`n_retired` is cumulative and only its `diff()` is a
departure count) — look for any place a running total is compared with, divided
by, or summed alongside a flow.

---

## Cycle 05 — 2026-08-09

**Mix:** 3 BVA · 4 semantic · 3 adversarial → `tests/testthat/test-adversarial-cycle05.R`

**Targets and why.** Discharged the class cycle 04 carried forward (cumulative
counters read as per-period rates) on the one function that takes a stock and a
flow as separate arguments, then the aging recurrence and forecast probabilities.

| # | cat | target | assumption challenged |
|---|---|---|---|
| 1 | BVA | `prevalence_from_incidence` | one-age and zero-age grids |
| 2 | BVA | `prevalence_from_incidence` | every argument closed on [0, 1] |
| 3 | BVA | `forecast_probabilities` | interval mass open at 0 and 1; exceedance is strict, so ties belong to neither tail |
| 4 | semantic | the DisMod recurrence | geometric decay with no onset; non-decreasing with no remission; the full-remission fixed point p\* = i/(1+i) |
| 5 | semantic | `prevalence_from_onset` | the alias is the same object, and inherits the guards |
| 6 | semantic | `forecast_probabilities` | exceedance monotone in the threshold; `n` counts what was summarised, `n_na` what was dropped |
| 7 | semantic | `forecast_probabilities` | the interval brackets the median and widens with its mass |
| 8 | adversarial | `entrant_regime_rolling_validation` | a cumulative stock and an annual flow are not interchangeable |
| 9 | adversarial | `entrant_regime_rolling_validation` | a duplicated year is skipped, not recycled into the quantiles |
| 10 | adversarial | `prevalence_from_incidence` | no admissible input escapes [0, 1] (fuzzed over the corner of the space) |

### Defects found and fixed — 2, in one function

**D9 · negative prevalence.** `prevalence_from_incidence()` validated nothing.
The recurrence `p[i] = p[i-1](1-r) + (1-p[i-1])i[i-1]` has no restoring force,
so a remission above 1 drives prevalence **negative** — measured
`0.40 → -0.20 → 0.10 → -0.05` — and an incidence above 1 drives it past 1
(`1.5`, `1.125`). Every argument is a probability and none was checked. A
negative prevalence becomes a negative case count in every downstream demand
total. **This is the third module with the same class:** cycle 03 found it as
negative provider counts, cycle 04 as sum-to-one validators without a range
check. *Fix:* all three arguments checked finite and in [0, 1], refused rather
than clamped — clamping would hide a mis-specified remission behind a
plausible-looking curve.

**D10 · the descending loop.** `for (i in 2:length(incidence))` counts **down**
at length 1, so a single-age grid ran the body at `i = 2` (growing `p` to
length 2), then at `i = 1` where `p[0]` is `numeric(0)`, and died with R's
"replacement has length zero". A legitimate degenerate input crashing on an
index, wearing the costume of an input error. *Fix:* `seq_len(n)[-1]`, plus an
explicit `numeric(0)` return at length 0. Swept the class: this was the **only**
`2:length()` / `1:length()` construction in `R/`.

**Wrong test, corrected in the test.** I asserted that full remission makes
prevalence equal last year's incidence. It does not — only the `(1 - P)`
susceptible fraction can acquire, so one step of history survives. The
expectation now pins the exact recurrence and its fixed point `p* = i/(1+i)`,
which is a stronger claim than the one I got wrong.

**Result:** 92 assertions, all passing. **2 defects found, 2 fixed.**

**Related files rerun:** `test-calibrate-prevalence-to-incidence.R` (15),
`test-forecast-probabilities.R` (18), `test-forecast-scorecard.R` (20),
`test-entrant-regime.R` (69), `test-entrant-precedence.R` (17),
`test-entrant-trajectory.R` (33), the four `test-demand-lifecourse*` files (48),
`test-adversarial-cycle04.R` (59) — 279 assertions, 0 problems.

**Bug class to sweep in a later cycle:** the range-check class has now appeared
in three separate modules (counts, shares, probabilities). Next sweep should be
systematic rather than opportunistic — enumerate every exported argument
documented as a probability, share, rate or count and check each has a range
guard, instead of waiting to trip over the next one.

---

## Cycle 06 — 2026-08-10

**Mix:** 3 BVA · 3 semantic · 4 adversarial → `tests/testthat/test-adversarial-cycle06.R`

**Targets and why.** Discharged the class cycle 05 carried forward, and did it
the way that cycle asked for: **systematically rather than opportunistically.**
Cycles 03, 04 and 05 each found the same class in a different module by tripping
over it. This cycle enumerated the surface first.

### The sweep

Parsed every roxygen `@param` in `R/` whose text names a probability, share,
fraction, rate, proportion, percent or hazard, restricted to exported
functions, and checked each for a range guard — then **probed the
high-consequence ones directly**, because the static heuristic has false
positives in both directions (it missed guards written as a local `chk()`
closure or a multi-line `assert_that`, and flagged parameters that are not
really rates).

**The sweep's main finding is that most of this surface is already guarded.**
`conservative_management_multipliers()`, `urps_migration_matrix()`,
`telemedicine_reach()`, `clear_access_trajectory()` and
`compute_namcs_demand_estimand()` all refuse out-of-range input. Test 8 pins
that group as a set, so a refactor that drops any of them fails in one place.

Three did not, and all three are multipliers that scale the headline demand
estimate directly.

| # | cat | target | assumption challenged |
|---|---|---|---|
| 1 | BVA | `project_urps_demand` | care-seeking and referral fractions closed on [0, 1] |
| 2 | BVA | `compute_demand_denominators` | a consultation rate is bounded below only; a per-1,000 rate is bounded by 1,000 |
| 3 | BVA | `.assert_in_range` | inclusive at both ends, names its caller and the offending value |
| 4 | semantic | `project_urps_demand` | demand scales with the exact PRODUCT of the two fractions; swapping them cannot move it |
| 5 | semantic | D1/D2/D3 | distinct estimands, linear in population, strictly ordered |
| 6 | semantic | `compute_demand_denominators` | an unknown age band stops; a legitimately absent band reduces demand |
| 7 | adversarial | the referral cascade | every stage removes women, so referrals never exceed the prevalent pool |
| 8 | adversarial | five already-guarded functions | the existing guards cannot be lost |
| 9 | adversarial | all three new guards | out of range is refused, never clamped |
| 10 | adversarial | `compute_demand_denominators` | a negative rate can no longer produce a negative case count |

### Defects found and fixed — 3

**D11 · negative demand cases.** `compute_demand_denominators()` validated the
age bands but not the rates. Measured: `consult_rate = -0.3` over a 5,000,000
woman population returned **D2 = −1,500,000 demand cases**, silently — a
negative number of consultations, carried into any downstream total that sums
the estimands without checking their sign. *Fix:* `consult_rate` finite and
≥ 0 (a woman may consult more than once a year, so there is no upper bound);
`surgery_rate_per_1000` finite and in [0, 1000], above which the model operates
on more women than exist.

**D12 · `compute_brfss_demand_estimand(care_seeking_rate, referral_rate)`** —
both documented as fractions, both multiplying the population directly, neither
checked. Above 1, more women reach a urogynaecologist than have the condition.

**D13 · `project_urps_demand(care_seeking_rate, referral_rate)`** — the same
pair on the other estimand, equally unguarded.

*Shared fix:* `.assert_in_range()` in `R/core-canonical_and_joins.R`, beside
`.recycle_aligned()`, so the fifth module inherits the rule instead of
rediscovering it. **Refused, never clamped:** a referral rate of 1.4 silently
becoming 1.0 would produce a plausible number from an impossible assumption and
report no problem. Test 9 exists to pin that distinction.

**Worth recording:** every insurance and income care-seeking multiplier is ≤ 1
(`Insured 1.00 / Uninsured 0.58 / Unknown 0.80`; `LT25k 0.72 … GT100k 1.00`),
which is what makes the cascade a genuine filter. A multiplier above 1 would let
the *effective* care-seeking rate exceed the rate the caller asked for, and the
new upper bound on the caller's argument would not catch it. Test 7 asserts the
filter property on the output, not on the constants, so it holds either way.

**Result:** 73 assertions, all passing. **3 defects found, 3 fixed.**
**Bug class closed** — this was the systematic pass the previous three cycles
kept deferring.

**Related files rerun:** `test-urps-population.R` (41), `test-access-clearing.R`
(37), `test-access-clearing-trajectory.R` (17), `test-telemedicine-reach.R` (14),
`test-namcs-demand-calibration.R` (27), `test-urps-prevention.R` (50),
`test-demand-and-validation.R` (150), `test-adversarial-cycle03.R` (59),
`test-adversarial-cycle05.R` (92) — 487 assertions, 0 problems.

**Bug class to sweep in a later cycle:** the fixture-shape assumption. Four of
this cycle's ten tests initially failed because I invented a helper that does
not exist (`example_population_cells()`) and a column that is not returned
(`total_visits`). None was a defect — but a test written against a
mis-remembered interface can also PASS for the wrong reason. Next cycle should
check whether any existing test asserts on a column the function does not
actually populate.

---

## Cycle 07 — 2026-08-10

**Mix:** 4 BVA · 3 semantic · 3 adversarial → `tests/testthat/test-adversarial-cycle07.R`

**Targets and why.** `R/geography-spatial_access_e2sfca.R` is a port of
`twostep`'s floating-catchment module and had never been tested against its
canonical source. Cycle 03 found a port regression that way (cliff's
concentration guard); the same diff against `~/twostep` found **four** dropped
guards here. Two of them produce a negative or inflated access surface rather
than an error.

| # | cat | target | assumption challenged |
|---|---|---|---|
| 1 | BVA | `e2sfca_band_weights` | monotone non-increasing, with equality legal and a 1e-9 tolerance |
| 2 | BVA | weights / `step2_power` | weights closed at 0; the power closed at 1 |
| 3 | BVA | `e2sfca_incremental_weights` | a single band does not trip the descending range |
| 4 | BVA | M2SFCA | closed at a cumulative weight of exactly 1 |
| 5 | semantic | incremental weights | they telescope: `sum(incr[b:n]) == W_b`, and M2SFCA telescopes to `W_b^2` |
| 6 | semantic | M2SFCA vs E2SFCA | squaring shifts share toward the nearest band and away from the outermost |
| 7 | semantic | `compute_e2sfca_access` | zero weighted demand is an UNDEFINED ratio, not zero access |
| 8 | adversarial | non-monotone weights | can no longer produce a negative demand weight |
| 9 | adversarial | band labels | names that are not minutes are refused, not arbitrarily reordered |
| 10 | adversarial | membership bands | a band outside the weight table stops the run, on both the E2SFCA and M2SFCA paths |

### Defects found and fixed — 4, all port regressions

**D14 · negative demand weight.** `c("30" = 0.5, "60" = 1.0)` produced an
incremental weight of **−0.5** for the 30-minute band. A negative demand weight
*subtracts* population from a provider's catchment, inflating its
supply-to-demand ratio and its access contribution. The port only warned — and
warned through `.msg_warn()`, a **message**, so it never reached `warnings()`
and could not be promoted with `options(warn = 2)`. Canonical `twostep` stops.

**D15 · M2SFCA bonus instead of penalty.** `c("30" = 1.5, "60" = 0.6)` at
`step2_power = 2` gave an incremental weight of **1.89**: squaring a cumulative
weight above 1 *increases* it, so the Delamater penalty becomes a bonus.
Canonical carries this exact guard with this exact rationale. Previously
recorded in `tests/canonical-overlap-registry.csv` as "Follow-up 1" and never
actioned.

**D16 · negative and non-finite weights** accepted outright; canonical uses
`checkmate::assert_numeric(lower = 0, any.missing = FALSE)`.

**D17 · band labels that are not minutes.** `order(as.numeric(c("near","far")))`
is `order(c(NA, NA))` — an arbitrary order — so weights were attached to
whichever band came out first. The only signal was R's own "NAs introduced by
coercion", which says nothing about access. Canonical stops.

**Also brought across:** `step2_power >= 1` (at 0.5 the incremental weights are
non-monotone — the 60-minute band gets 0.356 against the 30-minute band's
0.175, inverting the decay the method exists to encode), and canonical's
float-error clamp. The `wp[2:n]` indexing was replaced with `c(wp[-1], 0)`:
at `n = 1` the former is the descending range `c(2, 1)`, the same trap cycle 05
found live in the aging recurrence, masked here only by an empty assignment
index.

**Registry corrected.** `tests/canonical-overlap-registry.csv` rows 69 and 71
described both functions as `ported_weaker` with the exact text "here it only
warns" and "Follow-up 1". Those descriptions became false the moment the gap
closed, so both are reclassified `equivalent` with the change recorded. Leaving
them would have made the registry lie in the safe-looking direction.

**Result:** 54 assertions, all passing. **4 defects found, 4 fixed.**

**Related files rerun:** the eight `test-access-*` files (193),
`test-real-spatial-access.R` (15), `test-urps-access-anchors.R` (30),
`test-geographic-demand.R` (26), `test-geographic-holdout.R` (17),
`test-workforce-microsimulation.R` (58), `test-adversarial-cycle06.R` (73) —
0 problems.

**Pre-existing failure, unchanged:** `test-canonical-overlap.R:49`, the same
five stale rows against `isochrones` recorded in cycle 03. Not this cycle's,
and not affected by the two rows corrected above (those are `twostep` rows).

**Carried-forward class — sweep IN PROGRESS, not yet reported.** Cycle 06
carried forward test *vacuity*: an expectation that passes because its subject
is empty. `expect_true(all(x)))` is TRUE when `x` has length zero, so a test
asserting on a column the function never populates passes for the wrong reason.
Rather than guess statically, this cycle instrumented `all()` and `any()` and
re-ran **every** test file under a shadowing environment that records
zero-length invocations. The run was still going when the cycle closed; its
result is reported in cycle 08 rather than summarised early.
