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

---

## Cycle 08 — 2026-08-10

**Mix:** 3 BVA · 4 semantic · 3 adversarial → `tests/testthat/test-adversarial-cycle08.R`

**Targets and why.** The two thinnest priorities in this ledger: RNG state had
one test (cycle 01) and validation leakage two (cycle 02). Both are places where
a run can be wrong while every number in it looks ordinary — an irreproducible
run labelled reproducible, or a spec that passes a preregistration gate it does
not match.

### Cycle 07's carried-forward sweep — result

Instrumented `all()` and `any()` and re-ran **every** test file under a
shadowing environment recording zero-length invocations. Across the whole suite:
**2 vacuous assertions.** The worry was largely unfounded, which is the useful
part of the answer. Both survivors were real and are repaired in their own
files, not here.

* `test-numeric-guards.R` — the helper ended in `all(is.na(v) | is.finite(v))`
  over every numeric leaf of the result. For `assign_entrant_geography(0, ...)`
  there are **no** numeric leaves, so the "no public numeric path emits Inf or
  NaN" guard returned TRUE having inspected nothing. Replaced with
  `.finite_status()`, which returns `guarded` / `finite` / `nonfinite` /
  `no_numbers`; each case now declares which outcome it is evidence for, and a
  further assertion requires that at least four cases actually inspected numbers.
* `test-demand-dynamic-open.R:171` — `expect_false(any(grepl("Population
  conservation", msgs)))` ran on an **empty** `msgs`. The test's own comment
  justified the design by saying "the run also emits the exploratory-transitions
  declaration" — but the file's `sim_open()` helper *muffles* exactly that
  message, so nothing was captured and the assertion passed for the reason the
  comment says it must not. Now calls `simulate_dmdm_open()` directly and
  asserts the declaration IS present before asserting the conservation message
  is not.

| # | cat | target | assumption challenged |
|---|---|---|---|
| 1 | BVA | `seed_microsimulation` | an explicit seed always beats the environment; 0, 1, −1, INT_MAX are legal |
| 2 | BVA | `seed_microsimulation` | unset vs empty vs whitespace-padded |
| 3 | BVA | `.canonicalize_spec_v2` | a value and its own rendering hash differently; key order still irrelevant |
| 4 | semantic | `seed_microsimulation` | the RNG **kind** is pinned with the seed, not just the seed |
| 5 | semantic | relaxed mode | the unseeded path requires an explicit NA and announces itself |
| 6 | semantic | `make_run_id` | deterministic in strict, time-stamped in relaxed, and never mislabels |
| 7 | semantic | the frozen v1 record | still verifies under its own declared version |
| 8 | adversarial | `MICROSIM_SEED` | malformed is refused, not silently replaced |
| 9 | adversarial | `assert_spec_matches_prereg` | a spec cannot pass by impersonating the registered one |
| 10 | adversarial | version dispatch | re-registering an unchanged v1 spec is not mistaken for a change |

### Defects found and fixed — 2

**D18 · a seed nobody chose.** `MICROSIM_SEED` was parsed with
`suppressWarnings(as.integer(...))`, and the two modes then failed in opposite
unhelpful directions. Measured: `MICROSIM_SEED=twenty` in **strict** mode
returned **20260801** — the default — so the run was reproducible, but not the
run anyone asked for; in **relaxed** mode it returned **NA** and left the RNG
entirely unseeded, so the run was not reproducible at all. Either way a caller
set a seed and did not get it, with no diagnostic.

*The first fix was incomplete, and the test caught it.* Refusing only `NA`
still admitted `MICROSIM_SEED=3.7`, because `as.integer("3.7")` is `3` —
silent truncation to a *different valid seed*, the very substitution the guard
exists to stop. Now parsed as a double and checked for integrality and integer
range. `0x1F` and `1e3` are unambiguous integer spellings R accepts and are
explicitly still allowed (that expectation of mine was wrong and was corrected
in the test).

**D19 · preregistration hash collisions.** `.canonicalize_spec()` collapses
type. Measured, all four hashing identically:

```
list(a = list(b = 1))  ==  list(a = "{b=1}")        nested vs its own rendering
list(a = c(1, 2))      ==  list(a = "1,2")          vector vs its own rendering
list(a = TRUE)         ==  list(a = "TRUE")
list(a = 1/3)          ==  list(a = 0.333333333333333)   (format digits = 15)
```

The module's entire claim is that `assert_spec_matches_prereg()` cannot be
satisfied by anything other than the frozen spec, because "changing the
specification after preregistration is model selection on the held-out data".
A collision is exactly a way to satisfy it with a different spec.

*Fix, and the constraint on it:* `inst/extdata/preregistration/urps_pipeline_forecast_2024_2026.txt`
is a **real frozen record**, dated 2026-08-07, made while `board_certified_active`
still ended at 2023. Silently changing the hash function under it would be the
same offence the module exists to prevent, committed by the guard itself. So the
canonicalisation is **versioned**: records declare `prereg_version`, v1 records
are verified with v1 (and say so out loud when they are), and new records are
written under v2, which tags every leaf with its type and length. Test 10 exists
because the dispatch has its own failure mode — verifying a v1 record with the
v2 hash would make an *unchanged* spec look changed, and a guard that cries wolf
gets switched off.

**Ruled out.** Whether a `notes` field could forge `spec_hash` by injecting a
second `spec_hash:` line: `preregister_spec()` already strips newlines from
`notes`, and `$` on a duplicated key returns the first (real) one. Not a defect.

**Result:** 55 assertions, all passing. **2 defects found, 2 fixed**, plus 2
vacuous assertions repaired.

**Related files rerun:** `test-numeric-guards.R` (20),
`test-demand-dynamic-open.R` (32), `test-preregistration.R` (21),
`test-orchestrator-wiring.R` (56), `test-workforce-microsimulation.R` (58),
`test-backtest.R` (106), `test-adversarial-cycle01.R` (25),
`test-adversarial-cycle07.R` (54) — 372 assertions, 0 problems.

**Bug class to sweep in a later cycle:** silent coercion at a trust boundary.
`as.integer()` on a string was the instance here; look for `as.integer`,
`as.numeric` and `match.arg`-free string dispatch applied to anything arriving
from an environment variable, a file, or a user-supplied column, where a
truncation or an NA changes a value rather than rejecting it.

---

## Cycle 09 — 2026-08-10

**Mix:** 3 BVA · 3 semantic · 4 adversarial → `tests/testthat/test-adversarial-cycle09.R`

**Targets and why.** Cycle 08 carried forward silent coercion at a trust
boundary — `as.integer()`/`as.numeric()` on anything arriving from an
environment variable, a file or a user column, where a truncation or an NA
changes a value rather than rejecting it. The sweep found the sharpest instance
at the engine's own front door: `years`.

| # | cat | target | assumption challenged |
|---|---|---|---|
| 1 | BVA | `simulate_provider_career_once` | one year legal, a two-year gap refused, message names the horizon meant |
| 2 | BVA | `years` coercion | whole years only; empty/NA/unparseable stop |
| 3 | BVA | `project_supply_deterministic` | the same guard on the second entry point |
| 4 | semantic | panel year vs cohort age | they advance in lockstep over the whole horizon |
| 5 | semantic | horizon length | a longer horizon extends the panel, it does not rescale the dynamics |
| 6 | semantic | duplicated / unsorted years | normalised, because neither changes the number of steps |
| 7 | adversarial | a gapped horizon | can no longer report one step as five years |
| 8 | adversarial | any horizon shape | rows never outnumber steps |
| 9 | adversarial | `resolve_reproducibility_mode` | the permissive fallback is documented, pinned, and its exposure recorded |
| 10 | adversarial | the guard itself | not satisfiable by a horizon that merely looks contiguous |

### Defect found and fixed — 1, and it is the worst kind

**D20 · a horizon with a gap silently skipped four years of dynamics.**
Every step of the engine advances age by exactly one year
(`v_age[live] <- v_age[live] + 1`), so `years` is not a set of labels to report
against — it is *the number of one-year steps to take*. The front door was
`years <- sort(unique(as.integer(years)))`, which accepted anything.

Measured, before the fix:

```
simulate_provider_career_once(agents, c(2025, 2030), entrants = 5)
  year headcount mean_age
  2025        30     50.0
  2030        35     48.6     <- ONE year of aging, ONE year of entrants
```

Row 2 is labelled **2030** and has had a single step applied. Five entrants
instead of twenty-five; one year of aging instead of five. No error, no
warning, and every column individually plausible. This is the failure mode the
whole loop is looking for: a confident wrong number with a correct-looking
label.

*Fix:* `.check_projection_years()` on both engine entry points
(`simulate_provider_career_once` and its expected-value twin
`project_supply_deterministic`, which carried the identical line). It refuses
non-consecutive years, fractional years (`as.integer(2025.7)` was silently
`2025`), and empty/NA horizons — the last of which previously produced
`min() -> Inf` with a base R warning and an **empty panel** rather than a
failure. Deduplication and re-sorting are kept, because neither changes the
number of steps; test 6 pins that so the new guard cannot over-reach.

Every caller in the package already builds `years` as `a:b`. The invariant was
real and unstated; it is stated now.

### Recorded, not changed

`resolve_reproducibility_mode()` documents that an unrecognised value "warns and
falls back rather than failing, so a typo degrades to the permissive mode". That
is a deliberate, documented decision and test 9 pins it rather than overriding
it. Worth stating plainly, though: `REPRODUCIBILITY_MODE=strct` on a
publication run silently yields **relaxed**, and the only signal is a
`.msg_warn()` message — which never reaches `warnings()` and cannot be promoted
with `options(warn = 2)`. The exposure is now visible in a test rather than
implicit in a doc paragraph.

**Result:** 53 assertions, all passing. **1 defect found, 1 fixed.**

**Related files rerun:** `test-workforce-microsimulation.R` (58),
`test-supply-microsim-adversarial.R` (20), `test-orchestrator-wiring.R` (56),
`test-provider-state-machine.R` (25), `test-backtest.R` (106),
`test-entrant-trajectory.R` (33), and cycles 01/04/08 (139) — 437 assertions,
0 problems.

**Full suite:** launched at the close of this cycle; result reported in cycle 10
rather than summarised early.

**Bug class to sweep in a later cycle:** unstated invariants shared by two
entry points. `project_supply_deterministic()` carried the identical unguarded
line as the stochastic engine, and a guard on one of two doors is not a guard.
Look for other pairs — deterministic/stochastic twins, `*_once` vs `*_many`,
scenario vs baseline paths — where one has a check the other lacks.

---

## Cycle 10 — 2026-08-10

**Mix:** 4 BVA · 3 semantic · 3 adversarial → `tests/testthat/test-adversarial-cycle10.R`

### Full suite (launched at the close of cycle 09)

**3,943 passed · 5 failed · 0 errors · 31 skipped · 121 files.** Three failures
are the known pre-existing ones (`test-canonical-overlap.R` stale `isochrones`
rows; two `test-practice-survey.R` `access_ascertainment` failures). Two were
mine, and both are resolved here.

**Mine, trivial (fixed in `ef9679a`).** `test-export-wiring.R`: passing
`"project_supply_deterministic"` as a literal to cycle 09's new guard made the
orphan detector count the function as *wired*, the blind spot documented in the
registry header in cycle 03. Switched to the `sys.call(-1)` idiom already used
by `.recycle_aligned()` and `.assert_in_range()`.

**Mine, substantive — and I got the first diagnosis wrong.**
`test-adversarial-guards.R:140` ("effective FTE never exceeds headcount")
failed at HEAD, passed at pre-cycle-01. Bisected to cycle 03 — which touched no
FTE code at all. The test has **no `set.seed()`**, so cycle 03's new guards
shifted RNG consumption in an earlier test in the same file, and the new stream
surfaced something always reachable: 8 of 40 seeds exceed 1.0, worst ratio
1.00607.

I first reported this as "a real defect: supply in FTE can exceed supply in
heads". Reading the contract properly says otherwise. Two places in the repo
disagree:

* `supply-provider_microsimulation.R`: *"More FTE than people ... is
  dimensionally impossible under an hours-threshold FTE definition, so strict
  mode refuses it outright."*
* `calibrate_hours_intercept()`: *"...so base-year FTE tracks headcount and
  **all subsequent movement comes from the changing age and sex composition**."*

The second is what the model does. FTE is `hours / threshold` with no cap;
calibration solves the intercept so the **base cohort** averages exactly 1.0,
and entrants arriving at `MICROSIM_ENTRY_AGE` sit above that mean. Drift is the
intended output. And the measured 1.00607 is **inside the engine's own 1.02
tolerance**. So the assertion was stricter than the contract: a **wrong test**,
passing on luck of the ambient RNG stream.

Corrected in both places it appears — `test-adversarial-guards.R` and my own
cycle 01 test 6, which made the identical over-strong claim. Both now seed
explicitly and assert what is true: base year tracks headcount to 1e-6, drift
stays within `FTE_PER_HEAD_TOLERANCE`.

### Defect found and fixed — 1

**D21 · the dimensional guard ran on the base cohort only.** It computed mean
FTE over `base_agents`. Because `calibrate_hours_intercept()` makes that mean
exactly 1.0 *by construction*, the guard passed every time and never looked at
the projection. Measured with an older base cohort (ages 60–75, 25 entrants/yr):

```
base-year ratio 1.0000   ->   1.0783 by 2041
```

A 7.8% breach of the guard's own tolerance, in a **strict-mode** run, with
nothing firing. Exactly the class cycle 09 carried forward: a guard on one of
two doors. *Fix:* the same check applied to the projected panel, with the
tolerance hoisted to one shared `FTE_PER_HEAD_TOLERANCE` so the two cannot
drift apart.

**My first version of that fix was over-broad, and an existing test caught it.**
Written without a scope condition, it errored on
`test-hours-uncertainty-propagation.R` at a ratio of 1.156 — a run using a
**fitted** hours model, where the level comes from the fit rather than from an
intercept solved against the FTE threshold, so there is no calibration contract
to breach. The base-cohort guard already excludes that path
(`is.null(hours_model)`); the panel guard now does too.

| # | cat | target | assumption challenged |
|---|---|---|---|
| 1 | BVA | base-year ratio | exactly 1.0 for any cohort shape |
| 2 | BVA | `FTE_PER_HEAD_TOLERANCE` | one shared constant, above 1 and below 1.1 |
| 3 | BVA | single-provider cohort | the degenerate case is still exactly 1.0 |
| 4 | BVA | hours → FTE | exactly `hours / threshold`, no other scaling |
| 5 | semantic | composition drift | direction follows the base cohort's age |
| 6 | semantic | headcount vs FTE | one is a count, the other is not, neither derived from the other |
| 7 | semantic | `calibrate_hours_intercept` | it is a property of the cohort it was solved on |
| 8 | adversarial | the guard | covers the projection, not just the base cohort |
| 9 | adversarial | the guard | stays silent on a well-composed run (specificity) |
| 10 | adversarial | the ratio | holds under 25 independent seeds, not one lucky stream |

**Result:** 44 assertions, all passing. **1 defect found, 1 fixed; 2 over-strong
assertions corrected; 1 over-broad fix of my own caught by an existing test.**

**Related files rerun:** ten files, 423 assertions, 0 problems.

**Bug class to sweep in a later cycle:** unseeded stochastic tests. This whole
episode existed because `test-adversarial-guards.R:140` had no `set.seed()`, so
its verdict depended on which tests ran before it — it was neither reliably
passing nor reliably failing. Enumerate every test that calls a
`simulate_*`/`draw_*`/`run_*` function without seeding first, and decide each:
seed it, or assert a property that holds under every stream.

---

## Cycle 11 — 2026-08-10

**Mix:** 3 BVA · 4 semantic · 3 adversarial → `tests/testthat/test-adversarial-cycle11.R`

### Cycle 10's carried-forward class — measured, and closed

Parsing every test file (not grepping) found **79 `test_that` blocks that call a
stochastic function with no `set.seed()`**. Seeding all 79 blindly would be
busywork, so the question asked instead was the one that matters: *which
verdicts actually depend on the stream?*

Ran all 30 affected files under four different ambient seeds (1, 7, 101,
20260810) and compared pass/fail per test:

> **275 unique tests · verdicts that changed with the ambient seed: 0.**

(Measured against the tree as of cycle 10 — the sweep loaded the package before
this cycle's edits.) So after cycle 10 seeded the one genuinely stream-dependent
test, the suite is stream-stable. The 79 unseeded blocks assert properties that
hold under every stream, which is the second of the two acceptable answers the
class asked for. **Class closed, on evidence rather than on effort.**

But answering it pointed at the other end of the same problem.

### Defect found and fixed — 1, in six places

**D22 · functions that reseed the session as a side effect.** Eight functions in
`R/` call `set.seed()` internally, and `set.seed()` mutates **global** state. A
function that seeds for its own reproducibility silently reseeds the caller's
session: the run stays deterministic, it just stops being deterministic from the
seed anybody chose.

Measured: `geographic_holdout_cv(seed = 42)` changed the next three `runif()`
draws in the calling scope.

Worst of them: `supply-fraher_agent_supply.R` called **`set.seed(42L)`
unconditionally** in its synthetic-roster fallback. Any run that fell back to
synthetic data silently overrode the seed `seed_microsimulation()` had just
established, so every later draw in that run was a function of 42 rather than of
the run's declared seed — while the provenance record still reported the
declared one.

`calibration-psa.R` already saved and restored `.Random.seed` around its own
seeding, so the idiom was established in-repo.

> **Corrected in cycle 12.** This entry originally said "six siblings had not
> adopted it" and listed `geography-spatial_access_e2sfca.R` among them.
> `access_moe_ci()` in that file DID already carry a save/restore. The accurate
> count is **five** sites with no protection at all
> (`validation-geographic_holdout.R`, `supply-fraher_agent_supply.R`,
> `calibration-prevalence_to_incidence.R`, `demand-dynamic_multistate.R`,
> `demand-lifecourse.R`, `demand-lifecourse_uncertainty.R` — six functions
> across five files), plus **two** that carried a weaker form. See cycle 12.

*Fix:* `with_preserved_rng(seed, expr)` for the one site whose seeded region is
a self-contained block, and `.preserve_rng_scope()` — which installs the restore
as an `on.exit()` in the caller's frame, with the saved stream substituted into
the expression — for the sites whose seeded region is the whole body and would
otherwise need re-indenting. `seed_microsimulation()` is left alone and test 6
pins it as the **deliberate** exception, so "nothing reseeds the session" is not
a rule with a silent hole.

| # | cat | target | assumption challenged |
|---|---|---|---|
| 1 | BVA | `with_preserved_rng` | an ABSENT `.Random.seed` is restored as absent |
| 2 | BVA | a NULL seed | leaves the stream alone; the scope still restores |
| 3 | BVA | the scope | returns the expression's value; the seed takes effect inside |
| 4 | semantic | `geographic_holdout_cv` | reproducible folds without reseeding the caller |
| 5 | semantic | `loo` / `region` schemes | deterministic partitions consume no randomness at all |
| 6 | semantic | `seed_microsimulation` | the one function allowed to reseed the session |
| 7 | semantic | a run's declared seed | survives the functions called inside the run |
| 8 | adversarial | every seeded helper | none leaks its stream |
| 9 | adversarial | the scope | survives an error inside it (`on.exit`, not the success path) |
| 10 | adversarial | nested scopes | the outer stream is restored, not the inner one |

**Result:** 31 assertions, all passing. **1 defect found, 6 sites fixed.**

**Wrong test, corrected in the test:** I called `run_psa(n_draws = ...)`; the
argument is `n`. Same fixture-shape slip the ledger noted in cycle 06.

**Related files rerun:** `test-geographic-holdout.R` (17), `test-psa.R` (28),
`test-psa-adversarial.R` (31), `test-calibrate-prevalence-to-incidence.R` (15),
`test-demand-lifecourse.R` (12), `test-demand-lifecourse-uncertainty.R` (16),
`test-access-inference.R` (28), `test-38-fraher-agent-supply.R` (12),
`test-adversarial-cycle08.R` (55), `test-adversarial-cycle10.R` (44) — 258
assertions, 0 problems.

**Recorded, not changed:** with a mis-set intercept the base-cohort guard and
cycle 10's new panel guard now both report the same condition in the same run
(seen in the sweep logs as "Mean clinical FTE ... 1.104" followed by "Projected
FTE ... 1.104 in 2025"). Two true diagnostics for one real problem is noise, not
error; left as-is rather than adding suppression logic that could hide the
panel-only case the panel guard exists for.

**Bug class to sweep in a later cycle:** other *global* state mutated as a side
effect. `.Random.seed` was one; the same shape applies to `options()`,
`Sys.setenv()`, `par()`, working directory, and locale. Enumerate every write to
session-global state in `R/` and check each restores.

---

## Cycle 12 — 2026-08-10

**Mix:** 3 BVA · 3 semantic · 4 adversarial → `tests/testthat/test-adversarial-cycle12.R`

### Cycle 11's carried-forward class — swept, and it comes up clean

Searched `R/` for every write to session-global state: `options()`,
`Sys.setenv()`, `Sys.unsetenv()`, `setwd()`, `par()`, `Sys.setlocale()`, and
assignment into `globalenv()`.

> **The only session-global state this package writes is `.Random.seed`.**

No option, environment variable, working directory or locale is set anywhere in
`R/`. The `<<-` occurrences are closures accumulating into a local list, not
global writes. That is a genuinely good result, and test 4 pins it — "we do not
touch global options" is worth nothing unless something checks.

### Defect found and fixed — 1, and it corrects cycle 11's own record

**D23 · the established idiom could not restore an absent stream.** Two
functions already carried a hand-rolled save/restore:

```r
old <- if (exists(".Random.seed", ...)) get(...) else NULL
on.exit(if (!is.null(old)) assign(".Random.seed", old, ...))
```

In a **fresh session** there is nothing to restore, so `if (!is.null(old))` does
nothing and the seeded state is simply left behind. The function claims to be
RNG-neutral and is not: every later draw in that session becomes a function of
its internal seed. Measured — old form leaves `.Random.seed` present, the helper
removes it.

*Fix:* both consolidated onto `.preserve_rng_scope()`, which handles the absent
case. That removes the duplicated hand-rolled logic as well.

**This also corrects cycle 11.** That entry said six siblings "had not adopted"
the idiom and listed `geography-spatial_access_e2sfca.R` among them —
`access_moe_ci()` had adopted it, in the weaker form. Cycle 11's entry above is
annotated rather than rewritten. The accurate picture: **six functions with no
protection at all, and two with a form that had a hole in it.**

| # | cat | target | assumption challenged |
|---|---|---|---|
| 1 | BVA | three seeded entry points | a FRESH session is left unseeded |
| 2 | BVA | the restore | byte-exact, mid-stream, not merely "a plausible stream" |
| 3 | BVA | `RNGkind` | part of the state; only `seed_microsimulation()` may pin it |
| 4 | semantic | the whole package | writes no session-global state except the RNG |
| 5 | semantic | `resolve_reproducibility_mode` | reads the environment, never writes it |
| 6 | semantic | `psa_sample` | a pure function of (arguments, seed) |
| 7 | adversarial | the fresh-session hole | closed in both places that had it |
| 8 | adversarial | ten repeated calls | no accumulated drift |
| 9 | adversarial | the error path | an interrupted call still restores, fresh session included |
| 10 | adversarial | composition | two RNG-neutral calls nest without either seeing the other |

**Result:** 39 assertions, all passing. **1 defect found, 2 sites fixed, 1
earlier ledger entry corrected.**

**Related files rerun:** `test-adversarial-cycle11.R` (31), `test-psa.R` (28),
`test-psa-adversarial.R` (31), `test-psa-reporting.R` (8),
`test-access-inference.R` (28), `test-geographic-holdout.R` (17),
`test-real-spatial-access.R` (15), `test-adversarial-cycle07.R` (54),
`test-38-fraher-agent-supply.R` (12) — 224 assertions, 0 problems.

**Bug class to sweep in a later cycle:** guards written twice. `.Random.seed`
restoration existed in two hand-rolled copies and one shared helper, and the
copies were the ones with the hole. Look for other logic that appears in more
than one place — range checks, tolerance comparisons, contract-column lists —
and check whether the copies agree with each other and with the shared version.

---

## Cycle 13 — 2026-08-10

**Mix:** 4 BVA · 3 semantic · 3 adversarial → `tests/testthat/test-adversarial-cycle13.R`

**Targets and why.** Cycle 12 carried forward *guards written twice*:
`.Random.seed` restoration existed as two hand-rolled copies plus a shared
helper, and the copies were the ones with the hole. That is a shape, not an
incident, so this cycle went looking for the rest of it.

### The sweep

Searched `R/` for character vectors written verbatim in more than one file.
Nine came back. Most are harmless coincidences of vocabulary (`c("ui","pop","ai")`
appears in five files because those are the three conditions). **Three were the
same list, or the same rule, maintained in two places:**

| duplicate | where |
|---|---|
| the five demand age bands | `DEMAND_AGE_BANDS` **and a local shadow of the same name** inside `brfss_pfd_prevalence_for_demand_bands()` |
| the seven microsim band labels | `MICROSIM_AGE_BAND_LABELS` **and seven hardcoded labels** in `urps_partial_pooled_hazards()` |
| the gap identity tolerance | `0.01` written into `validate_urps_gap_projection()` **and** into `validation_report()` |

### Defect found and fixed — 1, in three places

**D24 · a constant with a private copy.** None of the three had diverged yet,
which is precisely when to fix them — a test pinning two copies as equal is
weaker than having one copy.

The demand-bands one is the sharpest. `brfss_pfd_prevalence_for_demand_bands()`
declared a **local variable with the same name as the package constant**, so it
shadowed it. The moment `DEMAND_AGE_BANDS` changed, that function would keep the
old five labels — and it feeds `compute_brfss_demand_estimand()`, whose "unknown
age band" guard would then have been comparing against bands nobody else used.
A guard checking a stale vocabulary reports clean.

*Fix:* all three consumers read the shared definition; the tolerance is hoisted
to `GAP_IDENTITY_TOLERANCE_FTE`. Test 8 is a standing gate — it asserts each
constant is defined exactly once in `R/` and that the seven microsim labels
appear as a literal exactly once, so a future copy-paste re-introduces the drift
loudly.

**Deliberately NOT merged.** `DEMAND_AGE_BANDS` and `URPS_POP_AGE_BANDS` are
both five labels long and neither is the other — they are the demand-side and
population-side vocabularies, with a crosswalk between them. Deduplicating by
"these are both five bands" would have merged two different things. Test 10
pins them as disjoint and checks that a population band passed where a demand
band is expected is refused rather than silently contributing zero.

**Already guarded, left alone.** `CALIBRATION_STATUS_RANK` has a deliberate
second copy (a local `status_rank` in the export contract) with an existing test
that greps both sources. Test 4 adds the properties the ranking itself must
have rather than duplicating that check.

| # | cat | target | assumption challenged |
|---|---|---|---|
| 1 | BVA | `GAP_IDENTITY_TOLERANCE_FTE` | one constant; closed at it; symmetric in sign |
| 2 | BVA | `DEMAND_AGE_BANDS` | five labels that tile the lifespan with no gap |
| 3 | BVA | `MICROSIM_AGE_BANDS` | one label short of the breaks; right-open at every boundary |
| 4 | BVA | `CALIBRATION_STATUS_RANK` | closed, ordered, and NA for an unknown tier |
| 5 | semantic | the BRFSS crosswalk | uses the package bands, not a private copy |
| 6 | semantic | the pooled hazard | keys to the bands the microsimulation ages through |
| 7 | semantic | both copies of the gap check | same verdict on the same frame |
| 8 | adversarial | `R/` | no duplicated definition of a shared vocabulary survives |
| 9 | adversarial | every consumer | reads the shared constant (asserted by identity of output) |
| 10 | adversarial | the two band vocabularies | deliberately different and not swappable |

**Four of my own test defects, corrected in the tests:** `r[["missing"]]` errors
where `r["missing"]` gives NA; the `validation_report()` column is `passed`, not
`pass`; and an adversarial test that tried to rebind a package constant could
not run at all (bindings are locked) — a test that can only skip proves nothing,
so it was replaced with one asserting each consumer's *output* is keyed to the
shared object.

**Result:** 50 assertions, all passing. **1 defect found, 3 sites fixed.**

**Related files rerun:** `test-urps-population.R` (41), `test-adequacy-evidence.R`
(81), `test-demand-and-validation.R` (150), `test-workforce-microsimulation.R`
(58), `test-partial-pooling-hazards.R` (9), `test-demand-estimands.R` (67), and
cycles 03/06/12 (171) — 577 assertions, 0 problems.

**Bug class to sweep in a later cycle:** a local variable that shadows a package
constant. The BRFSS one was found by looking for duplicated *literals*; a
shadow that assigns a computed value would not have been. Enumerate every
assignment inside a function whose name matches an existing package-level
binding.

---

## Cycle 14 — 2026-08-10

**Mix:** 3 BVA · 4 semantic · 3 adversarial → `tests/testthat/test-adversarial-cycle14.R`

### Cycle 13's carried-forward class — swept, clean

Parsed every function body in `R/` and matched every assignment target against
the package-level **constants** (top-level bindings whose value is not a
function).

> **In-function assignments shadowing a package constant: 0.**

The `DEMAND_AGE_BANDS` shadow fixed in cycle 13 was the only one. Class closed on
evidence.

### Defect found and fixed — 1

**D25 · a function that adds providers to the workforce added providers who were
never in it.** `add_returning_providers()` builds rows for clinicians coming back
into practice, then fills every column it did not set explicitly:

```r
new_agents[[col]] <- if (is.numeric(agents[[col]])) 0 else
  if (is.logical(agents[[col]])) FALSE else NA
```

`retirement_year` is numeric. The microsimulation's active predicate is
`is.na(retirement_year) | retirement_year > year`, so a returner filled with `0`
is **retired before the projection starts**. Measured:

```
active before: 10   add 3 returners   active after: 10
```

The rows exist, the headcount does not move, and nothing reports a problem.

*Fix:* `0` is the right absent-value for a **counter** and for nothing else.
`n_moves` gets `0`; logicals get `FALSE` (a returner is, by definition, back);
everything else gets a **typed NA**, which is what "not applicable / never
happened" means here. Returners also now get generated, namespaced ids —
`NA` ids cannot be joined to, deduplicated, or followed across years, and cycle
01 established that duplicate ids are treated as distinct clinicians.

Swept the class: this generic fill appears **once** in `R/`, and it is the only
place rows are appended to an agent table.

### Recorded as a test, not silently changed

`apply_provider_migration_matrix()` marks an out-of-country mover with
`left_country = TRUE` and `state = NA`. **Nothing in `R/` reads `left_country`.**
So an emigrated provider is absent from every state total while the national
active count still includes them — state FTE no longer sums to national FTE.

Which total is *right* is a modelling decision, not a bug to patch on my own
authority: an emigrant may legitimately remain in a national certification
count, which is what the observed series measures. Test 10 pins the arithmetic
instead — the gap between the two totals is exactly `sum(left_country)`, so a
consumer that wants either can compute it, and the discrepancy is visible rather
than latent.

| # | cat | target | assumption challenged |
|---|---|---|---|
| 1 | BVA | `add_returning_providers` | zero is a no-op; negative and NA counts are refused; fractional counts round |
| 2 | BVA | return year | active in the year of return, not before |
| 3 | BVA | multi-geography counts | zero means zero, not one |
| 4 | semantic | the active workforce | a returner counts toward it |
| 5 | semantic | absent values | `0` for counters, `FALSE` for logicals, typed NA for everything else |
| 6 | semantic | returner ids | present, unique, and cannot collide across return years |
| 7 | semantic | the agent schema | survives the append, columns and classes unchanged |
| 8 | adversarial | a projection | returners survive it instead of vanishing from it |
| 9 | adversarial | a returner's sex | unknown, and not invented by the append |
| 10 | adversarial | emigration | the national/state discrepancy is exactly recoverable |

**Result:** 42 assertions, all passing. **1 defect found, 1 fixed.**

**Related files rerun:** `test-origin-dependent-migration.R` (27),
`test-urps-migration.R` (46), `test-geographic-demand.R` (26),
`test-workforce-microsimulation.R` (58), `test-orchestrator-wiring.R` (56),
`test-hardening-batch-de.R` (6), `test-numeric-guards.R` (20), and cycles 01/13
(76) — 315 assertions, 0 problems.

**Bug class to sweep in a later cycle:** the default value that is only right
for one column type. `0` for a numeric was wrong for a *date-like* numeric;
the same shape is `""` for a character that is really a factor level, `FALSE`
for a logical that is really tri-state, and `1` for a multiplier that is really
a count. Enumerate every place a value is filled in for an unset column or a
missing lookup, and ask what that column MEANS rather than what type it is.

---

## Cycle 15 — 2026-08-10

**Mix:** 4 BVA · 3 semantic · 3 adversarial → `tests/testthat/test-adversarial-cycle15.R`

**Targets and why.** Cycle 14 carried forward *the default value that is only
right for one column type*. Enumerated every NA-fill in `R/` —
`x[is.na(x)] <- v`, `coalesce(x, v)`, `%||% v` — and asked what each column
**means** rather than what type it is.

Most are correct and documented: a missing retirement hazard takes the **highest**
tabulated one (conservative in the direction that matters), a missing band
population is deliberately zero with the reason in a comment, an unrecognised
calibration status ranks 0 so it cannot be a promotion. Test 7 pins that group
so a future tidy-up has to argue with a test. **One was not.**

### Defect found and fixed — 1, with two distinct causes

**D26 · a survival function that rises.** `retirement_survival()` filled every
missing lookup with `0`. But a lookup can miss in **two directions**, and only
one of them means zero:

* a `to_age` beyond the horizon really is 0 — everyone has left;
* a `to_age` at or below `from_age` is an interval that **has not elapsed**, and
  `S(t)` for that is **1**.

Measured before the fix, for `to_ages = 45, 50, 55, 60, 65` from age 50:

```
0.0000  0.0000  0.8947  0.7965  0.5452     <- a survival curve that RISES
```

Anything downstream reading that as a retention rate or a cumulative
probability was reading a curve that cannot exist.

**My first fix was incomplete and these tests caught it.** Setting the
`to_age <= from_age` cases to 1 left `retirement_survival(50, 50)` returning
**0.964** — because `seq(from_age, max_age - 1L)` **counts down** when
`max_age <= from_age`, producing `ages = c(50, 49)` and multiplying two
one-year survival probabilities over a zero-length interval. The value was never
`NA`, so the NA-fill never saw it.

That is the **third occurrence of the descending-range trap** in this ledger:
live in `prevalence_from_incidence()` (cycle 05), latent in
`e2sfca_incremental_weights()` (cycle 07), live here. *Swept:* the only other
`a:(b-1)` construction in `R/` is at `supply-provider_lifecycle.R:274`, and it
already carries an explicit guard naming this exact hazard.

| # | cat | target | assumption challenged |
|---|---|---|---|
| 1 | BVA | zero-length interval | `S(50 → 50) = 1` |
| 2 | BVA | the two miss-directions | before the start is 1, beyond the horizon is 0 |
| 3 | BVA | bounds | survival in [0, 1] at every requested age, three start ages |
| 4 | BVA | naming/order | the requested ages come back keyed as asked |
| 5 | semantic | monotonicity | non-increasing on a coarse grid and on 40:90 |
| 6 | semantic | construction | it IS `cumprod(1 - h)`, and it composes: `S(55→65) = S(55→60)·S(60→65)` |
| 7 | semantic | the correct NA-fills | stay correct |
| 8 | adversarial | fuzzed straddles | no `from`/`to` arrangement produces a rising curve |
| 9 | adversarial | `participation_fte` | the table has no gap for its `0.5` fallback to hide behind |
| 10 | adversarial | survival vs hazard | zero hazard ⇒ survival 1; certain departure ⇒ 0 |

**Result:** 80 assertions, all passing. **1 defect found, 2 causes fixed.**

**Recorded, not changed:** `participation_fte()` ends in
`out[is.na(out)] <- 0.5` — an invented half-time provider for any `(sex, age)`
the table does not cover. With the shipped `FUTUREDOCS_PARTICIPATION` (112 rows,
56 ages × 2 sexes, no gaps) that branch is **unreachable**, so it is a latent
trap rather than a live defect. Test 9 keeps it unreachable by pinning the
table's completeness — which is the thing that would actually change.

**Related files rerun:** `test-provider-lifecycle.R` (56),
`test-workforce-microsimulation.R` (58), `test-provider-state-machine.R` (25),
`test-scientific-benchmarks.R` (21), `test-urps-prevention.R` (50),
`test-supply-microsim-adversarial.R` (20), `test-boundary-values.R` (81), and
cycles 02/05/07/14 (234) — 545 assertions, 0 problems.

**Bug class to sweep in a later cycle:** an identity that is asserted at one
point but never across the range. `S(t)` was checked at individual ages and
never for monotonicity; the same shape is any function documented as
non-decreasing, bounded, or conserved that is only ever tested pointwise.
Enumerate the functions whose docs claim a shape (monotone, bounded, sums to a
total, telescopes) and test the shape, not a point on it.

---

## Cycle 16 — 2026-08-10

**Mix:** 3 BVA · 4 semantic · 3 adversarial → `tests/testthat/test-adversarial-cycle16.R`

**Targets and why.** Cycle 15 carried forward: *an identity asserted at a point
but never across the range.* `retirement_survival()` had been checked at
individual ages and never for monotonicity — which is exactly how a survival
curve that RISES went undetected. So this cycle enumerated the roxygen in `R/`
for claims about **shape** — monotone, non-decreasing, cumulative, telescopes,
conserved, sums to a total, bounded — and tested the shape rather than a point
on it.

### Result: no defect. The documented shapes hold.

That is the finding, and it is worth stating plainly rather than dressing up.
Ten tests, 62 assertions, and every documented shape survived — including the
ones that are only true over **part** of a range, which are the ones a
whole-range test gets wrong.

**Two claims that a naive shape test would have mis-flagged:**

* `WU2011_SURGERY_RATE_COMPONENTS$pop` is **not** monotone across the five
  bands: `0.500, 1.645, 2.719, 2.719, 1.069`. The doc says POP "rises
  monotonically **to ~71-73**" — the 65-79 band contains that peak, so the fall
  at 80+ is the source's shape, not a violation. Test 8 asserts rise-to-peak
  and fall-after, which is the actual claim.
* The empirical hazard is monotone **from the 60-64 band onward**, not
  throughout. The guard is a `cummax` anchored at exactly one band: anchored a
  band earlier it would flatten real mid-career non-monotonicity; a band later
  and the 70+ cell it exists for is uncovered. Test 1 pins the anchor from both
  sides.

**The guard is load-bearing, and test 10 proves it rather than assuming it.**
With `floor_sparse = FALSE` the raw series really does violate the shape —
`70+` is the observed 0/16 cell and comes back as **0.0000**, which would read
as "nobody retires after 70". Guarded, it inherits the 65-69 hazard. A test that
only checked the guarded series would pass even if the guard had become a no-op.

| # | cat | target | assumption challenged |
|---|---|---|---|
| 1 | BVA | the sparse-cell guard | applies from 60-64 onward and nowhere earlier |
| 2 | BVA | the single-year schedule | observed/literature splice lands exactly at 70 |
| 3 | BVA | Wu components | non-negative, and the component sum IS the combined table |
| 4 | semantic | hazard shape | non-decreasing from 60-64 under **every** pooling choice, not just the default |
| 5 | semantic | the single-year schedule | non-decreasing across its whole range, every consecutive pair |
| 6 | semantic | age-specific D3 | linear in population, and by-condition sums to combined |
| 7 | semantic | SUI vs POP | genuinely different curves, not proportional rescalings |
| 8 | adversarial | band-label mismatch | `"80plus"` vs `"80+"` refused, and the loss it would cause is material |
| 9 | adversarial | a shape battery | survival, telescoping weights, prevalence bounds — all across their ranges |
| 10 | adversarial | the guard itself | turning it off changes the shape, so it is not decoration |

**Result:** 62 assertions, all passing. **0 defects found.** Four cycles in
(05, 07, 15) established the descending-range and pointwise-identity classes;
this cycle is the one that found the rest of the surface clean.

**Related files rerun:** `test-calibration-sources.R` (24),
`test-provider-lifecycle.R` (56), `test-workforce-microsimulation.R` (58),
`test-demand-estimands.R` (67), `test-scientific-benchmarks.R` (21), and cycles
05/07/13/15 (276) — 502 assertions, 0 problems.

**Bug class to sweep in a later cycle:** a claim in prose that no test reads.
Test 8's premise — that a `"80plus"`/`"80+"` mismatch once lost 15% of cases —
is recorded in a source comment, and the only reason it is now checked is that
someone read the comment. Enumerate the "THE DEFECT THIS EXISTS FOR" and
"used to" narratives in `R/` comments and check each has a test that would fail
if the fix were reverted.

---

## Cycle 17 — 2026-08-10

**Mix:** 4 BVA · 3 semantic · 3 adversarial → `tests/testthat/test-adversarial-cycle17.R`

### Cycle 16's carried-forward class — swept, clean

Enumerated the "THE DEFECT THIS EXISTS FOR" / "used to" / "Measured before"
narratives in `R/` comments, resolved each to its enclosing function, and
checked whether any test names that function.

> **narrative-bearing functions: 33 · with no test naming them: 4**

All four are false positives — three are ordinary `@param x ... used to select`
prose rather than defect narratives, and the fourth is a module header whose
functions *are* covered (`test-meps-care-seeking.R`) under a call spelling the
scan missed. **The repository's defect narratives are tested.** Two spot-checks
confirmed it by hand: `safe_rbind()`'s type-coercion story (five geocoding runs,
364 of 1,540 rows silently NA'd) and the coordinate address screen (Glen Dale,
WV geocoded 131 km into Ohio) each have a test that would fail if the fix were
reverted.

### The finding the sweep surfaced

`care_seeking_multipliers()` carries the module's central scientific guard —
*"an estimate the data cannot distinguish from 1.0 cannot be quietly adopted as
if it were measured"* — implemented as

```r
identified = !(lo <= 1 && hi >= 1)      # over the ratio est / base
```

The flag is sound, and tests 1, 2 and 7 pin its boundary behaviour: an interval
that merely **touches** 1 is unidentified (closed on both sides, conservative in
the only safe direction), the `lo` clamp at 0 is cosmetic and cannot change a
verdict, and identification is a property of the **interval**, not of the
estimate's size — a huge multiplier with a useless interval is unidentified, a
tiny one cleanly resolved is identified.

**`base` is a predicted probability and nothing checks it.** At `base = 0` the
ratio is `Inf`, and the flag evaluates

```
!(Inf <= 1 && Inf >= 1)  ->  !(FALSE && TRUE)  ->  TRUE
```

so an **infinite multiplier is reported as IDENTIFIED** — the strongest claim
the function can make, produced by a division by zero. Test 8 states that
arithmetic explicitly and pins the property any caller needs (a non-finite
multiplier is never evidence); test 9 checks the live path across all three
factor terms and confirms every multiplier the fitted model produces is finite.
Not patched: forcing a value here would replace a visible `Inf` with an invented
finite one, and the reference-row-predicts-zero case is not reachable from the
fitted model — it is a contract gap, recorded rather than papered over.

| # | cat | target | assumption challenged |
|---|---|---|---|
| 1 | BVA | the flag's boundary | an interval touching 1 is unidentified |
| 2 | BVA | the `lo` clamp | cosmetic; cannot change a verdict |
| 3 | BVA | the reference level | its own multiplier is exactly 1, and never "identified" |
| 4 | BVA | the confidence level | widening loses identification, never gains it |
| 5 | semantic | the interval | brackets the point estimate |
| 6 | semantic | identification vs effect size | different questions |
| 7 | semantic | a non-factor term | refused, not an empty frame |
| 8 | adversarial | `base = 0` | an infinite multiplier reported as identified |
| 9 | adversarial | the live path | every multiplier finite across all three terms |
| 10 | adversarial | constants vs estimates | still distinguishable; the constants stay ≤ 1 so the cycle-06 cascade holds |

**Result:** 59 assertions, all passing, **0 skips**. **0 defects fixed; 1
contract gap recorded.**

**My own slip, and it is the third of its kind:** the first run had **6 of 10
tests skipping** because I called `build_meps_care_seeking_panel(year = 23)`
where the argument is `2023L` — `substr("23", 3, 4)` is `""`, so it looked for a
column named `PERWTF`. Six skips is a cycle that proves nothing (the cycle 13
lesson), so it was fixed rather than accepted. The ledger has now recorded this
fixture-shape class in cycles 06, 11 and 17.

**Related files rerun:** `test-meps-care-seeking.R` (25),
`test-provider-coordinates.R` (36), `test-backtest.R` (106),
`test-urps-population.R` (41), `test-demand-and-validation.R` (150),
`test-data-artifact-classification.R` (15), and cycles 06/16 (135) — 508
assertions, 0 problems.

**Bug class to sweep in a later cycle:** a ratio whose denominator is a
*modelled* quantity rather than an input. `est / base` here, `gap_fte /
required_fte` in `compute_fte_gap()`, `effective_fte / headcount` in the FTE
guard — the inputs are validated and the denominators are not, because they are
computed. Enumerate every division in `R/` whose denominator is produced by the
model itself and check each has a zero/degenerate guard.

---

## Cycle 18 — 2026-08-10

**Mix:** 3 BVA · 3 semantic · 4 adversarial → `tests/testthat/test-adversarial-cycle18.R`

*Rotation correction: the mix sequence drifted at cycle 15 (4/3/3 was run where
the rotation called for 3/3/4, and stayed one behind through 17). This cycle
restores the canonical 4/3/3 → 3/4/3 → 3/3/4 sequence from cycle 01.*

### The surface, enumerated

Cycle 17 carried forward: *a ratio whose denominator is a modelled quantity
rather than a validated input.* Enumerated all of it before touching anything.

| | count |
|---|---|
| division sites in `R/` | **275** in 63 files |
| with a non-literal denominator | **174** |
| unguarded in surrounding context (raw scan) | 66 |
| genuinely computed denominator with zero reachable, after hand classification | **5** |

Most of the 66 are false positives — literals, `dplyr::n()`, roxygen prose, or
parameters with their own range guard. The hand-classified survivors all share
one shape, and it is the one cycles 12 and 13 named: **the guard exists and the
copy does not use it.**

### Defect found and fixed — 1, in five places

**D27 · an undefined ratio reported as a favourable finding.**
`replacement_ratio = entrants / departures` in the orchestrator's outlook block.
`departures <- baseline_supply * rate`, and `rate` is modelled — verified
reachable: `implied_annual_departure_rate()` returns **exactly 0** under a zero
retirement schedule with no career change. So a "nobody leaves" scenario gave

```
rr = 55 / 0 = Inf   ->   classify_workforce_outlook(Inf) = "Adequate"
```

An infinite replacement ratio presented as a workforce finding, in a reported
table. `-Inf` classified as `"Insufficient"` by the same route; `NaN` already
fell through to NA only because `is.na(NaN)` is TRUE.

Five sites changed, each the smallest defensible fix:

| site | before | after |
|---|---|---|
| `core-run…:696` | `entrants / departures` | `ssot_safe_divide(...)` |
| `core-run…:853` | `100 * gap_fte / demand_clinical_fte` | `ssot_safe_divide(...)` |
| `core-run…:670` | `scen_fte / sq_demand_fte` | `ssot_safe_divide(..., default = 1)` |
| `reporting-baseline_gap.R:529` | `100 * (required - base) / required` | `ssot_safe_divide(...)` |
| `classify_workforce_outlook()` | only `is.na()` checked | `!is.finite()` → NA |

The middle three are the duplicated-guard pattern precisely:
`compute_fte_gap()` has computed `gap_pct` with `ssot_safe_divide()` since it
was written, and two other places compute the same quantity by hand and divide
raw — so one run could report `NA` and `Inf` for the same degenerate input
depending on which path produced it. Test 9 asserts all three now agree.

`demand_lift` takes `default = 1` rather than NA: it is a **multiplier**, and
"we cannot tell" has to mean "no lift" for a multiplier or the downstream
product becomes NA for a reason unrelated to demand.

### Already guarded — left alone

`medicare_workload_index` (explicit `stop()` on a non-positive reference mean),
`calibrate_wrvu_per_fte` (asserts the solved denominator > 0), `rebase_to_year`
(refuses a zero base), `supply_per_capita`, `capacity_category_adequacy`,
`compute_fte_gap`. Test 6 pins that group.

### Deliberately not fixed

* **`care_seeking_multipliers()` at `base = 0`** — left as a visible `Inf`, as
  instructed and as cycle 17 recorded. Inventing a finite value would replace a
  loud wrong answer with a quiet one, and the state is not reachable from the
  fitted model.
* **`left_country`** — untouched. Establishing the intended estimand is a
  modelling decision, not an implementation fix.
* **`percent_error = 100 * (pred - obs) / obs`** (4 sites in the back-test
  modules) — the denominator is an **observed** contract count, not a modelled
  quantity, so it is out of this cycle's scope by definition. A zero observed
  active-provider count is also not a state the contract can be in. Recorded
  rather than changed.
* **`ev / py`** in `supply-partial_pooling_hazard.R` — zero person-years gives
  NaN, but `py` comes from the canonical pooled-hazard artifact and is an input
  to this module rather than something it computes. Same reasoning; noted for a
  future input-validation cycle.

  > **Corrected in cycle 19.** This note was wrong. `py` is guarded on **both**
  > sides — `hazard_pooled_long()` filters `py > 0` and
  > `fit_partial_pooled_hazards()` stops on a non-positive cell with a message
  > naming the quantity. The unvalidated quantity there was `ev`, the
  > **numerator**. See cycle 19.

**Result:** 47 assertions, all passing, 0 skips. **1 defect found, 5 sites
fixed.** No scientific assumption changed: every non-degenerate input produces
the identical number it did before (test 9 asserts that explicitly).

**Related files rerun:** `test-orchestrator-wiring.R` (56),
`test-workforce-microsimulation.R` (58), `test-workload-to-fte.R` (76),
`test-numeric-guards.R` (20), `test-baseline-gap.R` (47), and cycles
03/04/10/17 (221) — 478 assertions, 0 problems.

**Bug class to sweep in a later cycle:** denominators that arrive as *inputs*
to a module but are computed by a different one — `py` in the pooled hazard,
`obs` in the back-test percent errors. Between-module boundaries are where "the
caller validated it" and "the callee validated it" can both be false.

---

## Cycle 19 — 2026-08-10

**Mix:** 4 BVA · 3 semantic · 3 adversarial → `tests/testthat/test-adversarial-cycle19.R`

**Targets and why.** Cycle 18 carried forward: *denominators that arrive as
inputs to one module but are computed by another* — the boundary where "the
caller validated it" and "the callee validated it" can both be false. It named
two suspects.

**One of them was wrong, and correcting it is where this cycle started.** `py`
(person-years) in the pooled-hazard module is guarded on **both** sides:
`hazard_pooled_long()` filters `py > 0`, and `fit_partial_pooled_hazards()`
stops on a non-positive cell with a message naming the quantity. Cycle 18's
ledger entry is annotated in place rather than rewritten.

Looking at the guarded denominator is what showed up the thing it cannot see.

### Defect found and fixed — 1

**D28 · the numerator nobody validated.** `ev` — the event count, the numerator
of every hazard in the module — was checked by neither side of the boundary. A
hazard is a probability, so `0 ≤ ev ≤ py`, and **both** bounds were unenforced.
Measured:

| input | `ev/py` | what happened |
|---|---|---|
| `ev = 25`, `py = 10` | **2.5** | passed `hazard_pooled_long()`; reached `blme` as `cbind(25, -15)` |
| `ev = -3`, `py = 10` | **−0.5** | passed both, returned in the table |

`d$unpooled <- d$ev / d$py` is written into the returned tibble **before** any
model is fitted, so those are reported quantities in their own right, not just
inputs to a fit.

**Where the failure surfaced matters as much as whether it did.** Unguarded,
`ev = 25` over `py = 10` died inside a third-party package with

```
Error: Value 2.31818 out of range (0, 1)
```

naming neither the column, the band, the subspecialty, nor this package. Test 6
asserts the new message carries all five. *Fix:* one shared
`.assert_event_counts()` called from both the reshape and the fit — cycle 13
established that two copies of a guard is how one starts accepting what the
other rejects.

**The zero-event cell still passes**, and test 3 exists to keep it that way: the
module header records "the 70+ cell is 0 events / 16 person-years" as the
motivating data, and a guard that refused `ev = 0` would break exactly the case
the module was built to pool.

| # | cat | target | assumption challenged |
|---|---|---|---|
| 1 | BVA | `ev` | closed at 0 and at `py`; refused just outside either |
| 2 | BVA | non-finite `ev` | NA/NaN/±Inf refused; the `py` guard still fires on its own terms |
| 3 | BVA | the 0-event cell | the motivating sparse cell still passes |
| 4 | BVA | `py = 0` | dropped, not divided by — and a 0-py cell carrying events is still refused |
| 5 | semantic | the unpooled hazard | a probability for every admissible input (fuzzed) |
| 6 | semantic | the error | names function, column, subspecialty, band and both values |
| 7 | semantic | both doors | same input, same verdict, one shared guard |
| 8 | adversarial | the denominator guard | cannot see a numerator problem — `py` is fine in all four cases |
| 9 | adversarial | the whole table | judged, not just row 1; several bad cells reported together |
| 10 | adversarial | multi-subspecialty | a bad cell in one is not hidden by good ones, and the pooled sum stays a probability |

**Result:** 84 assertions, all passing, 0 skips. **1 defect found, 2 sites
fixed, 1 earlier ledger entry corrected.**

**Related files rerun:** `test-partial-pooling-hazards.R` (9),
`test-calibration-sources.R` (24), `test-provider-lifecycle.R` (56),
`test-workforce-microsimulation.R` (58), `test-scientific-benchmarks.R` (21),
and cycles 13/16/18 (159) — 327 assertions, 0 problems.

### Full suite (launched at the close of cycle 18)

**4,403 passed · 3 failed · 0 errors · 31 skipped · 130 files.**

The three failures are exactly the known pre-existing ones —
`test-canonical-overlap.R` (stale `isochrones` rows) and the two
`test-practice-survey.R` `access_ascertainment` failures. Both of the failures
that were mine (cycles 09 and 10) are resolved and stayed resolved.

Against the cycle-09 baseline of 3,943 passed / 5 failed / 121 files: **+460
assertions, −2 failures, +9 files.**

**Bug class to sweep in a later cycle:** the guarded half of a pair. `py` was
validated and `ev` was not; the same asymmetry is available wherever two columns
must satisfy a joint constraint — numerator/denominator, lower/upper bound,
start/end year, observed/expected. Enumerate the column PAIRS that carry a
relation and check whether the relation is tested, or only each column
separately.

---

## Cycle 20 — 2026-08-10

**Mix:** 3 BVA · 4 semantic · 3 adversarial → `tests/testthat/test-adversarial-cycle20.R`

**Targets and why.** Cycle 19 carried forward *the guarded half of a pair*: `py`
was validated and `ev` was not, and the same asymmetry is available wherever two
columns carry a joint constraint. The richest instance in this package is an
**interval**.

### The sweep

**Seventeen files in `R/` produce a lower/upper pair** — `*_lo`/`*_hi`,
`lower_95`/`upper_95`, `pi95_lower`/`pi95_upper`, `conf_low`/`conf_high`,
`ci_low`/`ci_high`. Grepping every ordering comparison in `R/` returned
**exactly one match, and it was a read rather than a check**
(`validation-forecast_scorecard.R:129` assigns `lo` and `hi`, then uses them).

Nothing in the package asserted that a lower bound is below its upper bound.

### Defect found and fixed — 1, at both ends of the chain

**D29 · an inverted interval validated clean and then scored.** The gap
projection contract carries **ten** bound pairs in `OPTIONAL_COLS`.
`validate_urps_gap_projection()` checks required columns, cohort basis,
non-finite values *in the six required numeric columns*, and the gap arithmetic
— and checks the bound columns for nothing. Measured:

```
validate_urps_gap_projection(lower_95 = 1400, upper_95 = 1000, mode = "strict")
  -> PASSED CLEAN

forecast_scorecard() on that interval
  -> coverage 0, mean_width -400
```

A **negative interval width** reported as a forecast diagnostic, and a coverage
of 0 that reads as "the model never covers the truth" when the bounds are simply
swapped. An NA bound passed equally: the completeness guard added in cycle 03
covers only the six **required** numeric columns, and every bound column is
optional.

*Fix:* one shared `.interval_bound_problems()` in
`R/core-canonical_and_joins.R` beside `.recycle_aligned()` and
`.assert_in_range()`, called from the projection validator (strict errors,
relaxed warns, matching every other guard in that file) and from
`forecast_scorecard()` (stops — scoring an interval you cannot interpret is not
a diagnostic). Pairs are **derived from the naming conventions**, not listed, so
a bound column added later is covered without editing the validator — test 4
asserts that by feeding it a `something_new_lo`/`_hi` pair it has never seen.

**The realistic failure is not a garbage value.** Test 9 makes that explicit:
two correct numbers in the wrong columns, both finite, both plausible workforce
counts, carrying the *same six values* as the well-formed frame. Nothing about
the values distinguishes them — only the relation does.

| # | cat | target | assumption challenged |
|---|---|---|---|
| 1 | BVA | `lo == hi` | a point interval is legal; one unit inverted is not |
| 2 | BVA | non-finite bounds | malformed, not wide — the required-column guard never covered these |
| 3 | BVA | no interval at all | still valid; half a pair is not a pair |
| 4 | semantic | pair discovery | derived from naming, so an unseen pair is covered |
| 5 | semantic | all ten pairs | one rule, not just the headline band |
| 6 | semantic | strict vs relaxed | matches the file's other guards |
| 7 | semantic | `forecast_scorecard` | refuses to score an interval it cannot interpret |
| 8 | adversarial | one bad row in six | caught elementwise — the frame's MEAN width is positive |
| 9 | adversarial | a plausible swap | same six numbers, only the relation differs |
| 10 | adversarial | well-formed output | unaffected: no bands, one band, all six pairs, degenerate bands |

**Result:** 38 assertions, all passing, 0 skips. **1 defect found, 2 sites
fixed.**

**Related files rerun:** `test-forecast-scorecard.R` (20),
`test-orchestrator-wiring.R` (56), `test-monte-carlo.R` (32),
`test-interval-coverage.R` (20), `test-backtest.R` (106),
`test-demand-lifecourse-uncertainty.R` (16), `test-export-wiring.R` (10),
`test-adequacy-evidence.R` (81), `test-demand-and-validation.R` (150), and
cycles 03/18/19 (190) — 681 assertions, 0 problems.

**Bug class to sweep in a later cycle:** a guard whose scope is a hardcoded
column list. The cycle-03 completeness guard listed six required columns and
therefore could not see the ten optional ones; this cycle's guard derives its
scope from naming instead. Enumerate the validators that name their columns
literally and check what each cannot see.
