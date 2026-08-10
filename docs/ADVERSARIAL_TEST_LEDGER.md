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
