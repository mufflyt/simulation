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
