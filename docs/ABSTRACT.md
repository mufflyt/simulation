# Abstract

*Draft, generated from the model state at commit `29a19d2`. Every figure below
was produced by the run described in Methods; none is transcribed from an
earlier draft. See "Provenance" at the end.*

---

**Objective.** To project the supply of and demand for urogynecology and
reconstructive pelvic surgery (URPS) providers in the United States through
2050 in common full-time-equivalent (FTE) units, and to identify which model
inputs actually determine the answer.

**Methods.** We built a stochastic, individual-level microsimulation of the
board-certified URPS workforce (n = 1,339; 1,031 ABOG, 308 ABU), following the
IHS Markit / Dall Health Workforce Microsimulation Model. Supply was projected
from the observed roster using age- and sex-specific clinical hours, separate
retirement and career-change hazards, and NRMP-derived entry (70 per
appointment year), with 200 Monte Carlo iterations across 14 prespecified
scenarios. Demand was projected from age-banded Census female population
through a reproductive life-course pathway and converted to required FTE via a
work-RVU basket, with productivity solved against a base-year anchor
(5,193 wRVU/FTE) and visit volumes calibrated to NAMCS. Base-year adequacy was
estimated rather than assumed. Interval calibration was assessed by a
leakage-free historical back-test (10 prespecified arms, 2020 cutoff, 2023
target) and by rolling-origin validation in which a training window is admitted
only when its outcome was observable at the origin.

**Results.** Projected supply rises from 1,339 FTE in 2025 to 2,051 FTE in 2050
(95% Monte Carlo range 1,645–2,518), while required FTE rises from 1,377 to
1,584. Under the specified calibration this implies a base-year deficit of
38 FTE that closes during the late 2020s and becomes a model-implied surplus of
467 FTE by 2050.

That conclusion is **not robust to the base-year capacity anchor**, which is
derived by analogy from a published physical-therapy capacity distribution
rather than measured in this specialty. Required FTE is linear in the anchor,
so the surplus vanishes at an anchor **1.29× the current value** — equivalent
to base-year adequacy of 0.73 rather than the 0.948 assumed here. Below that
the model implies a surplus (783 FTE at 0.8×, 467 at 1.0×, 70 at 1.25×); above
it, a shortage (326 FTE at 1.5×). The anchor is not the only unmeasured input —
the hours curve is also uncalibrated for this specialty — but it is the only
one that can change the *sign* of the 2050 result.

By contrast, the delegation matrix and the demand calibration cancel out of the
projection entirely: because required FTE is `anchor × wRVU(t) / wRVU(base)`, a
2.1× change in demand calibration moved 2050 required FTE by 0.25%. Only the
anchor and the *shape* of demand growth move the answer.

Reported intervals are **not validated**. In the back-test the observed value
fell outside the 95% interval in 8 of 10 arms (20% coverage against 80%
required), so intervals are reported as Monte Carlo ranges rather than forecast
or prediction intervals.

**Conclusions.** Under an explicitly analogy-derived base-year anchor, the URPS
workforce is projected to move from a small current deficit to a surplus by
2050 — but that conclusion survives only while true base-year adequacy exceeds
about 0.73, and the assumed value (0.948) comes from another specialty. A
fielded URPS practice-capacity survey is
therefore the highest-value missing input, and no policy inference about
adequacy should rest on this projection until it exists. The finding that
delegation and demand calibration cancel is itself useful: it redirects
measurement effort away from inputs that cannot change the result.

---

## What this abstract deliberately does not claim

* **Not "the current shortage is 71 FTE."** The base-year figure has no direct
  external anchor in this specialty. The model-implied gap under the specified
  calibration is 71 FTE (5.18% shortfall, adequacy 0.948, tier
  `derived_by_analogy`); that is a different sentence and the package enforces
  the distinction (`baseline_gap_claim()`, `assert_external_anchor()`).
* **Not a prediction interval.** 2/10 arms covered. `interval_label()` refuses
  the stronger wording.
* **No geographic or access claim.** Provider coordinates are now 99.8%
  complete (1,336/1,339), but the drive-time access layer is not yet wired to a
  reported result, so the model makes no distributional claim.
* **Headcount is not capacity.** 25.0% of the roster billed no URPS Medicare
  services in CY2024 and the top quartile delivered 90.3% of volume. Supply
  figures are board-certified active providers, not delivered care.

## Open items that bear on the numbers

| item | resolved | in the reported estimand | cancels out |
|---|---|---|---|
| capacity anchor | no | yes | **no** — sets the sign |
| FTE (hours) curve | no | yes | no — shape worth ~176 FTE by 2050 |
| geographic access | no | no | n/a — absent, not wrong |
| delegation matrix | no | yes | yes |
| demand calibration | yes (NAMCS) | yes | yes |

## Provenance

Produced by `run_workforce_microsimulation()` on the production roster
(`scripts/run_with_production_roster.R`), 200 iterations, seed 20260801,
2025–2050, `calibration = "namcs"`, `allow_analogy = TRUE`,
`example_only = FALSE`, contract v3.0.0, roster snapshot 2026-07-22.
Back-test figures from `backtest_status()`; anchor sensitivity from
`baseline_anchor_sensitivity()`; open items from
`unresolved_calibration_items()`.
