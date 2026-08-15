# POP recurrence estimand audit — `0.12` and `0.40`

Companion to `POP_TRANSITION_ESTIMAND_AUDIT.md`, applying the same method to the
recurrence limb. **Neither value was changed.** The 140,762 anchor stays an
external diagnostic.

Verified on `feat/chia-inpatient-surgical-layer`, R 4.4.2, 2026-08-15.

---

## Verdict up front

| quantity | verdict |
|---|---|
| `recurrence prolapse_procedure per_entering = 0.40` | **legitimate estimand.** Share of recurrent presentations reoperated. Recurrence is a real clinical state, unlike `testing`. Sourceable as written. |
| `followup p_advance = 0.12` | **legitimate estimand, WRONG APPLICATION.** It is an annual hazard applied to a single cohort-year, so it captures only patients in their first post-operative year. |
| the anchor comparison | **sound in kind.** The anchor estimand is "pelvic organ prolapse surgical procedures", a procedure count, so it includes reoperations — the model is right to compare primary + reoperation against it. |

`0.40` is *not* the `0.55` problem repeated. Recurrent prolapse is a state a
patient genuinely occupies, and "share of recurrences undergoing reoperation" is
a quantity a study can report. Do not retire it.

## Finding 1: a stock-versus-flow error in the recurrence limb

The engine computes

```
recurrence entrants = THIS YEAR's primary operations x 0.12
                    = 1,142,682 x 0.12 = 137,122
reoperations        = 137,122 x 0.40   =  54,849
```

But recurrences presenting in any calendar year arise from the **accumulated
stock of everyone previously operated**, who remain at risk for years — not from
this year's operations alone. The engine applies **one year of exposure to one
cohort**.

In steady state with constant annual primary volume `P` and `N` years at risk:

| years at risk | reoperations | recurrence multiplier |
|---|---|---|
| 1 (**as implemented**) | 54,849 | **1.048** |
| 5 | 274,244 | 1.240 |
| 10 | 548,487 | 1.480 |
| 15 | 822,731 | 1.720 |

This is the same class of defect as the incident-versus-prevalent problem
already recorded for office visits: an annual transition rate applied to a flow
where the exposed population is a stock.

## Finding 2: as an annual hazard, the implied burden is too high

`0.12 x 0.40 = 4.8%` reoperation per year among the operated:

| horizon | cumulative reoperation implied |
|---|---|
| 1 year | 4.8% |
| 5 years | 21.8% |
| 7 years | 29.1% |
| 10 years | **38.9%** |

Published POP reoperation rates sit far below this — commonly single digits to
low teens at 5–10 years. A model implying ~39% of operated women are reoperated
within a decade is not defensible as an annual hazard.

The row's own note contains the tell: it calls `0.12` *"the ANNUAL recurrence
hazard"* and then justifies it with E-CARE, where *"failure accrues over years …
rising through 7 years."* **A cumulative multi-year observation is being used to
license an annual rate.** That is a horizon mismatch inside the justification
itself, and it is why the number is too large for the slot it occupies.

## Finding 3: the two errors run in opposite directions

This is the reason the recurrence limb has looked unremarkable.

- Finding 1 makes the exposed population **too small** (one cohort-year instead
  of the at-risk stock) — pushes reoperations *down*.
- Finding 2 makes the per-year rate **too high** — pushes reoperations *up*.

They partially cancel, and the resulting multiplier `1.048` looks modest and
plausible. It is plausible by coincidence, not by construction — exactly the
pattern found in the `testing` gate, where two unsourced numbers multiplied to
something less embarrassing than either deserved.

A correction that fixes only one side will move the total sharply and in a
direction that looks like a regression. Both must move together.

Rough scale of a jointly corrected limb: at ~1%/yr reoperation among the
operated (the order consistent with ~10% cumulative at 10 years) over a 10–15
year at-risk stock, the multiplier lands near **1.10–1.15**, against the current
`1.048`. So the recurrence limb is probably understated by roughly a factor of
two — material, but an order of magnitude smaller than the 8.51x primary-side
error, and in the same direction.

**Fixing recurrence cannot rescue the anchor.** It moves the total further up.

## What to source, and in what order

1. **Do not touch either value until the primary transition is sourced.** The
   recurrence limb is a second-order correction on a first-order error.
2. `0.40` — sourceable as written: share of recurrent POP presentations
   undergoing reoperation. Denominator: patients presenting with recurrence.
3. `0.12` — must be re-specified before sourcing. Decide explicitly whether it
   is (a) an annual hazard of recurrent *presentation* among all previously
   operated patients, which then requires the engine to expose the accumulated
   surgical stock rather than one cohort-year; or (b) a first-post-operative-year
   probability, which matches the current structure but is a different and much
   smaller quantity than E-CARE's multi-year curve.
4. UI carries the same structure (`0.08` hazard, `0.35` reoperation share) and
   AI carries a hazard with no reoperation row at all. Audit each separately —
   do not propagate any POP decision by analogy.

## Not changed

`0.12` and `0.40` are untouched. `0.35` is untouched. The
`condition_service_pathway_publishable` gate continues to refuse publication for
the whole pathway, so none of this is presentable regardless.
