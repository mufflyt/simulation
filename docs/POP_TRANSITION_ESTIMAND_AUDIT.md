# POP transition estimand audit

This audit first established what the two live POP transitions meant in the
engine, then implemented the minimum estimand correction. The historical
`testing p_advance = 0.55` is no longer treated as an empirical transition.
POP testing is now a **non-gating service-utilization bucket** with
`p_advance = 1.0` by construction.

The surviving `conservative p_advance = 0.35` is unchanged and remains
unsourced. The 140,762 annual POP-procedure anchor remains an external
diagnostic; it is not used to back-solve the model parameter.

Verified design on `feat/chia-inpatient-surgical-layer`, 2026-08-15.

---

## What the engine now means

`pathway_stage_entrants()` still walks `PATHWAY_STAGES` in order:

```
conservative -> testing -> procedure -> followup -> recurrence
```

But `p_advance = 1.0` is now used explicitly as a structural pass-through for
the POP testing stage. The stage can emit selective testing services without
changing who reaches the procedure stage.

For POP, the live estimands are now:

| quantity | value | interpretation |
|---|---:|---|
| care-engaged POP stock | 3,264,807 | annual denominator |
| conservative `p_advance` | **0.35** | annual transition from care-engaged conservative management to surgical management; **requires source** |
| testing urodynamics `per_entering` | 0.30 | selective utilization within surgical management; requires source |
| testing cystoscopy `per_entering` | 0.20 | selective utilization within surgical management; requires source |
| testing `p_advance` | **1.00** | structural pass-through; **not an empirical probability** |
| procedure `per_entering` | 1.00 | one prolapse procedure per patient entering procedure stage |
| annual recurrence hazard | 0.12 | still low-confidence / requires evidence review |
| recurrence reoperation share | 0.40 | still low-confidence / requires evidence review |

The old `0.55` confidence interval was removed. A structural identity does not
have an empirical CI.

---

## Why the old testing gate was invalid

Before this change, the module required patients to pass through testing before
a procedure could accrue. Yet POP testing emitted only:

- urodynamics: 0.30 service per entrant;
- cystoscopy: 0.20 service per entrant;
- total: 0.50 testing services per entrant.

Thus many patients entering the modeled testing stage received no modeled test,
while the stage's `p_advance = 0.55` determined whether they could reach surgery.
A patient could be gated out of an operation by a service state they never
occupied.

Testing utilization and treatment progression are different estimands. The
restructure separates them.

**UI is intentionally unchanged.** Its testing stage emits 1.20 services per
entrant and retains its existing 0.40 transition pending a separate estimand
audit. **AI is also unchanged**, but its 0.25 services per entrant make it a
separate high-priority structural audit; it must not be changed merely by
analogy to POP.

---

## Mutation contract

`tests/testthat/test-pop-testing-nongating.R` locks the new semantics:

1. every shipped POP testing row has `p_advance = 1.0` and no CI;
2. with 1,000 POP patients, entrants are 1,000 conservative -> 350 testing ->
   350 procedure -> 350 follow-up -> 42 recurrence;
3. setting POP urodynamics utilization from 0.30 to zero changes urodynamics
   volume but leaves prolapse-procedure volume unchanged;
4. UI testing remains at `p_advance = 0.40`;
5. the pathway remains `uncalibrated_illustrative`.

The pre-existing conservative-transition mutation test remains important:
halving the live POP conservative transition must halve downstream primary,
recurrence, and total POP procedure volume. Together, the two mutation tests
show which quantity is causal for progression and which is only utilization.

---

## Before/after procedure diagnostic

With the care-engaged POP stock fixed at 3,264,807:

### Before: artificial testing gate

```
3,264,807 x 0.35 x 0.55 = 628,475 primary procedures
628,475 x 0.12 x 0.40 = 30,167 recurrence procedures
Total = 658,642
658,642 / 140,762 = 4.68x anchor
```

### After: testing is non-gating

```
3,264,807 x 0.35 = 1,142,682 primary procedures
1,142,682 x 0.12 x 0.40 = 54,849 recurrence procedures
Total = 1,197,531
1,197,531 / 140,762 = 8.51x anchor
```

The discrepancy becomes larger **by design**. The old 0.55 was absorbing model
error without representing a coherent clinical transition. Removing it exposes
the entire mismatch in the surviving clinical pathway instead of hiding part of
it behind a testing artifact.

The annual conservative-to-surgical-management transition that would exactly
reproduce the frozen anchor, *if every other current POP pathway quantity were
held fixed*, is:

```
140,762 / (3,264,807 x (1 + 0.12 x 0.40)) = 0.04114
```

**0.04114 is a diagnostic, not a calibrated parameter.** It must not replace
0.35 unless evidence with a matching denominator and annual horizon supports it.

---

## Testing-service volumes are unchanged by this restructure

Testing entrants are determined by the upstream 0.35 transition, so replacing
the downstream 0.55 with structural pass-through does not change current testing
volume:

| service | annual volume at current 0.35 |
|---|---:|
| urodynamics | 342,805 |
| cystoscopy | 228,536 |

What changes is the downstream procedure, follow-up, and recurrence workload.
Later recalibration of the surviving 0.35 transition would change both testing
and procedure volumes together because both belong to the surgical-management
episode.

---

## Fixed-benchmark workload impact

For blast-radius interpretation only, applying the repository's current CMS
work-RVU basket, current URPS delegation matrix, 27.1% indirect-time gross-up,
and the fixed 7,500 work-RVU/FTE benchmark to the incremental downstream service
volume gives approximately **+388 URPS FTE** relative to the old 0.55-gated
specification.

This is **not a workforce forecast and not a recalibrated model result**. It is a
fixed-benchmark sensitivity showing that the removed structural gate had a large
workload consequence. The model's final FTE denominator and the surviving POP
transition remain subject to calibration.

---

## Evidence interpretation

Published POP treatment studies measure materially different denominators and
horizons: pessary-fitted cohorts, surgery candidates, randomized pessary arms,
or older Medicare populations with a POP diagnosis. They are useful bounds and
source-finding guides, but none directly identifies:

```
P(enter surgical management within one year | all care-engaged POP)
```

Therefore the next modeling task is **not** to choose the closest published
percentage. It is to source or construct that exact annual transition estimand.
The frozen 140,762 anchor and implied 0.04114 remain independent checks on
whether the sourced value and the rest of the pathway can coexist.

## Next actions

1. Source the surviving annual POP conservative -> surgical-management
   transition with a denominator- and horizon-matched estimate.
2. Audit whether the 0.12 recurrence hazard and 0.40 reoperation share represent
   compatible annual estimands; do not let them silently absorb the remaining
   anchor discrepancy.
3. Audit AI separately. Do not alter UI by analogy.
4. Keep `condition_service_pathway_publishable` red until the surviving pathway
   coefficients have acceptable provenance.
