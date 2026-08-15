# POP transition estimand audit

What the two live POP transitions actually mean in the engine, before either
value is changed. **Neither `0.35` nor `0.55` was altered by this audit.** The
140,762 anchor and the 4.68x discrepancy are retained as external diagnostics,
not as back-solve inputs.

Verified on `feat/chia-inpatient-surgical-layer`, R 4.4.2, 2026-08-15.

---

## What the engine means

`pathway_stage_entrants()` walks `PATHWAY_STAGES` in order, multiplying the
surviving cohort by each stage's single `p_advance`:

```
conservative -> testing -> procedure -> followup -> recurrence
```

The module documentation states the intent plainly: a procedure *"accrues only
to patients who failed conservative care AND completed testing."* So `testing`
is a **required gate on access to surgery**, not a service bucket.

Denominator and horizon, as implemented:

| | |
|---|---|
| Denominator of `conservative p_advance` | the full care-engaged POP stock (`FROZEN_CARE_ENGAGED[["pop"]]` = 3,264,807) |
| Horizon | **annual** — volumes are produced per `year`, and the anchor is an annual encounter count |
| Meaning of `conservative p_advance = 0.35` | annual probability that a care-engaged POP patient leaves conservative management for surgical workup |
| Meaning of `testing p_advance = 0.55` | annual probability that a patient in surgical workup **reaches surgery at all** |

---

## Finding 1: the `testing` stage is not a clinical state for POP

Service intensity at the testing stage, per entering patient:

| condition | services per entrant | composition | gates surgery at |
|---|---|---|---|
| **pop** | **0.50** | urodynamics 0.30, cystoscopy 0.20 | 0.55 |
| ui | 1.20 | urodynamics 0.85, cystoscopy 0.35 | 0.40 |
| **ai** | **0.25** | urodynamics 0.25 | 0.25 |

1,142,682 POP patients enter `testing`, and the stage delivers roughly 0.50
services per entrant. **About half of them receive no test at all** — yet
membership in the stage is what determines whether they can reach surgery. A
patient is gated out of an operation by a state they never occupied.

That is incoherent as a transition. Urodynamics and cystoscopy are *selectively*
used around surgical evaluation; not receiving either does not mean a patient
failed to progress to surgery. The stage is a service bucket wearing a
transition's clothes.

**UI is different and should not be changed by analogy.** At 1.20 services per
entrant, essentially everyone entering UI testing receives a test, so a gate
there is defensible. AI, at 0.25, has the same defect as POP but worse.

This is the structural defect hiding underneath the otherwise-live cascade. It
means `0.55` may have no estimand to source: there is no population "in testing"
whose surgical conversion rate a study could report, because the model's
"testing" population is defined by an arbitrary service arrangement rather than
by a clinical decision state.

## Finding 2: removing the gate makes the anchor gap WORSE, not better

This is the result that most constrains the redesign:

| specification | annual P(procedure \| care engaged) | vs required 0.0411 |
|---|---|---|
| required by the 140,762 anchor | 0.0411 | — |
| **current, 0.35 x 0.55** | **0.1925** | **4.68x high** |
| if `testing` becomes non-gating, 0.35 alone | 0.3500 | **8.51x high** |
| Medicare pessary -> surgery, 1 year (0.122) | 0.1220 | 2.97x high |

The `0.55` gate is currently **absorbing part of the discrepancy**. Deleting it
without touching `0.35` doubles the error. So the restructure cannot be
presented as a fix for the 4.68x; it isolates the error into a single parameter
that must then carry the whole burden, and that parameter would have to fall to
roughly `0.041` annually.

That is a feature, not an objection: one honest parameter at 8.51x is a better
object to source than two dishonest ones at 4.68x.

## Finding 3: the required value is not implausible once the denominator is right

Published longitudinal pessary-to-surgery evidence, kept separate by horizon
because these are not interchangeable:

| source | population | horizon | surgery |
|---|---|---|---|
| Medicare beneficiaries fitted with a pessary | pessary-fitted | 1 year | 12.2% |
| same | pessary-fitted | 7 years | 30.9% |
| successfully fitted pessary users | pessary-fitted | eventual (mostly <2y) | 31% |
| PEOPLE prospective cohort | pessary or surgery candidates | 24 months | 23.6% |
| pessary vs surgery RCT | randomised to pessary | 24 months | 54.1% crossover |
| Medicare POP diagnosis, any year | all diagnosed POP | 1 year | ~14-15% surgery, ~26% any treatment |

The RCT crossover at 54.1% versus the Medicare 1-year 12.2% is a 4.4x spread on
the *same nominal transition*, driven entirely by population selection and
treatment context. That spread is larger than the discrepancy being diagnosed,
which is why a number cannot be adopted from any of these directly.

The model's required 0.0411 sits about 3x below the Medicare 1-year 12.2%. That
is the expected direction: the model's denominator is **all care-engaged POP**,
which includes patients managed by observation alone and patients never fitted
with a pessary, while the Medicare figure conditions on pessary fitting — a
population already selected toward surgical candidacy. A ~3x denominator effect
between "care engaged" and "pessary fitted" is plausible. The anchor is
therefore not obviously irreconcilable; the current 0.35 is.

---

## Recommended restructure (not implemented)

```
POP care engaged
   |
   +-- conservative management
   |        |
   |        +-- ANNUAL transition to surgical management   <- the one parameter to source
   |
   +-> surgical management
          |
          +-- testing utilisation (urodynamics, cystoscopy) as per_entering,
          |   NON-GATING
          |
          +-- prolapse_procedure, per_entering = 1
```

Once a patient truly enters the surgical-management state,
`prolapse_procedure per_entering = 1.0` already expresses the terminal
operation. Testing becomes service intensity associated with the episode rather
than an attrition gate.

The key empirical quantity then becomes **one annual transition probability**,
not two multiplied ones.

### Before implementing

1. The same question must be asked of **AI** (0.25 services per entrant — same
   defect, worse) and answered separately for **UI** (1.20 — likely legitimate).
   Do not restructure all three by analogy.
2. Removing a gate changes FTE, because testing-stage service volume currently
   scales with a cohort that would be redefined. Quantify before/after.
3. The `condition_service_pathway_publishable` gate keeps refusing publication
   throughout, so none of this is presentable until the surviving parameter is
   sourced.

## What NOT to do next

Do not search for a paper reporting 55%. Test whether `0.55` denotes anything
real first — this audit says it probably does not. And do not adopt 12.2%: it is
the right *kind* of quantity, measured on the wrong denominator.
