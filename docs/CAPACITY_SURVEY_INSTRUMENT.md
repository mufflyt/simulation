# FPMRS national capacity survey — instrument and scoring specification

**Version 0.1 — frozen for cognitive interviewing. Not yet fielded.**

Design document. No code changes accompany it, and deliberately so: the survey
has to tell us whether a system-capacity decomposition is empirically
identifiable before the model grows a dimension to hold it.

The next uncertainty is not modelling. It is whether practising urogynecologists
interpret these questions the way the scoring specification assumes. The path
from here is fixed: cognitive interviews with 5-10 physicians across practice
settings, revise WORDING AND INTERPRETABILITY ONLY while the prespecified
scoring architecture stays put, pilot in a larger group, and only then decide on
a fifth calibration dimension. Changing the scoring architecture in response to
pilot results would be fitting the instrument to the answers.

## THE UNIT, frozen before anything else

Every count in Section A is:

> **unique new patients requesting FPMRS evaluation during the past 4 weeks**

Not referrals received, not unique patients requesting any appointment, not all
encounters, not procedures. Those are different denominators, and without a
frozen unit a high-volume practice answering in visits and another answering in
referrals both satisfy the scoring contract while measuring different things --
an error no downstream validation can detect, because both responses are
internally coherent.

Recall period is 4 weeks, not "a typical week". A typical week invites an
idealised average; 4 weeks is long enough to smooth clinic-schedule variation
and short enough to recall. The weekly figures the scorer expects are derived by
dividing by 4, in analysis, not by the respondent.

## Why a survey at all, when we have claims

Because utilization cannot distinguish *enough clinicians* from *as much care as
the clinicians could deliver*. HRSA's older approach set the base-year staffing
ratio to observed services over observed workforce, which assumes whatever care
is currently delivered is the correct amount; specialty projections inheriting
that assumption start at exactly 100% adequacy by construction.

The Dall lineage moved away from it by measuring baseline adequacy separately.
The physical-therapy model surveyed providers about spare capacity, overtime,
and patients they could not accommodate, and got 94.8% rather than assuming
100%. The physiatry model inferred a 10.6% baseline shortage the same way.
Neither derived adequacy from procedure counts.

This repo already carries the consumer: `capacity_survey_adequacy()`, with the
four-category arithmetic reproduced exactly in `capacity_category_adequacy()`.
What is missing is an FPMRS measurement to put through it.

## Hard constraints inherited from the scoring function

These are not stylistic. The instrument is unusable if it violates them.

**1. `additional` means a DIFFERENT quantity in each category.** The follow-up
must branch on the category answer; a single generic "how many additional
patients?" produces a number that cannot be scored.

| category | `additional` means |
|---|---|
| `equilibrium` | not asked — the ratio is 1.0 by definition |
| `surplus` | appointments that could have been taken **without** extending hours |
| `shortage_hours` | appointments accommodated **only by** extending hours |
| `shortage_unmet` | appointments that **could not** be accommodated |

**2. Each category divides by a different base**, which is why one generic
formula does not reproduce the published numbers:

```
surplus         1 + additional / (seen + additional)
shortage_hours  1 - additional / (seen - additional)
shortage_unmet  1 - additional / seen
```

**3. Denominators can vanish, and the function fails loudly when they do.**
`seen = 0` breaks surplus and unmet; `seen == additional` breaks
extended-hours (a respondent whose entire load was overtime). The instrument
must therefore require `seen > 0`, and for `shortage_hours` require
`additional < seen`, with in-form validation rather than post-hoc cleaning.

**4. Both quantities are per week, and `seen` is delivered, not scheduled.**
No-shows and cancellations are excluded; say so in the item stem.

---

## Section A — the anchor item (preserved from the published instrument)

Kept structurally intact for continuity with the PT and physiatry models. Only
the clinical noun changes. **Do not reword the four options into FPMRS-specific
language** — their comparability is the reason this item exists.

> **A1.** Over the **past 4 weeks**, thinking about **unique new patients
> requesting FPMRS evaluation**, which best describes your practice's ability to
> accommodate them?
>
> 1. We met all requests, and could **not** have taken more without extending hours → `equilibrium`
> 2. We met all requests, and could have taken **more** without extending hours → `surplus`
> 3. We met all requests, but **only by** extending hours or adding sessions → `shortage_hours`
> 4. We could **not** meet all requests → `shortage_unmet`

> **A2.** Over the **past 4 weeks**, how many **unique new patients requesting
> FPMRS evaluation** did you personally see? *(completed visits only; exclude
> no-shows and cancellations)*
> → `seen_4wk`, numeric, must be > 0; `seen = seen_4wk / 4`

> **A3.** *(branch on A1; not asked for `equilibrium`)*
> Over the same **past 4 weeks**, and in the same unit (**unique new patients
> requesting FPMRS evaluation**):
> - If **surplus**: how many *additional* such patients could you have seen without extending hours?
> - If **shortage_hours**: how many were accommodated *only by* extending hours or adding sessions?
> - If **shortage_unmet**: how many could *not* be accommodated?
>
> → `additional_4wk`, numeric, ≥ 0; if `shortage_hours`, must be **<** A2.
> `additional = additional_4wk / 4`

Aggregation for `capacity_survey_adequacy()`: convert both counts to a weekly
rate (divide by 4), then take the median `seen` and median `additional`
**within each category**, with `n` (or FTE weight) per category. The division is
an analysis step so that respondents are never asked to average.

**Section A is the primary empirical observation and is retained raw.** Whatever
decomposition Sections B and C support later, the effective-system adequacy
computed from A alone is what was measured; everything downstream of it requires
additional assumptions. That distinction survives into the manuscript.

---

## Section B — constraint attribution (new)

Asked only of A1 ∈ {`shortage_hours`, `shortage_unmet`}. "Unable to accommodate"
is an **outcome, not a diagnosis**, and a workforce model that books every
bottleneck as insufficient physician FTE will overstate the shortage.

> **B1.** Which of the following limited the care your practice could deliver?
> *(select all that apply)*
>
> - **Physician clinical capacity** — insufficient urogynecologist sessions/FTE
> - **Clinic operational capacity** — rooms, MA/RN staffing, scheduling staff, template design
> - **Procedural / OR capacity** — OR block time, procedure rooms, anesthesia, inpatient beds
> - **Referral / access constraints** — payer acceptance, geographic reach, institutional rules
> - **Demand pressure** — referral volume exceeding available system capacity

> **B2.** Of those selected, which was the **single most important** constraint?
> *(one only)*

Multi-select gives the decomposition; forced single choice gives something
analytically usable. Both are needed — the multi-select alone cannot be
apportioned, and the single choice alone discards genuine co-limitation.

---

## Section C — physician-attributable capacity (the item the model needs)

Section B records what respondents *believe* constrains them. Section C asks a
counterfactual, which is the quantity the workforce projection actually
requires. Two questions, deliberately concrete, requiring no abstract percentage.

> **C1.** If your practice added **0.5 FTE of urogynecologist clinical time**
> tomorrow, with no other staffing or facility changes, could you meaningfully
> increase the number of patients treated?
> → Yes / No / Unsure

> **C2.** If **clinic staffing, OR access, and procedure capacity** increased
> but urogynecologist FTE did **not**, could you meaningfully increase the
> number of patients treated?
> → Yes / No / Unsure

The joint distribution is the point:

| More physician FTE helps (C1) | More system capacity helps (C2) | Interpretation |
|---|---|---|
| Yes | No | Primarily **physician-constrained** |
| No | Yes | Primarily **system-constrained** |
| Yes | Yes | **Joint / complementary** constraint — both inputs bind |
| No | No | Constraint **not explained by either** intervention |

`Unsure` on either item is a fifth state and is kept as genuinely unidentified.
It is never imputed into one of the four cells. Analyses report best-case and
worst-case bounds by scoring `Unsure` as Yes and as No; if the conclusion turns
on which, the honest output is a range.

`0.5 FTE` rather than "more physician time" because a vague increment invites
respondents to imagine whatever amount would help. Half an FTE is a realistic
recruitment increment and small enough that "yes" is informative.

---

## Section D — supporting measures

Descriptive, and the external-validation link to the mystery-caller work.

| item | why |
|---|---|
| clinical FTE (self) and practice urogynecologist FTE | denominator for every rate below |
| accepting new patients (Y/N) | a full panel and a closed panel are different states |
| new-patient wait, business days | **directly comparable to the audit studies** |
| unused new-patient slots per week | spare capacity that a wait time cannot see |
| operative wait, weeks | separates clinic from OR bottleneck |
| payer restrictions (Medicaid, Medicare, commercial) | the audit found 23% outright Medicaid refusal |
| APP support (FTE, scope) | care delivered by the practice ≠ care delivered by the physician |
| practice setting, region, rurality | non-response weighting and subgroup comparison |

Business-day wait must be defined identically to the audit studies —
Mon–Fri **excluding US federal holidays** — or the comparison measures the
difference between two definitions rather than a change in access.

---

## Scoring specification

**Output 1 — effective-system adequacy.** Section A through
`capacity_survey_adequacy()` unchanged. This is the direct analogue of the
published 94.8%, and it answers "can the practice accommodate current demand?"
It is *not* physician-specific.

**Output 2 — physician-attributable adequacy.** The quantity the workforce model
needs. Among constrained respondents, the shortfall is attributed to physician
capacity only where the counterfactual says additional FTE would convert to
care:

```
physician_attributable_share = P(C1 = Yes | constrained)

adequacy_physician = 1 - (1 - adequacy_effective) * physician_attributable_share
```

So if effective adequacy is 0.90 and 60% of constrained practices say additional
physician FTE would increase care delivered, physician-attributable adequacy is
`1 - 0.10 × 0.60 = 0.94`. **The gap the workforce model should carry is the
smaller one.** Reporting effective adequacy as a physician shortage is the
overstatement this instrument exists to prevent.

Report both, and report Output 1 as the measurement. Output 2 is a
**derivation** that assumes the counterfactual judgements in C1 are accurate and
that constrained practices are exchangeable with respect to it. Neither
assumption is testable from this instrument. The difference between the two is
itself a finding — it quantifies how much of the access problem more
urogynecologists would not fix — but the primary empirical observation remains
effective-system adequacy.

Sensitivity: score `Unsure` as Yes and as No and report the range. Where Yes/Yes
predominates, replace the point attribution with a sensitivity allocation across
the plausible physician share, for the reason given under the decision rule
below: joint constraint is a finding about the world, not a defect in the
measurement.

---

## What this instrument can and cannot identify

**Can:** effective-system adequacy comparable to the published models; the
prevalence and ranking of constraint mechanisms; a physician-attributable share;
whether wait times and panel closure move together with self-reported constraint.

**Cannot:** the *magnitude* of any non-physician capacity shortfall — nobody is
asked how many OR blocks are missing; whether respondents' attributions are
accurate, since B1/B2 are beliefs and C1/C2 are counterfactual judgements, not
measurements; anything about practices that did not respond, which is the
central threat given that the most constrained practices are plausibly the least
likely to complete a survey.

Non-response is the dominant validity risk and should be addressed by design —
sampling frame from the board-certified roster (the model's own denominator, not
a membership list), FTE weighting, and an early-versus-late responder comparison
as a non-response proxy.

---

## Decision rule for a fifth calibration dimension

Deliberately committed **before** the data arrive, so the answer is not chosen
to fit the result.

Add a formal `system_capacity` dimension **only if** all three hold:

1. **Prevalence.** A non-trivial share of constrained respondents name a
   non-physician constraint as most important (B2). Pre-specified threshold:
   **≥ 25%**.
2. **Attribution is identifiable.** Note carefully what this does and does not
   mean. **A predominance of Yes/Yes is NOT a failure of the instrument.** It is
   a substantive finding: urogynecologist time and complementary resources are
   jointly binding, which is a real and reportable state of the world. What
   Yes/Yes leaves unidentified is narrower -- *how much of the observed capacity
   deficit to attribute to physician FTE* -- because a jointly binding
   constraint does not apportion itself.

   So the criterion is not "did respondents discriminate?" but "can the
   attribution share be estimated?" If Yes/Yes predominates, the correct
   response is a **sensitivity allocation** over the plausible physician share,
   reported as a range, rather than a new point estimate or a declaration that
   the decomposition failed.
3. **Consequence.** `adequacy_physician` and `adequacy_effective` differ enough
   to move the balance-reversal threshold beyond its reporting precision.

**Criterion (1) is a model-governance threshold, not a scientific cutoff.**
There is nothing special about 25%; it is a pre-registered line drawn to stop
the decision being made after seeing the data. Criteria (2) and (3) are the ones
carrying scientific weight. A 20% non-physician bottleneck that moves the
balance-reversal threshold substantially matters more than a 30% one that
changes nothing, and if that case arises the governance threshold should be
overridden explicitly and in writing -- not quietly.

If (2) cannot be satisfied even by a sensitivity allocation, system capacity
stays explanatory metadata on `baseline_adequacy`, with the reason stated. If
(1) and (2) hold but (3) fails, record it as a finding — the decomposition is
real but does not change the projection — and still keep it as metadata: a
dimension that never changes a verdict is scaffolding, and the estimand table is
only useful while every row in it can bite.

## Not in version 1

Bottleneck capacity, `C_effective = min(C_physician, C_clinic, C_OR)`, is the
natural eventual form and is **not** proposed for the first national paper. The
survey should collect what a future bottleneck model would need without the
projection assuming that structure now. Collecting the information is cheap;
committing the model to a functional form the data have not yet supported is not.
