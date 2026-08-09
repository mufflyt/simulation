# Historical Validation of National Urogynecology Workforce Projections: When the Validation Target Does Not Match the Workforce

**Submission draft for _Obstetrics & Gynecology_. Second choice: _Urogynecology_.**

Supporting detail is in `VALIDATION_SUPPLEMENT.md`. Underlying diagnostics remain
in `BACKTEST_2020_TO_2023.md`, `RESULTS_INTERVAL_CALIBRATION.md`,
`VALIDATION_RESULTS.md`, and `BACKTEST_CALIBRATION_AUDIT.md`.

## Precis

National urogynecology workforce projections were more sensitive to how the
workforce was counted than to assumptions about physician behavior, and
conventional validation could not detect the difference.

---

## Abstract

**OBJECTIVE:** To evaluate agreement between historical projections of the
clinically active urogynecology and reconstructive pelvic surgery (URPS)
workforce and subsequently observed national certification counts, and to
determine how definitional differences between the two affect apparent
forecast performance.

**METHODS:** We built a national stochastic microsimulation of the URPS
workforce, forecast physician headcount from information available through 2020,
and compared it with the observed 2023 board-certified count
(n = 1,306). Four specifications were prespecified and a fifth using
residency-match data added subsequently, each run with and without attrition
(10 configurations). A career-change hazard published after
the forecast origin was omitted. Agreement was summarized by percentage
difference, containment, and interval score. Temporal validation
used rolling origins from 2017 to 2020, admitting only training windows whose
outcomes were observable at the origin, and compared with leave-one-out
validation, which does not. Secondary analyses examined five cutoff-target windows and two
entrant definitions.

**RESULTS:** The projected workforce differed from the observed 2023
count by 99 providers (−7.6%). Fifty-nine percent of this
discrepancy was attributable to definitional mismatch: the model removed
physicians as they retired, whereas the certification series removes no one.
Forty-one percent reflected faster-than-assumed entry; assumptions about
physician behavior accounted for essentially none. The forecast with the highest
containment had the worst interval score: it contained the observation at all
four rolling origins but averaged 1,466 providers in width, with one lower bound
of −594.5; the definition-matched forecast was 15 times sharper (interval score
137 versus 1,466).
Leave-one-out validation made the same model appear 2.7 times closer with 3.6
times tighter intervals.

**CONCLUSION:** Historical validation can make an appropriate model
appear inaccurate when the validation target measures a different construct.
Workforce projections should define the clinical quantity before model
construction and, where possible, be validated against data measuring active
practice rather than cumulative certification.

---

## Introduction

Whether the United States will have enough urogynecologists to meet the needs of
an aging female population is an important question for the subspecialty.
Symptomatic pelvic floor disorders affect approximately one in four adult U.S.
women, and prevalence rises steeply with age.[1] Workforce projections may
influence fellowship training, geographic access to pelvic-floor care, and
planning for future clinical capacity. Yet these projections are rarely tested
against what subsequently occurs.

The gap is not particular to urogynecology. A systematic review of 40 health
workforce projection studies found that 8 (20%) compared predictions with
historical data and only 4 conducted external validation, identifying model
validity and transparent reporting as the field's principal weaknesses.[2] Where
validation has been done, it has compared projected totals against observed
totals. A historical comparison of Dutch
general-practitioner projections for target years 1998 to 2011 reported mean
absolute percentage errors of 1.9% to 14.9% and attributed the error to bias
rather than variance.[3]

Such comparisons do not ask whether the quantity the model simulates is the same
quantity used to check it. A workforce model simulates a stock of practicing
physicians. It is typically validated against a registry, such as board
certifications or licenses, assembled for a different purpose. If the model
removes physicians who retire and the registry never does, the two series measure
different things, and the resulting discrepancy will look like a flawed
retirement assumption to anyone examining only the residual.

We built a national microsimulation of the urogynecology and reconstructive
pelvic surgery (URPS) workforce and asked whether it reproduces the observed
number of board-certified urogynecologists when the forecast is made from
historical data alone. It does not, and the reasons matter more than the
difference itself. Our objective was to determine how much of the disagreement
is attributable to the definitions of the projected and observed workforces
rather than to assumptions about physician behavior, and whether conventional
validation summaries distinguish the two.

---

## Materials and Methods

### Validation design and outcome

We conducted a historical validation in which forecasts were generated using
only information available at a 2020 cutoff and were then compared with what was
subsequently observed. No model parameter was retuned after the observed value
was examined. The validation outcome throughout is physician headcount rather
than clinical full-time equivalents.

### Benchmark and estimand

The comparison series was the national count of board-certified URPS physicians
in 2023 (n = 1,306), comprising physicians certified through the American Board
of Obstetrics and Gynecology or the American Board of Urology and keyed on
subspecialty certification year. Automated checks verified geography, board
pathway, measure, and certification-year basis before any comparison, failing
rather than warning on mismatch.

This series is a cumulative certification count: the number retired is zero in
every year and the active count equals the ever-certified count. The workforce
model estimates a different quantity, the clinically active workforce, from
which physicians exit. Because the certification benchmark does not decrement
physicians after retirement, paired no-attrition configurations were used to
quantify the contribution of this definitional mismatch.

### Model configurations

Four model specifications were prespecified before the 2023 result was examined,
varying baseline cohort construction and workforce entry. A fifth specification
using residency-match data was added subsequently and evaluated alongside the
original four rather than in place of them. Each specification was run with and
without attrition, yielding 10 configurations.

*Baseline cohort.* The derived cohort reconstructed individual physicians from
the observed certification counts for each certification year, preserving
certification-year structure, with separate age-at-certification distributions
for the backlog of established practitioners certified in or before the first
URPS certification year and for subsequent fellowship-trained cohorts. The
synthetic cohort matched only the aggregate headcount, drawing ages from a
single distribution without certification-year structure. Distributional
parameters are given in the Supplement.

*Workforce entry.* Three approaches were evaluated: the model's shipped
assumption of 55 entrants per year; a rate estimated from pre-cutoff
certification data alone (32.67 per year); and a rate derived from National
Resident Matching Program fellowship match reports published on or before the
cutoff (49.73 per year).

*Workforce exit.* In the primary analysis, physicians left the clinically active
workforce through the age-specific retirement process available at the forecast
origin. Exits were absorbing. The model also implements a permanent 1.42% annual
career-change hazard for physicians under age 50, but that estimate was first
published in 2025 and was therefore omitted from the historical forecast. Its
omission reflects the absence of a contemporaneous estimate for that
parameterization rather than an assumption that no physician under 50 left the
specialty. The production model retains the career-change process, and the
Supplement reports a sensitivity analysis applying it.

Corresponding no-attrition configurations set the retirement hazards to zero, so
that the identical simulation engine produced both comparisons. These
configurations were used only to align the modeled quantity with the cumulative
certification benchmark. They are diagnostic comparisons, not alternative
estimates of the clinically active workforce.

### Simulation and uncertainty

Each configuration was simulated for 1,000 Monte Carlo iterations. The median
simulated headcount was the point projection, and the 2.5th and 97.5th
percentiles defined the 95% Monte Carlo interval.

Entrant-rate uncertainty was estimated from pre-cutoff observations only and
propagated by redrawing the entrant rate within each Monte Carlo iteration. Each
entrant-rate distribution was centered on its corresponding prespecified entrant
assumption, preserving the contrasts among configurations. Retirement was
applied stochastically to individual physicians, but the retirement-hazard
coefficients were held fixed because sampling uncertainty for those parameters
was not available from their source data.

### Measures of agreement

Agreement was evaluated three ways. Percentage difference was calculated as
100 x (projected median − observed count) / observed count. Containment recorded
whether the observed value fell within the 95% Monte Carlo interval. The
interval score, a proper scoring rule,[4] penalizes both interval width and
observations falling outside the interval, so that improved containment obtained
through wider intervals incurs an explicit loss of sharpness.

The ten configurations were alternative specifications evaluated against the
same single 2023 observation and therefore did not constitute ten independent
forecast occasions. The number of intervals containing the observed count is
reported as a containment count rather than as an empirical coverage rate.

### Temporal and sensitivity analyses

To evaluate performance across repeated out-of-time forecast occasions, we
performed rolling-origin validation using origins from 2017 through 2020, each
with a three-year target horizon, producing four comparisons. At each origin,
model fitting was restricted to observations whose outcomes would have been
observable at that time.

We contrasted this with leave-one-out validation, which permits training
observations whose outcomes occur after the forecast origin, to quantify the
effect of future-information leakage on apparent agreement and interval width.
Additional sensitivity analyses evaluated five historical cutoff-target windows,
the alternative certification-based and match-based definitions of workforce
entry, and the effect of applying the post-cutoff career-change hazard.

Model structure, parameter sources and their availability dates, complete
configuration results, and reproducibility procedures are provided in the
Supplement.

### Institutional review

[Insert institutional determination regarding whether this analysis of
aggregate certification counts and physician roster data constituted
human-subjects research, including protocol or determination number if
applicable.]

---

## Results

**Agreement with the observed count.** Across the ten configurations the median
difference from the observed 2023 certification count was −8.3%, ranging from
−3.14% to −16.23%. All configurations fell below the target. Two contained the
observed value, and in both, attrition had been suspended so that the simulated
quantity matched what the certification series counts.

**The discrepancy is dominated by definitional mismatch.** The shipped
configuration differed from the observed count by 99 providers (−7.6%). Table 1
decomposes that difference. Fifty-nine percent came from applying retirement to a
cumulative certification count, a series in which the number retired is zero in
every year and the active count equals the ever-certified count. A further 41%
reflected faster-than-assumed entry, with realized net entry of approximately 69
per year against pre-cutoff assumptions of 32.67 to 55. Assumptions about
physician behavior accounted for essentially none of the difference. Aligning
definitions moved every paired configuration by four to eight percentage points.
Applying the post-cutoff career-change hazard, reported in the Supplement as a
sensitivity analysis, widened the total discrepancy to 127 providers and raised
the definitional share to 68% without altering any conclusion.

**Table 1. Decomposition of the 99-provider discrepancy**

| Step | 2023 level | Change | Share | Per year |
|---|---:|---:|---:|---:|
| Shipped forecast (retirement applied, 55 entrants/yr) | 1,207 | n/a | n/a | n/a |
| Align definitions (attrition suspended on a cumulative count) | 1,265 | +58 | 59% | +19.3 |
| Close entrant-regime residual (realized entry exceeded assumption) | 1,306 | +41 | 41% | +13.7 |

**Containment and interval score disagree.** Table 2 shows the three candidate
forecasts scored over 2021 to 2023. The forecast with the highest containment had
the worst interval score, and the forecast with the best interval score did not
have the highest containment. The rolling-origin forecast contained the
observation at all four origins while carrying a mean interval width of 1,466
providers, including one lower bound of −594.5, an impossible value for a
cumulative count. The definition-matched forecast was approximately 15 times
sharper and scored an order of magnitude better despite lower containment.

**Table 2. Containment versus interval score**

| Forecast | Targets | Contained | Mean width | Interval score |
|---|---:|---:|---:|---:|
| Rolling origin (wide) | 4 | 4/4 | 1,465.6 | 1,465.6 |
| Sharp, attrition applied (definition mismatch) | 3 | 0/3 | 87.0 | 1,086.4 |
| Sharp, attrition suspended (definition matched) | 3 | 2/3 | 97.0 | 137.0 |

**Temporal validation.** All four rolling origins contained the observation, and
interval widths narrowed as the usable training record lengthened, from 3,185 at
the 2017 origin to 687 at the 2020 origin. The earliest origin contained the
observation only because its interval spanned −594.5 to +2,590, reflecting two
prior errors and a t multiplier of 12.71 on one degree of freedom. Containment
was achieved by width rather than by accuracy.

**Validation design.** The identical model and data appeared 2.7 times closer to
the observed series, with 3.6 times tighter intervals, under leave-one-out
validation than under rolling origin (median absolute difference 2.83% versus
7.55%; mean width 412 versus 1,466). Leave-one-out admits training windows whose
outcomes were not observable at the forecast origin; for the earliest origin, all
seven training points lay in its future.

**Window dependence.** Across five cutoff-target windows the direction of the
difference reversed, from +17.6% above the observed count for the 2016 to 2019
window to −8.4% below it for 2020 to 2023. The two entrant definitions also
diverged, with the residency-match-based predictor closer to the observed series
in every window (absolute difference 0.2% to 4.4%) than the certification-based
one (1.9% to 17.6%).

---

## Discussion

The principal finding was not that the workforce model should retain physicians
indefinitely to reproduce cumulative certification counts. The production model
appropriately removes physicians from the clinically active workforce using
empirical URPS retirement hazards because clinical capacity, rather than
accumulated credentials, is the quantity relevant to workforce planning.
Instead, most of the discrepancy between the projected active workforce and the
published 2023 certification count arose because the two quantities were not
operationally equivalent. Suspending attrition improved agreement with the
cumulative certification series while making the simulated workforce less
representative of clinical capacity. Historical validation can therefore make an
appropriate workforce model appear inaccurate when the validation target measures
a different construct.

This inverts the usual order of suspicion. When a workforce projection misses,
attention turns to whether physicians are retiring earlier than modeled or
whether fellowship output is keeping pace. Here the dominant problem was upstream
of both, in the correspondence between what was projected and what was counted. A
model can represent the behavior of urogynecologists faithfully and still produce
a misleading number for fellowship planning.

Conventional validation would have selected the worst forecast. Containment of
the observed value, the summary most workforce studies report, ranked highest
the forecast that the interval score ranked last. A sufficiently wide interval
contains almost anything, and reporting containment alone rewards exactly that;
proper scores were developed for forecast evaluation precisely because coverage
can be bought with width.[5] Apparent agreement also depends on how validation
is designed: admitting information unavailable at the forecast origin improved
apparent accuracy 2.7-fold without changing the model, and the direction of the
difference reversed with the choice of historical window. Any single cutoff-target
comparison would have supported a confident conclusion about the direction of
bias, and the opposite conclusion was equally available.

Three practical consequences follow for urogynecology workforce planning. First,
a projection should state what it counts, whether practicing physicians,
board-certified physicians, ever-certified physicians, or clinical full-time
equivalents, and the validation series should count the same thing. Second, a
projection checked against a single historical target should not be described as
validated. Third, narrow intervals are not evidence of accuracy; one
specification in our supplementary analysis produced an interval 8 providers wide
on a count of 1,273 and did not contain the observation.

The specific mismatch here reflects how one board publishes counts, but the class
of error is not specific to urogynecology. Workforce models simulate stocks of
practicing physicians and are validated against registries assembled for
administrative purposes. Certification series are typically cumulative and rarely
decremented, license files reflect renewal rather than practice, and national
provider identifiers persist after retirement. A model that simulates exit,
scored against a series that never removes anyone, is structurally guaranteed to
under-predict. Because validation is uncommon in this literature,[2] an error
class that surfaces only during validation will be under-detected, and the
validation that does occur compares totals: the Dutch general-practitioner
comparison attributed its error to bias rather than variance[3] without examining
whether the projected and observed quantities were equivalent, which is the
signature a definitional mismatch produces.

### Limitations

There is no gold-standard external validation of the active workforce in this
study, and none is claimed. The only national series available for URPS is
cumulative certification, which counts credentials rather than practicing
physicians. Everything reported here is agreement against an imperfect benchmark.
That does not make the exercise uninformative; it demonstrates what happens when
an imperfect benchmark is treated as truth. A licensure or billing-activity
series would measure clinical practice more closely, and neither is currently
linked to this model.

These are diagnostic findings rather than precise estimates of long-run operating
characteristics. The repeated-target evidence rests on four rolling origins,
which cannot estimate interval calibration with useful precision, and we do not
present the 4 of 4 containment result as evidence of calibration. The
single-target exercise cannot estimate coverage at all: ten configurations scored
against one realized value are alternative specifications, not independent
forecast occasions. The findings are qualitative by design, comprising a reversal
in ranking, a sign change, and a 2.7-fold gap, and none should be quoted as a
calibrated magnitude. The study covers one subspecialty, one country, and one
certification series. Finally, the entrant-regime correction was identified from
the errors it would be scored against and is preregistered for prospective
evaluation rather than reported as validated.

---

## References

1. Nygaard I, Barber MD, Burgio KL, et al. Prevalence of symptomatic pelvic
   floor disorders in US women. *JAMA*. 2008;300:1311-1316.
2. Lee JT, Crettenden I, Tran M, et al. Methods for health workforce projection
   model: systematic review and recommended good practice reporting guideline.
   *Hum Resour Health*. 2024;22:25.
3. Van Greuningen M, Batenburg RS, Van der Velden LFJ. The accuracy of general
   practitioner workforce projections. *Hum Resour Health*. 2013;11:31.
4. Gneiting T, Raftery AE. Strictly proper scoring rules, prediction, and
   estimation. *J Am Stat Assoc*. 2007;102:359-378.
5. Bracher J, Ray EL, Gneiting T, Reich NG. Evaluating epidemic forecasts in an
   interval format. *PLoS Comput Biol*. 2021;17:e1008618.
