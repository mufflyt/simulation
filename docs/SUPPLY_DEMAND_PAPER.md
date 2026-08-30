# Projected Supply and Need for Urogynecologic Care in the United States to 2050: A Microsimulation and Threshold Analysis

**DRAFT. Not submission-ready.** The demand arm of this model is not calibrated
to an independent national anchor (Table 1). This draft is written so that its
conclusions do not depend on the uncalibrated quantities: it reports the
conditions under which the projected balance reverses rather than a point
estimate of surplus or shortfall. Read the Limitations before quoting any
number. Companion validation study: `VALIDATION_PAPER.md`.

## Precis

A microsimulation projects that the urogynecology workforce overtakes projected
need around 2031, and reversing that conclusion would require a base-year
adequacy below 0.855 or demand growth nearly twice the modeled rate.

---

## Abstract

**OBJECTIVE:** To project the supply of urogynecology and reconstructive pelvic
surgery (URPS) physicians in the United States through 2050 using an
individual-level microsimulation, and to determine what would have to be true
about unmet need for the projected balance between supply and need to reverse.

**METHODS:** We built a stochastic microsimulation in which individual
physicians are reconstructed from national board-certification cohorts, age,
retire according to empirical physician hazards, and are replaced by fellowship
entrants. Clinical capacity was expressed as full-time equivalents (FTE).
Required FTE was derived from a base-year adequacy parameter and projected
forward on demographic change. Because no URPS capacity survey exists, base-year
adequacy was taken by analogy from a published physical-therapy workforce
survey, and each demand dimension carries a declared calibration status. Rather
than report a single projected gap, we computed the base-year adequacy and the
demand growth rate at which the projected 2050 balance changes sign. Fourteen
prespecified scenarios varied retirement timing, fellowship output, and
late-career clinical effort. The supply arm was validated out-of-time against
the observed 2023 certification count in a companion study.

**RESULTS:** Projected supply rose from 1,306 FTE in 2025 to 1,760 FTE in 2050
(Monte Carlo range 1,346 to 2,187), an increase of 34.8%, while required FTE
rose 15.2% to 1,587. The projected balance crossed from deficit to surplus in
2031 and reached +173 FTE by 2050. Thirteen of 14 scenarios ended 2050 above
required FTE; only the combined pessimistic scenario did not (1,490 FTE). The
2050 conclusion reversed only if base-year adequacy fell below 0.855, meaning a
base-year shortfall of 14.5% or worse, or if required FTE grew by more than
27.8% rather than the modeled 15.2%. Both published adequacy analogues, 0.948
and 0.894, sit above the reversal threshold.

**CONCLUSION:** Under every adequacy value for which published evidence exists,
this model projects that URPS supply growth outpaces demographic growth in need.
The conclusion is sensitive to a single unmeasured parameter, base-year
adequacy, and the threshold at which it reverses is now quantified. A URPS
capacity survey would settle it.

---

## Introduction

Whether the United States trains enough urogynecologists is usually argued from
headcount trends. Symptomatic pelvic floor disorders affect approximately one in
four adult women, and prevalence rises steeply with age,[1] so an aging
population is expected to increase need. Fellowship positions, however, are
fixed by accreditation and match capacity, and the subspecialty is young enough
that its retirement wave has not yet arrived.

Counting physicians does not answer the question. A workforce is adequate or
inadequate relative to the care its population requires, and the quantity that
matters is clinical capacity rather than credentials. Two models can agree
exactly on how many urogynecologists will be certified in 2050 and disagree
completely on whether that is enough, because they disagree about need.

The difficulty is that need is the harder side to measure, and workforce
projections routinely present it with a confidence their evidence does not
support. A systematic review of 40 health workforce projection studies found
that 8 compared predictions with historical data and only 4 conducted external
validation, identifying model validity and transparent reporting as the field's
principal weaknesses.[2] Demand assumptions are rarely separated from demand
evidence.

We take a different approach to that problem. Rather than assert a projected
shortfall, we make the weakest link explicit and ask how strong it would have to
be to change the answer. The model reports, for each dimension of demand,
whether the input is calibrated evidence or a declared assumption, and the
principal result is the value of the unmeasured parameter at which the projected
balance changes sign.

---

## Materials and Methods

### Model structure

We built a stochastic microsimulation of individual URPS physicians. Each agent
carries an age, sex, certification year, and clinical effort. Agents age one
year per cycle, retire according to age-specific empirical physician hazards,
and may leave the specialty before age 50 through a permanent career-change
process. New agents enter as fellowship graduates. The model runs from 2025
through 2050 with 200 Monte Carlo iterations, and results are reported as the
median with a Monte Carlo range.

The base-year cohort is reconstructed from national certification counts by
certification year rather than drawn synthetically, which preserves the age
structure implied by the subspecialty's establishment. This is a reconstruction,
not a roster: the contract ships aggregate counts without age, sex, or state,
and age is observed for fellowship cohorts and assumed for the founding cohort.

Supply is expressed as clinical FTE rather than headcount, using age- and
sex-specific patient-care hours, because capacity rather than credential count
is the quantity a planner needs.

### Estimating required FTE

Required FTE in the base year is the supply that would be needed for the current
population to be adequately served. It is obtained by dividing base-year
clinical FTE by a base-year adequacy parameter, the proportion of need currently
met. Required FTE is then projected forward on demographic change in the
size and age structure of the adult female population.

No capacity survey of urogynecologists exists. Base-year adequacy was therefore
taken by analogy from a published physical-therapy workforce survey, which
yields 0.948. A second published analogue yields 0.894. **The base-year gap is
therefore not a measurement. It is the adequacy assumption restated:** with
supply of 1,306 FTE and adequacy of 0.948, required FTE is 1,377 by
construction, and the resulting 71 FTE base-year deficit contains no information
that was not put into the adequacy parameter.

### Declared calibration status

Every demand dimension carries an explicit status (Table 1). We report these
rather than presenting demand as uniformly estimated, because none of the four
is calibrated to an independent observed series -- two are derived by analogy,
one is an explicit placeholder, and one is evidence-anchored but not fitted --
and a reader cannot weigh the projection without knowing which is which.

### Threshold analysis

Because base-year adequacy is assumed rather than measured, the projected gap
inherits that assumption directly and a point estimate of surplus or shortfall
would be uninformative. We therefore computed two reversal thresholds:

1. the base-year adequacy at which projected 2050 supply exactly equals required
   FTE, holding demographic growth fixed; and
2. the growth in required FTE from 2025 to 2050 at which the same equality
   holds, holding base-year adequacy at the shipped analogue.

A conclusion that survives across the full range of values a parameter could
plausibly take does not depend on knowing the parameter.

### Scenarios

Fourteen prespecified scenarios varied retirement timing (2 and 5 years earlier,
2 years later), fellowship output (10% expansion, constrained), late-career
clinical effort, and combinations. Demand-side scenarios were specified but did
not alter the required-FTE series in this run, and we do not report them as
demand sensitivity analyses.

### Validation

The supply arm was validated out-of-time in a companion study: forecasts
generated from information available at a 2020 cutoff were compared with the
observed 2023 national certification count. That study found that most of the
disagreement between projected and observed values arose from a definitional
mismatch between the projected quantity and the validation target rather than
from behavioral assumptions. The demand arm has no comparable validation.

---

## Results

### Projected supply and required FTE

Projected clinical supply rose from 1,306 FTE in 2025 to 1,760 FTE in 2050, an
increase of 34.8%, with a Monte Carlo range at 2050 of 1,346 to 2,187. Required
FTE rose 15.2% over the same period, from 1,377 to 1,587. Supply growth exceeded
growth in required FTE throughout.

**Table 1. Calibration status of each demand dimension**

| Dimension | Status | What that means |
|---|---|---|
| Care seeking | Evidence-anchored, not calibrated | Literal per-condition probabilities (0.48 UI, 0.52 POP, 0.38 AI) declared `evidence_anchored` by the model, not fitted to an observed care-seeking series |
| Disease burden | Derived by analogy | Structure adopted from another specialty |
| Access barriers | Uncalibrated, illustrative | Placeholder values |
| Baseline adequacy | Derived by analogy | Borrowed from a published physical-therapy survey; the tier is declared, not inferred |

**None of the four dimensions is calibrated.** National demand anchors are
`illustrative_fallback` placeholders, and the reported 3.6% backtest error is
circular, because the scaling factors were fitted to the same anchors they are
scored against.

Care seeking additionally cannot be identified on its own. Claims and survey
data recover the *product* of recognition, care seeking, referral and arrival,
never its components, so a separate care-seeking probability multiplied by the
others double-counts losses already inside the measured quantity. The model
therefore retires care seeking as an independent multiplier in favour of a
single annual entry rate, which is not yet sourced (see Limitations).

**Table 2. Projected supply and required FTE, baseline scenario**

| Year | Supply FTE (median) | Monte Carlo range | Required FTE | Balance |
|---:|---:|---:|---:|---:|
| 2025 | 1,306 | 1,306 to 1,306 | 1,377 | −71 |
| 2030 | 1,437 | 1,348 to 1,549 | 1,443 | −6 |
| 2035 | 1,545 | 1,375 to 1,752 | 1,488 | +57 |
| 2040 | 1,644 | 1,386 to 1,928 | 1,519 | +125 |
| 2045 | 1,709 | 1,372 to 2,075 | 1,550 | +159 |
| 2050 | 1,760 | 1,346 to 2,187 | 1,587 | +173 |

The projected balance crossed from deficit to surplus in **2031**.

### What would have to be true for a shortfall

**Table 3. Base-year adequacy and the 2050 balance**

| Base-year adequacy | Interpretation | Required FTE 2050 | Balance 2050 |
|---:|---|---:|---:|
| 0.948 | published analogue | 1,587 | +173 |
| 0.894 | second published analogue | 1,683 | +77 |
| 0.860 | | 1,750 | +10 |
| **0.855** | **reversal threshold** | **1,760** | **0** |
| 0.850 | | 1,770 | −10 |
| 0.800 | | 1,881 | −121 |

The projected 2050 surplus reverses only if base-year adequacy is below 0.855,
that is, if the current unmet need exceeds 14.5%. Both published analogues sit
above that threshold, so within the range for which any published evidence
exists, the direction of the conclusion does not change.

The second threshold concerns growth rather than level. Holding adequacy at
0.948, required FTE would have to grow by 27.8% between 2025 and 2050 to absorb
projected supply, against the 15.2% the demographic model produces. Demand
growth would have to be approximately 1.8 times the modeled rate.

### Scenarios

At 2050, 13 of 14 scenarios ended above required FTE. The exception was the
combined pessimistic scenario, at 1,490 FTE against 1,587 required. Scenario
medians at 2050 ranged from 1,490 (combined pessimistic) to 1,920 (combined
investment). Fellowship output and late-career clinical effort moved the 2050
projection more than retirement timing did: shifting retirement 5 years earlier
changed the 2050 median by 11 FTE, whereas constraining fellowship output
changed it by 111 FTE.

---

## Discussion

Within the range of adequacy values for which published evidence exists, this
model does not project a urogynecology workforce shortfall. Projected supply
grows roughly twice as fast as demographically driven need and overtakes it
around 2031. That conclusion holds at both published adequacy analogues and
across 13 of 14 scenarios.

The result should be read as conditional, and the conditions are now explicit.
The single parameter that governs it is base-year adequacy, and it is not
measured for this subspecialty. What the threshold analysis adds is a bound: a
reader who believes current unmet need exceeds 14.5% should reject the
conclusion, and a reader who believes it is smaller should accept it, at least
as far as demography is concerned. That is a more useful statement than a point
estimate whose precision would be inherited entirely from an assumption.

The more fragile input is not the level of need but its growth. The model grows
required FTE on demographic change alone, at 15.2% over 25 years, and demand
would need to grow at 1.8 times that rate to absorb projected supply. Anything
that raises care-seeking rather than population, wider insurance coverage,
greater willingness to seek care for incontinence, or a shift in surgical
thresholds, acts on exactly that term. Our access-barriers dimension, which is
where such effects would enter, is the dimension carrying placeholder values.
The honest reading is that the supply side of this question is now reasonably
well characterized and the demand side is not.

This asymmetry is worth stating plainly, because workforce projections are
usually presented with both sides equally confident. Our supply arm rests on
individual-level agents, empirical retirement hazards, and an out-of-time
validation against a subsequently observed national count. Our demand arm rests
on a base-year parameter borrowed from physical therapists, national anchors
that are round placeholder numbers, and no dimension calibrated to an observed
series. Presenting a single projected shortfall
would have concealed that difference behind a number.

Fellowship policy has more leverage here than retirement policy. Constraining
fellowship output moved the 2050 projection ten times as much as shifting
retirement 5 years earlier. If the subspecialty wishes to influence its 2050
capacity, the effective instrument is training volume and late-career clinical
effort, not retirement behavior.

### Limitations

The demand arm is not calibrated to an independent national anchor and should
not be quoted as an estimate of need. Two of four demand dimensions are
uncalibrated or undeclared, the national anchors are placeholders, and the
reported demand backtest error is circular. No point estimate of surplus or
shortfall from this model is defensible, which is why none is offered as a
finding.

Base-year adequacy is borrowed from a physical-therapy workforce survey. Whether
physical therapy is an appropriate donor specialty for urogynecology is
unestablished, and the choice of donor moves the base-year gap materially.

The base-year cohort is reconstructed from aggregate certification counts, not
observed from a roster. Age is assumed for the founding cohort. Sex, geography,
and practice setting are not observed at the individual level.

Required FTE grows on demographic change only. Changes in care-seeking,
insurance coverage, surgical indications, or delegation to advanced practice
providers are not projected, and each would act on the growth term to which the
conclusion is most sensitive.

Supply intervals are Monte Carlo ranges over parameter and individual
stochasticity, not validated forecast intervals. The companion validation study
found the interval machinery poorly calibrated against a single historical
target.

Finally, this is a national projection. Access to pelvic-floor care is
geographically concentrated, and a national balance is consistent with severe
local shortage.

---

## References

1. Nygaard I, Barber MD, Burgio KL, et al. Prevalence of symptomatic pelvic
   floor disorders in US women. *JAMA*. 2008;300:1311-1316.
2. Lee JT, Crettenden I, Tran M, et al. Methods for health workforce projection
   model: systematic review and recommended good practice reporting guideline.
   *Hum Resour Health*. 2024;22:25.
3. Zarek P, et al. Current and projected future supply and demand for physical
   therapists from 2022 to 2037. *Phys Ther*. 2025;105:pzaf014.
4. Dall TM, Chakrabarti R, Iacobucci W, et al. The complexities of physician
   supply and demand: projections from 2018 to 2033. IHS Markit for the
   Association of American Medical Colleges; 2020.
