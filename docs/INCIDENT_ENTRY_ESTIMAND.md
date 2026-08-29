# Incident entry into care — estimand contract and pre-registered estimator

`condition_service_pathway()` `per_entering` at the **conservative** stage, for
UI, POP and AI.

**Status: UNRESOLVED. The canonical pipeline refuses.** This contract is frozen
*before* data access so the definition cannot be adjusted once the answer is
visible. Nothing here may be estimated from the utilization anchor.

Prepared 2026-08-16/17 on `feat/chia-inpatient-surgical-layer`.

---

## 1. The defect, and why it is not POP-specific

`per_entering` at conservative entry ships as **1.00** for all three limbs. That
converts a prevalence **stock** into an annual **flow**: every prevalent patient
is counted as newly presenting, every year. Verified per limb, independently —
each refuses on its own:

```
ui   treated=2,538,780  -> REFUSES     pop  treated=3,264,807  -> REFUSES
ai   treated=  372,721  -> REFUSES
```

The **recurrence** stage also ships `per_entering = 1.00` and is **correct**
there: its `entering` is already an annual flow (42 per 1,000 treated, versus
1,000 at conservative entry), so one consultation per recurrence event is right.
The rule is therefore semantic, not numeric:

> `1.0` is valid when the denominator is already an annual event flow. It is
> invalid when a prevalent stock is being converted into new annual entrants.

## 2. The estimand

For condition *c*, age band *a*, year *t*:

```
q(c,a,t) =  women NEWLY ENTERING the relevant care pathway
            ------------------------------------------------
            prevalent ELIGIBLE disease stock
```

**"Incident" means incident ENTRY INTO CARE, not incident onset of disease.**
These are different quantities and the distinction is the whole design. A woman
may have UI for years before seeing anyone about it. Claims are excellent at
identifying *first observed care*; they are blind to women who have symptoms and
have never sought care. So a claims-only ratio of "new diagnoses ÷ previously
diagnosed patients" answers the wrong question — its denominator is
already-in-care patients, not the disease stock.

`per_entering` must be treated as a **HAZARD acting on those eligible to enter**,
not a multiplier applied to the whole stock. It must not regenerate a fresh
cohort of "new" patients from the entire prevalent pool each model year.

> **POST-FREEZE CLARIFICATION, 2026-08-29 — superseded wording. Read
> [`PATHWAY_STATE_TRANSITION_AUDIT.md`](PATHWAY_STATE_TRANSITION_AUDIT.md) §8
> before using the word "hazard" here.** Nothing above this note has been
> altered: the pre-registration stands as written, and this paragraph is from
> 2026-08-16/17, predating the §8 ruling. The note is dated so the later
> architectural decision cannot be mistaken for part of the frozen document. §8 adopted the **population-level rate**
> (denominator: *all* eligible prevalent women in the year, regardless of prior
> care history) and **rejected** the conditional hazard whose denominator is
> women who have never entered before. Consequence 1 of that ruling is that the
> quantity **is not a conditional hazard and must not be named like one**; the
> canonical name is `annual_first_urps_entry_rate`.
>
> The distinction is not pedantic. "Hazard acting on those eligible to enter"
> reads as a depleting never-entered risk set, which invites a depletion
> correction on top of a rate that already embeds depletion empirically --
> double-counting history. §8 consequence 2 forbids exactly that. What this
> paragraph gets right, and what survives, is the negative claim: the
> denominator is the DISEASE stock, not already-in-care patients.

## 3. THE DENOMINATOR PROBLEM — resolve before estimating anything

Traced from `R/demand-lifecourse.R:163`:

```
female population
   ↓  × p_ui / p_pop / p_ai            symptomatic prevalence
   ↓  × recognition                    symptom recognised
   ↓  × p_seek                         CARE-SEEKING
   ↓  × p_referral                     referred
   ↓  × p_eligible                     eligible
   ↓  × p_treated                      treated
"treated"  ──→ entering ──→ new_consultation
```

Composition of `treated`, as a share of prevalence:

| limb | recognition | p_seek | p_referral | p_eligible | p_treated | **product** |
|---|---:|---:|---:|---:|---:|---:|
| ui  | 0.55 | 0.45 | 0.40 | 1.00 | 0.70 | **0.0693** |
| pop | 0.60 | 0.50 | 0.55 | 1.00 | 0.65 | **0.1073** |
| ai  | 0.35 | 0.30 | 0.45 | 1.00 | 0.60 | **0.0284** |

**`treated` IS NOT THE PREVALENT ELIGIBLE STOCK.** It is 2.8–10.7% of
prevalence, already carrying care-seeking, referral and treatment. Applying a
hazard whose denominator is the *prevalent eligible stock* on top of `treated`
would count recognition × seeking × referral × treatment **twice**.

Two admissible resolutions; the choice is a modelling decision to be made
deliberately, not inferred here:

- **(A) Re-anchor the hazard.** Define `q` on the prevalent eligible stock and
  restructure the pathway so entry acts there, with `recognition`, `p_seek`,
  `p_referral` and `p_treated` either removed or explicitly reinterpreted. Risks
  discarding structure that is separately sourced.
- **(B) Condition the hazard.** Define `per_entering` as
  `P(new consultation this year | already in the treated subset)` and estimate a
  numerator restricted to that subset. Preserves the existing chain but requires
  the claims cohort to be restricted the same way — and that restriction may not
  be observable in claims.

**Every arrow above needs an explicit mathematical definition and a source
before any number is adopted.** Several currently have neither.

## 4. Numerator — MA APCD as primary

MA APCD CY2024: five years of claims (2020–2024) with member eligibility and
provider files, and longitudinal patient indexing across coverage records.

Three parallel cohorts — UI, POP, AI — each defined as **first observed
qualifying entry into urogynecologic care**:

> First outpatient evaluation by a urogynecology/FPMRS clinician carrying the
> relevant condition diagnosis, after no qualifying urogynecology encounter for
> that condition during the lookback.

Two design commitments:

- **Do NOT use CPT "new patient" codes.** A patient can be clinically new to
  urogynecology yet fail the billing definition because of prior care in the
  same group. The index event is the first qualifying *encounter* after washout.
- **Link rendering NPI to the existing urogynecologist roster**, in preference
  to payer specialty labels. That makes the numerator match the model's own
  `urps_office_visits` construct rather than a payer's taxonomy field.

**Count unique women, not visits.** Ten visits by one new patient are one entry.
Subsequent visits are measured separately — entry rate and
visits-per-entered-patient are different quantities, and conflating them is
plausibly a second instance of the same stock/flow error.

### Incidence years: 2023–2024

**2022 is deliberately NOT primary.** The 2020–2021 pandemic period suppressed
prior care, so returning patients in 2022 would masquerade as incident entrants.
A 2023 index has a full 2020–2022 lookback available.

### Coverage limits, stated up front

MA APCD covers commercial, MassHealth and Medicare Advantage. It does **not**
include **Medicare fee-for-service**, and some self-insured commercial coverage
has been absent since *Gobeille*. Since pelvic-floor burden is concentrated in
older women, this is a material gap, not a footnote — see §6.

## 5. Denominator — from the model's prevalence science, not from claims

```
P(c,a) = N(a) × Pr(eligible prevalent symptomatic c | a)
q(c,a) = I(c,a) / P(c,a)
```

using the prevalence definition already underlying the simulation. A claims rate
such as "1.8 new POP consultations per 1,000 women/year" is **not**
`per_entering` — it is the numerator only.

## 6. Medicare FFS — independent replication, started in parallel

Run the same algorithm in Medicare FFS (beneficiary-level enrollment via the
Master Beneficiary Summary File, longitudinal claims linkage) and compare
age-standardised entry rates at 65+ against the MA APCD estimate.

Concordance between two independent populations is **far stronger evidence than
fitting the parameter until national utilization comes out right.**

CMS RIF processing commonly takes **3–5 months**. Start the request now, in
parallel — it must not become the critical path.

## 7. MEPS — validation, not estimation

MEPS HC-254G (2024 office-based visits) supports a national sanity check: do the
implied annual counts of women receiving office-based care live in the right
universe? It is **not** the estimator — condition-specific samples thin out
quickly, especially for POP and AI.

## 8. Pre-registered sensitivity matrix

Fixed before the answer is seen.

| dimension | primary | sensitivities |
|---|---|---|
| washout | 24 mo | 12, 36 mo |
| index event | first qualifying URPS encounter | first any-specialist encounter |
| diagnosis | dx on index claim | dx ±90 days |
| enrollment | continuous | allow 1-month gap |
| provider | roster-linked FPMRS | specialty/taxonomy |
| years | 2023–24 | 2022–24 |
| case rule | ≥1 qualifying claim | ≥2 dx, or dx + procedure |

**If `q` swings from 0.08 to 0.40 across reasonable case definitions, that
uncertainty belongs in the simulation.** It must not be hidden behind a point
estimate.

## 9. Stratification — not one scalar

Estimate by the simulation's existing age bands (18–44, 45–54, 55–64, 65–74,
75+) and stratify by payer (commercial, MassHealth, Medicare Advantage). The
target is an **age-specific entry hazard**, not three replacement constants.

## 10. The 0.297 ratio is a HOLDOUT, never an estimator

| | |
|---|---:|
| predicted `urps_office_visits` | 16,226,458 |
| target | 4,814,760 |
| ratio | **0.297** |

Only *after* the entry estimates are locked and inserted **without calibration**
do we ask whether the discrepancy collapses.

- If the independently measured correction moves output toward 0.297 → strong
  evidence the stock/flow error drove the mismatch.
- If the empirical shares are ~0.10 or ~0.60 → **do not tune them.** That says
  another pathway term is also wrong.

## 11. Same cohort, next question: the recurrence limb

Follow each incident entrant forward to estimate conservative management after
entry, time to treatment, probability of procedure, repeat visits per episode,
recurrence/re-entry, and censoring. That attacks the known **0.12 / 0.40**
recurrence problem with the same data rather than a second expedition.

## 12. Questions this contract must answer before code

What is the prevalent stock · who is eligible to enter · what constitutes entry ·
can a person enter more than once · what resets eligibility · what is recurrence
rather than incident entry · what is the claims numerator · what is the external
prevalence denominator · what age strata · what washout · what censoring and
enrollment rules · what source supplies each quantity.

§3 is the open one and blocks the rest.

## 13. Disease incidence literature is a plausibility check only

Longitudinal studies of incident symptomatic POP describe **onset/progression of
disease**, not first entry into care. Useful for bounding, never a substitute.

## 14. Blocking status

- Guard stays wired; the canonical pipeline stays refusing.
- `per_entering` stays unresolved. **No placeholder.**
- `R CMD check` green / `scientific-readiness` red is deliberate — the package
  behaves correctly while the canonical configuration is not scientifically
  runnable.
- Blockers `ui_incident_entry`, `pop_incident_entry`, `ai_incident_entry` under
  `category=conservative_incident_entry`, emitted as `::SCIENTIFIC-BLOCKER::`
  markers at exit 1.
- Fixtures working around it — `valid_pathway()` in
  `tests/testthat/helper-setup.R` and `.github/scripts/_pathway_fixture.R` —
  are labelled as fixtures, not candidate values, and are to be **deleted** once
  the parameter is sourced.
- Priority order: **estimand contract → resolve §3 denominator → APCD cohort →
  age-specific first-care incidence → prevalence-denominator alignment →
  Medicare FFS replication → insert without calibration → observe the
  8.51× / 0.297 discrepancy → recurrence limb.**
