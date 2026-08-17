# AI (anal/faecal incontinence) pathway topology audit

The AI limb is mis-specified **upstream of recurrence**. This audit classifies
every current row, specifies the corrected topology, and records why the
correction is not yet applied to the live cascade.

**No numerical AI probability is sourced, calibrated or invented here.**

Prepared 2026-08-17. Companion to `docs/RECURRENCE_ENDPOINT_CONTRACT.md` §3.

---

## 1. Every current AI row, classified

| stage | service | per_entering | p_advance | correct classification | verdict |
|---|---|---:|---:|---|---|
| conservative | new_consultation | 1.00 | 0.30 | first specialty entry | **BLOCKED** — stock-as-flow (`ai_incident_entry`) |
| conservative | return_visit | 1.50 | 0.30 | conservative treatment | plausible placement |
| testing | urodynamics | 0.25 | 0.25 | diagnostic/evaluation | **WRONG MODALITY** — see §3 |
| procedure | **ptns** | 0.70 | 1.00 | **conservative / nondefinitive** | **MIS-SPECIFIED** — see §2 |
| followup | postoperative_care | 1.00 | 0.10 | post-treatment maintenance | depends on §2 |
| followup | return_visit | 0.50 | 0.10 | post-treatment maintenance | depends on §2 |
| recurrence | new_consultation | 1.00 | NA | late recurrent-care | undefined until §2 |
| recurrence | return_visit | 0.80 | NA | late recurrent-care | undefined until §2 |

Two states have **no row at all**: *definitive intervention* and
*early/persistent failure*. A third — *retreatment* — exists for UI and POP
(`recurrence → sling_procedure` / `prolapse_procedure`) but **not** for AI.

## 2. PTNS is not a definitive procedure

Percutaneous tibial neuromodulation is a course of office-based sessions and
sits in the **nonsurgical** treatment evidence. It currently occupies the
`procedure` stage, which means the AI limb has no definitive index treatment —
and therefore *recurrence after definitive treatment* is undefined for AI. That
is a more basic defect than the wrong recurrence value.

The definitive AI interventions are:

| treatment | applicability |
|---|---|
| **sacral neuromodulation (SNM)** | first-line surgical option, **with or without** a sphincter defect |
| **sphincteroplasty** | selected patients with an **external sphincter defect**; functional results commonly deteriorate over time |

Neither is in `URPS_CPT_BASKET`. They must **not** be averaged into one generic
"AI operation": their eligibility populations differ, and so do their
post-treatment processes (§4).

### Corrected topology

```
prevalent symptomatic AI/FI
        ↓
first specialty-care entry
        ↓
evaluation  (office clinical · anorectal manometry · endoanal ultrasound)
        ↓
conservative management
   ├── bowel optimisation
   ├── pelvic-floor rehabilitation / biofeedback
   └── PTNS                                   ← moved HERE, not deleted
        ↓
persistent treatment-requiring FI
        ↓
definitive intervention
   ├── SNM pathway                            ← eligibility independent of defect
   └── sphincteroplasty pathway               ← external sphincter defect only
```

### Why this is not yet applied to the live CSV

`PATHWAY_STAGES` must be contiguous from `conservative`, so removing PTNS from
`procedure` leaves that stage empty and breaks `validate_condition_pathway()`.
Filling it requires (a) SNM and sphincteroplasty rows and (b) **the fraction of
patients going to each** — which is exactly the kind of number this contract
forbids inventing.

Relocating PTNS's `per_entering = 0.70` into conservative management would also
silently re-purpose a utilization quantity measured for a different state.

So the live row is **marked mis-specified in its own notes** and left
numerically unchanged. The AI limb already refuses via `ai_incident_entry`, so
nothing is being reported from it meanwhile.

**Unblocked by:** an SNM/sphincteroplasty treatment-share source, plus the
corresponding CPT additions to `URPS_CPT_BASKET`.

## 3. The diagnostic placeholder is a urinary test in a bowel pathway

`ai/testing/urodynamics` is an explicit stand-in. The row says so. The real AI
diagnostics are anorectal physiology/manometry and endoanal ultrasound.

**Do not simply rename it.** Its `per_entering = 0.25` and its workload/cost
mapping were derived for *urinary* testing; renaming the label would transport a
urinary procedure's workload into the bowel pathway and make the error harder to
see, not easier.

Distinguish at minimum:

```
office clinical evaluation
anorectal physiology / manometry
endoanal ultrasound
```

Each unresolved until sourced. Leaving them unresolved is preferable to
transporting.

## 4. SNM and sphincteroplasty need DIFFERENT post-treatment models

This is the AI analogue of the POP `0.40` error, and it matters more here.

### Sphincteroplasty — a recurrence convolution fits

```
index repair → early/persistent failure → late demand-generating recurrent FI care
```

The existing `compute_recurrence_convolution()` framework is appropriate once
evidence with a compatible endpoint is found.

### SNM — TWO separate demand processes

```
successful test → implant → active device state
                                ├── A. recurrent / lost-efficacy FI care
                                └── B. device management
                                       (reprogramming · revision · replacement · explant)
```

**A device revision is not necessarily recurrent FI, and recurrent FI is not
necessarily a device revision.** Long-term SNM follow-up carries a substantial
revision/replacement/explant burden — in one multicentre cohort, 27 of 76
patients followed ≥5 years underwent at least one such device procedure — and
explantation occurs for causes including infection as well as loss of efficacy.

Collapsing B into A would repeat exactly the mistake `0.40` represents: a
retreatment/device-procedure rate standing in for a clinical recurrence
probability.

## 5. Recurrence-contract extension

`condition × index_treatment` now explicitly admits:

| group | endpoint status |
|---|---|
| `ai × sphincteroplasty` | may ultimately use a recurrent-care convolution |
| `ai × snm` | **unresolved** — the model must first decide whether it needs loss-of-efficacy recurrence, device-management workload, or **both as distinct quantities** |

Registered in `config/recurrence_evidence.csv`, `kernel_compatible = FALSE`.

## 6. Observed evidence, and the states it exposes

`config/ai_treatment_evidence.csv` now carries real rates. Two structural
findings follow from them.

### SNM has a TEST state the model lacks

```
persistent treatment-requiring AI
        ↓
   SNM test / stage 1
        ↓  P(implant | stage 1) ~ 0.797   [NY 2011-2014, FI-specific]
   permanent implant
        ↓
   device-maintenance state
```

Progression from test to permanent implant is **well below 100%**, so the test
phase must not be collapsed into the implant state. Both generate workload, and
they are different workloads.

### Device maintenance is NOT recurrence

```
permanent SNM implant
      ├── routine device follow-up
      ├── reprogramming            (loss of stimulation, adverse stimulation,
      │                             troubleshooting)
      ├── revision / replacement / explant   ~6.5% after stage 2
      └── recurrent / lost-efficacy FI care  ← the ONLY branch g_k may count
```

A revision proportion measured on implanted devices is a **device-maintenance
rate**, not a clinical recurrence probability. Reprogramming is attempted before
revision is considered, so even the revision count understates the maintenance
workload while overstating nothing about recurrence.

### The definitive-treatment rates, and why they are not yet canonical

| quantity | value | denominator |
|---|---:|---|
| any studied treatment | **0.096** | women 65+ with an FI diagnosis (n = 33,010) |
| anal procedures | 0.065 | same |
| SNM | **0.024** | same |
| PTNS | 0.009 | same |
| sphincteroplasty | **0.004** | same |
| PFPT / biofeedback | 0.001 | same |

The five modality rates sum to **0.103** against an any-treatment rate of
**0.096** — an excess of +0.007, so the categories are **not mutually
exclusive** and must not be treated as a partition.

`P(SNM | SNM or sphincteroplasty) = 0.024/0.028 = 0.857` is recorded as a
**descriptive conditional mix**, never a pathway probability: its denominator is
the wrong one, and it is derived from two non-exclusive rates.

**Neither 0.024 nor 0.004 is inserted.** Their denominator is *claims-diagnosed
FI among older Medicare women*, not the model's *persistent treatment-requiring
AI* state. Transport must be explicit and is not yet specified.

### The larger finding: this is realized care, not need

**Only 9.6% of women with a claims FI diagnosis received any studied treatment**
over a median ~2.5-year follow-up, and receipt varied with age, dual
eligibility, poverty, comorbidity and race.

That is direct evidence for the concern raised about
`annual_first_urps_entry_rate`: a claims-observed treatment probability is a
**realized-care transition under access**, not a latent-need probability. The
architecture should eventually be

```
latent treatment need  →  realized treatment under access
```

rather than one claims-derived parameter serving as both.

## 7. What is deliberately NOT decided

- The fraction of definitive AI treatment going to SNM vs sphincteroplasty.
  ASCRS notes sphincteroplasty use has fallen substantially while SNM is an
  established surgical option, so SNM may dominate future national workload —
  but that should be **measured, not assumed**.
- Any AI probability, hazard, or utilization value.
- Whether the AI recurrence stage should gain a retreatment/procedure row (it
  has none today, unlike UI and POP).
