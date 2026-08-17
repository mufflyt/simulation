# Recurrence endpoint contract

What `g_k` in `compute_recurrence_convolution()` is permitted to count.

**Status: FROZEN as a contract, UNRESOLVED as parameters.** No recurrence
probability is estimated or inserted here. The convolution is scientifically
usable only once `condition × index_treatment × recurrence_endpoint` is defined,
and this document defines it.

Prepared 2026-08-17. Companion to `docs/POP_RECURRENCE_ESTIMAND_AUDIT.md`.

---

## 1. The modelled endpoint

The workforce model predicts **demand-generating recurrent care**, not anatomic
recurrence and not a clinical-trial composite failure.

```
definitive index treatment
         ↓
    grace period ──────────→ early_failure_or_persistence   (still workload)
         ↓
 recurrence-free state
         ↓
first LATE demand-generating recurrent-care episode          ← g_k counts THIS
         ↓
 management after recurrence
   ├── observation
   ├── conservative treatment
   └── repeat procedure                                      ← 0.40 belongs here
```

`g_k` = probability that the **first late recurrent-care episode** occurs during
time-since-treatment interval *k*.

**`g_k` must NOT be:** anatomic failure alone · symptoms alone · composite trial
failure · cumulative retreatment · repeat operation · "any recurrence ever".
Those are validation or downstream quantities, and several of them are what the
available evidence actually reports.

### Early failure is workload, not zero

Grace-period events are excluded from `g_k` but **not discarded**. Total
post-treatment demand is

```
D_post(t) = D_early/persistent(t) + D_late_recurrence(t)
```

Letting the grace period silently convert real clinical demand into zero would
repeat the defect this whole exercise exists to remove, one interval earlier.

## 2. Grace period — an operational choice, declared in advance

| | |
|---|---|
| primary | **180 days** |
| sensitivity | 90 days |
| sensitivity | 365 days |

This is a modelling convention, **not** a literature-derived biological
constant, and it is fixed before any result is seen. Events inside the window
are classified `early_failure_or_persistence` and never enter the late kernel.

## 3. Per-limb contract

### POP

| field | value |
|---|---|
| index_treatment | `vaginal_native_apical`, `sacrocolpopexy`, `other_definitive_pop_surgery` |
| time_origin | date of definitive POP surgery |
| grace_period | 180 d primary; 90 / 365 d sensitivity |
| early_failure_or_persistence | POP-related care within the grace window |
| late_recurrent_care | first new POP-related care episode after the grace window |
| retreatment | any POP-directed treatment after recurrent care (pessary, PT, surgery) |
| reoperation | repeat POP surgical procedure |
| first_vs_repeated | **first** late episode drives `g_k`; repeats are a separate process |
| required_history_years | ≥ kernel horizon; E-CARE observes failure accruing through 7 y, so ≥ 7 |
| evidence endpoint available | composite / anatomic / symptomatic / retreatment — **none is the modelled endpoint** |
| kernel_compatible evidence | **none yet** |

**SUPeR and E-CARE are not interchangeable.** They studied different operations
and defined failure differently, which is precisely why `index_treatment` is a
grouping column rather than a note. Anatomic recurrence, symptomatic recurrence,
composite failure and repeat surgery are retained as **external validation**
endpoints — comparable in cumulative *shape*, never substitutable for `g_k`.

### UI

| field | value |
|---|---|
| index_treatment | `midurethral_sling`; other definitive UI treatments get their OWN value |
| time_origin | date of definitive anti-incontinence procedure |
| grace_period | 180 d primary; 90 / 365 d sensitivity |
| early_failure_or_persistence | SUI care within the grace window (includes never-cured persistence) |
| late_recurrent_care | first new care episode for persistent/recurrent SUI after the window |
| retreatment | any subsequent SUI-directed treatment |
| reoperation | repeat anti-incontinence procedure |
| first_vs_repeated | first late episode |
| required_history_years | ≥ kernel horizon; long-term MUS series run to 8–10 y |
| kernel_compatible evidence | **none yet** |

Symptom recurrence and repeat surgery **diverge substantially** in the published
MUS literature; repeat intervention is not a recurrence measure. The model's
`0.35` UI reoperation share sits downstream, not in `g_k`.

### AI — BLOCKED at a more basic level

| field | value |
|---|---|
| index_treatment | **UNDEFINED** — see below |
| everything downstream | cannot be specified until the index treatment is |

The model's AI `procedure` row is **`ptns`** (percutaneous tibial nerve
stimulation) and the row's own note says:

> *INCOMPLETE. Sacral neuromodulation and sphincteroplasty are the principal AI
> procedures…*

PTNS is a course of office-based sessions, **not a definitive procedure**.
"Recurrence after definitive treatment" is undefined when the modelled treatment
is not definitive. Compounding this:

- AI diagnostics are **urodynamics as an explicit STAND-IN** for anorectal
  manometry and endoanal ultrasound;
- the AI `recurrence` stage has **no procedure row at all**, unlike UI and POP.

**AI must NOT inherit POP/UI semantics.** Long-term sphincteroplasty series show
progressive worsening with heterogeneous outcome definitions, so symptom
deterioration cannot be converted into demand-generating recurrence by analogy.
If AI acquires multiple definitive treatments (sphincteroplasty, sacral
neuromodulation), each gets its own `index_treatment` and its own kernel.

**Prerequisite:** define the AI definitive index treatment(s) and add the
corresponding procedures to `URPS_CPT_BASKET`. Until then the AI recurrence limb
has no contract.

## 4. Compatibility is FALSE by default

`config/recurrence_evidence.csv` is the machine-readable register. `measure_type`
is constrained to:

```
discrete_hazard · cumulative_incidence · first_recurrence_probability_mass
repeat_treatment_rate · unsupported_or_unknown
```

A row is `kernel_compatible = FALSE` unless its endpoint **is** the modelled
late recurrent-care episode and its `measure_type` can be converted to `g_k`
without reinterpretation. Conversion routes:

| measure_type | route to `g_k` |
|---|---|
| `first_recurrence_probability_mass` | direct |
| `discrete_hazard` | `build_recurrence_kernel()` — `g_k = S_k h_k` |
| `cumulative_incidence` | `recurrence_mass_from_cumulative()` — `g_k = F(k) − F(k−1)` |
| `repeat_treatment_rate` | **none.** A retreatment proportion is not recurrence |
| `unsupported_or_unknown` | **none** |

## 5. The current parameters, recorded

```
0.12  current interpretation : annual recurrence hazard
      evidence invoked       : multi-year POP failure/retreatment curves
                               (SUPeR / E-CARE)
      measure_type           : unsupported_or_unknown
      kernel_compatible      : FALSE
      reason                 : a multi-year CUMULATIVE observation is being used
                               to license an ANNUAL rate; implies 29.1%
                               cumulative reoperation by 7 years

0.40  interpretation         : P(reoperation | recurrence)
      source                 : expert judgement
      measure_type           : repeat_treatment_rate
      kernel_compatible      : FALSE
      reason                 : not a recurrence rate at all. It may later belong
                               on P(repeat operation | recurrent-care episode),
                               DOWNSTREAM of g_k, but never inside it
```

Recurrent prolapse and recurrent *surgery* are different quantities — one
vaginal-hysterectomy/USLS cohort reports roughly 20% versus 10% — which is
exactly why a retreatment proportion cannot stand in for recurrence probability
mass.

## 6. Claims estimator output (not yet built)

```
condition · index_treatment · years_since_treatment
original_treated_n · still_observable_n
early_failure_or_persistence_n · first_late_recurrent_care_n
g_k · g_k_lo · g_k_hi · provenance
```

No value may be back-solved from modelled utilization.

## 7. Future validation, without conflating endpoints

Long-term SUPeR follow-up now extends to 10 years with treatment-specific
composite failure trajectories. Once claims-derived **recurrent-care** kernels
exist, their cumulative shape can be compared against those trajectories — as
corroboration of shape, never as a substitution of endpoint.
