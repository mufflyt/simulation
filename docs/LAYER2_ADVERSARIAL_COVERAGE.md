# Layer 2 adversarial specification — coverage

Every section of the Layer-2 specification, mapped to **IMPLEMENTED**,
**NOT APPLICABLE** (with the reason), or **DEFERRED** (with what it needs
first). Nothing is silently dropped.

Status as of 2026-08-15 on `feat/chia-inpatient-surgical-layer`.

**The single most important entry in this table:** roughly a third of the
specification (H–N, AA–AE, AR–AW) describes a **record-linkage / evidence-
synthesis system that does not exist in this repository**. Provider identity
resolution lives in `cliff` and `twostep`. Building collision corpora, evidence
monotonicity and fail-closed linkage tests here would produce elaborate
scaffolding around a subsystem that is not present — the appearance of rigour
without the substance. Those sections are marked NOT APPLICABLE **HERE** and
should be implemented in the repository that owns linkage.

---

## Implemented

| § | Gate | Where | Notes |
|---|---|---|---|
| A1–A3, AZ | Scientific canary mutations | `.github/scripts/adversarial/canaries.R` | 8 canaries, each killed by a **named** detector. 9 detector families. |
| A2 | Mutation classes | same | TRANSITION, CALIBRATION, DEMAND, WEIGHTING, AGGREGATION recorded per canary in `artifacts/adversarial/canaries.csv`. |
| A3 | Detector independence | same | A canary counts as killed only if its **expected** detector fires; a generic crash does not count. Baseline check ensures no detector is always-on. |
| B1–B4 | Property-based worlds | `.github/scripts/adversarial/metamorphic.R` | 300 randomized legal worlds nightly, 5,000 weekly. Asserts finiteness, non-negativity, cascade monotonicity, mass conservation. |
| B5 | Shrinking | same | A failing world is reduced to the POP-only pathway and written to `artifacts/adversarial/minimal_failing_pathway.csv`. |
| C1 | Row-order invariance | same | 25 random permutations + treated-vector reordering. |
| C2, C6 | Identifier / label renaming | same | Renaming a condition must not change results. |
| C5, V, W | Scale, weighting, duplicate metamorphism | same | Counts scale exactly; rates are scale invariant; weight 10 == two records of weight 5. |
| D | Chunk-size invariance | same | Chunk sizes 1 / 7 / 100 / 1000 recombine to the whole-cohort answer. |
| E, E1 | Execution-geometry invariance | same | 8 shards recombined in 10 random orders. See note below on parallelism. |
| F1–F4 | Independent reference implementation | `.github/scripts/assert-scientific-invariants.R` | Naive row-by-row recomputation sharing no code path with the engine. Currently agrees to 0.0 relative difference. |
| P | Negative controls | metamorphic.R | Irrelevant column, row names. |
| Q | Positive controls | metamorphic.R | Doubling an advance probability must roughly double procedures. |
| T | Boundary collisions | metamorphic.R | p = 0 and p = 1 exactly. |
| U | Numerical perturbation | metamorphic.R | 1e-12 input change moves output 2.9e-12; no discontinuity. |
| AF | Rare-event stress | metamorphic.R | Single-patient cohort. |
| AG | Extreme concentration | metamorphic.R | 100% of the cohort in one condition. |
| AH | Simpson's paradox | metamorphic.R | Aggregate and subgroup directions tracked separately. |
| AJ | Rare-category preservation | metamorphic.R | A 1-person stratum must not vanish. |
| AK | Unknown category | metamorphic.R | An unknown condition must be rejected or be an explicit no-op. |
| AL | Threshold fragility | metamorphic.R | Sweep 0.30–0.40; max step 3.3%, no cliff. |
| S, gate 14 | Temporal traps / future-data leakage | `.github/scripts/assert-temporal-integrity.R` | Enforces input censoring at the cutoff, four self-test traps proving the checker fires, and an estimand-match audit. **Leakage was investigated and EXCLUDED** — see below. |
| BC | Failure taxonomy | metamorphic.R | Failures are classified (NONDETERMINISM, CHUNK DEPENDENCE, INVARIANT VIOLATION, …), not "test failed". |
| BD | Adversarial manifest | `artifacts/adversarial/*.csv` | Canary table and failure table uploaded every run. |
| BE | Blocking policy | `scientific-adversarial.yaml` | All three gates blocking; scorecard fails if any did not actually run. |
| BF | Nightly vs weekly | same | Nightly 03:47 MST; deep run Sundays with 5,000 worlds. |
| BG | Robustness scorecard | same | Step summary + tracking issue, one issue updated by comment. |

## Not applicable to this repository

| § | Gate | Why not here |
|---|---|---|
| F5, H, H1, I, J, K, K1, L, M | Linkage: reference scorer, collision corpus, fail-closed, evidence monotonicity, tie-breaking, evidence-arm ablation, leave-one-arm-out | **There is no record-linkage system in `/simulation`.** Provider identity resolution lives in `cliff`/`twostep`. Implement there. |
| AA, AB, AC, AD, AE | Candidate-set expansion/contraction, evidence duplication, correlated evidence, false certainty | Same — all presuppose a candidate/evidence scorer. |
| AR, AS, AT, AU, AV, AW | High-specificity subset validation, false-match injection, true-match degradation, monotonicity report, ambiguity preservation, downstream linkage sensitivity | Same. |
| N, N1, AY | Source-dropout and source-dominance | Partially applicable but the demand pathway currently has **one** parameter source (expert judgement), so dropout has no arms to remove. Becomes meaningful once parameters are sourced. |
| E2 | Worker-failure reconstruction | The pathway engine is serial and has no shard/restart mechanism. Nothing to interrupt. |
| C3 | State-label permutation | States are character stage names consumed by name, never by numeric code, so there is no numeric-state assumption to break. |
| C4 | Unit metamorphism | Applicable in principle to the FTE/wRVU conversion; see DEFERRED below. |

## Deferred, with the prerequisite named

| § | Gate | Needs first |
|---|---|---|
| G, G1, G2 | Study fixtures, known-truth recovery, deliberate misspecification | A synthetic data-generating process for the **supply** side. High value — this is what distinguishes "the software reproduces itself" from "the methodology recovers a known truth". |
| R | Synthetic truth recovery | Same DGP. |
| AM, AN, AO | Leave-one-year / cohort / state out | Requires the estimation pipeline to be callable on a subset. |
| AP | Bootstrap structural stability | Requires the same. |
| AQ | Alternate-model cross-checks | Requires a second estimator for exits/entrants. |
| AI | Ecological-to-individual consistency | Requires individual-level microsimulation records; the demand path is currently aggregate. |
| AX | Uncertainty propagation | The supply engine already fails its interval standard (coverage 0.20 vs 0.80 required); propagation testing should follow the fix, not precede it. |
| BA, BB | Detector-coverage matrix and minimum multiplicity | Partially present — see below. Formalise once the deferred detectors exist. |

## Detector coverage matrix (BA/BB), current state

Multiplicity achieved today for defects the engine can express:

| Scientific defect | Property | Metamorphic | Reference | Canary | Invariant | Multiplicity |
|---|---|---|---|---|---|---|
| Cascade gains people | ✓ | | ✓ | ✓ | ✓ | **4** |
| Terminal scalar smuggled in | | | | ✓ | ✓ | 2 |
| Double counting (duplicate rows) | | ✓ | | ✓ | | 2 |
| Illegal probability | ✓ | | | ✓ | ✓ | 3 |
| Row-order dependence | ✓ | ✓ | | | | 2 |
| Negative / non-finite volume | ✓ | | | ✓ | ✓ | 3 |
| Care-seeking bypassed | | | | ✓ | ✓ | 2 |

Defects at multiplicity 2 are the fragile ones. The spec asks for **3
independent families** for catastrophic defects; the terminal-scalar and
double-counting rows do not yet meet that and are the next thing to strengthen.

## Honest limitations

- **"Parallelism invariance" is currently order-of-combination invariance.**
  The demand engine is serial and deterministic, so there are no workers to
  vary. The check is real but weaker than the spec intends, and will need
  redoing if the engine ever parallelises.
- **The canaries mutate the parameter surface, not compiled source.** Patching
  installed package code would test R's loader more than the model. This is the
  right trade here because this repository's science lives in its parameters,
  but it does mean a defect introduced in engine *code* rather than in
  parameters is caught only by the reference implementation.
- **Back-test coverage is 0.20 against a required 0.80**, with all 10 arms
  under-predicting. That is ratcheted, not fixed, and no adversarial gate
  changes it. It remains the largest known scientific defect.

## Leakage investigated and excluded — the driver is an estimand mismatch

Data leakage was the leading hypothesis for the back-test under-predicting every
historical arm. It is **not** the explanation:

- every arm declares `cohorts: <= 2020` in the manifest `leakage_audit`, and the
  entrant arms are drawn pre-cutoff by construction ("pre-2021 data",
  "pre-cutoff NRMP match");
- **target leakage inflates apparent accuracy** — predictions hug the
  observation — whereas these predictions are systematically *low*. The
  direction of the error is evidence against contamination.

The measured driver is a definitional asymmetry:

| `apply_attrition` | n | mean error | 95% coverage |
|---|---|---|---|
| FALSE (definition-matched) | 5 | −5.44% | 0.40 |
| TRUE | 5 | −11.29% | **0.00** |

`observed_series_applies_attrition = FALSE`, so the attrited arms compare an
**attrited prediction against a non-attrited observation**. That accounts for
**5.84 percentage points** of bias and *all* of the coverage loss in those arms.

This is a definitional defect, not a leakage defect and not a parameter-tuning
problem. Tests pin the diagnosis: if definition-matched arms ever stop being
less biased than mismatched ones, the investigation re-opens.

Note the residual: even the matched arms average −5.44% and cover only 0.40.
Fixing the mismatch is necessary but **not sufficient** — observed annual change
was 69/yr against 36/yr predicted under the shipped entrant assumption of 55,
so the entrant rate is the next thing to examine.
