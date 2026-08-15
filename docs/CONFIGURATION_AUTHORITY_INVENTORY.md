# Configuration authority inventory

Which declarative artifacts are **executable contracts**, which are **partial
contracts**, and which are merely **documentation**.

Established by execution proof, not grep. A file being read is not proof its
values survive to a model output — they can be overwritten downstream, or the
file can be read only by its own test. Where a status is LIVE below, a value was
mutated and a downstream quantity was shown to move.

Verified on `feat/chia-inpatient-surgical-layer`, R 4.4.2, 2026-08-15.

---

## Summary

| Artifact | Loader | Execution consumer | Status | Duplicate hard-coded value? | Mutation test? |
|---|---|---|---|---|---|
| `inst/extdata/pathway/condition_service_pathway.csv` | `condition_service_pathway()` | `pathway_service_volumes()` via `simulate_lifecourse_demand(use_condition_pathway = TRUE)` — the DEFAULT | **LIVE** | yes — `config/pop_cascade_transitions.yml` restates 0.35/0.55/0.12 | **no** |
| `demand_transition_registry()` | in-code tribbles | `lifecourse_risk_params()`, pathway params | **LIVE (params) / GATE-ONLY (refusal)** | no | no |
| `config/paths.yml` | `R/core-paths.R` | path resolution throughout | **LIVE** | no | no |
| `config/canonical_sources.yml` | canonical-data machinery | `R/calibration-sources.R`, `R/demand-urps.R` | **LIVE** | no | no |
| `config/chia_urps_inpatient_codes.yml` | `yaml::read_yaml` | `clinical_review_status()`, CHIA ETL | **LIVE** | no | yes (review gate) |
| `config/calibration_targets.yml` | `yaml::read_yaml` | `calibration_state()`, `assert_anchor_reviewed()`, `verify_calibration_anchors()` | **PARTIAL** | no | yes (integrity + gate) |
| `config/office_visit_validation_anchors.yml` | `yaml::read_yaml` | scripts + tests only; **no `R/` consumer** | **DOCUMENTATION (by design)** | no | yes (records-only) |
| `config/pop_cascade_transitions.yml` | `yaml::read_yaml` | **its own test only** | **INERT** | yes — duplicates the live CSV | n/a |
| `config/service_workload.yml` | **none** | **none — never read** | **DOCUMENTATION** | yes — `0.271`, `0.434` | **no** |
| `lifecourse_service_map()` (`per_treated`) | in-code tribble | only when `use_condition_pathway = FALSE` | **LEGACY / NON-DEFAULT** | parallel parameterisation of the same quantity | no |

---

## The POP correction

An earlier reading of this repo concluded the prolapse cascade was inert and that
the live model used a single terminal rate, `lifecourse_service_map()`
`per_treated = 0.25`, implying a 6.08x overstatement. **That was wrong.**

`use_condition_pathway` defaults to `TRUE` (`R/demand-lifecourse.R:253`), so the
staged pathway CSV is the live path and `per_treated = 0.25` is the legacy
branch. The cascade exists and executes:

```
treated_pop 3,264,807
  conservative   p_advance 0.35
  testing        p_advance 0.55
  procedure      per_entering 1.00
  followup       p_advance 0.12   ->  recurrence  prolapse_procedure 0.40
```

**Execution proof.** Halving POP conservative `p_advance` (0.35 -> 0.175) halves
prolapse volume exactly:

| `p_advance` | prolapse_procedure |
|---|---|
| 0.35 | 658,642 |
| 0.175 | 329,321 |
| ratio | **0.500** |

Stage split at the shipped values: procedure 628,475 + recurrence 30,167 =
658,642 against an anchor of 140,762 — **4.68x**, reproducing
`0.35 x 0.55 x (1 + 0.12 x 0.40)` exactly. The figure recorded in
`pop_cascade_transitions.yml` was correct; the earlier 6.08x was not.

**Consequence for sequencing:** the two-stage POP structure already exists.
Building another would duplicate live machinery. What POP needs is provenance for
the two `p_advance` values, not new structure.

---

## Findings, in the order worth fixing

### 1. `config/pop_cascade_transitions.yml` is inert and duplicates live values

Read by nothing but `tests/testthat/test-pop-cascade-gate.R`. It restates 0.35,
0.55, 0.12 and 0.40, which actually live in the pathway CSV. It reads as live
calibration and is not. Two failure modes: editing it changes nothing, and it can
drift silently from the CSV that does execute.

Fix: either delete it, or reduce it to an anchor-constraint record that
*derives* its numbers from `condition_service_pathway()` and asserts equality.

### 2. `config/service_workload.yml` is never read, and duplicates executable constants

Zero loader hits repo-wide. `urps_service_workload()` builds from CMS data in
code. Meanwhile the YAML carries `indirect_time_share: 0.271` and
`level_correction: 0.434`, which are independently hard-coded as
`INDIRECT_TIME_SHARE <- 0.271` (`R/supply-workload_to_fte.R:69`) and `0.434`
(`R/supply-delegation_evidence.R`). **No test reconciles the two.** A reviewer
correcting the YAML would change nothing and believe otherwise.

Fix: mark it provenance-only *and* add an equality gate, or make it authoritative.
Leaving duplicated executable constants ungated is the actual defect.

### 3. The demand-coefficient refusal gate is not on the execution path

`assert_publishable_demand_coefficients()` is reachable from
`R/calibration-validation.R:458` (a report), but **not** from
`core-run_workforce_microsimulation.R` or `demand-lifecourse.R`. Every POP
pathway parameter is `calibration_tier: uncalibrated_illustrative`, source
`placeholder (expert judgement; not evidence-anchored)` — so a simulation can run
and publish on placeholder coefficients without the gate ever firing.

| POP registry param | value | tier |
|---|---|---|
| `recognition` | 0.60 | uncalibrated_illustrative |
| `p_seek` | 0.50 | uncalibrated_illustrative |
| `p_referral` | 0.55 | uncalibrated_illustrative |
| `p_treated` | 0.65 | uncalibrated_illustrative |

### 4. `config/calibration_targets.yml` is a partial contract

`calibration_state()` and `assert_anchor_reviewed()` read it, and
`verify_calibration_anchors()` uses the declared `path` and `sha256`. Other
declared fields do not drive loading. The file mixes fields that are enforced
with fields that are descriptive, with nothing marking which is which.

### 5. Two parallel parameterisations of the same quantity

`lifecourse_service_map()` `per_treated` and the pathway CSV both answer "services
per patient", selected by `use_condition_pathway`. The legacy branch has no
provenance columns at all. Anything comparing model output across that flag is
comparing two different parameter sets.

---

## Method note

Grep alone produced a wrong answer here: `pop_cascade_transitions.yml` looks
inert *and is*, but the mechanism it describes is live under different names in a
different file — so "the config is inert" was true while "the cascade is inert"
was false. Only loading the pathway and mutating a value distinguished the two.

Every artifact marked LIVE above should acquire a mutation test. Three currently
have one; the pathway CSV — the single most consequential artifact in the table —
does not.
