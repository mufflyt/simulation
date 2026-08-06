# Access-clearing layer: endogenous accessibility outcomes

**Status:** design spec (no code yet). Phase-1 target for `geography-access_clearing.R`
and `reporting-access_outcomes.R`.

## 1. The gap this closes

The demand engine and the supply engine already exist. What is missing is the
**step that clears one against the other** and reports what a patient actually
experiences.

- **Demand chain (built):** `demand-lifecourse` /`data-urps_population`
  (synthetic population) → `demand-dynamic_multistate`/`-dynamic_open`,
  `demand-obstetric_exposure`, `demand-severity_sandvik` (incidence/severity) →
  `data-meps_care_seeking` + the symptom-severity care-seeking stage (#41) →
  `demand-condition_service_pathway` (referral/pathway) →
  `demand-namcs_visit_equations` / `calculate_visit_based_demand` (visit volume).
- **Supply tiers (built):** `supply_capacity_hierarchy()` — headcount → clinical
  FTE → effective wRVU capacity → **tier-4 accessible capacity**
  (`accessible_fraction × insurance_fraction`).
- **Geography (built):** `compute_e2sfca_access()` / `spatial_access_ratio()`,
  `isochrone_demand_from_tracts()`, `demand_by_travel_band()`.
- **What we do with them today (the limit):** `accessible_need_vs_capacity()`
  divides capacity by need to a single **adequacy ratio**, and headline output is
  *providers per 100 000*. Neither answers the clinically meaningful questions:
  *how long is the wait, what is the chance of getting an appointment, how large
  is a panel, how much demand goes unmet, and where.*

The repository already **names the right observables**: `data-practice_survey`
registers `wait_time` ("the only direct observable of unmet demand, and the
natural external validation target for the whole model") and `panel_size` as
capacity anchors. Nothing yet **produces** them. This layer does.

## 2. Scope

**In scope.** A per–geographic-unit, per-year **clearing step** that consumes (a)
completed-visit demand from the demand chain and (b) tier-4 accessible capacity,
and emits a labeled **access-outcomes** table.

**Out of scope.** Rebuilding the demand chain or supply engine (they exist); a
patient-level discrete-event queue (a later option, not Phase 1); changing the
`D1–D5` demand estimands or the `supply_capacity_hierarchy` tiers. This layer
*joins* them; it does not redefine them.

## 3. Data contract

**Unit of clearing:** an E2SFCA catchment (or tract cluster) × year. Everything
is per catchment; national figures are aggregates.

**Inputs (one row per catchment × year):**
| Field | Source |
|---|---|
| `demand_workload` (annual wRVU-equiv, or visits) | `calculate_visit_based_demand()` / `D`-series, allocated to geography via `isochrone_demand_from_tracts()` |
| `accessible_capacity` (annual wRVU-equiv, or provider-equiv) | `supply_capacity_hierarchy()` tier 4, distributed by `compute_e2sfca_access()` weights |
| `accessible_population`, `accessible_fte` | population within reach; tier-2 FTE within reach |
| `median_travel_time` | `demand_by_travel_band()` / E2SFCA (already available) |

**Output (`access_outcomes`, one row per catchment × year):** `utilization`,
`wait_time`, `p_appointment`, `panel_size`, `unmet_demand`, `median_travel_time`,
`calibration_status`. Aggregated to a national roll-up with the same columns.

## 4. The clearing model (the math, Phase 1)

Per catchment, define **utilization** `ρ = demand_workload / accessible_capacity`.

- **Unmet demand** `= max(0, demand_workload − accessible_capacity)`; served
  `= min(demand, capacity)`.
- **Wait time.** A single, labeled, monotone utilization→wait mapping. Default: a
  steady-state heavy-traffic approximation `wait ∝ ρ / (1 − ρ)` for `ρ < 1`,
  saturating to a flagged ceiling as `ρ → 1` and reported as *censored/unbounded*
  for `ρ ≥ 1` (never `NaN`, never negative). The proportionality constant is one
  calibration knob, fitted to `data-practice_survey` observed waits.
- **P(appointment within window `W`).** From the same queue, `P(wait ≤ W)`;
  folds the tier-4 `insurance_fraction` (a patient who cannot be accepted never
  gets the slot). Bounded `[0, 1]`.
- **Panel size** `= accessible_population / accessible_fte`.
- **Utilization (reported)** `= served / capacity`, capped at 1; the remainder is
  the unmet fraction.

Every output carries `calibration_status` (`assumed_illustrative` until the wait
mapping and panel benchmarks are fit).

## 5. The endogenous interaction ("interact every simulated year")

Phase 1 is a **pure transform** (no hidden state): each simulated year the
microsim updates capacity, the life-course model updates demand, and clearing
recomputes outcomes. Two labeled, opt-in couplings make supply and demand
genuinely interact rather than merely be compared:

- **Spatial overflow (Phase 2).** Unmet demand in a catchment spills to the
  next-nearest catchment with a travel penalty, raising *its* `ρ` and the
  patient's `median_travel_time`; or is censored as lost demand. A scenario
  switch, not a default. **Invariant:** `served + unmet + spilled = demand`.
- **Backlog carry-forward (Phase 3).** Unmet demand adds to next year's queue.
  Off by default (it introduces state); on only as an explicit scenario.

## 6. Estimand framing

The outputs are **access outcomes**, distinct from the `D1–D5` demand estimands
and the four supply tiers. Proposed labels (each with `calibration_status`):

| | outcome |
|---|---|
| `A1` | wait_time |
| `A2` | p_appointment |
| `A3` | panel_size |
| `A4` | utilization |
| `A5` | unmet_demand |

`median_travel_time` is already produced upstream and is carried through, not
recomputed.

## 7. Calibration & validation

- **Anchors that already exist:** `wait_time` and `panel_size` are registered in
  `data-practice_survey` as capacity anchors / validation targets — fit the wait
  mapping's constant and sanity-check panel size against them.
- **External benchmarks (flag as `assumed` until sourced):** specialty panel-size
  and utilization surveys.
- **Harness:** mirror the `validation-*` pattern (e.g. a `validation-access.R`
  back-test of predicted vs observed waits where a series exists), and add an
  `assert_access_outcomes_labeled()` guard so an un-calibrated outcome cannot be
  published silently — the same governance the demand/supply layers already use.

## 8. Module placement (8-family convention)

- `geography-access_clearing.R` — the clearing/queue engine and (Phase 2) spatial
  overflow. Spatial matching ⇒ `geography-`.
- `reporting-access_outcomes.R` — assembles and labels the `A`-series table for
  reporting; holds `assert_access_outcomes_labeled()`.
- `validation-access.R` — the back-test/anchor harness (Phase 1+).

## 9. Assumptions & limits (declare before publication)

1. Steady-state queue approximation, not discrete-event; homogeneous demand
   within a catchment.
2. A single service class in Phase 1; severity stratification (the #41
   care-seeking severity stage is the hook) is Phase 3.
3. The wait mapping's functional form is a labeled assumption; only its constant
   is fit.
4. Acceptance is modeled only through the tier-4 `insurance_fraction`; no
   insurer-network micro-detail.
5. wRVU-equivalent is the clearing currency; a visit-count currency is a
   supported alternative but the two must not be mixed within a run.

## 10. Testing plan

**Semantic invariants:** `ρ = demand/capacity`; `wait` strictly increasing in `ρ`
and → ceiling as `ρ → 1`; `unmet = 0` iff `capacity ≥ demand`;
`panel_size = population / fte`; doubling capacity halves `ρ`; full reach and
`ρ < 1` ⇒ high `p_appointment`; **overflow conserves demand**
(`served + unmet + spilled = demand`).

**Adversarial:** zero capacity ⇒ all demand unmet, wait reported censored (not
`NaN`/negative); `ρ > 1` ⇒ unmet, never a negative wait; negative / non-finite /
non-numeric inputs rejected loudly; empty catchment ⇒ `NA` row, not an error;
`p_appointment` never escapes `[0, 1]`.

## 11. Phased delivery

- **Phase 1** — static per-year clearing → `A1–A5` outcomes + semantic/adversarial
  tests + `data-practice_survey` validation targets. No state.
- **Phase 2** — spatial overflow/reallocation with travel penalty (demand-
  conserving).
- **Phase 3** — backlog carry-forward and severity-stratified clearing (consumes
  the #41 severity stage).

Each phase is one reviewable PR; Phase 1 stands alone and is the minimum that
turns "624 accessible-equivalent providers" into "median wait 34 days, 12% of
demand unmet, P(appointment)=0.83."
