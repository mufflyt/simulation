# Calibrated Urogynecology Service Shares Design

## Goal

Replace production reliance on borrowed provider-delegation assumptions with a
versioned, empirically calibrated service-share engine while preserving the
current matrix as an explicit legacy comparator.

## Estimands

The production engine keeps two estimands distinct:

1. `P(provider_type | service, condition, year)` for provider composition.
2. `P(URPS | physician-delivered service)` for the frozen CMS Medicare FFS
   partial-identification analysis.

The second estimand is evidence about the first. It is not silently relabeled as
an all-payer national share.

## Evidence architecture

### CMS

CMS Physician & Other Practitioners by Provider and Service supplies retained
NPI-HCPCS cells. The Geography file supplies unsuppressed national denominators.
The frozen 2024 linkage roster identifies URPS NPIs. The production builder must
reproduce the prespecified identity

`T = U + O + N + M`

and the bounds

`L = U / (T - N)` and `H = (U + M) / (T - N)`.

No capture-rate rescaling is allowed. Unknown CMS provider classes fail closed.
The Tier A female-specific basket is primary; Tier B sex-neutral codes are
secondary. E/M codes are excluded from the CMS share estimand.

### CHIA

Massachusetts CHIA Case Mix is a separate all-payer hospital evidence source.
It informs setting/payer transport and provider composition only where a
rendering/operating physician can be linked to an NPI and a pelvic-floor service
can be classified. CHIA and CMS are never row-bound into one pseudo-population.

### Claims composition

Claim-level provider composition uses a canonical service registry and provider
taxonomy registry. Production classification must fail on active example rules,
unknown active taxonomies, duplicate code ownership, and missing provenance.
Both NUCC URPS branches (`207VF0040X` and `2088F0040X`) are URPS.

## Calibration

The fixed `prior_strength = 20` remains available only in the historical
estimator. The calibrated path selects service-specific Dirichlet concentration
from held-out predictive performance. Provider shares are drawn jointly so each
service-condition-year-draw is nonnegative and sums to one.

CMS bounds enter as interval evidence on the conditional URPS share among
physicians. CHIA contributes a separate transport likelihood/diagnostic.
Disagreement between sources must increase transport uncertainty rather than be
hidden by simple averaging.

The calibration artifact stores share draws, selected concentrations,
holdout scores, source-fit diagnostics, evidence registry versions, input hashes,
seed, and git SHA.

## Simulation integration

`run_end_to_end_simulation()` gains

`service_share_engine = c("legacy_matrix", "calibrated")`,
`service_share_bundle = NULL`, and `service_share_draw = NULL`.

Legacy mode must be behaviorally identical to the pre-change runner. Calibrated
mode must fail closed when a valid bundle is absent. Calibrated routing replaces
the corresponding service/provider assumption; it is never multiplied by the
legacy share afterward.

The engine routes service volume into URPS, APP, and other-provider components,
converts URPS volume to service-specific work RVUs, and passes that workload into
required-FTE and practice-economics calculations. Inactive providers receive no
allocated URPS workload.

## Required accounting identities

For every modeled year and draw:

- provider shares sum to one within service cells;
- allocated service volume equals source service demand;
- allocated work RVU equals service volume times service work RVU;
- aggregate URPS work RVU equals provider-level allocated URPS work RVU;
- APP delegation is applied exactly once;
- CMS service evidence satisfies `T = U + O + N + M` exactly within tolerance;
- strict mode never silently falls back to an example or legacy value.

## Validation

Tests include taxonomy and registry invariants, CMS frozen-equation fixtures,
CHIA DuckDB fixtures, known-truth calibration recovery, held-out predictive
scoring, compositional conservation, source-dropout diagnostics, deterministic
seeding, legacy behavior preservation, calibrated end-to-end accounting, and
nightly real-data validation when external CMS/CHIA inputs are mounted.

## Delivery

The change is delivered as five stacked PRs:

1. Canonical registry + reusable CMS partial-identification builder.
2. CHIA service-share evidence and transport diagnostics.
3. Held-out calibration + joint uncertainty bundle.
4. Simulation/workload integration with legacy preservation.
5. Nightly validation, provenance manifests, source-dropout and reproducibility
   checks.
