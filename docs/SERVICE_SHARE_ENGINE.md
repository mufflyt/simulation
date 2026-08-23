# Calibrated Urogynecology Service-Share Engine

The calibrated service-share path replaces borrowed provider-routing assumptions
with empirically estimated provider composition while keeping the historical
runner available as an explicit comparator.

## Production contract

Use `service_share_engine = "calibrated"` only with a bundle produced by
`calibrate_service_share_model()`. The bundle must contain every service emitted
by the active condition-to-service pathway. Missing services stop the run; there
is no implicit fallback to `provider_routing_prior()` or
`URPS_DELEGATION_MATRIX`.

The calibrated path applies provider routing once. In particular, the historical
APP patient-capacity multiplier is disabled because APP service allocation is
already represented in the calibrated provider composition.

Work RVUs are calculated service by service. The routing/workload bridge uses the
CMS-calibrated URPS workload basket and explicitly maps sacral neuromodulation to
CMS HCPCS 64581, which is already present in `CMS_WORK_RVU`.

## Evidence scopes

CMS and CHIA are intentionally not pooled.

- CMS provides the frozen 2024 Medicare FFS partial-identification interval for
  `P(URPS | physician-delivered service)`, preserving
  `T = U + O + N + M` and the suppression-robust lower and upper bounds.
- CHIA supplies Massachusetts all-payer hospital provider-composition and
  transport evidence only where classified service events and NPIs are
  available.
- Claim-level composition supplies
  `P(provider_type | service, condition, year)` and must contain at least two
  years per calibrated service so shrinkage can be selected out of sample.

When CHIA and CMS disagree, the transport variance increases. The implementation
does not hide source disagreement by averaging the two source estimates.

## Validation

Portable CI runs R CMD check on repository tests and fixtures. Real-data
validation is a separate manually dispatched job on the `urps-real-data`
self-hosted runner. It requires mounted paths for claims events, the calibrated
bundle, CMS evidence, and CHIA evidence; missing paths are fatal.

The validation runner checks:

- provider-share composition sums;
- routed service-volume conservation;
- service-level and provider-level work-RVU conservation;
- frozen CMS `T/U/O/N/M` accounting;
- claims+CMS, claims+CHIA, and combined-source dropout fits;
- deterministic, evidence-sensitive reproducibility digests; and
- machine-readable evidence and configuration provenance.

Real-data artifacts are written under `artifacts/service_share_validation/` with
timestamps. A validation artifact is not evidence that the source data were
available unless the mounted real-data workflow completed successfully.
