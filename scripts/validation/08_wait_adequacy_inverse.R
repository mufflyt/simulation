# scripts/validation/08_wait_adequacy_inverse.R ------------------------------
#
# Audit: what URPS mystery-caller wait times can and cannot say about the
# base-year adequacy anchor. Exercises the full wait-adequacy inverse API and
# makes the identification failure impossible to miss.

base::message(
  "08_wait_adequacy_inverse.R: starting URPS wait calibration audit."
)

target_tbl <- urps_wait_calibration_targets(
  include_preliminary = TRUE
)

base::message(
  "08_wait_adequacy_inverse.R: registered observations = ",
  base::format(nrow(target_tbl), big.mark = ","),
  "."
)

identification <- wait_adequacy_identification_status()

base::message(
  "08_wait_adequacy_inverse.R: current reference adequacy = ",
  format(round(identification$reference_adequacy, 3), nsmall = 3),
  "."
)

base::message(
  "08_wait_adequacy_inverse.R: implied demand/capacity = ",
  format(round(identification$reference_utilization, 3), nsmall = 3),
  "."
)

# The finite-wait inverse: implied adequacy is > 1 by construction, which is the
# whole point -- it cannot reach a shortage anchor below 1.0.
inverse_tbl <- invert_clear_access_wait(
  wait_business_days = target_tbl$wait_business_days,
  wait_scale = 5
)

print(inverse_tbl)

# The conditional fit: hold an assumed adequacy > 1 and read off the wait scale
# it implies. Adequacy and scale are jointly unidentified from one wait, so this
# is deliberately conditional.
fitted_scale <- fit_wait_scale_given_adequacy(
  adequacy = 1.05,
  waits = target_tbl$wait_business_days,
  weights = target_tbl$evidence_weight
)

base::message(
  "08_wait_adequacy_inverse.R: wait scale at adequacy 1.05 = ",
  format(round(fitted_scale, 2), nsmall = 2),
  " business days."
)

inverse_surface <- urps_wait_inverse_surface(
  targets = target_tbl
)

best_fit_tbl <- inverse_surface |>
  dplyr::slice_min(
    order_by = .data$squared_error,
    n = 20,
    with_ties = FALSE
  )

# The evidence ledger and the one-row summary, both filed as non-identifiable.
wait_evidence_tbl <- urps_wait_adequacy_evidence()

fix_summary_tbl <- summarize_wait_adequacy_fix()

print(fix_summary_tbl)

timestamp <- base::format(
  Sys.time(),
  "%Y%m%d_%H%M%S"
)

saved_path <- file.path(
  "artifacts",
  paste0(
    "urps_wait_inverse_surface_",
    timestamp,
    ".csv"
  )
)

readr::write_csv(
  inverse_surface,
  saved_path
)

base::message(
  "08_wait_adequacy_inverse.R: saved inverse surface to ",
  normalizePath(
    saved_path,
    mustWork = FALSE
  )
)

print(best_fit_tbl)

print(wait_evidence_tbl)

base::message(
  paste(
    "08_wait_adequacy_inverse.R: IMPORTANT:",
    "Lizeth and Rabice provide URPS-specific measured access evidence,",
    "but the present clear_access() response function cannot identify",
    "an adequacy below 1.0 from a finite observed wait."
  )
)

base::message(
  "08_wait_adequacy_inverse.R: complete."
)
