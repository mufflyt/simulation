#!/usr/bin/env Rscript
# Preregister the in-training pipeline forecast for certification years 2024-2026
#
#   Rscript scripts/preregister_pipeline_forecast.R
#
# WHY A PREREGISTRATION AND NOT A SCORE. docs/ENTRANT_REGIME_MODEL.md sec 6 says
# the missing piece is "a cutoff whose validation window nobody has looked at".
# That window still does not exist in the contract. `board_certified_active` runs
# to 2023 and no further; the only later row is a 2025 `roster_snapshot`, which
# backtest_target_candidates() already lists as a REJECTED target because it is a
# different measure -- a roster headcount, not a cumulative certification count.
# Scoring against it would be the exact substitution validate_backtest_target()
# fails closed on, and the gap makes the point: 1,339 roster against a predicted
# 1,424 cumulative certifications is ~85 people, which is roughly what a roster
# net of departures and non-practising diplomates would drop.
#
# So this freezes the prediction instead. The forecast below is fixed and hashed
# now, before the 2024-2026 counts are published, so that when they arrive the
# comparison is genuinely out-of-sample rather than a story told afterwards.
#
# WHAT MAKES THIS FORECAST WORTH FREEZING. It does not use the entrant model at
# all. Every fellow who will certify in 2024-2026 is ALREADY IN TRAINING and
# counted by ACGME, so the forecast needs no view on entrant growth, no
# regime-break term, and no position on whether NRMP or ACGME counts entry
# correctly. Its only free parameters are the two pathway conversions, each
# estimated on data ending in 2023.

suppressPackageStartupMessages(library(urpssim))

PATH <- "inst/extdata/preregistration/urps_pipeline_forecast_2024_2026.txt"
FROZEN_AT <- Sys.getenv("PREREG_FROZEN_AT", unset = format(Sys.Date()))
BASE_YEAR <- 2023L
BASE_CUMULATIVE <- 1306L   # board_certified_active, national, ABOG_PLUS_ABU, v3.0.0

fellows <- acgme_urps_fellows()
rates <- entrant_to_cert_ratio_by_pathway()
rate <- stats::setNames(rates$ratio, rates$parent)

entering <- function(parent, year) {
  v <- fellows$year_1[fellows$parent == parent & fellows$entry_year == year]
  if (length(v)) as.numeric(v) else NA_real_
}

years <- 2024:2026
pred <- data.frame(cert_year = years)
for (p in names(URPS_FELLOWSHIP_YEARS_BY_PATHWAY)) {
  lag <- URPS_FELLOWSHIP_YEARS_BY_PATHWAY[[p]]
  pred[[p]] <- vapply(years, function(y) entering(p, y - lag) * rate[[p]], numeric(1))
}
pred$annual <- rowSums(pred[, names(URPS_FELLOWSHIP_YEARS_BY_PATHWAY)])
pred$cumulative <- BASE_CUMULATIVE + cumsum(pred$annual)

spec <- list(
  protocol_version = "1",
  model = "in-training pipeline: ACGME year-1 cohort x pathway conversion",
  estimand = "board_certified_active, national, ABOG_PLUS_ABU, contract v3.0.0",
  # Naming the measure inside the hash is the point: it makes substituting the
  # 2025 roster_snapshot a spec mismatch rather than a judgement call.
  target_measure = "board_certified_active",
  target_geography = "national",
  target_pathway = "ABOG_PLUS_ABU",
  forbidden_targets = "roster_snapshot (different measure); contract v2.1.0 values",
  base_year = BASE_YEAR,
  base_cumulative = BASE_CUMULATIVE,
  cert_lag_obgyn = URPS_FELLOWSHIP_YEARS_BY_PATHWAY[["obgyn"]],
  cert_lag_urology = URPS_FELLOWSHIP_YEARS_BY_PATHWAY[["urology"]],
  conversion_obgyn = round(rate[["obgyn"]], 6),
  conversion_urology = round(rate[["urology"]], 6),
  conversion_fitted_through = 2023L,
  entrant_source = "ACGME Data Resource Book, year-1 fellows, both parent pathways",
  predicted_annual_2024 = round(pred$annual[pred$cert_year == 2024], 3),
  predicted_annual_2025 = round(pred$annual[pred$cert_year == 2025], 3),
  predicted_annual_2026 = round(pred$annual[pred$cert_year == 2026], 3),
  predicted_cumulative_2024 = round(pred$cumulative[pred$cert_year == 2024], 3),
  predicted_cumulative_2025 = round(pred$cumulative[pred$cert_year == 2025], 3),
  predicted_cumulative_2026 = round(pred$cumulative[pred$cert_year == 2026], 3),
  metric = "absolute and percent error on cumulative count at each cert_year",
  scoring_rule = paste(
    "score only when board_certified_active is published for the year;",
    "no substitution of roster_snapshot or any other measure;",
    "no refit of the conversions after any target year is observed")
)

rec <- preregister_spec(
  spec, PATH, frozen_at = FROZEN_AT,
  notes = paste(
    "Frozen while board_certified_active ends at 2023, so 2024-2026 are unseen.",
    "The forecast uses fellows already in training and does not invoke the",
    "entrant regime model."))

cat("Preregistered:", PATH, "\n")
cat("spec_hash:", rec$spec_hash, "\n")
cat("frozen_at:", rec$frozen_at, "\n\n")
print(round(pred, 2), row.names = FALSE)
