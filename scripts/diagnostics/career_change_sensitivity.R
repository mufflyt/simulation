#!/usr/bin/env Rscript
# Sensitivity: what the POST-CUTOFF career-change hazard would have done -
#
#   Rscript scripts/diagnostics/career_change_sensitivity.R
#
# WHY THIS IS A SENSITIVITY ANALYSIS AND NOT THE PRIMARY RESULT. The primary
# 2020->2023 back-test omits the permanent under-50 career-change process
# because its only estimate, 1.42%/yr, comes from Zarek et al, Phys Ther
# 2025;105:pzaf014 and did not exist at the 2020 forecast origin. Omission is
# not a claim that the hazard is zero: HWSM represented under-50 exit as
# temporary labour-force participation with re-entry, a different process this
# model does not implement, so there was no 2020-vintage value to substitute.
#
# Leakage is a property of the PRIMARY analysis. Demonstrating that the effect
# is modest does not make a post-cutoff parameter admissible in a forecast that
# claims a 2020 origin, so this run is reported alongside the primary result
# rather than in place of it.
#
# Writes artifacts/diagnostics/career_change_sensitivity.csv.

suppressPackageStartupMessages({
  if (!requireNamespace("urpssim", quietly = TRUE)) {
    pkgload::load_all(".", quiet = TRUE)
  } else {
    library(urpssim)
  }
})

N_ITER <- as.integer(Sys.getenv("BACKTEST_ITERATIONS", "1000"))
OBS <- 1306

primary_path <- "artifacts/backtest_2020_to_2023_summary.csv"
if (!file.exists(primary_path))
  stop("Run scripts/run_backtest_2020_to_2023.R first.", call. = FALSE)
primary <- utils::read.csv(primary_path, stringsAsFactors = FALSE)

cached <- "artifacts/.backtest_career_change_sensitivity.rds"
sens <- if (file.exists(cached) && !nzchar(Sys.getenv("BACKTEST_FORCE"))) {
  message("Reusing ", cached, " (set BACKTEST_FORCE=1 to re-run)")
  readRDS(cached)
} else {
  b <- run_backtest(n_iterations = N_ITER,
                    career_change_hazard = CAREER_CHANGE_HAZARD_UNDER_50)
  saveRDS(b, cached)
  b
}

j <- merge(primary[, c("arm", "predicted_median", "percent_error",
                       "pi95_lower", "pi95_upper", "within_95")],
           sens$summary[, c("arm", "predicted_median", "percent_error",
                            "pi95_lower", "pi95_upper", "within_95")],
           by = "arm", suffixes = c("_primary", "_sensitivity"))

out <- data.frame(
  arm = j$arm,
  applies_attrition = !grepl("no-attrition", j$arm),
  median_primary = j$predicted_median_primary,
  median_sensitivity = j$predicted_median_sensitivity,
  median_difference = j$predicted_median_sensitivity - j$predicted_median_primary,
  pct_diff_primary = round(j$percent_error_primary, 2),
  pct_diff_sensitivity = round(j$percent_error_sensitivity, 2),
  width_primary = round(j$pi95_upper_primary - j$pi95_lower_primary, 1),
  width_sensitivity = round(j$pi95_upper_sensitivity - j$pi95_lower_sensitivity, 1),
  contained_primary = j$within_95_primary,
  contained_sensitivity = j$within_95_sensitivity,
  stringsAsFactors = FALSE
)
out <- out[order(out$arm), ]

# Decomposition under both specifications. The definitional component is the
# only one that moves: the no-attrition arms are identical by construction, so
# the entrant-regime residual is fixed at OBS - (no-attrition median).
dec <- function(s) {
  on <- s$predicted_median[s$arm == "1. Derived cohort, entrants = 55 (shipped assumption)"]
  off <- s$predicted_median[grepl("^1\\..*no-attrition", s$arm)]
  c(attr_on = on, attr_off = off, total = OBS - on,
    definitional = off - on, regime = OBS - off,
    pct_definitional = 100 * (off - on) / (OBS - on),
    pct_regime = 100 * (OBS - off) / (OBS - on))
}
dp <- dec(primary); ds <- dec(sens$summary)

cat("\n== Career-change sensitivity: primary (omitted) vs 1.42%/yr (Zarek 2025) ==\n\n")
print(out, row.names = FALSE)
cat("\n-- Discrepancy decomposition, arm 1 --\n")
print(round(rbind(primary = dp, sensitivity = ds), 2))
cat("\nContainment: primary", sum(out$contained_primary), "/", nrow(out),
    " sensitivity", sum(out$contained_sensitivity), "/", nrow(out), "\n")

stopifnot(all(out$median_difference[!out$applies_attrition] == 0))
cat("CHECK PASSED: every no-attrition arm is identical under both",
    "specifications, so the sensitivity isolates the career-change process.\n")

dir.create("artifacts/diagnostics", recursive = TRUE, showWarnings = FALSE)
utils::write.csv(out, "artifacts/diagnostics/career_change_sensitivity.csv",
                 row.names = FALSE)
cat("\nWrote artifacts/diagnostics/career_change_sensitivity.csv\n")
