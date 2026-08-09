#!/usr/bin/env Rscript
# Historical back-test: fit through 2020, project 2021-2023, score against the
# observed 2023 count the model never saw.
#
#   Rscript scripts/run_backtest_2020_to_2023.R
#
# Writes:
#   artifacts/backtest_2020_to_2023_summary.csv
#   artifacts/backtest_2020_to_2023_iterations.parquet
#   artifacts/backtest_2020_to_2023_trajectory.csv
#   figures/backtest_2020_to_2023.png
#   docs/BACKTEST_2020_TO_2023.md

suppressPackageStartupMessages({
  library(dplyr); library(tibble); library(tidyr); library(ggplot2)
})
if (!requireNamespace("urpssim", quietly = TRUE)) {
  pkgload::load_all(".", quiet = TRUE)
} else {
  library(urpssim)
}

N_ITER <- as.integer(Sys.getenv("BACKTEST_ITERATIONS", "1000"))
dir.create("artifacts", showWarnings = FALSE)
dir.create("figures", showWarnings = FALSE)
dir.create("docs", showWarnings = FALSE)

cached <- "artifacts/.backtest_raw.rds"
bt <- if (file.exists(cached) && !nzchar(Sys.getenv("BACKTEST_FORCE"))) {
  message("Reusing ", cached, " (set BACKTEST_FORCE=1 to re-run)")
  readRDS(cached)
} else {
  b <- run_backtest(n_iterations = N_ITER)
  saveRDS(b, cached)
  b
}

# ---- Tabular artifacts -----------------------------------------------------

utils::write.csv(bt$summary, "artifacts/backtest_2020_to_2023_summary.csv",
                 row.names = FALSE)

# THE DRIFT GATE, placed here because THIS is the moment drift is created.
# BACKTEST_RECORD_2020_2023 is a transcription of the file just overwritten, and
# every projection carries a status derived from that constant. Re-scoring
# without updating it leaves the package reporting a validation result no
# artifact supports -- which happened once already, when extending the NRMP
# series moved arm 5 from -2.53% to -4.36%.
#
# Loud, and non-fatal: the artifact is legitimately regenerated before the
# constant can be updated from it, so failing here would block the very step
# that fixes it.
cat("\n===== RECORD DRIFT CHECK =====\n")
.drift <- verify_backtest_record("artifacts/backtest_2020_to_2023_summary.csv")
if (isTRUE(.drift$current) && isTRUE(.drift$checksum_matches)) {
  cat("BACKTEST_RECORD_2020_2023 matches this artifact. Nothing to update.\n")
} else {
  cat("!! BACKTEST_RECORD_2020_2023 IS NOW STALE.\n")
  if (isFALSE(.drift$checksum_matches)) {
    cat(sprintf("   artifact sha256 %s, record transcribed from %s\n",
                substr(.drift$observed_sha256, 1, 16),
                substr(.drift$expected_sha256, 1, 16)))
  }
  if (!is.null(.drift$mismatches) && nrow(.drift$mismatches)) {
    cat(sprintf("   %d field(s) differ:\n", nrow(.drift$mismatches)))
    print(as.data.frame(.drift$mismatches), row.names = FALSE)
  }
  cat("\n   TO FIX, in the SAME commit as this artifact:\n")
  cat("     Rscript scripts/diagnostics/emit_backtest_record.R\n")
  cat("     ...paste into R/validation-backtest_status.R, then update\n")
  cat(sprintf("     BACKTEST_RECORD_SHA256 <- \"%s\"\n", .drift$observed_sha256))
}
utils::write.csv(bt$trajectory, "artifacts/backtest_2020_to_2023_trajectory.csv",
                 row.names = FALSE)

# Provenance manifest: which contract artifact these numbers were scored
# against, so a stale artifact is detectable rather than silent.
manifest <- list(
  generated_by = "scripts/run_backtest_2020_to_2023.R",
  cutoff_year = bt$settings$cutoff_year,
  target_year = bt$settings$target_year,
  n_iterations = bt$settings$n_iterations,
  seed = bt$settings$seed,
  target_value = bt$target$value,
  target_rationale = bt$target$rationale,
  retired_values_rejected = bt$target$retired_values_rejected,
  observed_series_applies_attrition = bt$target$observed_series_applies_attrition,
  leakage_audit = bt$leakage_audit,
  urps_artifact = bt$provenance
)
jsonlite::write_json(manifest, "artifacts/backtest_2020_to_2023_manifest.json",
                     auto_unbox = TRUE, pretty = TRUE, null = "null")

if (requireNamespace("arrow", quietly = TRUE)) {
  arrow::write_parquet(bt$iterations,
                       "artifacts/backtest_2020_to_2023_iterations.parquet")
} else {
  warning("arrow not installed; iterations written as CSV instead")
  utils::write.csv(bt$iterations,
                   "artifacts/backtest_2020_to_2023_iterations.csv",
                   row.names = FALSE)
}

# ---- Figure ----------------------------------------------------------------

# Short labels built from the arm number, not by regex-mangling the full label
# (which turned "rnorm(52, 9)" into ", 9)").
arm_labels <- c(
  "1" = "1. Derived cohort\nentrants = 55",
  "2" = "2. Derived cohort\nentrants from pre-2021",
  "3" = "3. Synthetic rnorm(52, 9)\nentrants = 55",
  "4" = "4. Synthetic rnorm(52, 9)\nentrants from pre-2021"
)
traj <- bt$trajectory %>%
  mutate(panel = factor(ifelse(apply_attrition,
                        "Model as specified (applies attrition)",
                        "Definition-matched (no attrition, both sides)"),
                        levels = c("Model as specified (applies attrition)",
                                   "Definition-matched (no attrition, both sides)")),
         arm_short = unname(arm_labels[substr(arm, 1, 1)]))

p <- ggplot(traj, aes(x = year)) +
  geom_ribbon(aes(ymin = pi95_lower, ymax = pi95_upper, fill = arm_short), alpha = 0.12) +
  geom_ribbon(aes(ymin = pi80_lower, ymax = pi80_upper, fill = arm_short), alpha = 0.20) +
  geom_line(aes(y = predicted_median, colour = arm_short), linewidth = 0.7) +
  geom_line(aes(y = observed), colour = "grey15", linewidth = 1.1) +
  geom_point(aes(y = observed), colour = "grey15", size = 2) +
  facet_grid(arm_short ~ panel, switch = "y") +
  scale_y_continuous(limits = c(1090, 1320)) +
  scale_x_continuous(breaks = 2020:2023,
                     guide = guide_axis(check.overlap = TRUE)) +
  labs(
    title = "URPS workforce back-test: fit through 2020, project 2021-2023",
    subtitle = paste0("Black line is observed (national, ABOG+ABU, board-certified active, contract v3.0.0).\n",
                      "Bands are 80% and 95% prediction intervals over ",
                      format(bt$settings$n_iterations, big.mark = ","),
                      " iterations."),
    x = NULL, y = "Providers (headcount)",
    caption = "No 2021-2023 information was used to estimate any model parameter."
  ) +
  theme_minimal(base_size = 11) +
  theme(legend.position = "none",
        plot.title = element_text(size = rel(1.1)),
        plot.title.position = "plot",
        strip.text.y.left = element_text(angle = 0, hjust = 0, size = rel(0.85)),
        strip.text.x = element_text(face = "bold"),
        panel.spacing = unit(1, "lines"))

ggsave("figures/backtest_2020_to_2023.png", p, width = 10, height = 8.5,
       dpi = 150, bg = "white")

message("Artifacts written.")
print(as.data.frame(bt$summary[, c("arm", "observed", "predicted_median",
                                   "absolute_error", "percent_error",
                                   "within_80", "within_95")]))
