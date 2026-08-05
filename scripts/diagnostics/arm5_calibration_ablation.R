#!/usr/bin/env Rscript
# Arm 5 forensic calibration audit: uncertainty ablation.
#
#   Rscript scripts/diagnostics/arm5_calibration_ablation.R
#
# WHAT THIS IS. Arm 5 (definition-matched, no attrition) is the most accurate
# arm in the back-test (-2.53%) and has the narrowest interval (width 8). This
# script decomposes that width, then adds one uncertainty component at a time.
#
# WHY IT REIMPLEMENTS THE ARM. The definition-matched arm is a pure stock-flow
# identity -- endpoint = n0 + sum(entrants), departures zero by construction --
# so an explicit Monte Carlo over that identity is FAITHFUL and lets each
# component be switched independently. S0 is asserted to reproduce the engine's
# frozen result before anything is added; if it does not, the script stops.
#
# NO TUNING. Every distribution below comes from pre-cutoff data, an external
# source, or a range labelled PRESPECIFIED. The observed 2023 endpoint is used
# ONLY to score, never to choose a value. Note in particular that the single
# pre-cutoff conversion observation is BELOW 1 and therefore moves the point
# estimate AWAY from the observation.

suppressPackageStartupMessages(pkgload::load_all(".", quiet = TRUE))

N_ITER <- 20000L
CUTOFF <- 2020L; TARGET <- 2023L; H <- TARGET - CUTOFF   # 3 transitions
set.seed(20260804L)

N0 <- sum(backtest_cohorts_through(CUTOFF)$n_certified)
OBS <- mufflyaccess::urps_count(TARGET, geography = "national", include_urology = TRUE)

# ---- Pre-cutoff evidence ---------------------------------------------------

# (i) NRMP filled positions. The frozen arm used appointment years 2017-2020.
NRMP_FROZEN <- c(59, 59, 58, 56)                       # 2017-2020
NRMP_EXTENDED <- c(57, 59, 59, 58, 56)                 # + 2015, fetched 2026-08-04

# (ii) Fellowship non-completion. ACGME URPS withdrawal/dismissal counts,
# mufflyt/cliff data/acgme_fellowship_attrition.csv. PRE-CUTOFF academic years
# only: 2018-19 (2 dropouts) and 2019-20 (0 dropouts, 1 transfer). Denominator
# is the matched cohort of ~59. Transfers may complete elsewhere and are not
# counted as losses.
ACGME_DROPOUTS_PRECUTOFF <- c(2, 0)
ACGME_COHORT <- 59

# (iii) Appointment -> certification lag. Fellowship is 3 years; ABOG
# subspecialty certification follows written and oral examinations, typically 1
# to 2 years after graduation. PRESPECIFIED structural range of 4 to 5 years,
# not fitted.
LAG_YEARS <- 4:5

# (iv) Conversion, matched fellows -> board certifications. Exactly ONE clean
# pre-cutoff pair exists: appointment 2015 (57 filled) -> certification 2019
# (48). Ratio 0.842. The 2016 NRMP report is not retrievable and the 2020
# certification year is an examination artifact, so n = 1.
# A ratio can exceed 1 -- NRMP's own footnote says FPMRS "also includes programs
# not accredited by the ACGME", so people certify who never entered the match --
# and fall below 1 through attrition and timing. The PRESPECIFIED spread is
# +/-25% around the single observation, centred on the OBSERVATION and not on 1.
CONVERSION_POINT <- 48 / 57
CONVERSION_SPREAD <- 0.25

# (v) Baseline stock. The contract reports 2020 = 1099 exactly on the same
# ABOG_PLUS_ABU basis as the target, and quantifies no undercount. Geography
# variation is 1 provider (national 1099 vs CONUS 1098). PRESPECIFIED +/-2%
# completeness range, labelled as an assumption because the contract offers no
# sampling distribution for it.
BASELINE_UNCERTAINTY <- 0.02

# ---- Draws -----------------------------------------------------------------

# Component A: process variation. Fractional entrants resolve to an integer by a
# Bernoulli draw each year, exactly as the engine does.
draw_process <- function(rate) sum(floor(rate) + (stats::runif(H) < (rate - floor(rate))))

sim <- function(n, series, use_sampling, use_completion, use_lag,
                use_conversion, use_baseline) {
  vapply(seq_len(n), function(i) {
    rate <- mean(series)
    if (use_sampling) rate <- stats::rnorm(1, mean(series), series_mean_se(series))
    if (use_lag) {
      # Which appointment cohorts feed the window depends on the lag. Draw a lag
      # and take the mean of the cohorts it selects; with a short series this
      # widens the rate rather than shifting it systematically.
      lag <- sample(LAG_YEARS, 1)
      k <- max(1L, length(series) - (lag - min(LAG_YEARS)))
      rate <- mean(series[seq_len(k)])
      if (use_sampling && k > 1) rate <- stats::rnorm(1, rate, series_mean_se(series[seq_len(k)]))
    }
    if (use_completion) {
      # Beta posterior on the dropout rate from the pre-cutoff ACGME counts,
      # Jeffreys prior. Losses reduce the cohort reaching certification.
      d <- stats::rbeta(1, sum(ACGME_DROPOUTS_PRECUTOFF) + 0.5,
                        length(ACGME_DROPOUTS_PRECUTOFF) * ACGME_COHORT -
                          sum(ACGME_DROPOUTS_PRECUTOFF) + 0.5)
      rate <- rate * (1 - d)
    }
    if (use_conversion) {
      rate <- rate * stats::runif(1, CONVERSION_POINT * (1 - CONVERSION_SPREAD),
                                  CONVERSION_POINT * (1 + CONVERSION_SPREAD))
    }
    base <- N0
    if (use_baseline) base <- N0 * stats::runif(1, 1 - BASELINE_UNCERTAINTY,
                                                1 + BASELINE_UNCERTAINTY)
    base + draw_process(max(rate, 0))
  }, numeric(1))
}

score <- function(label, x) {
  q <- stats::quantile(x, c(0.025, 0.975), names = FALSE)
  med <- stats::median(x)
  data.frame(spec = label, median = med, abs_error = med - OBS,
             pct_error = 100 * (med - OBS) / OBS,
             lo95 = q[1], hi95 = q[2], width95 = q[2] - q[1],
             covered = OBS >= q[1] && OBS <= q[2], stringsAsFactors = FALSE)
}

# ---- S0: reproduce the frozen arm ------------------------------------------

s0 <- score("S0 frozen: process + NRMP sampling",
            sim(N_ITER, NRMP_FROZEN, TRUE, FALSE, FALSE, FALSE, FALSE))

frozen <- utils::read.csv("artifacts/frozen_2026-08-04_backtest10/backtest_2020_to_2023_summary.csv",
                          stringsAsFactors = FALSE)
fr <- frozen[grepl("NRMP", frozen$arm) & !frozen$apply_attrition, ]
cat("=== S0 REPRODUCTION CHECK (against the frozen engine result) ===\n")
cat(sprintf("engine  median %7.1f  width %5.1f\n", fr$predicted_median,
            fr$pi95_upper - fr$pi95_lower))
cat(sprintf("script  median %7.1f  width %5.1f\n", s0$median, s0$width95))
if (abs(s0$median - fr$predicted_median) > 2 || abs(s0$width95 - (fr$pi95_upper - fr$pi95_lower)) > 4) {
  stop("S0 does not reproduce the frozen arm; the reimplementation is not faithful.",
       call. = FALSE)
}
cat("REPRODUCED.\n\n")

# ---- Ablation: add one component at a time ---------------------------------

rows <- list(s0)
rows[[2]] <- score("S1 + extend NRMP series to 2015-2020",
                   sim(N_ITER, NRMP_EXTENDED, TRUE, FALSE, FALSE, FALSE, FALSE))
rows[[3]] <- score("S2 + fellowship non-completion (ACGME)",
                   sim(N_ITER, NRMP_EXTENDED, TRUE, TRUE, FALSE, FALSE, FALSE))
rows[[4]] <- score("S3 + appointment->certification lag",
                   sim(N_ITER, NRMP_EXTENDED, TRUE, TRUE, TRUE, FALSE, FALSE))
rows[[5]] <- score("S4 + matched->certified conversion",
                   sim(N_ITER, NRMP_EXTENDED, TRUE, TRUE, TRUE, TRUE, FALSE))
rows[[6]] <- score("S5 + baseline-stock completeness",
                   sim(N_ITER, NRMP_EXTENDED, TRUE, TRUE, TRUE, TRUE, TRUE))

tab <- do.call(rbind, rows)
tab$d_width <- c(NA, round(diff(tab$width95), 1))
tab$d_pct_error <- c(NA, round(diff(tab$pct_error), 2))

cat("=== ABLATION TABLE (Arm 5, definition-matched; observed =", OBS, ") ===\n")
print(data.frame(spec = tab$spec, median = round(tab$median, 1),
                 pct_err = round(tab$pct_error, 2), width = round(tab$width95, 1),
                 covered = tab$covered, d_width = tab$d_width,
                 d_pct = tab$d_pct_error), row.names = FALSE)

cat("\n=== CONVERSION COMPONENT, DIRECTION CHECK ===\n")
cat(sprintf("single pre-cutoff observation: 2015 appointment (57) -> 2019 certification (48) = %.3f\n",
            CONVERSION_POINT))
cat("It is BELOW 1, so S4 moves the point estimate DOWN, away from the observed\n")
cat("endpoint. A tuned conversion factor would have been placed above 1.\n")

utils::write.csv(tab, "artifacts/diagnostics/arm5_ablation_table.csv", row.names = FALSE)
cat("\nWrote artifacts/diagnostics/arm5_ablation_table.csv\n")
