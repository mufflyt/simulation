#!/usr/bin/env Rscript
# Back-test diagnostics: full arm table + uncertainty decomposition.
#
# DIAGNOSTIC ONLY. Nothing here is wired into the shipped model. The
# decomposition deliberately runs configurations the package refuses to ship
# (an invented retirement-hazard CV) in order to measure how much each
# parameter family could contribute; measuring a contribution is not a licence
# to adopt the spread. See scratchpad/backtest_debug_log.md.
#
#   Rscript scratchpad/backtest_diagnostics.R

suppressPackageStartupMessages({ library(dplyr) })
pkgload::load_all(".", quiet = TRUE)

CUT <- 2020L; TGT <- 2023L; N <- 1000L; SEED <- 20260802L
obs <- vapply(CUT:TGT, function(y)
  mufflyaccess::urps_count(y, geography = "national", include_urology = TRUE),
  numeric(1))
names(obs) <- as.character(CUT:TGT)
cat("Observed stock:", paste(names(obs), obs, sep = "=", collapse = "  "), "\n")

cohort0 <- backtest_cohort_at(CUT)
est <- backtest_entrant_estimate(CUT, agents = cohort0)

# ---- 1. Full arm table (queue item F) --------------------------------------

arm_row <- function(cohort, entrants, attrition, spec, label, seed) {
  a <- run_backtest_arm(cohort, entrants_per_year = entrants, cutoff_year = CUT,
                        target_year = TGT, n_iterations = N,
                        apply_attrition = attrition, param_spec = spec, seed = seed)
  p <- a$iterations$headcount[a$iterations$year == TGT]
  q <- stats::quantile(p, c(0.025, 0.25, 0.75, 0.975), names = FALSE)
  tibble::tibble(
    arm = label, cohort = cohort, entrants = round(entrants, 2),
    attrition = attrition,
    observed = unname(obs[as.character(TGT)]),
    pred_mean = mean(p), pred_sd = stats::sd(p), pred_median = stats::median(p),
    p2.5 = q[1], p25 = q[2], p75 = q[3], p97.5 = q[4],
    abs_error = stats::median(p) - unname(obs[as.character(TGT)]),
    pct_error = 100 * (stats::median(p) - unname(obs[as.character(TGT)])) /
      unname(obs[as.character(TGT)]),
    covered_95 = unname(obs[as.character(TGT)]) >= q[1] &&
      unname(obs[as.character(TGT)]) <= q[4],
    width_95 = q[4] - q[1],
    seed = seed, cutoff_year = CUT,
    source_vintage = "urps contract v3.0.0; certifications through 2020 only"
  )
}

spec_of <- function(mean_e) supply_parameter_spec(
  entrant_series = unname(est$yearly), entrant_mean = mean_e,
  departures = est$departures)

tbl <- dplyr::bind_rows(
  arm_row("derived",   55,                  TRUE,  spec_of(55),  "1 derived/assumed/attr",   SEED + 1),
  arm_row("derived",   55,                  FALSE, spec_of(55),  "1 derived/assumed/noattr", SEED + 1),
  arm_row("derived",   est$gross_entrants,  TRUE,  spec_of(est$gross_entrants), "2 derived/est/attr",   SEED + 2),
  arm_row("derived",   est$gross_entrants,  FALSE, spec_of(est$gross_entrants), "2 derived/est/noattr", SEED + 2),
  arm_row("synthetic", 55,                  TRUE,  spec_of(55),  "3 synth/assumed/attr",     SEED + 3),
  arm_row("synthetic", 55,                  FALSE, spec_of(55),  "3 synth/assumed/noattr",   SEED + 3),
  arm_row("synthetic", est$gross_entrants,  TRUE,  spec_of(est$gross_entrants), "4 synth/est/attr",     SEED + 4),
  arm_row("synthetic", est$gross_entrants,  FALSE, spec_of(est$gross_entrants), "4 synth/est/noattr",   SEED + 4)
)
cat("\n===== FULL ARM TABLE =====\n")
print(as.data.frame(tbl %>% mutate(across(where(is.numeric), ~round(.x, 2)))))
utils::write.csv(tbl, "scratchpad/backtest_arm_table.csv", row.names = FALSE)

# ---- 2. Uncertainty decomposition (queue item 4) ---------------------------
# One parameter family at a time, holding the arm fixed at the definition-
# matched configuration (no attrition), so the target and the estimand agree.

decomp <- function(label, spec, attrition = FALSE, entrants = est$gross_entrants) {
  a <- run_backtest_arm("derived", entrants_per_year = entrants, cutoff_year = CUT,
                        target_year = TGT, n_iterations = N,
                        apply_attrition = attrition, param_spec = spec, seed = SEED)
  p <- a$iterations$headcount[a$iterations$year == TGT]
  tibble::tibble(family = label, sd = stats::sd(p), width_95 = diff(stats::quantile(p, c(.025, .975))),
                 median = stats::median(p), covered = obs["2023"] >= stats::quantile(p, .025) &
                   obs["2023"] <= stats::quantile(p, .975))
}

# hazard_cv > 0 is NOT a shipped configuration: the published hazards carry no
# standard errors. Used here only to size the contribution it would make.
d <- dplyr::bind_rows(
  decomp("none (individual stochasticity only)", NULL),
  decomp("entrants only",                        spec_of(est$gross_entrants)),
  decomp("attrition only [INVENTED cv=0.20]",
         supply_parameter_spec(entrant_mean = est$gross_entrants, hazard_cv = 0.20),
         attrition = TRUE),
  decomp("entrants + attrition [INVENTED cv]",
         supply_parameter_spec(entrant_series = unname(est$yearly),
                               entrant_mean = est$gross_entrants,
                               departures = est$departures, hazard_cv = 0.20),
         attrition = TRUE)
)
cat("\n===== UNCERTAINTY DECOMPOSITION (derived cohort) =====\n")
print(as.data.frame(d %>% mutate(across(where(is.numeric), ~round(.x, 2)))))

# ---- 3. Arithmetic checks (queue item 5) -----------------------------------
cat("\n===== STRUCTURAL CHECKS =====\n")
n0 <- sum(backtest_cohorts_through(CUT)$n_certified)
cat("n0 (cumulative certs through 2020):", n0, " observed 2020:", obs["2020"],
    " match:", identical(as.numeric(n0), as.numeric(obs["2020"])), "\n")
cat("Transitions applied: predicted(no attr, e=55) - n0 =",
    tbl$pred_median[tbl$arm == "1 derived/assumed/noattr"] - n0,
    "=> years advanced =",
    (tbl$pred_median[tbl$arm == "1 derived/assumed/noattr"] - n0) / 55,
    "(expect 3)\n")
cat("Entrant rate that reproduces 2023 exactly:", (obs["2023"] - n0) / 3, "\n")
cat("Observed certification flow 2021-2023:",
    paste(urps_certification_cohorts()$n_certified[
      urps_certification_cohorts()$cert_year >= 2021], collapse = ", "), "\n")
cat("Pre-cutoff window 2018-2020:", paste(est$yearly, collapse = ", "),
    " mean =", round(est$gross_entrants, 2), " sd =", round(est$sd_entrants, 2), "\n")
