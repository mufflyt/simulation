#!/usr/bin/env Rscript
# How much did calibrating the ABOG departure hazard move the forecast, and does
# the calibrated hazard's tighter interval survive a stochastic back-test?
#
#   Rscript scripts/diagnostics/hazard_calibration_delta.R
#
# THE QUESTION. build_urps_exit_hazard() prefers cliff's exposure-based age-band
# empirical hazard (81 observed departures over person-years at risk, tier
# "calibrated") over the HWSM Weibull survival analogy (tier
# "derived_by_analogy") it replaced. This diagnostic quantifies the consequence
# of that swap two ways, holding EVERYTHING else identical so the delta isolates
# the hazard channel:
#
#   PART 1 (deterministic). Project the 2023 roster to 2050 under each hazard
#   with an identical entrant schedule. Reports (a) net supply -- which barely
#   moves, because young entrants dominate an entrant-fed re-anchored stock, the
#   supply-side echo of the demand-basket cancellation -- and (b) pure attrition
#   of the 2023 cohort, which moves a lot, because the analogy mis-shaped the age
#   profile (too high in mid-career, too low past 64; the real data has a
#   late-career retirement cliff).
#
#   PART 2 (stochastic back-test). Hindcast the fixed roster cohort's attrition
#   from 2013, propagating BOTH demographic Bernoulli noise and the arm's own
#   hazard uncertainty (hazard_cv: 0.111 calibrated vs 0.150 analogy), and score
#   bias, 95% coverage, and mean interval width against the observed active
#   series. "Observed active" uses the panel's PRESPECIFIED D2 meaningful-Medicare
#   activity flag, monotone-ized to a last-active year (a provider is counted
#   active through their last D2-active year). The point is to TEST, not assume,
#   whether the calibrated hazard's smaller hazard_cv translates into tighter
#   intervals once propagated. It does NOT: the calibrated arm's higher old-age
#   exit probabilities add demographic variance that offsets the CV advantage, so
#   propagated widths come out essentially equal (the raw CV ratio overstates the
#   gain). The calibrated arm also over-predicts attrition of this D2-active
#   cohort -- but see the confounds below before reading that as the hazard being
#   wrong. Numbers are printed and written to the CSV; do not hardcode them here.
#
# HONEST LIMITS. Deterministic ages are the documented 2060 - cert_year proxy and
# entrants enter at 37 (both identical across arms, so they set the LEVEL but
# cancel in the A-B delta). The back-test is CONFOUNDED and is a caution, not a
# validation: the cert proxy over-ages the pre-2013 cohort, and the observed
# series is Medicare-D2 billing PERSISTENCE, a stickier construct than the cliff
# ANCHORED departure the hazard was fit on -- so an over-retiring bias here does
# not by itself convict the calibrated old-age rates. Right-censored at 2023. The
# robust result is PART 1's deterministic delta; PART 2 is an honesty check that
# deflates the CV-only interval claim.
#
# FAIL-CLOSED. Needs the committed panel + age-band hazard CSV. Absent either, it
# prints what is missing and exits 0 without inventing a number.
# Writes artifacts/diagnostics/hazard_calibration_delta.csv.

suppressPackageStartupMessages({library(dplyr)})

# Load the hazard builder: installed package in CI, else source the module from a
# repo checkout so the diagnostic is runnable without a full (duckdb-gated) load.
if (requireNamespace("urpssim", quietly = TRUE)) {
  library(urpssim)
} else if (file.exists("R/supply-retirement_hazard.R")) {
  source("R/supply-retirement_hazard.R")
} else if (requireNamespace("pkgload", quietly = TRUE)) {
  pkgload::load_all(".", quiet = TRUE)
}

haz_csv <- Find(function(p) nzchar(p) && file.exists(p), c(
  system.file("extdata", "provider_year", "retirement_hazard_by_ageband.csv",
              package = "urpssim"),
  "inst/extdata/provider_year/retirement_hazard_by_ageband.csv"))
panel_fp <- Find(function(p) nzchar(p) && file.exists(p), c(
  system.file("extdata", "provider_year", "provider_year_activity_long.csv",
              package = "urpssim"),
  "inst/extdata/provider_year/provider_year_activity_long.csv"))

if (is.null(haz_csv) || is.null(panel_fp)) {
  message("hazard_calibration_delta: required committed artifacts are absent, so ",
          "nothing is computed (fail-closed).")
  message("Missing: ", paste(c(
    if (is.null(haz_csv)) "retirement_hazard_by_ageband.csv",
    if (is.null(panel_fp)) "provider_year_activity_long.csv"), collapse = ", "))
  quit(save = "no", status = 0L)
}

# --- both hazard tables from the SAME builder --------------------------------
A <- build_urps_exit_hazard(cliff_duckdb_path = NULL, cliff_ageband_csv = haz_csv,
                            verbose = FALSE)                 # calibrated
B <- build_urps_exit_hazard(cliff_duckdb_path = NULL,
                            cliff_ageband_csv = "___absent___", verbose = FALSE)  # analogy
message(sprintf("Arm A  source=%-24s n_events=%d  hazard_cv=%.3f",
                A$source, A$n_events, A$hazard_cv))
message(sprintf("Arm B  source=%-24s n_events=%d  hazard_cv=%.3f",
                B$source, B$n_events, B$hazard_cv))

haz_lookup <- function(res) setNames(res$exit_probs$prob_exit,
                                     paste(res$exit_probs$age, res$exit_probs$sex))
hA <- haz_lookup(A); hB <- haz_lookup(B)
px <- function(hv, age, sex) {
  age <- pmin(pmax(age, 30L), 80L)                 # carry end rates outside 30..80
  out <- hv[paste(age, sex)]; out[is.na(out)] <- 0; as.numeric(out)
}

# --- committed provider-year panel -> 2023 roster + D2 last-active year -------
panel <- utils::read.csv(panel_fp, stringsAsFactors = FALSE)
panel$sex <- ifelse(panel$gender == "F", "Female",
                    ifelse(panel$gender == "M", "Male", NA))
panel$age_proxy <- 2060L - as.integer(panel$cert_year)   # documented cert proxy

roster23 <- panel |>
  filter(year == 2023, !is.na(sex), !is.na(age_proxy),
         age_proxy >= 30, age_proxy <= 90) |>
  distinct(npi, .keep_all = TRUE) |>
  transmute(npi, sex, age = age_proxy)
message(sprintf("2023 roster: %d providers (%d F / %d M); age proxy median=%d",
                nrow(roster23), sum(roster23$sex == "Female"),
                sum(roster23$sex == "Male"), stats::median(roster23$age)))

# =============================================================================
# PART 1 - deterministic delta
# =============================================================================
to_state <- function(df) {
  t <- table(paste(df$age, df$sex)); setNames(as.numeric(t), names(t))
}
ENTRANTS <- 65; ENTRY_AGE <- 37L; F_SHARE <- 0.60; MAXAGE <- 85L
project_det <- function(hv, with_entrants) {
  st <- to_state(roster23)
  age <- as.integer(sub(" .*", "", names(st))); sex <- sub("^\\S+ ", "", names(st))
  supply <- c("2023" = sum(st))
  for (yr in 2024:2050) {
    df <- data.frame(age = age + 1L, sex = sex, n = st * (1 - px(hv, age, sex)))
    df <- df[df$age <= MAXAGE, ]
    if (with_entrants) df <- rbind(df,
      data.frame(age = ENTRY_AGE, sex = "Female", n = ENTRANTS * F_SHARE),
      data.frame(age = ENTRY_AGE, sex = "Male",   n = ENTRANTS * (1 - F_SHARE)))
    ag <- stats::aggregate(n ~ age + sex, df, sum)
    st <- setNames(ag$n, paste(ag$age, ag$sex)); age <- ag$age; sex <- ag$sex
    supply[as.character(yr)] <- sum(st)
  }
  supply
}
sA <- project_det(hA, TRUE);  sB <- project_det(hB, TRUE)
nA <- project_det(hA, FALSE); nB <- project_det(hB, FALSE)
n0 <- sum(to_state(roster23)); yrs <- as.character(seq(2023, 2050, 5))

message("\n=== PART 1a: net active supply (entrants @65/yr, identical both arms) ===")
print(data.frame(year = yrs, calibrated = round(sA[yrs]), analogy = round(sB[yrs]),
                 delta = round(sA[yrs] - sB[yrs]),
                 pct = sprintf("%+.1f%%", 100 * (sA[yrs] - sB[yrs]) / sB[yrs])),
      row.names = FALSE)
message("\n=== PART 1b: pure attrition of the 2023 cohort (no entrants) ===")
print(data.frame(year = yrs, calibrated = round(nA[yrs]), analogy = round(nB[yrs]),
                 cal_surv = sprintf("%.1f%%", 100 * nA[yrs] / n0),
                 ana_surv = sprintf("%.1f%%", 100 * nB[yrs] / n0)), row.names = FALSE)

# =============================================================================
# PART 2 - stochastic attrition back-test of the fixed cohort from 2013
# =============================================================================
# Monotone last-active year from the prespecified D2 meaningful-Medicare flag.
d2 <- panel |>
  filter(!is.na(sex), !is.na(age_proxy)) |>
  mutate(d2 = as.logical(d2_meaningful_partb))
last_active <- d2 |> filter(d2) |> group_by(npi) |>
  summarise(last_active_year = max(year), .groups = "drop")
meta <- d2 |> distinct(npi, sex, cert_year) |>
  inner_join(last_active, by = "npi")

BASE <- 2013L; TEST <- (BASE + 1L):2023L
cohort <- meta |>
  filter(!is.na(cert_year), as.integer(cert_year) <= BASE, last_active_year >= BASE) |>
  transmute(sex, age0 = 2060L - as.integer(cert_year),
            depart_year = last_active_year + 1L)   # first inactive year (right-censored at 2024+)
observed <- vapply(TEST, function(y) sum(cohort$depart_year > y), integer(1))
n_base <- nrow(cohort)
message(sprintf("\nBack-test cohort D2-active in %d: %d providers; observed active %d -> %d by 2023",
                BASE, n_base, observed[1], observed[length(observed)]))

set.seed(42L); NDRAW <- 2000L
sim_arm <- function(hv, cv) {
  # matrix draws x test-years of the surviving active count
  out <- matrix(0L, nrow = NDRAW, ncol = length(TEST))
  for (d in seq_len(NDRAW)) {
    shock <- stats::rlnorm(1, meanlog = -0.5 * cv^2, sdlog = cv)  # hazard uncertainty
    alive <- rep(TRUE, n_base); age <- cohort$age0; sex <- cohort$sex
    for (j in seq_along(TEST)) {
      p <- pmin(1, px(hv, age, sex) * shock)
      alive[alive] <- stats::runif(sum(alive)) >= p[alive]        # Bernoulli exits
      out[d, j] <- sum(alive); age <- age + 1L
    }
  }
  out
}
score <- function(mat) {
  med <- apply(mat, 2, stats::median)
  lo  <- apply(mat, 2, stats::quantile, 0.025)
  hi  <- apply(mat, 2, stats::quantile, 0.975)
  data.frame(
    bias_mean   = mean(med - observed),
    coverage95  = mean(observed >= lo & observed <= hi),
    width_mean  = mean(hi - lo),
    width_rel   = mean((hi - lo) / med))
}
mA <- sim_arm(hA, A$hazard_cv); mB <- sim_arm(hB, B$hazard_cv)
scA <- score(mA); scB <- score(mB)

message("\n=== PART 2: stochastic attrition back-test (2013 cohort, 2014-2023) ===")
bt <- data.frame(
  arm        = c("calibrated (cliff empirical)", "analogy (HWSM Weibull)"),
  hazard_cv  = c(A$hazard_cv, B$hazard_cv),
  bias_mean  = round(c(scA$bias_mean, scB$bias_mean), 1),
  coverage95 = sprintf("%.0f%%", 100 * c(scA$coverage95, scB$coverage95)),
  width_mean = round(c(scA$width_mean, scB$width_mean), 1),
  width_rel  = sprintf("%.1f%%", 100 * c(scA$width_rel, scB$width_rel)))
print(bt, row.names = FALSE)
message(sprintf("Interval width: calibrated is %.0f%% %s than the analogy.",
                100 * abs(scA$width_mean - scB$width_mean) / scB$width_mean,
                ifelse(scA$width_mean < scB$width_mean, "TIGHTER", "WIDER")))

# --- persist ------------------------------------------------------------------
dir.create("artifacts/diagnostics", recursive = TRUE, showWarnings = FALSE)
det <- data.frame(measure = "net_supply_2050",
                  calibrated = round(sA["2050"]), analogy = round(sB["2050"]),
                  delta = round(sA["2050"] - sB["2050"]))
det2 <- data.frame(measure = "cohort2023_surviving_2050",
                   calibrated = round(nA["2050"]), analogy = round(nB["2050"]),
                   delta = round(nA["2050"] - nB["2050"]))
rec <- dplyr::bind_rows(
  cbind(part = "deterministic", dplyr::bind_rows(det, det2),
        hazard_cv = NA, coverage95 = NA, width_mean = NA),
  data.frame(part = "backtest", measure = bt$arm,
             calibrated = NA, analogy = NA, delta = NA,
             hazard_cv = bt$hazard_cv,
             coverage95 = as.numeric(sub("%", "", bt$coverage95)) / 100,
             width_mean = bt$width_mean))
out_fp <- "artifacts/diagnostics/hazard_calibration_delta.csv"
utils::write.csv(rec, out_fp, row.names = FALSE)
message("\nWrote ", out_fp)
