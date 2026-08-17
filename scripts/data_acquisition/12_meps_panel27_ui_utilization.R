#!/usr/bin/env Rscript
# MEPS Panel 27 (2022-2023): UI office-visit utilization, person-level.
#
# PHENOTYPE. Primary is CCSR == "GEN008" (AHRQ label: URINARY INCONTINENCE).
# ICD10CDX == "N39" is NOT used to expand the primary analysis: at three
# characters it conflates urinary tract infection (N39.0) with stress and urge
# incontinence (N39.3/.4). Sample size bought that way corrupts the phenotype.
#
# WASHOUT. The two-year frame does NOT identify true first-ever urogynecologic
# care. It identifies:
#     newly observed in 2023 = qualifying 2023 visit AND no qualifying 2022 visit
# which is "newly observed after a one-year washout". A patient treated in 2021
# and returning in 2023 is misclassified as new. This estimate must therefore
# never populate a parameter named first_entry_rate.
suppressPackageStartupMessages({library(dplyr); library(survey)})
options(survey.lonely.psu = "adjust")
strip <- function(d) { d[] <- lapply(d, function(x)
  if (inherits(x, "haven_labelled")) unclass(x) else x); d }

rd <- function(f) strip(readRDS(file.path("data-raw/meps", paste0("meps_", f, ".rds"))))
long <- rd("h252")
c22 <- rd("h241"); l22 <- rd("h239i"); o22 <- rd("h239g")
c23 <- rd("h249"); l23 <- rd("h248i"); o23 <- rd("h248g")

ui_persons <- function(cond, link, year_label) {
  ui <- cond |> filter(CCSR1X == "GEN008")
  ev <- link |> filter(EVENTYPE == 1) |>
        inner_join(ui |> select(DUPERSID, CONDIDX), by = c("DUPERSID","CONDIDX")) |>
        distinct(DUPERSID, EVNTIDX)
  message(sprintf("  %s: %d GEN008 conditions, %d persons, %d linked office visits, %d persons with a visit",
                  year_label, nrow(ui), n_distinct(ui$DUPERSID),
                  nrow(ev), n_distinct(ev$DUPERSID)))
  ev
}
message("GEN008 phenotype:")
e22 <- ui_persons(c22, l22, "2022"); e23 <- ui_persons(c23, l23, "2023")

v22 <- e22 |> count(DUPERSID, name = "visits_2022")
v23 <- e23 |> count(DUPERSID, name = "visits_2023")

# ---------------------------------------------------------------------------
# ESTIMAND CORRECTION (reviewer, 2026-08).
#
# The prior version built the cohort as v23 |> left_join(v22), so the
# denominator was conditioned on having a 2023 visit. That estimator answers
# E(2023 visits | 2022 care AND >=1 2023 visit), which is bounded below by 1 by
# construction and preferentially selects high utilizers. It cannot be used as
# a per-patient intensity.
#
# The baseline cohort is now anchored in 2022 and ZEROES ARE RETAINED:
#     baseline = qualifying GEN008-linked care in 2022
#                + longitudinally observable in 2023
#
#     annual_return_probability      = P(any qualifying 2023 visit | 2022 care)
#     conditional_followup_intensity = E(2023 visits | 2022 care, >=1 2023 visit)
#     unconditional_followup_intensity
#                                    = E(2023 visits | 2022 care)   [zeros kept]
#                                    = return_probability x conditional_intensity
#
# unconditional_followup_intensity is the quantity that maps to
#     return_visit <- previous_care_engaged * unconditional_followup_intensity
# and is safer than a separate retention_rate until "still under care but no
# visit this year" can be distinguished empirically from true disengagement.
# ---------------------------------------------------------------------------

wt   <- grep("^LONGWT", names(long), value = TRUE)[1]
strv <- grep("^VARSTR", names(long), value = TRUE)[1]
psu  <- grep("^VARPSU", names(long), value = TRUE)[1]
sexc <- grep("^SEX$",   names(long), value = TRUE)[1]
agec <- grep("^AGE.*X$|^AGELAST", names(long), value = TRUE)[1]
message(sprintf("\ndesign: weight=%s strata=%s psu=%s  sex=%s age=%s",
                wt, strv, psu, sexc, agec))

# index month and post-index visit counts within 2023, for the entrant analysis
o23m <- o23 |>
  select(DUPERSID, EVNTIDX, OBDATEYR, OBDATEMM) |>
  semi_join(e23, by = c("DUPERSID", "EVNTIDX")) |>
  mutate(mm = suppressWarnings(as.numeric(OBDATEMM))) |>
  filter(!is.na(mm), mm >= 1, mm <= 12)

idx <- o23m |>
  group_by(DUPERSID) |>
  summarise(index_mm          = min(mm),
            post_index_visits = sum(mm > min(mm)),
            .groups = "drop") |>
  mutate(months_observable = 12 - index_mm)   # calendar-year right censoring

# Build the design on the FULL longitudinal file, then subset. Never drop rows
# before svydesign() -- dropping them discards the variance structure.
d <- long |>
  mutate(.female = .data[[sexc]] == 2,
         .adult  = .data[[agec]] >= 18,
         .obs23  = .data[[wt]] > 0) |>
  left_join(v22, by = "DUPERSID") |>
  left_join(v23, by = "DUPERSID") |>
  left_join(idx, by = "DUPERSID") |>
  mutate(visits_2022 = coalesce(visits_2022, 0L),
         visits_2023 = coalesce(visits_2023, 0L),      # ZEROES RETAINED
         baseline    = .female & .adult & .obs23 & visits_2022 > 0,
         returned    = baseline & visits_2023 > 0,
         entrant     = .female & .adult & .obs23 & visits_2022 == 0 & visits_2023 > 0)

des <- svydesign(id = ~ get(psu), strata = ~ get(strv), weights = ~ get(wt),
                 data = d, nest = TRUE)

# NOTE: survey treats a logical as a two-level factor, so svymean(~lgl) returns
# BOTH levels and coef(.)[1] is the FALSE cell. Every indicator is therefore
# coerced with as.numeric() at the formula, and confint is indexed [1, ] rather
# than by flat position -- confint() is column-major, so ci[2] on a two-row
# result is the second LOWER bound, not the upper bound.
fmt <- function(est, label, n) {
  stopifnot(length(coef(est)) == 1L)
  ci <- confint(est)
  message(sprintf("  %-32s n=%3d  %6.3f  (95%% CI %6.3f - %6.3f)",
                  label, n, coef(est)[1], ci[1, 1], ci[1, 2]))
  invisible(c(estimate = unname(coef(est)[1]),
              lo = unname(ci[1, 1]), hi = unname(ci[1, 2]), n = n))
}

n_base <- sum(d$baseline); n_ret <- sum(d$returned); n_ent <- sum(d$entrant)
message(sprintf("\ncohorts: baseline(2022 care)=%d  returned in 2023=%d  entrants=%d",
                n_base, n_ret, n_ent))

message("\n=== continuing utilization, denominator anchored in 2022 ===")
b <- subset(des, baseline)
p_ret  <- fmt(svymean(~ as.numeric(returned), b, na.rm = TRUE), "annual_return_probability",       n_base)
uncond <- fmt(svymean(~ visits_2023, b, na.rm = TRUE), "unconditional_followup_intensity", n_base)
cond   <- fmt(svymean(~ visits_2023, subset(des, returned), na.rm = TRUE),
              "conditional_followup_intensity",  n_ret)

message(sprintf("\nidentity check (must reconcile exactly): return_prob x conditional = %.3f  vs  unconditional = %.3f",
                p_ret["estimate"] * cond["estimate"], uncond["estimate"]))

# --- entrants: calendar-year right censoring ------------------------------
# A patient first observed in January has ~11 months to accumulate follow-up;
# one first observed in November has one. Averaging post-index visits across
# them without accounting for index month is not interpretable.
message("\n=== apparent 2023 entrants (NEWLY OBSERVED AFTER ONE-YEAR WASHOUT) ===")
ent <- subset(des, entrant)
fmt(svymean(~ index_mm,          ent, na.rm = TRUE), "index_month",              n_ent)
fmt(svymean(~ months_observable, ent, na.rm = TRUE), "months_observable",        n_ent)
fmt(svymean(~ post_index_visits, ent, na.rm = TRUE), "post_index_visits (raw)",  n_ent)

# person-time rate: total post-index visits / total observable person-months
pm <- svyratio(~ post_index_visits, ~ months_observable, ent, na.rm = TRUE)
pm_ci <- confint(pm)
message(sprintf("  %-32s n=%3d  %6.4f  (95%% CI %6.4f - %6.4f)  visits/person-month",
                "post_index_rate_per_month", n_ent, coef(pm)[1], pm_ci[1], pm_ci[2]))

# fixed-window sensitivity: index early enough to permit >=6 months observation
n_fw <- sum(d$entrant & !is.na(d$index_mm) & d$index_mm <= 6)
message(sprintf("\nfixed-window sensitivity (index month <= 6, >=6 months observable): n=%d", n_fw))
if (n_fw >= 5) {
  fmt(svymean(~ post_index_visits, subset(des, entrant & index_mm <= 6), na.rm = TRUE),
      "post_index_visits (>=6mo window)", n_fw)
} else {
  message("  REFUSED: n < 5. Not estimated.")
}

message("\nNOTE: no model parameter is written by this script. Adequacy of n and",
        "\n      uncertainty is adjudicated in config/office_visit_validation_anchors.yml.")
