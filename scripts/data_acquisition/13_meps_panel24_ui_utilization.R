#!/usr/bin/env Rscript
# MEPS Panel 24 (2019-2022): UI office-visit utilization, four-year design.
#
# WHY PANEL 24 RATHER THAN MORE OF THE SAME. Panel 24 is the COVID-extended
# panel: nine rounds spanning 2019-2022 (HC-245). Four years does not merely add
# sample -- it buys two things Panel 27's two-year frame cannot provide:
#
#   1. A REAL WASHOUT. Panel 27 could only ask "no qualifying visit in the prior
#      calendar year". Here the 2021 entry cohort can require a clean 2019 AND
#      2020, so "newly observed" is closer to actually new.
#
#   2. UNCENSORED FOLLOW-UP. Panel 27's follow-up ended on 31 December, so an
#      entrant indexed in August had four months of observation and the raw
#      post-index mean was biased downward. Here follow-up runs a full 12 months
#      from the index visit, crossing the year boundary. Index month stops being
#      a nuisance parameter.
#
# THE TWO COHORTS ARE KEPT SEPARATE. Panel 24 straddles the COVID utilization
# shock. The 2020 entry cohort indexes into the collapse in ambulatory care; the
# 2021 cohort indexes into the rebound. Pooling them would average across a
# structural break and present the result as sampling noise. Agreement between
# them is far stronger evidence than a pooled point estimate.
#
# PHENOTYPE and the ESTIMAND DECOMPOSITION are identical to Panel 27 by
# construction -- see 12_meps_panel27_ui_utilization.R. Zeroes are retained; the
# denominator is never conditioned on the outcome.
suppressPackageStartupMessages({library(dplyr); library(survey)})
options(survey.lonely.psu = "adjust")
strip <- function(d) { d[] <- lapply(d, function(x)
  if (inherits(x, "haven_labelled")) unclass(x) else x); d }
rd <- function(f) strip(readRDS(file.path("data-raw/meps", paste0("meps_", f, ".rds"))))

long <- rd("h245")                                    # Panel 24, 2019-2022
cond <- list("2019" = rd("h214"), "2020" = rd("h222"),
             "2021" = rd("h231"), "2022" = rd("h241"))
link <- list("2019" = rd("h213if1"), "2020" = rd("h220if1"),
             "2021" = rd("h229if1"), "2022" = rd("h239i"))
offi <- list("2019" = rd("h213g"), "2020" = rd("h220g"),
             "2021" = rd("h229g"), "2022" = rd("h239g"))

# CCSR must exist in every year or the phenotype is not comparable across the
# panel. Refuse rather than silently fall back to CCS or to a 3-char ICD stem.
for (y in names(cond)) {
  if (!"CCSR1X" %in% names(cond[[y]]))
    stop(sprintf("CCSR1X absent from %s conditions file. Phenotype is not ",
                 "comparable across the panel; refusing to substitute.", y))
}

# ---- qualifying UI office visits, with dates, by year ---------------------
qualifying <- function(y) {
  ui <- cond[[y]] |> filter(CCSR1X == "GEN008")
  ev <- link[[y]] |> filter(EVENTYPE == 1) |>
    inner_join(ui |> select(DUPERSID, CONDIDX), by = c("DUPERSID", "CONDIDX")) |>
    distinct(DUPERSID, EVNTIDX)
  dt <- offi[[y]] |>
    select(DUPERSID, EVNTIDX, any_of(c("OBDATEYR", "OBDATEMM"))) |>
    semi_join(ev, by = c("DUPERSID", "EVNTIDX")) |>
    mutate(yr = suppressWarnings(as.numeric(OBDATEYR)),
           mm = suppressWarnings(as.numeric(OBDATEMM))) |>
    filter(!is.na(yr), !is.na(mm), mm >= 1, mm <= 12) |>
    mutate(t = yr * 12 + mm)                # months since epoch, for windowing
  message(sprintf("  %s: %5d GEN008 conditions, %4d persons, %4d dated visits, %4d persons with a visit",
                  y, nrow(ui), n_distinct(ui$DUPERSID), nrow(dt), n_distinct(dt$DUPERSID)))
  dt |> select(DUPERSID, EVNTIDX, t)
}
message("GEN008 phenotype, Panel 24:")
vis <- bind_rows(lapply(names(cond), function(y) qualifying(y) |> mutate(year = as.integer(y))))

wt   <- grep("^LONGWT", names(long), value = TRUE)[1]
strv <- grep("^VARSTR", names(long), value = TRUE)[1]
psu  <- grep("^VARPSU", names(long), value = TRUE)[1]
sexc <- grep("^SEX$",   names(long), value = TRUE)[1]
agec <- grep("^AGE.*X$|^AGELAST", names(long), value = TRUE)[1]
message(sprintf("\ndesign: weight=%s strata=%s psu=%s  sex=%s age=%s", wt, strv, psu, sexc, agec))

base <- long |>
  mutate(.female = .data[[sexc]] == 2,
         .adult  = .data[[agec]] >= 18,
         .obs    = .data[[wt]] > 0) |>
  select(DUPERSID, .female, .adult, .obs,
         all_of(c(wt, strv, psu)))

fmt <- function(est, label, n) {
  stopifnot(length(coef(est)) == 1L)
  ci <- confint(est)
  message(sprintf("    %-34s n=%3d  %7.3f  (95%% CI %7.3f - %7.3f)",
                  label, n, coef(est)[1], ci[1, 1], ci[1, 2]))
  invisible(c(estimate = unname(coef(est)[1]), lo = unname(ci[1, 1]),
              hi = unname(ci[1, 2]), n = n))
}

# ---- continuing utilization: baseline year -> next year -------------------
# Denominator anchored in the BASELINE year, zeroes retained.
continuing <- function(y0, y1) {
  message(sprintf("\n--- continuing: %d care -> %d return ---", y0, y1))
  v0 <- vis |> filter(year == y0) |> count(DUPERSID, name = "v0")
  v1 <- vis |> filter(year == y1) |> count(DUPERSID, name = "v1")
  d <- base |>
    left_join(v0, by = "DUPERSID") |> left_join(v1, by = "DUPERSID") |>
    mutate(v0 = coalesce(v0, 0L), v1 = coalesce(v1, 0L),   # ZEROES RETAINED
           baseline = .female & .adult & .obs & v0 > 0,
           returned = baseline & v1 > 0)
  des <- svydesign(id = ~ get(psu), strata = ~ get(strv), weights = ~ get(wt),
                   data = d, nest = TRUE)
  nb <- sum(d$baseline); nr <- sum(d$returned)
  message(sprintf("    baseline n=%d, returned n=%d", nb, nr))
  if (nb < 5) { message("    REFUSED: baseline n < 5."); return(invisible(NULL)) }
  b <- subset(des, baseline)
  p <- fmt(svymean(~ as.numeric(returned), b, na.rm = TRUE), "annual_return_probability", nb)
  u <- fmt(svymean(~ v1, b, na.rm = TRUE), "unconditional_followup_intensity", nb)
  if (nr >= 5) {
    cc <- fmt(svymean(~ v1, subset(des, returned), na.rm = TRUE), "conditional_followup_intensity", nr)
    message(sprintf("    identity: %.3f x %.3f = %.3f vs unconditional %.3f",
                    p["estimate"], cc["estimate"], p["estimate"] * cc["estimate"], u["estimate"]))
  } else message("    conditional intensity REFUSED: returners n < 5.")
  invisible(NULL)
}

# ---- entrants: true washout, uncensored 12-month follow-up ----------------
# washout_years must ALL be clean; index in index_year; follow-up is the 12
# months strictly after the index visit, which crosses the calendar boundary.
entrants <- function(washout_years, index_year) {
  message(sprintf("\n--- entrants: washout %s, index %d, 12-month follow-up ---",
                  paste(washout_years, collapse = "+"), index_year))
  wash <- vis |> filter(year %in% washout_years) |> distinct(DUPERSID)
  ix <- vis |> filter(year == index_year) |>
    group_by(DUPERSID) |> summarise(t_index = min(t), .groups = "drop") |>
    anti_join(wash, by = "DUPERSID")                     # clean washout
  fu <- vis |> inner_join(ix, by = "DUPERSID") |>
    filter(t > t_index, t <= t_index + 12) |>            # UNCENSORED 12 months
    count(DUPERSID, name = "post12")
  d <- base |>
    left_join(ix, by = "DUPERSID") |> left_join(fu, by = "DUPERSID") |>
    mutate(post12  = coalesce(post12, 0L),               # ZEROES RETAINED
           entrant = .female & .adult & .obs & !is.na(t_index),
           index_mm = ifelse(is.na(t_index), NA_real_, ((t_index - 1) %% 12) + 1))
  ne <- sum(d$entrant)
  message(sprintf("    entrant n=%d", ne))
  if (ne < 5) { message("    REFUSED: entrant n < 5."); return(invisible(NULL)) }
  des <- svydesign(id = ~ get(psu), strata = ~ get(strv), weights = ~ get(wt),
                   data = d, nest = TRUE)
  e <- subset(des, entrant)
  fmt(svymean(~ post12,   e, na.rm = TRUE), "post_index_visits_12mo", ne)
  fmt(svymean(~ index_mm, e, na.rm = TRUE), "index_month (nuisance now)", ne)
  invisible(NULL)
}

message("\n================ CONTINUING UTILIZATION ================")
continuing(2019, 2020); continuing(2020, 2021); continuing(2021, 2022)

message("\n================ ENTRANTS (COHORTS KEPT SEPARATE) ================")
entrants(washout_years = 2019,          index_year = 2020)   # 1-year washout
entrants(washout_years = c(2019, 2020), index_year = 2021)   # 2-year washout

message("\nNOTE: no model parameter is written by this script. The 2020 and 2021",
        "\n      entry cohorts straddle the COVID shock and are NOT pooled.",
        "\n      Adequacy is adjudicated in config/office_visit_validation_anchors.yml.")
