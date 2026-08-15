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

# order 2023 visits within the year to count post-index follow-up
o23m <- o23 |> select(DUPERSID, EVNTIDX, OBDATEYR, OBDATEMM) |>
        semi_join(e23, by = c("DUPERSID","EVNTIDX")) |>
        mutate(mm = suppressWarnings(as.numeric(OBDATEMM))) |>
        filter(!is.na(mm), mm >= 1, mm <= 12)
post_index <- o23m |> group_by(DUPERSID) |>
  summarise(index_mm = min(mm), post_index_visits = sum(mm > min(mm)), .groups = "drop")

coh <- v23 |>
  left_join(v22, by = "DUPERSID") |>
  mutate(visits_2022 = coalesce(visits_2022, 0L),
         cohort = if_else(visits_2022 > 0, "continuing", "newly_observed")) |>
  left_join(post_index, by = "DUPERSID")

wt <- grep("^LONGWT", names(long), value = TRUE)[1]
str <- grep("^VARSTR", names(long), value = TRUE)[1]
psu <- grep("^VARPSU", names(long), value = TRUE)[1]
sexc <- grep("^SEX$", names(long), value = TRUE)[1]
agec <- grep("^AGE.*X$|^AGELAST", names(long), value = TRUE)[1]
message(sprintf("\ndesign: weight=%s strata=%s psu=%s  sex=%s age=%s", wt, str, psu, sexc, agec))

base <- long |> select(DUPERSID, w = all_of(wt), s = all_of(str), p = all_of(psu),
                       sx = all_of(sexc), ag = all_of(agec)) |>
        filter(sx == 2, ag >= 20)
d <- base |> left_join(coh, by = "DUPERSID") |>
     mutate(visits_2023 = coalesce(visits_2023, 0L),
            post_index_visits = coalesce(post_index_visits, 0L),
            cohort = coalesce(cohort, "no_ui_care"))

des <- svydesign(id = ~p, strata = ~s, weights = ~w, nest = TRUE, data = d)
report <- function(sub, var, label) {
  n <- nrow(subset(d, eval(sub, d)))
  if (n < 2) { message(sprintf("  %-34s n=%d  TOO FEW", label, n)); return(invisible(NULL)) }
  ss <- subset(des, eval(sub, d))
  m <- svymean(as.formula(paste0("~", var)), ss, na.rm = TRUE); ci <- confint(m)
  message(sprintf("  %-34s n=%3d  %.2f (95%% CI %.2f-%.2f)", label, n,
                  coef(m), ci[1], ci[2]))
}
message("\n=== weighted estimates, adult women, LONGWT ===")
report(quote(cohort == "continuing"), "visits_2023",
       "annual_followup_rate")
report(quote(cohort == "newly_observed"), "post_index_visits",
       "first_year_followup_rate")
message("\nunweighted cohort sizes:")
print(table(d$cohort[d$cohort != "no_ui_care"]))
