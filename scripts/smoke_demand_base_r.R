#!/usr/bin/env Rscript
# Base-R smoke test for the URPS demand stack.
#
# Exercises the BASE-R cores of the demand pipeline WITHOUT the tidyverse,
# devtools or testthat -- so the pipeline can be sanity-checked in a locked-down
# session where CRAN egress is blocked and only base R is available. It is not a
# replacement for devtools::test() (which covers the tidyverse paths too); it is
# the check you can always run.
#
#   Rscript scripts/smoke_demand_base_r.R           # from the repo root
#
# Sources only the base-R modules and calls only their base-R entry points
# (the DMDM engines, the contract exporters, the onset fitter core, the
# geographic-demand module), plus an optional round-trip through cliff's ingestion
# if a cliff checkout is found. Exits non-zero on the first failure.

ok <- function(cond, msg) {
  if (!isTRUE(cond)) stop("FAIL: ", msg, call. = FALSE)
  cat("  pass:", msg, "\n")
}
src <- function(f) if (file.exists(f)) source(f) else stop("missing ", f, call. = FALSE)

# --- load base-R modules ----------------------------------------------------
# R/10 first: it defines .msg_warn() and resolve_reproducibility_mode(), which
# the provenance guards in R/29 (calibration) and R/30 (population conservation)
# call. R/30's guard only fires when a reweight leaks, so the dependency went
# unnoticed until R/29's calibration gate started running on every call. R/10 is
# base R and sources under --vanilla, so the "no tidyverse" contract holds.
src("R/10-repro_provenance.R")             # .msg_warn, resolve_reproducibility_mode
src("R/29-demand_dynamic_multistate.R")   # dmdm_default_transitions, simulate_dmdm
src("R/30-demand_dynamic_open.R")          # simulate_dmdm_open (+ helpers)
src("R/31-dmdm_fit_transitions.R")         # .fit_onset_coefs, .fit_stage_transitions
src("R/32-geographic_demand.R")            # geographic (isochrone) demand
src("R/geographic_holdout_validation.R")   # geographic held-out (spatial CV)
src("R/58-pop_transitions.R")              # literature POP onset + staged transitions
src("R/export_demand_contract.R")          # export_hdmm/dmdm_demand_contract

set.seed(1)
mk <- function(ages, vag) data.frame(
  age = ages, cumulative_vaginal_deliveries = vag,
  years_since_last_vaginal_birth = pmax(0, ages - 30), bmi = 28,
  hysterectomy = 0, menopause_status = as.integer(ages >= 51), comorbidity = 0)

cat("== DMDM closed engine (R/29) ==\n")
lo <- simulate_dmdm(mk(sample(45:70, 8000, TRUE), 0L), 2025, 2045, seed = 42, allow_uncalibrated = TRUE)
hi <- simulate_dmdm(mk(sample(45:70, 8000, TRUE), 3L), 2025, 2045, seed = 42, allow_uncalibrated = TRUE)
ok(nrow(lo) == 21L, "one row per year")
ok(all(diff(lo$living) <= 0), "closed cohort shrinks via mortality")
ok(hi$prev_pop[21] > lo$prev_pop[21], "more vaginal deliveries -> higher prolapse prevalence")

cat("== DMDM open engine (R/30) ==\n")
agents <- function(ages, vag, w) cbind(mk(ages, vag), weight = w, p_ui = .05, p_pop = .025, p_ai = .025)
init <- agents(40:84, 2L, 1e5)
ent <- do.call(rbind, lapply(2026:2035, function(y) { d <- agents(40, 2L, 1e5); d$entry_year <- y; d }))
op <- simulate_dmdm_open(init, ent, 2025, 2035, allow_uncalibrated = TRUE)
ok(all(op$population > 0.5 * op$population[1]), "open population replenishes (no collapse)")
proj <- do.call(rbind, lapply(2025:2035, function(y) data.frame(year = y, age = 40:90, population = 1e6)))
opr <- simulate_dmdm_open(init, ent, 2025, 2035, pop_by_age_year = proj, allow_uncalibrated = TRUE)
ok(abs(opr$population[1] - 45e6) < 1, "reweighting: counts match the projection")

cat("== onset fitter core (R/31) ==\n")
N <- 20000; age <- sample(40:85, N, TRUE); vag <- rpois(N, 2)
df <- data.frame(from = 0L,
  event = rbinom(N, 1, plogis(-3 + 0.30 * vag + 0.30 * ((age - 50) / 10))),
  age = age, cumulative_vaginal_deliveries = vag,
  years_since_last_vaginal_birth = pmax(0, age - 30), bmi = 28,
  hysterectomy = 0, menopause_status = as.integer(age >= 51), comorbidity = 0)
est <- .fit_onset_coefs(df)
ok(abs(est[["avag"]] - 0.30) < 0.06, "recovers vaginal-delivery onset coefficient")

cat("== geographic (isochrone) demand (R/32) ==\n")
geo <- data.frame(need = c(100, 400, 250), nearest_provider_min = c(15, 90, 240),
                  access_ratio = c(3, 0.5, 0.1), capacity = c(120, 300, 40))
gd <- geographic_demand_summary(geo)
ok(abs(gd$beyond_share - 250 / 750) < 1e-9, "need beyond 180 min computed")
ok(gd$need_weighted_access < mean(geo$access_ratio), "need-weighted access below unweighted")
# tract age-band population -> need bridge (script 08 -> R/32)
tr_pop <- data.frame(GEOID = c("A", "B", "C"),
  female_20_39 = c(1000, 500, 200), female_40_59 = c(2000, 800, 300),
  female_60_64 = c(500, 200, 100), female_65_79 = c(800, 400, 150),
  female_80plus = c(300, 150, 50), nearest_provider_min = c(15, 90, 240))
prev_band <- c("20-39" = .05, "40-59" = .20, "60-64" = .35, "65-79" = .45, "80+" = .50)
nt <- tract_need_from_population(tr_pop, prevalence = prev_band)
ok(abs(nt$need[1] - 1135) < 1e-9, "tract need = sum(pop_band * prevalence_band)")
gs <- isochrone_demand_from_tracts(tr_pop, prevalence = prev_band)
ok(abs(gs$total_need - sum(nt$need)) < 1e-9, "isochrone assembly totals the tract need")

cat("== geographic held-out CV (R/geographic_holdout_validation.R) ==\n")
set.seed(42); Gh <- 60; xh <- runif(Gh, 0.5, 5)
gh <- data.frame(geo = paste0("g", 1:Gh), x = xh, obs = rpois(Gh, exp(1.2 + 0.5 * xh)))
rh <- geographic_holdout_cv(gh, "obs", "x", geo = "geo", scheme = "loo")
ok(rh$metrics$r2_oos > 0.3 && rh$metrics$spearman > 0.6, "recovers a real spatial relationship out-of-sample")
gh2 <- gh; gh2$obs[1] <- 100000L
p1 <- geographic_holdout_cv(gh2, "obs", "x", geo = "geo", scheme = "loo")$predictions
ok(p1$predicted[p1$geo == "g1"] < 1000, "leakage-free: outlier geo cannot predict its own value")

cat("== literature POP transitions (R/33) ==\n")
ptr <- dmdm_transitions_with_pop_literature()
ok(ptr$calibration_status == "derived_by_analogy", "POP overlay marked derived_by_analogy")
ok(ptr$provenance$ui == "placeholder_uncalibrated", "UI/AI left as placeholders")
ok(ptr$onset$pop[["avag"]] > 0, "vaginal delivery is a positive POP onset driver")
ok(ptr$pop_regression[["1"]] > ptr$pop_progression[["1"]],
   "mild POP regresses more than it progresses (the feature UI lacks)")
plo <- simulate_dmdm(mk(sample(45:70, 4000, TRUE), 0L), 2025, 2035, transitions = ptr, seed = 7, allow_uncalibrated = TRUE)
phi <- simulate_dmdm(mk(sample(45:70, 4000, TRUE), 3L), 2025, 2035, transitions = ptr, seed = 7, allow_uncalibrated = TRUE)
ok(phi$prev_pop[11] > plo$prev_pop[11], "literature transitions: more vaginal deliveries -> more POP")

cat("== staged POP transition fit (R/31 .fit_stage_transitions) ==\n")
set.seed(11); M <- 60000; fs <- sample(0:3, M, TRUE)
pu <- c(`0` = .10, `1` = .08, `2` = .05, `3` = .03); pd <- c(`1` = .20, `2` = .08, `3` = .03)
ts <- vapply(seq_len(M), function(i) { s <- fs[i]; u <- runif(1)
  a <- pu[[as.character(s)]]; b <- if (s > 0) pd[[as.character(s)]] else 0
  if (u < a && s < 4L) s + 1L else if (u < a + b && s > 0L) s - 1L else s }, integer(1))
sf <- .fit_stage_transitions(data.frame(from_stage = fs, to_stage = ts))
ok(abs(sf$progression[["1"]] - .08) < .02, "recovers stage 1->2 progression rate")
ok(abs(sf$regression[["1"]] - .20) < .02, "recovers stage 1->0 regression rate")

cat("== contract exporters (R/export_demand_contract.R) ==\n")
hd <- export_hdmm_demand_contract(
  data.frame(year = 2025:2030, care_seeking_national = seq(4e6, 4.6e6, length.out = 6),
             service_units_national = seq(9e6, 10.8e6, length.out = 6)),
  output_directory = tempfile("h_"), verbose = FALSE, allow_uncalibrated = TRUE)
ok(all(c("tier5_care_seeking", "tier6_procedural") %in% hd$data$denominator_tier), "HDMM tiers 5-6 emitted")
dm_traj <- data.frame(year = 2025:2030, population = seq(45e6, 48e6, length.out = 6),
                      prev_ui = seq(.2, .26, length.out = 6), prev_pop = seq(.08, .14, length.out = 6),
                      prev_ai = seq(.05, .07, length.out = 6))
dm <- export_dmdm_demand_contract(dm_traj, output_directory = tempfile("d_"), verbose = FALSE,
                                  allow_uncalibrated = TRUE)
ok("tier3_prevalent_pfd" %in% dm$data$denominator_tier, "DMDM tier3 emitted")
# Built from the literature POP transitions -> per-tier provenance is stamped.
dmp <- export_dmdm_demand_contract(dm_traj, output_directory = tempfile("dp_"),
                                   transitions = dmdm_transitions_with_pop_literature(),
                                   verbose = FALSE, allow_uncalibrated = TRUE)
pop_row <- dmp$data[dmp$data$denominator_tier == "dmdm_pop", ][1, ]
ui_row  <- dmp$data[dmp$data$denominator_tier == "dmdm_ui", ][1, ]
ok(pop_row$tier_calibration_status == "derived_by_analogy", "dmdm_pop tier is derived_by_analogy")
ok(ui_row$tier_calibration_status == "placeholder_uncalibrated", "dmdm_ui tier stays placeholder")

cat("== cliff ingestion round-trip (optional) ==\n")
cliff_fn <- Find(file.exists, c("../cliff/R/dpmm_contract.R", "/home/user/cliff/R/dpmm_contract.R"))
if (!is.null(cliff_fn)) {
  source(cliff_fn)
  ct <- read_dpmm_demand_contract(dm$csv_path)
  d3 <- dpmm_alt_d1_index(ct$data, 2025:2035, base_year = 2025L, tier = "tier3_prevalent_pfd")
  ok(abs(d3[1] - 100) < 1e-9, "cliff consumes DMDM tier3 (rebased to 100)")
  # cliff reads the POP-specific literature series and its per-tier provenance
  ctp <- read_dpmm_demand_contract(dmp$csv_path)
  dpop <- dpmm_alt_d1_index(ctp$data, 2025:2035, base_year = 2025L, tier = "dmdm_pop")
  ok(dpmm_series_usable(dpop), "cliff consumes DMDM POP-specific series (dmdm_pop)")
  ok(dpmm_tier_status(ctp, "dmdm_pop") == "derived_by_analogy",
     "cliff reads dmdm_pop provenance = derived_by_analogy")
} else {
  cat("  skip: no cliff checkout found\n")
}

cat("\nALL BASE-R DEMAND SMOKE CHECKS PASSED\n")
