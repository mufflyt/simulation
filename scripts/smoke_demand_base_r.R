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
src("R/29-demand_dynamic_multistate.R")   # dmdm_default_transitions, simulate_dmdm
src("R/30-demand_dynamic_open.R")          # simulate_dmdm_open (+ helpers)
src("R/31-dmdm_fit_transitions.R")         # .fit_onset_coefs (base-R core)
src("R/32-geographic_demand.R")            # geographic (isochrone) demand
src("R/export_demand_contract.R")          # export_hdmm/dmdm_demand_contract

set.seed(1)
mk <- function(ages, vag) data.frame(
  age = ages, cumulative_vaginal_deliveries = vag,
  years_since_last_vaginal_birth = pmax(0, ages - 30), bmi = 28,
  hysterectomy = 0, menopause_status = as.integer(ages >= 51), comorbidity = 0)

cat("== DMDM closed engine (R/29) ==\n")
lo <- simulate_dmdm(mk(sample(45:70, 8000, TRUE), 0L), 2025, 2045, seed = 42)
hi <- simulate_dmdm(mk(sample(45:70, 8000, TRUE), 3L), 2025, 2045, seed = 42)
ok(nrow(lo) == 21L, "one row per year")
ok(all(diff(lo$living) <= 0), "closed cohort shrinks via mortality")
ok(hi$prev_pop[21] > lo$prev_pop[21], "more vaginal deliveries -> higher prolapse prevalence")

cat("== DMDM open engine (R/30) ==\n")
agents <- function(ages, vag, w) cbind(mk(ages, vag), weight = w, p_ui = .05, p_pop = .025, p_ai = .025)
init <- agents(40:84, 2L, 1e5)
ent <- do.call(rbind, lapply(2026:2035, function(y) { d <- agents(40, 2L, 1e5); d$entry_year <- y; d }))
op <- simulate_dmdm_open(init, ent, 2025, 2035)
ok(all(op$population > 0.5 * op$population[1]), "open population replenishes (no collapse)")
proj <- do.call(rbind, lapply(2025:2035, function(y) data.frame(year = y, age = 40:90, population = 1e6)))
opr <- simulate_dmdm_open(init, ent, 2025, 2035, pop_by_age_year = proj)
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

cat("== contract exporters (R/export_demand_contract.R) ==\n")
hd <- export_hdmm_demand_contract(
  data.frame(year = 2025:2030, care_seeking_national = seq(4e6, 4.6e6, length.out = 6),
             service_units_national = seq(9e6, 10.8e6, length.out = 6)),
  output_directory = tempfile("h_"), verbose = FALSE)
ok(all(c("tier5_care_seeking", "tier6_procedural") %in% hd$data$denominator_tier), "HDMM tiers 5-6 emitted")
dm <- export_dmdm_demand_contract(
  data.frame(year = 2025:2030, population = seq(45e6, 48e6, length.out = 6),
             prev_ui = seq(.2, .26, length.out = 6), prev_pop = seq(.08, .14, length.out = 6),
             prev_ai = seq(.05, .07, length.out = 6)),
  output_directory = tempfile("d_"), verbose = FALSE)
ok("tier3_prevalent_pfd" %in% dm$data$denominator_tier, "DMDM tier3 emitted")

cat("== cliff ingestion round-trip (optional) ==\n")
cliff_fn <- Find(file.exists, c("../cliff/R/dpmm_contract.R", "/home/user/cliff/R/dpmm_contract.R"))
if (!is.null(cliff_fn)) {
  source(cliff_fn)
  ct <- read_dpmm_demand_contract(dm$csv_path)
  d3 <- dpmm_alt_d1_index(ct$data, 2025:2035, base_year = 2025L, tier = "tier3_prevalent_pfd")
  ok(abs(d3[1] - 100) < 1e-9, "cliff consumes DMDM tier3 (rebased to 100)")
} else {
  cat("  skip: no cliff checkout found\n")
}

cat("\nALL BASE-R DEMAND SMOKE CHECKS PASSED\n")
