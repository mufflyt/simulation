#!/usr/bin/env Rscript
# ===========================================================================
# Calibrate the DMDM (dynamic multistate disease model) so its simulated
# age-specific pelvic-floor-disorder prevalence reproduces the Nygaard 2008 /
# Wu 2014 age-band targets. This is the "Route B" target-fit (no SWAN microdata):
# for each condition we fit the onset LEVEL (a0), the onset AGE-GRADIENT (aage), and
# the annual REMISSION to minimize age-band relative error, using the engine's own
# aging Markov recurrence at a population-representative covariate profile. It flips
# the transitions' status placeholder_uncalibrated -> calibrated and saves them; a
# MAPE tolerance gate makes it FAIL LOUD if any condition (or the composite any-PFD)
# does not match, so "calibrated" is earned, not asserted.
#
# Cures the placeholder "8% -> 99%" runaway: raising remission and setting a0/aage to
# the data pulls the Markov plateau p* = onset/(onset+remission) down to the observed
# ~0.33 any-PFD level instead of saturating toward 1.
#
# Output: artifacts/calibrated_dmdm_transitions.rds  (status = "calibrated")
# Run:    Rscript scripts/calibrate_dmdm_prevalence.R
# ===========================================================================
suppressPackageStartupMessages(pkgload::load_all(".", quiet = TRUE))
NS  <- asNamespace("urpssim")
onp <- get(".dmdm_onset_p", NS)

# ---- targets: Nygaard/Wu age-band prevalence. AI = anal incontinence proxied by FI.
TARGET <- list(ui  = get(".UI_PREVALENCE_BY_BAND",  NS),
               pop = get(".POP_PREVALENCE_BY_BAND", NS),
               ai  = get(".FI_PREVALENCE_BY_BAND",  NS))
PFD_TARGET <- get(".PFD_PREVALENCE_BY_BAND", NS)
BANDS <- list("18-34" = 18:34, "35-44" = 35:44, "45-64" = 45:64, "65-74" = 65:74, "75+" = 75:89)
AGES  <- 18:89
# population-representative covariate profile for the marginal onset (bmi centered -> 0)
REP <- list(vag = 2, ysl = 0, bmi = 27, hyst = 0, com = 0)
TOL <- 0.10   # genuine calibration tolerance (age-band MAPE)

# Age-specific prevalence via the engine's aging Markov recurrence (what the open
# engine actually computes), at a population-representative covariate profile.
sim_prev_by_age <- function(a, rem) {
  meno  <- as.integer(AGES >= 51)
  onset <- onp(a, AGES, REP$vag, REP$ysl, REP$bmi, REP$hyst, meno, REP$com)
  p <- numeric(length(AGES))
  for (i in 2:length(AGES)) p[i] <- p[i - 1] * (1 - rem) + (1 - p[i - 1]) * onset[i - 1]
  setNames(p, AGES)
}
band_prev <- function(p) vapply(BANDS, function(ag) mean(p[as.character(ag)], na.rm = TRUE), numeric(1))
mape      <- function(sim, tgt) mean(abs(sim - tgt) / tgt)

# Fit onset LEVEL (a0), AGE-GRADIENT (aage) and REMISSION per condition to the age-band
# target. NOTE (documented limitation): the 2-state monotone-onset Markov cannot
# reproduce Nygaard's 65-74 -> 75+ PLATEAU and overshoots the oldest band by ~25%;
# tightening that requires a model-structure enhancement (age-varying remission or a
# non-monotone onset). TOL is set accordingly and the overshoot is reported.
fit_cond <- function(cc, a0v, rem0) {
  tgt <- TARGET[[cc]]
  obj <- function(par) {
    a <- a0v; a["a0"] <- par[1]; a["aage"] <- par[2]
    sum(((band_prev(sim_prev_by_age(a, stats::plogis(par[3]))) - tgt) / tgt)^2)
  }
  o <- stats::optim(c(a0v["a0"], a0v["aage"], stats::qlogis(rem0)), obj,
                    method = "Nelder-Mead", control = list(maxit = 5000, reltol = 1e-12))
  a <- a0v; a["a0"] <- o$par[1]; a["aage"] <- o$par[2]; rem <- stats::plogis(o$par[3])
  list(a = a, rem = rem, sim = band_prev(sim_prev_by_age(a, rem)), tgt = tgt)
}

tr <- dmdm_default_transitions()
res <- lapply(c(ui = "ui", pop = "pop", ai = "ai"),
              function(cc) fit_cond(cc, tr$onset[[cc]], tr$remission[cc]))

# apply fitted parameters (status is flipped to "calibrated" ONLY if the gate below passes)
for (cc in names(res)) { tr$onset[[cc]] <- res[[cc]]$a; tr$remission[cc] <- res[[cc]]$rem }
tr$calibration_method <- sprintf("target_fit_nygaard_ageband_%s", Sys.getenv("RECOVER_DATE", "2026-08-08"))

# ---- validate: per-condition + composite any-PFD age-band MAPE ----------------
cat("condition  band-MAPE   a0      aage    remission\n")
for (cc in names(res)) cat(sprintf("  %-4s      %5.1f%%   %6.3f  %6.3f  %6.3f\n",
  cc, 100 * mape(res[[cc]]$sim, res[[cc]]$tgt), res[[cc]]$a["a0"], res[[cc]]$a["aage"], res[[cc]]$rem))
pfd_sim  <- 1 - (1 - res$ui$sim) * (1 - res$pop$sim) * (1 - res$ai$sim)
pfd_mape <- mape(pfd_sim, PFD_TARGET)
cat(sprintf("\nany-PFD band-MAPE = %.1f%%\n", 100 * pfd_mape))
cat("any-PFD simulated:", paste(sprintf("%.3f", pfd_sim), collapse = " "), "\n")
cat("any-PFD target   :", paste(sprintf("%.3f", PFD_TARGET), collapse = " "), "\n")

# Old-age fidelity matters most: cliff's demand is women 65+, so the 65-74 and 75+
# bands must not overshoot. Gate on BOTH worst-band MAPE and old-age overshoot.
worst        <- max(vapply(names(res), function(cc) mape(res[[cc]]$sim, res[[cc]]$tgt), numeric(1)), pfd_mape)
old_overshoot <- max((pfd_sim[c("65-74", "75+")] - PFD_TARGET[c("65-74", "75+")]) /
                     PFD_TARGET[c("65-74", "75+")])
cat(sprintf("worst band-MAPE = %.1f%% ; any-PFD old-age (65+) overshoot = %.1f%%\n",
            100 * worst, 100 * old_overshoot))

if (worst >= TOL || old_overshoot >= 0.10) {
  message(sprintf(paste0(
    "\nNOT CALIBRATED: parameter fit hits a STRUCTURAL wall (worst MAPE %.1f%%, 65+ overshoot %.1f%%).\n",
    "The 2-state monotone-onset Markov cannot reproduce Nygaard's 65-74 -> 75+ plateau by\n",
    "tuning onset level/gradient/remission alone -- prevalence accumulates and overshoots the\n",
    "oldest bands (the ages that drive cliff's 65+ demand). The minimal REAL fix is a disease-\n",
    "model enhancement: age-varying remission (remission rises modestly at older ages) or a\n",
    "non-monotone onset. That changes urpssim's disease model, so it needs sign-off before\n",
    "editing dmdm_default_transitions() + the recurrence. Status LEFT uncalibrated (no artifact\n",
    "written) rather than stamping 'calibrated' on an old-age-overshooting fit."),
    100 * worst, 100 * old_overshoot))
  quit(status = 0)
}

tr$status <- "calibrated"; tr$calibration_status <- "calibrated"
dir.create("artifacts", showWarnings = FALSE)
saveRDS(tr, "artifacts/calibrated_dmdm_transitions.rds")
cat(sprintf("\nPASS (worst MAPE %.1f%%, 65+ overshoot %.1f%%). Wrote artifacts/calibrated_dmdm_transitions.rds (status=calibrated)\n",
            100 * worst, 100 * old_overshoot))
