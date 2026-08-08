#!/usr/bin/env Rscript
# Per-condition validation of the DMDM against CORRECTED age-band prevalence targets
# INCLUDING the >=80 band (Wu 2014 PMC3970401; Nygaard 2008 PMC2918416). Diagnostic
# only -- writes nothing, changes no parameters. It answers: with remission FIXED at
# the longitudinally (SWAN) estimated / literature values (age-invariant, since SWAN
# does not support an age effect on 1->0), where does the ONSET structure fail?
#
# Key corrections over the earlier run:
#  * the package "75+" target (0.386) conflated ~70-79 with the >=80 SURGE; Wu/Nygaard
#    both RISE at >=80 (any-PFD 39.6->52.7 Wu; 36.8->49.7 Nygaard). No plateau to model.
#  * validate EACH condition, not the aggregate. Per Wu the older-age patterns differ:
#    UI rises, POP DECLINES, FI dips then rises -- non-monotone onset, not remission.
suppressPackageStartupMessages(pkgload::load_all(".", quiet = TRUE))
NS  <- asNamespace("urpssim"); onp <- get(".dmdm_onset_p", NS)

# Wu 2014 older-age decade targets (authoritative for 60+, where demand lives) + the
# package's younger bands for context. AI = anal incontinence, proxied by FI.
TARGET <- list(
  ui  = c("60-69" = 0.247, "70-79" = 0.297, "80+" = 0.382),
  pop = c("60-69" = 0.051, "70-79" = 0.043, "80+" = 0.040),   # DECLINES with age
  ai  = c("60-69" = 0.165, "70-79" = 0.143, "80+" = 0.210))   # dips then rises
BANDS <- list("60-69" = 60:69, "70-79" = 70:79, "80+" = 80:89)
AGES  <- 18:89
REP   <- list(vag = 2, ysl = 0, bmi = 27, hyst = 0, com = 0)

sim_prev_by_age <- function(a, rem) {
  meno  <- as.integer(AGES >= 51)
  onset <- onp(a, AGES, REP$vag, REP$ysl, REP$bmi, REP$hyst, meno, REP$com)
  p <- numeric(length(AGES))
  for (i in 2:length(AGES)) p[i] <- p[i - 1] * (1 - rem) + (1 - p[i - 1]) * onset[i - 1]
  setNames(p, AGES)
}
band_prev <- function(p) vapply(BANDS, function(ag) mean(p[as.character(ag)]), numeric(1))

tr <- readRDS("artifacts/swan_dmdm_transitions.rds")   # SWAN-fitted where available
cat(sprintf("remission (fixed, longitudinal/literature): ui=%.3f pop=%.3f ai=%.3f\n\n",
            tr$remission["ui"], tr$remission["pop"], tr$remission["ai"]))
for (cc in c("ui", "pop", "ai")) {
  sim <- band_prev(sim_prev_by_age(tr$onset[[cc]], tr$remission[cc]))
  tgt <- TARGET[[cc]]
  cat(sprintf("%s  aage=%+.3f  monotone-onset\n", toupper(cc), tr$onset[[cc]]["aage"]))
  cat(sprintf("   band     60-69   70-79    80+\n"))
  cat(sprintf("   sim   %7.3f %7.3f %7.3f\n", sim["60-69"], sim["70-79"], sim["80+"]))
  cat(sprintf("   Wu    %7.3f %7.3f %7.3f\n", tgt["60-69"], tgt["70-79"], tgt["80+"]))
  cat(sprintf("   err   %6.0f%% %6.0f%% %6.0f%%   (worst %.0f%%)\n\n",
              100*(sim["60-69"]-tgt["60-69"])/tgt["60-69"],
              100*(sim["70-79"]-tgt["70-79"])/tgt["70-79"],
              100*(sim["80+"]-tgt["80+"])/tgt["80+"],
              100*max(abs(sim-tgt)/tgt)))
}
cat("Reading: a monotone onset can track a rising condition (UI) but structurally cannot\n")
cat("reproduce POP's decline or FI's dip-then-rise -- an ONSET-shape problem, per condition,\n")
cat("to be fixed with a parsimonious non-linear age term (spline/piecewise), remission fixed.\n")
