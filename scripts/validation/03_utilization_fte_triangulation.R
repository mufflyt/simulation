#!/usr/bin/env Rscript
# Independent demand triangulation ----
#
#   Rscript scripts/validation/03_utilization_fte_triangulation.R
#
# ESTIMAND, stated so it cannot be confused with the other one:
#
#   PHYSICIAN FTE required for MODELLED UTILISATION under an ASSUMED
#   TEAM-DELEGATION PATTERN and EXTERNAL PHYSICIAN PRODUCTIVITY.
#
# All three dependencies are named on purpose. An earlier draft called this
# "utilisation-based FTE", which hid the fact that the delegation matrix is
# doing as much identification work as the productivity denominator.
#
# That is NOT "FTE required to meet population need". Utilisation is care
# actually sought under current access, insurance and referral conditions.
# Unmet need is excluded by construction, and the mystery-caller series (mean
# wait 23.1 -> 40.8 business days, 2020-2026) is direct evidence that current
# utilisation understates what people would seek if they could.
#
# WHY IT IS A TRIANGULATION AND NOT A VALIDATION. It shares no parameter with
# the adequacy anchor, so agreement would be convergent validity. It still
# cannot say whether observed utilisation is adequate care. HRSA's HWSM
# separates the same two steps -- estimate utilisation, then convert volume to
# FTE with an external productivity ratio -- for the same reason.
#
# THE ANCHOR IS NOT USED ANYWHERE HERE. The reference model computes
# required_fte(t) = anchor x wRVU(t)/wRVU(base), where anchor = observed supply
# / borrowed adequacy. At the base year that is 1306 / 0.948 = 1377.3 exactly:
# it is a statement about how many providers exist, not about how much care is
# used. This script divides utilisation by an EXTERNAL productivity benchmark
# instead, so the two estimates share no calibration.
#
# UNITS MUST MATCH. The numerator is annual work RVUs (encounter mix weighted by
# expected wRVU), so the denominator must be wRVU per clinical FTE -- never
# visits per FTE. A new consultation, a pessary check, urodynamics, a sling and
# a prolapse repair are not equivalent units of physician workload, which is
# why the wRVU route is preferred over a visit count for this specialty.

suppressPackageStartupMessages({
  if (!requireNamespace("urpssim", quietly = TRUE)) pkgload::load_all(".", quiet = TRUE) else library(urpssim)
})
source(file.path("scripts", "validation", "_provenance.R"))

YEARS <- 2025:2050
REPORT_YEARS <- c(2025, 2035, 2050)

# ---- PRESPECIFICATION, recorded before the AUGS/MGMA values were seen --------
#
# Before obtaining the AUGS/MGMA urogynecology productivity benchmarks, we
# prespecified the comparison between the external productivity distribution and
# the model-implied productivity calibration. The reference model implies
# 5,193.1 raw wRVU per clinical FTE before the common indirect-time adjustment.
# Because the utilisation-based and reference estimates use the same
# physician-attributed workload numerator, their FTE ratio is determined by the
# productivity ratio alone. We therefore define the primary comparison as the
# AUGS/MGMA median wRVU/FTE divided by 5,193.1, with a ratio of 1.00 indicating
# exact agreement. Ratios greater than 1.00 indicate that the reference model
# estimates more required FTE than the utilisation-based calculation, whereas
# ratios below 1.00 indicate the opposite. The AUGS/MGMA 25th and 75th
# percentile productivity values will define prespecified sensitivity estimates.
# These comparisons will be interpreted as evidence about the
# utilisation-to-FTE conversion and NOT as estimates of unmet need or workforce
# adequacy.
#
# No pass/fail threshold is declared. There is no substantive basis for calling
# some particular deviation "convergent validity", and a binary label would
# discard the direction and magnitude of disagreement, which are the
# informative parts.
MODEL_IMPLIED_RAW_WRVU_PER_FTE <- 5193.1

# ---- The productivity denominator -------------------------------------------
#
# A specialty-specific distribution is required, not a family proxy. AUGS/MGMA
# publish a urogynecology productivity report (2025 edition, 2024 data) giving
# work RVUs by percentile.
#
# THE VALUES ARE READ FROM A GITIGNORED FILE, NOT HARDCODED. They are licensed
# report content: committing them would republish purchased benchmark values in
# a public repository. The file is a declared input, hashed into the manifest
# and rechecked at completion, so reproducibility does not require the source
# report to be committed -- only that whoever reproduces the run holds the same
# licensed extract. This is the mirror image of the mapping tables, which ARE
# committed precisely because they need review and carry nothing licensed.
#
# Expected columns: source, edition, tier, p25, p50, p75
PRODUCTIVITY_FILE <- file.path("data-raw", "productivity", "augs_mgma_wrvu.csv")

# THE SOURCE REPORT IS HASHED TOO, and it is declared NOW rather than when the
# report arrives. The CSV is what the analysis consumes; the report hash
# establishes which licensed source the transcription came from, so a
# transcription error or a swapped edition is detectable after the fact.
#
# Declaring it now is not premature: hash_inputs() records a missing file as NA,
# and adding the declaration later would force a code edit at the moment the
# data arrives -- destroying the property that learning the answer cannot change
# the analysis. Any of the usual extensions is accepted; the first one present
# is hashed.
PRODUCTIVITY_REPORT <- {
  cand <- file.path("data-raw", "productivity",
                    paste0("augs_mgma_report", c(".pdf", ".xlsx", ".xls", ".csv")))
  present <- cand[file.exists(cand)]
  if (length(present)) present[1] else cand[1]
}

PRODUCTIVITY <- if (file.exists(PRODUCTIVITY_FILE)) {
  z <- utils::read.csv(PRODUCTIVITY_FILE, stringsAsFactors = FALSE)
  stopifnot(nrow(z) == 1L,
            all(c("source", "edition", "tier", "p25", "p50", "p75") %in% names(z)))
  list(source = paste0(z$source, " (", z$edition, ")"), tier = z$tier,
       p25 = z$p25, p50 = z$p50, p75 = z$p75)
} else {
  # Fallback keeps the script runnable and self-gating before the report lands.
  list(source = "WRVU_PER_FTE_BENCHMARK (OB/GYN-family proxy)",
       tier = "family_proxy",
       p25 = WRVU_PER_FTE_BENCHMARK[["low"]],
       p50 = WRVU_PER_FTE_BENCHMARK[["median"]],
       p75 = WRVU_PER_FTE_BENCHMARK[["high"]])
}

SPECIALTY_SPECIFIC <- identical(PRODUCTIVITY$tier, "specialty_specific")

RUN <- begin_validation_run(
  "utilization_fte_triangulation",
  params = list(years = "2025-2050", productivity_source = PRODUCTIVITY$source,
                productivity_tier = PRODUCTIVITY$tier,
                p25 = PRODUCTIVITY$p25, p50 = PRODUCTIVITY$p50, p75 = PRODUCTIVITY$p75,
                population_source = "census_npp_mid", units = "work RVUs / (wRVU per FTE)",
                prespecified_ratio_denominator = MODEL_IMPLIED_RAW_WRVU_PER_FTE),
  # A family-proxy denominator cannot produce a citable specialty estimate. The
  # run is forced exploratory rather than left to the reader's judgement.
  require_clean = SPECIALTY_SPECIFIC,
  exploratory  = !SPECIALTY_SPECIFIC,
  inputs = c(productivity_benchmark = PRODUCTIVITY_FILE,
             productivity_source_report = PRODUCTIVITY_REPORT))

if (!SPECIALTY_SPECIFIC) {
  message("\n*** GATED: productivity tier is '", PRODUCTIVITY$tier, "'. ",
          "A urogynecology-specific distribution (AUGS/MGMA) is required before ",
          "these FTE values can be cited. Running EXPLORATORY.\n")
}

# ---- Utilisation, from the REAL Census population ---------------------------
pop <- resolve_demand_population(YEARS)
message("population source: ", pop$source)
stopifnot(!identical(pop$source, "example"))

roster <- urps_provider_roster(load_urps_roster())
gap <- baseline_gap(
  mufflyaccess::urps_count(year = 2023L, measure = "board_certified_active",
                           geography = "national", include_urology = TRUE),
  capacity_survey_adequacy(example_capacity_survey())$adequacy,
  method = "capacity_survey", calibration_status = "derived_by_analogy",
  source = "Zarek 2025 PTJ", evidence = "reference model only; NOT used by this estimand")
ref <- suppressMessages(run_workforce_microsimulation(
  roster = roster, years = YEARS, subspecialty = "FPMRS", pop_by_band = pop$pop_by_band,
  baseline_gap_estimate = gap, n_iterations = 5, calibration = "namcs",
  supply_scenarios = supply_scenario_registry(70)[1], allow_analogy = TRUE, verbose = FALSE))

ind <- INDIRECT_TIME_SHARE
fte_at <- function(wrvu, p) wrvu / (p * (1 - ind))

rows <- lapply(YEARS, function(y) {
  sv <- ref$service_volumes[ref$service_volumes$year == y, c("service", "volume")]
  wr <- sum(service_volume_to_wrvu(sv)$work_rvu, na.rm = TRUE)
  refr <- ref$fte_gap$required_fte[ref$fte_gap$year == y][1]
  data.frame(year = y, service_volume = sum(sv$volume), work_rvu = wr,
             fte_p25 = fte_at(wr, PRODUCTIVITY$p25),
             fte_p50 = fte_at(wr, PRODUCTIVITY$p50),
             fte_p75 = fte_at(wr, PRODUCTIVITY$p75),
             reference_model_fte = refr,
             diff_vs_p50_pct = 100 * (refr - fte_at(wr, PRODUCTIVITY$p50)) /
                                     fte_at(wr, PRODUCTIVITY$p50))
})
tri <- do.call(rbind, rows)

cat("\n=== Independent demand triangulation ===\n")
print(tri[tri$year %in% REPORT_YEARS, ], row.names = FALSE, digits = 5)

# ---- Workload waterfall ------------------------------------------------------
#
# Where the physician-attributable numerator actually comes from. Naive
# volume x wRVU is 18.1M for 2025; the pipeline's numerator is 5.2M. The whole
# 3.47x contraction is the delegation matrix, and this table shows which
# services cause it rather than leaving it as an aggregate factor.
#
# NOTE ON THE MATRIX'S MEANING. `urps_share` is the URPS SUBSPECIALIST share.
# `other_clinician_share` is other PHYSICIANS (general gynaecology, urology) --
# not APPs. Reading it as an APP share makes a ~30% surgical share look absurd
# when the implied physician total is ~84%, which is close to the ~82%
# claims-attributed physician share observed in URPS-relevant episodes.
wf_year <- function(y) {
  sv <- ref$service_volumes[ref$service_volumes$year == y, c("service", "volume")]
  wl <- urps_service_workload()[, c("service", "work_rvu")]
  dm <- as.data.frame(URPS_DELEGATION_MATRIX)[, c("service", "urps_share",
                                                  "app_share", "other_clinician_share")]
  m <- merge(merge(sv, wl, by = "service"), dm, by = "service", all.x = TRUE)
  m$raw_wrvu <- m$volume * m$work_rvu
  m$physician_wrvu <- m$raw_wrvu * m$urps_share
  m$withheld <- m$raw_wrvu - m$physician_wrvu
  m$pct_of_withheld <- 100 * m$withheld / sum(m$withheld)
  m$implied_physician_total <- m$urps_share + m$other_clinician_share
  m <- m[order(-m$withheld), ]
  cbind(year = y, m[, c("service", "volume", "work_rvu", "raw_wrvu", "urps_share",
                        "app_share", "implied_physician_total", "physician_wrvu",
                        "pct_of_withheld")])
}
waterfall <- do.call(rbind, lapply(REPORT_YEARS, wf_year))
cat("\n=== Workload waterfall (base year) ===\n")
print(waterfall[waterfall$year == REPORT_YEARS[1], ], row.names = FALSE, digits = 5)
w0 <- waterfall[waterfall$year == REPORT_YEARS[1], ]
cat(sprintf("\nraw %s -> physician-attributable %s  (x%.4f)\n",
            format(round(sum(w0$raw_wrvu)), big.mark = ","),
            format(round(sum(w0$physician_wrvu)), big.mark = ","),
            sum(w0$physician_wrvu) / sum(w0$raw_wrvu)))

# ---- Identity check ----------------------------------------------------------
#
# THE TRAP THIS CLOSES. The narrative once quoted raw productivities (7500 vs
# 5193) against a table showing FTE computed at EFFECTIVE productivity
# (raw x (1 - indirect)). A reviewer dividing the printed wRVU by the printed
# FTE gets 5467 and 3786, not 7500 and 5193, and reasonably concludes the
# analysis is wrong. The (1 - indirect) factor cancels from the RATIO, so the
# conclusion held -- but only by luck of what was being compared.
#
# Every quantity is now printed on both scales, and the run FAILS if the ratio
# implied by the FTE columns disagrees with the ratio of the benchmarks.
identity <- do.call(rbind, lapply(REPORT_YEARS, function(y) {
  r <- tri[tri$year == y, ]
  eff_bench   <- PRODUCTIVITY$p50 * (1 - ind)
  eff_from_p50 <- r$work_rvu / r$fte_p50
  eff_ref     <- r$work_rvu / r$reference_model_fte
  data.frame(year = y,
             raw_benchmark_p50 = PRODUCTIVITY$p50,
             indirect_share = ind,
             effective_benchmark_p50 = eff_bench,
             effective_implied_by_fte_p50 = eff_from_p50,
             effective_implied_by_reference = eff_ref,
             raw_implied_by_reference = eff_ref / (1 - ind),
             ratio_from_fte_columns = eff_from_p50 / eff_ref,
             ratio_from_raw_benchmarks = PRODUCTIVITY$p50 / (eff_ref / (1 - ind)))
}))
cat("\n=== Identity check: raw vs effective productivity ===\n")
print(identity, row.names = FALSE, digits = 6)

tol <- 1e-8
bad <- abs(identity$ratio_from_fte_columns - identity$ratio_from_raw_benchmarks) > tol |
       abs(identity$effective_benchmark_p50 - identity$effective_implied_by_fte_p50) > 1e-6
if (any(bad)) {
  stop("identity check FAILED in year(s) ",
       paste(identity$year[bad], collapse = ", "),
       ": the FTE columns and the stated benchmarks imply different productivity ",
       "ratios. An undisclosed conversion is present.", call. = FALSE)
}
cat(sprintf("identity holds in all %d reported years (tolerance %g):\n", nrow(identity), tol))
cat("  effective = raw x (1 - indirect), and (1 - indirect) cancels from the ratio.\n")

# ---- Decomposition ----------------------------------------------------------
# The two estimates share a numerator (the same wRVU total) and differ only in
# the denominator, so the whole gap is productivity. Stating it as a ratio makes
# that unavoidable rather than something a reader has to infer.
dec <- do.call(rbind, lapply(REPORT_YEARS, function(y) {
  r <- tri[tri$year == y, ]
  implied <- r$work_rvu / (r$reference_model_fte * (1 - ind))
  data.frame(year = y, work_rvu = r$work_rvu,
             implied_productivity_reference = implied,
             benchmark_p50 = PRODUCTIVITY$p50,
             ratio_p50_over_implied = PRODUCTIVITY$p50 / implied,
             numerator_shared = TRUE)
}))
cat("\n=== Decomposition: the difference is entirely the denominator ===\n")
print(dec, row.names = FALSE, digits = 5)

# ---- Has the model drifted away from the prespecified denominator? ----------
#
# The prespecification targets 5,193.1 raw wRVU/FTE, the model-implied value on
# 2026-08-08. If upstream development changes that to, say, 5,400 while the
# header still names 5,193.1, the analysis must NOT silently compute against the
# new number: that would let model development conducted before the external
# answer was known quietly move the target.
#
# So the prespecified comparison is preserved unconditionally, and a second
# comparison against the current calibration is reported ONLY when the model has
# actually moved. The former tests the hypothesis frozen on 2026-08-08; the
# latter describes the current model. Authoritative status requires the two to
# coincide -- otherwise this is a new analysis version, not the frozen one.
current_implied <- unique(round(dec$implied_productivity_reference, 1))
drifted <- length(current_implied) != 1L ||
  abs(current_implied[1] - MODEL_IMPLIED_RAW_WRVU_PER_FTE) > 0.5

if (drifted && SPECIALTY_SPECIFIC) {
  stop(sprintf(paste(
    "prespecified estimand no longer matches the model state: the frozen",
    "comparison targets %.1f raw wRVU/FTE, the current model implies %s.",
    "Authoritative status is refused. Revalidate the new model, report the",
    "prespecified R against %.1f REGARDLESS, and treat any comparison against",
    "the new denominator as a new analysis version."),
    MODEL_IMPLIED_RAW_WRVU_PER_FTE, paste(current_implied, collapse = "/"),
    MODEL_IMPLIED_RAW_WRVU_PER_FTE), call. = FALSE)
}
if (drifted) {
  cat(sprintf(paste0("\n*** MODEL DRIFT: prespecified denominator %.1f, current %s.",
                     " The prespecified R is preserved; R_current is reported",
                     " alongside it.\n"),
              MODEL_IMPLIED_RAW_WRVU_PER_FTE, paste(current_implied, collapse = "/")))
}

# ---- The prespecified primary comparison ------------------------------------
R_ratio <- PRODUCTIVITY$p50 / MODEL_IMPLIED_RAW_WRVU_PER_FTE
prespec <- data.frame(
  productivity_source = PRODUCTIVITY$source, tier = PRODUCTIVITY$tier,
  external_p50 = PRODUCTIVITY$p50,
  model_implied_raw = MODEL_IMPLIED_RAW_WRVU_PER_FTE,
  R = R_ratio,
  direction = if (abs(R_ratio - 1) < 1e-9) "exact agreement"
              else if (R_ratio > 1) "reference model requires MORE FTE than utilisation-based"
              else "reference model requires FEWER FTE than utilisation-based",
  sensitivity_p25 = PRODUCTIVITY$p25 / MODEL_IMPLIED_RAW_WRVU_PER_FTE,
  sensitivity_p75 = PRODUCTIVITY$p75 / MODEL_IMPLIED_RAW_WRVU_PER_FTE,
  # Reported for transparency; equal to the prespecified denominator unless the
  # model has moved, in which case authoritative status is refused above.
  current_model_implied = current_implied[1],
  model_drifted = drifted,
  R_current = PRODUCTIVITY$p50 / current_implied[1])
cat("\n=== prespecified primary comparison (R = external p50 / model-implied) ===\n")
print(prespec, row.names = FALSE, digits = 5)
cat("Interpreted as evidence about the utilisation-to-FTE conversion only --\n")
cat("not unmet need, not workforce adequacy.\n")

complete_validation_run(RUN, tables = list(
  prespecified_comparison = prespec,
  triangulation = tri,
  productivity_decomposition = dec,
  workload_waterfall = waterfall,
  identity_check = identity))
