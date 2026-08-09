#!/usr/bin/env Rscript
# Claims-attributed provider mix for URPS-relevant episodes ----
#
#   Rscript scripts/validation/04_delegation_claims_evidence.R
#
# WHAT THIS ESTIMATES, and what it deliberately does not.
#
# The delegation matrix answers two different questions at once:
#   (1) what share of workload is physician vs APP / PT / other clinician?
#   (2) among PHYSICIAN workload, what share is URPS rather than general
#       gynaecology or urology?
#
# This script addresses (1) only, and that is a SCOPE statement, not a defect.
# It CANNOT address (2): the archive carries no provider identifier of any kind -- 127 distinct columns, zero NPI/UPIN/TIN,
# and the data dictionary's only identifier entry is "Unique person
# identifier" (the patient). CMS specialty code 16 pools FPMRS with general
# OB/GYN, so the URPS/generalist split is not observable here at all.
#
# COVERAGE IS NARROW AND MUST TRAVEL WITH THE ESTIMATE. CADR_2023's episode
# types map to 2 of the 11 modelled services -- sling_procedure and
# pessary_care -- which together are 22.5% of the wRVU withheld by delegation.
# The uncovered 77.5% includes return_visit (26.9%) and new_consultation
# (19.3%), i.e. exactly the office-based services where the physician/APP
# question is most open and incident-to billing most distorting. So the
# strongest evidence here lands where the assumption is least contested.
#
# INCIDENT-TO MAKES THIS AN UPPER BOUND. An APP service billed under the
# supervising physician's NPI is recorded as physician-rendered. The
# claims-attributed physician share is therefore a CEILING on hands-on
# physician delivery, tightest for surgery and loosest for office visits.
# Intended use is the upper arm of a delegation sensitivity surface:
#   lower     = team-care assumption
#   reference = best-supported service-specific estimate
#   upper     = claims-attributed rendering share (this script)
#
# EPISODE- VS wRVU-WEIGHTING. Within a service the two are identical (every
# episode carries the same wRVU). They diverge when POOLING across services,
# because one sling (12.29 wRVU) is not one pessary fitting (0.89 wRVU). The
# wRVU-weighted figure is the one that belongs in an FTE sensitivity analysis;
# both are reported so the difference is visible rather than asserted.

suppressPackageStartupMessages({
  if (!requireNamespace("urpssim", quietly = TRUE)) pkgload::load_all(".", quiet = TRUE) else library(urpssim)
  library(data.table)
})
source(file.path("scripts", "validation", "_provenance.R"))

# A STABLE PATH, not a session scratchpad. The extract previously lived in a
# per-session cache directory that can be cleared between runs -- a run identity
# pointing at a volatile path is not an identity. data-raw/ is gitignored and
# already holds the NPI-bearing roster, so it is the consistent home for a
# claims extract that must not be committed.
ARCHIVE_DIR <- Sys.getenv("CADR_DIR", unset = file.path("data-raw", "cadr"))
SPEC_FILE <- file.path(ARCHIVE_DIR, "Provider specialty_23JAN2023.csv")
DICTIONARY <- file.path(ARCHIVE_DIR, "Lowder AUGS data dictionary 1.30.23.xlsx")

# MAPPINGS ARE EXTERNAL, VERSION CONTROLLED AND HASHED. They express scientific
# choices -- is a certified nurse midwife an APP? does a Burch colposuspension
# map to sling? -- and a choice buried in code is a choice nobody reviews. They
# carry no sensitive data, so unlike the claims extract they are committed.
SPECIALTY_MAP <- file.path("scripts", "validation", "mappings", "cms_specialty_class.csv")
EPISODE_MAP   <- file.path("scripts", "validation", "mappings", "cadr_episode_service.csv")

RUN <- begin_validation_run(
  "delegation_claims_evidence",
  params = list(source = "CADR_2023 Provider specialty_23JAN2023.csv",
                episodes_mapped = "UI Sling -> sling_procedure; Pessary -> pessary_care",
                coverage_of_withheld_wrvu = "22.5%",
                interpretation = "UPPER BOUND on physician attribution (incident-to)",
                urps_vs_generalist = "NOT identifiable: no provider identifier in archive"),
  # PROMOTED TO AUTHORITATIVE. This was exploratory while its provenance was not
  # reproducible: the source lived in a session scratchpad, the specialty and
  # episode mappings were constants inside this script, an undeclared /tmp
  # intermediate fed the matrix comparison, and the derivation had never been
  # through A/B. All four are fixed, and an exploratory A/B pair
  # (20260808T192755 / 20260808T192802) reproduced all six tables at zero
  # tolerance, which is the evidence that promotion is warranted.
  #
  # SCOPE IS UNCHANGED BY PROMOTION. This remains claims-attributed provider mix
  # for sling and pessary episodes, 2008-2016. It does not establish
  # URPS-versus-generalist attribution, actual hands-on APP delivery for
  # incident-to-billable services, or the delegation matrix as a whole. Being
  # narrower than an adjacent question was never what made it exploratory.
  #
  # The FTE triangulation in 03 stays non-citable for a different reason: its
  # parameters are unresolved, not its provenance.
  require_clean = TRUE, exploratory = FALSE,
  # The dictionary defines the episode types this script maps to modelled
  # services. It is an ANALYTIC input: change it and the crosswalk's meaning
  # changes without the R script changing, so it belongs in the run identity.
  inputs = c(cadr_provider_specialty = SPEC_FILE, cadr_data_dictionary = DICTIONARY,
             specialty_class_map = SPECIALTY_MAP, episode_service_map = EPISODE_MAP))

stopifnot(file.exists(SPEC_FILE), file.exists(SPECIALTY_MAP), file.exists(EPISODE_MAP))
smap <- utils::read.csv(SPECIALTY_MAP, stringsAsFactors = FALSE)
emap <- utils::read.csv(EPISODE_MAP, stringsAsFactors = FALSE)
d <- as.data.table(data.table::fread(SPEC_FILE, showProgress = FALSE))

# ---- Unmapped-code audit: unknown codes FAIL, never swept into "other" -------
#
# A CADR refresh could introduce a specialty code this mapping has never seen.
# Routing it silently to "other" would change the scientific meaning while every
# provenance check passed -- the manifest would be perfectly accurate about
# inputs that were being misclassified. Unknown codes stop the run and are named.
trt_cols <- grep("^index_car_trt_prvdr_spclty", names(d), value = TRUE)
seen <- sort(unique(stats::na.omit(as.integer(unlist(d[, ..trt_cols])))))
unmapped <- setdiff(seen, smap$cms_specialty_code)
code_audit <- data.frame(observed_codes = length(seen),
                         mapped = length(seen) - length(unmapped),
                         unmapped = length(unmapped),
                         unmapped_codes = paste(unmapped, collapse = ", "))
cat("\n=== unmapped-code audit ===\n"); print(code_audit, row.names = FALSE)
if (length(unmapped)) {
  stop("unmapped CMS specialty code(s): ", paste(unmapped, collapse = ", "),
       ". Classify them in ", SPECIALTY_MAP, " -- letting them fall to 'other' ",
       "would change the provider mix while every provenance check passed.",
       call. = FALSE)
}
unmapped_ep <- setdiff(unique(d$episode_type), emap$episode_type)
if (length(unmapped_ep))
  stop("unmapped episode type(s): ", paste(unmapped_ep, collapse = ", "),
       ". Declare them in ", EPISODE_MAP, call. = FALSE)

cls <- stats::setNames(smap$provider_class, as.character(smap$cms_specialty_code))
c1 <- as.integer(d$index_car_trt_prvdr_spclty1)
d[, provider := ifelse(is.na(c1), "missing", unname(cls[as.character(c1)]))]
svc <- stats::setNames(emap$modelled_service, emap$episode_type)
d[, service := unname(svc[episode_type])]

cat("\n=== episode coverage ===\n")
print(d[, .N, by = .(episode_type, mapped_service = fifelse(is.na(service), "-- not modelled --", service))][order(-N)])

wl <- as.data.table(urps_service_workload())[, .(service, work_rvu)]
mapped <- d[!is.na(service)][wl, on = "service", nomatch = 0]

# ---- Service-specific shares (episode- and wRVU-weighted are identical here) --
by_svc <- mapped[, .(episodes = .N), by = .(service, provider)]
by_svc[, share_pct := 100 * episodes / sum(episodes), by = service]
cat("\n=== claims-attributed provider mix by service (all years) ===\n")
print(dcast(by_svc, service ~ provider, value.var = "share_pct", fill = 0), digits = 4)

# ---- Pooled: episode- vs wRVU-weighted --------------------------------------
pooled <- mapped[, .(episodes = .N, wrvu = sum(work_rvu)), by = provider]
pooled[, `:=`(episode_weighted_pct = 100 * episodes / sum(episodes),
              wrvu_weighted_pct    = 100 * wrvu / sum(wrvu))]
cat("\n=== pooled across the two covered services ===\n")
print(pooled[order(-wrvu_weighted_pct), .(provider, episodes, episode_weighted_pct, wrvu_weighted_pct)],
      digits = 4)
cat("\nThe two columns differ because a sling carries 12.29 wRVU and a pessary\n")
cat("fitting 0.89: episode weighting silently up-weights the cheaper service.\n")

# ---- Trend -------------------------------------------------------------------
mapped[, period := fifelse(year_episode_start <= 2011, "2008-2011", "2012-2016")]
trend <- mapped[, .(physician_pct = 100 * sum(provider == "physician") / .N,
                    APP_pct = 100 * sum(provider == "APP") / .N, n = .N),
                by = .(service, period)][order(service, period)]
# A flat series here means no material temporal trend in CLAIMS-ATTRIBUTED
# physician share. It does not establish that actual APP delivery was stable:
# rising incident-to billing would present as exactly this flatness.
cat("\n=== early vs late: claims-attributed physician share ===\n")
print(trend, digits = 4)
yearly <- mapped[, .(physician_pct = 100 * sum(provider == "physician") / .N, n = .N),
                 by = .(service, year_episode_start)][order(service, year_episode_start)]

# ---- Comparison with the current matrix --------------------------------------
dm <- as.data.table(URPS_DELEGATION_MATRIX)[, .(service, urps_share, app_share,
                                                other_clinician_share)]
dm[, model_physician_share := urps_share + other_clinician_share]
obs <- dcast(by_svc, service ~ provider, value.var = "share_pct", fill = 0)
cmp <- dm[obs, on = "service", nomatch = 0]
cmp[, claims_physician_share := physician / 100]
cmp[, difference_pp := 100 * (claims_physician_share - model_physician_share)]

# SERVICE VOLUMES ARE REGENERATED, NOT READ FROM A TEMP FILE. An earlier version
# read /tmp/sv.rds -- a leftover from an ad-hoc exploratory run. That file sat
# outside the run identity, was never hashed, could vanish between runs, and was
# produced by a script that no longer exists. Any A/B comparison against it would
# have been meaningless: the inputs could change without the manifest noticing,
# which is precisely the failure the input-hashing exists to prevent. Volumes now
# come from the pinned model and the Census population, both already covered by
# model_sha and the contract.
pop <- resolve_demand_population(2025L)
vol_run <- suppressMessages(run_workforce_microsimulation(
  roster = urps_provider_roster(load_urps_roster()), years = 2025:2026,
  subspecialty = "FPMRS", pop_by_band = pop$pop_by_band,
  baseline_gap_estimate = baseline_gap(
    1306, capacity_survey_adequacy(example_capacity_survey())$adequacy,
    method = "capacity_survey", calibration_status = "derived_by_analogy",
    source = "Zarek 2025 PTJ", evidence = "volume generation only"),
  n_iterations = 2, calibration = "namcs",
  supply_scenarios = supply_scenario_registry(70)[1],
  allow_analogy = TRUE, verbose = FALSE))
sv <- vol_run$service_volumes
sv <- as.data.table(sv[sv$year == 2025, c("service", "volume")])[wl, on = "service", nomatch = 0][
  , raw := volume * work_rvu]
cmp <- cmp[sv[, .(service, raw)], on = "service", nomatch = 0]
cmp[, wrvu_effect_of_difference := raw * (claims_physician_share - model_physician_share)]
cat("\n=== model assumption vs claims, and what the gap is worth ===\n")
print(cmp[, .(service, model_physician_share, claims_physician_share, difference_pp,
              raw_wrvu = raw, wrvu_effect_of_difference)], digits = 4)

complete_validation_run(RUN, tables = list(
  unmapped_code_audit     = code_audit,
  provider_mix_by_service = by_svc,
  pooled_weighting        = pooled,
  trend_by_period         = trend,
  trend_by_year           = yearly,
  matrix_comparison       = cmp))
