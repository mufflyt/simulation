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
# This script addresses (1) only. It CANNOT address (2): the archive carries no
# provider identifier of any kind -- 127 distinct columns, zero NPI/UPIN/TIN,
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

ARCHIVE_DIR <- Sys.getenv("CADR_DIR", unset = file.path(
  "/Users/tylermuffly/Library/Caches/claude-code-tmp/claude-501",
  "-Users-tylermuffly-simulation/27db139d-2f96-4756-94bd-43329a05efe1/scratchpad/apd"))
SPEC_FILE <- file.path(ARCHIVE_DIR, "Provider specialty_23JAN2023.csv")

# Episode type -> modelled service. Burch is a stress-incontinence
# colposuspension (n = 68, 0.3%): reported, but NOT folded into sling, because
# silently merging a distinct procedure to boost coverage is how a mapping
# becomes untraceable.
EPISODE_TO_SERVICE <- c("UI Sling" = "sling_procedure", "Pessary" = "pessary_care")
UNMAPPED_EPISODES  <- c("PT", "Open burch", "Laparoscopic burch")

# Explicit classification of every code observed in the data. Anything absent
# from these vectors falls to "unclassified" rather than being assumed to be a
# physician -- an unrecognised code must not inflate the physician share.
APP_CODES   <- c(50L, 97L, 89L, 42L)              # NP, PA, CNS, certified nurse midwife
PT_CODES    <- c(65L)                             # physical therapist
OTHER_NONMD <- c(43L, 35L, 70L, 51L, 64L, 67L)    # CRNA, chiropractic, group practice, suppliers
PHYSICIAN_CODES <- c(1L,2L,4L,6L,7L,8L,9L,10L,11L,12L,13L,14L,16L,20L,24L,25L,26L,28L,29L,
                     30L,33L,34L,36L,37L,38L,39L,40L,44L,46L,48L,66L,77L,78L,79L,81L,82L,
                     83L,84L,85L,86L,90L,91L,92L,93L,94L,98L,99L)

classify <- function(code) {
  ifelse(is.na(code), "missing",
  ifelse(code %in% APP_CODES, "APP",
  ifelse(code %in% PT_CODES, "PT",
  ifelse(code %in% OTHER_NONMD, "other_nonphysician",
  ifelse(code %in% PHYSICIAN_CODES, "physician", "unclassified")))))
}

RUN <- begin_validation_run(
  "delegation_claims_evidence",
  params = list(source = "CADR_2023 Provider specialty_23JAN2023.csv",
                episodes_mapped = paste(names(EPISODE_TO_SERVICE), collapse = "/"),
                coverage_of_withheld_wrvu = "22.5%",
                interpretation = "UPPER BOUND on physician attribution (incident-to)",
                urps_vs_generalist = "NOT identifiable: no provider identifier in archive"),
  # No provider identifier and 2/11 service coverage: this cannot be a matrix
  # replacement, so it is not allowed to look like citable evidence.
  require_clean = FALSE, exploratory = TRUE)

stopifnot(file.exists(SPEC_FILE))
d <- as.data.table(data.table::fread(SPEC_FILE, showProgress = FALSE))
d[, provider := classify(as.integer(index_car_trt_prvdr_spclty1))]
d[, service := EPISODE_TO_SERVICE[episode_type]]

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
cat("\n=== early vs late (does 2008-2016 transport to 2025?) ===\n")
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

sv <- readRDS("/tmp/sv.rds")[readRDS("/tmp/sv.rds")$year == 2025, c("service", "volume")]
sv <- as.data.table(sv)[wl, on = "service", nomatch = 0][, raw := volume * work_rvu]
cmp <- cmp[sv[, .(service, raw)], on = "service", nomatch = 0]
cmp[, wrvu_effect_of_difference := raw * (claims_physician_share - model_physician_share)]
cat("\n=== model assumption vs claims, and what the gap is worth ===\n")
print(cmp[, .(service, model_physician_share, claims_physician_share, difference_pp,
              raw_wrvu = raw, wrvu_effect_of_difference)], digits = 4)

complete_validation_run(RUN, tables = list(
  provider_mix_by_service = by_svc,
  pooled_weighting        = pooled,
  trend_by_period         = trend,
  trend_by_year           = yearly,
  matrix_comparison       = cmp))
