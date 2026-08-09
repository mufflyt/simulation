#!/usr/bin/env Rscript
# URPS share among physician-delivered care: partial identification ----
#
#   Rscript scripts/validation/05_urps_share_partial_identification.R
#
# THE SPECIFICATION IS FROZEN IN docs/PRESPEC_URPS_SHARE.md (commit faf72dc),
# written before any roster-linked quantity was computed. Read it first. This
# script implements it and adds nothing to it. If a choice here is not in that
# document, it is a bug.
#
# WHY BOUNDS RATHER THAN A CORRECTED POINT ESTIMATE. CMS suppresses every
# NPI x HCPCS x POS cell serving fewer than 11 beneficiaries. That deletes the
# low-volume tail specifically, so 40-54% of national volume on the operative
# codes has no provider attached, and the providers it removes are
# disproportionately the ones doing a handful of cases a year. Rescaling the
# observed share by 1/capture would assume the suppressed volume looks like the
# retained volume, which is precisely the assumption the suppression mechanism
# violates. Instead the unidentified volume M is carried as unidentified, and
# the answer is an interval whose width is the honest cost of the suppression.
#
# THE PRIMARY DENOMINATOR COMES FROM A DIFFERENT FILE. T is the national total
# from the Geography release, which aggregates before suppression. Summing the
# provider file for a denominator would silently adopt the suppressed universe.

suppressPackageStartupMessages({
  if (!requireNamespace("urpssim", quietly = TRUE)) pkgload::load_all(".", quiet = TRUE) else library(urpssim)
  library(data.table)
})
source(file.path("scripts", "validation", "_provenance.R"))

PROV_SVC <- file.path("data-raw", "cms_psps", "PHY_R26_P05_V10_D24_Prov_Svc.csv")
GEO_SVC  <- file.path("data-raw", "cms_psps", "MUP_PHY_R26_P05_V10_D24_Geo.csv")
TYPE_MAP <- file.path("scripts", "validation", "mappings", "cms_provider_type_class.csv")
ROSTER   <- file.path("data-raw", "urps_roster", "urps_roster_2026-07-22.csv")

# ---- Code sets, per PRESPEC section 3 ---------------------------------------
#
# TIER A is the primary and every code in it is anatomically female-specific.
# TIER B adds the sex-neutral procedural codes and is secondary, because its
# denominator imports male and non-pelvic-floor utilisation.
# E/M IS ABSENT DELIBERATELY. 99203-99205 and 99212-99214 have national totals
# of 24.4M and 179.8M services -- all of Medicare outpatient E/M across all of
# medicine. The PUF carries no diagnosis, so those visits cannot be restricted
# to urogynaecologic care, and a share against that denominator would describe
# the size of the subspecialty rather than who delivers its care. It is not
# computed at all: an uninterpretable number invites misuse more than an absent
# one does. The cost -- 45.6% of physician work RVU left unidentified -- is
# stated in the output rather than hidden.
TIER_A <- data.table(
  service = c("pessary_care",
              rep("sling_procedure", 3),
              rep("prolapse_procedure", 9)),
  hcpcs   = c("57160",
              "57288", "51992", "57287",
              "57240", "57250", "57260", "57265", "57282", "57283", "57425",
              "57120", "57268"))
TIER_B_EXTRA <- data.table(
  service = c(rep("urodynamics", 5), "cystoscopy", "botox_bladder", "ptns",
              "bladder_instillation"),
  hcpcs   = c("51726", "51728", "51729", "51741", "51784",
              "52000", "52287", "64566", "51700"))
BASKET <- rbind(TIER_A, TIER_B_EXTRA)

RUN <- begin_validation_run(
  "urps_share_partial_identification",
  params = list(
    prespecification = "docs/PRESPEC_URPS_SHARE.md @ faf72dc",
    data_year = "2024 Medicare FFS Part B",
    primary = "Tier A, 13 female-pelvic-floor-specific codes",
    estimand = "share of physician-delivered services from roster-matched URPS physicians",
    identification = "partial: bounds L=U/(T-N), H=(U+M)/(T-N)",
    em_excluded = "yes -- no condition-specific denominator exists in the PUF",
    ascertainment = "roster 2024 ascertainment UNDOCUMENTED (see PRESPEC section 9)"),
  require_clean = TRUE, exploratory = FALSE,
  inputs = c(cms_prov_svc = PROV_SVC, cms_geo_svc = GEO_SVC,
             provider_type_map = TYPE_MAP))

stopifnot(file.exists(PROV_SVC), file.exists(GEO_SVC), file.exists(TYPE_MAP),
          file.exists(ROSTER))

# ---- Frozen roster, per PRESPEC section 9 -----------------------------------
#
# cert_year <= 2024 makes the roster contemporaneous with the data year: a
# physician certified in 2025 was not a URPS subspecialist while billing 2024
# services, and counting them would import a later state of the workforce into
# an earlier measurement.
ros <- fread(ROSTER, showProgress = FALSE)
# nzchar(NA) is TRUE, so an is.na() test is required as well -- without it the
# six NA-NPI rows survive the filter, enter roster_npi as NA, and are reported
# as zero blanks while quietly sitting in the numerator's key set.
ros[, npi := trimws(as.character(npi))]
roster_npi <- unique(ros[!is.na(npi) & nzchar(npi) &
                         !is.na(cert_year) & cert_year <= 2024L, npi])
roster_note <- data.frame(
  rows_in_file = nrow(ros),
  distinct_npi = uniqueN(ros$npi),
  blank_or_na_npi = sum(is.na(ros$npi) | !nzchar(ros$npi)),
  cert_year_le_2024 = length(roster_npi),
  provenance_sidecar_states = "1100 rows / 1092 unique NPIs -- DOES NOT DESCRIBE THIS FILE",
  ascertainment_2024 = "UNDOCUMENTED; do not quote a completeness figure")
cat("\n=== frozen roster ===\n"); print(t(roster_note))

# ---- T: national totals, from the Geography release -------------------------
geo <- fread(GEO_SVC, showProgress = FALSE)
nat <- geo[Rndrng_Prvdr_Geo_Lvl == "National" & HCPCS_Cd %in% BASKET$hcpcs]
missing_codes <- setdiff(BASKET$hcpcs, unique(nat$HCPCS_Cd))
if (length(missing_codes))
  stop("basket code(s) absent from the Geography national file: ",
       paste(missing_codes, collapse = ", "),
       ". A denominator cannot be assembled for them.", call. = FALSE)
Tn <- merge(BASKET, nat[, .(hcpcs = HCPCS_Cd, srvcs = Tot_Srvcs)], by = "hcpcs",
            allow.cartesian = TRUE)[, .(T_s = sum(srvcs)), by = service]

# ---- Provider file: U, O, N -------------------------------------------------
prov <- fread(PROV_SVC,
              select = c("Rndrng_NPI", "Rndrng_Prvdr_Type", "HCPCS_Cd",
                         "Place_Of_Srvc", "Tot_Benes", "Tot_Srvcs"),
              showProgress = FALSE)
prov <- prov[HCPCS_Cd %in% BASKET$hcpcs]
prov[, npi := trimws(as.character(Rndrng_NPI))]

# UNKNOWN PROVIDER TYPES STOP THE RUN. Routing one to a default would change
# the denominator while every provenance check passed. A CMS refresh that
# finally introduces an FPMRS type must be noticed, not absorbed.
# read.csv, not fread: the mapping carries a commented rationale header and
# fread has no comment.char, so it read the comment as data and reported every
# type as unmapped. Same reader as the other mappings in this directory.
tmap <- as.data.table(utils::read.csv(TYPE_MAP, stringsAsFactors = FALSE,
                                      comment.char = "#"))
unmapped <- setdiff(unique(prov$Rndrng_Prvdr_Type), tmap$cms_provider_type)
type_audit <- data.frame(observed_types = uniqueN(prov$Rndrng_Prvdr_Type),
                         unmapped = length(unmapped),
                         unmapped_types = paste(unmapped, collapse = "; "))
cat("\n=== provider-type audit ===\n"); print(type_audit, row.names = FALSE)
if (length(unmapped))
  stop("unmapped Rndrng_Prvdr_Type: ", paste(unmapped, collapse = "; "),
       ". Classify in ", TYPE_MAP, " -- a default would move the denominator ",
       "while the manifest looked complete.", call. = FALSE)
prov <- merge(prov, tmap, by.x = "Rndrng_Prvdr_Type", by.y = "cms_provider_type",
              all.x = TRUE)
prov <- merge(prov, BASKET, by.x = "HCPCS_Cd", by.y = "hcpcs", allow.cartesian = TRUE)

# ROSTER MEMBERSHIP WINS OVER CMS TYPE. A roster-matched NPI carrying a
# nonphysician CMS type is a data anomaly, not evidence the person is not a
# physician; the roster is the authority on the individual. Counted in U, and
# the count is reported so the anomaly is visible rather than absorbed.
prov[, on_roster := npi %chin% roster_npi]
anomaly <- prov[on_roster == TRUE & provider_class != "physician",
                .(npis = uniqueN(npi), srvcs = sum(Tot_Srvcs)), by = provider_class]
cat("\n=== roster-matched NPIs with a nonphysician CMS type ===\n")
if (nrow(anomaly)) print(anomaly, row.names = FALSE) else cat("  none\n")

prov[, bucket := fifelse(on_roster, "U",
                  fifelse(provider_class == "physician", "O", "N"))]
comp <- dcast(prov[, .(srvcs = sum(Tot_Srvcs)), by = .(service, bucket)],
              service ~ bucket, value.var = "srvcs", fill = 0)
for (b in c("U", "O", "N")) if (!b %in% names(comp)) comp[, (b) := 0]

# ---- M, the unidentified remainder, and the bounds --------------------------
x <- merge(Tn, comp, by = "service")
x[, M := T_s - U - O - N]
if (any(x$M < 0))
  stop("M < 0 for: ", paste(x$service[x$M < 0], collapse = ", "),
       ". The provider file exceeds the national total, which breaks the ",
       "identity T = U + O + N + M and invalidates every bound below.",
       call. = FALSE)

x[, `:=`(L = U / (T_s - N),
         H = (U + M) / (T_s - N),
         observed_cell = U / (U + O),
         capture = (U + O + N) / T_s)]
x[, tier := fifelse(service %in% TIER_A$service, "A (primary)", "B (secondary)")]
setorder(x, tier, -T_s)

cat("\n=== service-specific partial identification ===\n")
print(x[, .(tier, service, T_s = round(T_s), U = round(U), O = round(O),
            N = round(N), M = round(M),
            L_pct = round(100 * L, 1), H_pct = round(100 * H, 1),
            observed_cell_pct = round(100 * observed_cell, 1),
            capture_pct = round(100 * capture, 1))], row.names = FALSE)

# ---- wRVU-weighted aggregate ------------------------------------------------
#
# FROM SUMMED WORKLOADS, never by averaging the service-specific percentages:
# one sling is 12.29 work RVU and one pessary fitting 0.89, so an unweighted
# mean of the three percentages would treat them as equivalent.
wl <- as.data.table(urps_service_workload())[, .(service, w = work_rvu)]
x <- merge(x, wl, by = "service")
agg <- function(d, label) data.table(
  aggregate = label, services = nrow(d),
  L_pct = 100 * sum(d$U * d$w) / sum((d$T_s - d$N) * d$w),
  H_pct = 100 * sum((d$U + d$M) * d$w) / sum((d$T_s - d$N) * d$w),
  observed_cell_pct = 100 * sum(d$U * d$w) / sum((d$U + d$O) * d$w),
  capture_pct = 100 * sum((d$U + d$O + d$N) * d$w) / sum(d$T_s * d$w))
tierA <- x[service %in% TIER_A$service]
aggregates <- rbind(agg(tierA, "Tier A (primary, female-specific)"),
                    agg(x, "Tier B (secondary, + sex-neutral)"))
cat("\n=== wRVU-weighted bounds on P(URPS | physician-delivered) ===\n")
print(aggregates[, lapply(.SD, function(v) if (is.numeric(v)) round(v, 2) else v)],
      row.names = FALSE)

# ---- What the model currently assumes ---------------------------------------
#
# Comparison only. The matrix's urps_share is a share of TOTAL workload; the
# bounds above are conditional on physician delivery. Rendered comparable by
# dividing the matrix value by its own implied physician total.
dm <- as.data.table(URPS_DELEGATION_MATRIX)[
  service %in% x$service, .(service, urps_share, app_share, other_clinician_share)]
dm[, model_urps_given_phys := urps_share / (urps_share + other_clinician_share)]
cmp <- merge(x[, .(service, tier, L, H)], dm[, .(service, model_urps_given_phys)],
             by = "service")
cmp[, verdict := fifelse(model_urps_given_phys < L, "BELOW the lower bound",
                  fifelse(model_urps_given_phys > H, "ABOVE the upper bound",
                          "inside the identified interval"))]
setorder(cmp, tier, service)
cat("\n=== model assumption against the identified interval ===\n")
print(cmp[, .(tier, service, L_pct = round(100 * L, 1), H_pct = round(100 * H, 1),
              model_pct = round(100 * model_urps_given_phys, 1), verdict)],
      row.names = FALSE)

cat("\nSCOPE. Medicare FFS Part B 2024 only; sling and prolapse patients skew\n")
cat("younger than the Medicare population, so this is not the national mix.\n")
cat("Unmatched NPIs are NON-ROSTER PHYSICIANS, not generalists -- non-match is\n")
cat("equally consistent with a roster miss, and roster ascertainment for 2024\n")
cat("is undocumented. The E/M component, 45.6% of physician work RVU, is not\n")
cat("identified by this analysis.\n")

complete_validation_run(RUN, tables = list(
  roster_definition   = roster_note,
  provider_type_audit = type_audit,
  roster_type_anomaly = if (nrow(anomaly)) as.data.frame(anomaly) else
                          data.frame(provider_class = character(), npis = integer(),
                                     srvcs = numeric()),
  service_bounds      = as.data.frame(x),
  wrvu_aggregates     = as.data.frame(aggregates),
  model_comparison    = as.data.frame(cmp)))
