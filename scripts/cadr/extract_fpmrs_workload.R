#!/usr/bin/env Rscript
# =============================================================================
# extract_fpmrs_workload.R
# -----------------------------------------------------------------------------
# Track B of the urogynecology workforce-DEMAND model.
#
# ESTIMAND: FPMRS-attributable CLINICAL WORKLOAD generated AFTER a patient
# enters treatment, per TREATED patient, by treatment pathway. This is NOT
# total episode cost, NOT total claims, NOT facility cost. We isolate the
# professional (physician) work that a female-pelvic-medicine practice performs
# downstream of the index treatment decision.
#
# DATA: Lowder/AUGS "CADR" Medicare claims cohort (SUI/POP treatment episodes,
# age >=65, national, index years 2008-2016). Delivered as a 1.1 GB zip.
#   - Base "CADR/"       : VISIT-LEVEL long files (one row per E&M visit,
#                          per subsequent PT/pessary encounter, per procedure,
#                          per complication visit) -> supports true COUNTS.
#                          Covers all 5 pathways (PT, pessary, sling, open
#                          Burch, lap Burch), index years 2008-2016.
#   - "CADR_2023/"       : a re-analysis. Its analytic file march_2/data.csv is
#                          per-patient COST-AGGREGATED (Medicare allowed $, 2016
#                          dollars) into named service buckets; covers 3 pathways
#                          (PT/pessary/sling), index years 2008-2015; Burch
#                          dropped. Used here as a COST companion, not for counts
#                          (it has no visit counts, no CPT, no specialty column).
#
# ATTRIBUTION TIERS (documented per component in the outputs):
#   Tier 1 (provider specialty + service): available ONLY for the INDEX
#           treatment and the pre-index period (Provider specialty index.csv:
#           index_car_trt_prvdr_spclty / index_car_EM_prvdr_spclty, CMS codes).
#           FPMRS has NO distinct CMS specialty code in 2008-2016; urogynecology
#           is billed as OB/GYN(16) or Urology(34), so "FPMRS-delivered" index
#           treatment is proxied by specialty in {16,34}.
#   Tier 2 (CPT/service-type attribution, specialty unavailable): all POST-index
#           workload. The cost buckets/visit files are pre-labeled by service
#           type (E&M new/return/consult by level; cystoscopy; UDS; cystometrics;
#           uroflowmetry; PVR; pessary; sling; sling-repair), which map 1:1 to CPT
#           groups -> we attribute by that service type. No per-claim specialty.
#   Tier 3 (derived by analogy): the CPT->wRVU crosswalk values (representative
#           2025 CMS work RVUs per level/procedure) and any cost->count fallback.
#
# NEVER MERGED: FPMRS professional | PT | anesthesia | facility | ED |
#               inpatient non-FPMRS | other specialists. Facility/total cost is
#               NEVER converted into FPMRS workload.
#
# OUTPUTS (aggregate only, no patient rows): scripts/cadr/outputs/*.csv
# =============================================================================

suppressWarnings(suppressMessages({
  library(data.table)
  library(digest)
}))

set.seed(20260808L)

# ---- paths ------------------------------------------------------------------
ZIP <- Sys.getenv("CADR_ZIP",
  unset = "/Users/tylermuffly/Documents/alternative_payments_dropbox_Data.zip")
OUT <- file.path(dirname(sub("--file=", "",
  grep("--file=", commandArgs(FALSE), value = TRUE)[1])), "outputs")
if (length(OUT) == 0 || is.na(OUT) || !nzchar(OUT))
  OUT <- "/Users/tylermuffly/simulation/scripts/cadr/outputs"
dir.create(OUT, showWarnings = FALSE, recursive = TRUE)
TMP <- tempfile("cadr_extract_"); dir.create(TMP)
on.exit(unlink(TMP, recursive = TRUE), add = TRUE)

stopifnot(file.exists(ZIP))

msg <- function(...) cat(sprintf(...), "\n")

# ---- helpers ----------------------------------------------------------------
# Extract one zip member to TMP and return its path.
extract_member <- function(member) {
  dest <- file.path(TMP, gsub("[/ ]", "_", basename(member)))
  ok <- system2("unzip", c("-p", shQuote(ZIP), shQuote(member)),
                stdout = dest, stderr = FALSE)
  if (!file.exists(dest) || file.info(dest)$size == 0)
    stop("Failed to extract: ", member)
  dest
}
# numeric coercion that treats base-file "." and file "NA" as missing
num <- function(x) {
  if (is.numeric(x)) return(x)
  x <- as.character(x)
  x[x %in% c(".", "NA", "", "N/A")] <- NA
  suppressWarnings(as.numeric(x))
}
sha256 <- function(path) digest::digest(file = path, algo = "sha256")

# ---- CPT -> wRVU crosswalk (Tier 3; representative 2025 CMS work RVUs) -------
# E&M by level (new 99202-99205; return 99211-99215; office consult 99242-99245;
# level-1 legacy codes 99201/99241 approximated). Chosen to align with the cliff
# RVU25A vintage already used in urps_service_workload_rvu.csv.
WRVU_EM <- c(
  new_1 = 0.48, new_2 = 0.93, new_3 = 1.60, new_4 = 2.60, new_5 = 3.50,
  return_1 = 0.18, return_2 = 0.70, return_3 = 1.30, return_4 = 1.92, return_5 = 2.80,
  consult_1 = 0.64, consult_2 = 1.34, consult_3 = 1.88, consult_4 = 3.02, consult_5 = 3.77
)
# Post-index office procedures (professional work RVU per encounter).
WRVU_PROC <- c(
  cystoscopy   = 1.53,  # 52000
  UDS          = 1.89,  # complex cystometrogram w/ voiding pressure 51728/51729
  vUDS         = 2.00,  # video UDS
  cystometrics = 1.51,  # simple CMG 51725/51726
  uroflowmetry = 0.40,  # 51736/51741
  cathPVR      = 0.50,  # 51701 straight cath PVR
  usPVR        = 0.00,  # 51798 US bladder residual: technical-only, 0 work RVU
  UTeval       = 0.30
)
# Index treatment wRVU (Tier 1/2). Sling & pessary aligned to cliff.
WRVU_INDEX <- c(
  `UI Sling`           = 12.2864,  # 57288/51992 mix (cliff sling_procedure)
  Pessary              = 0.89,     # 57160 pessary fitting (cliff pessary_care)
  `Open burch`         = 12.44,    # 51840 open retropubic urethropexy
  `Laparoscopic burch` = 11.65,    # 51990 lap urethral suspension
  PT                   = NA        # index PT delivered by a physical therapist
)                                  # -> PT bucket, NOT FPMRS professional wRVU

PATHWAYS <- c("UI Sling", "Pessary", "PT", "Open burch", "Laparoscopic burch")

# =============================================================================
# 0. PROVENANCE
# =============================================================================
msg("== extracting members & recording SHA-256 provenance ==")
members <- c(
  march2_2023   = "Data/CADR_2023/march_2/data.csv",
  procedures2023= "Data/CADR_2023/_Procedures.csv",
  sling_index   = "Data/CADR/Sling index.csv",
  pessary_index = "Data/CADR/Pessary index.csv",
  pt_index      = "Data/CADR/PT index.csv",
  openburch_idx = "Data/CADR/Open burch index.csv",
  lapburch_idx  = "Data/CADR/Laparoscopic burch index.csv",
  em_post       = "Data/CADR/E-M visits post-index.csv",
  em_pre        = "Data/CADR/E-M visits pre-index.csv",
  otherproc_post= "Data/CADR/Other procedures post-index.csv",
  subs_pt       = "Data/CADR/Subsequent PT.csv",
  subs_pessary  = "Data/CADR/Subsequent pessary.csv",
  compl_surgery = "Data/CADR/Complications - surgery.csv",
  compl_pessPT  = "Data/CADR/Complications - pessary and PT.csv",
  prov_spec     = "Data/CADR/Provider specialty index.csv",
  cohort_desc   = "Data/CADR/Cohort descriptors.csv"
)
paths <- vapply(members, extract_member, character(1))
prov <- data.table(
  logical_name = names(members),
  zip_member   = unname(members),
  sha256       = vapply(paths, sha256, character(1)),
  bytes        = file.info(paths)$size
)
prov <- rbind(
  data.table(logical_name = "ZIP_ARCHIVE", zip_member = ZIP,
             sha256 = sha256(ZIP), bytes = file.info(ZIP)$size),
  prov
)
fwrite(prov, file.path(OUT, "input_provenance_sha256.csv"))
msg("  provenance -> input_provenance_sha256.csv (%d files)", nrow(prov))

# =============================================================================
# 1. VINTAGE DETERMINATION
# =============================================================================
msg("== vintage determination ==")
cohort   <- fread(paths["cohort_desc"])
march2   <- fread(paths["march2_2023"])

# Base cohort: index-year range from cohort descriptors
base_years <- range(num(cohort$year_episode_start), na.rm = TRUE)
# 2023 analytic: index-year range from march_2 (dates like 2013-01-01)
m2_year <- as.integer(substr(as.character(march2$year_episode_start), 1, 4))
m2_years <- range(m2_year, na.rm = TRUE)
m2_paths <- sort(unique(march2$sui_treatment))

vintage <- data.table(
  dataset = c("CADR (base, visit-level)", "CADR_2023 (march_2 analytic)"),
  index_year_min = c(base_years[1], m2_years[1]),
  index_year_max = c(base_years[2], m2_years[2]),
  n_episodes = c(uniqueN(paste(cohort$WU_ID, cohort$episode_number)),
                 uniqueN(march2$WU_ID)),
  pathways = c("PT; Pessary; UI Sling; Open burch; Laparoscopic burch",
               paste(m2_paths, collapse = "; ")),
  granularity = c("visit/claim-level long (COUNTS derivable)",
                  "per-patient cost-aggregated buckets (COSTS only)"),
  role = c("PRIMARY utilization vintage (counts + wRVU)",
           "COST companion (2023 re-analysis, 3 pathways)")
)
fwrite(vintage, file.path(OUT, "vintage_decision.csv"))
print(vintage)

# =============================================================================
# 2. COHORT (denominators) per pathway from index files
# =============================================================================
read_idx <- function(p, label) {
  d <- fread(p)
  d[, .(WU_ID, episode_number,
        year = num(year_episode_start),
        pathway = label,
        idx_sling_repair = if ("index_sling_repair" %in% names(d))
          num(index_sling_repair) else NA_real_,
        idx_mesh = if ("index_mesh" %in% names(d)) num(index_mesh) else NA_real_,
        idx_fistula = if ("index_fistula" %in% names(d)) num(index_fistula) else NA_real_,
        idx_STP = if ("index_STP" %in% names(d)) num(index_STP) else NA_real_,
        idx_IandD = if ("index_I_and_D" %in% names(d)) num(index_I_and_D) else NA_real_)]
}
cohort_paths <- rbindlist(list(
  read_idx(paths["sling_index"],   "UI Sling"),
  read_idx(paths["pessary_index"], "Pessary"),
  read_idx(paths["pt_index"],      "PT"),
  read_idx(paths["openburch_idx"], "Open burch"),
  read_idx(paths["lapburch_idx"],  "Laparoscopic burch")
), fill = TRUE)
cohort_paths[, key := paste(WU_ID, episode_number, sep = "_")]
setkey(cohort_paths, key)
msg("  cohort denominators: %s",
    paste(sprintf("%s=%d", names(table(cohort_paths$pathway)),
                  table(cohort_paths$pathway)), collapse = ", "))

# =============================================================================
# 3. POST-INDEX E&M visits (Tier 2) -> counts + wRVU, by level
# =============================================================================
# Each row = one E&M visit; exactly one *_lvlN_cost bucket is populated.
classify_em <- function(p) {
  d <- fread(p)
  lvlcols <- grep("_(new|return|consult)_lvl[1-5]_cost$", names(d), value = TRUE)
  # melt facility + professional level cols; keep first populated per row
  d[, .row := .I]
  m <- melt(d, id.vars = c("WU_ID", "episode_number", ".row"),
            measure.vars = lvlcols, variable.name = "bucket", value.name = "v")
  m[, v := num(v)]
  m <- m[!is.na(v) & v != 0]
  # bucket like OP_facility_new_lvl3_cost or professional_return_lvl2_cost
  m[, cls := sub(".*_(new|return|consult)_lvl[1-5]_cost$", "\\1", bucket)]
  m[, lvl := as.integer(sub(".*_lvl([1-5])_cost$", "\\1", bucket))]
  # one visit may appear as both facility+professional row for same level ->
  # collapse to one visit per (.row, cls, lvl)
  v <- unique(m[, .(WU_ID, episode_number, .row, cls, lvl)])
  v[, emkey := paste0(cls, "_", lvl)]
  v[, wrvu := WRVU_EM[emkey]]
  v[, key := paste(WU_ID, episode_number, sep = "_")]
  v
}
em_post <- classify_em(paths["em_post"])
em_pre  <- classify_em(paths["em_pre"])

em_post_agg <- em_post[, .(
  post_em_visits = .N,
  post_em_new     = sum(cls == "new"),
  post_em_return  = sum(cls == "return"),
  post_em_consult = sum(cls == "consult"),
  post_em_wrvu    = sum(wrvu, na.rm = TRUE)
), by = key]
em_pre_agg <- em_pre[, .(
  pre_em_visits = .N,
  pre_em_neweval = sum(cls %in% c("new", "consult")),  # initial specialist eval
  pre_em_wrvu    = sum(wrvu, na.rm = TRUE)
), by = key]

# =============================================================================
# 4. POST-INDEX office procedures (cystoscopy/UDS/PVR...) (Tier 2)
# =============================================================================
op <- fread(paths["otherproc_post"])
proccols <- grep("^professional_.*_cost$", names(op), value = TRUE)
op[, .row := .I]
mo <- melt(op, id.vars = c("WU_ID", "episode_number", ".row"),
           measure.vars = proccols, variable.name = "bucket", value.name = "v")
mo[, v := num(v)]
mo <- mo[!is.na(v) & v > 0]
mo[, proc := sub("^professional_(.*)_cost$", "\\1", bucket)]
mo[, wrvu := WRVU_PROC[proc]]
mo[, key := paste(WU_ID, episode_number, sep = "_")]
proc_agg <- mo[, .(
  post_procedures = .N,
  post_cystoscopy = sum(proc == "cystoscopy"),
  post_uds        = sum(proc %in% c("UDS", "vUDS", "cystometrics", "uroflowmetry")),
  post_pvr        = sum(proc %in% c("cathPVR", "usPVR")),
  post_proc_wrvu  = sum(wrvu, na.rm = TRUE)
), by = key]

# =============================================================================
# 5. Subsequent PT (PT bucket) & subsequent pessary (FPMRS) (Tier 2)
# =============================================================================
spt <- fread(paths["subs_pt"])
spt[, key := paste(WU_ID, episode_number, sep = "_")]
spt_agg <- spt[, .(subsequent_PT_sessions = .N), by = key]   # PT provider bucket

spe <- fread(paths["subs_pessary"])
spe[, key := paste(WU_ID, episode_number, sep = "_")]
spe_agg <- spe[, .(subsequent_pessary_visits = .N,
                   subsequent_pessary_wrvu = .N * WRVU_INDEX[["Pessary"]]), by = key]

# =============================================================================
# 6. Complications requiring FPMRS management (Tier 2)
# =============================================================================
# surgical complications: one row per complication visit; professional cost =
# FPMRS/surgeon management. Flags identify FPMRS-managed reoperative events.
cs <- fread(paths["compl_surgery"])
cs[, key := paste(WU_ID, episode_number, sep = "_")]
fpmrs_reop_flags <- intersect(
  c("sling_repair", "mesh", "mesh_1yr", "fistula", "I_and_D", "STP",
    "foreign_body_left", "bladder_proc", "ureter_proc", "vaginal_proc"),
  names(cs))
cs_agg <- cs[, {
  reop <- 0L
  for (f in fpmrs_reop_flags) reop <- reop + sum(num(get(f)) == 1, na.rm = TRUE)
  .(compl_surg_visits = .N,
    compl_reop_events = reop)
}, by = key]

cp <- fread(paths["compl_pessPT"])
cp[, key := paste(WU_ID, episode_number, sep = "_")]
cp_agg <- cp[, .(compl_pessPT_visits = .N), by = key]

# =============================================================================
# 7. ASSEMBLE per-patient (per treated episode) table, fill zeros
# =============================================================================
pt <- copy(cohort_paths[, .(key, pathway, WU_ID, episode_number,
                            idx_sling_repair, idx_mesh)])
merge0 <- function(x, y) {
  z <- y[x, on = "key"]
  numcols <- setdiff(names(y), "key")
  for (c in numcols) z[is.na(get(c)), (c) := 0]
  z
}
for (tb in list(em_post_agg, em_pre_agg, proc_agg, spt_agg, spe_agg,
                cs_agg, cp_agg)) pt <- merge0(pt, tb)

# index treatment count = 1 per treated episode; index wRVU by pathway
pt[, index_procedure := 1L]
pt[, index_wrvu := WRVU_INDEX[pathway]]

# ---- FPMRS professional wRVU per treated patient (KEEP PT/anesth/facility OUT)
# Components: index treatment (if FPMRS-billable proc) + post E&M + post office
# procedures + subsequent pessary + complication reoperations (each reop ~ index
# wRVU proxy). PT sessions are the PT bucket and excluded. usPVR contributes ~0.
pt[, fpmrs_wrvu :=
     fifelse(is.na(index_wrvu), 0, index_wrvu) +   # PT pathway index_wrvu=NA->0
     post_em_wrvu + post_proc_wrvu + subsequent_pessary_wrvu +
     compl_reop_events * fifelse(is.na(index_wrvu), 0, index_wrvu)]

# reoperation indicator (any FPMRS reoperative event: index-window flag OR
# complication-window event)
pt[, reoperation := as.integer(
  (idx_sling_repair %in% 1) | (idx_mesh %in% 1) | (compl_reop_events > 0))]

# =============================================================================
# 8. AGGREGATE workload per treated patient by pathway + bootstrap SE
# =============================================================================
metrics <- c("index_procedure", "pre_em_neweval", "post_em_visits",
             "post_em_return", "post_procedures", "post_cystoscopy",
             "post_uds", "subsequent_pessary_visits", "subsequent_PT_sessions",
             "compl_surg_visits", "compl_pessPT_visits", "compl_reop_events",
             "reoperation", "fpmrs_wrvu")

boot_se <- function(x, B = 1000L) {
  n <- length(x)
  if (n < 2) return(NA_real_)
  m <- numeric(B)
  for (b in seq_len(B)) m[b] <- mean(x[sample.int(n, n, replace = TRUE)])
  sd(m)
}

rows <- list()
for (pw in PATHWAYS) {
  sub <- pt[pathway == pw]
  n <- nrow(sub)
  r <- list(pathway = pw, n_treated_episodes = n,
            n_women = uniqueN(sub$WU_ID))
  for (mt in metrics) {
    x <- sub[[mt]]
    r[[paste0(mt, "_per_pt")]] <- round(mean(x), 4)
  }
  # SEs for the two headline quantities
  r[["post_em_visits_per_pt_SE"]]  <- round(boot_se(sub$post_em_visits), 4)
  r[["fpmrs_wrvu_per_pt_SE"]]      <- round(boot_se(sub$fpmrs_wrvu), 4)
  r[["reoperation_prob_SE"]]       <- round(boot_se(sub$reoperation), 5)
  rows[[pw]] <- as.data.table(r)
}
workload <- rbindlist(rows, fill = TRUE)
setnames(workload, "reoperation_per_pt", "reoperation_prob")
fwrite(workload, file.path(OUT, "workload_per_treated_patient.csv"))
msg("== workload table ==")
print(workload[, .(pathway, n_treated_episodes, post_em_visits_per_pt,
                   post_procedures_per_pt, subsequent_pessary_visits_per_pt,
                   subsequent_PT_sessions_per_pt, reoperation_prob,
                   fpmrs_wrvu_per_pt)])

# =============================================================================
# 9. TIER-1 index treatment provider specialty distribution
# =============================================================================
ps <- fread(paths["prov_spec"], colClasses = "character")
CMS_SPEC <- c("16" = "OB/GYN", "34" = "Urology", "50" = "Nurse Practitioner",
              "97" = "Physician Assistant", "65" = "Physical Therapist",
              "08" = "Family Practice", "8" = "Family Practice",
              "11" = "Internal Medicine", "02" = "General Surgery",
              "2" = "General Surgery", "98" = "Gyn/Oncology-Other")
ps[, trt := index_car_trt_prvdr_spclty1]
ps[trt == "" | is.na(trt), trt := "missing"]
ps[, trt_label := fifelse(trt %in% names(CMS_SPEC), CMS_SPEC[trt],
                          fifelse(trt == "missing", "missing", "other"))]
ps[, fpmrs_proxy := as.integer(trt %in% c("16", "34"))]
spec_tab <- ps[, .(n = .N), by = .(episode_type, trt_label)][order(episode_type, -n)]
fwrite(spec_tab, file.path(OUT, "tier1_index_treatment_specialty.csv"))
spec_summary <- ps[, .(n = .N,
                       pct_OBGYN_or_Urology = round(100 * mean(fpmrs_proxy), 1),
                       pct_missing = round(100 * mean(trt == "missing"), 1)),
                   by = episode_type]
fwrite(spec_summary, file.path(OUT, "tier1_index_specialty_summary.csv"))
msg("== Tier-1 index treatment specialty (FPMRS proxy = OB/GYN or Urology) ==")
print(spec_summary)

# =============================================================================
# 10. CADR_2023 COST companion (professional buckets, 2023 vintage)
# =============================================================================
# Keep buckets SEPARATE. FPMRS professional = index treatment prof +
# E&M/consult prof + office-procedure prof + complication prof. Anesthesia, ED,
# facility (OP/IP/ASC) are reported but NEVER folded into FPMRS workload.
m2 <- march2
numeric_cols <- setdiff(names(m2), c("sui_treatment", "Type of Cost",
     "Pre or Post Index Procedure", "race", "state", "Region", "Division"))
for (c in numeric_cols) set(m2, j = c, value = num(m2[[c]]))
prof_cols <- grep("^professional_", names(m2), value = TRUE)
prof_cols <- setdiff(prof_cols, "professional_anesth_cost")          # anesth out
prof_cols <- setdiff(prof_cols, grep("^edprofessional", prof_cols, value = TRUE))
m2[, fpmrs_prof := rowSums(as.matrix(.SD), na.rm = TRUE), .SDcols = prof_cols]
m2[, anesth_prof := rowSums(cbind(num(professional_anesth_cost)), na.rm = TRUE)]
m2[, ed_prof := rowSums(cbind(num(edprofessional_cost),
                              num(edprofessional_complication_cost)), na.rm = TRUE)]
m2[, facility := rowSums(cbind(num(IP_facility_cost), num(OP_facility_cost),
                              num(ASC_facility_cost)), na.rm = TRUE)]
n_m2 <- m2[, .(n = uniqueN(WU_ID)), by = sui_treatment]
cost_comp <- m2[, .(
  fpmrs_professional_cost = sum(fpmrs_prof, na.rm = TRUE),
  anesthesia_cost = sum(anesth_prof, na.rm = TRUE),
  ed_professional_cost = sum(ed_prof, na.rm = TRUE),
  facility_cost = sum(facility, na.rm = TRUE)
), by = sui_treatment]
cost_comp <- n_m2[cost_comp, on = "sui_treatment"]
cost_comp[, `:=`(
  fpmrs_professional_cost_per_pt = round(fpmrs_professional_cost / n, 2),
  anesthesia_cost_per_pt = round(anesthesia_cost / n, 2),
  ed_professional_cost_per_pt = round(ed_professional_cost / n, 2),
  facility_cost_per_pt = round(facility_cost / n, 2)
)]
fwrite(cost_comp[, .(sui_treatment, n,
        fpmrs_professional_cost_per_pt, anesthesia_cost_per_pt,
        ed_professional_cost_per_pt, facility_cost_per_pt)],
       file.path(OUT, "cadr2023_cost_companion_per_pt.csv"))
msg("== CADR_2023 professional cost per pt (2016 $, buckets kept separate) ==")
print(cost_comp[, .(sui_treatment, n, fpmrs_professional_cost_per_pt,
                    anesthesia_cost_per_pt, facility_cost_per_pt)])

# =============================================================================
# 11. COMPARISON to cliff demand assumptions
# =============================================================================
comp <- data.table(
  cliff_file = c(
    "care_pathway.csv", "urps_service_workload_rvu.csv",
    "urps_service_workload_rvu.csv", "urps_service_workload_rvu.csv",
    "urps_service_workload_rvu.csv", "urps_service_workload_rvu.csv",
    "urps_service_workload_rvu.csv", "staffing_conversion.csv",
    "staffing_conversion.csv", "care_pathway.csv", "care_pathway.csv"),
  parameter = c(
    "treatment_mix POP conservative share = 0.50 (low confidence)",
    "new_consultation work_rvu",
    "return_visit work_rvu",
    "pessary_care work_rvu / visits",
    "urodynamics + cystoscopy work_rvu / rate",
    "sling_procedure work_rvu",
    "postoperative_care (bundled 090-global = 0 wRVU)",
    "services->FTE (work_rvu volumes)",
    "wrvu_per_fte benchmark",
    "prevalence / lifetime surgery risk",
    "care_seeking fraction"),
  cadr_flag = c(
    "replaceable_by_CADR",
    "partially_informed",
    "replaceable_by_CADR",
    "replaceable_by_CADR",
    "replaceable_by_CADR",
    "supported",
    "partially_informed",
    "partially_informed",
    "not_informed_by_CADR",
    "not_informed_by_CADR",
    "not_informed_by_CADR"),
  cadr_evidence = c(
    "CADR gives realized conservative(pessary/PT) vs surgical(sling) index mix in an aged-Medicare SUI/POP cohort; can replace the setting-dependent 0.50 for >=65 (but it is a treated-cohort mix, not population).",
    "CADR counts pre-index new+consult E&M (initial specialist eval) per treated pt; level mix supports a wRVU but eval intensity, not the 2.535 value itself.",
    "CADR counts POST-index return E&M visits per treated pt by level -> downstream return-visit VOLUME per treated pt, the missing multiplier on the 1.396 wRVU.",
    "CADR counts subsequent pessary visits per treated pt (pessary maintenance workload) -> visits-per-pt to multiply pessary_care wRVU.",
    "CADR counts post-index cystoscopy + urodynamics per treated pt -> downstream diagnostic procedure VOLUME per treated pt.",
    "CADR confirms sling as index surgical procedure; wRVU value itself is the CMS PFS anchor (unchanged).",
    "CADR shows postop return E&M + reoperations OUTSIDE the 090-global window -> the true postop workload is NOT zero; global-period assumption understates it.",
    "CADR supplies the per-treated-patient service VOLUMES (visits, procedures, reoperations) that feed sum(volume*wRVU); it does not supply the wrvu_per_fte denominator.",
    "wRVU-per-FTE productivity benchmark is a workforce parameter; CADR (claims utilization) cannot inform it.",
    "prevalence & lifetime surgery risk are population epidemiology; CADR is a treated cohort -> out of scope by design.",
    "care-seeking is upstream of treatment entry; CADR observes only already-treated patients -> cannot inform.")
)
fwrite(comp, file.path(OUT, "cliff_parameter_comparison_flags.csv"))
msg("== cliff parameter comparison flags ==")
print(comp[, .(parameter, cadr_flag)])

msg("\nDONE. Outputs in %s", OUT)
