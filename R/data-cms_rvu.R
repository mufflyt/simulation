# CMS Work RVU Calibration ----
#
# Replaces the placeholder work RVUs with values taken from the official CMS
# Physician Fee Schedule Relative Value File. This is the calibration step that
# `assert_publishable_workload()` was gating on.
#
# The values in `CMS_WORK_RVU` were extracted from PPRRVU25_JAN.csv in the
# CMS RVU25A release (https://www.cms.gov/files/zip/rvu25a.zip), unmodified
# codes only (blank modifier). `refresh_cms_work_rvu()` re-derives them from a
# newer release so the table can be re-verified rather than trusted.
#
# Several placeholders were materially wrong, which is the point of doing this:
#
#   service               placeholder   CMS 2025   error
#   cystoscopy (52000)          2.20       1.53    +44%
#   urodynamics (51729)         1.60       2.51    -36%
#   bladder instillation        0.40       0.60    -33%
#   PTNS (64566)                0.45       0.60    -25%
#   sling (57288)              12.70      12.13     +5%
#
# CALIBRATION TIERS ---------------------------------------------------------
# Not every input can be anchored to a published source, and pretending
# otherwise would defeat the guards. Each input carries one of:
#
#   "calibrated"          anchored to an external published source (CMS RVUs)
#   "solved"              determined by an internal constraint, not assumed
#                         (the hours intercept, given the FTE definition)
#   "derived_by_analogy"  structure from a published source in a DIFFERENT
#                         specialty, mapped across by clinical analogy
#                         (delegation shares, from physiatry)
#   "uncalibrated_illustrative"  a placeholder
#
# `assert_publishable_workload()` accepts the first two, requires explicit
# opt-in for the third, and refuses the fourth.

CALIBRATION_TIERS <- c("calibrated", "solved", "derived_by_analogy",
                       "uncalibrated_illustrative")

CMS_RVU_RELEASE <- "RVU25A (PPRRVU25_JAN), CMS Physician Fee Schedule, 2025"
CMS_RVU_URL <- "https://www.cms.gov/files/zip/rvu25a.zip"

#' CMS work RVUs for the urogynecology procedure basket
#'
#' Work RVUs only (not practice-expense or malpractice RVUs), unmodified codes.
#' `glob` is the global-period indicator: `090` means routine postoperative care
#' is bundled into the procedure's work RVU and must NOT be counted again.
#'
#' @format A tibble with `hcpcs`, `description`, `work_rvu`, `glob`.
#' @source `r CMS_RVU_RELEASE`
#' @export
CMS_WORK_RVU <- tibble::tribble(
  ~hcpcs,   ~description,                        ~work_rvu, ~glob,
  "99203",  "Office o/p new low 30 min",              1.60, "XXX",
  "99204",  "Office o/p new mod 45 min",              2.60, "XXX",
  "99205",  "Office o/p new hi 60 min",               3.50, "XXX",
  "99212",  "Office o/p est sf 10 min",               0.70, "XXX",
  "99213",  "Office o/p est low 20 min",              1.30, "XXX",
  "99214",  "Office o/p est mod 30 min",              1.92, "XXX",
  "99215",  "Office o/p est hi 40 min",               2.80, "XXX",
  "57288",  "Sling operation for SUI",               12.13, "090",
  "57287",  "Revise/remove sling",                   11.15, "090",
  "51992",  "Laparoscopic sling operation",          14.87, "090",
  "57282",  "Colpopexy extraperitoneal",             11.63, "090",
  "57283",  "Colpopexy intraperitoneal",             11.66, "090",
  "57284",  "Repair paravaginal defect open",        14.33, "090",
  "57425",  "Laparoscopic colpopexy",                17.03, "090",
  "57120",  "Colpocleisis (Le Fort)",                 8.28, "090",
  "57240",  "Anterior colporrhaphy",                 10.08, "090",
  "57250",  "Posterior colporrhaphy",                10.08, "090",
  "57260",  "Combined ant/post colporrhaphy",        13.25, "090",
  "57265",  "Combined ap colporrhaphy w/enterocele", 15.00, "090",
  "57268",  "Repair of enterocele",                   7.57, "090",
  "52000",  "Cystourethroscopy",                      1.53, "000",
  "52287",  "Cystoscopy chemodenervation (Botox)",    3.20, "000",
  "51726",  "Complex cystometrogram",                 1.71, "000",
  "51728",  "Cystometrogram with voiding pressure",   2.11, "000",
  "51729",  "Cystometrogram with VP and UP",          2.51, "000",
  "51741",  "Complex uroflowmetry",                   0.17, "XXX",
  "51784",  "Anal/urinary muscle EMG",                0.75, "XXX",
  "64566",  "PTNS posterior tibial neurostimulation", 0.60, "000",
  "64581",  "Open sacral neurostimulator implant",   12.20, "090",
  "57160",  "Pessary fitting/insertion",              0.89, "000",
  "51700",  "Bladder instillation",                   0.60, "000"
)

# ---- Service -> CPT basket -------------------------------------------------

# Each modelled service maps to one or more CPT codes with a MIX WEIGHT. The
# weights are a case-mix assumption, declared here rather than buried inside a
# single representative code, and they must sum to 1 within each service.
#
# Level-of-service mixes for E/M follow the shape of specialist outpatient
# billing (new visits concentrated at 99204, established at 99213/99214).
# Procedure mixes reflect that most sling operations are the open retropubic /
# transobturator code and most prolapse repairs are compartment-specific
# colporrhaphy rather than sacrocolpopexy.
URPS_CPT_BASKET <- tibble::tribble(
  ~service,                ~hcpcs,   ~mix,
  "new_consultation",      "99203",  0.20,
  "new_consultation",      "99204",  0.65,
  "new_consultation",      "99205",  0.15,

  "return_visit",          "99212",  0.15,
  "return_visit",          "99213",  0.55,
  "return_visit",          "99214",  0.30,

  "pessary_care",          "57160",  1.00,

  "urodynamics",           "51726",  0.20,
  "urodynamics",           "51728",  0.35,
  "urodynamics",           "51729",  0.30,
  "urodynamics",           "51741",  0.10,
  "urodynamics",           "51784",  0.05,

  "cystoscopy",            "52000",  1.00,
  "botox_bladder",         "52287",  1.00,
  "ptns",                  "64566",  1.00,
  "bladder_instillation",  "51700",  1.00,

  "sling_procedure",       "57288",  0.78,
  "sling_procedure",       "51992",  0.10,
  "sling_procedure",       "57287",  0.12,

  "prolapse_procedure",    "57240",  0.18,
  "prolapse_procedure",    "57250",  0.14,
  "prolapse_procedure",    "57260",  0.20,
  "prolapse_procedure",    "57265",  0.12,
  "prolapse_procedure",    "57282",  0.10,
  "prolapse_procedure",    "57283",  0.08,
  "prolapse_procedure",    "57425",  0.10,
  "prolapse_procedure",    "57120",  0.05,
  "prolapse_procedure",    "57268",  0.03
)

URPS_SERVICE_SETTING <- c(
  new_consultation = "ambulatory", return_visit = "ambulatory",
  pessary_care = "ambulatory", urodynamics = "ambulatory",
  cystoscopy = "ambulatory", botox_bladder = "ambulatory",
  ptns = "ambulatory", bladder_instillation = "ambulatory",
  sling_procedure = "operative", prolapse_procedure = "operative",
  postoperative_care = "ambulatory", indirect_clinical_work = "indirect"
)

#' Validate that a CPT basket's mix weights sum to 1 per service
#'
#' @param basket Service-to-CPT basket.
#' @param tol Tolerance on the per-service sums.
#' @return (Invisibly) the basket.
#' @export
validate_cpt_basket <- function(basket = URPS_CPT_BASKET, tol = 1e-8) {
  s <- stats::aggregate(basket$mix, by = list(service = basket$service), FUN = sum)
  bad <- s$service[abs(s$x - 1) > tol]
  if (length(bad)) {
    stop(sprintf("validate_cpt_basket: mix weights must sum to 1 per service; offending: %s",
                 paste(bad, collapse = ", ")), call. = FALSE)
  }
  unknown <- setdiff(basket$hcpcs, CMS_WORK_RVU$hcpcs)
  if (length(unknown)) {
    stop(sprintf("validate_cpt_basket: CPT code(s) absent from CMS_WORK_RVU: %s",
                 paste(unknown, collapse = ", ")), call. = FALSE)
  }
  invisible(basket)
}

#' Mix-weighted work RVU per unit of each modelled service
#'
#' @param basket Service-to-CPT basket.
#' @param rvu CMS work-RVU table.
#' @return Tibble: `service`, `work_rvu`, `n_codes`, `codes`.
#' @keywords internal
service_work_rvu <- function(basket = URPS_CPT_BASKET, rvu = CMS_WORK_RVU) {
  validate_cpt_basket(basket)
  j <- merge(basket, rvu[, c("hcpcs", "work_rvu")], by = "hcpcs", all.x = TRUE)
  agg <- stats::aggregate(
    list(work_rvu = j$mix * j$work_rvu),
    by = list(service = j$service), FUN = sum
  )
  n <- stats::aggregate(list(n_codes = j$hcpcs), by = list(service = j$service),
                        FUN = length)
  codes <- stats::aggregate(list(codes = j$hcpcs), by = list(service = j$service),
                            FUN = function(z) paste(sort(z), collapse = "+"))
  out <- merge(merge(agg, n, by = "service"), codes, by = "service")
  tibble::as_tibble(out[order(out$service), ])
}

#' Build the workload basket from CMS work RVUs
#'
#' Produces the table consumed by [convert_workload_to_fte()], with
#' `calibration_status = "calibrated"` because every work RVU traces to the CMS
#' file.
#'
#' Postoperative care carries a work RVU of ZERO by design: every procedure in
#' the basket has a 090-day global period, so routine postoperative visits are
#' already bundled into the procedure's work RVU. Counting them again would
#' double-count the surgeon's work.
#'
#' @param basket Service-to-CPT basket.
#' @param rvu CMS work-RVU table.
#' @return Tibble in the shape of `URPS_SERVICE_WORKLOAD`.
#' @keywords internal
build_workload_from_cms <- function(basket = URPS_CPT_BASKET, rvu = CMS_WORK_RVU) {
  sw <- service_work_rvu(basket, rvu)

  extra <- tibble::tibble(
    service = c("postoperative_care", "indirect_clinical_work"),
    work_rvu = c(0, NA_real_),
    n_codes = c(0L, 0L),
    codes = c("bundled in 090-day global period", "not billable")
  )
  out <- dplyr::bind_rows(sw, extra)

  dplyr::mutate(
    out,
    setting = unname(URPS_SERVICE_SETTING[.data$service]),
    unit = dplyr::case_when(
      .data$service %in% c("new_consultation", "return_visit", "pessary_care",
                           "postoperative_care") ~ "encounter",
      .data$service %in% c("ptns", "bladder_instillation") ~ "session",
      .data$service == "urodynamics" ~ "study",
      .data$service == "indirect_clinical_work" ~ "fte_share",
      TRUE ~ "procedure"
    ),
    label = .data$codes,
    calibration_status = "calibrated",
    source = CMS_RVU_RELEASE
  )
}

# ---- Re-derivation from a fresh CMS release --------------------------------

#' Parse an official CMS PPRRVU file
#'
#' @param path Path to a `PPRRVU*.csv` extracted from a CMS RVU release.
#' @param skip Header rows to skip; the CMS layout puts the data from row 10.
#' @return Tibble of `hcpcs`, `mod`, `description`, `work_rvu`, `glob`.
#' @keywords internal
load_cms_pprrvu <- function(path, skip = 9L) {
  if (!file.exists(path)) {
    stop(sprintf("CMS PPRRVU file not found: %s\n  Download %s and extract it.",
                 path, CMS_RVU_URL), call. = FALSE)
  }
  d <- utils::read.csv(path, skip = skip, header = FALSE,
                       stringsAsFactors = FALSE, colClasses = "character")
  names(d)[1:15] <- c("hcpcs", "mod", "description", "status", "medicare_payment",
                      "work_rvu", "nonfac_pe", "nonfac_na", "fac_pe", "fac_na",
                      "mp_rvu", "nonfac_total", "fac_total", "pctc", "glob")
  d <- d[trimws(d$mod) == "", c("hcpcs", "mod", "description", "work_rvu", "glob")]
  d$work_rvu <- suppressWarnings(as.numeric(d$work_rvu))
  tibble::as_tibble(d[!is.na(d$work_rvu), ])
}

#' Re-derive the shipped work-RVU table from a newer CMS release
#'
#' @param path Path to a `PPRRVU*.csv`.
#' @param codes CPT codes to extract; defaults to those the model uses.
#' @return Tibble in the shape of [CMS_WORK_RVU].
#' @keywords internal
refresh_cms_work_rvu <- function(path, codes = CMS_WORK_RVU$hcpcs) {
  d <- load_cms_pprrvu(path)
  out <- d[d$hcpcs %in% codes, c("hcpcs", "description", "work_rvu", "glob")]
  missing <- setdiff(codes, out$hcpcs)
  if (length(missing)) {
    .msg_warn(sprintf("CPT code(s) absent from this CMS release: %s",
                      paste(missing, collapse = ", ")))
  }
  tibble::as_tibble(out[!duplicated(out$hcpcs), ])
}

#' Compare the shipped work RVUs against a CMS release
#'
#' Reports every code whose work RVU has moved, so an annual update is a
#' reviewable diff rather than a silent change in every projected number.
#'
#' @param path Path to a `PPRRVU*.csv`.
#' @param shipped The shipped table to compare against.
#' @param tol Absolute tolerance.
#' @return Tibble of differing codes (empty when the shipped table is current).
#' @keywords internal
verify_cms_work_rvu <- function(path, shipped = CMS_WORK_RVU, tol = 1e-8) {
  fresh <- refresh_cms_work_rvu(path, shipped$hcpcs)
  j <- merge(shipped[, c("hcpcs", "work_rvu")],
             fresh[, c("hcpcs", "work_rvu")],
             by = "hcpcs", suffixes = c("_shipped", "_file"))
  d <- j[abs(j$work_rvu_shipped - j$work_rvu_file) > tol, ]
  if (nrow(d)) {
    d$pct_change <- 100 * (d$work_rvu_file - d$work_rvu_shipped) / d$work_rvu_shipped
    .msg_warn(sprintf("%d work RVU(s) differ from the shipped table.", nrow(d)))
  }
  tibble::as_tibble(d)
}
