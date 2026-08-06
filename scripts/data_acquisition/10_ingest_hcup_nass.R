#!/usr/bin/env Rscript
# =============================================================================
# HCUP NASS (Nationwide Ambulatory Surgery Sample) — ingest -> base-year anchors
# =============================================================================
#
# PURPOSE:
#   Build the base-year PROCEDURE-VOLUME calibration anchors the demand model
#   compares its predictions against (R/calibration-demand_lifecourse.R;
#   config/calibration_targets.yml): national weighted counts of SUI sling and
#   POP repair, in the `category`/`observed` shape calibrate_lifecourse_demand()
#   expects. NASS is the right frame because most URPS procedures are OUTPATIENT
#   (NIS is inpatient/ICD-10-PCS and undercounts slings — see the config note).
#
# WHY THIS IS AN INGESTER, NOT A DOWNLOADER:
#   NASS is a LICENSED file. There is no free API. You obtain it from the HCUP
#   Central Distributor after completing the online HCUP Data Use Agreement
#   training: https://hcup-us.ahrq.gov/tech_assist/centdist.jsp
#   Place the purchased core file locally and point this script at it.
#
#   FREE AGGREGATE FALLBACK: if you only need the national totals (not microdata),
#   HCUP Fast Stats / HCUPnet publish weighted procedure counts you can enter by
#   hand as the anchors — https://datatools.ahrq.gov/hcup-fast-stats — and
#   Medicare Part B (which you already have) anchors the 65+ share.
#
# INPUT (set the path):
#   NASS_CORE_PATH env var or --file=, pointing at a NASS core file already read
#   into a delimited table (CSV/TSV). If you have the raw fixed-width ASCII, run
#   the HCUP-supplied load program (SAS/SPSS/Stata) first, or read with the HCUP
#   file specifications, then export to CSV.
#   Expected columns (NASS core): YEAR, DISCWT (discharge weight), and CPT1..CPTk
#   (procedure CPT/HCPCS). Column names are configurable below.
#
# OUTPUT:
#   data-raw/hcup/nass_<year>_urps_anchors.csv   (category, observed, source, year)
#   data/anchors/sling_volume.csv , data/anchors/prolapse_volume.csv
#   data-raw/hcup/nass_<year>_manifest.txt
#
#   Use directly:
#     obs <- readr::read_csv("data-raw/hcup/nass_2021_urps_anchors.csv")
#     calibrate_lifecourse_demand(fte$service_volumes, obs[, c("category","observed")])
# =============================================================================

suppressPackageStartupMessages({
  for (p in c("dplyr", "readr", "here")) {
    if (!requireNamespace(p, quietly = TRUE))
      stop("Package '", p, "' is required. install.packages('", p, "')", call. = FALSE)
  }
  library(dplyr); library(readr)
})

# ---- Config: input path + column names --------------------------------------
args <- commandArgs(trailingOnly = TRUE)
file_arg <- sub("^--file=", "", grep("^--file=", args, value = TRUE))
nass_path <- if (length(file_arg)) file_arg else Sys.getenv("NASS_CORE_PATH")
if (nchar(nass_path) == 0 || !file.exists(nass_path)) {
  stop(
    "NASS core file not found.\n",
    "  Set NASS_CORE_PATH=/path/to/nass_core.csv  or pass --file=/path/...\n",
    "  Acquire NASS: https://hcup-us.ahrq.gov/tech_assist/centdist.jsp (licensed).\n",
    "  Free totals fallback: https://datatools.ahrq.gov/hcup-fast-stats\n",
    call. = FALSE)
}

YEAR_COL   <- "YEAR"
WEIGHT_COL <- "DISCWT"                       # NASS discharge weight -> national estimate
CPT_COLS   <- paste0("CPT", 1:30)           # NASS carries up to ~30 CPT slots

# ---- URPS CPT code sets (editable; verify against your NASS year's coding) ---
# Sling for stress urinary incontinence.
SLING_CPT <- c("57288",          # sling operation for SUI
               "51992")          # laparoscopic sling (include if you count it)
# Pelvic organ prolapse repair (anterior/posterior/apical/obliterative).
PROLAPSE_CPT <- c("57240", "57250", "57260", "57265", "57268",  # ant/post/combined/enterocele
                  "57282", "57283",                              # sacrospinous / uterosacral (apical)
                  "57284", "57285", "57423",                     # paravaginal (open / lap)
                  "57425",                                       # lap sacrocolpopexy
                  "57120")                                       # colpocleisis (LeFort)

# ---- Read + validate --------------------------------------------------------
message("Reading NASS core: ", nass_path)
nass <- readr::read_csv(nass_path, show_col_types = FALSE, guess_max = 1e5)
need <- c(YEAR_COL, WEIGHT_COL)
if (!all(need %in% names(nass)))
  stop("NASS file missing column(s): ", paste(setdiff(need, names(nass)), collapse = ", "),
       ". Adjust YEAR_COL/WEIGHT_COL to your extract's names.", call. = FALSE)
cpt_present <- intersect(CPT_COLS, names(nass))
if (!length(cpt_present))
  stop("No CPT columns found (looked for ", CPT_COLS[1], "..", tail(CPT_COLS, 1),
       "). Adjust CPT_COLS to your extract's names.", call. = FALSE)
message("  ", format(nrow(nass), big.mark = ","), " records; ",
        length(cpt_present), " CPT columns; year(s): ",
        paste(sort(unique(nass[[YEAR_COL]])), collapse = ", "))

# any-CPT-slot membership, then weighted national count
has_any <- function(df, codes) {
  hits <- vapply(cpt_present, function(cc) as.character(df[[cc]]) %in% codes,
                 logical(nrow(df)))
  if (is.null(dim(hits))) hits else rowSums(hits) > 0   # single- vs multi-column
}
w <- as.numeric(nass[[WEIGHT_COL]])
sling_flag    <- has_any(nass, SLING_CPT)
prolapse_flag <- has_any(nass, PROLAPSE_CPT)

by_year <- function(flag) {
  tapply(w[flag], nass[[YEAR_COL]][flag], sum, na.rm = TRUE)
}
sling_by_year    <- by_year(sling_flag)
prolapse_by_year <- by_year(prolapse_flag)

base_year <- as.integer(max(nass[[YEAR_COL]], na.rm = TRUE))  # latest year present
gv <- function(v) as.numeric(v[as.character(base_year)])
anchors <- tibble::tibble(
  category = c("sling_procedure_volume", "prolapse_procedure_volume"),
  observed = c(gv(sling_by_year), gv(prolapse_by_year)),
  source   = sprintf("HCUP NASS %d, DISCWT-weighted; CPT %s / %s",
                     base_year, paste(SLING_CPT, collapse = "+"),
                     paste(PROLAPSE_CPT, collapse = "+")),
  year     = base_year
)
print(anchors)

# ---- Write anchors ----------------------------------------------------------
hcup_dir   <- here::here("data-raw", "hcup");  dir.create(hcup_dir, showWarnings = FALSE, recursive = TRUE)
anchor_dir <- here::here("data", "anchors");   dir.create(anchor_dir, showWarnings = FALSE, recursive = TRUE)

out_csv <- file.path(hcup_dir, sprintf("nass_%d_urps_anchors.csv", base_year))
readr::write_csv(anchors, out_csv)
# split files matching config/calibration_targets.yml paths
readr::write_csv(anchors[anchors$category == "sling_procedure_volume", ],
                 file.path(anchor_dir, "sling_volume.csv"))
readr::write_csv(anchors[anchors$category == "prolapse_procedure_volume", ],
                 file.path(anchor_dir, "prolapse_volume.csv"))
message("Saved anchors: ", out_csv)

writeLines(c(
  sprintf("HCUP NASS %d — URPS base-year procedure anchors", base_year),
  paste("Generated:", Sys.time()),
  paste("Input:", nass_path),
  paste("Weight:", WEIGHT_COL, "| CPT slots scanned:", length(cpt_present)),
  "",
  sprintf("sling_procedure_volume    = %s (CPT %s)",
          format(round(gv(sling_by_year)), big.mark = ","), paste(SLING_CPT, collapse = "+")),
  sprintf("prolapse_procedure_volume = %s (CPT %s)",
          format(round(gv(prolapse_by_year)), big.mark = ","), paste(PROLAPSE_CPT, collapse = "+")),
  "",
  "Feeds R/calibration-demand_lifecourse calibrate_lifecourse_demand() as the `observed` anchors.",
  "Also update config/calibration_targets.yml sha256 for the split files.",
  "NASS is licensed (HCUP Central Distributor). Free totals: HCUP Fast Stats.",
  "CPT sets are editable at the top of this script — verify against your year."
), file.path(hcup_dir, sprintf("nass_%d_manifest.txt", base_year)))

message("Done. NASS URPS anchors ready for base-year calibration.")
