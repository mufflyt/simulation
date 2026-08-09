#!/usr/bin/env Rscript
# Disposable intermediates: what they were, and how to get them back ----
#
#   Rscript scripts/dev/regenerate_intermediates.R     # check every path resolves
#
# WHY THIS EXISTS. Acquiring the external data for this project left ~208 MB of
# derived files in a session scratchpad: a 9.7 MB extract from the 3.25 GB CMS
# PUF, four MEPS 2023 public-use files, a prototype survey-weighted fit, and the
# CMS DCAT catalogue. All of it was deleted, on the argument that every piece is
# either superseded or regenerable.
#
# "Regenerable" is a claim, and an unverified claim about a deleted file is
# indistinguishable from a guess. This file turns each claim into a declaration
# that can be checked, and points at the command that actually rebuilds each.
#
# WHAT IT CHECKS, AND WHAT IT DELIBERATELY DOES NOT. The check verifies that the
# REGENERATION PATH still resolves: the producing script is present, the
# registry entry exists, the superseding code is in R/. It does NOT verify that
# the external file still downloads, because that would make a test depend on
# a third party's web server and turn an outage into a red build. A path that
# resolves is what the repository can be responsible for; whether nrmp.org or
# data.cms.gov is up today is not.
#
# See docs/DATA_ARTIFACT_INVENTORY.md for the four artifact classes and the
# full argument per file, and
# docs/DATA_PROVENANCE_NRMP.md for the case that established the pattern.

`%||%` <- function(a, b) if (is.null(a)) b else a

# ---- The declarations -------------------------------------------------------
#
# One row per file that was deleted. `requires` lists repository paths that must
# exist for the regeneration story to be true; `kind` says why the file is safe
# to lose.
#
#   regenerable  a committed script rebuilds it from a recorded source
#   superseded   the thing it prototyped now ships in R/, so rebuilding the
#                prototype would recover a worse version of shipped code
#   refetchable  one documented command away from a public endpoint
INTERMEDIATES <- list(
  list(
    id       = "cms_basket_extract",
    file     = "urps_basket_prov_svc.rds",
    size     = "9.7 MB",
    kind     = "regenerable",
    what     = paste("Rows of the CMS Medicare Physician & Other Practitioners",
                     "by-Provider-and-Service 2024 PUF restricted to the URPS",
                     "CPT basket. A working extract, never an input to anything."),
    why_safe = paste("Analysis 05 reads the RAW 3.25 GB CSV directly, not this",
                     "extract -- so nothing cited depends on it. The PUF is on",
                     "disk with its SHA-256 in config/canonical_sources.yml."),
    requires = c("data-raw/cms_psps/DOWNLOAD.md",
                 "config/canonical_sources.yml",
                 "scripts/validation/05_urps_share_partial_identification.R",
                 "scripts/data/build_urps_basket_prov_svc.R"),
    registry = c("cms_mup_phy_2024_prov_svc", "cms_mup_phy_2024_geo"),
    command  = "Rscript scripts/data/build_urps_basket_prov_svc.R"
  ),
  list(
    id       = "meps_2023",
    file     = "meps_FYC_2023.rds, meps_COND_2023.rds, meps_CLNK_2023.rds, meps_ob_2023.rds",
    size     = "~13 MB",
    kind     = "regenerable",
    what     = "MEPS 2023 full-year-consolidated, conditions, condition-event link, and office-based files.",
    why_safe = "A committed acquisition script downloads all four from AHRQ.",
    requires = c("scripts/data_acquisition/06_download_meps_2023.R"),
    registry = character(),
    command  = "Rscript scripts/data_acquisition/06_download_meps_2023.R"
  ),
  list(
    id       = "meps_prototype_fit",
    file     = "fitted_model.rds",
    size     = "4.5 MB",
    kind     = "superseded",
    what     = paste("A survey-weighted care-seeking fit from a 2026-08-03",
                     "prototype (a list of `panel_n` and `model`), built by an",
                     "ad-hoc script that read the MEPS files above."),
    why_safe = paste("The care-seeking model it prototyped now ships as",
                     "R/data-meps_care_seeking.R, with its figures committed to",
                     "figures/. Restoring the prototype would recover an earlier,",
                     "worse version of shipped code."),
    requires = c("R/data-meps_care_seeking.R",
                 "figures/meps_care_seeking_multipliers.png",
                 "figures/meps_care_seeking_comorbidity.png"),
    registry = character(),
    command  = "(none -- superseded; do not restore)"
  ),
  list(
    id       = "cms_dcat_catalogue",
    file     = "cms_datajson.json",
    size     = "2.8 MB",
    kind     = "refetchable",
    what     = "The data.cms.gov DCAT catalogue, used to resolve stable download URLs for the 2024 PUFs.",
    why_safe = paste("data-raw/cms_psps/DOWNLOAD.md records the exact query, and",
                     "the resolved downloadURLs are written out beside the",
                     "verified byte counts and SHA-256s."),
    requires = c("data-raw/cms_psps/DOWNLOAD.md"),
    registry = character(),
    command  = "curl -s https://data.cms.gov/data.json   # see data-raw/cms_psps/DOWNLOAD.md"
  )
)

# ---- Checking ---------------------------------------------------------------

registered_sources <- function(root = ".") {
  f <- file.path(root, "config", "canonical_sources.yml")
  if (!file.exists(f)) return(character())
  # Deliberately a line scan rather than a YAML parse: this must report a
  # missing key, not fail to load because some unrelated entry is malformed.
  ln <- readLines(f, warn = FALSE)
  trimws(sub(":.*$", "", grep("^  [a-z0-9_]+:\\s*$", ln, value = TRUE)))
}

#' Check that every declared regeneration path still resolves
#'
#' @return data.frame of id, kind, ok, and any missing prerequisite.
check_intermediates <- function(root = ".") {
  reg <- registered_sources(root)
  rows <- lapply(INTERMEDIATES, function(x) {
    miss_files <- x$requires[!file.exists(file.path(root, x$requires))]
    miss_reg   <- setdiff(x$registry, reg)
    data.frame(
      id = x$id, kind = x$kind,
      ok = length(miss_files) == 0L && length(miss_reg) == 0L,
      missing = paste(c(miss_files, if (length(miss_reg))
        paste0("canonical_sources.yml:", miss_reg)), collapse = "; "),
      stringsAsFactors = FALSE)
  })
  do.call(rbind, rows)
}

# The rebuild itself lives in scripts/data/build_urps_basket_prov_svc.R, which
# resolves the PUF through resolve_canonical() and stamps the artifact class
# into the object. It is not duplicated here: one implementation, per
# docs/CANONICAL_SOURCES_AUDIT.md.

# ---- CLI --------------------------------------------------------------------

.invoked_directly <- function() {
  f <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
  length(f) == 1L && basename(sub("^--file=", "", f)) == "regenerate_intermediates.R"
}

if (.invoked_directly()) {
  args <- commandArgs(trailingOnly = TRUE)
  mk <- grep("^--make=", args, value = TRUE)

  if (length(mk)) {
    stop("Rebuild commands live with the thing they build. For the CMS basket ",
         "subset: Rscript scripts/data/build_urps_basket_prov_svc.R. ",
         "See docs/DATA_ARTIFACT_INVENTORY.md.", call. = FALSE)
  } else {
    st <- check_intermediates(".")
    cat("\n=== disposable intermediates: does each regeneration path resolve? ===\n")
    for (i in seq_len(nrow(st))) {
      cat(sprintf("  %-22s %-12s %s\n", st$id[i], st$kind[i],
                  if (st$ok[i]) "OK" else paste("MISSING:", st$missing[i])))
    }
    cat("\n=== how to get each one back ===\n")
    for (x in INTERMEDIATES)
      cat(sprintf("  %-22s %s\n", x$id, x$command))
    cat("\nThis checks that the PATH resolves, not that the remote endpoint is up.\n",
        "A third party's outage is not this repository's red build.\n", sep = "")
    if (!all(st$ok)) quit(status = 1L)
  }
}
