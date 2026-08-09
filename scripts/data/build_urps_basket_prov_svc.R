#!/usr/bin/env Rscript
# URPS basket subset of the CMS Provider & Service PUF ----
#
#   Rscript scripts/data/build_urps_basket_prov_svc.R [--out=PATH]
#
# ============================================================================
# THIS SCRIPT CREATES A CONVENIENCE SUBSET OF THE CMS PUF. The resulting RDS is
# NOT a canonical input, is NOT consumed by manuscript analysis 05, and may be
# deleted and regenerated without changing the evidence chain.
# ============================================================================
#
# WHY THAT BANNER IS THE FIRST THING IN THE FILE. The output is called
# `urps_basket_prov_svc.rds`. It is 9.7 MB, it sits next to real data, and its
# name reads like a frozen manuscript input. Somebody finding it in six months
# will reasonably assume analysis 05 consumes it. Analysis 05 does not:
#
#   CMS Provider & Service 2024 CSV  (3.25 GB)
#       |
#       | canonical input, SHA-256 in config/canonical_sources.yml
#       v
#   05_urps_share_partial_identification.R
#       |
#       +-- reads the raw PUF directly
#       |
#       +-- produces run-identified evidence in artifacts/validation/
#
# versus this script:
#
#   CMS Provider & Service 2024 CSV
#       |
#       +-- optional extraction
#           v
#       urps_basket_prov_svc.rds
#           |
#           +-- convenience / cache only; nothing downstream
#
# `tests/testthat/test-data-artifact-classification.R` enforces the difference:
# it fails if 05 ever starts reading this file, because at that moment the RDS
# would become an input to a manuscript-citable result and would need a manifest
# and a hash rather than a cheerful banner.
#
# THE SOURCE IS RESOLVED THROUGH THE REGISTRY, NOT BY FILENAME. resolve_canonical()
# looks the PUF up in config/canonical_sources.yml and verifies its SHA-256, so
# this cannot silently extract from whichever similarly named CSV happens to be
# in data-raw/cms_psps/. A convenience file built from an unverified source is
# worse than no convenience file: it looks like the real thing and is not.
#
# NOT BYTE-IDENTICAL TO ANY EARLIER EXTRACT, stated rather than glossed. An
# ad-hoc version of this subset existed in a session scratchpad and was deleted;
# its exact column selection was never recorded. This rebuilds the same CONTENT
# from the same hashed source. If byte-identity mattered, the file would be an
# artifact with a manifest, not a convenience subset.
#
# See docs/DATA_ARTIFACT_INVENTORY.md for the four artifact classes and the rule
# that separates them.

suppressPackageStartupMessages({
  if (!requireNamespace("urpssim", quietly = TRUE)) pkgload::load_all(".", quiet = TRUE) else library(urpssim)
  library(data.table)
})

CANONICAL_ID <- "cms_mup_phy_2024_prov_svc"
DEFAULT_OUT  <- file.path("artifacts", "cache", "urps_basket_prov_svc.rds")

# Columns the subset keeps. Named explicitly so the file's shape is a decision
# recorded here rather than whatever the reader happened to select that day.
KEEP <- c("Rndrng_NPI", "Rndrng_Prvdr_Type", "Rndrng_Prvdr_State_Abrvtn",
          "HCPCS_Cd", "Place_Of_Srvc", "Tot_Benes", "Tot_Srvcs")

build_urps_basket_prov_svc <- function(out = DEFAULT_OUT, verbose = TRUE) {
  # resolve_canonical() fails closed on a missing registry entry, a missing
  # file, and a SHA-256 mismatch. All three are the failure this script must
  # not paper over.
  puf <- tryCatch(resolve_canonical(CANONICAL_ID), error = function(e) {
    stop("Cannot resolve the canonical CMS PUF (", CANONICAL_ID, "): ",
         conditionMessage(e),
         "\n\nRe-acquire it first. data-raw/cms_psps/DOWNLOAD.md carries the ",
         "DCAT query, the direct URL, the verified byte count and the SHA-256.",
         call. = FALSE)
  })

  basket <- unique(as.character(URPS_CPT_BASKET$hcpcs))
  if (verbose) {
    message("canonical source : ", puf)
    message("basket codes     : ", length(basket))
    message("scanning         : ",
            format(file.size(puf) / 1e9, digits = 3), " GB (single pass)")
  }

  d <- data.table::fread(puf, select = KEEP, showProgress = verbose)
  d <- d[d$HCPCS_Cd %chin% basket]

  # The provenance travels INSIDE the object, so a stray copy of this file can
  # still say what it is. A convenience subset that has been emailed to someone
  # loses its directory and its README; it does not lose its attributes.
  data.table::setattr(d, "urpssim_artifact_class", "derived_intermediate")
  data.table::setattr(d, "urpssim_canonical_source", CANONICAL_ID)
  data.table::setattr(d, "urpssim_source_sha256",
                      digest::digest(file = puf, algo = "sha256"))
  data.table::setattr(d, "urpssim_built_by",
                      "scripts/data/build_urps_basket_prov_svc.R")
  data.table::setattr(d, "urpssim_not_a_manuscript_input", TRUE)

  dir.create(dirname(out), recursive = TRUE, showWarnings = FALSE)
  saveRDS(d, out, compress = "xz")
  if (verbose)
    message("wrote            : ", out, "  (", nrow(d), " rows, ",
            format(file.size(out) / 1e6, digits = 3), " MB)")
  invisible(out)
}

.invoked_directly <- function() {
  f <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
  length(f) == 1L && basename(sub("^--file=", "", f)) == "build_urps_basket_prov_svc.R"
}

if (.invoked_directly()) {
  a <- grep("^--out=", commandArgs(trailingOnly = TRUE), value = TRUE)
  build_urps_basket_prov_svc(out = if (length(a)) sub("^--out=", "", a[1]) else DEFAULT_OUT)
}
