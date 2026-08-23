# The package is now loaded by tests/testthat.R (or by pkgload during
# devtools::test()), so tests call exported functions directly. This helper only
# attaches the tidyverse verbs that test fixtures use directly, and provides a
# source-tree fallback for running test_dir() outside a package context.

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(tibble)
})

if (!requireNamespace("urpssim", quietly = TRUE) ||
    !exists("run_workforce_microsimulation", mode = "function")) {
  if (requireNamespace("pkgload", quietly = TRUE)) {
    root <- if (file.exists("DESCRIPTION")) "." else file.path("..", "..")
    suppressMessages(pkgload::load_all(root, quiet = TRUE, export_all = TRUE))
  }
}

# ---------------------------------------------------------------------------
# LOCATING THE SOURCE TREE, WHICH IS NOT THE SAME AS LOCATING A PACKAGE.
#
# Seventeen call sites across fourteen test files walked upward looking for a
# DESCRIPTION and treated the first hit as the repository root. That is wrong
# in exactly the execution contexts CI uses. Under covr and R CMD check the
# suite runs from inside the INSTALLED package, which also carries a
# DESCRIPTION -- so the walk stopped there, the guard `skip_if(length(root) ==
# 0)` never fired, and the test proceeded to look for R/*.R sources, scripts/
# and docs/ in a tree that ships none of them.
#
# The failures that produced were not "source tree absent"; they were assertion
# errors that read like real defects: "status text names non-existent path(s):
# R/geography-demand.R". A guard that cannot fire is worse than no guard,
# because it converts a missing precondition into a false accusation against
# the code.
#
# Meta/ is the discriminator: R's installer creates it, and it never exists in
# a source checkout. DESCRIPTION says "a package lives here"; DESCRIPTION
# without Meta/ says "the SOURCES live here", which is the actual precondition
# every one of those tests needs.
#
# Returns character(0) rather than erroring, so the existing call-site idiom --
# skip_if(length(root) == 0, ...) then root[1] -- keeps working unchanged.
.source_tree_root <- function() {
  candidates <- c(".", "..", file.path("..", ".."),
                  file.path("..", "..", ".."), file.path("..", "..", "..", ".."))
  for (p in candidates) {
    if (file.exists(file.path(p, "DESCRIPTION")) && !dir.exists(file.path(p, "Meta"))) {
      return(p)
    }
  }
  character(0)
}

# ---------------------------------------------------------------------------
# A SCIENTIFICALLY VALID PATHWAY, for tests of machinery rather than of the
# canonical parameterization.
#
# The shipped table carries per_entering = 1.00 on new_consultation, which turns
# a prevalence STOCK into an annual FLOW, and assert_incident_not_prevalent()
# refuses it. That refusal is CORRECT and stays until the parameter is sourced
# (docs/INCIDENT_ENTRY_ESTIMAND.md).
#
# Most tests that hit the refusal are not about that parameter at all -- they
# check that a mutation propagates, that volumes convert to FTE, that an
# exporter carries lo/hi columns. Those need a pathway that RUNS, and asserting
# a refusal in them would delete the coverage they exist to provide.
#
# 0.25 IS A FIXTURE, NOT A CANDIDATE VALUE. It is chosen only to be a plausible
# flow rather than a stock. The real parameter is unresolved and its estimator
# is pre-registered before data access; nothing here may be read as an estimate
# of it, and no test should tune it to make a number come out right.
#
# Tests that ARE about the canonical configuration must NOT use this -- they
# assert the refusal explicitly, and the end-to-end canonical run lives in
# .github/scripts/assert-canonical-science.R, which stays red by design.
valid_pathway <- function(pathway = condition_service_pathway()) {
  pathway$per_entering[pathway$service == "new_consultation"] <- 0.25
  pathway
}

# ---------------------------------------------------------------------------
# The default CMS service-share pipeline (calibrate_service_share_model() and
# everything built on it: combine_service_share_evidence(),
# draw_compositional_service_shares(), allocate_urps_service_workload(), ...)
# is deliberately fail-closed: it only runs against the real 2024 CMS
# Provider-and-Service / Geography PUFs plus a frozen linkage roster, never a
# silent placeholder. Those raw files are not vendored (they are large real
# CMS extracts, fetched by scripts/data_acquisition/), so on a checkout that
# does not have them this whole family of tests has nothing to exercise.
# Skip with a reason rather than error -- an error here reads as "the
# calibration pipeline is broken," when the real state is "the machine
# running this test does not have the 2024 CMS PUFs downloaded."
.skip_unless_cms_service_share_data <- function() {
  paths <- urpssim:::.cms_service_share_input_paths()
  missing <- names(paths)[!file.exists(paths)]
  skip_if(
    length(missing) > 0,
    paste0(
      "real CMS service-share input(s) not present: ",
      paste(missing, collapse = ", ")
    )
  )
}

# run_end_to_end_simulation(..., run_practice_economics = TRUE) calibrates
# payer mix from namcs_urps_payer_mix(), which fails closed against the
# real pooled NAMCS RDS (data-raw/namcs/namcs_pooled_2015_2019.rds) rather
# than a placeholder. That file is *.rds-gitignored (large real survey
# extract, see data-raw/namcs/02-namcs_multiyear_acquire.R), so it is
# never present on a fresh CI checkout even though it is vendored locally.
# testthat::test_file() runs with the working directory set to
# tests/testthat/, not the repo root (see test-practice-payer-mix.R's
# .pooled_namcs_path()), so both relative forms must be checked.
.skip_unless_namcs_pooled_data <- function() {
  rel <- "data-raw/namcs/namcs_pooled_2015_2019.rds"
  skip_if_not(
    file.exists(rel) || file.exists(file.path("..", "..", rel)),
    paste0("real pooled NAMCS input not present: ", rel)
  )
}

# calibrate_service_share_model()'s real API requires an `events` argument
# (real per-service/condition/year/provider_group compositional event
# counts) with at least two years per service for its leave-latest-year-out
# concentration selection. This fixture is synthetic (not real CMS/CHIA
# data) but schema-valid: two services, two years each, so the held-out
# cross-validation the function performs has something real to score. Lives
# here (not in test-calibration-service-shares.R) so it is available
# regardless of which service-share test file testthat runs first.
.synthetic_service_share_events <- function() {
  groups <- provider_routing_groups()
  tidyr::crossing(
    service = c("sling_procedure", "pessary_care"),
    condition = "Pelvic Floor Disorder",
    year = c(2022L, 2023L),
    provider_group = groups
  ) |>
    dplyr::mutate(
      service_events = dplyr::case_when(
        provider_group == "urps" ~ 40,
        provider_group == "general_obgyn" ~ 30,
        provider_group == "general_urology" ~ 15,
        provider_group == "app" ~ 10,
        TRUE ~ 5
      )
    )
}

# service_share_full_routing_fixture() lives in helper-service-share.R
# (single source of truth: derives its service list from
# service_share_required_routing_services() instead of a second hardcoded
# copy). All helper-*.R files load before any test-*.R file regardless of
# alphabetical order among the helpers, so test-core-run-end-to-end-
# service-shares.R and test-core-service-share-engine.R both see it.
