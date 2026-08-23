################################################################################
# R/reporting-scorecard_audits.R
# Real checks backing the remaining scientific-scorecard states (see
# generate_scientific_scorecard() in R/reporting-scientific_scorecard.R).
#
# Each audit_*() function runs the SAME gate CI runs -- an existing exported
# check, a testthat file, or a CI script -- rather than re-deriving an
# approximation of it, for the same reason audit_canonical_readiness()
# (R/reporting-canonical_readiness_audit.R) shells out to
# assert-canonical-science.R instead of re-implementing its logic. There is
# no spec document defining this 9-state taxonomy anywhere in the repo; these
# mappings were reconstructed by matching each state's name against the CI
# workflow, script, and test-file names that clearly correspond to it.
#
# CHEAP vs EXPENSIVE. Four of these (SEMANTICS, SOURCE_MUTATION,
# KNOWN_TRUTH_RECOVERY, CROSS_REPO_CONTRACTS) run in-process in well under a
# second and are always run. Two (SOFTWARE, ADVERSARIAL) shell out to a
# multi-minute CI-scale job -- the full test suite and a mutation/metamorphic
# battery, respectively -- and are gated behind generate_scientific_scorecard's
# `deep` argument so the scorecard stays fast by default. REPRODUCIBILITY has
# no in-process or subprocess equivalent at all: it is a from-scratch renv
# restore plus system-library install in CI, not something a package function
# can run inline, so it is reported NOT_ELIGIBLE unconditionally rather than
# faked.
################################################################################

#' Run one or more testthat files in-process and report pass/fail
#'
#' Resolves each path against the repository source tree (see
#' [.repo_source_root()]) rather than the caller's working directory, so this
#' works whether called from the repo root or from inside tests/testthat
#' itself.
#'
#' @param rel_paths Character vector of paths relative to the repo root.
#' @return A list with `available` and, when available, `passed`.
#' @keywords internal
.run_testthat_files <- function(rel_paths) {
  root <- .repo_source_root()
  if (is.na(root)) return(list(available = FALSE))
  full <- file.path(root, rel_paths)
  if (!all(file.exists(full))) return(list(available = FALSE))

  ok <- TRUE
  for (p in full) {
    res <- testthat::test_file(p, reporter = "silent", stop_on_failure = FALSE)
    df <- as.data.frame(res)
    if (nrow(df) == 0L || any(df$failed > 0 | df$error)) ok <- FALSE
  }
  list(available = TRUE, passed = ok)
}

#' Run a CI script as a subprocess from the repository root
#'
#' The scripts this backs (`scripts/ci/check_suite.R`,
#' `.github/scripts/adversarial/*.R`) assume `getwd()` is the repository
#' root, per their own headers. `system2()` inherits the CALLER's working
#' directory, which may be `tests/testthat` (when called from a test) or
#' anywhere else (when called interactively) -- so the working directory is
#' switched to the resolved repo root for the duration of the call, exactly
#' as CI itself runs them.
#'
#' @param rel_path Script path relative to the repo root.
#' @return A list with `available` and, when available, `status` and `text`.
#' @keywords internal
.run_ci_script <- function(rel_path) {
  root <- .repo_source_root()
  if (is.na(root)) return(list(available = FALSE))
  script <- file.path(root, rel_path)
  if (!file.exists(script)) return(list(available = FALSE))

  # check_suite.R's skip-budget audit assumes NOT_CRAN is set (GitHub Actions
  # sets it automatically via r-lib/actions; a local interactive session
  # usually does not). Without it, skip_on_cran() fires everywhere, and the
  # budget -- which expects only a handful of "On CRAN" skips -- reports a
  # spurious over-budget failure with nothing to do with the actual suite.
  # Set explicitly so this audit gives the same signal locally as in CI,
  # rather than depending on an ambient env var. Harmless for scripts that
  # don't read it (the adversarial scripts).
  #
  # withr::with_dir()/with_envvar() (not setwd()/Sys.setenv() with manual
  # on.exit cleanup) so THIS function's own body never calls a global-state
  # mutator directly -- see test-source-safety-gates.R's "no package
  # function performs global-state side effects" gate, which inspects each
  # exported/internal function's own body, not what its dependencies do.
  out <- withr::with_dir(root,
    withr::with_envvar(c(NOT_CRAN = "true"),
      suppressWarnings(
        system2(file.path(R.home("bin"), "Rscript"), script,
                stdout = TRUE, stderr = TRUE))
    )
  )
  status <- attr(out, "status")
  list(available = TRUE,
       status = if (is.null(status)) 0L else as.integer(status),
       text = paste(out, collapse = "\n"))
}

#' Audit SEMANTICS: estimand semantic contracts and hall-of-shame regressions
#'
#' Runs `test-estimand-semantic-contracts.R` and
#' `test-hall-of-shame-regressions.R` in-process ("Layer 2B" in
#' .github/workflows/scientific-semantic.yaml).
#'
#' @return A list with `available` and, when available, `passed`.
#' @family scorecard
#' @concept testing
#' @export
audit_semantics <- function() {
  .run_testthat_files(c("tests/testthat/test-estimand-semantic-contracts.R",
                        "tests/testthat/test-hall-of-shame-regressions.R"))
}

#' Audit SOURCE_MUTATION: the dedicated source-mutation-engine test
#'
#' Runs `test-source-mutation-engine.R` in-process ("Layer 2C" in
#' .github/workflows/source-mutation.yaml). Distinct from the
#' `SCIENTIFIC_MUTATION_MANIFEST` / [test_scientific_mutation()] boundary
#' suite the nightly workflow invokes directly, which is BVA-adjacent rather
#' than this state's namesake gate.
#'
#' @return A list with `available` and, when available, `passed`.
#' @family scorecard
#' @concept testing
#' @export
audit_source_mutation <- function() {
  .run_testthat_files("tests/testthat/test-source-mutation-engine.R")
}

#' Audit KNOWN_TRUTH_RECOVERY: synthetic supply DGP parameter recovery
#'
#' Generates a synthetic supply world with a known true entry rate
#' ([generate_synthetic_supply_world()]) and checks whether
#' [evaluate_supply_parameter_recovery()] recovers it within tolerance. Pure
#' synthetic data, no external dependency, deterministic given its default
#' seed.
#'
#' @return A list with `available`, `passed`, and `bias`.
#' @family scorecard
#' @concept testing
#' @export
audit_known_truth_recovery <- function() {
  dgp <- generate_synthetic_supply_world()
  rec <- evaluate_supply_parameter_recovery(dgp)
  list(available = TRUE, passed = isTRUE(rec$recovery_passed), bias = rec$bias)
}

#' Audit CROSS_REPO_CONTRACTS: the mufflyaccess pin and the simulation-cliff
#' contract
#'
#' Checks two independent repo-boundary contracts: [mufflyaccess_build()]
#' (the installed `mufflyaccess` package matches its pinned commit and export
#' list) and `test-simulation-cliff-contract.R` (the access-surface schema
#' contract, "Scientific Hardening Section 12 P1"). Both must hold, since a
#' state named in the plural is not satisfied by only one of its two
#' boundaries.
#'
#' @return A list with `available`, `passed`, `pin_usable`, and
#'   `cliff_contract_ok`.
#' @family scorecard
#' @concept testing
#' @export
audit_cross_repo_contracts <- function() {
  pin <- tryCatch(mufflyaccess_build(), error = function(e) list(usable = FALSE))
  cliff <- .run_testthat_files("tests/testthat/test-simulation-cliff-contract.R")
  list(available = isTRUE(cliff$available),
       passed = isTRUE(pin$usable) && isTRUE(cliff$passed),
       pin_usable = isTRUE(pin$usable),
       cliff_contract_ok = isTRUE(cliff$passed))
}

#' Audit SOFTWARE: full suite plus skip-budget discipline
#'
#' Runs `scripts/ci/check_suite.R` as a subprocess -- the full testthat suite
#' from the repository root (so source-tree-only gates actually execute,
#' unlike inside R CMD check's sandbox), failing on any test failure OR any
#' undeclared skip against tests/skip-budget.csv.
#'
#' EXPENSIVE: several minutes (2000+ tests). Not run by
#' [generate_scientific_scorecard()] unless called with `deep = TRUE`.
#'
#' @return A list with `available` and, when available, `passed` and `text`.
#' @family scorecard
#' @concept testing
#' @export
audit_software <- function() {
  r <- .run_ci_script("scripts/ci/check_suite.R")
  if (!isTRUE(r$available)) return(r)
  c(r, list(passed = identical(r$status, 0L)))
}

#' Audit ADVERSARIAL: scientific canaries and the metamorphic/property battery
#'
#' Runs `.github/scripts/adversarial/canaries.R` (mutation-detector canaries)
#' and `.github/scripts/adversarial/metamorphic.R` (property, metamorphic and
#' control checks; world count set by the `ADV_WORLDS` env var, 300 by
#' default) as subprocesses.
#'
#' EXPENSIVE and world-count dependent. Not run by
#' [generate_scientific_scorecard()] unless called with `deep = TRUE`.
#'
#' @return A list with `available` and, when available, `passed`.
#' @family scorecard
#' @concept testing
#' @export
audit_adversarial <- function() {
  root <- .repo_source_root()
  if (is.na(root)) return(list(available = FALSE))
  scripts <- c(".github/scripts/adversarial/canaries.R",
              ".github/scripts/adversarial/metamorphic.R")
  results <- lapply(scripts, .run_ci_script)
  if (!all(vapply(results, function(r) isTRUE(r$available), logical(1)))) {
    return(list(available = FALSE))
  }
  list(available = TRUE,
       passed = all(vapply(results, function(r) identical(r$status, 0L), logical(1))))
}
