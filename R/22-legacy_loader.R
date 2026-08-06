# Legacy Script Loader ----
#
# The DPMM / SWAN / early-workforce scripts live in `inst/legacy/`. They are NOT
# part of the package: they carry top-level executable code (`workforce.R` alone
# has ~196 top-level statements), so loading the package must not run them.
#
# FROZEN, AND NOT SHIPPED. Two facts establish that nothing here is load-bearing,
# and both are checked rather than asserted:
#
#   * no call sites. Every name in this file is referenced from exactly one
#     place in the repository -- test-repo-hygiene.R. No module, script or
#     vignette calls the loader.
#   * no shared surface. The legacy scripts define 131 function names, the
#     package defines 468, and the intersection is EMPTY. R/42 and R/47 are
#     reimplementations of the SWAN work, not extractions of it.
#
# The scripts are therefore reference material: a record of what the analysis
# used to be, kept so a published number can be traced back to its origin. They
# are excluded from the built package by .Rbuildignore, which means this loader
# works in a SOURCE CHECKOUT ONLY. That is deliberate -- shipping 25,588 lines of
# frozen script to every installation bought nothing, and the previous
# .Rbuildignore shipped all of it while stripping the README that explains it.
#
# Adding a call to load_legacy() from package code would un-freeze the directory.
# Extract what you need into a tested module instead.
#
# They also share 15 function names across three files. Sourcing several of them
# in sequence silently redefines those names, and WHICH IMPLEMENTATION YOU GET
# DEPENDS ON SOURCE ORDER. That is the failure mode this loader exists to remove:
# it sources in a declared order, records every shadowed name, and reports the
# collisions instead of letting them pass unnoticed.
#
# Ownership is declared in LEGACY_CANONICAL below. The `03-dppm_validate_SWAN_better.R`
# implementation supersedes the older `dppm_validate_SWAN.R` one (see the README
# "DONE in dppm_validate_SWAN_better.R"), and `99-unsorted-fragments.R` -- the file
# formerly named "Unsure what to do with this.R" -- is superseded by both.

#' Legacy scripts, in the order they should be sourced
#'
#' Later files win on collision, so the canonical implementation of a shared name
#' must be sourced LAST. The validation family is therefore ordered
#' unsorted-fragments -> dppm_validate_SWAN -> 03-dppm_validate_SWAN_better.
#' @keywords internal
LEGACY_LOAD_ORDER <- c(
  "99-unsorted-fragments.R",
  "dppm_validate_SWAN.R",
  "03-dppm_validate_SWAN_better.R",
  "01-dppm_setup.R",
  "02-dppm_read_in_swan_files.R",
  "04-dppm_model_calibration.R",
  "05-dppm_50_year_national_incontinence.R",
  "supply_of_the_urogyn_workforce.R",
  "workforce.R"
)

#' Declared owner of each name defined in more than one legacy file
#'
#' Every entry is the file whose implementation should survive. The loader
#' asserts that the load order actually delivers it.
#' @export
LEGACY_CANONICAL <- c(
  calculate_age_gradient                 = "dppm_validate_SWAN.R",
  calculate_dpmm_incidence_rates         = "03-dppm_validate_SWAN_better.R",
  calculate_model_performance_metrics    = "03-dppm_validate_SWAN_better.R",
  calculate_swan_incidence_rates         = "03-dppm_validate_SWAN_better.R",
  calculate_swan_incontinence_prevalence = "03-dppm_validate_SWAN_better.R",
  create_validation_report               = "dppm_validate_SWAN.R",
  generate_validation_recommendations    = "dppm_validate_SWAN.R",
  generate_validation_summary            = "03-dppm_validate_SWAN_better.R",
  prepare_swan_data_for_validation       = "dppm_validate_SWAN.R",
  save_validation_results                = "dppm_validate_SWAN.R",
  validate_age_patterns                  = "03-dppm_validate_SWAN_better.R",
  validate_dpmm_with_swan_data           = "03-dppm_validate_SWAN_better.R",
  validate_incidence_rates               = "03-dppm_validate_SWAN_better.R",
  validate_prevalence_patterns           = "03-dppm_validate_SWAN_better.R",
  validate_severity_progression          = "03-dppm_validate_SWAN_better.R"
)

#' Directory holding the legacy scripts
#'
#' Source checkouts only. The scripts are frozen reference material and are
#' excluded from the built package, so `system.file()` will not find them in an
#' installation and this errors with that explanation rather than a bare
#' not-found.
#'
#' @return Path to `inst/legacy` in the source tree.
#' @keywords internal
legacy_dir <- function() {
  # system.file() is still tried first so an installation that deliberately
  # carries the directory (a vendored copy, an .Rbuildignore edit) keeps working.
  installed <- system.file("legacy", package = "urpssim")
  if (nzchar(installed) && dir.exists(installed)) return(installed)
  for (p in c("inst/legacy", file.path("..", "..", "inst", "legacy"))) {
    if (dir.exists(p)) return(p)
  }
  stop(paste(
    "Legacy script directory not found. inst/legacy is FROZEN reference material",
    "and is excluded from the built package (.Rbuildignore), so it is reachable",
    "from a source checkout only. Nothing in the package depends on it: the",
    "legacy scripts share no function name with R/. If you need one of those",
    "implementations, extract it into a tested module rather than restoring the",
    "directory to the build."), call. = FALSE)
}

#' Inventory every function defined by the legacy scripts
#'
#' @param dir Directory holding the scripts.
#' @return Tibble of `file`, `fn`, `line`.
#' @keywords internal
legacy_definitions <- function(dir = legacy_dir()) {
  files <- list.files(dir, pattern = "[.]R$", full.names = TRUE)
  out <- lapply(files, function(f) {
    x <- readLines(f, warn = FALSE)
    m <- grep("^[a-zA-Z._][a-zA-Z0-9._]* *<- *function", x)
    if (!length(m)) return(NULL)
    tibble::tibble(file = basename(f), fn = sub(" *<- *function.*", "", x[m]), line = m)
  })
  dplyr::bind_rows(out)
}

#' Names defined by more than one legacy script
#'
#' @param dir Directory holding the scripts.
#' @return Tibble of `fn` and the files defining it.
#' @export
legacy_collisions <- function(dir = legacy_dir()) {
  defs <- legacy_definitions(dir)
  defs %>%
    dplyr::distinct(.data$fn, .data$file) %>%
    dplyr::group_by(.data$fn) %>%
    dplyr::filter(dplyr::n() > 1) %>%
    dplyr::summarise(files = paste(sort(.data$file), collapse = ", "),
                     n_files = dplyr::n(), .groups = "drop") %>%
    dplyr::arrange(.data$fn)
}

#' Is an expression a definition rather than an executable statement?
#'
#' @param x A parsed expression.
#' @return TRUE when the expression defines a function or a literal constant.
#'
#' Assignments of a function, and assignments of a literal constant, are
#' definitions. Everything else -- a top-level `read_rds()`, a `load()`, a plot
#' call -- is script code that must not run merely because the file was loaded.
.is_definition <- function(x) {
  if (!is.call(x)) return(FALSE)
  head_fn <- as.character(x[[1]])[1]
  if (head_fn == "library" || head_fn == "require" ||
      head_fn == "suppressPackageStartupMessages") return(FALSE)
  if (!head_fn %in% c("<-", "=", "<<-")) return(FALSE)
  rhs <- x[[3]]
  if (is.call(rhs) && identical(as.character(rhs[[1]])[1], "function")) return(TRUE)
  # Constant assignments: literals, or calls composed only of literals (c(),
  # tribble(), setNames() and the like). Anything that touches the filesystem or
  # the network fails this test.
  safe <- c("c", "list", "character", "numeric", "integer", "logical",
            "setNames", "structure", "tibble", "tribble", "data.frame",
            "as_tibble", "rep", "seq", "seq_len", "seq_along", "matrix", "factor")
  if (is.call(rhs)) {
    fns <- all.names(rhs, functions = TRUE)
    fns <- setdiff(fns, all.vars(rhs))
    fns <- sub("^.*::", "", fns)
    return(all(fns %in% c(safe, "::", ":::", ":", "-", "+", "*", "/", "^")))
  }
  TRUE
}

#' Source the legacy scripts in a declared order, reporting every collision
#'
#' By default only DEFINITIONS are evaluated. The legacy scripts interleave
#' function definitions with analysis code that reads data files at top level --
#' `workforce.R` alone has ~196 top-level statements, several of them
#' `load()`/`read_rds()` calls. Sourcing them wholesale would try to read a
#' filesystem that may not exist and would run analyses nobody asked for, so the
#' loader evaluates the function definitions and skips the rest.
#'
#' @param dir Directory holding the scripts.
#' @param envir Environment to source into.
#' @param files Subset of [LEGACY_LOAD_ORDER] to load.
#' @param functions_only Evaluate only definitions (default). Set FALSE to run
#'   the scripts as scripts, which requires the external data to be present.
#' @param quiet Suppress the collision report.
#' @return (Invisibly) a tibble of shadowed names and the file that won.
#' @export
load_legacy <- function(dir = legacy_dir(), envir = parent.frame(),
                        files = LEGACY_LOAD_ORDER, functions_only = TRUE,
                        quiet = FALSE) {
  defs <- legacy_definitions(dir)
  shadowed <- list()
  seen <- character(0)
  n_skipped <- 0L

  for (f in files) {
    p <- file.path(dir, f)
    if (!file.exists(p)) {
      .msg_warn(sprintf("Legacy script not found, skipping: %s", f))
      next
    }
    here <- unique(defs$fn[defs$file == f])
    clash <- intersect(here, seen)
    if (length(clash)) {
      shadowed[[length(shadowed) + 1L]] <-
        tibble::tibble(fn = clash, winner = f)
    }
    seen <- union(seen, here)

    if (isTRUE(functions_only)) {
      e <- parse(p, keep.source = FALSE)
      for (x in e) {
        if (!.is_definition(x)) {
          n_skipped <- n_skipped + 1L
          next
        }
        # A "constant" assignment can still reference a variable produced by
        # skipped script code (e.g. `x <- c(total_visits / 1000)`). Evaluate
        # defensively so one such line cannot abort the whole load.
        ok <- tryCatch({ eval(x, envir = envir); TRUE }, error = function(e) FALSE)
        if (!ok) n_skipped <- n_skipped + 1L
      }
    } else {
      sys.source(p, envir = envir, keep.source = FALSE)
    }
  }

  if (isTRUE(functions_only) && n_skipped > 0 && !quiet) {
    .msg_info(sprintf("Loaded legacy definitions; skipped %d top-level script statement(s). Pass functions_only = FALSE to run them.",
                      n_skipped))
  }

  out <- dplyr::bind_rows(shadowed)
  if (nrow(out) > 0) {
    out <- out %>% dplyr::group_by(.data$fn) %>%
      dplyr::slice_tail(n = 1) %>% dplyr::ungroup()
    if (!quiet) {
      .msg_info(sprintf("Loaded %d legacy scripts; %d name(s) were redefined:",
                        length(files), nrow(out)))
      for (i in seq_len(nrow(out))) {
        .msg_info(sprintf("  %-42s -> %s", out$fn[i], out$winner[i]))
      }
    }
  }
  invisible(out)
}

#' Check that the load order delivers the declared canonical implementations
#'
#' @param dir Directory holding the scripts.
#' @param order Load order to check.
#' @param canonical Declared ownership map.
#' @return Tibble of mismatches (empty when the order is correct).
#' @export
check_legacy_canonical <- function(dir = legacy_dir(),
                                   order = LEGACY_LOAD_ORDER,
                                   canonical = LEGACY_CANONICAL) {
  defs <- legacy_definitions(dir)
  rows <- lapply(names(canonical), function(fn) {
    files <- unique(defs$file[defs$fn == fn])
    winner <- files[which.max(match(files, order))]
    if (identical(winner, unname(canonical[[fn]]))) return(NULL)
    tibble::tibble(fn = fn, declared = unname(canonical[[fn]]), actual = winner)
  })
  dplyr::bind_rows(rows)
}
