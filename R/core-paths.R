# External Data Path Resolution ----
#
# Replaces the hardcoded personal paths that were scattered through the legacy
# scripts (`/Users/tylermuffly/Dropbox (Personal)/...`, `~/Dropbox/workforce/...`).
# Twenty-odd literals meant the code ran on exactly one machine, and one of them
# executed at SOURCE time, so simply loading the file failed for anyone else.
#
# Resolution order, first hit wins:
#   1. the `SIMULATION_DATA_ROOT` environment variable;
#   2. `external_data_root:` in config/paths.local.yml (gitignored, per-machine);
#   3. `external_data_root:` in config/paths.yml (committed default);
#   4. `~/Dropbox (Personal)` or `~/Dropbox`, whichever exists.
#
# Nothing here reads a file at load time. Resolution is lazy and callers decide
# whether a missing path is fatal.

.paths_config <- function() {
  candidates <- c(
    file.path(getwd(), "config", "paths.local.yml"),
    file.path(getwd(), "config", "paths.yml")
  )
  out <- list()
  for (p in rev(candidates[file.exists(candidates)])) {
    cfg <- tryCatch(yaml::read_yaml(p), error = function(e) NULL)
    if (is.list(cfg)) out <- utils::modifyList(out, cfg)
  }
  out
}

#' Root directory for external (non-repository) data
#'
#' @param must_work Error when no candidate root exists on this machine.
#' @return Path to the external data root.
#' @keywords internal
simulation_data_root <- function(must_work = FALSE) {
  env <- Sys.getenv("SIMULATION_DATA_ROOT", unset = "")
  if (nzchar(env)) return(normalizePath(env, mustWork = FALSE))

  cfg <- .paths_config()$external_data_root
  if (!is.null(cfg) && nzchar(cfg)) {
    return(normalizePath(path.expand(cfg), mustWork = FALSE))
  }

  fallbacks <- path.expand(c("~/Dropbox (Personal)", "~/Dropbox"))
  hit <- fallbacks[dir.exists(fallbacks)]
  if (length(hit) > 0) return(hit[1])

  if (must_work) {
    stop("No external data root found. Set SIMULATION_DATA_ROOT, or add ",
         "`external_data_root:` to config/paths.local.yml.", call. = FALSE)
  }
  fallbacks[1]
}

#' Resolve a path beneath the external data root
#'
#' @param ... Path components appended to the root.
#' @param must_work Error if the resolved path does not exist.
#' @return Resolved path.
#' @family paths
#' @concept core
#' @export
external_path <- function(..., must_work = FALSE) {
  p <- file.path(simulation_data_root(), ...)
  if (must_work && !file.exists(p)) {
    stop(sprintf("External data file not found: %s\n  Set SIMULATION_DATA_ROOT or edit config/paths.local.yml.", p),
         call. = FALSE)
  }
  p
}

#' Resolve a path inside the SWAN data directory
#'
#' The legacy scripts referenced the SWAN directory eleven different ways. This
#' is the single entry point.
#'
#' @param ... Path components beneath the SWAN directory.
#' @param must_work Error if the resolved path does not exist.
#' @return Resolved path.
#' @family paths
#' @concept core
#' @export
swan_path <- function(..., must_work = FALSE) {
  sub <- .paths_config()$swan_subdir %||% file.path("workforce", "Dall_model", "data", "SWAN")
  external_path(sub, ..., must_work = must_work)
}

#' Resolve a path inside the raw-data directory
#'
#' @param ... Path components beneath the raw-data directory.
#' @param must_work Error if the resolved path does not exist.
#' @return Resolved path.
#' @family paths
#' @concept core
#' @export
data_raw_path <- function(..., must_work = FALSE) {
  sub <- .paths_config()$data_raw_subdir %||% file.path("tyler", "data-raw")
  external_path(sub, ..., must_work = must_work)
}

#' Report which external inputs are reachable on this machine
#'
#' Run before a legacy analysis script rather than discovering a missing file
#' halfway through a long job.
#'
#' @return Tibble of logical name, resolved path, and existence.
#' @family paths
#' @concept core
#' @export
check_external_data <- function() {
  items <- list(
    swan_dir = swan_path(),
    swan_merged = swan_path("merged_longitudinal_data.rds"),
    census_projections = data_raw_path("np2023-t2.xlsx")
  )
  tibble::tibble(
    name = names(items),
    path = unlist(items, use.names = FALSE),
    exists = file.exists(unlist(items, use.names = FALSE))
  )
}


#' Locate a committed artifact by walking up from the working directory
#'
#' The one implementation of the "find `artifacts/<file>` from the package root,
#' from `tests/testthat`, or from a script directory" search. testthat runs with
#' the working directory two levels down, so a root-relative path silently fails
#' there, and a check that cannot read its input must not look like a pass.
#'
#' Returns NULL rather than erroring: `artifacts/` does not ship, so absence is
#' the normal case in an installed package and is not itself a fault.
#'
#' @param ... Path components beneath `artifacts/`.
#' @param start Directory to search upward from.
#' @return Normalized path, or NULL.
#' @family paths
#' @concept core
#' @export
#' @examples
#' artifact_path("does_not_exist.csv")
artifact_path <- function(..., start = ".") {
  for (p in c(start, file.path(start, ".."), file.path(start, "..", ".."))) {
    f <- file.path(p, "artifacts", ...)
    if (file.exists(f)) return(normalizePath(f))
  }
  NULL
}
