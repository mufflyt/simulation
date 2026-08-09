#!/usr/bin/env Rscript
# Function-name overlap with the sibling repositories ----
#
#   Rscript scripts/dev/audit_canonical_overlap.R
#
# WHY THIS IS A SCRIPT AND NOT A ONE-OFF. docs/CANONICAL_SOURCES_AUDIT.md took
# three passes. The first two asked "does this suspected function have a twin?"
# and both produced confident wrong answers -- the second pass concluded
# `wilson_ci` "appears in no sibling package" from a targeted check of three
# repositories, and `isochrones` had one all along. Only the third pass
# enumerated the full namespace intersection first, and it immediately found two
# byte-identical copies of installed exports that two rounds of suspicion had
# walked past.
#
# A finding produced by an exhaustive method decays back to anecdote the moment
# the method stops being run. This is the method, kept runnable.
#
# WHAT IT DOES NOT DO. It matches on NAME. The same computation under two
# different names is invisible to it -- which is exactly how `wilson_ci` and
# `calculate_proportion_ci` coexisted in this package undetected. A clean run
# means no NEW unclassified collision, not an absence of duplication.

`%||%` <- function(a, b) if (is.null(a)) b else a

SIBLINGS <- c("isochrones", "twostep", "cliff", "mufflyaccess",
              "mysterymaps", "mysterycall")

# Sibling checkouts sit beside this one. Resolved relative to the repository
# rather than hardcoded to a home directory, so the audit runs from a clone in
# any location -- and reports honestly when a sibling is simply not there.
sibling_repo_paths <- function(root) {
  parent <- dirname(normalizePath(root, mustWork = FALSE))
  p <- file.path(parent, SIBLINGS)
  names(p) <- SIBLINGS
  p[dir.exists(p)]
}

# Top-level definitions only. A function defined inside another is not part of
# any namespace and cannot be the thing a caller reached for by mistake, so
# indented definitions are deliberately excluded by the ^ anchor.
top_level_defs <- function(dir) {
  if (!dir.exists(dir)) return(character())
  files <- list.files(dir, pattern = "[.][Rr]$", recursive = TRUE, full.names = TRUE)
  if (!length(files)) return(character())
  lines <- unlist(lapply(files, readLines, warn = FALSE), use.names = FALSE)
  hits <- grep("^[A-Za-z._][A-Za-z0-9._]* *<- *function", lines, value = TRUE)
  sort(unique(sub(" *<- *function.*$", "", hits)))
}

# R/ if the sibling is a package; the whole tree otherwise. isochrones is a
# project rather than a package (one export, >1,300 files), so its R/ is still
# the right place to look but its names carry less weight -- recorded in the
# audit, not compensated for here.
repo_defs <- function(path) {
  top_level_defs(if (dir.exists(file.path(path, "R"))) file.path(path, "R") else path)
}

#' Every (sibling, function) pair whose name is also defined in this repository
canonical_overlap <- function(root = ".") {
  mine <- top_level_defs(file.path(root, "R"))
  paths <- sibling_repo_paths(root)
  out <- lapply(names(paths), function(nm) {
    shared <- intersect(mine, repo_defs(paths[[nm]]))
    if (!length(shared)) return(NULL)
    data.frame(sibling = nm, fn = shared, stringsAsFactors = FALSE)
  })
  out <- do.call(rbind, out)
  if (is.null(out)) out <- data.frame(sibling = character(), fn = character(),
                                      stringsAsFactors = FALSE)
  out[order(out$fn, out$sibling), , drop = FALSE]
}

REGISTRY <- function(root = ".") file.path(root, "tests", "canonical-overlap-registry.csv")

read_overlap_registry <- function(root = ".") {
  utils::read.csv(REGISTRY(root), comment.char = "#", stringsAsFactors = FALSE)
}

#' Compare the live intersection against the registry
#'
#' @return list of `unclassified` (collisions with no registry row -- the gate
#'   condition) and `stale` (registry rows that no longer collide).
overlap_status <- function(root = ".") {
  live <- canonical_overlap(root)
  reg <- read_overlap_registry(root)
  key <- function(d) paste(d$sibling, d$fn, sep = "\r")
  list(live = live, registry = reg,
       unclassified = live[!key(live) %in% key(reg), , drop = FALSE],
       stale = reg[!key(reg) %in% key(live), , drop = FALSE],
       missing_siblings = setdiff(SIBLINGS, names(sibling_repo_paths(root))))
}

# ---- Report, only when invoked directly ------------------------------------
#
# The test sources this file for the functions above and must not trigger a
# report as a side effect.
.invoked_directly <- function() {
  f <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
  length(f) == 1L && basename(sub("^--file=", "", f)) == "audit_canonical_overlap.R"
}

if (.invoked_directly()) {
  st <- overlap_status(".")
  cat("\n=== sibling repositories found ===\n")
  p <- sibling_repo_paths(".")
  for (nm in names(p)) cat(sprintf("  %-13s %s\n", nm, p[[nm]]))
  if (length(st$missing_siblings))
    cat("  NOT PRESENT:", paste(st$missing_siblings, collapse = ", "), "\n")

  cat("\n=== name collisions by sibling ===\n")
  print(as.data.frame(table(sibling = st$live$sibling)), row.names = FALSE)

  cat("\n=== classification of the registered collisions ===\n")
  print(as.data.frame(table(classification = st$registry$classification)), row.names = FALSE)

  unexamined <- st$registry[st$registry$classification == "unexamined", ]
  if (nrow(unexamined)) {
    cat("\n=== still unexamined (honest, but not a resting state) ===\n")
    print(unexamined[, c("fn", "sibling")], row.names = FALSE)
  }

  if (nrow(st$stale)) {
    cat("\n=== STALE registry rows (no longer collide) ===\n")
    print(st$stale[, c("fn", "sibling")], row.names = FALSE)
  }

  if (nrow(st$unclassified)) {
    cat("\n=== NEW UNCLASSIFIED COLLISIONS ===\n")
    print(st$unclassified, row.names = FALSE)
    cat("\nOpen each, compare it, and add a row to tests/canonical-overlap-registry.csv\n",
        "with the classification that fits. `unexamined` is allowed and is not a\n",
        "way of dismissing one -- it is a promise recorded in a file.\n", sep = "")
  } else {
    cat("\nNo new unclassified collisions.\n")
  }
}
