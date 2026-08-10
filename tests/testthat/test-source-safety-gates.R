# Source-safety gates for the package's own code.
#
# These sit alongside test-repo-hygiene.R (duplicate defs, hardcoded paths,
# install.packages, source-time reads) and test-export-wiring.R (unwired
# exports). Each one below forbids a class of defect the hardening work kept
# meeting by hand -- an empty-range loop that reverses, a library() side effect
# in code that should only ever be imported from, a sapply() whose return shape
# nobody pinned. A gate is cheaper than finding the next one by accident.
#
# WHY INSPECT THE LOADED NAMESPACE, NOT R/ SOURCE. The repo-hygiene gates read
# the source tree and therefore skip inside <pkg>.Rcheck/, spending the
# "repository root not reachable" skip budget. These walk the abstract syntax
# tree of every function in the loaded `urpssim` namespace instead, so they run
# identically under R CMD check (installed pkg), scripts/ci/check_suite.R
# (load_all), and a plain test_check -- and never go dark. The trade is that
# they see function bodies only, which is exactly where these defects live (the
# top-level-executable-code gate in repo-hygiene already forbids the rest).

# ---- AST helpers ------------------------------------------------------------

# Every call node inside a language object. The missing() guard is load-bearing:
# an empty argument slot (the column of `df[i, ]`) is the empty symbol, and
# forcing it raises "argument is missing" -- so we detect and skip it rather
# than recurse into it.
.ssg_calls_in <- function(expr) {
  out <- list()
  rec <- function(e) {
    if (missing(e)) return(invisible())
    if (is.call(e)) {
      out[[length(out) + 1L]] <<- e
      for (k in seq_along(e)) rec(e[[k]])
    } else if (is.pairlist(e)) {
      for (k in seq_along(e)) rec(e[[k]])
    }
  }
  rec(expr)
  out
}

.ssg_head <- function(cl) {
  if (is.call(cl) && (is.symbol(cl[[1]]) || is.character(cl[[1]]))) as.character(cl[[1]])
  else NA_character_
}

# `1:length(x)` and its siblings. When x is empty, length(x) is 0 and 1:0 is
# c(1L, 0L) -- the loop runs twice on nonexistent indices instead of zero times.
# seq_len()/seq_along() are the fix. Matches `:` whose left side is the literal
# 1 and whose right side is a call to a size function.
.ssg_bad_ranges <- function(body) {
  size_fns <- c("length", "nrow", "ncol", "NROW", "NCOL", "nlevels")
  Filter(function(cl) {
    if (!identical(.ssg_head(cl), ":")) return(FALSE)
    lhs <- cl[[2]]; rhs <- cl[[3]]
    is.numeric(lhs) && length(lhs) == 1L && lhs == 1 &&
      is.call(rhs) && (.ssg_head(rhs) %in% size_fns)
  }, .ssg_calls_in(body))
}

.ssg_calls_named <- function(body, names) {
  Filter(function(cl) .ssg_head(cl) %in% names, .ssg_calls_in(body))
}

.ssg_qualified_pkgs <- function(body) {
  hits <- Filter(function(cl) .ssg_head(cl) %in% c("::", ":::"), .ssg_calls_in(body))
  vapply(hits, function(cl) as.character(cl[[2]]), character(1))
}

# The package's OWN functions (closure environment is the namespace), keyed by
# name so a failure can name the offender. Imported functions live elsewhere and
# are excluded -- we do not gate dplyr's code.
.ssg_own_functions <- function() {
  ns <- asNamespace("urpssim")
  nms <- ls(ns, all.names = TRUE)
  own <- Filter(function(n) {
    obj <- tryCatch(get(n, envir = ns), error = function(e) NULL)
    is.function(obj) && identical(environment(obj), ns)
  }, nms)
  stats::setNames(lapply(own, function(n) body(get(n, envir = ns))), own)
}

# For each own function, collect the offenders a detector returns; report as
# "fn (n)" so the message points straight at the code.
.ssg_scan <- function(detector) {
  bodies <- .ssg_own_functions()
  hits <- vapply(bodies, function(b) length(detector(b)), integer(1))
  offenders <- hits[hits > 0]
  if (length(offenders)) paste0(names(offenders), " (", offenders, ")") else character(0)
}

# ---- the gates --------------------------------------------------------------

test_that("no package function builds a 1:length() / 1:nrow() range", {
  offenders <- .ssg_scan(.ssg_bad_ranges)
  expect_equal(offenders, character(0),
               info = paste("Use seq_len()/seq_along(); 1:n reverses when n is 0. In:",
                            paste(offenders, collapse = ", ")))
})

test_that("no package function performs global-state side effects", {
  # Library code is imported from, never library()'d; it must not mutate the
  # working directory, environment variables, the search path, or output sinks.
  # requireNamespace() (the guarded optional-dependency idiom) is deliberately
  # NOT in this list -- only the bare state-mutating forms are.
  banned <- c("library", "require", "setwd", "attach",
              "Sys.setenv", "Sys.unsetenv", "sink")
  offenders <- .ssg_scan(function(b) .ssg_calls_named(b, banned))
  expect_equal(offenders, character(0),
               info = paste("Global-state side effect in library code:",
                            paste(offenders, collapse = ", ")))
})

test_that("no package function uses sapply (return shape is unpinned)", {
  # sapply simplifies to a vector, a matrix, or a list depending on its input,
  # so a caller that expects one silently gets another on an edge case. vapply
  # states the shape and errors instead. This is the silent-wrong-result class.
  offenders <- .ssg_scan(function(b) .ssg_calls_named(b, "sapply"))
  expect_equal(offenders, character(0),
               info = paste("Prefer vapply() to sapply() in:", paste(offenders, collapse = ", ")))
})

test_that("every declared Import is actually used", {
  # The complement of repo-hygiene's "declared Imports cover the namespaces the
  # package calls": that fails on an UNDECLARED dependency, this fails on a DEAD
  # one -- a package carried in DESCRIPTION that no code references, which
  # bloats the install and lies about what the package needs.
  desc <- read.dcf(system.file("DESCRIPTION", package = "urpssim"))
  imports <- sub("\\s*\\(.*", "", trimws(unlist(strsplit(desc[, "Imports"], ","))))
  base_pkgs <- c("stats", "utils", "tools", "datasets", "splines", "methods",
                 "grDevices", "graphics", "grid", "parallel", "compiler")
  declared <- setdiff(imports, base_pkgs)

  imported <- names(getNamespaceImports("urpssim"))
  in_body <- unlist(lapply(.ssg_own_functions(), .ssg_qualified_pkgs))
  referenced <- unique(c(imported, in_body))

  dead <- setdiff(declared, referenced)
  expect_equal(dead, character(0),
               info = paste("Declared in Imports but referenced nowhere:",
                            paste(dead, collapse = ", ")))
})

test_that("the source-safety detectors fire on planted violations", {
  # A gate that cannot fail guards nothing (cf. repo-hygiene's detector self-
  # test). Prove each detector flags a known-bad body and passes a clean one.
  clean  <- function(x) { for (i in seq_along(x)) x[i] <- x[i] + 1L; x }
  bad_rng <- function(x) { for (i in 1:length(x)) x[i] <- 0L; x }
  bad_rng2 <- function(df) { for (i in 1:nrow(df)) df[i, ] <- NA; df }
  bad_side <- function(x) { library(stats); setwd(tempdir()); x }
  bad_sapp <- function(x) sapply(x, sqrt)

  expect_length(.ssg_bad_ranges(body(clean)), 0L)
  expect_length(.ssg_bad_ranges(body(bad_rng)), 1L)
  expect_length(.ssg_bad_ranges(body(bad_rng2)), 1L)
  expect_length(.ssg_calls_named(body(clean), c("library", "setwd", "sapply")), 0L)
  expect_length(.ssg_calls_named(body(bad_side), c("library", "setwd", "sapply")), 2L)
  expect_length(.ssg_calls_named(body(bad_sapp), "sapply"), 1L)
})
