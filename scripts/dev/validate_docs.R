#!/usr/bin/env Rscript
# No-duckdb documentation validator, run from the repository root ----
#
#   Rscript scripts/dev/validate_docs.R
#
# WHY THIS EXISTS. The R-CMD-check gate uses error-on: "warning", and the three
# consecutive merges that reached main in a failing state did so on exactly the
# defects R CMD check reports only after a full build: an .Rd whose \usage no
# longer matches the function formals (codoc), an @export tag with no matching
# NAMESPACE entry, an export() with no man page, and malformed Rd. Locally those
# are invisible, because roxygenising and R CMD check both load the package, and
# the package imports duckdb, which is not installed on every contributor's
# machine (and blocks a plain `make document`). So the checks that catch the
# main-reddening class of defect never run before the push that triggers them.
#
# This script reproduces that specific class of check WITHOUT loading the
# package: it only parses the R sources and the generated man/*.Rd. It needs
# base R and nothing else, so it runs anywhere `Rscript` does. It is NOT a
# substitute for R CMD check -- it does not run examples, tests, or the full
# codoc pass -- it is the pre-push tripwire for the three defects that keep
# reaching main.
#
# It fails (exit 1) on: an unparseable / structurally invalid .Rd; an
# @export'd object absent from NAMESPACE; an export() with no documenting .Rd;
# and an exported function whose formals disagree with its \usage.
#
# WHEN IT DISAGREES WITH A FRESH roxygenise(), roxygenise() is right and the
# committed .Rd/NAMESPACE are stale -- regenerate them. This script reads what
# is committed, which is exactly what CI builds.

`%||%` <- function(a, b) if (is.null(a) || length(a) == 0L) b else a

R_DIR   <- "R"
MAN_DIR <- "man"
NS_FILE <- "NAMESPACE"

fail <- function(section, items) {
  cat(sprintf("\n==== %s (%d) ====\n", section, length(items)))
  for (x in items) cat("  ", x, "\n", sep = "")
}

problems <- 0L

# ---- 0. Preconditions -------------------------------------------------------
stopifnot("run from the repository root" =
            dir.exists(R_DIR) && dir.exists(MAN_DIR) && file.exists(NS_FILE))

r_files  <- list.files(R_DIR, pattern = "[.][Rr]$", full.names = TRUE)
rd_files <- list.files(MAN_DIR, pattern = "[.]Rd$", full.names = TRUE)

# ---- 1. Every .Rd is structurally valid (checkRd) ---------------------------
# checkRd tags each finding with a severity in parentheses, "checkRd: (N) ...".
# N >= 5 is a warning or error (the class R CMD check fails on under error-on:
# warning); lower levels are notes. "non-ASCII" notes are excluded outright:
# they are governed by `Encoding: UTF-8` in DESCRIPTION, which a per-file
# checkRd cannot see, so flagging them here is a false positive.
rd_fatal <- function(m) {
  s <- paste(as.character(m), collapse = " ")
  if (grepl("non-ASCII", s, fixed = TRUE)) return(FALSE)
  lvl <- suppressWarnings(as.integer(sub(".*checkRd: \\((-?[0-9]+)\\).*", "\\1", s)))
  is.na(lvl) || lvl >= 5L                        # unparseable level -> treat as fatal
}
rd_bad <- character(0)
for (f in rd_files) {
  # checkRd RETURNS its findings as a character vector (class "checkRd"); a
  # genuine parse failure THROWS. Distinguish by condition class, not
  # is.character() -- the findings vector is itself character.
  res <- tryCatch(tools::checkRd(f), error = function(e) e)
  if (inherits(res, "condition")) {              # parse threw
    rd_bad <- c(rd_bad, sprintf("%s: %s", basename(f), conditionMessage(res)))
  } else if (length(res)) {                      # checkRd findings
    errs <- Filter(rd_fatal, res)
    if (length(errs))
      rd_bad <- c(rd_bad, sprintf("%s: %s", basename(f),
                                  paste(vapply(errs, as.character, ""), collapse = "; ")))
  }
}
if (length(rd_bad)) { problems <- problems + length(rd_bad); fail("INVALID Rd", rd_bad) }

# ---- Helpers: parse committed sources without evaluating them ---------------

# All top-level `name <- function(...)` definitions across R/, as name -> formals.
# parse() does not evaluate, so no dependency (duckdb included) is loaded.
source_formals <- list()
for (f in r_files) {
  exprs <- tryCatch(parse(f, keep.source = FALSE),
                    error = function(e) { cat("PARSE ERROR", basename(f), conditionMessage(e), "\n"); NULL })
  for (e in exprs) {
    if (is.call(e) && length(e) == 3L &&
        (identical(e[[1]], as.name("<-")) || identical(e[[1]], as.name("="))) &&
        is.call(e[[3]]) && identical(e[[3]][[1]], as.name("function"))) {
      nm <- if (is.name(e[[2]])) as.character(e[[2]]) else next
      source_formals[[nm]] <- names(e[[3]][[2]]) %||% character(0)
    }
  }
}

# NAMESPACE directives (parsed, not evaluated): export(), S3method(), etc.
ns_exprs <- parse(NS_FILE, keep.source = FALSE)
ns_exports  <- character(0)
ns_s3       <- character(0)
for (e in ns_exprs) {
  if (!is.call(e)) next
  d <- as.character(e[[1]])
  if (d == "export")   ns_exports <- c(ns_exports, vapply(as.list(e)[-1], as.character, ""))
  if (d == "S3method") ns_s3      <- c(ns_s3, paste(as.character(e[[2]]), as.character(e[[3]]), sep = "."))
}
ns_exports <- unique(ns_exports)

# roxygen @export tags in the sources: the object named on the next code line.
rox_exports <- character(0)
for (f in r_files) {
  ln <- readLines(f, warn = FALSE)
  ex <- grep("^#'\\s*@export\\s*$", ln)          # bare @export only (skip @exportS3Method etc.)
  for (i in ex) {
    j <- i + 1L
    while (j <= length(ln) && grepl("^\\s*#'", ln[j])) j <- j + 1L   # skip further roxygen
    if (j > length(ln)) next
    m <- regmatches(ln[j], regexec("^\\s*\"?([A-Za-z.][A-Za-z0-9._]*)\"?\\s*(<-|=)", ln[j]))[[1]]
    if (length(m) >= 2L) rox_exports <- c(rox_exports, m[2])
  }
}
rox_exports <- unique(rox_exports)

# Man aliases: every \alias{} across man/, so an export can be matched to a page.
man_aliases <- unique(unlist(lapply(rd_files, function(f) {
  ln <- readLines(f, warn = FALSE)
  regmatches(ln, regexec("^\\\\alias\\{(.+)\\}", ln)) |>
    (\(m) vapply(m, function(x) if (length(x) >= 2L) x[2] else NA_character_, ""))() |>
    (\(x) x[!is.na(x)])()
})))

# ---- 2. @export  <->  NAMESPACE export() ------------------------------------
# roxygen tagged it, but the committed NAMESPACE does not export it: a stale
# NAMESPACE. This is the "tags missing from NAMESPACE" failure named in the
# R-CMD-check workflow header.
tagged_not_exported <- setdiff(rox_exports, c(ns_exports, ns_s3))
# Exclude names that are S3 methods (generic.class) whose generic is exported;
# these are declared via S3method(), already covered by ns_s3.
tagged_not_exported <- tagged_not_exported[!tagged_not_exported %in% ns_s3]
if (length(tagged_not_exported)) {
  problems <- problems + length(tagged_not_exported)
  fail("@export NOT IN NAMESPACE (regenerate NAMESPACE)", tagged_not_exported)
}

# ---- 3. export()  ->  has a man page ----------------------------------------
# Operator re-exports (%>% and friends) are conventionally documented under a
# shared reexports page or not at all; exclude non-syntactic names so they do
# not read as undocumented.
checkable_exports <- ns_exports[grepl("^[A-Za-z.]", ns_exports)]
exported_undocumented <- setdiff(checkable_exports, man_aliases)
if (length(exported_undocumented)) {
  problems <- problems + length(exported_undocumented)
  fail("EXPORTED BUT UNDOCUMENTED (no \\alias)", exported_undocumented)
}

# ---- 4. codoc-lite: exported function formals == its \usage args ------------
# The defect: an .Rd \usage that lists arguments the function no longer has (or
# omits new ones). R CMD check reports this as a codoc mismatch and, under
# error-on: warning, fails. We compare argument-name SETS, which is what codoc
# keys on; defaults and order are not checked here.
usage_args <- function(rd_path, fn) {
  ln <- paste(readLines(rd_path, warn = FALSE), collapse = "\n")
  block <- regmatches(ln, regexec("\\\\usage\\{(.*?)\\n\\}", ln))[[1]]
  if (length(block) < 2L) return(NULL)
  usage <- block[2]
  # Isolate the call whose head is `fn`: from "fn(" to its matching ")".
  start <- regexpr(sprintf("(^|\\n)\\s*%s\\s*\\(", gsub("([.\\\\])", "\\\\\\1", fn)), usage)
  if (start < 0) return(NULL)
  s <- start + attr(start, "match.length") - 1L   # index of "("
  depth <- 0L; end <- NA_integer_
  for (k in s:nchar(usage)) {
    ch <- substr(usage, k, k)
    if (ch == "(") depth <- depth + 1L
    if (ch == ")") { depth <- depth - 1L; if (depth == 0L) { end <- k; break } }
  }
  if (is.na(end)) return(NULL)
  call_txt <- substr(usage, s, end)               # "(...)" including parens
  # A function needs a body to parse; " NULL" supplies one. The shell is only
  # inspected with formals() and never called, so the body is inert.
  f <- tryCatch(eval(parse(text = paste0("function", call_txt, " NULL"))),
                error = function(e) NULL)
  if (is.null(f)) return(NULL)
  names(formals(f)) %||% character(0)
}

codoc_bad <- character(0)
for (fn in intersect(ns_exports, names(source_formals))) {
  rd <- file.path(MAN_DIR, paste0(fn, ".Rd"))
  if (!file.exists(rd)) {
    # aliased under a different page; find the page carrying the alias.
    hit <- rd_files[vapply(rd_files, function(f)
      any(grepl(sprintf("^\\\\alias\\{%s\\}", gsub("([.\\\\])", "\\\\\\1", fn)),
                readLines(f, warn = FALSE))), logical(1))]
    if (!length(hit)) next
    rd <- hit[1]
  }
  ua <- usage_args(rd, fn)
  if (is.null(ua)) next                            # no parseable usage for fn
  fa <- source_formals[[fn]]
  if (!setequal(ua, fa)) {
    codoc_bad <- c(codoc_bad, sprintf(
      "%s: \\usage(%s) != formals(%s)",
      fn, paste(setdiff(ua, fa), collapse = ","), paste(setdiff(fa, ua), collapse = ",")))
  }
}
if (length(codoc_bad)) { problems <- problems + length(codoc_bad); fail("CODOC MISMATCH", codoc_bad) }

# ---- Verdict ----------------------------------------------------------------
if (problems == 0L) {
  cat(sprintf("OK: %d man pages, %d exports, %d exported functions checked -- no doc drift.\n",
              length(rd_files), length(ns_exports), length(intersect(ns_exports, names(source_formals)))))
  quit(status = 0)
}
cat(sprintf("\n%d documentation problem(s). These fail R CMD check --as-cran (error-on: warning).\n", problems))
quit(status = 1)
