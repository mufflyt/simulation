#!/usr/bin/env Rscript
# Full suite + skip budget, run from the repository root ----
#
#   Rscript scripts/ci/check_suite.R
#
# TWO THINGS R CMD CHECK CANNOT DO, which is why this exists alongside it.
#
# 1. RUN THE GATES THAT NEED THE SOURCE TREE. R CMD check runs tests inside
#    <pkg>.Rcheck/, where config/, data-raw/ and artifacts/ are not present --
#    they are .Rbuildignore'd, and data-raw/urps_roster deliberately so, because
#    the extract carries NPIs. Every test that looks for the repository root
#    therefore skips itself. That silently disabled the frozen back-test record
#    drift gate (6 tests) and the mufflyaccess contract pin (3 tests) -- two of
#    the guards this repository most depends on -- in CI, while the summary line
#    reported them as fine.
#
# 2. TELL A SKIPPED GATE FROM A PASSING ONE. `FAIL 0 | SKIP 66 | PASS 2337` is
#    indistinguishable from `FAIL 0 | SKIP 0 | PASS 2403` at a glance, and the
#    difference is 66 assertions nobody is making. The roster-dependent
#    coordinate tests skipped for months -- including the by-pathway coverage
#    assertion that keeps the urology-at-0% hole closed -- and nothing said so.
#
# So this script fails the build when a test fails, AND when the skip inventory
# does not match tests/skip-budget.csv. A skip is legitimate; an UNDECLARED skip
# is a gate going dark.
#
# WHEN THIS FAILS ON A NEW SKIP, the fix is almost never to add a row. Ask first
# whether the skip condition should hold at all: "sf not installed" in CI means
# install sf, not declare that the spatial guards do not run.

suppressWarnings(suppressMessages(pkgload::load_all(".", quiet = TRUE)))

BUDGET <- "tests/skip-budget.csv"

res <- testthat::test_dir("tests/testthat", stop_on_failure = FALSE,
                          reporter = "silent")
df <- as.data.frame(res)

# ---- Failures ---------------------------------------------------------------
bad <- df[df$failed > 0 | df$error, , drop = FALSE]
if (nrow(bad)) {
  cat("\n==== FAILURES ====\n")
  for (i in seq_len(nrow(bad))) cat(sprintf("  %s :: %s\n", bad$file[i], bad$test[i]))
}

# ---- Skip inventory ---------------------------------------------------------
rows <- list()
for (i in seq_len(nrow(df))) {
  if (!isTRUE(df$skipped[i])) next
  for (x in res[[i]]$results) {
    if (!inherits(x, "expectation_skip")) next
    reason <- trimws(sub("\n.*$", "", conditionMessage(x)))
    reason <- sub("^Reason: ", "", reason)
    rows[[length(rows) + 1L]] <- data.frame(file = df$file[i], test = df$test[i],
                                            reason = reason, stringsAsFactors = FALSE)
  }
}
inv <- if (length(rows)) do.call(rbind, rows) else
  data.frame(file = character(), test = character(), reason = character())

cat(sprintf("\nfiles %d  tests %d  failed %d  skipped %d\n",
            length(unique(df$file)), nrow(df), nrow(bad), nrow(inv)))

budget <- utils::read.csv(BUDGET, stringsAsFactors = FALSE, comment.char = "#")
matched <- rep(NA_integer_, nrow(inv))
for (b in seq_len(nrow(budget))) {
  hit <- is.na(matched) & grepl(budget$reason_pattern[b], inv$reason)
  matched[hit] <- b
}

cat("\n==== SKIP INVENTORY ====\n")
for (b in seq_len(nrow(budget))) {
  n <- sum(matched == b, na.rm = TRUE)
  flag <- if (n > budget$max_tests[b]) " <-- OVER BUDGET" else ""
  cat(sprintf("%3d / %-3d  %-46s%s\n", n, budget$max_tests[b],
              budget$reason_pattern[b], flag))
}

problems <- character()
over <- budget$reason_pattern[vapply(seq_len(nrow(budget)),
  function(b) sum(matched == b, na.rm = TRUE) > budget$max_tests[b], logical(1))]
if (length(over)) {
  problems <- c(problems, sprintf(
    "%d declared skip reason(s) exceeded their budget: %s. More tests went dark than %s allows.",
    length(over), paste(over, collapse = "; "), BUDGET))
}

undeclared <- inv[is.na(matched), , drop = FALSE]
if (nrow(undeclared)) {
  cat("\n==== UNDECLARED SKIPS ====\n")
  for (i in seq_len(nrow(undeclared)))
    cat(sprintf("  %-44s %s\n     %s\n", undeclared$file[i], undeclared$test[i],
                undeclared$reason[i]))
  problems <- c(problems, sprintf(
    "%d test(s) skipped for a reason not declared in %s.", nrow(undeclared), BUDGET))
}

if (nrow(bad)) problems <- c(problems, sprintf("%d test(s) failed.", nrow(bad)))

if (length(problems)) {
  cat("\n")
  for (p in problems) cat("ERROR: ", p, "\n", sep = "")
  quit(status = 1L)
}
cat("\nSuite clean and every skip declared.\n")
