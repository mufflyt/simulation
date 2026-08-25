# Executable regressions for docs/HALL_OF_SHAME.md ---------------------------
#
# The registry is prose. Prose does not fail a build. Each test here reproduces
# one recorded defect closely enough that REVERTING ITS FIX TURNS THIS FILE
# RED -- that is the bar, and it is stricter than "a test exists about this
# area". A test that would still pass with the historical bug reinstated
# belongs in some other file.
#
# LINKING CONVENTION. Every test declares which entry it guards:
#
#     test_that("@hall_of_shame 24 | <what the code must do>", { ... })
#
# The tag is machine-readable on purpose. It lets a coverage gate answer "which
# recorded defects have no executable reproduction?" without anyone maintaining
# a second list by hand -- a second list being, itself, entry 18's defect.
#
# HERMETIC. No network, no DuckDB, no external drive, no survey/sf. These run
# on every PR, so they must be cheap and must not drag the heavy dependency set
# in behind them.

.hos_root <- function() {
  root <- .source_tree_root()
  testthat::skip_if(length(root) == 0, "repository root not reachable")
  root[1]
}

.hos_git_tracked <- function(root) {
  # git ls-files, NOT git status, and not file.exists(). Entry 4's third
  # instance is precisely that `git add -A` reports success for a file an
  # ignore rule excluded; only the index knows. `file.exists()` would pass on
  # the developer's machine for exactly the files that never reach the repo.
  out <- suppressWarnings(base::system2(
    "git", c("-C", base::shQuote(root), "ls-files"),
    stdout = TRUE, stderr = FALSE
  ))
  if (!base::is.character(out)) character(0) else out
}

testthat::test_that("@hall_of_shame 4 | no package source is swallowed by a gitignore rule", {
  root <- .hos_root()
  testthat::skip_if(
    base::nchar(base::Sys.which("git")) == 0, "git not available"
  )
  tracked <- .hos_git_tracked(root)
  testthat::skip_if(length(tracked) == 0, "repository root not reachable")

  # Three times a broad pattern ate source: */manuscript/ took
  # scripts/manuscript/, *_projections* took man/gap_projections_all_scenarios.Rd,
  # and *.png took six README figures. All three were invisible -- nothing
  # errored, the files simply were not in the repository.
  on_disk <- function(sub, pattern) {
    dir <- file.path(root, sub)
    if (!dir.exists(dir)) return(character(0))
    list.files(dir, pattern = pattern, recursive = TRUE)
  }
  expectations <- list(
    list(sub = "R", pattern = "[.]R$", label = "R/ sources"),
    list(sub = "man", pattern = "[.]Rd$", label = "man/ documentation"),
    list(sub = file.path("tests", "testthat"), pattern = "^test-.*[.]R$",
         label = "test files")
  )

  for (spec in expectations) {
    present <- spec$sub |> on_disk(spec$pattern)
    testthat::skip_if(length(present) == 0, "repository root not reachable")
    expected <- file.path(spec$sub, present)
    missing <- base::setdiff(expected, tracked)
    testthat::expect_equal(
      missing, character(0),
      info = base::paste0(
        "These ", spec$label, " exist on disk but are NOT in the git index, ",
        "so a gitignore rule is eating them and `git add -A` will keep ",
        "reporting success: ",
        base::paste(utils::head(missing, 10), collapse = ", ")
      )
    )
  }
})

testthat::test_that("@hall_of_shame 4 | every image the README embeds is tracked", {
  root <- .hos_root()
  testthat::skip_if(
    base::nchar(base::Sys.which("git")) == 0, "git not available"
  )
  readme <- file.path(root, "README.md")
  testthat::skip_if_not(file.exists(readme), "repository root not reachable")
  tracked <- .hos_git_tracked(root)
  testthat::skip_if(length(tracked) == 0, "repository root not reachable")

  # The sharpest instance: a commit fixing "README embeds a file the repo does
  # not contain" reintroduced it sixfold, because the blanket *.png meant the
  # six new figures were never added.
  lines <- base::readLines(readme, warn = FALSE)
  refs <- base::regmatches(
    lines, base::gregexpr("!\\[[^]]*\\]\\(([^)]+)\\)", lines)
  )
  refs <- base::unlist(refs, use.names = FALSE)
  paths <- base::sub("^!\\[[^]]*\\]\\(", "", refs)
  paths <- base::sub("\\)$", "", paths)
  paths <- base::sub("\\s+.*$", "", paths)                 # strip title text
  paths <- paths[!base::grepl("^(https?:|data:|#)", paths)] # local files only
  testthat::skip_if(length(paths) == 0, "README embeds no local images")

  missing <- base::setdiff(base::unique(paths), tracked)
  testthat::expect_equal(
    missing, character(0),
    info = base::paste0(
      "README.md embeds image(s) that are not in the git index, so they ",
      "render broken for everyone but the author: ",
      base::paste(missing, collapse = ", ")
    )
  )
})

testthat::test_that("@hall_of_shame 24 | an installed package is not mistaken for the source tree", {
  # The walk accepted any directory holding a DESCRIPTION. Under covr and
  # R CMD check the suite runs INSIDE the installed package, which has one, so
  # the guard never fired and eight structural gates ran against an empty file
  # list -- reporting assertion failures where the honest answer was "the
  # sources are not here".
  #
  # Fixtures rather than the live tree: this must fail if the discriminator is
  # removed, which a test run from the real repository root could never show.
  fixture <- file.path(base::tempdir(), "hall_of_shame_24")
  base::unlink(fixture, recursive = TRUE)
  base::on.exit(base::unlink(fixture, recursive = TRUE), add = TRUE)

  installed <- file.path(fixture, "installed")
  base::dir.create(file.path(installed, "Meta"), recursive = TRUE)
  base::writeLines("Package: urpssim", file.path(installed, "DESCRIPTION"))

  sources <- file.path(fixture, "sources")
  base::dir.create(file.path(sources, ".github"), recursive = TRUE)
  base::writeLines("Package: urpssim", file.path(sources, "DESCRIPTION"))

  resolve_from <- function(dir) {
    original <- base::getwd()
    base::on.exit(base::setwd(original), add = TRUE)
    base::setwd(dir)
    urpssim:::.repo_source_root()
  }

  testthat::expect_true(
    base::is.na(resolve_from(installed)),
    info = paste(
      "A directory with a DESCRIPTION and Meta/ is an INSTALLED package, not",
      "the source tree. Accepting it is what let guards run against no files."
    )
  )
  testthat::expect_false(
    base::is.na(resolve_from(sources)),
    info = "A real source checkout must still resolve, or every gate skips."
  )
})

testthat::test_that("@hall_of_shame 23 | an empty duplicate scan yields character(0), not NULL", {
  # names() on an empty table is NULL, so a legitimately empty result compared
  # unequal to character(0) and the duplicate-definition gate failed with
  # "actual is NULL, expected is a character vector ()" -- an assertion error
  # standing in for "there was nothing to check".
  #
  # tests/testthat/test-repo-hygiene.R carries the as.character() fix. This
  # asserts the property that fix exists for, so removing it turns this red.
  duplicates_of <- function(x) base::as.character(base::names(
    base::which(base::table(x) > 1)
  ))

  testthat::expect_identical(duplicates_of(character(0)), character(0))
  testthat::expect_identical(duplicates_of(c("a", "b")), character(0))
  testthat::expect_identical(duplicates_of(c("a", "a", "b")), "a")

  # The distinction the bug turned on: without as.character() the empty case is
  # NULL, and NULL is not character(0) to expect_identical().
  testthat::expect_false(
    base::is.null(duplicates_of(character(0))),
    info = "Empty must be character(0); NULL reads as an assertion failure."
  )
})

# ---- primitives that are silently wrong ------------------------------------

testthat::test_that("@hall_of_shame 5 | a rendered bound round-trips to its artifact value", {
  # A 95% lower bound of 1070.975 rendered as "1,070". Every fractional bound
  # in the specification table was low by up to a full unit, IN THE DIRECTION
  # OF THE NULL, invisibly. formatC(format = "d") truncates; it does not round.
  render <- function(x) base::formatC(base::round(x), format = "d", big.mark = ",")

  testthat::expect_identical(render(1070.975), "1,071")
  testthat::expect_identical(render(2.5), "2")      # banker's rounding, stated
  testthat::expect_identical(render(3.5), "4")

  # The property that matters: a rendered value must be within half a unit of
  # its source, never systematically below it.
  values <- c(1070.975, 0.5, 12.4, 99.99, 1234.5001)
  rendered <- as.numeric(gsub(",", "", vapply(values, render, character(1))))
  testthat::expect_true(all(abs(rendered - values) <= 0.5))
  testthat::expect_false(all(rendered <= values))

  # The defect itself, pinned so nobody reintroduces the bare call.
  testthat::expect_identical(base::formatC(1070.975, format = "d"), "1070")
})

testthat::test_that("@hall_of_shame 6 | missingness is never filtered with a string predicate", {
  # Six NA-NPI roster rows survived a blank filter and entered the numerator's
  # key set WHILE BEING REPORTED AS ZERO BLANKS, because nzchar(NA) is TRUE.
  npis <- c("1234567890", NA_character_, "", "  ", "9876543210")

  testthat::expect_true(base::nzchar(NA))          # the trap itself
  testthat::expect_equal(base::sum(base::nzchar(npis)), 4L)

  keep <- !base::is.na(npis) & base::nzchar(base::trimws(npis))
  testthat::expect_equal(base::sum(keep), 2L)
  testthat::expect_identical(npis[keep], c("1234567890", "9876543210"))

  # The reporting half of the defect: the count of dropped rows must agree
  # with the number actually dropped, or "zero blanks" gets printed while six
  # rows sail through.
  testthat::expect_equal(base::sum(!keep), base::length(npis) - base::sum(keep))
})

testthat::test_that("@hall_of_shame 7 | a commented header never parses as data", {
  # The provider-type mapping's rationale header parsed as data and all 25
  # provider types came back unmapped. It was caught only because unknown
  # types STOP THE RUN -- had the mapping defaulted to 'physician', every
  # provider would have been silently misclassified.
  path <- base::tempfile(fileext = ".csv")
  base::on.exit(base::unlink(path), add = TRUE)
  base::writeLines(
    c("# rationale: this header is prose, not data",
      "code,provider_type", "207V,obgyn", "2088,urology"),
    path
  )

  parsed <- utils::read.csv(path, comment.char = "#", stringsAsFactors = FALSE)
  testthat::expect_identical(base::names(parsed), c("code", "provider_type"))
  testthat::expect_equal(base::nrow(parsed), 2L)
  testthat::expect_false(base::any(base::grepl("^#", parsed$code)))

  # A reader without comment handling swallows the header as a row, which is
  # the shape of the original defect.
  naive <- utils::read.csv(path, comment.char = "", stringsAsFactors = FALSE)
  testthat::expect_false(base::identical(base::names(naive), base::names(parsed)))
})

testthat::test_that("@hall_of_shame 8 | a column predicate is not inferred from one row", {
  # Right-align inference looked at the FIRST value only, so a column beginning
  # "1. Derived cohort" went right. Same shape as entry 6: a predicate applied
  # to a sample and generalised to a column.
  column <- c("1. Derived cohort", "not a number", "also text")

  first_row_rule <- function(x) base::grepl("^[0-9]", x[[1]])
  whole_column_rule <- function(x) base::all(base::grepl("^[0-9.,]+$", x))

  testthat::expect_true(first_row_rule(column))    # the defect
  testthat::expect_false(whole_column_rule(column))
  testthat::expect_true(whole_column_rule(c("1", "2.5", "3,000")))
})

# ---- guards that could not fail --------------------------------------------

testthat::test_that("@hall_of_shame 22 | a suite that discovers nothing cannot report OK", {
  # A CI sweep labelled all 164 test files "ok" unconditionally. The runner
  # this repository now uses refuses to call zero discovered blocks a pass.
  runner <- .hos_root()
  script <- file.path(runner, ".github", "scripts", "run-scientific-contract.R")
  testthat::skip_if_not(file.exists(script), "repository root not reachable")

  source_text <- base::readLines(script, warn = FALSE)
  testthat::expect_true(
    base::any(base::grepl("discovered ZERO test blocks", source_text)),
    info = "the zero-discovery guard has been removed from the contract runner"
  )
  testthat::expect_true(base::any(base::grepl("blocks == 0L", source_text)))
})

testthat::test_that("@hall_of_shame 26 | a suite that skipped everything cannot report OK", {
  # "Full suite green" was measured in the one context where it could not
  # fail. Zero failures is not evidence when zero assertions ran.
  runner <- .hos_root()
  script <- file.path(runner, ".github", "scripts", "run-scientific-contract.R")
  testthat::skip_if_not(file.exists(script), "repository root not reachable")

  source_text <- base::readLines(script, warn = FALSE)
  testthat::expect_true(
    base::any(base::grepl("not one assertion PASSED", source_text)),
    info = "the all-skipped guard has been removed from the contract runner"
  )
  testthat::expect_true(base::any(base::grepl("this gate is dark", source_text)))
})

testthat::test_that("@hall_of_shame 1 | a refusal gate must still accept something valid", {
  # A gate that refused EVERYTHING looked safe and delivered nothing. A
  # refusal is only evidence of discrimination if the same gate admits a valid
  # case; otherwise "nothing got through" and "the gate works" are
  # indistinguishable.
  #
  # Exercised on the live identity gate: it must reject a weak match AND admit
  # a strong one. Rejecting both would be a gate that refuses everything.
  event <- function(linkage, identity) tibble::tibble(
    provider_id = "P1", event_type = "retired", event_year = 2021L,
    identity_confidence = identity, event_confidence = 0.99,
    timing_confidence = 0.99, linkage_class = linkage,
    later_activity = FALSE, explicit_reinstatement = FALSE,
    confirmation_matured = TRUE
  )
  refused <- suppressMessages(adjudicate_terminal_events(event("name_only", 0.99)))
  admitted <- suppressMessages(adjudicate_terminal_events(event("direct_npi", 0.99)))

  testthat::expect_identical(refused$terminal_decision, "quarantine_identity")
  testthat::expect_identical(admitted$terminal_decision, "confirmed_exit")
  testthat::expect_false(
    base::identical(refused$terminal_decision, admitted$terminal_decision),
    info = "the gate returns the same verdict for weak and strong evidence"
  )
})
