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
