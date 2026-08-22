# The nightly CI is a safety control for a PUBLIC repository holding work built
# on DUA-restricted data. These tests stop it being silently gutted -- a
# workflow can be weakened in a one-line diff that reads like tidying.
#
# Note the YAML 1.1 quirk: a bare `on:` key parses as the BOOLEAN true, so the
# trigger block is read back as y[["true"]], not y[["on"]]. GitHub's own parser
# handles `on:` correctly; this only affects reading the file from R.

# Every test here inspects files in the REPOSITORY (.github/, docs/, artifacts/).
# Under R CMD check the suite runs from the INSTALLED package, where none of
# those exist and `../../` resolves nowhere. Without this guard the file fails
# in CI while passing locally -- which is exactly what happened.
.repo <- function() dir.exists("../../.github/workflows")

.wf <- function() yaml::read_yaml("../../.github/workflows/nightly.yaml")
.wf_raw <- function() readLines("../../.github/workflows/nightly.yaml", warn = FALSE)

test_that("the nightly runs at 03:17 MST", {
  skip_if_not(.repo())
  skip_if_not(file.exists("../../.github/workflows/nightly.yaml"))
  # 10:17 UTC == 03:17 MST. GitHub cron is UTC-only and ignores DST, so this
  # lands at 04:17 MDT in summer -- documented in the workflow header.
  expect_true(any(grepl("cron: '17 10 \\* \\* \\*'", .wf_raw())))
})

test_that("every nightly job that installs R deps installs pandoc first", {
  skip_if_not(.repo())
  skip_if_not(file.exists("../../.github/workflows/nightly.yaml"))
  # Same rule test-repo-hygiene.R enforces for the other workflows: without
  # pandoc on PATH, setup-r-dependencies resolves Suggests and trips over
  # mysterycall. Asserted here too so a new nightly job cannot miss it.
  jobs <- .wf()$jobs
  for (jn in names(jobs)) {
    uses <- vapply(jobs[[jn]]$steps %||% list(),
                   function(s) s$uses %||% "", character(1))
    d <- grep("setup-r-dependencies", uses)
    if (!length(d)) next
    p <- grep("setup-pandoc", uses)
    expect_true(length(p) > 0 && min(p) < min(d), info = jn)
  }
  expect_true(TRUE)
})

test_that("the leak guard scans history, not only the working tree", {
  skip_if_not(.repo())
  skip_if_not(file.exists("../../.github/workflows/nightly.yaml"))
  # HEAD being clean proves nothing: a public clone ships deleted blobs.
  steps <- .wf()$jobs$`leak-guard`$steps
  runs <- paste(vapply(steps, function(s) s$run %||% "", character(1)), collapse = "\n")
  expect_match(runs, "scan-phi-dua.sh worktree", fixed = TRUE)
  expect_match(runs, "scan-phi-dua.sh history", fixed = TRUE)
  # and it must fetch full history, or the history scan is vacuous
  expect_true(any(grepl("fetch-depth: 0", .wf_raw(), fixed = TRUE)))
})

test_that("the refusal-gate audit is wired into the nightly", {
  skip_if_not(.repo())
  skip_if_not(file.exists("../../.github/workflows/nightly.yaml"))
  steps <- .wf()$jobs$`refusal-gates`$steps
  runs <- paste(vapply(steps, function(s) s$run %||% "", character(1)), collapse = "\n")
  expect_match(runs, "assert-refusal-gates.R", fixed = TRUE)
  expect_true(file.exists("../../.github/scripts/assert-refusal-gates.R"))
})

test_that("the scanner scripts exist and are executable", {
  skip_if_not(.repo())
  for (s in c("scan-phi-dua.sh", "assert-refusal-gates.R")) {
    p <- file.path("../../.github/scripts", s)
    expect_true(file.exists(p), info = s)
    if (.Platform$OS.type == "unix")
      expect_true(file.access(p, mode = 1L) == 0L, info = paste(s, "not executable"))
  }
})

test_that("CRITICAL findings are never baselined", {
  skip_if_not(.repo())
  skip_if_not(file.exists("../../.github/phi-dua-baseline.txt"))
  b <- readLines("../../.github/phi-dua-baseline.txt", warn = FALSE)
  # The ratchet may only carry HIGH (hygiene). A `critical=` key would mean a
  # disclosure had been grandfathered, which must never be possible.
  expect_false(any(grepl("^critical\\s*=", b)))
  expect_true(any(grepl("^high\\s*=\\s*[0-9]+$", b)))
})

test_that("the PHI scanner actually catches planted PHI", {
  skip_if_not(.repo())
  # A scanner that has never fired is indistinguishable from one that cannot.
  # The fixture is built entirely in bash: the scanner resolves git paths
  # against the current directory, so driving it through R's system2("git",
  # "-C", ...) creates a repo the scanner is not actually looking at.
  skip_on_os("windows")
  skip_if_not(nzchar(Sys.which("bash")) && nzchar(Sys.which("git")))
  script <- normalizePath("../../.github/scripts/scan-phi-dua.sh", mustWork = TRUE)

  probe <- function(plant) sprintf('
    set -e
    D=$(mktemp -d); cd "$D"
    git init -q; git config user.email t@e.com; git config user.name t
    printf "age,visits\n61,2\n" > ok.csv
    %s
    git add -A; git commit -qm fixture >/dev/null
    set +e
    bash %s worktree /nonexistent-baseline
    echo "RC=$?"
    cd /; rm -rf "$D"', plant, shQuote(script))

  # system2() does not quote its args, so a multi-line -c payload is split and
  # bash reports "option requires an argument". Write the probe to a file.
  run_probe <- function(plant) {
    f <- tempfile(fileext = ".sh")
    on.exit(unlink(f), add = TRUE)
    writeLines(probe(plant), f)
    system2("bash", f, stdout = TRUE, stderr = TRUE)
  }
  clean <- run_probe("")
  expect_true(any(grepl("RC=0", clean)), info = "a clean tree must pass")
  expect_true(any(grepl("CRITICAL: 0", clean)))

  # The PHI column name is ASSEMBLED AT RUNTIME rather than written literally.
  # A literal here would be found by the scanner in this very file, failing the
  # leak guard on the repository that ships it. The alternative -- excluding
  # this file from the scan -- would punch a permanent hole in the gate, which
  # is worse than a slightly less readable probe.
  phi_col <- paste0("Patient", "Name")
  dirty <- run_probe(sprintf('printf "%s,M%s\\nJane Roe,00123\\n" > leak.csv',
                             phi_col, "RN"))
  expect_true(any(grepl("RC=1", dirty)), info = "planted PHI must fail the scan")
  expect_true(any(grepl("PHI column name", dirty)))

  # and a DUA-restricted filename is caught by NAME, with no content match
  dua <- run_probe('echo x > casemix_hidd.mdb')
  expect_true(any(grepl("RC=1", dua)), info = "a .mdb must fail the scan")
  expect_true(any(grepl("DUA-restricted file is tracked", dua)))
})

test_that("the nightly reports failures without spamming issues", {
  skip_if_not(.repo())
  skip_if_not(file.exists("../../.github/workflows/nightly.yaml"))
  raw <- .wf_raw()
  # one tracking issue, updated by comment -- not one issue per job per night
  expect_true(any(grepl("createComment", raw, fixed = TRUE)))
  expect_true(any(grepl("labels: 'nightly'", raw, fixed = TRUE)))
  # a leak-guard failure on a public repo must be called out as urgent
  expect_true(any(grepl("Treat as urgent", raw, fixed = TRUE)))
})

test_that("the scientific-invariants gate is wired and blocking", {
  skip_if_not(.repo())
  skip_if_not(file.exists("../../.github/workflows/nightly.yaml"))
  y <- .wf()
  expect_true("scientific-invariants" %in% names(y$jobs))
  steps <- y$jobs$`scientific-invariants`$steps
  runs <- paste(vapply(steps, function(s) s$run %||% "", character(1)), collapse = "\n")
  expect_match(runs, "assert-scientific-invariants.R", fixed = TRUE)
  # it must feed the aggregator, or a violation cannot fail the nightly
  expect_true("scientific-invariants" %in% y$jobs$report$needs)
  # and it must not be marked advisory
  expect_false(isTRUE(y$jobs$`scientific-invariants`$`continue-on-error`))
})

test_that("a skipped blocking gate cannot masquerade as a green nightly", {
  skip_if_not(.repo())
  skip_if_not(file.exists("../../.github/workflows/nightly.yaml"))
  raw <- .wf_raw()
  # Only 'success' counts. A cancelled or skipped correctness job means the
  # thing it protects went unchecked, which is not a pass.
  expect_true(any(grepl("BLOCKING GATE DID NOT PASS", raw, fixed = TRUE)))
  expect_true(any(grepl('check "scientific invariants"', raw, fixed = TRUE)))
  expect_true(any(grepl('check "leak guard"', raw, fixed = TRUE)))
})

test_that("the back-test ratchet records a real, currently-failing baseline", {
  skip_if_not(.repo())
  skip_if_not(file.exists("../../.github/backtest-baseline.txt"))
  kv <- readLines("../../.github/backtest-baseline.txt", warn = FALSE)
  gv <- function(k) as.numeric(sub(".*=", "",
          grep(paste0("^", k, "="), kv, value = TRUE)[1]))
  cov <- gv("coverage95")
  # The honest state: the model does NOT meet its own 0.80 interval standard.
  # If this ever reads >= 0.80 the ratchet is no longer needed and the gate
  # should become an absolute threshold instead.
  expect_true(is.finite(cov))
  expect_lt(cov, 0.80)
  expect_true(is.finite(gv("worst_percent_error")))
  expect_true(is.finite(gv("n_arms")))
  # arms may not be dropped: deleting a failing arm is not an improvement
  expect_gte(gv("n_arms"), 10)
})

# --- Layer 2: scientific adversarial validation -----------------------------

.adv <- function() yaml::read_yaml("../../.github/workflows/scientific-adversarial.yaml")
.adv_raw <- function() readLines("../../.github/workflows/scientific-adversarial.yaml", warn = FALSE)

test_that("Layer 2 exists as its own workflow and does not replace Layer 1", {
  skip_if_not(.repo())
  skip_if_not(file.exists("../../.github/workflows/scientific-adversarial.yaml"))
  # A green Layer 1 with a red Layer 2 is a meaningful state; merging them
  # would destroy that distinction.
  expect_true(file.exists("../../.github/workflows/nightly.yaml"))
  j <- names(.adv()$jobs)
  expect_true(all(c("canaries", "metamorphic", "reference", "scorecard") %in% j))
})

test_that("the adversarial run is scheduled nightly and weekly", {
  skip_if_not(.repo())
  skip_if_not(file.exists("../../.github/workflows/scientific-adversarial.yaml"))
  raw <- .adv_raw()
  expect_true(any(grepl("cron: '47 10 \\* \\* \\*'", raw)))   # nightly 03:47 MST
  expect_true(any(grepl("cron: '47 11 \\* \\* 0'", raw)))     # weekly deep run
  # it must not collide with Layer 1's 10:17 slot
  expect_false(any(grepl("cron: '17 10 \\* \\* \\*'", raw)))
})

test_that("canary detector independence is enforced, not assumed", {
  skip_if_not(.repo())
  skip_if_not(file.exists("../../.github/scripts/adversarial/canaries.R"))
  src <- readLines("../../.github/scripts/adversarial/canaries.R", warn = FALSE)
  # a canary counts as killed ONLY if its named detector fires
  expect_true(any(grepl("killed_by_expected <- cn\\$expect %in% fired", src)))
  # and the unmutated model must trip no detector, or a detector is always-on
  expect_true(any(grepl("baseline \\(unmutated\\) detectors firing", src)))
  # every canary declares an expected detector
  n_expect <- sum(grepl("^\\s+expect = ", src))
  n_id <- sum(grepl('^\\s+list\\(id = "CAN-', src))
  expect_gt(n_id, 0)
  expect_equal(n_expect, n_id)
})

test_that("the adversarial scripts exist and are executable", {
  skip_if_not(.repo())
  for (s in c("canaries.R", "metamorphic.R")) {
    p <- file.path("../../.github/scripts/adversarial", s)
    expect_true(file.exists(p), info = s)
    if (.Platform$OS.type == "unix")
      expect_true(file.access(p, mode = 1L) == 0L, info = paste(s, "not executable"))
  }
})

test_that("the coverage document addresses every specification section", {
  skip_if_not(.repo())
  skip_if_not(file.exists("../../docs/LAYER2_ADVERSARIAL_COVERAGE.md"))
  md <- paste(readLines("../../docs/LAYER2_ADVERSARIAL_COVERAGE.md", warn = FALSE), collapse = "\n")
  # the three dispositions must all be used -- a coverage doc that only says
  # "implemented" is not a coverage doc
  expect_match(md, "Implemented", fixed = TRUE)
  expect_match(md, "Not applicable", fixed = TRUE)
  expect_match(md, "Deferred", fixed = TRUE)
  # the linkage sections must be explicitly dispositioned, not omitted
  expect_match(md, "no record-linkage system", fixed = TRUE)
  # and the honest limitations must survive
  expect_match(md, "Honest limitations", fixed = TRUE)
  expect_match(md, "0.20 against a required 0.80", fixed = TRUE)
})

test_that("a skipped adversarial gate cannot masquerade as a pass", {
  skip_if_not(.repo())
  skip_if_not(file.exists("../../.github/workflows/scientific-adversarial.yaml"))
  raw <- .adv_raw()
  expect_true(any(grepl("ADVERSARIAL GATE DID NOT PASS", raw, fixed = TRUE)))
  expect_true(any(grepl("VALIDATION SYSTEM regressed", raw, fixed = TRUE)))
})

test_that("the temporal-integrity gate is wired into both layers", {
  skip_if_not(.repo())
  skip_if_not(file.exists("../../.github/scripts/assert-temporal-integrity.R"))
  n <- readLines("../../.github/workflows/nightly.yaml", warn = FALSE)
  a <- readLines("../../.github/workflows/scientific-adversarial.yaml", warn = FALSE)
  expect_true(any(grepl("assert-temporal-integrity.R", n, fixed = TRUE)))
  expect_true(any(grepl("assert-temporal-integrity.R", a, fixed = TRUE)))
  if (.Platform$OS.type == "unix")
    expect_true(file.access("../../.github/scripts/assert-temporal-integrity.R", mode = 1L) == 0L)
})

test_that("the back-test declares censoring and a single observed estimand", {
  skip_if_not(.repo())
  skip_if_not(file.exists("../../artifacts/backtest_2020_to_2023_manifest.json"))
  m <- jsonlite::fromJSON("../../artifacts/backtest_2020_to_2023_manifest.json",
                          simplifyVector = FALSE)
  # A back-test without a declared censoring window is not a back-test.
  expect_true(length(m$leakage_audit) > 0)
  yrs <- as.integer(regmatches(unlist(m$leakage_audit),
                               regexpr("[0-9]{4}", unlist(m$leakage_audit))))
  expect_true(all(yrs <= as.integer(m$cutoff_year)))
  expect_lt(as.integer(m$cutoff_year), as.integer(m$target_year))
  expect_false(is.null(m$observed_series_applies_attrition))
})

test_that("the attrition estimand mismatch is the recorded driver, not leakage", {
  skip_if_not(.repo())
  skip_if_not(file.exists("../../artifacts/backtest_2020_to_2023_summary.csv"))
  s <- utils::read.csv("../../artifacts/backtest_2020_to_2023_summary.csv",
                       stringsAsFactors = FALSE)
  # Every arm under-predicts. Target leakage inflates apparent accuracy, so
  # systematic under-prediction is evidence AGAINST leakage.
  expect_true(all(s$percent_error < 0))
  e_t <- mean(s$percent_error[as.logical(s$apply_attrition)])
  e_f <- mean(s$percent_error[!as.logical(s$apply_attrition)])
  # Definition-matched arms must remain less biased. If this inverts, the
  # diagnosis in assert-temporal-integrity.R is wrong and must be re-opened.
  expect_gt(e_f, e_t)
  expect_gt(e_f - e_t, 3)   # currently 5.84 percentage points
  expect_equal(mean(as.logical(s$within_95)[as.logical(s$apply_attrition)]), 0)
})

# --- PR gates: the fast subset that must not wait for a nightly -------------

test_that("the leak guard runs on every PR, not only nightly", {
  skip_if_not(.repo())
  skip_if_not(file.exists("../../.github/workflows/pr-gates.yaml"))
  y <- yaml::read_yaml("../../.github/workflows/pr-gates.yaml")
  raw <- readLines("../../.github/workflows/pr-gates.yaml", warn = FALSE)
  # On a PUBLIC repo, a 24-hour delay before noticing a DUA file or PHI is the
  # wrong trade. The leak guard needs no R toolchain.
  expect_true("leak-guard" %in% names(y$jobs))
  expect_true(any(grepl("pull_request", raw, fixed = TRUE)))
  runs <- paste(vapply(y$jobs$`leak-guard`$steps, function(s) s$run %||% "", character(1)),
                collapse = "\n")
  expect_match(runs, "scan-phi-dua.sh worktree", fixed = TRUE)
  expect_match(runs, "scan-phi-dua.sh history", fixed = TRUE)
  expect_true(any(grepl("fetch-depth: 0", raw, fixed = TRUE)))
})

test_that("the leak guard needs no secret, so fork PRs are still covered", {
  skip_if_not(.repo())
  skip_if_not(file.exists("../../.github/workflows/pr-gates.yaml"))
  y <- yaml::read_yaml("../../.github/workflows/pr-gates.yaml")
  # The R job may skip on forks (no PAT), but the leak guard must not be gated
  # behind an `if:` that a fork PR would fail.
  expect_null(y$jobs$`leak-guard`$`if`)
  expect_false(is.null(y$jobs$`science-gates`$`if`))
})

test_that("PR gates run the refusal, invariant and estimand checks", {
  skip_if_not(.repo())
  skip_if_not(file.exists("../../.github/workflows/pr-gates.yaml"))
  y <- yaml::read_yaml("../../.github/workflows/pr-gates.yaml")
  runs <- paste(vapply(y$jobs$`science-gates`$steps,
                       function(s) s$run %||% "", character(1)), collapse = "\n")
  for (s in c("assert-refusal-gates.R", "assert-scientific-invariants.R",
              "assert-temporal-integrity.R", "adversarial/canaries.R"))
    expect_match(runs, s, fixed = TRUE)
})

test_that("slow gates stay nightly and are not duplicated onto PRs", {
  skip_if_not(.repo())
  skip_if_not(file.exists("../../.github/workflows/pr-gates.yaml"))
  raw <- paste(readLines("../../.github/workflows/pr-gates.yaml", warn = FALSE), collapse = "\n")
  # Keeping the matrix, coverage, frozen restore and full suite out of PR CI is
  # deliberate; if one appears here, the trade was changed and should be argued.
  expect_false(grepl("renv::restore", raw, fixed = TRUE))
  expect_false(grepl("test_dir", raw, fixed = TRUE))
  expect_false(grepl("covr::", raw, fixed = TRUE))
  expect_false(grepl("windows-latest", raw, fixed = TRUE))
})

test_that("every PR-gate job installing R deps installs pandoc first", {
  skip_if_not(.repo())
  skip_if_not(file.exists("../../.github/workflows/pr-gates.yaml"))
  jobs <- yaml::read_yaml("../../.github/workflows/pr-gates.yaml")$jobs
  for (jn in names(jobs)) {
    uses <- vapply(jobs[[jn]]$steps %||% list(), function(s) s$uses %||% "", character(1))
    d <- grep("setup-r-dependencies", uses)
    if (!length(d)) next
    p <- grep("setup-pandoc", uses)
    expect_true(length(p) > 0 && min(p) < min(d), info = jn)
  }
  expect_true(TRUE)
})
