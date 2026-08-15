# The nightly CI is a safety control for a PUBLIC repository holding work built
# on DUA-restricted data. These tests stop it being silently gutted -- a
# workflow can be weakened in a one-line diff that reads like tidying.
#
# Note the YAML 1.1 quirk: a bare `on:` key parses as the BOOLEAN true, so the
# trigger block is read back as y[["true"]], not y[["on"]]. GitHub's own parser
# handles `on:` correctly; this only affects reading the file from R.

.wf <- function() yaml::read_yaml("../../.github/workflows/nightly.yaml")
.wf_raw <- function() readLines("../../.github/workflows/nightly.yaml", warn = FALSE)

test_that("the nightly runs at 03:17 MST", {
  skip_if_not(file.exists("../../.github/workflows/nightly.yaml"))
  # 10:17 UTC == 03:17 MST. GitHub cron is UTC-only and ignores DST, so this
  # lands at 04:17 MDT in summer -- documented in the workflow header.
  expect_true(any(grepl("cron: '17 10 \\* \\* \\*'", .wf_raw())))
})

test_that("every nightly job that installs R deps installs pandoc first", {
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
})

test_that("the leak guard scans history, not only the working tree", {
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
  skip_if_not(file.exists("../../.github/workflows/nightly.yaml"))
  steps <- .wf()$jobs$`refusal-gates`$steps
  runs <- paste(vapply(steps, function(s) s$run %||% "", character(1)), collapse = "\n")
  expect_match(runs, "assert-refusal-gates.R", fixed = TRUE)
  expect_true(file.exists("../../.github/scripts/assert-refusal-gates.R"))
})

test_that("the scanner scripts exist and are executable", {
  for (s in c("scan-phi-dua.sh", "assert-refusal-gates.R")) {
    p <- file.path("../../.github/scripts", s)
    expect_true(file.exists(p), info = s)
    if (.Platform$OS.type == "unix")
      expect_true(file.access(p, mode = 1L) == 0L, info = paste(s, "not executable"))
  }
})

test_that("CRITICAL findings are never baselined", {
  skip_if_not(file.exists("../../.github/phi-dua-baseline.txt"))
  b <- readLines("../../.github/phi-dua-baseline.txt", warn = FALSE)
  # The ratchet may only carry HIGH (hygiene). A `critical=` key would mean a
  # disclosure had been grandfathered, which must never be possible.
  expect_false(any(grepl("^critical\\s*=", b)))
  expect_true(any(grepl("^high\\s*=\\s*[0-9]+$", b)))
})

test_that("the PHI scanner actually catches planted PHI", {
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

  dirty <- run_probe('printf "PatientName,MRN\nJane Roe,00123\n" > leak.csv')
  expect_true(any(grepl("RC=1", dirty)), info = "planted PHI must fail the scan")
  expect_true(any(grepl("PHI column name", dirty)))

  # and a DUA-restricted filename is caught by NAME, with no content match
  dua <- run_probe('echo x > casemix_hidd.mdb')
  expect_true(any(grepl("RC=1", dua)), info = "a .mdb must fail the scan")
  expect_true(any(grepl("DUA-restricted file is tracked", dua)))
})

test_that("the nightly reports failures without spamming issues", {
  skip_if_not(file.exists("../../.github/workflows/nightly.yaml"))
  raw <- .wf_raw()
  # one tracking issue, updated by comment -- not one issue per job per night
  expect_true(any(grepl("createComment", raw, fixed = TRUE)))
  expect_true(any(grepl("labels: 'nightly'", raw, fixed = TRUE)))
  # a leak-guard failure on a public repo must be called out as urgent
  expect_true(any(grepl("Treat as urgent", raw, fixed = TRUE)))
})
