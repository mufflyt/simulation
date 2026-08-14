# The manuscript-eligibility gate ----
#
# WHY THESE ARE HERE. scripts/manuscript/build_tables.R enforces one rule: a
# manuscript table may not be built from a source that is exploratory, a
# fallback, failed, incomplete or otherwise non-citable. That rule protects the
# paper's numbers, and a rule of that weight cannot rest on having been read
# once. Two directions must both hold, and only one of them is obvious:
#
#   REFUSE what is ineligible  -- the obvious direction.
#   ADMIT what is eligible     -- the direction that actually broke. The first
#     version of the gate free-text scanned MANIFEST.txt and refused all five
#     authoritative run directories, because every manifest carries the line
#     `exploratory   FALSE` and the scan matched its own field name. A gate that
#     refuses everything looks safe and delivers nothing.
#
# Fixtures are synthesised in tempdir() rather than read from
# artifacts/validation/, which is untracked and absent from a fresh clone.

# find_root() ERRORS when no DESCRIPTION ancestor exists, and this runs at top
# level, so under R CMD check -- which copies tests to a temp tree without the
# source root -- it aborted the file before any skip could apply. Resolve it
# tolerantly and skip instead, the same shape test-export-wiring.R uses.
root <- tryCatch(rprojroot::find_root(rprojroot::has_file("DESCRIPTION")),
                 error = function(e) NULL)
skip_if(is.null(root), "repository root not reachable (source tree absent under R CMD check)")
skip_if_not(file.exists(file.path(root, "scripts", "manuscript", "_eligibility.R")))
source(file.path(root, "scripts", "validation", "_provenance.R"), local = TRUE)
source(file.path(root, "scripts", "manuscript", "_eligibility.R"), local = TRUE)

# A minimal run directory of the shape begin_validation_run() produces.
fake_run <- function(dir, exploratory = FALSE, completed = TRUE,
                     clean = TRUE, table = data.frame(verdict = "PASS", n = 1L)) {
  dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  writeLines(c(
    sprintf("analysis             %s", basename(dir)),
    sprintf("tree_clean_model     %s", if (clean) "TRUE" else "FALSE"),
    sprintf("exploratory          %s", if (exploratory) "TRUE" else "FALSE"),
    "status               started",
    sprintf("run_id               %s", basename(dir))),
    file.path(dir, "MANIFEST.txt"))
  if (completed)
    writeLines(c("completed  now",
                 sprintf("status     %s", if (exploratory)
                   "EXPLORATORY -- not citable" else "authoritative_awaiting_reproduction")),
               file.path(dir, "COMPLETED"))
  if (exploratory)
    writeLines("EXPLORATORY RUN -- NOT CITABLE.", file.path(dir, "EXPLORATORY"))
  utils::write.csv(table, file.path(dir, "results.csv"), row.names = FALSE)
  dir
}

test_that("an authoritative run directory is ADMITTED", {
  g <- gate_run(fake_run(file.path(tempfile(), "20260808T000000_demo_abc1234")))
  expect_true(g$ok)
  expect_length(g$problems, 0)
  expect_equal(g$run_id, "20260808T000000_demo_abc1234")
})

test_that("`exploratory FALSE` in a manifest does not read as ineligibility", {
  # The exact regression that made the gate refuse every real source.
  m <- c("exploratory          FALSE", "status               started")
  expect_length(scan_disqualifying(unname(parse_manifest(m)), "manifest value"), 0)
  expect_equal(unname(parse_manifest(m)[["exploratory"]]), "FALSE")
})

test_that("an EXPLORATORY run is refused", {
  g <- gate_run(fake_run(file.path(tempfile(), "20260808T000000_demo_abc1234_EXPLORATORY"),
                         exploratory = TRUE))
  expect_false(g$ok)
  expect_match(paste(g$problems, collapse = " "), "EXPLORATORY", ignore.case = TRUE)
})

test_that("a run with no COMPLETED marker is refused", {
  g <- gate_run(fake_run(file.path(tempfile(), "20260808T000000_demo_abc1234"),
                         completed = FALSE))
  expect_false(g$ok)
  expect_match(paste(g$problems, collapse = " "), "COMPLETED")
})

test_that("a run launched from a dirty model tree is refused", {
  g <- gate_run(fake_run(file.path(tempfile(), "20260808T000000_demo_abc1234"), clean = FALSE))
  expect_false(g$ok)
  expect_match(paste(g$problems, collapse = " "), "dirty")
})

test_that("a manifest that declares exploratory TRUE is refused even without the marker", {
  d <- fake_run(file.path(tempfile(), "20260808T000000_demo_abc1234"))
  writeLines(c("tree_clean_model     TRUE", "exploratory          TRUE",
               "run_id               20260808T000000_demo_abc1234"),
             file.path(d, "MANIFEST.txt"))
  g <- gate_run(d)
  expect_false(g$ok)
  expect_match(paste(g$problems, collapse = " "), "exploratory = TRUE")
})

test_that("a disqualifying tag in the DATA is refused, not just in the metadata", {
  # The demand back-test case: a clean-looking run whose own column says the
  # anchors were illustrative. Nothing about the run directory is wrong.
  g <- gate_run(fake_run(file.path(tempfile(), "20260808T000000_demo_abc1234"),
                         table = data.frame(mape = 3.58,
                                            anchors_source = "illustrative_fallback")))
  expect_false(g$ok)
  expect_match(paste(g$problems, collapse = " "), "illustrative|fallback")
})

test_that("a FAIL verdict is content, not a status tag", {
  # n = 250 not converging IS the Monte-Carlo finding. If `failed` were
  # broadened to match `FAIL`, table 3 would become unbuildable.
  expect_length(scan_disqualifying(c("FAIL", "PASS"), "verdict"), 0)
  g <- gate_run(fake_run(file.path(tempfile(), "20260808T000000_demo_abc1234"),
                         table = data.frame(n = c(250L, 1000L), verdict = c("FAIL", "PASS"))))
  expect_true(g$ok)
})

test_that("a flat CSV with no sidecar manifest is not evidence", {
  csv <- tempfile(fileext = ".csv")
  utils::write.csv(data.frame(x = 1), csv, row.names = FALSE)
  g <- gate_pinned(csv, tempfile(fileext = ".json"))
  expect_false(g$ok)
  expect_match(paste(g$problems, collapse = " "), "no sidecar manifest")
})

test_that("a pinned artifact whose manifest omits required fields is refused", {
  csv <- tempfile(fileext = ".csv"); js <- tempfile(fileext = ".json")
  utils::write.csv(data.frame(x = 1), csv, row.names = FALSE)
  writeLines(jsonlite::toJSON(list(generated_by = "x.R"), auto_unbox = TRUE), js)
  g <- gate_pinned(csv, js)
  expect_false(g$ok)
  expect_match(paste(g$problems, collapse = " "), "omits required field")
})

test_that("a fully declared pinned artifact is ADMITTED", {
  csv <- tempfile(fileext = ".csv"); js <- tempfile(fileext = ".json")
  utils::write.csv(data.frame(arm = "derived", predicted = 1207), csv, row.names = FALSE)
  writeLines(jsonlite::toJSON(list(
    generated_by = "scripts/run_backtest_2020_to_2023.R", cutoff_year = 2020L,
    target_year = 2023L, n_iterations = 1000L, seed = 20260802L,
    target_value = 1306L), auto_unbox = TRUE), js)
  g <- gate_pinned(csv, js)
  expect_true(g$ok)
  expect_match(g$run_id, "seed=20260802")
})

test_that("every disqualifying token is matched case-insensitively", {
  unmatched <- Filter(function(tok) length(scan_disqualifying(toupper(tok), "t")) == 0L,
                      DISQUALIFYING)
  expect_equal(unmatched, character())
})
