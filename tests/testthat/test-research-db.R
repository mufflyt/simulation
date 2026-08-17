# Fail-closed access to a removable research database.
#
# The defect these guard against: ENTRY_PANEL_DB_DEFAULT held a path that did
# not exist, and DuckDB CREATES a database from a missing path -- so "missing
# input" silently became "zero observed events". Resolving the mount fixed half
# of that. These pin the other half: absence must REFUSE, not return empty.

test_that("a required database that is absent STOPS", {
  expect_error(
    resolve_research_db("DuckDB/definitely_not_here.duckdb",
                        volume_pattern = "NoSuchVolume*",
                        env_var = "URPS_TEST_DB_ABSENT",
                        required = TRUE, what = "test db"),
    "unavailable")
})

test_that("the refusal explains WHY silence would be dangerous", {
  # A reader who sees this error must understand that skipping would have
  # produced zeroes, not an error. The message carries that.
  err <- tryCatch(
    resolve_research_db("DuckDB/nope.duckdb", "NoSuchVolume*",
                        "URPS_TEST_DB_ABSENT", required = TRUE, what = "test db"),
    error = conditionMessage)
  expect_match(err, "DuckDB would create an empty database", ignore.case = TRUE)
  expect_match(err, "URPS_TEST_DB_ABSENT")
})

test_that("an OPTIONAL database that is absent skips with a stated reason", {
  expect_message(
    p <- resolve_research_db("DuckDB/nope.duckdb", "NoSuchVolume*",
                             "URPS_TEST_DB_ABSENT", required = FALSE,
                             what = "optional db"),
    "SKIPPING \\(optional\\)")
  expect_true(is.na(p))
})

test_that("an env override pointing at a nonexistent file still fails closed", {
  # The override must not become a way around the existence check.
  withr::with_envvar(c(URPS_TEST_DB_OVERRIDE = "/tmp/not-a-real-db.duckdb"), {
    expect_error(
      resolve_research_db("DuckDB/x.duckdb", "NoSuchVolume*",
                          "URPS_TEST_DB_OVERRIDE", required = TRUE,
                          what = "override db"),
      "unavailable")
  })
})

test_that("open_research_db REFUSES a missing path rather than connecting", {
  # THE CRITICAL ONE. If this ever connects, DuckDB creates the file and the
  # test suite itself starts manufacturing empty databases.
  tmp <- file.path(tempdir(), "never-created.duckdb")
  if (file.exists(tmp)) unlink(tmp)
  expect_error(open_research_db(tmp, required_tables = "anything"),
               "Refusing to connect")
  expect_false(file.exists(tmp))   # nothing was created
})

test_that("open_research_db refuses NA rather than treating it as a path", {
  expect_error(open_research_db(NA_character_), "no path")
})

test_that("a real database verifies schema, counts and provenance", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("DBI")
  tmp <- file.path(tempdir(), paste0("rdb-", as.integer(runif(1, 1, 1e6)), ".duckdb"))
  on.exit(unlink(tmp), add = TRUE)
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = tmp)
  DBI::dbWriteTable(con, "filled", data.frame(x = 1:5))
  DBI::dbWriteTable(con, "empty", data.frame(x = integer(0)))
  DBI::dbDisconnect(con, shutdown = TRUE)

  # missing table named explicitly
  expect_error(open_research_db(tmp, required_tables = "absent_table"),
               "required table\\(s\\) absent")

  # AN EMPTY REQUIRED TABLE IS A FAILURE, not a zero result -- this is exactly
  # what a mis-resolved path produces.
  expect_error(open_research_db(tmp, required_tables = "empty"),
               "EMPTY")

  # the good path: provenance is recorded for the canonical run
  h <- open_research_db(tmp, required_tables = "filled")
  on.exit(try(DBI::dbDisconnect(h$con, shutdown = TRUE), silent = TRUE), add = TRUE)
  expect_s3_class(h$provenance, "tbl_df")
  expect_equal(h$provenance$resolved_path, tmp)
  expect_gt(h$provenance$file_size_bytes, 0)
  expect_match(h$provenance$row_counts, "filled=5")
  expect_true(nzchar(h$provenance$file_modified))
})

test_that("a below-minimum row count is refused", {
  skip_if_not_installed("duckdb")
  tmp <- file.path(tempdir(), paste0("rdb2-", as.integer(runif(1, 1, 1e6)), ".duckdb"))
  on.exit(unlink(tmp), add = TRUE)
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = tmp)
  DBI::dbWriteTable(con, "small", data.frame(x = 1:3))
  DBI::dbDisconnect(con, shutdown = TRUE)
  expect_error(
    open_research_db(tmp, required_tables = "small", min_rows = c(small = 100)),
    "below the required minimum")
})
