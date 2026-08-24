# Retirement artifact immutability and the v2 confidence schema -------------
#
# Hermetic: an in-memory DuckDB and plain data frames. No file DuckDB, no
# roster, no network.

.rart_consensus <- function(n = 3L, shift = 0) {
  tibble::tibble(
    npi = base::sprintf("%010d", base::seq_len(n)),
    retirement_year = 2020L + base::seq_len(n) + shift,
    exit_class = "licensure_lapse"
  )
}

.rart_sources <- function() {
  tibble::tibble(
    source_name = c("nppes", "state_board"),
    source_table = c("nppes_2024", "board_2024"),
    row_count = c(100L, 50L),
    content_hash = c("aaa", "bbb")
  )
}

.rart_evidence <- function(n = 6L,
                           identity = NULL,
                           event = NULL,
                           timing = NULL,
                           activity = NULL) {
  seq_values <- base::seq_len(n)
  tibble::tibble(
    npi = base::sprintf("%010d", seq_values),
    signal_source = "state_board",
    evidence_family = "licensure",
    retirement_year = 2020L,
    identity_confidence = identity %||% (0.90 + seq_values / 100),
    event_confidence = event %||% (0.80 + seq_values / 100),
    timing_confidence = timing %||% (0.70 + seq_values / 100),
    activity_confidence = activity %||% (0.60 + seq_values / 100)
  )
}

# ---- artifact identity -----------------------------------------------------

testthat::test_that("the artifact id is reproducible for identical inputs", {
  first <- suppressMessages(build_retirement_artifact_manifest(
    .rart_consensus(), .rart_sources(), code_sha = "abc123",
    config_version = "v1"
  ))
  second <- suppressMessages(build_retirement_artifact_manifest(
    .rart_consensus(), .rart_sources(), code_sha = "abc123",
    config_version = "v1"
  ))

  # If a rebuild of identical inputs produced a new id, immutability would
  # degrade into "every build is new" and the manifest would prove nothing.
  testthat::expect_identical(first$artifact_id, second$artifact_id)
  testthat::expect_identical(first$consensus_hash, second$consensus_hash)
  testthat::expect_true(first$created_at != second$created_at ||
                          base::is.na(first$created_at))
})

testthat::test_that("column and row order are presentation, not content", {
  base_manifest <- suppressMessages(build_retirement_artifact_manifest(
    .rart_consensus(), .rart_sources(), code_sha = "abc123"
  ))
  shuffled <- .rart_consensus()[base::c(3L, 1L, 2L), base::c(3L, 1L, 2L)]
  shuffled_manifest <- suppressMessages(build_retirement_artifact_manifest(
    shuffled, .rart_sources(), code_sha = "abc123"
  ))

  testthat::expect_identical(
    base_manifest$artifact_id, shuffled_manifest$artifact_id
  )
})

testthat::test_that("any change that could change the science changes the id", {
  reference <- suppressMessages(build_retirement_artifact_manifest(
    .rart_consensus(), .rart_sources(), schema_version = 2L,
    code_sha = "abc123", config_version = "v1"
  ))$artifact_id

  variants <- base::list(
    different_consensus = suppressMessages(build_retirement_artifact_manifest(
      .rart_consensus(shift = 5), .rart_sources(), schema_version = 2L,
      code_sha = "abc123", config_version = "v1")),
    different_sources = suppressMessages(build_retirement_artifact_manifest(
      .rart_consensus(),
      dplyr::mutate(.rart_sources(), content_hash = c("zzz", "bbb")),
      schema_version = 2L, code_sha = "abc123", config_version = "v1")),
    different_code = suppressMessages(build_retirement_artifact_manifest(
      .rart_consensus(), .rart_sources(), schema_version = 2L,
      code_sha = "def456", config_version = "v1")),
    different_config = suppressMessages(build_retirement_artifact_manifest(
      .rart_consensus(), .rart_sources(), schema_version = 2L,
      code_sha = "abc123", config_version = "v2")),
    different_schema = suppressMessages(build_retirement_artifact_manifest(
      .rart_consensus(), .rart_sources(), schema_version = 3L,
      code_sha = "abc123", config_version = "v1"))
  )

  for (variant_name in base::names(variants)) {
    testthat::expect_false(
      base::identical(variants[[variant_name]]$artifact_id, reference),
      info = base::paste(variant_name, "did not change the artifact id")
    )
  }
})

# ---- append-only persistence ----------------------------------------------

.rart_con <- function() {
  testthat::skip_if_not_installed("duckdb")
  testthat::skip_if_not_installed("DBI")
  con <- DBI::dbConnect(duckdb::duckdb())
  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS credentials")
  con
}

testthat::test_that("a consensus artifact is appended, never replaced", {
  con <- .rart_con()
  base::on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  first_consensus <- .rart_consensus()
  first_manifest <- suppressMessages(build_retirement_artifact_manifest(
    first_consensus, .rart_sources(), code_sha = "abc123"
  ))
  suppressMessages(persist_retirement_consensus_artifact(
    con, first_consensus, first_manifest
  ))

  second_consensus <- .rart_consensus(shift = 10)
  second_manifest <- suppressMessages(build_retirement_artifact_manifest(
    second_consensus, .rart_sources(), code_sha = "abc123"
  ))
  suppressMessages(persist_retirement_consensus_artifact(
    con, second_consensus, second_manifest
  ))

  stored <- DBI::dbGetQuery(
    con, "SELECT artifact_id, COUNT(*) AS n FROM credentials.retirement_consensus_artifacts GROUP BY artifact_id ORDER BY artifact_id"
  )
  # The earlier build must still be readable: a manuscript citing it does not
  # become unverifiable because somebody rebuilt.
  testthat::expect_equal(base::nrow(stored), 2L)
  testthat::expect_true(
    first_manifest$artifact_id %in% stored$artifact_id
  )
  testthat::expect_true(
    second_manifest$artifact_id %in% stored$artifact_id
  )
})

testthat::test_that("re-persisting identical content is a no-op", {
  con <- .rart_con()
  base::on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  consensus <- .rart_consensus()
  manifest <- suppressMessages(build_retirement_artifact_manifest(
    consensus, .rart_sources(), code_sha = "abc123"
  ))
  suppressMessages(persist_retirement_consensus_artifact(con, consensus, manifest))
  suppressMessages(persist_retirement_consensus_artifact(con, consensus, manifest))

  total <- DBI::dbGetQuery(
    con, "SELECT COUNT(*) AS n FROM credentials.retirement_consensus_artifacts"
  )$n
  testthat::expect_equal(base::as.integer(total), base::nrow(consensus))
})

testthat::test_that("mutating an existing artifact id is refused", {
  con <- .rart_con()
  base::on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  consensus <- .rart_consensus()
  manifest <- suppressMessages(build_retirement_artifact_manifest(
    consensus, .rart_sources(), code_sha = "abc123"
  ))
  suppressMessages(persist_retirement_consensus_artifact(con, consensus, manifest))

  # Same declared id, different content: exactly what CREATE OR REPLACE did
  # silently, and the reason a table name is not an identifier for a result.
  forged <- manifest
  forged$consensus_hash <- "0000000000"
  testthat::expect_error(
    suppressMessages(persist_retirement_consensus_artifact(
      con, .rart_consensus(shift = 99), forged
    )),
    "Refusing to mutate"
  )
})

# ---- v2 confidence schema --------------------------------------------------

testthat::test_that("all four confidence dimensions are required", {
  for (dimension in retirement_confidence_dimensions()) {
    incomplete <- .rart_evidence()
    incomplete[[dimension]] <- NULL
    testthat::expect_error(
      suppressMessages(validate_retirement_evidence_v2(incomplete)),
      dimension, fixed = TRUE
    )
  }
})

testthat::test_that("a legacy confidence broadcast across all four dimensions is refused", {
  # The migration hazard: this satisfies every name-based check while
  # asserting the same unverified claim four times, and it makes the identity
  # gate unenforceable because the identity component no longer exists
  # separately.
  legacy_value <- base::seq(0.90, 0.95, length.out = 6L)
  broadcast <- .rart_evidence(
    identity = legacy_value, event = legacy_value,
    timing = legacy_value, activity = legacy_value
  )
  broadcast$final_confidence <- legacy_value

  testthat::expect_error(
    suppressMessages(validate_retirement_evidence_v2(broadcast)),
    "broadcast across the v2 schema"
  )
  # The diagnostic must name the legacy column when it is still present.
  testthat::expect_error(
    suppressMessages(validate_retirement_evidence_v2(broadcast)),
    "final_confidence", fixed = TRUE
  )
})

testthat::test_that("genuinely independent dimensions pass", {
  testthat::expect_true(
    suppressMessages(validate_retirement_evidence_v2(.rart_evidence()))
  )
})

testthat::test_that("incidental ties do not trip the broadcast check", {
  # Some rows legitimately agree across dimensions. Only near-universal
  # agreement indicates a broadcast, so the check must not fire on a handful.
  partial <- .rart_evidence(n = 10L)
  partial$event_confidence[1:2] <- partial$identity_confidence[1:2]
  partial$timing_confidence[1:2] <- partial$identity_confidence[1:2]
  partial$activity_confidence[1:2] <- partial$identity_confidence[1:2]

  testthat::expect_true(
    suppressMessages(validate_retirement_evidence_v2(partial))
  )
})

testthat::test_that("confidence values outside [0, 1] are refused", {
  invalid <- .rart_evidence()
  invalid$identity_confidence[[2]] <- 1.4
  testthat::expect_error(
    suppressMessages(validate_retirement_evidence_v2(invalid)),
    "outside"
  )
})

testthat::test_that("a dated signal without timing confidence warns", {
  undated_timing <- .rart_evidence()
  undated_timing$timing_confidence[[1]] <- NA_real_
  testthat::expect_warning(
    suppressMessages(validate_retirement_evidence_v2(undated_timing)),
    "timing_confidence"
  )
})
