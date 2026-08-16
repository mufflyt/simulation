# NRMP source series: gates, provenance, and format-change detection.
#
# The series is compiled into the package (data-raw/ does not ship), so these
# tests are the only thing standing between a mis-parsed PDF column and a
# back-test arm that looks fine.

nsp_csv <- function() {
  root <- .source_tree_root()
  if (length(root) == 0) return(NULL)
  p <- file.path(root[1], "data-raw", "calibration", "nrmp_urps_entrants_series.csv")
  if (file.exists(p)) p else NULL
}

test_that("every source row passes GATE 1: filled/offered reproduces printed % filled", {
  p <- nsp_csv(); skip_if(is.null(p), "NRMP series CSV not reachable (source tree absent under R CMD check)")
  d <- utils::read.csv(p, stringsAsFactors = FALSE)
  recomputed <- round(100 * d$positions_filled / d$positions_offered, 1)
  # This is the gate that proves the column mapping, independently of any
  # remembered value. A moved column fails here rather than silently returning
  # positions offered where matches were meant.
  expect_true(all(abs(recomputed - d$pct_filled_all) <= 0.15),
              info = paste("offending years:",
                           paste(d$appointment_year[abs(recomputed - d$pct_filled_all) > 0.15],
                                 collapse = ", ")))
})

test_that("every source row passes GATE 2: values match the documented human read", {
  p <- nsp_csv(); skip_if(is.null(p), "NRMP series CSV not reachable (source tree absent under R CMD check)")
  d <- utils::read.csv(p, stringsAsFactors = FALSE)
  expected_filled <- c(`2010` = 30L, `2011` = 40L, `2012` = 37L, `2013` = 48L,
                       `2014` = 50L, `2015` = 57L, `2016` = 53L, `2017` = 59L,
                       `2018` = 59L, `2019` = 58L, `2020` = 56L, `2025` = 70L)
  got <- stats::setNames(d$positions_filled, as.character(d$appointment_year))
  expect_equal(got[names(expected_filled)], expected_filled)
})

test_that("self-verification fixtures span the format eras", {
  p <- nsp_csv(); skip_if(is.null(p), "NRMP series CSV not reachable (source tree absent under R CMD check)")
  d <- utils::read.csv(p, stringsAsFactors = FALSE)
  get <- function(y, f) d[[f]][d$appointment_year == y]

  # 2010-2012 era: the row label WRAPS after "and", so a label-anchored parser
  # that assumed "...and Reconstructive" would miss it entirely.
  expect_equal(get(2010, "positions_filled"), 30L)
  expect_equal(get(2010, "positions_offered"), 34L)
  # 2013-2016 era.
  expect_equal(get(2015, "positions_filled"), 57L)
  expect_equal(get(2015, "positions_offered"), 58L)
  # The independently verified anchor: cliff's data/nrmp_fellowship_entrants.csv
  # carries 70 for 2025, read by a different person from the same PDF.
  expect_equal(get(2025, "positions_filled"), 70L)
})

test_that("appointment years are unique and no report is counted twice", {
  p <- nsp_csv(); skip_if(is.null(p), "NRMP series CSV not reachable (source tree absent under R CMD check)")
  d <- utils::read.csv(p, stringsAsFactors = FALSE)
  expect_equal(anyDuplicated(d$appointment_year), 0L)
  expect_equal(anyDuplicated(d$source_url), 0L)
  # Each report is published in its own appointment year; that identity is what
  # makes available_by_year a usable leakage filter.
  expect_equal(d$report_published, d$appointment_year)
  expect_equal(d$available_by_year, d$appointment_year)
})

test_that("full provenance travels with every row", {
  p <- nsp_csv(); skip_if(is.null(p), "NRMP series CSV not reachable (source tree absent under R CMD check)")
  d <- utils::read.csv(p, stringsAsFactors = FALSE)
  for (f in c("report_title", "table_name", "source_url", "retrieved_on")) {
    expect_true(all(nzchar(as.character(d[[f]]))), info = f)
  }
  expect_true(all(grepl("^https://", d$source_url)))
  expect_true(all(grepl("Table 1", d$table_name)))
})

test_that("filled never exceeds offered, and the compiled series matches the CSV", {
  s <- nrmp_entrant_series()
  expect_true(all(s$positions_filled <= s$positions_offered))
  expect_equal(anyDuplicated(s$appointment_year), 0L)

  p <- nsp_csv(); skip_if(is.null(p), "NRMP series CSV not reachable (source tree absent under R CMD check)")
  d <- utils::read.csv(p, stringsAsFactors = FALSE)
  m <- merge(d, s, by = "appointment_year", suffixes = c("_csv", "_pkg"))
  expect_equal(nrow(m), nrow(s))
  expect_equal(m$positions_filled_csv, m$positions_filled_pkg)
  expect_equal(m$positions_offered_csv, m$positions_offered_pkg)
})

test_that("the establishment ramp is excluded from growth estimation", {
  # Extending to 2010 exposed a structural break. A first-to-last CAGR over the
  # whole series returns ~4.9%/yr by averaging a one-off ramp with a plateau.
  full <- nrmp_growth_rates(from = 2010L)
  plateau <- nrmp_growth_rates()
  expect_equal(plateau$estimated_from, urpssim:::NRMP_PLATEAU_FROM)
  expect_gt(full$offered, plateau$offered)
  expect_lt(plateau$offered, 0.03)
  expect_error(nrmp_growth_rates(from = 2030L), "fewer than two observations")
})
