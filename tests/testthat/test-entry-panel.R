# Individual-level entry panel.
#
# The derivation is tested on hand-built panels so the logic is pinned without
# the 84 GB external database; the database-backed path is exercised only when
# the volume is attached, and skipped rather than faked when it is not.

ep_panel <- function(..., npi = "1234567890", years = 2013:2024,
                     fellowship = NA_integer_, cert = NA_integer_) {
  n <- length(years)
  d <- data.frame(
    npi = npi, year = years,
    fellowship_completion_year = fellowship, certification_year = cert,
    ev_pecos_enrolled = NA, ev_nppes_observed = NA,
    ev_nppes_taxonomy = NA_character_, ev_nppes_student = NA,
    ev_nppes_state = NA_character_,
    ev_partb_billed = NA, ev_partb_services = NA_real_,
    ev_partb_state = NA_character_, ev_partb_type = NA_character_,
    ev_openpay_paid = NA,
    ev_certified = if (is.na(cert)) NA else years >= cert,
    stringsAsFactors = FALSE
  )
  e <- list(...)
  for (nm in names(e)) {
    v <- e[[nm]]
    d[[nm]] <- if (length(v) == 1L) rep(v, n) else v
  }
  d
}
ep_derive1 <- function(d) .ep_derive(d)[1, ]

# ---- three-state semantics --------------------------------------------------

test_that("unknown is kept distinct from inactive", {
  # The error this exists to prevent: an NA meaning 'no source could see this
  # year' silently becoming FALSE, which asserts the clinician was not
  # practising. active_practice_observed must be NA when nothing could observe.
  blind <- ep_panel(ev_pecos_enrolled = NA, ev_partb_billed = NA)
  got <- .ep_derive(blind)
  expect_true(all(is.na(got$active_practice_observed)))
  expect_equal(unique(got$entry_confidence), "unknown")
  expect_true(is.na(got$entry_year_best[1]))

  seen_negative <- ep_panel(ev_pecos_enrolled = FALSE, ev_partb_billed = FALSE)
  got2 <- .ep_derive(seen_negative)
  expect_true(all(got2$active_practice_observed %in% FALSE))   # observed, absent
  expect_false(any(is.na(got2$active_practice_observed)))
})

test_that("a single positive source makes the year active despite an NA sibling", {
  d <- ep_panel(ev_pecos_enrolled = NA,
                ev_partb_billed = c(rep(FALSE, 6), rep(TRUE, 6)))
  got <- .ep_derive(d)
  expect_equal(sum(got$active_practice_observed %in% TRUE), 6L)
  expect_equal(sum(got$active_practice_observed %in% FALSE), 6L)
  expect_equal(got$entry_year_best[1], 2019L)
})

# ---- what may and may not establish entry -----------------------------------

test_that("Open Payments alone cannot establish entry", {
  # Industry pays FELLOWS. A payment does not distinguish a trainee from an
  # attending, so on its own it yields low confidence and is never treated as a
  # practice-grade source.
  d <- ep_panel(ev_openpay_paid = c(rep(FALSE, 4), rep(TRUE, 8)))
  got <- ep_derive1(d)
  expect_equal(got$entry_confidence, "low")
  expect_equal(got$entry_source, "open_payments")
  expect_true(is.na(got$entry_year_post_fellowship))   # no practice-grade source
})

test_that("certification alone bounds entry from above and is flagged low", {
  d <- ep_panel(cert = 2020L)
  got <- ep_derive1(d)
  expect_equal(got$entry_confidence, "low")
  expect_equal(got$entry_source, "certification")
  expect_equal(got$entry_year_best, 2020L)
})

test_that("a practice-grade source outranks corroborating evidence", {
  d <- ep_panel(ev_openpay_paid = TRUE,
                ev_pecos_enrolled = c(rep(FALSE, 5), rep(TRUE, 7)),
                cert = 2013L)
  got <- ep_derive1(d)
  expect_equal(got$entry_source, "pecos_enrollment")
  expect_equal(got$entry_year_best, 2018L)
  expect_true(got$entry_confidence %in% c("moderate", "high"))
})

test_that("two practice sources agreeing within a year give high confidence", {
  d <- ep_panel(ev_pecos_enrolled = c(rep(FALSE, 5), rep(TRUE, 7)),
                ev_partb_billed = c(rep(FALSE, 6), rep(TRUE, 6)))
  got <- ep_derive1(d)
  expect_equal(got$entry_confidence, "high")
  expect_equal(got$entry_year_best, 2018L)
  expect_false(got$evidence_conflict)
})

# ---- conflict ---------------------------------------------------------------

test_that("practice sources far apart are a conflict, not an average", {
  d <- ep_panel(ev_pecos_enrolled = c(rep(FALSE, 2), rep(TRUE, 10)),
                ev_partb_billed = c(rep(FALSE, 9), rep(TRUE, 3)))
  got <- ep_derive1(d)
  expect_true(got$evidence_conflict)          # 2015 vs 2022
  expect_equal(got$entry_year_best, 2015L)    # earliest still reported
})

test_that("practice evidence predating fellowship completion is a conflict", {
  # The case the pilot dry run surfaced: OB/GYN generalists bill Medicare years
  # before subspecialty fellowship, so unrestricted first-billing measures entry
  # to GENERALIST practice.
  d <- ep_panel(ev_partb_billed = c(rep(TRUE, 12)), fellowship = 2020L)
  got <- ep_derive1(d)
  expect_true(got$evidence_conflict)
  expect_equal(got$entry_year_best, 2013L)
})

test_that("the post-fellowship estimand ignores pre-fellowship practice", {
  d <- ep_panel(ev_partb_billed = TRUE, fellowship = 2020L)
  got <- ep_derive1(d)
  expect_equal(got$entry_year_best, 2013L)              # unrestricted, retained
  expect_equal(got$entry_year_post_fellowship, 2020L)   # restricted, defensible
  expect_equal(got$years_from_fellowship_to_entry, 0L)  # measured off the restricted one
})

test_that("years_from_fellowship_to_entry is NA when fellowship year is unknown", {
  d <- ep_panel(ev_partb_billed = c(rep(FALSE, 5), rep(TRUE, 7)))
  got <- ep_derive1(d)
  expect_true(is.na(got$years_from_fellowship_to_entry))
  expect_equal(got$entry_year_post_fellowship, 2018L)
})

# ---- NPPES taxonomy transition ----------------------------------------------

test_that("a taxonomy exit requires both states to have been observed", {
  # A clinician only ever seen with a specialty taxonomy has no OBSERVED
  # transition; inventing one would date entry to the first snapshot.
  never_student <- ep_panel(ev_nppes_student = FALSE)
  expect_true(is.na(ep_derive1(never_student)$first_nppes_taxonomy_exit_year))

  still_student <- ep_panel(ev_nppes_student = TRUE)
  expect_true(is.na(ep_derive1(still_student)$first_nppes_taxonomy_exit_year))

  transitioned <- ep_panel(ev_nppes_student = c(rep(TRUE, 6), rep(FALSE, 6)))
  got <- ep_derive1(transitioned)
  expect_equal(got$first_nppes_taxonomy_exit_year, 2019L)
  expect_equal(got$entry_source, "nppes_taxonomy_exit_student")
})

test_that("a taxonomy reverting to student does not create an earlier exit", {
  # Student, specialty, student again, specialty. The exit is the first
  # specialty year AFTER the last student year, not the first specialty year.
  d <- ep_panel(ev_nppes_student = c(TRUE, TRUE, FALSE, FALSE, TRUE, TRUE,
                                     FALSE, FALSE, FALSE, FALSE, FALSE, FALSE))
  expect_equal(ep_derive1(d)$first_nppes_taxonomy_exit_year, 2019L)
})

# ---- input validation -------------------------------------------------------

test_that("malformed cohorts are refused before any query runs", {
  expect_error(build_entry_panel(character(0)), "empty")
  expect_error(build_entry_panel("123"), "not 10 digits")
  expect_error(build_entry_panel(c("1234567890", "1234567890")), "duplicate")
  expect_error(build_entry_panel(data.frame(id = "1234567890")), "`npi` column")
  # A numeric NPI column loses leading zeros; the message must say so.
  expect_error(build_entry_panel(data.frame(npi = 123456789)), "leading zeros")
})

test_that("a missing database is reported with the path, not a driver error", {
  expect_error(build_entry_panel("1234567890", db = "/nonexistent/x.duckdb"),
               "/nonexistent/x.duckdb")
  expect_error(build_entry_panel("1234567890", db = "/nonexistent/x.duckdb"),
               "Attach the external volume")
})

test_that("summarise_entry_panel refuses a frame it did not build", {
  expect_error(summarise_entry_panel(data.frame(npi = "1")), "build_entry_panel")
})

# ---- source table contract --------------------------------------------------

test_that("Open Payments is never graded as practice evidence", {
  s <- ENTRY_EVIDENCE_SOURCES
  expect_equal(s$grade[s$source == "open_payments"], "corroborating")
  expect_equal(s$grade[s$source == "certification"], "bounding")
  expect_setequal(s$source[s$grade == "practice"],
                  c("pecos_enrollment", "nppes_taxonomy_exit_student", "part_b_billing"))
  # Open Payments carries no NPI before 2015 and Part B begins 2013; a window
  # that drifts would silently change what an NA means.
  expect_equal(s$first_year[s$source == "open_payments"], 2015L)
  expect_equal(s$first_year[s$source == "part_b_billing"], 2013L)
})

# ---- end-to-end, only when the volume is attached ---------------------------

test_that("the database-backed panel builds with the expected shape", {
  skip_if_not(file.exists(ENTRY_PANEL_DB_DEFAULT), "credentials database not attached")
  # The roster lives under data-raw/, which is .Rbuildignore'd, so it is absent
  # from the .Rcheck tree even though the relative path resolves in the source
  # tree. Guarding only the database made this test fail under `make check`
  # exclusively on machines where the volume IS attached -- the gate passed for
  # everyone else, which is why it went unnoticed.
  roster <- "../../data-raw/urps_roster/urps_roster_2026-07-22.csv"
  skip_if_not(file.exists(roster), "urps roster not present (data-raw absent under R CMD check)")
  r <- utils::read.csv(roster, colClasses = "character")
  co <- data.frame(npi = head(unique(r$npi), 3), stringsAsFactors = FALSE)
  p <- build_entry_panel(co, years = 2018:2023)
  expect_s3_class(p, "entry_panel")
  expect_equal(nrow(p), 18L)
  expect_true(all(c("active_practice_observed", "entry_year_best", "entry_source",
                    "entry_confidence", "evidence_available", "evidence_conflict",
                    "years_from_fellowship_to_entry") %in% names(p)))
  expect_equal(nrow(summarise_entry_panel(p)), 3L)
})
