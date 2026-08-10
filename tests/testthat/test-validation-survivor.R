# THE FREEZE. This file is the single place where the survivor-conditioning
# counts appear as literals. The artifact supplies them, the R module reads
# them, the figure and the manuscript sentence are generated from them -- and
# this file is what stops any of those from changing quietly. A rebuild that
# moves a number fails here, which is the point: the counts are a published
# claim, so they should be as hard to change as one.
#
# Rebuilt with scripts/data_acquisition/09_build_survivor_falsification.R.

skip_if_no_artifact <- function() {
  testthat::skip_if_not(
    nzchar(system.file("extdata", "survivor_falsification.json",
                       package = "urpssim")),
    "survivor-conditioning artifact not installed")
}

test_that("the identity universe and excluded denominator close", {
  skip_if_no_artifact()
  d <- survivor_falsification_artifact()$denominators

  expect_equal(d$identity_universe, 1500L)
  expect_equal(d$retained, 1339L)
  expect_equal(d$excluded_total, 161L)
  expect_equal(d$retained + d$excluded_total, d$identity_universe)

  # 155 NPI-linkable + 6 without a usable NPI = 161. The six are blank, not
  # malformed: an identity the roster never carried, rather than one we mangled.
  expect_equal(d$linkage_denominator, 155L)
  expect_equal(d$excluded_without_npi, 6L)
  expect_equal(d$linkage_denominator + d$excluded_without_npi, d$excluded_total)
  expect_equal(d$excluded_blank_npi, 6L)
  expect_equal(d$excluded_malformed_npi, 0L)
})

test_that("the Part B windows partition the linkage denominator", {
  skip_if_no_artifact()
  a <- survivor_falsification_artifact()
  n155 <- a$denominators$linkage_denominator

  # 121 observed in the validation window, 34 not.
  expect_equal(a$partb$any_validation, 121L)
  expect_equal(n155 - a$partb$any_validation, 34L)

  # 129 observed anywhere in the frame, 26 not.
  expect_equal(a$partb$any_frame, 129L)
  expect_equal(a$partb$none_frame, 26L)
  expect_equal(a$partb$any_frame + a$partb$none_frame, n155)

  # The validation window is inside the frame, so its count cannot exceed it.
  expect_lte(a$partb$any_validation, a$partb$any_frame)
})

test_that("the persistent subgroup is frozen at 69 physicians / 414 provider-years", {
  skip_if_no_artifact()
  a <- survivor_falsification_artifact()

  expect_equal(a$partb$persistent_validation, 69L)
  expect_lte(a$partb$persistent_validation, a$partb$any_validation)
  expect_equal(a$partb$provider_years_persistent, 69L * 6L)
  expect_equal(a$partb$provider_years_persistent, 414L)

  # Six validation years, so the arithmetic above is 69 x 6 and not 69 x
  # something else.
  v <- a$windows$validation
  expect_equal(length(seq.int(v[1], v[2])), 6L)
  expect_equal(a$partb$provider_years_persistent,
               a$partb$persistent_validation * length(seq.int(v[1], v[2])))
})

test_that("the no-Part-B residual partitions into the directory classes", {
  skip_if_no_artifact()
  a <- survivor_falsification_artifact()

  expect_equal(a$directory$sustained, 9L)
  expect_equal(a$directory$isolated, 8L)
  expect_equal(a$directory$neither, 9L)
  expect_equal(a$directory$sustained + a$directory$isolated +
                 a$directory$neither, a$partb$none_frame)
  expect_equal(a$directory$sustained + a$directory$isolated +
                 a$directory$neither, 26L)
})

test_that("directory evidence is never reported as billed care", {
  skip_if_no_artifact()
  tbl <- survivor_falsification_table()

  # Tier 2 is enrolment/listing. It must carry a weaker label, must never carry
  # the tier-1 "direct" language, and must never be summed with Part B.
  t2 <- tbl[!is.na(tbl$tier) & tbl$tier == 2L, ]
  expect_gt(nrow(t2), 0L)
  expect_true(all(grepl("listing|enrol", t2$strength, ignore.case = TRUE)))
  expect_false(any(grepl("direct", t2$strength, ignore.case = TRUE)))

  # No directory row may be labelled tier 1.
  t1 <- tbl[!is.na(tbl$tier) & tbl$tier == 1L, ]
  expect_false(any(grepl("directory", t1$evidence, ignore.case = TRUE)))
  expect_true(all(grepl("direct", t1$strength, ignore.case = TRUE)))

  # The directory cannot speak to the early validation years, so its window
  # must start after the validation window does.
  a <- survivor_falsification_artifact()
  expect_gt(a$windows$directory[1], a$windows$validation[1])
})

test_that("absence from Part B is not recorded as clinical inactivity", {
  skip_if_no_artifact()
  tbl <- survivor_falsification_table()

  # The residual row is named for what was observed, not for what it implies.
  # "No Part B billing" is a statement about a data source; "inactive",
  # "retired" or "left the workforce" would be a claim the data cannot support,
  # because the 26 include physicians with sustained directory listings.
  resid <- tbl$evidence[grepl("No Part B", tbl$evidence)]
  expect_length(resid, 1L)
  expect_false(any(grepl("inactive|retired|left|departed|attrit",
                         tbl$evidence, ignore.case = TRUE)))
  expect_false(any(grepl("inactive|retired|left the workforce",
                         tbl$strength, ignore.case = TRUE)))
})

test_that("the guard accepts the artifact as built", {
  skip_if_no_artifact()
  expect_true(assert_survivor_falsification())
})

test_that("the guard rejects tampering with the table", {
  skip_if_no_artifact()
  a <- survivor_falsification_artifact()

  bad <- survivor_falsification_table(a)
  bad$n[bad$evidence == "No Part B billing anywhere"] <- 30L
  expect_error(assert_survivor_falsification(a, tbl = bad),
               "table and record disagree")

  bad2 <- survivor_falsification_table(a)
  bad2$n[bad2$evidence == "Part B billing in ALL SIX validation years"] <- 200L
  expect_error(assert_survivor_falsification(a, tbl = bad2),
               "table and record disagree")
})

test_that("the guard rejects tampering with the record", {
  skip_if_no_artifact()
  a <- survivor_falsification_artifact()

  bad <- survivor_falsification_record(a)
  bad$n_persistent_billers <- 80L
  expect_error(assert_survivor_falsification(a, rec = bad),
               "table and record disagree")

  # Provider-years must track the count they are derived from.
  bad2 <- survivor_falsification_record(a)
  bad2$provider_years_erased <- 500L
  expect_error(assert_survivor_falsification(a, rec = bad2),
               "provider-years erased")
})

test_that("the guard rejects a broken denominator or window", {
  skip_if_no_artifact()

  # A denominator that does not close.
  a <- survivor_falsification_artifact()
  a$denominators$excluded_without_npi <- 10L
  expect_error(assert_survivor_falsification(a), "does not close")

  # Windows that do not nest: more observed in the validation window than in
  # the frame that contains it.
  b <- survivor_falsification_artifact()
  b$partb$any_validation <- b$partb$any_frame + 5L
  expect_error(assert_survivor_falsification(b))

  # A residual that does not partition.
  d <- survivor_falsification_artifact()
  d$directory$isolated <- d$directory$isolated + 3L
  expect_error(assert_survivor_falsification(d), "does not partition")
})

test_that("the guard rejects a relabelled tier", {
  skip_if_no_artifact()
  a <- survivor_falsification_artifact()

  bad <- survivor_falsification_table(a)
  bad$strength[!is.na(bad$tier) & bad$tier == 2L] <- "direct: care billed"
  expect_error(assert_survivor_falsification(a, tbl = bad), "tier-1 language")
})

test_that("the artifact carries usable provenance", {
  skip_if_no_artifact()
  p <- survivor_falsification_artifact()$provenance

  # Roster checksums, so a changed roster is detectable.
  expect_match(p$abog$sha256, "^[0-9a-f]{64}$")
  expect_match(p$abu$sha256, "^[0-9a-f]{64}$")

  # The Medicare panel is too large to checksum, so it is identified by what
  # was queried instead. Its coverage must span the analysis frame.
  a <- survivor_falsification_artifact()
  expect_lte(p$duckdb$partb$min_year, a$windows$frame[1])
  expect_gte(p$duckdb$partb$max_year, a$windows$frame[2])
  expect_gt(p$duckdb$partb$n_rows, 0)
})

test_that("the annual panel partitions and is consistent with the totals", {
  skip_if_no_artifact()
  a <- survivor_falsification_artifact()
  an <- a$annual

  expect_equal(nrow(an), length(seq.int(a$windows$frame[1], a$windows$frame[2])))
  expect_true(all(an$retained_observed + an$excluded_observed ==
                    an$total_observed))
  expect_true(all(an$total_observed <= an$eligible_total))

  # The persistent subgroup is a subset of the excluded group in every year.
  expect_true(all(an$persistent_observed <= an$excluded_observed))
  expect_true(all(an$persistent_observed <= a$partb$persistent_validation))

  # Inside the validation window all 69 bill in every year by construction, so
  # the annual count is purely the number of them CERTIFIED by that year: it
  # rises monotonically and reaches the full 69 in the final year. It starts
  # below 69 because a few were already billing Medicare before they certified
  # in the subspecialty -- as generalists, who do not belong in an
  # already-certified denominator until they certify.
  v <- seq.int(a$windows$validation[1], a$windows$validation[2])
  pv <- an$persistent_observed[an$year %in% v]
  expect_false(is.unsorted(pv))
  expect_equal(pv[length(pv)], a$partb$persistent_validation)
  expect_equal(pv[1], 66L)
})

test_that("the generated statement agrees with the artifact", {
  skip_if_no_artifact()
  s <- survivor_falsification_statement()

  # Prose is generated, never retyped, so the sentence must carry exactly the
  # frozen counts.
  expect_match(s, "155 excluded")
  expect_match(s, "121 \\(78\\.1%\\)")
  expect_match(s, "69 \\(44\\.5%\\)")
  expect_match(s, "414 directly observed provider-years")
})

test_that("the committed supplemental table matches its source", {
  skip_if_no_artifact()
  # A table checked into the repository is the easiest place for a number to go
  # stale, so the committed copy is compared against the generator rather than
  # trusted. Absent during R CMD check, where figures/ is not shipped.
  f <- "../../figures/survivor_falsification_table.md"
  testthat::skip_if_not(file.exists(f), "supplemental table not in this tree")
  expect_identical(readLines(f), survivor_falsification_markdown())
})

test_that("the contract still does not ascertain retirement", {
  skip_if_no_artifact()
  testthat::skip_if_not_installed("mufflyaccess")

  # The central claim -- that this series cannot separate active workforce from
  # ever-certified, and that absence is not retirement -- rests on the contract
  # not ascertaining retirement. That is the contract's own declaration, not our
  # reading of it, so it is pinned here rather than restated in prose. If this
  # ever flips, the survivor-conditioning argument needs revisiting, not
  # repeating.
  expect_identical(mufflyaccess::urps_retirement_status(), "not_ascertained")

  measures <- unique(as.data.frame(mufflyaccess::urps_counts_long())$measure)
  expect_false(any(grepl("retire", measures, ignore.case = TRUE)))
})

test_that("a zero denominator cannot publish a non-finite rate", {
  skip_if_no_artifact()

  # The partition check is satisfied by 0 + 0 + 0 == 0, so an artifact in which
  # no excluded physician lacks Part B passes every structural test while making
  # each directory rate undefined. Before this guard the table, the markdown and
  # the figure all carried "NaN" and nothing objected.
  a <- survivor_falsification_artifact()
  a$partb$none_frame <- 0L
  a$partb$any_frame <- a$denominators$linkage_denominator
  a$directory$sustained <- 0L
  a$directory$isolated <- 0L
  a$directory$neither <- 0L

  expect_true(all(is.na(survivor_falsification_table(a)$pct[8:10])))
  expect_error(assert_survivor_falsification(a), "non-finite percentage")

  # The shipped artifact has no undefined rate.
  expect_true(all(is.finite(survivor_falsification_table()$pct)))
})
