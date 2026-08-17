# AI definitive-treatment rates.
#
# The error being prevented: a single SNM-vs-sphincteroplasty "share". The two
# do not exhaust AI treatment and do not share an eligibility set, so a forced
# split invents a competition between them. These tests contain no sourced AI
# probability -- the fixtures are arbitrary and exist to pin structure.

.ai_rates <- function(snm = 40, sph = 10, den = 1000, ...) {
  ai_definitive_treatment_rates(
    snm_n = snm, sphincteroplasty_n = sph,
    ai_treated_population_n = den,
    snm_indication_linked = TRUE, snm_indication_window = "same_claim",
    source = "test fixture", ...)
}

test_that("the two rates do NOT sum to one", {
  # THE POINT. Many women with FI receive neither treatment.
  r <- .ai_rates()
  testthat::expect_equal(nrow(r), 2L)
  testthat::expect_lt(sum(r$rate), 1)
  testthat::expect_equal(r$rate, c(0.04, 0.01))
})

test_that("each rate carries its OWN denominator", {
  r <- .ai_rates()
  testthat::expect_true(all(r$denominator_n == 1000))
  testthat::expect_true(all(r$denominator %in% AI_TREATMENT_DENOMINATORS))
})

test_that("sphincteroplasty can use a repair-eligible denominator", {
  # Its eligibility is anatomic; SNM's is not. The denominators may differ.
  r <- .ai_rates(repair_eligible_n = 200)
  sph <- r[r$treatment == "sphincteroplasty", ]
  testthat::expect_equal(sph$denominator_n, 200)
  testthat::expect_equal(sph$denominator, "repair_eligible_population")
  testthat::expect_equal(sph$rate, 0.05)
  # SNM is unaffected -- the two rates are independent
  testthat::expect_equal(r$rate[r$treatment == "snm"], 0.04)
})

test_that("a bare SNM CPT count is REFUSED", {
  # SNM implantation codes also serve urinary urgency/OAB and retention, so the
  # codes are not indication-specific.
  testthat::expect_error(
    ai_definitive_treatment_rates(40, 10, 1000, snm_indication_linked = FALSE,
                                  snm_indication_window = "same_claim",
                                  source = "x"),
    "not indication-specific")
})

test_that("the SNM indication WINDOW must be recorded", {
  # The window materially changes the count and is a preregistered choice.
  testthat::expect_error(
    ai_definitive_treatment_rates(40, 10, 1000, snm_indication_linked = TRUE,
                                  source = "x"),
    "snm_indication_window must be recorded")
})

test_that("provenance is mandatory", {
  testthat::expect_error(
    ai_definitive_treatment_rates(40, 10, 1000, TRUE, "same_claim"),
    "source is required")
})

test_that("a count exceeding its denominator is refused", {
  testthat::expect_error(.ai_rates(snm = 2000), "exceeds the AI care population")
  testthat::expect_error(.ai_rates(sph = 300, repair_eligible_n = 200),
                         "exceeds the repair-eligible denominator")
})

test_that("uncertainty accompanies each rate separately", {
  r <- .ai_rates()
  testthat::expect_true(all(r$rate_lo < r$rate & r$rate_hi > r$rate))
})

test_that("the conditional share is DERIVED and labelled as non-canonical", {
  r <- .ai_rates()
  s <- ai_conditional_treatment_share(r)
  testthat::expect_equal(s$value, 40 / 50)
  testthat::expect_equal(s$denominator, "treated_with_either")
  testthat::expect_match(s$use, "must not be used as a pathway probability")
})

test_that("changing SNM uptake does not change the sphincteroplasty RATE", {
  # Under a forced split it necessarily would. This is the structural
  # difference between two independent rates and one share.
  a <- .ai_rates(snm = 40)
  b <- .ai_rates(snm = 400)
  testthat::expect_equal(a$rate[a$treatment == "sphincteroplasty"],
                         b$rate[b$treatment == "sphincteroplasty"])
  # ...but the DERIVED conditional share does move, as it should
  testthat::expect_false(isTRUE(all.equal(
    ai_conditional_treatment_share(a)$value,
    ai_conditional_treatment_share(b)$value)))
})

# ---------------------------------------------------------------------------
# Evidence register
# ---------------------------------------------------------------------------

test_that("no AI treatment evidence is canonical yet", {
  reg <- ai_treatment_evidence_register()
  testthat::expect_gte(nrow(reg), 4L)
  testthat::expect_false(any(as.logical(reg$canonical_compatible)))
  testthat::expect_equal(ai_treatment_rate_status(), "unresolved_requires_source")
})

test_that("the Medicare benchmark records its scope limits", {
  reg <- ai_treatment_evidence_register()
  med <- reg[grepl("Medicare", reg$geography), ]
  testthat::expect_gte(nrow(med), 2L)
  testthat::expect_true(all(med$age_group == "65+"))
  testthat::expect_match(paste(med$transportability, collapse = " "), "older women only")
  # values NOT extracted -- nothing invented from a described study
  testthat::expect_true(all(is.na(med$value) | !nzchar(as.character(med$value))))
})

test_that("the New York study is marked NON-transportable and conditional", {
  reg <- ai_treatment_evidence_register()
  ny <- reg[grepl("New York", reg$geography), ]
  testthat::expect_gte(nrow(ny), 2L)
  testthat::expect_match(paste(ny$transportability, collapse = " "), "NON-TRANSPORTABLE")
  # its denominator is the CONDITIONAL one, not the care population
  testthat::expect_true(all(ny$denominator == "patients treated with SNM or sphincteroplasty"))
})

test_that("PTNS is absent from the definitive-treatment register", {
  # It is conservative/nondefinitive; the Medicare study treats it as a
  # distinct category from SNM and sphincteroplasty.
  reg <- ai_treatment_evidence_register()
  testthat::expect_false(any(grepl("ptns", reg$treatment, ignore.case = TRUE)))
})
