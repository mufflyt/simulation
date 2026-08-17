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
  # DIRECTLY OBSERVED rows only. The derived 0.857 shares this geography but
  # has no denominator_n by construction -- it is a ratio of two rates, not a
  # measured proportion, which is itself why it cannot be a pathway parameter.
  med <- reg[grepl("Medicare", reg$geography) & !is.na(reg$denominator_n), ]
  testthat::expect_gte(nrow(med), 2L)
  testthat::expect_true(all(med$age_group == "65+"))
  testthat::expect_true(all(med$calendar_period == "2010-2018"))
  # The denominator travels with every observed rate -- that is what makes them
  # transportable-or-not rather than free-floating numbers.
  testthat::expect_true(all(med$denominator_n == 33010))
  testthat::expect_match(paste(med$notes, collapse = " "),
                         "not the modelled persistent treatment-requiring AI state")
})

test_that("the New York study is marked non-transportable", {
  reg <- ai_treatment_evidence_register()
  ny <- reg[grepl("New York", reg$geography), ]
  testthat::expect_gte(nrow(ny), 3L)
  testthat::expect_true(all(ny$calendar_period == "2011-2014"))
  testthat::expect_match(paste(ny$notes, collapse = " "),
                         "Non-transportable|NOT stationary|QUALITATIVE")
  testthat::expect_false(any(as.logical(ny$canonical_compatible), na.rm = TRUE))
})

test_that("PTNS is present but labelled conservative, not definitive", {
  # It IS in the register now -- the source treats it as a category distinct
  # from SNM and sphincteroplasty, which is precisely the corroboration that it
  # does not belong in the definitive-procedure state.
  reg <- ai_treatment_evidence_register()
  ptns <- reg[reg$treatment == "ptns", ]
  testthat::expect_equal(nrow(ptns), 1L)
  testthat::expect_match(ptns$notes, "CONSERVATIVE/NONDEFINITIVE")
  testthat::expect_false(as.logical(ptns$canonical_compatible))
})

# ---------------------------------------------------------------------------
# Observed evidence: rates recorded, denominators preserved, nothing inserted
# ---------------------------------------------------------------------------

test_that("the modality categories are NOT mutually exclusive", {
  # The five modality rates sum ABOVE the any-treatment rate, so they cannot be
  # a partition and must never be normalised into one.
  reg <- ai_treatment_evidence_register()
  mods <- reg[reg$treatment %in% c("anal_procedures", "snm", "ptns",
                                   "sphincteroplasty", "pfpt_biofeedback") &
                !is.na(reg$denominator_n) & reg$denominator_n == 33010, ]
  any_tx <- reg$value[reg$treatment == "any_studied_treatment"]
  testthat::expect_gt(sum(mods$value), any_tx)
  testthat::expect_true(all(as.logical(mods$mutually_exclusive) == FALSE))
})

test_that("modality numerators were NOT reconstructed from rounded rates", {
  # Multiplying a rounded percentage by 33,010 would fabricate false precision.
  reg <- ai_treatment_evidence_register()
  mods <- reg[reg$treatment %in% c("snm", "sphincteroplasty", "ptns",
                                   "pfpt_biofeedback", "anal_procedures") &
                !is.na(reg$denominator_n) & reg$denominator_n == 33010, ]
  testthat::expect_true(all(is.na(mods$observed_n)))
  # ...whereas the quantities actually reported as counts ARE recorded
  any_tx <- reg[reg$treatment == "any_studied_treatment", ]
  testthat::expect_equal(any_tx$observed_n, 3160)
  testthat::expect_equal(any_tx$denominator_n, 33010)
})

test_that("the derived conditional mix is stored but non-canonical", {
  reg <- ai_treatment_evidence_register()
  d <- reg[grepl("received SNM or sphincteroplasty", reg$quantity), ]
  testthat::expect_equal(round(d$value, 3), 0.857)
  testthat::expect_false(as.logical(d$canonical_compatible))
  testthat::expect_match(d$notes, "NOT A PATHWAY PROBABILITY")
})

test_that("the SNM test-to-implant state is registered", {
  # A pathway state the model currently lacks entirely.
  reg <- ai_treatment_evidence_register()
  s1 <- reg[grepl("permanent implant \\| SNM stage 1", reg$quantity), ]
  testthat::expect_equal(s1$value, 0.797)
  testthat::expect_equal(s1$denominator_n, 621)
  testthat::expect_lt(s1$value, 1)   # must not be collapsed into the implant state
})

test_that("device revision is registered as maintenance, not recurrence", {
  reg <- ai_treatment_evidence_register()
  rev <- reg[grepl("revision/replacement/explant", reg$quantity), ]
  testthat::expect_equal(rev$value, 0.065)
  testthat::expect_equal(rev$denominator_n, 495)
  testthat::expect_match(rev$notes, "NOT clinical recurrence|never populate a recurrence kernel")
})

test_that("NOTHING is canonical, including the strongest evidence", {
  # 0.024 and 0.004 are real observed rates but their denominator is
  # claims-diagnosed FI among older Medicare women, not the modelled state.
  reg <- ai_treatment_evidence_register()
  testthat::expect_false(any(as.logical(reg$canonical_compatible), na.rm = TRUE))
  snm <- reg[reg$treatment == "snm" & !is.na(reg$denominator_n) &
               reg$denominator_n == 33010, ]
  testthat::expect_equal(snm$value, 0.024)
  testthat::expect_match(snm$notes, "not the modelled persistent treatment-requiring AI state")
})

test_that("the realized-care-not-need finding is recorded", {
  # Only 9.6% of women with a claims FI diagnosis received any studied
  # treatment -- direct evidence that observed treatment is a realized-care
  # transition under access, not a latent-need probability.
  reg <- ai_treatment_evidence_register()
  any_tx <- reg[reg$treatment == "any_studied_treatment", ]
  testthat::expect_equal(any_tx$value, 0.096)
  testthat::expect_match(any_tx$notes, "REALIZED-CARE transition")
  testthat::expect_match(any_tx$notes, "NOT a latent-need probability")
})
