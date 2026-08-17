# AI claims basket.
#
# NOT an extraction of Halani et al's code list -- that paper is paywalled with
# no PMC deposit, so its definitions could not be read. This basket is
# independently constructed from primary code sources and aligned to their
# published categories. These tests pin the traps that make a naive AI basket
# wrong.

.basket <- function() {
  p <- system.file("extdata", "ai_claims_basket.yml", package = "urpssim")
  if (!nzchar(p) || !file.exists(p)) {
    p <- if (file.exists("config/ai_claims_basket.yml")) "config/ai_claims_basket.yml"
         else "../../config/ai_claims_basket.yml"
  }
  skip_if_not(file.exists(p), "claims basket not present")
  yaml::read_yaml(p)
}

test_that("the basket does NOT claim to replicate the source paper", {
  # It is aligned to published CATEGORIES, not copied from a code list nobody
  # here has seen. Overstating that would make a comparison of counts look like
  # a validation when it is a comparison of two different code sets.
  txt <- paste(readLines(system.file("extdata", "ai_claims_basket.yml",
                                     package = "urpssim") |>
                           (\(p) if (nzchar(p) && file.exists(p)) p else
                             "../../config/ai_claims_basket.yml")()),
               collapse = " ")
  expect_match(txt, "NOT an extraction|INDEPENDENTLY CONSTRUCTED")
  expect_match(txt, "paywalled")
})

test_that("SNM codes are flagged as NOT indication-specific", {
  b <- .basket()
  snm <- b$treatments$sacral_neuromodulation
  expect_equal(snm$indication_specific, "none")
  expect_match(snm$indication_warning, "urinary urgency/OAB")
  expect_true(snm$definitive)
})

test_that("PTNS is flagged NOT indication-specific AND session-counted", {
  # Two separate traps. 64566 is described in overactive-bladder terms, and it
  # is a SINGLE TREATMENT in a repeated series -- counting claims counts
  # sessions, not patients.
  b <- .basket()
  p <- b$treatments$ptns
  expect_equal(p$indication_specific, "none")
  expect_match(p$unit_warning, "SESSIONS, not patients")
  expect_false(isTRUE(p$definitive))
})

test_that("PTNS is recorded as conservative, matching the topology audit", {
  b <- .basket()
  expect_match(b$treatments$ptns$placement_note, "CONSERVATIVE, not definitive")
})

test_that("the biofeedback era break is carried, both directions", {
  # 90911 deleted 2020-01-01, replaced by 90912/90913. A basket with only one
  # side silently returns zero for the other era.
  b <- .basket()
  bf <- b$treatments$pelvic_floor_pt_biofeedback
  codes <- vapply(bf$cpt, function(x) x$code, character(1))
  expect_setequal(codes, c("90911", "90912", "90913"))
  expect_match(bf$era_warning, "silently return zero")
  old <- Filter(function(x) x$code == "90911", bf$cpt)[[1]]
  expect_equal(old$valid_through, "2019-12-31")
})

test_that("the ICD-9 era is covered, not just ICD-10", {
  # A 2010-2018 Medicare series spans the 2015-10-01 transition. ICD-10 alone
  # truncates the early years.
  b <- .basket()
  d <- b$diagnosis$fecal_incontinence
  expect_true(length(d$icd9) >= 1L)
  expect_equal(d$icd9[[1]]$code, "787.6")
  expect_match(d$icd9[[1]]$note, "BOTH systems|silently truncates")
})

test_that("ambiguous FI diagnosis codes are flagged for review, not assumed", {
  # R15.0 (incomplete defecation) and R15.2 (urgency) are not necessarily
  # incontinence, and including them changes the denominator materially.
  b <- .basket()
  d <- b$diagnosis$fecal_incontinence$icd10
  flags <- setNames(lapply(d, function(x) x$include), vapply(d, function(x) x$code, character(1)))
  expect_equal(flags[["R15.0"]], "review")
  expect_equal(flags[["R15.2"]], "review")
  expect_true(isTRUE(flags[["R15.9"]]))
  expect_true(isTRUE(flags[["R15.1"]]))
})

test_that("anal_procedures is EMPTY rather than guessed", {
  # The largest observed category (6.5%). Guessing its contents would let it
  # dominate any AI workload estimate while corresponding to nothing.
  b <- .basket()
  ap <- b$treatments$anal_procedures
  expect_equal(ap$status, "NOT_DEFINED")
  expect_length(ap$cpt, 0L)
  expect_match(ap$note, "Leaving it EMPTY rather than guessing")
})

test_that("SNM lead and generator codes are distinguishable by stage", {
  # The test-to-implant transition lives between them. Lumping all five codes
  # destroys the state the evidence exposed.
  b <- .basket()
  st <- vapply(b$treatments$sacral_neuromodulation$cpt,
               function(x) x$stage, character(1))
  expect_true(all(c("test_or_lead", "permanent_implant", "device_maintenance") %in% st))
})

test_that("device-maintenance codes are labelled maintenance, not recurrence", {
  b <- .basket()
  cpt <- b$treatments$sacral_neuromodulation$cpt
  maint <- Filter(function(x) x$stage == "device_maintenance", cpt)
  expect_setequal(vapply(maint, function(x) x$code, character(1)), c("64585", "64595"))
  expect_match(paste(b$open_questions, collapse = " "), "maintenance, NOT recurrence")
})

test_that("the basket is NOT wired into URPS_CPT_BASKET yet", {
  # Candidate status. Every entry needs clinical review first.
  basket <- urps_service_workload()$service
  expect_false(any(grepl("64561|64590|46750|64566|90912", basket)))
})
