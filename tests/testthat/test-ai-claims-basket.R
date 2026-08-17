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
  expect_match(ap$note, "STILL NOT DEFINED, and deliberately so")
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

# ---------------------------------------------------------------------------
# Expanded from a payer clinical policy for FI treatments
# ---------------------------------------------------------------------------

test_that("the sphincteroplasty family is FOUR codes, not one", {
  # A 46750-only basket misses 46760 (muscle transplant) and 46761 (levator
  # imbrication / Park posterior anal repair) entirely.
  b <- .basket()
  codes <- vapply(b$treatments$sphincteroplasty$cpt, function(x) x$code, character(1))
  expect_true(all(c("46750", "46760", "46761") %in% codes))
  expect_match(b$treatments$sphincteroplasty$family_note, "FOUR CODES, NOT ONE")
})

test_that("the paediatric code is excluded and the unlisted code is flagged", {
  b <- .basket()
  cpt <- b$treatments$sphincteroplasty$cpt
  child <- Filter(function(x) x$code == "46751", cpt)[[1]]
  expect_false(child$include)
  unlisted <- Filter(function(x) x$code == "46999", cpt)[[1]]
  expect_equal(unlisted$include, "review")
  expect_match(unlisted$note, "catches any anal procedure")
})

test_that("SNM reprogramming codes are present and marked maintenance", {
  # 95970/95971/95972 capture the reprogramming attempted BEFORE revision, so
  # a revision-only view understates device-maintenance workload.
  b <- .basket()
  prog <- b$treatments$sacral_neuromodulation$device_programming_cpt
  codes <- vapply(prog, function(x) x$code, character(1))
  expect_setequal(codes, c("95970", "95971", "95972"))
  expect_true(all(vapply(prog, function(x) x$stage, character(1)) == "device_maintenance"))
  expect_match(b$treatments$sacral_neuromodulation$device_programming_note,
               "UNDERSTATES device-maintenance workload")
  expect_match(b$treatments$sacral_neuromodulation$device_programming_note,
               "never populate g_k")
})

test_that("the reprogramming codes carry the WIDEST indication warning", {
  # Their descriptors span brain, cranial nerve, spinal cord and peripheral
  # nerve generators -- broader even than 64590.
  b <- .basket()
  expect_match(b$treatments$sacral_neuromodulation$device_programming_note,
               "brain, cranial nerve, spinal cord")
  expect_match(b$treatments$sacral_neuromodulation$device_programming_note,
               "INDICATION RISK IS EXTREME")
})

test_that("HCPCS device supplies are separated from professional services", {
  # Summing supply codes with CPT implantation codes double-counts one episode.
  b <- .basket()
  h <- b$treatments$sacral_neuromodulation$hcpcs_device_supplies
  codes <- vapply(h, function(x) x$code, character(1))
  expect_true("A4290" %in% codes)
  expect_true(any(grepl("^L86", codes)))
  expect_match(b$treatments$sacral_neuromodulation$hcpcs_note, "DOUBLE-COUNT")
  # A4290 is the TEST lead specifically -- one of the few supply codes that
  # helps separate test from implant.
  a4290 <- Filter(function(x) x$code == "A4290", h)[[1]]
  expect_equal(a4290$stage, "test_or_lead")
})

test_that("PTNS non-coverage for FI is recorded", {
  # Observed PTNS volume reflects coverage policy as well as clinical choice.
  b <- .basket()
  expect_match(b$treatments$ptns$coverage_note, "do NOT support coverage")
})

test_that("anal_procedures remains EMPTY, with candidates recorded not adopted", {
  b <- .basket()
  ap <- b$treatments$anal_procedures
  expect_length(ap$cpt, 0L)
  expect_equal(ap$status, "NOT_DEFINED")
  # the search is recorded so it need not be repeated, but nothing is adopted
  expect_gte(length(ap$candidates_not_adopted), 2L)
  expect_match(ap$why_still_empty, "guessing with extra steps")
})

test_that("the payer ICD range is recorded as evidence, NOT as resolution", {
  # A coverage rule is not a research case definition, and the difference is
  # stated rather than glossed.
  b <- .basket()
  n <- b$diagnosis$fecal_incontinence$notes
  expect_match(n, "R15.0-R15.9")
  expect_match(n, "COVERAGE rule is not a research case definition")
  # the review flags are UNCHANGED by that evidence
  d <- b$diagnosis$fecal_incontinence$icd10
  flags <- setNames(lapply(d, function(x) x$include), vapply(d, function(x) x$code, character(1)))
  expect_equal(flags[["R15.0"]], "review")
  expect_equal(flags[["R15.2"]], "review")
})
