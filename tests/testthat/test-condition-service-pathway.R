# Condition-specific service pathway (R/51-condition_service_pathway.R).
#
# The point of the cascade is that downstream stages are CONDITIONAL on upstream
# ones. The flat map it replaces could not express that: a UI patient drew PTNS
# and a sling in the same year as independent annual rates. These tests lock the
# conditioning, the validation that keeps the table honest, and the fact that
# post-operative follow-up now exists at all.

mk_pathway <- function() {
  tibble::tribble(
    ~condition, ~stage,         ~service,             ~per_entering, ~p_advance,
    "ui",       "conservative", "new_consultation",   1.0,           0.50,
    "ui",       "conservative", "return_visit",       2.0,           0.50,
    "ui",       "testing",      "urodynamics",        1.0,           0.40,
    "ui",       "procedure",    "sling_procedure",    1.0,           1.00,
    "ui",       "followup",     "postoperative_care", 2.0,           0.10,
    "ui",       "recurrence",   "new_consultation",   1.0,           NA
  )
}

test_that("entrants compound p_advance down the cascade", {
  e <- pathway_stage_entrants(c(ui = 1000), mk_pathway())
  got <- stats::setNames(e$entering, e$stage)
  expect_equal(unname(got[["conservative"]]), 1000)
  expect_equal(unname(got[["testing"]]), 500)      # 1000 * 0.50
  expect_equal(unname(got[["procedure"]]), 200)    # 500  * 0.40
  expect_equal(unname(got[["followup"]]), 200)     # 200  * 1.00
  expect_equal(unname(got[["recurrence"]]), 20)    # 200  * 0.10
})

test_that("a procedure accrues only to patients who reached it", {
  # This is the dependency the flat map could not express: with 1000 treated and
  # per_entering = 1, a flat map yields 1000 slings; the cascade yields 200.
  v <- pathway_service_volumes(c(ui = 1000), 2025L, mk_pathway())
  sling <- v$volume[v$service == "sling_procedure"]
  expect_equal(sling, 200)
  expect_lt(sling, 1000)
})

test_that("post-operative follow-up is generated, which the flat map never did", {
  v <- pathway_service_volumes(c(ui = 1000), 2025L, mk_pathway())
  expect_true("postoperative_care" %in% v$service)
  expect_equal(v$volume[v$service == "postoperative_care"], 400)  # 200 entering * 2.0
})

test_that("services are summed across conditions and stages", {
  # new_consultation appears in both conservative and recurrence; the collapsed
  # result must add them rather than keep the first.
  v <- pathway_service_volumes(c(ui = 1000), 2025L, mk_pathway())
  expect_equal(v$volume[v$service == "new_consultation"], 1000 + 20)
  expect_equal(nrow(v[v$service == "new_consultation", ]), 1L)
})

test_that("by_stage keeps the audit trail", {
  v <- pathway_service_volumes(c(ui = 1000), 2025L, mk_pathway(), by_stage = TRUE)
  expect_true(all(c("condition", "stage") %in% names(v)))
  expect_setequal(unique(v$stage),
                  c("conservative", "testing", "procedure", "followup", "recurrence"))
})

test_that("a terminal NA p_advance does not carry the cohort forward", {
  # Treating NA as "advance everyone" would invent an infinite tail of
  # recurrences; it must terminate.
  p <- mk_pathway()
  e <- pathway_stage_entrants(c(ui = 1000), p)
  expect_equal(nrow(e), 5L)
  expect_true(all(is.finite(e$entering)))
})

# ---- validation ------------------------------------------------------------

test_that("an unknown stage name is rejected", {
  p <- mk_pathway(); p$stage[1] <- "triage"
  expect_error(validate_condition_pathway(p), "Unknown pathway stage")
})

test_that("p_advance must be constant within a condition-stage", {
  # Divergent values make the next stage's entrant count ambiguous.
  p <- mk_pathway(); p$p_advance[2] <- 0.90
  expect_error(validate_condition_pathway(p), "not constant")
})

test_that("a stage with no upstream stage is rejected", {
  # Otherwise it silently receives nobody and reads as "this condition has no
  # surgery" rather than "a row is missing".
  p <- mk_pathway()[mk_pathway()$stage != "testing", ]
  expect_error(validate_condition_pathway(p), "missing upstream")
})

test_that("a service absent from the workload basket is caught early", {
  p <- mk_pathway(); p$service[3] <- "anorectal_manometry"
  expect_error(
    validate_condition_pathway(p, known_services = c("new_consultation", "return_visit",
                                                     "sling_procedure", "postoperative_care")),
    "absent from the workload basket")
})

test_that("probabilities outside [0, 1] are rejected", {
  p <- mk_pathway(); p$p_advance[1:2] <- 1.4
  expect_error(validate_condition_pathway(p), "probability")
})

# ---- the shipped table -----------------------------------------------------

test_that("the shipped pathway validates against the real workload basket", {
  p <- condition_service_pathway()
  expect_true(validate_condition_pathway(p, known_services = urps_service_workload()$service))
  expect_setequal(unique(p$condition), c("ui", "pop", "ai"))
})

test_that("the shipped pathway is declared uncalibrated", {
  # Every row is expert judgement. If this ever reports otherwise without the
  # sources being filled in, the calibration gate has been defeated.
  expect_identical(condition_pathway_status(), "uncalibrated_illustrative")
})

test_that("every shipped row carries provenance", {
  p <- condition_service_pathway()
  expect_true(all(c("confidence", "source", "notes") %in% names(p)))
  expect_false(any(is.na(p$source) | !nzchar(p$source)))
})

# ---- integration with the life-course pipeline ------------------------------

test_that("the staged pathway is the default and is recorded in meta", {
  pop <- data.frame(age = 40:85, population = 1e5)
  s <- simulate_lifecourse_demand(pop, 2025L, n = 3000, seed = 1)
  expect_identical(s$meta$service_pathway, "condition_staged")
  expect_identical(s$meta$pathway_status, "uncalibrated_illustrative")
  expect_false(is.null(s$meta$stage_volumes))
})

test_that("the flat map remains reproducible for comparison", {
  pop <- data.frame(age = 40:85, population = 1e5)
  flat <- simulate_lifecourse_demand(pop, 2025L, n = 3000, seed = 1,
                                     use_condition_pathway = FALSE)
  expect_identical(flat$meta$service_pathway, "flat_service_map")
  # The flat map generated no post-operative follow-up; that is the gap the
  # staged pathway closes.
  expect_false("postoperative_care" %in% flat$service_volumes$service)
  staged <- simulate_lifecourse_demand(pop, 2025L, n = 3000, seed = 1)
  expect_true("postoperative_care" %in% staged$service_volumes$service)
})

test_that("staged volumes still convert to FTE (every service matches)", {
  pop <- data.frame(age = 40:85, population = 1e5)
  s <- simulate_lifecourse_demand(pop, 2025L, n = 3000, seed = 1)
  f <- convert_workload_to_fte(s$service_volumes,
                               wrvu_per_fte = WRVU_PER_FTE_BENCHMARK[["median"]])
  expect_true(is.finite(f$required_fte))
  expect_gt(f$required_fte, 0)
})
