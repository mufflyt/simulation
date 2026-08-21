test_that("acgme_procedural_minimums reflects exact 2025 Case Log rules", {
  mins <- acgme_procedural_minimums()

  expect_equal(mins$acgme_minimum[mins$category == "Sacrocolpopexy"], 20L)
  expect_equal(mins$acgme_minimum[mins$category == "Sling procedures"], 50L)
  expect_equal(mins$acgme_minimum[mins$category == "Urinary fistula repair"], 2L)
  expect_equal(mins$acgme_minimum[mins$category == "Rectovaginal fistula repair"], 1L)
  expect_equal(mins$acgme_minimum[mins$category == "Prolapse operations"], 130L)
  expect_equal(mins$acgme_minimum[mins$category == "Urethrolysis"], 0L)
})

test_that("acgme_program_requirements does not impose false 2.0 clinical FTE rule", {
  reqs <- acgme_program_requirements()
  expect_false(reqs$has_faculty_fte_per_fellow_rule)
  expect_true(reqs$requires_urologist_faculty)
  expect_true(reqs$requires_obgyn_faculty)
})

test_that("simulate_acgme_fellowship_capacity correctly identifies procedural case bottlenecks", {
  prog <- tibble::tribble(
    ~program_id, ~year, ~approved_complement, ~faculty_urologists, ~faculty_obgyn, ~funding_slots, ~site_capacity,
    "Prog1", 2025L, 4L, 1L, 1L, 4L, 4L,
    "Prog2", 2025L, 4L, 0L, 1L, 4L, 4L # Missing Urologist faculty
  )

  case_vol <- tibble::tribble(
    ~program_id, ~category, ~annual_case_volume, ~fellow_accessible_share, ~qualifying_role_prob,
    "Prog1", "Sacrocolpopexy", 10, 1.0, 1.0, # 10*3 = 30 cases / 20 = 1 fellow capacity (Bottleneck!)
    "Prog1", "Sling procedures", 100, 1.0, 1.0,
    "Prog2", "Sacrocolpopexy", 100, 1.0, 1.0,
    "Prog2", "Sling procedures", 100, 1.0, 1.0
  )

  sim <- simulate_acgme_fellowship_capacity(prog, case_vol)
  res <- sim$program_capacity

  prog1_res <- dplyr::filter(res, program_id == "Prog1")
  prog2_res <- dplyr::filter(res, program_id == "Prog2")

  expect_equal(prog1_res$max_simulated_capacity, 1L)
  expect_match(prog1_res$primary_bottleneck, "Sacrocolpopexy")
  expect_equal(prog2_res$max_simulated_capacity, 0L)
  expect_match(prog2_res$primary_bottleneck, "Faculty Composition")
})
