# Tests for R/urps_migration.R — URPS geographic migration module
#
# Guards:
#  1. CONUS_STATE_URBANICITY: correct coverage and values.
#  2. classify_state_urbanicity: lookup, NA for unknown, warning.
#  3. urps_migration_matrix: row sums, diagonal dominance, rural→urban asymmetry.
#  4. urps_migration_matrix: out_of_country sink appended correctly.
#  5. apply_urps_migration: state column updated; left_country flag set.
#  6. agent_urbanicity_summary: fractions sum to 1, correct counts.
#  7. rural_urban_concentration: returns expected columns, shares sum to 1.
#  8. migration_drift_scenario: values threaded into matrix correctly.
#  9. URPS_MIGRATION_HAZARD: values in (0, 1) and career-stage ordering.

# ---- Fixtures ----------------------------------------------------------------

make_agents <- function(n = 20, states = NULL) {
  if (is.null(states)) {
    states <- rep(c("NY", "CA", "WV", "MT"), length.out = n)
  }
  tibble::tibble(
    provider_id = sprintf("p%02d", seq_len(n)),
    state       = states,
    entry_year  = rep(2010L, n),
    age         = rep(45L, n),
    sex         = "female"
  )
}

# ---- 1. CONUS_STATE_URBANICITY -----------------------------------------------

test_that("CONUS_STATE_URBANICITY contains exactly 49 CONUS units", {
  expect_equal(length(CONUS_STATE_URBANICITY), 49L)
})

test_that("CONUS_STATE_URBANICITY contains only 'metro' and 'nonmetro'", {
  expect_true(all(CONUS_STATE_URBANICITY %in% c("metro", "nonmetro")))
})

test_that("Alaska and Hawaii are absent from CONUS_STATE_URBANICITY", {
  expect_false("AK" %in% names(CONUS_STATE_URBANICITY))
  expect_false("HI" %in% names(CONUS_STATE_URBANICITY))
})

test_that("known rural states are classified as nonmetro", {
  expect_equal(CONUS_STATE_URBANICITY[["WV"]], "nonmetro")
  expect_equal(CONUS_STATE_URBANICITY[["MT"]], "nonmetro")
  expect_equal(CONUS_STATE_URBANICITY[["WY"]], "nonmetro")
})

test_that("known urban states are classified as metro", {
  expect_equal(CONUS_STATE_URBANICITY[["NY"]], "metro")
  expect_equal(CONUS_STATE_URBANICITY[["CA"]], "metro")
  expect_equal(CONUS_STATE_URBANICITY[["NJ"]], "metro")
})

# ---- 2. classify_state_urbanicity --------------------------------------------

test_that("classify_state_urbanicity returns correct values for known states", {
  result <- classify_state_urbanicity(c("NY", "WV", "CA", "MT"))
  expect_equal(result, c("metro", "nonmetro", "metro", "nonmetro"))
})

test_that("classify_state_urbanicity returns NA for unknown state with warning", {
  expect_warning(
    out <- classify_state_urbanicity(c("NY", "XX")),
    regexp = "not in urbanicity_lookup"
  )
  expect_equal(out[1], "metro")
  expect_true(is.na(out[2]))
})

test_that("classify_state_urbanicity is vectorised", {
  states <- rep(c("NY", "WV"), 10)
  out    <- classify_state_urbanicity(states)
  expect_length(out, 20L)
  expect_equal(out[seq(1, 20, 2)], rep("metro", 10))
  expect_equal(out[seq(2, 20, 2)], rep("nonmetro", 10))
})

# ---- 3. urps_migration_matrix: structure and row sums -----------------------

test_that("urps_migration_matrix rows sum to 1 for a small state set", {
  states <- c("NY", "CA", "TX", "WV", "MT")
  mat    <- urps_migration_matrix(states, out_of_country_rate = 0)
  row_sums <- tapply(mat$probability, mat$origin, sum)
  expect_true(all(abs(row_sums - 1) < 1e-8))
})

test_that("urps_migration_matrix with out_of_country_rate > 0 rows sum to 1", {
  states <- c("NY", "WV", "MT")
  mat    <- urps_migration_matrix(states, out_of_country_rate = 0.005)
  row_sums <- tapply(mat$probability, mat$origin, sum)
  expect_true(all(abs(row_sums - 1) < 1e-8))
})

test_that("urps_migration_matrix passes validate_migration_matrix", {
  states <- c("NY", "CA", "WV", "MT", "TX")
  mat    <- urps_migration_matrix(states)
  expect_silent(validate_migration_matrix(mat))
})

test_that("diagonal entries are elevated relative to off-diagonal mean", {
  states <- c("NY", "CA", "WV", "MT")
  mat    <- urps_migration_matrix(states, same_state_prior = 0.25,
                                   out_of_country_rate = 0)
  for (st in states) {
    diag_p  <- mat$probability[mat$origin == st & mat$destination == st]
    off_p   <- mat$probability[mat$origin == st & mat$destination != st]
    expect_gt(diag_p, mean(off_p),
              label = paste("diagonal not dominant for origin", st))
  }
})

# ---- 4. Rural-urban asymmetry -----------------------------------------------

test_that("nonmetro origins send more probability mass to metro destinations", {
  states <- c("NY", "CA", "WV", "MT")
  mat    <- urps_migration_matrix(states, out_of_country_rate = 0)

  wv_to_metro <- sum(
    mat$probability[mat$origin == "WV" &
                    mat$destination %in% names(CONUS_STATE_URBANICITY[CONUS_STATE_URBANICITY == "metro"])]
  )
  ny_to_metro <- sum(
    mat$probability[mat$origin == "NY" &
                    mat$destination %in% names(CONUS_STATE_URBANICITY[CONUS_STATE_URBANICITY == "metro"])]
  )
  # Rural WV origin should concentrate more mass on metro destinations than NY
  # (adjusted for the diagonal — NY already has high own-state mass)
  # Compare off-diagonal metro shares
  wv_to_metro_off <- sum(
    mat$probability[mat$origin == "WV" & mat$destination != "WV" &
                    mat$destination %in% names(CONUS_STATE_URBANICITY[CONUS_STATE_URBANICITY == "metro"])]
  )
  ny_to_nonmetro_off <- sum(
    mat$probability[mat$origin == "NY" & mat$destination != "NY" &
                    mat$destination %in% names(CONUS_STATE_URBANICITY[CONUS_STATE_URBANICITY == "nonmetro"])]
  )
  expect_gt(wv_to_metro_off, ny_to_nonmetro_off)
})

test_that("zero rural_to_urban drift still produces a valid matrix", {
  states <- c("NY", "WV", "MT")
  mat    <- urps_migration_matrix(states, rural_to_urban = 0, urban_to_rural = 0,
                                   out_of_country_rate = 0)
  row_sums <- tapply(mat$probability, mat$origin, sum)
  expect_true(all(abs(row_sums - 1) < 1e-8))
})

# ---- 5. out_of_country sink --------------------------------------------------

test_that("out_of_country destination appears when rate > 0", {
  states <- c("NY", "WV")
  mat    <- urps_migration_matrix(states, out_of_country_rate = 0.005)
  expect_true("out_of_country" %in% mat$destination)
})

test_that("out_of_country probability equals rate for each origin", {
  states <- c("NY", "CA", "WV")
  rate   <- 0.003
  mat    <- urps_migration_matrix(states, out_of_country_rate = rate)
  oc     <- mat[mat$destination == "out_of_country", ]
  expect_true(all(abs(oc$probability - rate) < 1e-8))
})

test_that("out_of_country destination absent when rate = 0", {
  mat <- urps_migration_matrix(c("NY", "WV"), out_of_country_rate = 0)
  expect_false("out_of_country" %in% mat$destination)
})

# ---- 6. apply_urps_migration -------------------------------------------------

test_that("apply_urps_migration returns a data frame with the same columns", {
  agents <- make_agents(30)
  mat    <- urps_migration_matrix(c("NY", "CA", "WV", "MT"))
  set.seed(42)
  out    <- apply_urps_migration(agents, year = 2026, migration_matrix = mat)
  expect_true(is.data.frame(out))
  expect_true(all(names(agents) %in% names(out)))
})

test_that("apply_urps_migration adds n_moves and left_country columns", {
  agents <- make_agents(20)
  mat    <- urps_migration_matrix(c("NY", "CA", "WV", "MT"))
  set.seed(99)
  out    <- apply_urps_migration(agents, year = 2026, migration_matrix = mat)
  expect_true("n_moves" %in% names(out))
  expect_true("left_country" %in% names(out))
})

test_that("agents without state column pass through unchanged", {
  agents <- tibble::tibble(provider_id = "p1", age = 45, entry_year = 2010L)
  mat    <- urps_migration_matrix(c("NY", "WV"))
  out    <- apply_urps_migration(agents, 2026, migration_matrix = mat)
  expect_equal(names(out), names(agents))
})

test_that("apply_urps_migration builds matrix on the fly when NULL", {
  agents <- make_agents(10, states = rep(c("NY", "WV"), 5))
  set.seed(7)
  out <- apply_urps_migration(agents, year = 2026, migration_matrix = NULL)
  expect_true(is.data.frame(out))
  expect_true("n_moves" %in% names(out))
})

# ---- 7. agent_urbanicity_summary --------------------------------------------

test_that("agent_urbanicity_summary fractions sum to 1", {
  agents <- make_agents(40, states = c(rep("NY", 30), rep("WV", 10)))
  summ   <- agent_urbanicity_summary(agents)
  expect_equal(sum(summ), 1, tolerance = 1e-10)
})

test_that("agent_urbanicity_summary returns correct metro fraction", {
  agents <- make_agents(40, states = c(rep("NY", 30), rep("WV", 10)))
  summ   <- agent_urbanicity_summary(agents)
  expect_equal(summ[["metro"]],    0.75, tolerance = 1e-10)
  expect_equal(summ[["nonmetro"]], 0.25, tolerance = 1e-10)
  expect_equal(summ[["unknown"]],  0,    tolerance = 1e-10)
})

test_that("agent_urbanicity_summary handles NA states as unknown", {
  agents <- make_agents(10, states = c(rep("NY", 8), NA, NA))
  summ   <- agent_urbanicity_summary(agents)
  expect_equal(summ[["unknown"]], 0.2, tolerance = 1e-10)
})

# ---- 8. rural_urban_concentration -------------------------------------------

test_that("rural_urban_concentration returns expected columns", {
  df <- tibble::tibble(
    year        = rep(2025:2027, each = 4),
    state       = rep(c("NY", "CA", "WV", "MT"), 3),
    effective_fte = rep(c(10, 8, 2, 1), 3)
  )
  out <- rural_urban_concentration(df)
  expect_true(all(c("year", "metro_fte", "nonmetro_fte",
                    "metro_share", "nonmetro_share") %in% names(out)))
})

test_that("rural_urban_concentration shares sum to 1 each year", {
  df <- tibble::tibble(
    year        = rep(2025:2027, each = 4),
    state       = rep(c("NY", "CA", "WV", "MT"), 3),
    effective_fte = rep(c(10, 8, 2, 1), 3)
  )
  out <- rural_urban_concentration(df)
  expect_true(all(abs(out$metro_share + out$nonmetro_share - 1) < 1e-8))
})

test_that("rural_urban_concentration falls back to headcount column", {
  df <- tibble::tibble(
    year      = 2025L,
    state     = c("NY", "WV"),
    headcount = c(100L, 20L)
  )
  out <- rural_urban_concentration(df, fte_col = "effective_fte")
  expect_equal(out$metro_fte + out$nonmetro_fte, 120)
})

# ---- 9. migration_drift_scenario --------------------------------------------

test_that("migration_drift_scenario threads custom drift into the matrix", {
  custom <- migration_drift_scenario(rural_to_urban = 0.30, urban_to_rural = 0.05)
  expect_equal(custom$rural_to_urban, 0.30)
  expect_equal(custom$urban_to_rural, 0.05)

  states <- c("NY", "WV", "MT")
  mat_default <- urps_migration_matrix(states, out_of_country_rate = 0)
  mat_custom  <- urps_migration_matrix(states,
                                        rural_to_urban = custom$rural_to_urban,
                                        urban_to_rural = custom$urban_to_rural,
                                        out_of_country_rate = 0)
  # Custom (lower rural→urban) should shift less mass to urban from WV
  wv_to_ny_default <- mat_default$probability[mat_default$origin == "WV" &
                                              mat_default$destination == "NY"]
  wv_to_ny_custom  <- mat_custom$probability[mat_custom$origin == "WV" &
                                              mat_custom$destination == "NY"]
  expect_lt(wv_to_ny_custom, wv_to_ny_default)
})

# ---- 10. URPS_MIGRATION_HAZARD constants -------------------------------------

test_that("URPS_MIGRATION_HAZARD values are in (0, 1)", {
  expect_true(all(URPS_MIGRATION_HAZARD > 0 & URPS_MIGRATION_HAZARD < 1))
})

test_that("early career hazard exceeds mid and late", {
  expect_gt(URPS_MIGRATION_HAZARD[["early_career"]], URPS_MIGRATION_HAZARD[["mid_career"]])
  expect_gt(URPS_MIGRATION_HAZARD[["mid_career"]],   URPS_MIGRATION_HAZARD[["late_career"]])
})

test_that("URPS_RURAL_URBAN_DRIFT values are in [0, 1]", {
  expect_true(URPS_RURAL_URBAN_DRIFT$rural_to_urban >= 0 &&
              URPS_RURAL_URBAN_DRIFT$rural_to_urban <= 1)
  expect_true(URPS_RURAL_URBAN_DRIFT$urban_to_rural >= 0 &&
              URPS_RURAL_URBAN_DRIFT$urban_to_rural <= 1)
})

test_that("rural_to_urban exceeds urban_to_rural (net rural outflow)", {
  expect_gt(URPS_RURAL_URBAN_DRIFT$rural_to_urban,
            URPS_RURAL_URBAN_DRIFT$urban_to_rural)
})
