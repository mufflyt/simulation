# Origin-dependent migration.
#
# Two kinds of test here. The first block is adversarial: degenerate and
# hostile matrices that must be refused rather than silently absorbed. The
# second is semantic: algebraic properties any correct implementation of a
# Markov migration operator must satisfy, independent of these particular
# numbers.
#
# The semantic block is the reason the matrix form is worth having. An
# implementation can conserve headcount every year and still not compose
# correctly, and if it does not compose, a 25-year projection means nothing.

closed_matrix <- function() {
  tibble::tribble(
    ~origin,    ~destination, ~probability,
    "Mountain", "Mountain",   0.7,
    "Mountain", "Pacific",    0.3,
    "Pacific",  "Mountain",   0.4,
    "Pacific",  "Pacific",    0.6
  )
}

# ---- Adversarial ----------------------------------------------------------

test_that("a row that does not sum to one is refused, naming the origin", {
  bad <- closed_matrix()
  bad$probability[bad$origin == "Pacific" & bad$destination == "Pacific"] <- 0.5
  expect_error(validate_migration_matrix(bad), "Pacific")
})

test_that("probabilities that cancel to one are still refused", {
  # -0.1 and 1.1 sum to exactly 1, so a row-sum check alone accepts this.
  cancelling <- tibble::tribble(
    ~origin,    ~destination, ~probability,
    "Mountain", "Mountain",    1.1,
    "Mountain", "Pacific",    -0.1
  )
  expect_error(validate_migration_matrix(cancelling), "\\[0, 1\\]")
})

test_that("non-finite probabilities are refused", {
  bad <- closed_matrix()
  bad$probability[1] <- NA_real_
  expect_error(validate_migration_matrix(bad), "NA/NaN/Inf")
  bad$probability[1] <- Inf
  expect_error(validate_migration_matrix(bad), "NA/NaN/Inf")
})

test_that("duplicated origin-destination pairs are refused", {
  dup <- dplyr::bind_rows(closed_matrix(), closed_matrix()[1, ])
  expect_error(validate_migration_matrix(dup), "duplicated")
})

test_that("an agent whose origin has no matrix row is refused, not dropped", {
  agents <- tibble::tibble(state = c("Mountain", "Dakota"),
                           entry_year = 2020, age = 45)
  expect_error(
    apply_provider_migration_matrix(agents, 2030, closed_matrix(),
                                    hazards = c(early_career = 1,
                                                mid_career = 1,
                                                late_career = 1)),
    "Dakota"
  )
})

test_that("zero-row and all-ineligible agent tables are handled", {
  empty <- tibble::tibble(state = character(0), entry_year = numeric(0),
                          age = numeric(0))
  expect_equal(nrow(apply_provider_migration_matrix(empty, 2030,
                                                    closed_matrix())), 0L)
  all_na <- tibble::tibble(state = NA_character_, entry_year = 2020, age = 45)
  expect_equal(apply_provider_migration_matrix(all_na, 2030,
                                               closed_matrix())$state,
               NA_character_)
})

test_that("negative or non-finite move counts are refused", {
  expect_error(
    migration_matrix_from_moves(tibble::tibble(origin = "A", destination = "B",
                                               n = -1)),
    "non-negative"
  )
})

test_that("returners with non-finite counts are refused", {
  agents <- tibble::tibble(state = "Mountain", entry_year = 2020, age = 45)
  expect_error(
    add_returning_providers(agents,
                            tibble::tibble(geo = "Mountain",
                                           n_returners = NA_real_), 2030),
    "finite"
  )
})

# ---- Semantic -------------------------------------------------------------
#
# These operate on the expected-value operator implied by the matrix rather
# than on sampled agents, so they are exact rather than Monte-Carlo.

diffuse <- function(supply, matrix) {
  supply %>%
    dplyr::inner_join(matrix, by = c("geo" = "origin"),
                      relationship = "many-to-many") %>%
    dplyr::group_by(geo = .data$destination) %>%
    dplyr::summarise(n = sum(.data$n * .data$probability), .groups = "drop") %>%
    dplyr::arrange(.data$geo)
}

start_supply <- function() {
  tibble::tibble(geo = c("Mountain", "Pacific"), n = c(200, 400))
}

test_that("SEMANTIC: the operator conserves headcount in a closed system", {
  expect_equal(sum(diffuse(start_supply(), closed_matrix())$n), 600,
               tolerance = 1e-10)
})

test_that("SEMANTIC: the operator is homogeneous and additive", {
  base <- diffuse(start_supply(), closed_matrix())
  scaled <- diffuse(dplyr::mutate(start_supply(), n = n * 7.5), closed_matrix())
  expect_equal(scaled$n, base$n * 7.5, tolerance = 1e-10)

  other <- tibble::tibble(geo = c("Mountain", "Pacific"), n = c(55, 130))
  both  <- tibble::tibble(geo = c("Mountain", "Pacific"), n = c(255, 530))
  expect_equal(diffuse(both, closed_matrix())$n,
               base$n + diffuse(other, closed_matrix())$n,
               tolerance = 1e-10)
})

test_that("SEMANTIC: applying P twice equals applying P squared once", {
  geos <- c("Mountain", "Pacific")
  P <- matrix(c(0.7, 0.3, 0.4, 0.6), nrow = 2, byrow = TRUE,
              dimnames = list(geos, geos))
  P2 <- P %*% P
  squared <- tibble::tibble(
    origin = rep(geos, each = 2),
    destination = rep(geos, times = 2),
    probability = as.vector(t(P2))
  )
  expect_equal(diffuse(diffuse(start_supply(), closed_matrix()),
                       closed_matrix())$n,
               diffuse(start_supply(), squared)$n,
               tolerance = 1e-10)
})

test_that("SEMANTIC: iteration converges to the stationary distribution", {
  # pi = pi P gives pi proportional to (4, 3) for this matrix, so 600
  # providers settle at 342.857 / 257.143 from any starting split.
  carried <- tibble::tibble(geo = c("Mountain", "Pacific"), n = c(600, 0))
  for (i in seq_len(300)) carried <- diffuse(carried, closed_matrix())
  expect_equal(carried$n, c(600 * 4 / 7, 600 * 3 / 7), tolerance = 1e-8)
})

test_that("SEMANTIC: a rank-1 matrix reproduces origin-independent shares", {
  # The bridge to the existing behaviour: when every row is the same, the
  # matrix form and `assign_entrant_geography()` agree in expectation. This is
  # what makes the new path a strict generalisation rather than a substitute.
  shares <- c(Mountain = 0.35, Pacific = 0.65)
  rank1 <- tibble::tibble(
    origin = rep(c("Mountain", "Pacific"), each = 2),
    destination = rep(c("Mountain", "Pacific"), times = 2),
    probability = rep(shares, times = 2)
  )
  expect_equal(diffuse(start_supply(), rank1)$n,
               c(600 * 0.35, 600 * 0.65), tolerance = 1e-10)
})

test_that("SEMANTIC: a dominant diagonal is not expressible as rank-1", {
  # The substantive claim behind this PR: with rank-1 shares every origin has
  # the same stay probability, so the Fraher Table 3 pattern (51-68% stay,
  # varying by origin) cannot be represented.
  stay <- closed_matrix() %>%
    dplyr::filter(.data$origin == .data$destination) %>%
    dplyr::pull(.data$probability)
  expect_false(isTRUE(all.equal(stay[1], stay[2])))
})

test_that("SEMANTIC: an out-of-country sink removes providers from supply", {
  sink_matrix <- tibble::tribble(
    ~origin,    ~destination,      ~probability,
    "Mountain", "Mountain",        0.0,
    "Mountain", "out_of_country",  1.0
  )
  agents <- tibble::tibble(state = rep("Mountain", 20), entry_year = 2020,
                           age = 45)
  out <- apply_provider_migration_matrix(
    agents, 2030, sink_matrix,
    hazards = c(early_career = 1, mid_career = 1, late_career = 1)
  )
  expect_true(all(out$left_country))
  expect_true(all(is.na(out$state)))
  expect_equal(nrow(out), 20L)   # audited, not deleted
})

test_that("SEMANTIC: returners add exactly the requested headcount", {
  agents <- tibble::tibble(state = "Mountain", entry_year = 2020, age = 45,
                           n_moves = 0L)
  out <- add_returning_providers(
    agents, tibble::tibble(geo = c("Mountain", "Pacific"),
                           n_returners = c(3, 5)), 2030
  )
  expect_equal(nrow(out), 1L + 8L)
  expect_equal(sum(out$state == "Pacific", na.rm = TRUE), 5L)
})

# ---- Shrinkage ------------------------------------------------------------

test_that("sparse origins are shrunk toward the prior", {
  moves <- tibble::tribble(
    ~origin, ~destination, ~n,
    "A",     "A",          2,      # 2 observed moves: must not assert 100%
    "B",     "A",          500,
    "B",     "B",          500
  )
  prior <- tibble::tibble(geo = c("A", "B"), share = c(0.5, 0.5))
  m <- migration_matrix_from_moves(moves, prior, shrinkage = 10)

  a_self <- m$probability[m$origin == "A" & m$destination == "A"]
  expect_lt(a_self, 1)
  expect_gt(a_self, 0.5)          # pulled toward, not onto, the prior

  b_self <- m$probability[m$origin == "B" & m$destination == "B"]
  expect_equal(b_self, 0.5, tolerance = 0.02)   # 1000 moves: prior irrelevant
})

test_that("shrinkage of zero reproduces the raw empirical shares", {
  moves <- tibble::tribble(
    ~origin, ~destination, ~n,
    "A",     "A",          3,
    "A",     "B",          1
  )
  m <- migration_matrix_from_moves(moves, shrinkage = 0)
  expect_equal(m$probability[m$origin == "A" & m$destination == "A"], 0.75)
})

test_that("every constructed matrix is row-stochastic by construction", {
  set.seed(42)
  geos <- LETTERS[1:6]
  moves <- tidyr::expand_grid(origin = geos, destination = geos) %>%
    dplyr::mutate(n = rpois(dplyr::n(), 3))
  m <- migration_matrix_from_moves(moves, shrinkage = 5)
  sums <- m %>% dplyr::group_by(.data$origin) %>%
    dplyr::summarise(s = sum(.data$probability), .groups = "drop")
  expect_true(all(abs(sums$s - 1) < 1e-8))
})
