# Guards for the E2SFCA distance-decay weight-preset sensitivity registry.

.mk_access_inputs <- function() {
  membership <- tibble::tibble(
    demand_id   = c("X", "Y", "Y", "Z"),
    provider_id = c("A", "A", "B", "B"),
    band        = c(30, 60, 30, 120)
  )
  list(
    membership = membership,
    supply = tibble::tibble(provider_id = c("A", "B"), supply = c(10, 5)),
    demand = tibble::tibble(demand_id = c("X", "Y", "Z"), population = c(1000, 2000, 1500))
  )
}

test_that("the preset registry lists the twostep decay presets", {
  expect_setequal(e2sfca_weight_presets(),
                  c("base", "sharper", "slower", "drop180", "gaussian"))
})

test_that("gaussian band weights are normalised to the 30-min band and decreasing", {
  g <- gaussian_band_weights()
  expect_equal(unname(g[["30"]]), 1)
  expect_true(all(diff(unname(g)) < 0))          # strictly decreasing with distance
  expect_equal(names(g), c("30", "60", "120", "180"))
})

test_that("drop180 zeroes the 180-min band; sharper decays faster than slower", {
  expect_equal(unname(urpssim:::E2SFCA_WEIGHT_PRESETS$drop180[["180"]]), 0)
  expect_lt(urpssim:::E2SFCA_WEIGHT_PRESETS$sharper[["120"]], urpssim:::E2SFCA_WEIGHT_PRESETS$slower[["120"]])
})

test_that("compute_access_preset dispatches and rejects unknown presets", {
  io <- .mk_access_inputs()
  base <- compute_access_preset(io$membership, io$supply, io$demand, preset = "base")
  expect_equal(base$meta$band_weights, urpssim:::E2SFCA_WEIGHT_PRESETS$base[names(base$meta$band_weights)])
  expect_error(compute_access_preset(io$membership, io$supply, io$demand, preset = "nope"),
               "Unknown weight preset")
})

test_that("the sensitivity sweep returns one row per preset with finite means", {
  io <- .mk_access_inputs()
  sweep <- access_weight_sensitivity(io$membership, io$supply, io$demand)
  expect_equal(nrow(sweep), length(e2sfca_weight_presets()))
  expect_true(all(is.finite(sweep$mean_access)))
  expect_true(all(sweep$zero_access_share >= 0 & sweep$zero_access_share <= 100))
  # A sharper decay cannot raise national mean access above the gentler 'slower'
  # preset here (fewer far-provider contributions).
  expect_lte(sweep$mean_access[sweep$preset == "sharper"],
             sweep$mean_access[sweep$preset == "slower"] + 1e-9)
})
