test_that("compositional draws are bit-for-bit reproducible given seed", {
  draws1 <- draw_compositional_service_shares(n_draws = 10, seed = 42L)
  draws2 <- draw_compositional_service_shares(n_draws = 10, seed = 42L)

  expect_equal(draws1$share, draws2$share)
})
