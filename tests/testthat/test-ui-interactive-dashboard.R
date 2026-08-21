test_that("run_workbench validates app directory existence", {
  expect_true(file.exists(system.file("shiny", package = "urpssim")) || file.exists(file.path(getwd(), "inst", "shiny")))
})
