# The 2023 URPS baseline dependency chain and its consumption.
#
#   mufflyaccess::urps_count()   (upstream SSOT contract)
#     -> urps_baseline_supply()  (simulation's canonical adapter)
#       -> scripts consume $national
#
# These pin the adapter to the contract and enforce that the affected scripts no
# longer hand-type the current national baseline. This is NOT a global ban on the
# literal 1306 -- historical fixtures / frozen artifacts may legitimately carry it.

suppressPackageStartupMessages(library(here))

test_that("adapter is pinned to the upstream contract (national, not CONUS)", {
  skip_if_not(requireNamespace("mufflyaccess", quietly = TRUE), "mufflyaccess not installed")
  b <- suppressMessages(urps_baseline_supply())
  expect_equal(
    b$national,
    mufflyaccess::urps_count(year = 2023L, measure = "board_certified_active",
                             geography = "national", include_urology = TRUE))
  # $national is the intended denominator; guard against a later CONUS swap.
  expect_false(identical(b$national, b$conus))
})

test_that("affected scripts consume the adapter, not a hand-typed national baseline", {
  affected <- c(
    "scripts/diagnostics/career_change_sensitivity.R",
    "scripts/validation/02_monte_carlo_convergence.R",
    "scripts/validation/03_utilization_fte_triangulation.R",
    "scripts/validation/04_delegation_claims_evidence.R",
    "R/supply-provider_microsimulation.R"
  )
  offenders <- character(0)
  for (rel in affected) {
    p <- here::here(rel)
    if (!file.exists(p)) next
    lines <- readLines(p, warn = FALSE)
    for (i in seq_along(lines)) {
      ln <- lines[i]
      if (grepl("1306", ln, fixed = TRUE) && !grepl("^[[:space:]]*#", ln))
        offenders <- c(offenders, sprintf("%s:%d: %s", rel, i, trimws(ln)))
    }
  }
  expect_equal(
    length(offenders), 0L,
    info = paste0(
      "These files should consume urps_baseline_supply()$national, not a hand-typed ",
      "1306. Non-comment 1306 found:\n", paste(offenders, collapse = "\n")))
})

test_that("negative control: the script-guard detector distinguishes code from comment", {
  is_hardcode <- function(ln) grepl("1306", ln, fixed = TRUE) && !grepl("^[[:space:]]*#", ln)
  expect_true(is_hardcode("base_supply_fte = 1306,"))
  expect_false(is_hardcode("# was hardcoded 1306 before the adapter"))
  expect_false(is_hardcode("base_supply_fte = urps_baseline_supply()$national"))
})
