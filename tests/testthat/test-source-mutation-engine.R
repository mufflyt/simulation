# tests/testthat/test-source-mutation-engine.R
# Scientific Hardening Section 7 & 8: Source Mutation Testing Suite

# Mutation 1 is split into its own test_that: it is the one mutation in this
# manifest whose detector (assert_estimand_compatible -> read_estimand_registry)
# reads config/estimands.yml, which is excluded from the built package
# (.Rbuildignore: ^config$) and genuinely absent under covr's isolated temp
# install. Left in the combined test below, tryCatch(..., error = identity)
# quietly turned "config unreachable" into "killed = FALSE" -- a false
# negative unrelated to whether the detector actually works -- and there is
# no way to skip just this assertion without also losing real coverage of
# mutations 2-4, which do not depend on config/ and must keep running.
test_that("test_scientific_mutation kills the D6-to-D3 substitution mutation", {
  skip_if(length(.source_tree_root()) == 0,
          "estimand registry unreachable (source tree absent under R CMD check/covr)")
  res1 <- test_scientific_mutation(1) # D6 to D3 substitution
  expect_true(res1$killed)
  expect_equal(res1$detector_fired, "assert_estimand_compatible")
})

test_that("test_scientific_mutation kills the remaining high-priority scientific mutations", {
  res2 <- test_scientific_mutation(2) # CHIA con=NULL
  expect_true(res2$killed)
  expect_equal(res2$detector_fired, "build_chia_inpatient_urps_series")

  res3 <- test_scientific_mutation(3) # Zero routes travel kernel
  expect_true(res3$killed)

  res4 <- test_scientific_mutation(4) # Out of range denominator year
  expect_true(res4$killed)
})
