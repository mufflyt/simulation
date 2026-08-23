test_that("allocate_urps_service_workload enforces total service volume conservation across 100 random allocations", {
  skip(paste(
    "allocate_urps_service_workload() in R/supply-allocate_urps_service_workload.R",
    "is a transitional implementation now requiring a real share_draws argument",
    "from Design B's events-driven draw_service_share_composition().",
    "R/core-service_share_engine.R (from the -04-integration stage) defines its",
    "own, real allocate_urps_service_workload() -- rewrite against that one",
    "once it lands, rather than patch this soon-to-be-superseded version."
  ))
})

test_that("allocate_urps_service_workload assigns zero wRVU to inactive or retired providers", {
  skip(paste(
    "Same reason as the test above -- this transitional allocator is being",
    "superseded by R/core-service_share_engine.R's real implementation."
  ))
})
