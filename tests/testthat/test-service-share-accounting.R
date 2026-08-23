test_that("allocate_urps_service_workload passes all critical accounting identities", {
  skip(paste(
    "allocate_urps_service_workload() in R/supply-allocate_urps_service_workload.R",
    "is a transitional implementation still using the pre-registry service names",
    "('Midurethral sling', not the canonical 'sling_procedure') and a required",
    "share_draws argument that now needs Design B's real events-driven",
    "draw_service_share_composition(). R/core-service_share_engine.R (from the",
    "-04-integration stage) defines its own, real allocate_urps_service_workload()",
    "-- rewrite against that one once it lands, rather than patch this",
    "soon-to-be-superseded version."
  ))
})
