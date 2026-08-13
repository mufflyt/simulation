# Forward response map: base-year adequacy -> simulated appointment access.
#
# Demonstrates the adequacy -> (demand, capacity) -> clear_access bridge added
# in R/calibration-access_response_bridge.R. This is the FORWARD half of the
# calibration loop the model has never had: vary adequacy, read the implied
# national wait and appointment-obtainment. Fitting adequacy (or wait_scale) to
# an observed wait distribution (Rabice 2019 / the Lizeth anchor) is the next
# step and is deliberately not done here.
#
# Run from the package root:
#   Rscript scripts/simulate_access_from_adequacy.R

pkgload::load_all(".", quiet = TRUE)

base_supply_fte <- 1306

# A sweep across adequacy. Below 1 the steady-state queue saturates (rho > 1,
# infinite wait) for a single national cell -- surfaced, not hidden. Above 1 the
# wait is finite and falls as slack grows.
adequacy_grid <- c(0.90, 0.948, 1.00, 1.10, 1.25, 1.50, 2.00)

sweep <- lapply(adequacy_grid, function(a) {
  out <- suppressMessages(
    simulate_access_for_adequacy(
      adequacy = a,
      base_supply_fte = base_supply_fte,
      wait_scale = 30
    )
  )
  data.frame(
    adequacy = a,
    rho = 1 / a,
    wait_A1 = out$value[out$estimand == "A1"],
    censored_share_A1b = out$value[out$estimand == "A1b"],
    p_appointment_A2 = out$value[out$estimand == "A2"],
    unmet_A5 = out$value[out$estimand == "A5"]
  )
})
sweep <- do.call(rbind, sweep)
print(sweep, row.names = FALSE)

base::cat(
  "\nReading: a single national adequacy < 1 saturates the queue, so the",
  "\nobserved finite wait (Rabice 2019: 23.1 business days) is inconsistent with",
  "\na below-one national mean under this queue -- it implies catchment",
  "\nheterogeneity or an effective utilization below 1/adequacy. Supplying a",
  "\ncatchment-level adequacy vector (rather than a scalar) is how a finite",
  "\nnational wait is reproduced; fitting that against the observed distribution",
  "\nis the calibration step still to come.\n"
)

# Heterogeneity demonstration: the same mean adequacy, but dispersed across
# catchments, yields a finite national wait because cells above 1 clear.
catchment_adequacy <- c(0.7, 0.9, 1.1, 1.4, 2.0)
het <- suppressMessages(
  simulate_access_for_adequacy(
    adequacy = catchment_adequacy,
    base_supply_fte = base_supply_fte / length(catchment_adequacy),
    wait_scale = 30,
    catchment = paste0("c", seq_along(catchment_adequacy))
  )
)
base::cat(
  "\nDispersed catchments (adequacy", paste(catchment_adequacy, collapse = ", "),
  "), mean", round(mean(catchment_adequacy), 3), ":\n"
)
print(het, row.names = FALSE)
