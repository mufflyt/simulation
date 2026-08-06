# PSA wiring for the workforce 2050 FTE gap ----
#
# Adapts run_workforce_microsimulation() as the `evaluate` function for the
# generic PSA engine (R/calibration-psa.R): each draw runs the full supply x demand model and
# returns the final-year FTE gap. Requires the mufflyaccess contract (or an
# explicit base-year supply), so it is skip-tested where the contract is absent.
#
# Inputs varied (the dominant drivers of the 2050 gap, all top-level orchestrator
# knobs): annual entrants, the retirement hazard source, the demand-population
# series (Census NPP low/mid/high), and the base-year adequacy. Conversion-floor,
# hours-schedule, and access-weight sensitivity are model-internal or target a
# different output (the access surface); supply a custom `evaluate` to include
# them.

#' Default PSA input set for the workforce 2050 gap
#'
#' @return List of `psa_input()` specs.
#' @keywords internal
psa_workforce_gap_inputs <- function() {
  list(
    psa_uniform("baseline_entrants", 45, 90),          # NRMP URPS ~70; range around it
    psa_discrete("retirement_source", c("hwsm", "urps_empirical")),
    psa_discrete("population_series", c("mid", "low", "hi")),  # Census NPP demand series
    psa_uniform("base_adequacy", 0.85, 1.02)           # base-year supply adequacy
  )
}

#' Probabilistic sensitivity analysis of the workforce 2050 FTE gap
#'
#' Wires [run_workforce_microsimulation()] as the PSA evaluator and returns the
#' distribution of the final-year gap over joint draws of the uncertain inputs.
#'
#' @param n Number of PSA draws.
#' @param years Projection years (the gap is read at `max(years)`).
#' @param subspecialty Subspecialty label.
#' @param baseline_supply Base-year supplied FTE; NULL reads it from the
#'   `mufflyaccess` contract.
#' @param inputs PSA input specs ([psa_workforce_gap_inputs()]).
#' @param n_iterations Microsimulation replicates per draw (kept modest; the PSA
#'   outer loop already provides the uncertainty spread).
#' @param seed RNG seed.
#' @param verbose Logical.
#' @return A [run_psa()] result with outputs `gap_pct` and `gap_fte`.
#' @export
psa_workforce_gap <- function(n = 200,
                              years = 2025:2050,
                              subspecialty = "FPMRS",
                              baseline_supply = NULL,
                              inputs = psa_workforce_gap_inputs(),
                              n_iterations = 60,
                              seed = 20260801L,
                              verbose = TRUE) {
  have_ma <- requireNamespace("mufflyaccess", quietly = TRUE)
  if (is.null(baseline_supply) && !have_ma) {
    stop(paste0("psa_workforce_gap() needs the mufflyaccess contract for the base-year ",
                "supply, or an explicit `baseline_supply`."), call. = FALSE)
  }
  base_fte <- baseline_supply
  if (is.null(base_fte)) {
    contract <- urps_baseline_supply(year = 2023L, include_urology = TRUE)
    base_fte <- contract[["national"]]
  }
  final_year <- max(years)

  evaluate <- function(p) {
    # A uniform draw is not a capacity survey. It was labelled as one, which put
    # "capacity_survey" in the validation artifact of every PSA replicate. The
    # honest description is an assumption whose range is bracketed by the
    # published analogues, so that is what the evidence ledger records.
    gap_obj <- baseline_gap(
      base_supply_fte = base_fte,
      adequacy = p$base_adequacy,
      method = "assumed",
      calibration_status = "assumed_with_evidence",
      source = "PSA prior; no URPS capacity survey exists",
      evidence = c(
        "PSA draw of base-year adequacy over uniform(0.85, 1.02)",
        "Range brackets the published analogues: physiatry 0.894, PT 0.948"
      )
    )
    res <- run_workforce_microsimulation(
      baseline_supply = baseline_supply,
      years = years,
      subspecialty = subspecialty,
      population_series = p$population_series,
      baseline_entrants = round(p$baseline_entrants),
      retirement_source = p$retirement_source,
      baseline_gap_estimate = gap_obj,
      n_iterations = n_iterations,
      # Sweeping the base-year adequacy is the point of this analysis, so the
      # non-measured tier is declared rather than tripping the gate on every draw.
      allow_analogy = TRUE,
      seed = seed,
      verbose = FALSE
    )
    fin <- res$fte_gap[res$fte_gap$year == final_year, , drop = FALSE]
    c(gap_pct = as.numeric(fin$gap_pct), gap_fte = as.numeric(fin$gap_fte))
  }

  run_psa(inputs, evaluate, n = n, seed = seed, verbose = verbose)
}
