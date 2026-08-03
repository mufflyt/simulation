# PI-facing summary report + access composer ----
#
# workforce_summary_report() folds a run's headline results into one compact
# object (with a print method): the final-year FTE gap, the demand-concordance
# verdict, the replacement-ratio outlook, the calibration tier of the inputs, and
# -- when provider isochrones are supplied -- the access-desert share.
#
# workforce_access_summary() composes the real access layer (real tract demand x
# provider isochrones x supply) into the desert share + threshold shares, so the
# spatial output can travel alongside supply and demand.

#' Summarise real geographic access for a workforce supply state
#'
#' Composes [real_access_surface()] + [summarize_access()] into the headline
#' access statistics. The tract demand is real (vendored); the provider
#' isochrone polygons are the external artifact the caller supplies.
#'
#' @param iso_sf Provider isochrone polygons (`sf`); see
#'   [load_provider_isochrones()].
#' @param supply Tibble `provider_id`, `supply`.
#' @param weights Cumulative band weights.
#' @param step2_power 1 = E2SFCA, 2 = M2SFCA.
#' @param mode Reproducibility mode.
#' @return List: `mean_access`, `access_desert_share_pct` (population with zero
#'   modelled access), `threshold_shares`, `n_tracts`.
#' @export
workforce_access_summary <- function(iso_sf, supply,
                                     weights = E2SFCA_DEFAULT_WEIGHTS,
                                     step2_power = 1,
                                     mode = resolve_reproducibility_mode()) {
  surface <- real_access_surface(iso_sf, supply, weights = weights,
                                 step2_power = step2_power, mode = mode)
  s <- summarize_access(surface$access)
  list(
    mean_access = s$mean_access,
    access_desert_share_pct = 100 * s$zero_access_share,
    threshold_shares = s$threshold_shares,
    n_tracts = nrow(surface$access)
  )
}

# Defensive getter for fields whose shape has evolved across versions.
.pick <- function(x, ...) {
  for (k in c(...)) if (!is.null(x[[k]])) return(x[[k]])
  NULL
}

#' Fold a workforce run into a compact PI-facing summary
#'
#' @param result A [run_workforce_microsimulation()] result.
#' @param access Optional [workforce_access_summary()] output to fold in.
#' @return An object of class `workforce_summary_report`.
#' @export
workforce_summary_report <- function(result, access = NULL) {
  assertthat::assert_that(is.list(result))
  meta <- result$scenario_meta %||% list()

  # Final-year FTE gap.
  gap <- NULL
  if (!is.null(result$fte_gap) && nrow(result$fte_gap) > 0) {
    fy <- max(result$fte_gap$year)
    fin <- result$fte_gap[result$fte_gap$year == fy, , drop = FALSE]
    gap <- list(year = fy, supplied_fte = fin$supplied_fte[1],
                required_fte = fin$required_fte[1], gap_fte = fin$gap_fte[1],
                gap_pct = fin$gap_pct[1])
  }

  # Demand-concordance verdict (defensive across shapes).
  conc <- result$concordance %||% list()
  concordance <- list(
    informative = .pick(conc, "informative", "distinct_estimands"),
    robust = .pick(conc, "robust", "conclusion_agrees"),
    trough_year = .pick(conc, "trough_year")
  )

  report <- structure(list(
    run_id = result$run_id %||% NA_character_,
    subspecialty = meta$subspecialty %||% NA_character_,
    years = meta$years,
    gap = gap,
    concordance = concordance,
    outlook = result$outlook,
    calibration = meta$calibration %||% (if (exists("calibration_status_report"))
      tryCatch(calibration_status_report(), error = function(e) NULL)),
    example_only = meta$example_only,
    access = access
  ), class = "workforce_summary_report")
  report
}

#' @export
print.workforce_summary_report <- function(x, ...) {
  cat("== URPS workforce summary ==\n")
  cat(sprintf("run: %s   subspecialty: %s\n", x$run_id %||% "NA",
              x$subspecialty %||% "NA"))
  if (isTRUE(x$example_only)) cat("** EXAMPLE / illustrative inputs -- not a result **\n")

  if (!is.null(x$gap)) {
    cat(sprintf("\n%d FTE gap: %.0f supplied vs %.0f required  (%.0f FTE, %+.1f%%)\n",
                x$gap$year, x$gap$supplied_fte, x$gap$required_fte,
                x$gap$gap_fte, x$gap$gap_pct))
  }

  if (!is.null(x$concordance$robust)) {
    cat(sprintf("demand concordance: informative = %s, conclusion robust = %s",
                format(x$concordance$informative), format(x$concordance$robust)))
    if (!is.null(x$concordance$trough_year)) cat(sprintf(", trough %s", x$concordance$trough_year))
    cat("\n")
  }

  if (!is.null(x$outlook) && is.data.frame(x$outlook)) {
    sc <- intersect(c("scenario_label", "scenario"), names(x$outlook))[1]
    cat("\nreplacement-ratio outlook:\n")
    for (i in seq_len(nrow(x$outlook))) {
      cat(sprintf("  %-22s ratio %.2f  %s\n",
                  x$outlook[[sc]][i], x$outlook$replacement_ratio[i], x$outlook$outlook[i]))
    }
  }

  if (!is.null(x$access)) {
    cat(sprintf("\naccess desert share: %.1f%% of women have zero modelled access (%d tracts)\n",
                x$access$access_desert_share_pct, x$access$n_tracts))
  }

  if (!is.null(x$calibration) && is.data.frame(x$calibration)) {
    tiers <- table(x$calibration[[intersect(c("tier", "status"), names(x$calibration))[1]]])
    cat("\ncalibration tiers: ",
        paste(sprintf("%s=%d", names(tiers), as.integer(tiers)), collapse = ", "), "\n")
  }
  invisible(x)
}
