# Manuscript-ready figures ----
#
# Publication figures for the workforce model, each a PURE function taking tidy
# tibbles and returning a ggplot (so they are testable without running the full
# model). They apply the repo's five ggplot2 standards: check.overlap guides on
# crowded year axes, size.unit="pt" text, plot.title size rel(1.1),
# plot.title.position="plot", and white backgrounds on save.
#
# ggplot2 is in Suggests; every figure requires it at call time.

.need_ggplot <- function(fn) {
  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    stop(sprintf("%s() needs the 'ggplot2' package (Suggests).", fn), call. = FALSE)
  }
}

# Shared theme applying the title standards; base_size scales text.
.urpssim_theme <- function(base_size = 12) {
  ggplot2::theme_minimal(base_size = base_size) +
    ggplot2::theme(
      plot.title = ggplot2::element_text(size = ggplot2::rel(1.1)),
      plot.title.position = "plot",
      legend.position = "bottom"
    )
}

# Replacement-outlook palette (cliff cutpoints).
URPSSIM_OUTLOOK_COLORS <- c(Adequate = "#2c7fb8", Marginal = "#f0a500",
                            Insufficient = "#d7301f")

#' Supply vs required FTE, with a Monte-Carlo supply ribbon
#'
#' @param supply Tibble with `year`, `effective_fte_median`, `effective_fte_lo`,
#'   `effective_fte_hi` (a [run_supply_microsimulation()] summary).
#' @param required Tibble with `year`, `required_fte`.
#' @param title Plot title.
#' @return A ggplot object.
#' @export
fig_supply_vs_required <- function(supply, required,
                                   title = "Projected URPS supply vs required FTE") {
  .need_ggplot("fig_supply_vs_required")
  assertthat::assert_that(all(c("year", "effective_fte_median", "effective_fte_lo",
                                "effective_fte_hi") %in% names(supply)))
  assertthat::assert_that(all(c("year", "required_fte") %in% names(required)))

  ggplot2::ggplot(supply, ggplot2::aes(x = .data$year)) +
    ggplot2::geom_ribbon(ggplot2::aes(ymin = .data$effective_fte_lo,
                                      ymax = .data$effective_fte_hi),
                         fill = "#2c7fb8", alpha = 0.20) +
    ggplot2::geom_line(ggplot2::aes(y = .data$effective_fte_median, colour = "Supplied (effective FTE)"),
                       linewidth = 1.1) +
    ggplot2::geom_line(data = required,
                       ggplot2::aes(y = .data$required_fte, colour = "Required FTE"),
                       linewidth = 1.1, linetype = "dashed") +
    ggplot2::scale_colour_manual(values = c("Supplied (effective FTE)" = "#2c7fb8",
                                            "Required FTE" = "#d7301f")) +
    ggplot2::scale_x_continuous(guide = ggplot2::guide_axis(check.overlap = TRUE)) +
    ggplot2::labs(title = title, x = NULL, y = "Full-time-equivalent providers",
                  colour = NULL) +
    .urpssim_theme()
}

#' Adequacy over time, with the trough year marked
#'
#' @param coverage Long tibble with `year`, `estimand`, `adequacy`
#'   (a [compute_demand_coverage()] result). Adequacy is supply growth relative
#'   to demand growth, rebased to 1.0 at the base year.
#' @param title Plot title.
#' @return A ggplot object.
#' @export
fig_adequacy_trough <- function(coverage,
                                title = "Supply adequacy relative to demand growth") {
  .need_ggplot("fig_adequacy_trough")
  assertthat::assert_that(all(c("year", "estimand", "adequacy") %in% names(coverage)))

  trough <- coverage[which.min(coverage$adequacy), , drop = FALSE]
  ggplot2::ggplot(coverage, ggplot2::aes(x = .data$year, y = .data$adequacy,
                                         colour = .data$estimand)) +
    ggplot2::geom_hline(yintercept = 1, linetype = "dotted") +
    ggplot2::geom_line(linewidth = 1) +
    ggplot2::geom_point(data = trough, size = 3, shape = 21, fill = "white") +
    ggplot2::geom_text(data = trough,
                       ggplot2::aes(label = sprintf("trough %d", .data$year)),
                       vjust = 1.8, size = 12, size.unit = "pt", show.legend = FALSE) +
    ggplot2::scale_x_continuous(guide = ggplot2::guide_axis(check.overlap = TRUE)) +
    ggplot2::labs(title = title, x = NULL, y = "Adequacy (base year = 1.0)",
                  colour = "Demand estimand") +
    .urpssim_theme()
}

#' Replacement-ratio outlook by scenario
#'
#' @param outlook Tibble with a scenario label column, `replacement_ratio`, and
#'   `outlook` (Adequate/Marginal/Insufficient).
#' @param scenario_col Name of the scenario label column.
#' @param title Plot title.
#' @return A ggplot object.
#' @export
fig_replacement_outlook <- function(outlook, scenario_col = "scenario_label",
                                    title = "Replacement-ratio outlook by scenario") {
  .need_ggplot("fig_replacement_outlook")
  assertthat::assert_that(all(c(scenario_col, "replacement_ratio", "outlook") %in% names(outlook)))
  d <- outlook
  d[[scenario_col]] <- stats::reorder(d[[scenario_col]], d$replacement_ratio)

  ggplot2::ggplot(d, ggplot2::aes(x = .data$replacement_ratio, y = .data[[scenario_col]],
                                  fill = .data$outlook)) +
    ggplot2::geom_vline(xintercept = c(0.8, 1.2), linetype = "dotted") +
    ggplot2::geom_col() +
    ggplot2::scale_fill_manual(values = URPSSIM_OUTLOOK_COLORS, drop = FALSE) +
    ggplot2::labs(title = title, x = "Replacement ratio (entrants / departures)",
                  y = NULL, fill = NULL) +
    .urpssim_theme()
}

#' Population share with access at or above each threshold
#'
#' @param threshold_shares Tibble with `threshold`, `pop_share_at_or_above`
#'   (the `threshold_shares` element of [summarize_access()]).
#' @param title Plot title.
#' @return A ggplot object.
#' @export
fig_access_threshold_shares <- function(threshold_shares,
                                        title = "Population share by modelled access level") {
  .need_ggplot("fig_access_threshold_shares")
  assertthat::assert_that(all(c("threshold", "pop_share_at_or_above") %in% names(threshold_shares)))

  ggplot2::ggplot(threshold_shares,
                  ggplot2::aes(x = factor(.data$threshold),
                               y = 100 * .data$pop_share_at_or_above)) +
    ggplot2::geom_col(fill = "#2c7fb8") +
    ggplot2::geom_text(ggplot2::aes(label = sprintf("%.0f%%", 100 * .data$pop_share_at_or_above)),
                       vjust = -0.4, size = 12, size.unit = "pt") +
    ggplot2::labs(title = title,
                  x = "Access threshold (per 100k women)",
                  y = "Population at or above (%)") +
    .urpssim_theme()
}

#' Save a named list of figures as white-background PNGs
#'
#' @param figures Named list of ggplot objects.
#' @param dir Output directory.
#' @param width,height,dpi ggsave parameters.
#' @return (Invisibly) the written file paths.
#' @export
save_manuscript_figures <- function(figures, dir = "outputs/figures",
                                    width = 7, height = 4.5, dpi = 300) {
  .need_ggplot("save_manuscript_figures")
  dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  paths <- character(0)
  for (nm in names(figures)) {
    p <- file.path(dir, sprintf("%s.png", nm))
    ggplot2::ggsave(p, figures[[nm]], width = width, height = height, dpi = dpi,
                    bg = "white")
    paths <- c(paths, p)
  }
  .msg_info(sprintf("Wrote %d figure(s) to %s", length(paths), dir))
  invisible(paths)
}
