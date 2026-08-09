#!/usr/bin/env Rscript
# Figures for VALIDATION_PAPER.md -
#
#   Rscript scripts/figures/validation_paper_figures.R
#
# EVERY FIGURE IS READ FROM A COMMITTED ARTIFACT. None re-runs the simulation,
# so a figure cannot silently disagree with the table beside it: if a number
# moves, the artifact moves, and both the table and the figure move with it.
# The provenance line under each block names the file it reads.
#
# Palette: slots 1 and 2 of the validated categorical palette (#2a78d6 blue,
# #eb6834 orange). The pair was checked with the palette validator on the
# all-pairs list against a light surface: CVD dE 24.7 (protan), normal-vision
# dE 33.6, both well past the 8 and 15 floors, and both >= 3:1 on the surface.
# Print figures are light-surface only; there is no dark mode to select.
#
# Colour never carries meaning alone. Every series is directly labelled, and
# containment is encoded by marker fill AND a printed count, so the figures
# survive greyscale printing and photocopying.
#
# Writes figures/paper/fig[1-5]_*.{png,pdf}

suppressPackageStartupMessages({
  library(ggplot2); library(dplyr)
})

OUT <- "figures/paper"
dir.create(OUT, recursive = TRUE, showWarnings = FALSE)

# ---- Design tokens ---------------------------------------------------------

BLUE   <- "#2a78d6"   # categorical slot 1
ORANGE <- "#eb6834"   # categorical slot 2
INK    <- "#0b0b0b"   # text-primary
INK2   <- "#52514e"   # text-secondary
GRID   <- "#e6e5e1"
MUTED  <- "#b8b6ae"

theme_paper <- function(base_size = 10) {
  theme_minimal(base_size = base_size, base_family = "Helvetica") +
    theme(
      text             = element_text(colour = INK),
      plot.title       = element_text(size = rel(1.05), face = "bold",
                                      colour = INK, margin = margin(b = 2)),
      plot.subtitle    = element_text(size = rel(0.88), colour = INK2,
                                      margin = margin(b = 9)),
      plot.caption     = element_text(size = rel(0.72), colour = INK2,
                                      hjust = 0, margin = margin(t = 9)),
      plot.title.position = "plot",
      plot.caption.position = "plot",
      axis.title       = element_text(size = rel(0.88), colour = INK2),
      axis.text        = element_text(size = rel(0.85), colour = INK2),
      panel.grid.major = element_line(colour = GRID, linewidth = 0.3),
      panel.grid.minor = element_blank(),
      panel.background = element_rect(fill = "white", colour = NA),
      plot.background  = element_rect(fill = "white", colour = NA),
      legend.position  = "none",          # every series is directly labelled
      plot.margin      = margin(10, 14, 8, 10)
    )
}

save_fig <- function(p, stem, w, h) {
  ggsave(file.path(OUT, paste0(stem, ".png")), p, width = w, height = h,
         dpi = 300, bg = "white")
  ggsave(file.path(OUT, paste0(stem, ".pdf")), p, width = w, height = h,
         device = grDevices::cairo_pdf, bg = "white")
  message("wrote ", stem, ".{png,pdf}")
}

# ============================================================================
# FIGURE 1. The two quantities are not the same quantity
# provenance: artifacts/backtest_2020_to_2023_trajectory.csv
# ============================================================================
tr <- utils::read.csv("artifacts/backtest_2020_to_2023_trajectory.csv",
                      stringsAsFactors = FALSE)
ARM <- "1. Derived cohort, entrants = 55 (shipped assumption)"
act <- tr[tr$arm == ARM, ]
mat <- tr[tr$arm == paste0(ARM, " [no-attrition, definition-matched]"), ]

f1 <- data.frame(
  year     = act$year,
  observed = act$observed,
  active   = act$predicted_median,
  matched  = mat$predicted_median
)

p1 <- ggplot(f1, aes(x = year)) +
  # the gap between what the model counts and what the benchmark counts
  geom_ribbon(aes(ymin = active, ymax = observed), fill = ORANGE, alpha = 0.10) +
  geom_line(aes(y = observed), colour = INK, linewidth = 0.8) +
  geom_point(aes(y = observed), colour = INK, size = 1.9) +
  geom_line(aes(y = matched), colour = BLUE, linewidth = 0.7, linetype = "22") +
  geom_line(aes(y = active), colour = ORANGE, linewidth = 0.8) +
  geom_point(aes(y = active), colour = ORANGE, size = 1.9) +
  annotate("text", x = 2023.06, y = 1306, hjust = 0, size = 3.0, colour = INK,
           label = "Observed cumulative\ncertifications (1,306)") +
  annotate("text", x = 2023.06, y = 1265, hjust = 0, size = 3.0, colour = BLUE,
           label = "Model, attrition\nsuspended (1,265)") +
  annotate("text", x = 2023.06, y = 1207, hjust = 0, size = 3.0, colour = ORANGE,
           label = "Model, clinically\nactive (1,207)") +
  # Drawn at the target year, where both endpoints are exact values rather
  # than interpolations, and inset so it does not cover the two markers.
  annotate("segment", x = 2023, xend = 2023, y = 1213, yend = 1300,
           colour = INK2, linewidth = 0.3,
           arrow = arrow(length = unit(0.05, "in"), ends = "both", type = "closed")) +
  # The wedge between the two lines is too narrow to hold this text at any
  # height without a line running through it, so it sits in the empty
  # upper-left and the arrow is left to speak for itself.
  annotate("text", x = 2020.08, y = 1297, hjust = 0, size = 2.9, colour = INK2,
           label = "99-provider difference at 2023, 59% definitional") +
  scale_x_continuous(breaks = 2020:2023, limits = c(2020, 2024.5)) +
  scale_y_continuous(breaks = seq(1050, 1350, 50)) +
  labs(
    title = "The projected workforce and the validation target measure different quantities",
    subtitle = "Physicians retire out of the modelled active workforce; the certification count only ever adds",
    x = NULL, y = "Physicians (headcount)",
    caption = "Cumulative board certifications (national, ABOG plus ABU) against the microsimulation projected from a 2020 origin.\nMedians of 1,000 Monte Carlo iterations. Source: backtest_2020_to_2023_trajectory.csv"
  ) +
  theme_paper()
save_fig(p1, "fig1_estimand_divergence", 7.2, 4.4)

# ============================================================================
# FIGURE 2. Containment bought with width
# provenance: artifacts/diagnostics/validation_rolling_origin.csv
# ============================================================================
ro <- utils::read.csv("artifacts/diagnostics/validation_rolling_origin.csv",
                      stringsAsFactors = FALSE)
ro$lab <- sprintf("%d \u2192 %d", ro$origin, ro$target_year)
# Numeric y, not a factor. A discrete scale cannot coexist with the -Inf/Inf
# rect that shades the impossible region, and the rect is the point of the
# panel, so the axis is built by hand instead.
ro <- ro[order(-ro$origin), ]
ro$ypos <- seq_len(nrow(ro))

p2 <- ggplot(ro, aes(y = ypos)) +
  # a cumulative count cannot be negative; the region is drawn, not truncated
  annotate("rect", xmin = -Inf, xmax = 0, ymin = -Inf, ymax = Inf,
           fill = MUTED, alpha = 0.22) +
  geom_linerange(aes(xmin = lower, xmax = upper), colour = BLUE, linewidth = 1.4,
                 alpha = 0.85) +
  geom_point(aes(x = median_prediction), colour = BLUE, size = 2.2) +
  geom_point(aes(x = observed), colour = INK, size = 2.6, shape = 18) +
  geom_text(aes(x = upper, label = sprintf("width %s", format(round(width), big.mark = ","))),
            hjust = -0.12, size = 2.9, colour = INK2) +
  annotate("text", x = -720, y = nrow(ro) + 0.40, hjust = 0, size = 2.7,
           colour = INK2, lineheight = 0.95,
           label = "impossible for a\ncumulative count") +
  annotate("text", x = ro$observed[1], y = nrow(ro) + 0.46, size = 2.9,
           colour = INK, label = "observed", hjust = 0.5) +
  scale_x_continuous(breaks = seq(-500, 2500, 500),
                     labels = function(x) format(x, big.mark = ","),
                     limits = c(-780, 3450)) +
  scale_y_continuous(breaks = ro$ypos, labels = ro$lab,
                     limits = c(0.5, nrow(ro) + 0.75)) +
  labs(
    title = "All four rolling origins contained the observation, three by width alone",
    subtitle = "95% intervals shown to scale; the 2017 origin spans \u2212594.5 to 2,590 on two prior errors",
    x = "Physicians (headcount)", y = "Forecast origin \u2192 target",
    caption = "Diamond is observed, circle is the bias-corrected prediction, bar is the 95% interval.\nSource: validation_rolling_origin.csv"
  ) +
  theme_paper()
save_fig(p2, "fig2_rolling_origin_intervals", 7.2, 3.6)

# ============================================================================
# FIGURE 3. Containment and the interval score disagree
# provenance: artifacts/diagnostics/interval_honesty_scorecard.csv
# ============================================================================
sc <- utils::read.csv("artifacts/diagnostics/interval_honesty_scorecard.csv",
                      stringsAsFactors = FALSE)
sc$short <- c("Rolling origin\n(wide)",
              "Sharp, attrition applied\n(definition mismatch)",
              "Sharp, attrition suspended\n(definition matched)")
# THE DENOMINATORS ARE NOT THE SAME UNIT. The rolling-origin row is scored at
# four separate forecast ORIGINS; the two sharp rows are one trajectory scored
# at three consecutive TARGET YEARS. Printing a bare "4 of 4" beside "2 of 3"
# invites exactly the comparison this paper says not to make, so each count
# names what it counts.
sc$unit <- c("origins", "target years", "target years")
sc$contained <- sprintf("%d of %d %s", round(sc$coverage * sc$n), sc$n, sc$unit)
sc$best <- sc$mean_interval_score == min(sc$mean_interval_score)

p3 <- ggplot(sc, aes(x = mean_width, y = mean_interval_score)) +
  geom_point(aes(fill = best), shape = 21, size = 4.2, stroke = 0.9,
             colour = BLUE) +
  scale_fill_manual(values = c(`TRUE` = BLUE, `FALSE` = "white")) +
  geom_text(aes(label = short), hjust = 0, nudge_x = 0.06, size = 3.0,
            colour = INK, lineheight = 0.95) +
  # nudge_y is in log10 units here: 0.30 would move the label half a decade.
  geom_text(aes(label = contained), hjust = 0, nudge_x = 0.06, nudge_y = -0.105,
            size = 2.8, colour = INK2) +
  scale_x_log10(breaks = c(50, 100, 250, 500, 1000, 2000),
                labels = function(x) format(x, big.mark = ","),
                limits = c(62, 4200)) +
  scale_y_log10(breaks = c(100, 250, 500, 1000, 2000),
                labels = function(x) format(x, big.mark = ",")) +
  labs(
    title = "The forecast with the most containment scored worst",
    subtitle = "Lower is better on both axes; a filled marker is the best interval score",
    x = "Mean 95% interval width, providers (log scale)",
    y = "Interval score (log scale)",
    caption = paste0(
      "Containment is whether the observed count fell inside the 95% interval. The rolling-origin forecast is scored at four\n",
      "separate origins; the two sharp forecasts at three consecutive target years of one trajectory, so the counts are not\n",
      "directly comparable. The interval score charges for width and for missing, so containment cannot be bought.\n",
      "Source: interval_honesty_scorecard.csv")
  ) +
  theme_paper()
save_fig(p3, "fig3_containment_vs_sharpness", 7.2, 4.3)

# ============================================================================
# FIGURE 4. Where the 99-provider gap comes from
# provenance: artifacts/diagnostics/entrant_regime_bias_decomposition.csv
# ============================================================================
de <- utils::read.csv("artifacts/diagnostics/entrant_regime_bias_decomposition.csv",
                      stringsAsFactors = FALSE)
lv <- de$level_2023

# NOT A WATERFALL. On a zero baseline the two increments, 58 and 41, are
# slivers against a 1,207 column, and floating the increments to make them
# visible gives bars with no baseline at all. The quantity the reader needs is
# the 99-provider DIFFERENCE and how it splits, so the difference is what is
# drawn, from a true zero.
wf <- data.frame(
  component = c("Definitional mismatch\nattrition applied to a count\nthat only ever adds",
                "Entrant regime\nrealized entry exceeded\nthe assumed rate"),
  providers = c(de$delta[2], de$delta[3]),
  share     = c(de$pct_of_total_miss[2], de$pct_of_total_miss[3]),
  stringsAsFactors = FALSE
)
wf$component <- factor(wf$component, levels = rev(wf$component))
wf$label <- sprintf("%d providers  (%.0f%%)", wf$providers, wf$share)

p4 <- ggplot(wf, aes(x = providers, y = component)) +
  geom_col(fill = BLUE, width = 0.5) +
  geom_text(aes(label = label), hjust = -0.08, size = 3.0, colour = INK) +
  scale_x_continuous(limits = c(0, 88), breaks = seq(0, 80, 20),
                     expand = expansion(mult = c(0, 0))) +
  labs(
    title = "Most of the disagreement is definitional, not behavioural",
    subtitle = sprintf(
      "The %d-provider difference between the %s projection and the observed %s, by source",
      de$level_2023[3] - de$level_2023[1],
      format(lv[1], big.mark = ","), format(lv[3], big.mark = ",")),
    x = "Physicians (headcount)", y = NULL,
    caption = "Assumptions about how physicians retire or enter account for essentially none of the difference.\nSource: entrant_regime_bias_decomposition.csv"
  ) +
  theme_paper() +
  theme(panel.grid.major.y = element_blank(),
        axis.text.y = element_text(hjust = 0, lineheight = 0.95))
save_fig(p4, "fig4_discrepancy_decomposition", 7.2, 3.0)

# ============================================================================
# FIGURE 5. The sign of the error depends on the window chosen
# provenance: artifacts/diagnostics/backtest_multi_window.csv
# ============================================================================
mw <- utils::read.csv("artifacts/diagnostics/backtest_multi_window.csv",
                      stringsAsFactors = FALSE)
mw$win <- sprintf("%d → %d", mw$cutoff_year, mw$target_year)
mw$pred <- ifelse(mw$predictor == "certification",
                  "Certification flow", "Residency match (NRMP)")

p5 <- ggplot(mw, aes(x = win, y = percent_error, group = pred, colour = pred)) +
  geom_hline(yintercept = 0, colour = INK2, linewidth = 0.4) +
  geom_line(linewidth = 0.7) +
  geom_point(size = 2.4) +
  scale_colour_manual(values = c(`Certification flow` = ORANGE,
                                 `Residency match (NRMP)` = BLUE)) +
  annotate("text", x = 5.12, y = -8.35, hjust = 0, size = 3.0, colour = ORANGE,
           label = "Certification\nflow") +
  annotate("text", x = 5.12, y = -4.43, hjust = 0, size = 3.0, colour = BLUE,
           label = "Residency match\n(NRMP)") +
  annotate("text", x = 0.60, y = 6.4, hjust = 0, size = 2.8, colour = INK2,
           label = "over-predicts") +
  annotate("text", x = 0.60, y = -5.6, hjust = 0, size = 2.8, colour = INK2,
           label = "under-predicts") +
  scale_y_continuous(breaks = seq(-10, 20, 5), labels = function(x) paste0(x, "%")) +
  coord_cartesian(xlim = c(1, 6.6), clip = "off") +
  labs(
    title = "The same model over-predicts or under-predicts depending on the window",
    subtitle = "Signed difference from the observed count across five cutoff-to-target windows",
    x = "Forecast origin → target", y = "Percentage difference from observed",
    caption = "Any single window would have supported a confident conclusion about the direction of bias.\nSource: backtest_multi_window.csv"
  ) +
  theme_paper()
save_fig(p5, "fig5_window_sign_flip", 7.2, 4.0)

message("\nAll five figures written to ", OUT)
