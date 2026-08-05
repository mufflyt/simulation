#!/usr/bin/env Rscript

# Generate the three README model-overview figures. The projection and cohort
# figures are deliberately labelled exploratory: they use the certification-
# cohort reconstruction and an analogy-derived capacity-survey stand-in, not a
# production active-provider roster or URPS-specific hours survey.

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(pkgload)
})

root <- normalizePath(".")
pkgload::load_all(root, quiet = TRUE)
dir.create(file.path(root, "figures"), showWarnings = FALSE, recursive = TRUE)

fig_path <- function(name) file.path(root, "figures", name)

# 1. Exploratory national supply versus required-FTE trajectory.
supply <- urps_baseline_supply(year = 2023L, include_urology = TRUE)
gap <- baseline_gap(
  base_supply_fte = supply$national,
  adequacy = capacity_survey_adequacy(example_capacity_survey())$adequacy,
  method = "capacity_survey",
  # The arithmetic is a capacity survey's; the DISTRIBUTION is physical therapy's.
  # Only the tier records that, and the figure is exploratory because of it.
  calibration_status = "derived_by_analogy",
  source = "Zarek 2025 PTJ (physical therapists, n = 1,423)",
  evidence = "Illustrative physical-therapy capacity distribution; replace with URPS survey"
)
run <- run_workforce_microsimulation(
  years = 2025:2050,
  baseline_gap_estimate = gap,
  # A README illustration, not the project's uncertainty analysis. Keep this
  # small enough to regenerate interactively; published runs use more draws.
  n_iterations = 10,
  baseline_entrants = 55,
  allow_analogy = TRUE,
  output_dir = NULL,
  verbose = FALSE
)

supply_plot_data <- run$supply |>
  filter(.data$scenario == run$scenario_meta$reference_scenario) |>
  transmute(year = .data$year, estimate = .data$effective_fte_median,
            lo = .data$effective_fte_lo, hi = .data$effective_fte_hi,
            series = "Projected supplied FTE")
demand_plot_data <- run$required_fte |>
  transmute(year = .data$year, estimate = .data$required_fte,
            lo = NA_real_, hi = NA_real_, series = "Required FTE")
trajectory <- bind_rows(supply_plot_data, demand_plot_data)

p_trajectory <- ggplot() +
  geom_ribbon(data = filter(trajectory, .data$series == "Projected supplied FTE"),
              aes(.data$year, ymin = .data$lo, ymax = .data$hi),
              fill = "#0072B2", alpha = .18) +
  geom_line(data = trajectory, aes(.data$year, .data$estimate, colour = .data$series),
            linewidth = .95) +
  scale_colour_manual(values = c("Projected supplied FTE" = "#0072B2",
                                 "Required FTE" = "#D55E00")) +
  labs(
    title = "Exploratory national URPS supply and required-FTE trajectory",
    subtitle = "Certification-cohort reconstruction and an analogy-derived capacity-survey stand-in",
    x = NULL, y = "FTE", colour = NULL,
    caption = "Supply band: 95% Monte Carlo interval. This exploratory figure is not externally validated FTE-gap evidence."
  ) +
  theme_minimal(base_size = 12) +
  theme(legend.position = "top", panel.grid.minor = element_blank(),
        plot.caption = element_text(hjust = 0, colour = "grey25"))
ggsave(fig_path("readme_supply_demand_trajectory.png"), p_trajectory,
       width = 10, height = 5.8, dpi = 200)

# 2. Baseline certification-cohort reconstruction, not an observed roster.
set.seed(20260801L)
cohort <- agents_from_certification_cohorts(baseline_year = 2025L)
cohort_plot <- cohort |>
  mutate(age_band = cut(.data$age, breaks = c(25, 35, 45, 55, 65, 75, 90), right = FALSE,
                        labels = c("25–34", "35–44", "45–54", "55–64", "65–74", "75–89")),
         sex = if_else(.data$sex == "female", "Female", "Male")) |>
  count(.data$age_band, .data$sex)

p_cohort <- ggplot(cohort_plot, aes(.data$age_band, .data$n, fill = .data$sex)) +
  geom_col(width = .72) +
  scale_fill_manual(values = c(Female = "#CC79A7", Male = "#56B4E9")) +
  labs(
    title = "Baseline certification-cohort reconstruction used by the exploratory supply model",
    subtitle = "Age is derived from certification year; sex is simulated at the configured cohort share",
    x = "Age band in 2025", y = "Providers", fill = NULL,
    caption = "This is not an observed active-provider roster. The pre-2014 backlog cohort is explicitly assumed."
  ) +
  theme_minimal(base_size = 12) +
  theme(legend.position = "top", panel.grid.minor = element_blank(),
        plot.caption = element_text(hjust = 0, colour = "grey25"))
ggsave(fig_path("readme_baseline_cohort_composition.png"), p_cohort,
       width = 10, height = 5.8, dpi = 200)

# 3. Dimensionally explicit demand-to-FTE pathway.
nodes <- tibble::tribble(
  ~x, ~y, ~label,
  1.0, 3, "Female population\nby age band",
  3.3, 3, "PFD risk and\ncare-seeking",
  5.6, 3, "Service volumes\nby setting",
  7.9, 3, "CMS work-RVU\nbasket",
  10.2, 3, "Required\nclinical FTE"
)
edges <- tibble::tibble(x = c(1.7, 4.0, 6.3, 8.6), xend = c(2.6, 4.9, 7.2, 9.5), y = 2.55, yend = 2.55)

p_pathway <- ggplot() +
  geom_segment(data = edges, aes(.data$x, .data$y, xend = .data$xend, yend = .data$yend),
               arrow = grid::arrow(length = grid::unit(3, "mm")), linewidth = .7, colour = "grey45") +
  geom_text(data = nodes, aes(.data$x, .data$y, label = .data$label),
            colour = "#1B1B1B", size = 4.1, lineheight = .95, fontface = "bold") +
  annotate("text", x = 5.6, y = 1.85,
           label = "Demand is converted to FTE only after services are specified and valued in work RVUs.",
           size = 4.2, colour = "grey25") +
  coord_cartesian(xlim = c(.3, 10.9), ylim = c(1.4, 4), clip = "off") +
  labs(title = "Demand-to-FTE pathway", subtitle = "The model never divides provider FTE by cases, visits, or procedures") +
  theme_void(base_size = 12) +
  theme(plot.title = element_text(face = "bold"), plot.subtitle = element_text(colour = "grey25"),
        plot.margin = margin(18, 18, 18, 18),
        plot.background = element_rect(fill = "white", colour = NA),
        panel.background = element_rect(fill = "white", colour = NA))
ggsave(fig_path("readme_demand_to_fte_pathway.png"), p_pathway,
       width = 11, height = 3.8, dpi = 200, bg = "white")

message("Wrote README model-overview figures to figures/")
