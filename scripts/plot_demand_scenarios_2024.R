devtools::load_all(quiet = TRUE)
suppressPackageStartupMessages({
  library(ggplot2)
  library(dplyr)
  library(tidyr)
  library(scales)
})

scenarios <- c("status_quo", "insurance_equity", "income_equity", "full_equity")
labels    <- c("Status quo", "Insurance equity", "Income equity", "Full equity")
pal <- c("Status quo"       = "#4E79A7",
         "Insurance equity" = "#F28E2B",
         "Income equity"    = "#E15759",
         "Full equity"      = "#76B7B2")

run_year <- function(rds, yr) {
  brfss <- load_brfss_women(rds, verbose = FALSE)
  cells <- build_urps_population_cells(brfss_women = brfss, verbose = FALSE)
  dplyr::bind_rows(lapply(scenarios, function(s) {
    d <- project_urps_demand(cells, access_scenario = s, verbose = FALSE)
    d$scenario       <- s
    d$scenario_label <- factor(labels[match(s, scenarios)], levels = labels)
    d$year           <- yr
    d
  }))
}

message("Building demand for 2023...")
d23 <- run_year("data-raw/brfss/brfss_2023_women18plus.rds", 2023)
message("Building demand for 2024...")
d24 <- run_year("data-raw/brfss/brfss_2024_women18plus.rds", 2024)
all <- dplyr::bind_rows(d23, d24) |>
  dplyr::mutate(year = factor(year))

# ---- Summary tables ---------------------------------------------------------
totals <- all |>
  dplyr::group_by(year, scenario, scenario_label) |>
  dplyr::summarise(total_fte = sum(demand_fte, na.rm = TRUE), .groups = "drop") |>
  dplyr::group_by(year) |>
  dplyr::mutate(lift_pct = 100 * (total_fte /
    total_fte[scenario == "status_quo"] - 1)) |>
  dplyr::ungroup()

cat("\n=== Demand scenario summary ===\n")
print(
  totals |>
    dplyr::select(Year = year, Scenario = scenario_label,
                  `Required FTE` = total_fte, `Lift vs SQ (%)` = lift_pct) |>
    dplyr::mutate(`Required FTE` = round(`Required FTE`),
                  `Lift vs SQ (%)` = round(`Lift vs SQ (%)`, 1)),
  n = Inf
)

# ---- Plot A: grouped bar — total FTE by scenario and year -------------------
p_bar <- ggplot(totals,
                aes(x = scenario_label, y = total_fte,
                    fill = scenario_label, alpha = year)) +
  geom_col(position = position_dodge(width = 0.7), width = 0.6) +
  geom_text(aes(label = sprintf("%.0f", total_fte)),
            position = position_dodge(width = 0.7),
            vjust = -0.5, size = 3.2) +
  scale_fill_manual(values = pal, guide = "none") +
  scale_alpha_manual(name = "Survey year", values = c("2023" = 0.55, "2024" = 1.0)) +
  scale_y_continuous(labels = comma, expand = expansion(mult = c(0, 0.12))) +
  labs(
    title    = "Urogynecology demand under access equity scenarios — 2023 vs 2024 BRFSS",
    subtitle = "Required FTE if insurance / income care-seeking barriers are removed",
    x = NULL, y = "Required FTE (national)",
    caption  = "Barriers: uninsured 0.58×, LT$25k 0.72× (Richter 2007 AJOG; MEPS 2020).\nDarker bars = 2024 BRFSS."
  ) +
  theme_minimal(base_size = 13) +
  theme(plot.title   = element_text(face = "bold"),
        plot.caption = element_text(colour = "grey50", size = 9),
        panel.grid.major.x = element_blank(),
        legend.position = "bottom")

# ---- Plot B: demand by age group, faceted by year ---------------------------
age_long <- all |>
  dplyr::mutate(age_group = factor(age_group,
                  levels = c("18-34","35-44","45-64","65-74","75+")))

p_age <- ggplot(age_long,
                aes(x = age_group, y = demand_fte,
                    colour = scenario_label, group = scenario_label,
                    linetype = year)) +
  geom_line(linewidth = 1.0) +
  geom_point(aes(shape = year), size = 2.5) +
  facet_wrap(~year, ncol = 2) +
  scale_colour_manual(name = "Scenario", values = pal) +
  scale_linetype_manual(name = "Year", values = c("2023" = "dashed", "2024" = "solid")) +
  scale_shape_manual(name = "Year", values = c("2023" = 1, "2024" = 16)) +
  scale_y_continuous(labels = comma) +
  labs(
    title    = "Demand by age group — 2023 vs 2024 BRFSS",
    subtitle = "Lift from equity scenarios is largest in the 45–64 band both years",
    x = "Age group", y = "Required FTE",
    caption  = "Imputed PFD prevalence (Nygaard 2008 / Wu 2014); BRFSS survey-weighted."
  ) +
  theme_minimal(base_size = 12) +
  theme(plot.title      = element_text(face = "bold"),
        plot.caption    = element_text(colour = "grey50", size = 9),
        legend.position = "bottom",
        strip.text      = element_text(face = "bold", size = 12))

# ---- Plot C: year-over-year lift per scenario (lollipop) --------------------
yoy <- totals |>
  dplyr::select(year, scenario_label, total_fte) |>
  tidyr::pivot_wider(names_from = year, values_from = total_fte) |>
  dplyr::mutate(yoy_change = `2024` - `2023`,
                yoy_pct    = 100 * (`2024` / `2023` - 1))

p_yoy <- ggplot(yoy, aes(x = scenario_label, y = yoy_change, colour = scenario_label)) +
  geom_segment(aes(xend = scenario_label, y = 0, yend = yoy_change),
               linewidth = 1.2) +
  geom_point(size = 5) +
  geom_text(aes(label = sprintf("%+.0f FTE\n(%+.1f%%)", yoy_change, yoy_pct)),
            vjust = -0.6, size = 3.3, colour = "grey30") +
  geom_hline(yintercept = 0, linetype = "dashed", colour = "grey60") +
  scale_colour_manual(values = pal, guide = "none") +
  scale_y_continuous(labels = comma, expand = expansion(mult = c(0.1, 0.25))) +
  labs(
    title    = "Year-over-year demand change: 2024 vs 2023 BRFSS",
    subtitle = "Driven by population growth (93M vs 90M survey-weighted women)",
    x = NULL, y = "FTE change (2024 minus 2023)",
    caption  = "Same barrier multipliers and prevalence assumptions both years."
  ) +
  theme_minimal(base_size = 13) +
  theme(plot.title   = element_text(face = "bold"),
        plot.caption = element_text(colour = "grey50", size = 9),
        panel.grid.major.x = element_blank())

# ---- Save -------------------------------------------------------------------
out_dir <- "output/plots"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
ggsave(file.path(out_dir, "demand_2024_bar.png"),    p_bar, width = 10, height = 5.5, dpi = 150)
ggsave(file.path(out_dir, "demand_2024_by_age.png"), p_age, width = 11, height = 5.5, dpi = 150)
ggsave(file.path(out_dir, "demand_2024_yoy.png"),    p_yoy, width = 8,  height = 5,   dpi = 150)
message("Saved to output/plots/")
