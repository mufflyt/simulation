devtools::load_all(quiet = TRUE)
suppressPackageStartupMessages({
  library(ggplot2)
  library(dplyr)
  library(tidyr)
})

# ---- 1. Build BRFSS demand cells -------------------------------------------
message("Building BRFSS population cells...")
cells <- build_urps_population_cells(verbose = TRUE)

# ---- 2. Project demand for each access scenario ----------------------------
scenarios <- c("status_quo", "insurance_equity", "income_equity", "full_equity")
labels    <- c("Status quo", "Insurance equity", "Income equity", "Full equity")

demand_list <- lapply(scenarios, function(s) {
  d <- project_urps_demand(cells, access_scenario = s, verbose = FALSE)
  d$scenario       <- s
  d$scenario_label <- labels[match(s, scenarios)]
  d
})
demand_all <- bind_rows(demand_list)

# Compute national totals per scenario
totals <- demand_all %>%
  group_by(scenario, scenario_label) %>%
  summarise(total_fte = sum(demand_fte, na.rm = TRUE), .groups = "drop") %>%
  mutate(
    scenario_label = factor(scenario_label, levels = labels),
    lift_pct = 100 * (total_fte / total_fte[scenario == "status_quo"] - 1)
  )

sq_fte <- totals$total_fte[totals$scenario == "status_quo"]

# ---- 3. Plot A: total FTE demand by scenario (bar) -------------------------
p_bar <- ggplot(totals, aes(x = scenario_label, y = total_fte, fill = scenario_label)) +
  geom_col(width = 0.65) +
  geom_text(aes(label = sprintf("%.0f FTE\n%+.1f%%", total_fte, lift_pct)),
            vjust = -0.4, size = 3.5, lineheight = 0.9) +
  geom_hline(yintercept = sq_fte, linetype = "dashed", colour = "grey40") +
  scale_y_continuous(
    labels = scales::comma,
    expand = expansion(mult = c(0, 0.15))
  ) +
  scale_fill_manual(
    values = c("Status quo"       = "#4E79A7",
               "Insurance equity" = "#F28E2B",
               "Income equity"    = "#E15759",
               "Full equity"      = "#76B7B2"),
    guide = "none"
  ) +
  labs(
    title    = "Urogynecology demand under insurance and income equity scenarios",
    subtitle = "Required FTE if care-seeking barriers for uninsured / low-income women are removed\n(BRFSS 2023 insurance × income × age × race demand cells)",
    x        = NULL,
    y        = "Required FTE (national)",
    caption  = "Barriers: uninsured 0.58×, LT$25k 0.72× (Richter 2007 AJOG; MEPS 2020). Dashed line = status quo."
  ) +
  theme_minimal(base_size = 13) +
  theme(plot.title = element_text(face = "bold"),
        plot.caption = element_text(colour = "grey50", size = 9),
        panel.grid.major.x = element_blank())

# ---- 4. Plot B: demand by age group, all scenarios (line) ------------------
age_long <- demand_all %>%
  mutate(
    scenario_label = factor(scenario_label, levels = labels),
    age_group = factor(age_group, levels = c("18-34","35-44","45-64","65-74","75+"))
  )

p_age <- ggplot(age_long, aes(x = age_group, y = demand_fte,
                               colour = scenario_label, group = scenario_label)) +
  geom_line(linewidth = 1.1) +
  geom_point(size = 2.5) +
  scale_colour_manual(
    name   = "Scenario",
    values = c("Status quo"       = "#4E79A7",
               "Insurance equity" = "#F28E2B",
               "Income equity"    = "#E15759",
               "Full equity"      = "#76B7B2")
  ) +
  scale_y_continuous(labels = scales::comma) +
  labs(
    title    = "Demand by age group under access equity scenarios",
    subtitle = "Lift is concentrated in working-age bands where uninsured / low-income rates are highest",
    x        = "Age group",
    y        = "Required FTE",
    caption  = "BRFSS 2023 population-weighted demand cells; imputed PFD prevalence (Nygaard 2008 / Wu 2014)."
  ) +
  theme_minimal(base_size = 13) +
  theme(plot.title   = element_text(face = "bold"),
        plot.caption = element_text(colour = "grey50", size = 9),
        legend.position = "bottom")

# ---- 5. Plot C: barrier composition — pop weight by insurance × income ------
barrier_share <- cells %>%
  filter(!is.na(insurance), !is.na(income_tier)) %>%
  mutate(
    income_tier  = factor(income_tier,
                          levels = c("LT25k","25k_50k","50k_100k","GT100k"),
                          labels = c("<$25k","$25-50k","$50-100k",">$100k")),
    insurance    = factor(insurance, levels = c("Insured","Uninsured","Unknown"))
  ) %>%
  group_by(insurance, income_tier) %>%
  summarise(pop_weight = sum(pop_weight, na.rm = TRUE), .groups = "drop") %>%
  mutate(share = pop_weight / sum(pop_weight))

p_comp <- ggplot(barrier_share,
                 aes(x = income_tier, y = share, fill = insurance)) +
  geom_col(position = "stack", width = 0.7) +
  scale_y_continuous(labels = scales::percent) +
  scale_fill_manual(
    name   = "Insurance",
    values = c("Insured"   = "#4E79A7",
               "Uninsured" = "#E15759",
               "Unknown"   = "#BAB0AC")
  ) +
  labs(
    title    = "Population share by income tier and insurance status",
    subtitle = "BRFSS 2023 survey-weighted — basis for care-seeking barrier assignment",
    x        = "Income tier",
    y        = "Share of female population (18+)",
    caption  = "Survey-weighted cell totals from BRFSS 2023 women 18+."
  ) +
  theme_minimal(base_size = 13) +
  theme(plot.title   = element_text(face = "bold"),
        plot.caption = element_text(colour = "grey50", size = 9),
        legend.position = "right")

# ---- 6. Save plots ----------------------------------------------------------
out_dir <- "output/plots"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

ggsave(file.path(out_dir, "demand_scenario_bar.png"),    p_bar,  width = 8,  height = 5.5, dpi = 150)
ggsave(file.path(out_dir, "demand_scenario_by_age.png"), p_age,  width = 9,  height = 5.5, dpi = 150)
ggsave(file.path(out_dir, "barrier_composition.png"),    p_comp, width = 8,  height = 5,   dpi = 150)

message("\nSaved:")
message("  output/plots/demand_scenario_bar.png")
message("  output/plots/demand_scenario_by_age.png")
message("  output/plots/barrier_composition.png")

# ---- 7. Print summary table -------------------------------------------------
cat("\n=== Demand scenario summary ===\n")
print(
  totals %>%
    select(Scenario = scenario_label, `Required FTE` = total_fte, `Lift vs SQ` = lift_pct) %>%
    mutate(`Required FTE` = round(`Required FTE`), `Lift vs SQ` = round(`Lift vs SQ`, 1)),
  n = Inf
)
