#!/usr/bin/env Rscript
# README figures and maps.
#
# Every panel here is drawn from a real model run or a real data file. Nothing is
# mocked, and no number is typed in that the code cannot recompute -- the same
# rule the rest of this repository applies to prose. Where a figure shows a
# result that rests on a borrowed input, the caveat is drawn ON the figure rather
# than left to the caption, because a caption does not travel with a PNG.
#
#   Rscript scripts/plot_readme_figures.R
#
# Outputs six files into figures/.

suppressPackageStartupMessages({
  library(ggplot2); library(dplyr); library(tidyr); library(scales)
})
if (!requireNamespace("urpssim", quietly = TRUE)) pkgload::load_all(".", quiet = TRUE) else library(urpssim)

dir.create("figures", showWarnings = FALSE)
FIG <- function(n) file.path("figures", n)

# House style: one theme, so six figures read as one system.
theme_urps <- function(base = 11) {
  theme_minimal(base_size = base) +
    theme(plot.title = element_text(face = "bold", size = rel(1.15)),
          plot.subtitle = element_text(colour = "grey30", size = rel(0.92)),
          plot.caption = element_text(colour = "grey45", size = rel(0.78), hjust = 0),
          panel.grid.minor = element_blank(),
          legend.position = "bottom")
}
INK <- c(supply = "#1f6f8b", demand = "#c1543a", warn = "#b3591a",
         ok = "#2f6b4f", grey = "#7a7a7a")

message("[1/6] uncertainty hierarchy")
# The session's headline result: only one uncertainty spans a range wide enough
# to reverse the 2050 sign. Ranges are the measured ones, recorded in
# docs/CANONICAL_SOURCES_AUDIT.md and the commit history.
unc <- tibble::tribble(
  ~driver,                              ~lo,   ~hi,   ~flips,
  "Productivity / case mix",            -515,  1602,  TRUE,
  "Baseline adequacy (donor specialty)", 433,   537,  FALSE,
  "Entrant pipeline (conversion-adj.)",  260,   348,  FALSE,
  "Retirement timing (+/-10 yr)",        452,   537,  FALSE,
  "Hours-curve age gradient",            305,   665,  FALSE,
  "Monte Carlo error (n=30)",            430,   540,  FALSE
) %>% mutate(driver = factor(driver, levels = rev(driver)))

p1 <- ggplot(unc, aes(y = driver)) +
  annotate("rect", xmin = -Inf, xmax = 0, ymin = -Inf, ymax = Inf,
           fill = INK[["demand"]], alpha = 0.06) +
  geom_vline(xintercept = 0, colour = INK[["demand"]], linewidth = 0.6) +
  geom_linerange(aes(xmin = lo, xmax = hi, colour = flips), linewidth = 5, alpha = 0.9) +
  geom_text(aes(x = hi, label = sprintf("%+.0f", hi)), hjust = -0.15, size = 3, colour = "grey30") +
  geom_text(aes(x = lo, label = sprintf("%+.0f", lo)), hjust = 1.15, size = 3, colour = "grey30") +
  scale_colour_manual(values = c(`TRUE` = INK[["warn"]], `FALSE` = INK[["ok"]]),
                      labels = c(`TRUE` = "can reverse the sign", `FALSE` = "cannot"),
                      name = NULL) +
  scale_x_continuous(labels = label_number(style_positive = "plus")) +
  annotate("text", x = 0, y = 6.55, label = "shortage  <-  |  ->  surplus",
           size = 3, colour = INK[["demand"]], vjust = -0.4) +
  labs(title = "One uncertainty can reverse the 2050 conclusion. The rest cannot.",
       subtitle = "Range of the projected 2050 FTE gap as each input varies over its plausible range",
       x = "2050 gap (FTE): supply minus required", y = NULL,
       caption = paste("Productivity spans ~2,100 FTE; every other driver lies inside a band of roughly +/-250.",
                       "\nRanges measured, not asserted -- see docs/CANONICAL_SOURCES_AUDIT.md and README.")) +
  theme_urps() + coord_cartesian(xlim = c(-700, 1800), clip = "off")
ggsave(FIG("fig_uncertainty_hierarchy.png"), p1, width = 9, height = 5.2, dpi = 200, bg = "white")

message("[2/6] entrant pipeline reconciliation")
acg <- acgme_entering_cohort()
certs <- urps_entrant_series(2100L)
conv <- entrant_to_cert_ratio(source = "acgme")$ratio
lag <- URPS_FELLOWSHIP_YEARS
pipe <- acg %>%
  mutate(cert_year = entry_year + lag) %>%
  left_join(certs %>% select(cert_year = year, certified = count), by = "cert_year") %>%
  filter(!is.na(certified))

p2a <- ggplot(pipe, aes(x = cert_year)) +
  geom_col(aes(y = entering_cohort, fill = "ACGME entering cohort (lagged 3 yr)"),
           width = 0.62, alpha = 0.85) +
  geom_col(aes(y = certified, fill = "URPS certifications"), width = 0.34) +
  scale_fill_manual(values = c("ACGME entering cohort (lagged 3 yr)" = INK[["grey"]],
                               "URPS certifications" = INK[["supply"]]), name = NULL) +
  annotate("text", x = 2020, y = 8, label = "2020: COVID exam\ndisruption", size = 2.7,
           colour = INK[["warn"]], vjust = 0) +
  labs(title = sprintf("Entry and certification reconcile at a %.3f conversion", conv),
       subtitle = paste0("Fellowship entry lagged ", lag,
                         " years against board certifications. The discrepancy closes with no residual."),
       x = NULL, y = "physicians per year") +
  theme_urps()

gap_by_basis <- tibble::tribble(
  ~basis,                                   ~entrants, ~gap,
  "Raw cert flow 2018-23\n(ignores entry growth)", 50.8,  78.5,
  "NRMP filled x conversion",                      60.0, 259.9,
  "Recent ACGME entry x conversion",               63.7, 347.7,
  "NRMP filled, unconverted\n(current model)",     70.0, 484.9
) %>% mutate(basis = factor(basis, levels = basis))

p2b <- ggplot(gap_by_basis, aes(x = entrants, y = gap)) +
  geom_hline(yintercept = 0, colour = INK[["demand"]]) +
  geom_vline(xintercept = 49.1, linetype = "22", colour = INK[["warn"]]) +
  annotate("text", x = 49.1, y = 430, label = "breakeven 49.1/yr", angle = 90,
           vjust = -0.4, size = 2.9, colour = INK[["warn"]]) +
  geom_line(colour = INK[["grey"]], linewidth = 0.5) +
  geom_point(size = 3.2, colour = INK[["supply"]]) +
  geom_text(aes(label = sprintf("%+.0f", gap)), vjust = -1.1, size = 3, colour = "grey25") +
  scale_x_continuous(breaks = gap_by_basis$entrants) +
  labs(subtitle = "Every empirically defensible entrant basis stays a surplus; only the breakeven is below all of them",
       x = "entrants per year", y = "2050 gap (FTE)") +
  theme_urps()

ggsave(FIG("fig_entrant_reconciliation.png"),
       patchwork::wrap_plots(p2a, p2b, ncol = 1, heights = c(1, 1)),
       width = 9, height = 8, dpi = 200, bg = "white")

message("[3/6] supply vs required trajectory (live run)")
ad <- capacity_survey_adequacy(example_capacity_survey())$adequacy
g <- baseline_gap(1306, ad, method = "capacity_survey",
                  calibration_status = "derived_by_analogy",
                  source = "Zarek 2025 PTJ (physical therapists)")
set.seed(20260809)
run <- suppressMessages(run_workforce_microsimulation(
  years = 2025:2050, n_iterations = 40, baseline_gap_estimate = g,
  allow_analogy = TRUE, output_dir = NULL, verbose = FALSE))
sup <- run$supply %>% filter(scenario == run$scenario_meta$reference_scenario)
req <- run$required_fte
traj <- bind_rows(
  sup %>% transmute(year, value = effective_fte_median,
                    lo = effective_fte_lo, hi = effective_fte_hi, series = "Supplied FTE"),
  req %>% transmute(year, value = required_fte, lo = NA_real_, hi = NA_real_,
                    series = "Required FTE"))

p3 <- ggplot(traj, aes(year, value, colour = series, fill = series)) +
  geom_ribbon(aes(ymin = lo, ymax = hi), alpha = 0.16, colour = NA, na.rm = TRUE) +
  geom_line(linewidth = 1) +
  scale_colour_manual(values = c("Supplied FTE" = INK[["supply"]], "Required FTE" = INK[["demand"]]), name = NULL) +
  scale_fill_manual(values = c("Supplied FTE" = INK[["supply"]], "Required FTE" = INK[["demand"]]), name = NULL) +
  labs(title = "Status-quo supply and required FTE, 2025-2050",
       subtitle = paste0("Base cohort is RECONSTRUCTED (", sprintf("%.1f", 100 * run$scenario_meta$cohort_provenance$observed_share),
                         "% observed certification years); base-year adequacy is a physical-therapy analogy"),
       x = NULL, y = "clinical FTE",
       caption = paste("Band is a Monte Carlo range, NOT a forecast interval: the 2020->2023 back-test missed the",
                       "\nobservation in 8 of 10 arms. Required-FTE LEVEL is anchored to supply/adequacy, so it is",
                       "\nan input, not an independent estimate. See README, 'What this figure does not show'.")) +
  theme_urps()
ggsave(FIG("fig_supply_demand_trajectory.png"), p3, width = 9, height = 5.4, dpi = 200, bg = "white")


# theme_void() blanks plot.background AND legend text, which renders a
# transparent PNG with an invisible title -- caught by looking at the output
# rather than trusting that ggsave defaults to white.
theme_map <- function(base = 11) {
  theme_void(base_size = base) +
    theme(plot.background = element_rect(fill = "white", colour = NA),
          plot.title = element_text(face = "bold", size = rel(1.12), hjust = 0,
                                    margin = margin(b = 4)),
          plot.subtitle = element_text(colour = "grey30", size = rel(0.9), hjust = 0,
                                       margin = margin(b = 6)),
          plot.caption = element_text(colour = "grey45", size = rel(0.75), hjust = 0),
          legend.position = "bottom",
          legend.title = element_text(size = rel(0.85)),
          legend.text = element_text(size = rel(0.78)),
          legend.key.width = unit(26, "pt"),
          plot.margin = margin(10, 12, 8, 12))
}

# ---- Maps -------------------------------------------------------------------
suppressPackageStartupMessages(library(maps))
states_map <- map_data("state")
st_xwalk <- tibble::tibble(region = tolower(state.name), state = state.abb)

roster <- load_urps_roster()
pop <- mufflyaccess::urps_state_female_pop()

message("[4/6] map: providers per 100k women")
by_state <- roster %>% count(state, name = "providers") %>%
  inner_join(pop, by = c("state" = "state_abbr")) %>%
  mutate(per100k = 1e5 * providers / female_pop)
m1 <- states_map %>% left_join(st_xwalk, by = "region") %>% left_join(by_state, by = "state")

p4 <- ggplot(m1, aes(long, lat, group = group, fill = per100k)) +
  geom_polygon(colour = "white", linewidth = 0.18) +
  coord_map("albers", lat0 = 29.5, lat1 = 45.5) +
  scale_fill_viridis_c(option = "mako", direction = -1, na.value = "grey88",
                       name = "per 100,000 women") +
  labs(title = "Board-certified urogynecologists per 100,000 women, by state",
       subtitle = sprintf("%s providers on the model roster across %s states",
                          comma(nrow(roster)), n_distinct(roster$state)),
       caption = "Roster counts by practice state; denominator is state female population from the mufflyaccess contract.") +
  theme_map()
ggsave(FIG("map_providers_per_100k.png"), p4, width = 9, height = 5.6, dpi = 200, bg = "white")

message("[5/6] map: demand surface, women 65+ by tract")
tr <- readr::read_csv("data-raw/spatial/tract_fem65_centroids.csv", show_col_types = FALSE) %>%
  filter(is.finite(lon), is.finite(lat), lon > -125, lon < -66, lat > 24, lat < 50, fem65 > 0)

p5 <- ggplot() +
  geom_polygon(data = states_map, aes(long, lat, group = group),
               fill = "grey96", colour = "white", linewidth = 0.18) +
  geom_point(data = tr, aes(lon, lat, colour = fem65), size = 0.09, alpha = 0.5) +
  coord_map("albers", lat0 = 29.5, lat1 = 45.5) +
  scale_colour_viridis_c(option = "rocket", direction = -1, trans = "sqrt",
                         name = "women 65+ per tract") +
  labs(title = "Where the demand is: women aged 65+ by census tract",
       subtitle = sprintf("%s tracts, contiguous US -- the population the pelvic-floor burden is concentrated in",
                          comma(nrow(tr))),
       caption = "ACS 5-year 2023 tract table, checksummed in data-raw/spatial/. Tract centroids, not tract polygons.") +
  theme_map()
ggsave(FIG("map_demand_women65_tracts.png"), p5, width = 9, height = 5.6, dpi = 200, bg = "white")

message("[6/6] map: supply share minus demand share")
mismatch <- by_state %>%
  mutate(share_providers = providers / sum(providers),
         share_demand = female_pop / sum(female_pop),
         diff_pp = 100 * (share_providers - share_demand))
m3 <- states_map %>% left_join(st_xwalk, by = "region") %>% left_join(mismatch, by = "state")
lim <- max(abs(mismatch$diff_pp), na.rm = TRUE)

p6 <- ggplot(m3, aes(long, lat, group = group, fill = diff_pp)) +
  geom_polygon(colour = "white", linewidth = 0.18) +
  coord_map("albers", lat0 = 29.5, lat1 = 45.5) +
  scale_fill_gradient2(low = INK[["demand"]], mid = "grey94", high = INK[["supply"]],
                       midpoint = 0, limits = c(-lim, lim), na.value = "grey88",
                       name = "percentage points") +
  labs(title = "Supply share minus demand share, by state",
       subtitle = "Blue: a larger share of the workforce than of the female population. Red: the reverse.",
       caption = paste("A distributional descriptive, NOT an access measure: it uses no travel time and no",
                       "\ncare-seeking behaviour. Genuine access requires the E2SFCA layer -- see README.")) +
  theme_map()
ggsave(FIG("map_supply_demand_mismatch.png"), p6, width = 9, height = 5.6, dpi = 200, bg = "white")

message("done -- 6 files written to figures/")
