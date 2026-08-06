#!/usr/bin/env Rscript

# Plot the condition-specific service pathway (R/supply-entrant_regime) against the flat service map
# it replaces.
#
# Two panels, because there are two distinct claims:
#
#   A. the cascade -- patients thin out as they move conservative -> testing ->
#      procedure -> follow-up -> recurrence, so a procedure accrues only to those
#      who reached it. One series per facet, so no legend: the facet strip names
#      the condition and the bar length carries the magnitude.
#   B. what that does to service volume -- two series (flat, staged), so a legend
#      is required and both are also direct-labelled.
#
# Colours are the validated categorical slots 1 and 2 (blue, orange); the pair
# clears every check (adjacent CVD dE 24.7 protan, normal-vision dE 33.6,
# contrast >= 3:1 on the light surface). Text stays in ink tokens rather than
# series colour, so identity is never carried by colour alone.
#
# NOTE ON WHAT THIS FIGURE IS NOT: every pathway rate is expert judgement
# (confidence = "low", status "uncalibrated_illustrative"). The figure shows
# STRUCTURE -- how workload redistributes when the cascade is modelled -- not a
# workforce estimate. The subtitle says so.
#
# ---------------------------------------------------------------------------
# HOW THIS FIGURE IS MADE (everything needed to reproduce it)
# ---------------------------------------------------------------------------
#
# Run:      Rscript scripts/plot_condition_service_pathway.R      (from repo root)
# Writes:   figures/condition_service_pathway.png                 (override with
#           the PATHWAY_FIGURE environment variable)
# Needs:    nothing external -- no SIMULATION_DATA_ROOT, no mounted drive, no
#           mufflyaccess. The population is synthetic and defined below, so the
#           figure rebuilds identically on any machine.
#
# INPUTS
#   pop_by_age  a synthetic exponential age structure, ages 40-85,
#               population = round(2e6 * exp(-0.02 * (age - 40))). Illustrative;
#               it is NOT the Census-NPP series the model uses in production.
#   n = 5e4     synthetic persons drawn per run by simulate_lifecourse_demand().
#   seed = 1    both runs use the same seed, so flat and staged differ ONLY by
#               the pathway argument and not by Monte Carlo noise. This is the
#               whole point of the comparison -- do not vary the seed between the
#               two calls.
#   year = 2025 single cross-section.
#
# WHAT IS COMPUTED
#   staged   simulate_lifecourse_demand(..., use_condition_pathway = TRUE)
#   flat     simulate_lifecourse_demand(..., use_condition_pathway = FALSE)
#   Panel A  pathway_stage_entrants() on staged$treated_national -- the cascade.
#   Panel B  the two runs' $service_volumes, outer-joined by service. The flat
#            map has NO postoperative_care row (that is the gap being shown), so
#            the join yields NA and it is plotted as 0; the caption says which.
#   FTE      convert_workload_to_fte() at WRVU_PER_FTE_BENCHMARK[["median"]] for
#            both runs; the percentage in the subtitle is computed, not typed.
#
# COLOUR CHOICES (validated, not eyeballed)
#   Slots 1 and 2 of the reference categorical palette: #2a78d6 blue (Staged),
#   #eb6834 orange (Flat). Validated with the data-viz validator in light mode
#   against surface #fcfcfb -- all six checks PASS: lightness band, chroma floor,
#   adjacent CVD separation (dE 24.7 protan, 32.7 tritan; >= 8 target),
#   normal-vision floor (dE 33.6; >= 15 floor), contrast >= 3:1.
#   Panel A encodes magnitude by bar length only, so it uses a single hue -- a
#   colour ramp there would encode the stage twice (position already carries it).
#   Text uses ink tokens (#0b0b0b primary, #52514e secondary), never the series
#   colour, so identity never rests on colour alone.
#
# DELIBERATE CHOICES A REVIEWER SHOULD KNOW ABOUT
#   * Panel A uses scales = "free_x". Each condition is its own funnel and AI is
#     ~10x smaller than UI/POP, so a shared scale would flatten AI to invisible.
#     The cost is that bar lengths are NOT comparable across facets, which the
#     panel subtitle states outright and the direct labels defuse.
#   * Panel A has no x axis (labels = NULL): with free scales an axis would
#     invite exactly the cross-facet comparison the labels are there to prevent.
#   * Bars are dodged with a gap (width 0.66 inside position_dodge 0.74) so
#     adjacent fills are separated by surface rather than touching.
#   * Output is 13.5 x 8.5 in at 200 dpi (2700 x 1700 px) on the light surface.
#
# The figure was rendered and visually inspected for label collisions, overflow
# and geometry before being committed; re-inspect after changing any dimension.

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(patchwork)
  library(pkgload)
})

root <- normalizePath(".")
pkgload::load_all(root, quiet = TRUE)

output_path <- Sys.getenv("PATHWAY_FIGURE",
                          file.path(root, "figures", "condition_service_pathway.png"))
dir.create(dirname(output_path), showWarnings = FALSE, recursive = TRUE)

# ---- palette + theme -------------------------------------------------------

series_1   <- "#2a78d6"   # categorical slot 1 (blue)
series_2   <- "#eb6834"   # categorical slot 2 (orange)
ink_1      <- "#0b0b0b"   # text-primary
ink_2      <- "#52514e"   # text-secondary
surface_1  <- "#fcfcfb"
grid_col   <- "#e6e5e1"

theme_pathway <- theme_minimal(base_size = 11) +
  theme(
    plot.background   = element_rect(fill = surface_1, colour = NA),
    panel.background  = element_rect(fill = surface_1, colour = NA),
    panel.grid.major.y = element_blank(),
    panel.grid.minor  = element_blank(),
    panel.grid.major.x = element_line(colour = grid_col, linewidth = 0.3),
    axis.text         = element_text(colour = ink_2),
    axis.title        = element_text(colour = ink_2),
    strip.text        = element_text(colour = ink_1, face = "bold", hjust = 0),
    plot.title        = element_text(colour = ink_1, face = "bold", size = 13),
    plot.subtitle     = element_text(colour = ink_2, size = 9.5),
    plot.caption      = element_text(colour = ink_2, size = 8, hjust = 0),
    legend.position   = "top",
    legend.title      = element_blank(),
    legend.text       = element_text(colour = ink_1)
  )

condition_labels <- c(ui = "Urinary incontinence (UI)",
                      pop = "Prolapse (POP)",
                      ai  = "Anal incontinence (AI)")
stage_labels <- c(conservative = "Conservative", testing = "Testing",
                  procedure = "Procedure", followup = "Follow-up",
                  recurrence = "Recurrence")

# ---- run the model ---------------------------------------------------------

pop_by_age <- tibble::tibble(age = 40:85,
                             population = round(2e6 * exp(-0.02 * (40:85 - 40))))

staged <- simulate_lifecourse_demand(pop_by_age, 2025L, n = 5e4, seed = 1,
                                     use_condition_pathway = TRUE)
flat   <- simulate_lifecourse_demand(pop_by_age, 2025L, n = 5e4, seed = 1,
                                     use_condition_pathway = FALSE)

fte <- function(s) convert_workload_to_fte(
  s$service_volumes, wrvu_per_fte = WRVU_PER_FTE_BENCHMARK[["median"]])$required_fte
fte_staged <- fte(staged)
fte_flat   <- fte(flat)

message(sprintf("flat FTE %.1f | staged FTE %.1f (%.0f%%)",
                fte_flat, fte_staged, 100 * (fte_staged / fte_flat - 1)))

# ---- Panel A: the cascade --------------------------------------------------

entrants <- pathway_stage_entrants(staged$treated_national) %>%
  mutate(condition = factor(condition_labels[condition],
                            levels = unname(condition_labels)),
         stage = factor(stage_labels[stage], levels = rev(unname(stage_labels))))

panel_a <- ggplot(entrants, aes(x = entering, y = stage)) +
  geom_col(fill = series_1, width = 0.68) +
  geom_text(aes(label = scales::label_number(scale_cut = scales::cut_short_scale())(entering)),
            hjust = -0.15, size = 3, colour = ink_2) +
  facet_wrap(~condition, ncol = 1, scales = "free_x") +
  scale_x_continuous(expand = expansion(mult = c(0, 0.22)), labels = NULL) +
  labs(title = "A. Patients thin out along the pathway",
       subtitle = paste("Entrants per stage. A procedure accrues only to patients who failed conservative care and completed testing.",
                        "\nEach condition is scaled to its own maximum, so bar lengths compare WITHIN a panel, not across them -- read the labels.",
                        "\nAI is an order of magnitude smaller than UI and POP."),
       x = NULL, y = NULL) +
  theme_pathway +
  theme(axis.ticks = element_blank())

# ---- Panel B: what it does to service volume -------------------------------

vols <- full_join(
  transmute(flat$service_volumes, service, Flat = volume),
  transmute(staged$service_volumes, service, Staged = volume),
  by = "service") %>%
  # The flat map never generated post-operative follow-up at all, so its value is
  # absent rather than zero. Plot it as zero and let the caption say which it is.
  mutate(across(c(Flat, Staged), ~tidyr::replace_na(.x, 0))) %>%
  tidyr::pivot_longer(c(Flat, Staged), names_to = "model", values_to = "volume") %>%
  mutate(service = gsub("_", " ", service),
         service = factor(service, levels = rev(sort(unique(service)))),
         model = factor(model, levels = c("Flat", "Staged")))

panel_b <- ggplot(vols, aes(x = volume / 1e6, y = service, fill = model)) +
  geom_col(position = position_dodge(width = 0.74), width = 0.66) +
  scale_fill_manual(values = c(Flat = series_2, Staged = series_1)) +
  scale_x_continuous(expand = expansion(mult = c(0, 0.06))) +
  labs(title = "B. Workload shifts out of procedures and into follow-up",
       subtitle = "National service units (millions), 2025. Post-operative care is new: the flat map produced none.",
       x = "Service units (millions)", y = NULL) +
  theme_pathway

# ---- compose ---------------------------------------------------------------

fig <- (panel_a | panel_b) +
  plot_annotation(
    title = "Condition-specific service pathway vs the flat service map",
    subtitle = sprintf(
      "Required FTE %.0f staged vs %.0f flat (%+.0f%%). UI, POP and AI were always modelled separately; what is new is the stage cascade.",
      fte_staged, fte_flat, 100 * (fte_staged / fte_flat - 1)),
    caption = paste(
      "STRUCTURE, NOT AN ESTIMATE. Every pathway rate is expert judgement (confidence = \"low\";",
      "condition_pathway_status() = \"uncalibrated_illustrative\"), so assert_publishable_workload() still refuses these numbers.",
      "\nAI testing and procedure stages use stand-in CPT codes: anorectal manometry, endoanal ultrasound, sacral neuromodulation and",
      "sphincteroplasty are absent from URPS_CPT_BASKET, so AI procedural workload is understated.",
      "\nPost-operative care carries work RVU 0 (090-day global period), so it adds volume without adding RVUs."),
    theme = theme(
      plot.background = element_rect(fill = surface_1, colour = NA),
      plot.title      = element_text(colour = ink_1, face = "bold", size = 15),
      plot.subtitle   = element_text(colour = ink_2, size = 10),
      plot.caption    = element_text(colour = ink_2, size = 7.5, hjust = 0)))

ggsave(output_path, fig, width = 13.5, height = 8.5, dpi = 200, bg = surface_1)
message("Wrote ", output_path)
