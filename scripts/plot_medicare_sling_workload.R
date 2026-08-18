#!/usr/bin/env Rscript

# Plot annual Medicare FFS sling activity for clinicians tagged as URPS.
#
# This analysis is intentionally service-specific: the available cache contains
# CPT 57288 claims only, so it is NOT an estimate of total URPS capacity or
# clinical-hours FTE. `MEDICARE_SLING_CACHE` can point to an equivalent local
# cache; the default is the project's external-drive location.

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(patchwork)
  library(pkgload)
})

root <- normalizePath(".")
pkgload::load_all(root, quiet = TRUE)

cache_path <- Sys.getenv(
  "MEDICARE_SLING_CACHE",
  # Resolved, not hardcoded: the volume name varies across remounts
  # (MufflySamsung / MufflySamsung 1 / MufflySamsung 1 1).
  if (requireNamespace("researchpaths", quietly = TRUE)) {
    researchpaths::resolve_file_on_volume(
      relative_path  = file.path("sling-volume-patterns", "data", "cache",
                                 "provider_volume.rds"),
      volume_pattern = "MufflySamsung*",
      env_var        = "URPS_SLING_VOLUME_CACHE"
    )
  } else NA_character_
)
output_path <- Sys.getenv(
  "MEDICARE_SLING_FIGURE",
  file.path(root, "figures", "medicare_sling_workload_index.png")
)

if (is.na(cache_path) || !file.exists(cache_path)) {
  message("Medicare sling cache not present; skipping medicare_sling_workload_index.png generation.")
  quit(save = "no", status = 0)
}
dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)

claims_input <- readRDS(cache_path) |>
  filter(.data$subspecialty_abog == "URPS" |
           .data$specialty_group == "URPS (urology)") |>
  transmute(
    npi = as.character(.data$Rndrng_NPI),
    year = as.integer(.data$puf_year),
    clinician_tag = if_else(.data$specialty_group == "URPS (urology)",
                            "Urology-tagged", "OB/GYN-tagged"),
    work_rvu = .data$annual_sling_count *
      CMS_WORK_RVU$work_rvu[CMS_WORK_RVU$hcpcs == "57288"]
  )

# This cache contains only clinicians with a reported CPT 57288 line. Its
# roster is therefore appropriate for this service-specific activity analysis,
# but not for estimating the active URPS workforce.
roster <- distinct(claims_input, npi, year, clinician_tag)
provider <- medicare_work_rvu_by_provider(
  claims = select(claims_input, npi, year, work_rvu),
  roster = roster,
  minimum_wrvu = 0
)
indexed <- bind_rows(lapply(split(provider, provider$year), medicare_workload_index))

summary <- indexed |>
  group_by(.data$year, .data$clinician_tag) |>
  summarise(
    providers = n(),
    mean_index = mean(.data$medicare_workload_index),
    se = stats::sd(.data$medicare_workload_index) / sqrt(n()),
    lo = pmax(0, mean_index - 1.96 * se),
    hi = mean_index + 1.96 * se,
    .groups = "drop"
  )

cols <- c("OB/GYN-tagged" = "#D55E00", "Urology-tagged" = "#0072B2")
p_workload <- ggplot(summary, aes(.data$year, .data$mean_index,
                                  colour = .data$clinician_tag,
                                  group = .data$clinician_tag)) +
  geom_hline(yintercept = 1, linetype = "dashed", colour = "grey45", linewidth = .45) +
  geom_errorbar(aes(ymin = .data$lo, ymax = .data$hi), width = .16, linewidth = .5) +
  geom_line(linewidth = .9) +
  geom_point(size = 2.3) +
  annotate("text", x = 2013.15, y = 1.015,
           label = "1.0 = annual combined-cohort average", hjust = 0,
           size = 3.2, colour = "grey35") +
  scale_colour_manual(values = cols) +
  scale_x_continuous(breaks = 2013:2023) +
  coord_cartesian(ylim = c(.55, 1.35)) +
  labs(
    title = "Relative Medicare sling workload was consistently higher in the OB/GYN-tagged cohort",
    subtitle = "CPT 57288 sling procedures; each year, 1.0 equals the average sling volume across the combined cohort",
    x = NULL, y = "Mean sling volume relative to annual cohort average", colour = NULL
  ) +
  theme_minimal(base_size = 12) +
  theme(legend.position = "top", panel.grid.minor = element_blank())

p_counts <- ggplot(summary, aes(.data$year, .data$providers, fill = .data$clinician_tag)) +
  geom_col(position = position_dodge(width = .72), width = .62) +
  scale_fill_manual(values = cols) +
  scale_x_continuous(breaks = 2013:2023) +
  labs(
    x = NULL, y = "Providers with a reported CPT 57288 line", fill = NULL,
    caption = paste("Claims are Medicare FFS CPT 57288 only. This is a service-specific activity comparison—",
                    "not total URPS capacity or clinical-hours FTE.")
  ) +
  theme_minimal(base_size = 12) +
  theme(legend.position = "none", panel.grid.minor = element_blank(),
        plot.caption = element_text(hjust = 0, colour = "grey25"))

ggsave(output_path, p_workload / p_counts + plot_layout(heights = c(2.1, 1)),
       width = 11, height = 8.1, dpi = 220)
message("Wrote ", output_path)
