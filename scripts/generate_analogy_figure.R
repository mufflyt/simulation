#!/usr/bin/env Rscript
# =============================================================================
# Generate Restaurant Analogy Figure for Latent Adequacy (theta_g)
# =============================================================================

suppressPackageStartupMessages({
  library(ggplot2)
  library(dplyr)
  library(tibble)
  library(patchwork)
})

cat("Generating Restaurant Analogy Figure for Latent Adequacy...\n")

dir.create("artifacts/figures", recursive = TRUE, showWarnings = FALSE)

theme_analogy <- function() {
  theme_minimal(base_size = 12) +
    theme(
      plot.title = element_text(face = "bold", size = 13, color = "#1a365d", hjust = 0.5),
      plot.subtitle = element_text(size = 10, color = "#4a5568", hjust = 0.5),
      axis.title = element_text(face = "bold", size = 11, color = "#2d3748"),
      legend.position = "bottom",
      legend.title = element_blank(),
      panel.grid.minor = element_blank(),
      panel.grid.major = element_line(color = "#edf2f7"),
      plot.background = element_rect(fill = "#f7fafc", color = "#cbd5e0")
    )
}

# Data for Analogy Comparison
analogy_data <- tibble(
  Concept = factor(c("1. Directory Listing", "2. Open Practice Filter", "3. Access Delay / Wait", "4. Public Coverage", "5. Underlying Capacity"),
                   levels = c("5. Underlying Capacity", "4. Public Coverage", "3. Access Delay / Wait", "2. Open Practice Filter", "1. Directory Listing")),
  `Restaurant Analogy` = c("Calling Google Map Numbers (52% reach)", "Seated at Open Table (95% seated)", "Table Wait Time (15m vs 3h)", "Discount Voucher Acceptance", "Hidden Kitchen Throughput"),
  `URPS Healthcare Reality` = c("Mystery-Caller Directory (52.3% appt)", "Eligible Practice Rate (95.3% appt)", "Appointment Wait Days (15d vs 55d)", "Medicaid Acceptance Rate (42.1%)", "Latent Regional Adequacy (θ_g)"),
  Value_High = c(85, 98, 15, 80, 85),
  Value_Low = c(25, 60, 55, 20, 20)
)

df_long <- analogy_data %>%
  tidyr::pivot_longer(cols = c(Value_High, Value_Low), names_to = "Access_Level", values_to = "Score") %>%
  mutate(
    Access_Label = ifelse(Access_Level == "Value_High", "High-Access Region (θ_g = 85%)", "Low-Access Region (θ_g = 20%)")
  )

p_analogy <- ggplot(df_long, aes(x = Concept, y = Score, fill = Access_Label)) +
  geom_col(position = position_dodge(0.7), width = 0.65) +
  geom_text(aes(label = sprintf("%.0f%%", Score)), position = position_dodge(0.7), hjust = -0.2, size = 3.5, fontface = "bold") +
  scale_fill_manual(values = c("#2b6cb0", "#e53e3e")) +
  scale_y_continuous(limits = c(0, 110), labels = function(x) paste0(x, "%")) +
  coord_flip() +
  labs(
    title = "The Restaurant Reservation Analogy vs. Healthcare Latent Adequacy (θ_g)",
    subtitle = "Comparing High-Access vs. Low-Access Regions across all four empirical access clues",
    x = "Access Clue / Metric",
    y = "Performance Score / Benchmark (%)"
  ) +
  theme_analogy()

ggsave("artifacts/figures/fig_analogy_restaurant_vs_healthcare.png", p_analogy, width = 9.5, height = 5.5, dpi = 300)

cat("Restaurant Analogy Figure successfully generated!\n")
