#!/usr/bin/env Rscript
# Probabilistic sensitivity analysis of the workforce 2050 FTE gap.
#
# Runs the full supply x demand model over a joint Latin-Hypercube sample of the
# uncertain inputs and decomposes which ones drive the spread (PRCC tornado).
#
#   Rscript scripts/run_psa_example.R
#
# The workforce evaluator needs the mufflyaccess contract; without it, this
# demonstrates the engine on a synthetic gap model so the output shape is visible.

suppressPackageStartupMessages({ library(dplyr); library(tibble) })
if (!requireNamespace("urpssim", quietly = TRUE)) pkgload::load_all(".", quiet = TRUE)

if (requireNamespace("mufflyaccess", quietly = TRUE)) {
  message("Running PSA on the workforce model (mufflyaccess available)...")
  psa <- psa_workforce_gap(n = 200, n_iterations = 60, verbose = TRUE)
  output <- "gap_pct"
} else {
  message("mufflyaccess not installed - demonstrating the engine on a synthetic gap model.")
  inputs <- list(
    psa_uniform("baseline_entrants", 45, 90),
    psa_discrete("retirement_source", c("hwsm", "urps_empirical")),
    psa_uniform("base_adequacy", 0.85, 1.02),
    psa_discrete("population_series", c("mid", "low", "hi"))
  )
  evaluate <- function(p) {
    dem <- switch(p$population_series, low = -2, mid = 0, hi = 2)
    ret <- if (p$retirement_source == "urps_empirical") -1.5 else 0
    -(1 - p$base_adequacy) * 100 - 0.15 * (p$baseline_entrants - 67) + dem - ret
  }
  psa <- run_psa(inputs, evaluate, n = 400, seed = 20260801L, verbose = FALSE)
  output <- "output"
}

cat("\n===== 2050 GAP DISTRIBUTION =====\n")
g <- psa$draws[[output]]
cat(sprintf("median %.1f  |  95%% interval [%.1f, %.1f]  |  P(deficit) = %.0f%%\n",
            median(g, na.rm = TRUE), quantile(g, .025, na.rm = TRUE),
            quantile(g, .975, na.rm = TRUE), 100 * mean(g < 0, na.rm = TRUE)))

cat("\n===== TORNADO: which inputs drive the spread (PRCC) =====\n")
tor <- psa_tornado(psa, output = output, method = "prcc")
print(tor)

cat("\n===== VARIANCE SHARES (SRRC) =====\n")
sv <- psa_srrc(psa, output = output)
print(sv$coefficients)
cat(sprintf("model R^2 (rank) = %.2f\n", sv$model_r2))

if (requireNamespace("ggplot2", quietly = TRUE)) {
  dir.create("outputs", showWarnings = FALSE)
  ggplot2::ggsave("outputs/psa_tornado.png", plot_psa_tornado(tor),
                  width = 7, height = 4, dpi = 150, bg = "white")
  cat("\nTornado figure written to outputs/psa_tornado.png\n")
}
