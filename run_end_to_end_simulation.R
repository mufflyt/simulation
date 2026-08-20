#!/usr/bin/env Rscript
# Standalone Master End-to-End Simulation Runner
# Runs the entire URPS workforce, spatial competition, survival, and workload simulation pipeline.

suppressMessages(pkgload::load_all("."))

res <- run_end_to_end_simulation(
  start_year = 2025L,
  end_year = 2035L,
  n_agents = 500L,
  save_outputs = TRUE
)

cat("\nSummary Sentence:\n", res$workload_decomposition$summary_sentence, "\n\n")
