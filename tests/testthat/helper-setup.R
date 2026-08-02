# Source the microsimulation modules so tests can call them without a package.
# testthat auto-sources helper-*.R before running tests.

# The modules use the magrittr pipe (%>%) and dplyr/tidyr verbs, so the
# tidyverse stack must be attached before the module functions run.
suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(purrr)
  library(tibble)
})

.mods <- c(
  "10-repro_provenance.R", "11-canonical_and_joins.R",
  "12-provider_microsimulation.R", "13-demand_urps.R",
  "14-spatial_access_e2sfca.R", "15-run_workforce_microsimulation.R"
)
.r_dir <- if (dir.exists("R")) "R" else file.path("..", "..", "R")
for (.m in .mods) {
  .p <- file.path(.r_dir, .m)
  if (file.exists(.p)) source(.p)
}
