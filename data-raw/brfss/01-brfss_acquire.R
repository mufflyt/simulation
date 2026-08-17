# BRFSS Data Acquisition ----
#
# Source: Behavioral Risk Factor Surveillance System (BRFSS), CDC/NCCDPHP
# Survey: Annual cross-sectional telephone survey of U.S. adults 18+
# Format: SAS Transport (.XPT) — read with haven::read_xpt()
# DUA:    None required (public use file)
#
# How the raw files were obtained:
#   2023: https://www.cdc.gov/brfss/annual_data/2023/files/LLCP2023XPT.zip
#   2024: https://www.cdc.gov/brfss/annual_data/2024/files/LLCP2024XPT.zip
#
#   Download the .zip, extract the .XPT inside, and place at:
#     data-raw/brfss/LLCP2023.XPT
#     data-raw/brfss/LLCP2024.XPT
#
# Survey design (for svydesign / svyglm):
#   ids     = ~_PSU
#   strata  = ~_STSTR
#   weights = ~_LLCPWT
#   nest    = TRUE
#
# Key variable changes 2023 → 2024:
#   _HLTHPL1 → _HLTHPL2  (have health coverage: 1=Yes, 2=No)
#   PRIMINS1  → PRIMINS2  (primary insurance type)
#   All other key variables (_AGEG5YR, _IMPRACE, _LLCPWT, _BMI5CAT,
#   _SMOKER3, INCOME3, SEXVAR, HADHYST2, DIABETE4, HAVARTH4) unchanged.
#
# This script produces:
#   data-raw/brfss/brfss_<year>_women18plus.rds
#   data-raw/brfss/brfss_<year>_manifest.txt

library(haven)
library(dplyr)

# ---- Configuration by year --------------------------------------------------

BRFSS_YEAR_CONFIG <- list(
  "2023" = list(
    xpt_path     = file.path("data-raw", "brfss", "LLCP2023.XPT"),
    zip_url      = "https://www.cdc.gov/brfss/annual_data/2023/files/LLCP2023XPT.zip",
    hlthpl_var   = "_HLTHPL1",
    primins_var  = "PRIMINS1"
  ),
  "2024" = list(
    xpt_path     = file.path("data-raw", "brfss", "LLCP2024.XPT"),
    zip_url      = "https://www.cdc.gov/brfss/annual_data/2024/files/LLCP2024XPT.zip",
    hlthpl_var   = "_HLTHPL2",
    primins_var  = "PRIMINS2"
  )
)

# ---- Process a single year --------------------------------------------------

process_brfss_year <- function(year) {
  cfg <- BRFSS_YEAR_CONFIG[[as.character(year)]]
  if (is.null(cfg)) stop("No config for BRFSS year: ", year)

  if (!file.exists(cfg$xpt_path)) {
    stop(
      "BRFSS ", year, " XPT not found at '", cfg$xpt_path, "'.\n",
      "Download and unzip from:\n  ", cfg$zip_url, "\n",
      "Place the extracted .XPT at: ", cfg$xpt_path
    )
  }

  cat("Reading BRFSS", year, "XPT (may take ~60 s)...\n")
  raw <- haven::read_xpt(cfg$xpt_path)
  cat("Raw rows:", nrow(raw), "| cols:", ncol(raw), "\n")

  # Filter to adult women 18+: SEXVAR 2 = Female; _AGEG5YR 1-14 covers 18+
  women <- raw |>
    dplyr::filter(
      .data$SEXVAR == 2,
      !is.na(.data[["_AGEG5YR"]]),
      .data[["_AGEG5YR"]] %in% 1:14
    )
  cat("Women 18+:", nrow(women), "\n")

  # Rename year-specific variables to stable canonical names
  women <- dplyr::rename(women,
    `_HLTHPL` = dplyr::all_of(cfg$hlthpl_var)
  )
  if (cfg$primins_var %in% names(women)) {
    women <- dplyr::rename(women, PRIMINS = dplyr::all_of(cfg$primins_var))
  }

  out_rds <- file.path("data-raw", "brfss",
                       paste0("brfss_", year, "_women18plus.rds"))
  saveRDS(women, out_rds)
  cat("Saved:", out_rds, "\n")

  # Write manifest
  manifest_lines <- c(
    paste("BRFSS", year, "Download Manifest"),
    paste("Generated:", Sys.time()),
    paste("Source URL:", cfg$zip_url),
    paste("XPT size (MB):", round(file.size(cfg$xpt_path) / 1e6, 1)),
    paste("RDS size (MB):", round(file.size(out_rds) / 1e6, 1)),
    paste("Raw record count:", nrow(raw)),
    paste("Women record count:", nrow(women)),
    paste("Columns:", ncol(women)),
    "",
    "Key variable notes:",
    paste("  Health coverage var:", cfg$hlthpl_var, "→ renamed to _HLTHPL"),
    paste("  Primary insurance var:", cfg$primins_var, "→ renamed to PRIMINS"),
    "",
    "Survey design: ids=~_PSU, strata=~_STSTR, weights=~_LLCPWT, nest=TRUE",
    "DUA required: NO"
  )
  manifest_path <- file.path("data-raw", "brfss",
                             paste0("brfss_", year, "_manifest.txt"))
  writeLines(manifest_lines, manifest_path)
  cat("Manifest:", manifest_path, "\n\n")

  invisible(women)
}

# ---- Run for target year(s) -------------------------------------------------
# To process 2024 data:
process_brfss_year(2024)

# To re-process 2023:
# process_brfss_year(2023)
