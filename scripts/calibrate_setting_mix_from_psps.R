#!/usr/bin/env Rscript
# calibrate_setting_mix_from_psps.R
#
# Replaces the illustrative URPS_DEFAULT_SETTING_MIX with place-of-service
# shares derived from the CMS Medicare Physician & Other Practitioners PUF
# (Geography & Service or Provider & Service level).
#
# Usage:
#   Rscript scripts/calibrate_setting_mix_from_psps.R
#
# Or from the R console:
#   source("scripts/calibrate_setting_mix_from_psps.R")
#
# After running, review the printed mix, then restart R / reload the package.

pkgload::load_all(quiet = TRUE)

# ---- Locate the PSPS file ----------------------------------------------------

psps_dir  <- "data-raw/cms_psps"
geo_file  <- file.path(psps_dir, "MUP_PHY_R26_P05_V10_D24_Geo.csv")
prov_file <- file.path(psps_dir, "PHY_R26_P05_V10_D24_Prov_Svc.csv")

if (file.exists(geo_file)) {
  psps_path <- geo_file
  file_type <- "geo_svc"
  message("Using Geography & Service file: ", psps_path)
} else if (file.exists(prov_file)) {
  psps_path <- prov_file
  file_type <- "prov_svc"
  message("Using Provider & Service file: ", psps_path,
          "\n(large file — may take 1-2 min to read)")
} else {
  stop(
    "No PSPS file found in ", psps_dir, "/\n",
    "Expected one of:\n  ", geo_file, "\n  ", prov_file, "\n",
    "See data-raw/cms_psps/DOWNLOAD.md for download instructions.",
    call. = FALSE
  )
}

# ---- Compute shares ----------------------------------------------------------

message("Computing place-of-service shares from URPS CPT basket ...")
shares <- load_psps_pos_shares(psps_path, file_type = file_type)

message("\nDerived URPS_DEFAULT_SETTING_MIX (CMS PSPS 2024):\n")
print(shares, n = Inf)

# ---- Sanity checks -----------------------------------------------------------

sums <- tapply(shares$share, shares$service, sum)
bad  <- names(sums[abs(sums - 1) > 1e-6])
if (length(bad)) {
  warning("Shares do not sum to 1 for: ", paste(bad, collapse = ", "),
          "\nCheck CPT basket coverage before pasting into supply-urps_settings.R.")
} else {
  message("\nAll service shares sum to 1. Ready to paste into R/supply-urps_settings.R.")
}

# ---- Emit copy-paste block ---------------------------------------------------

message("\n\n# ---- Paste this block into R/supply-urps_settings.R -----------------------\n")
message("URPS_DEFAULT_SETTING_MIX <- tibble::tribble(")
message("  ~service, ~setting, ~share,")
for (i in seq_len(nrow(shares))) {
  r <- shares[i, ]
  message(sprintf('  %-28s %-25s %.4f%s',
    paste0('"', r$service, '",'),
    paste0('"', r$setting, '",'),
    r$share,
    if (i < nrow(shares)) "" else ""))
}
message(")")
message("\nURPS_DEFAULT_SETTING_MIX_STATUS <- \"calibrated_psps_2024\"")
message("URPS_DEFAULT_SETTING_MIX_SOURCE <- \"CMS MUP_PHY 2024 place-of-service shares\"")

# ---- Write shares to file for audit ------------------------------------------

out_path <- file.path(psps_dir, "psps_2024_setting_shares.csv")
utils::write.csv(shares, out_path, row.names = FALSE)
message("\nAudit CSV written to: ", out_path)
