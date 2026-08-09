#!/usr/bin/env Rscript
# Medicare FFS realized-care trajectory --------------------------------------
#
# Reads the CMS Provider-and-Service PUF directly from an external directory,
# selects only procedure-specific URPS HCPCS codes with DuckDB, writes a
# realized-care artifact, and plots annual national procedure trajectories.
#
# Required:
#   MEDICARE_PROVIDER_SERVICE_DIR=/path/to/Prov_Svc/files
# Optional:
#   MEDICARE_REALIZED_CARE_OUTPUT_DIR=/path/to/output  (default: artifacts/)
#
# The PUF has neither diagnoses nor beneficiary age. Generic E/M codes are
# deliberately excluded by urps_medicare_service_crosswalk(); this is a
# Medicare-FFS observed-use series, not latent all-payer demand.

suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
  library(dplyr)
  library(ggplot2)
})

root <- normalizePath(".")
if (!requireNamespace("urpssim", quietly = TRUE)) pkgload::load_all(root, quiet = TRUE)

input_dir <- Sys.getenv("MEDICARE_PROVIDER_SERVICE_DIR", unset = "")
if (!nzchar(input_dir) || !dir.exists(input_dir)) {
  stop("Set MEDICARE_PROVIDER_SERVICE_DIR to the directory containing *Prov_Svc.csv files.", call. = FALSE)
}
output_dir <- Sys.getenv("MEDICARE_REALIZED_CARE_OUTPUT_DIR", unset = file.path(root, "artifacts"))
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

files <- sort(list.files(input_dir, pattern = "Prov_Svc\\.csv$", full.names = TRUE))
if (!length(files)) stop("No Provider-and-Service CSV files found in ", input_dir, call. = FALSE)
years <- as.integer(sub(".*_D([0-9]{2})_Prov_Svc\\.csv$", "20\\1", basename(files)))
if (anyNA(years)) stop("Could not derive four-digit year from one or more file names", call. = FALSE)
requested_years <- Sys.getenv("MEDICARE_REALIZED_CARE_YEARS", unset = "")
if (nzchar(requested_years)) {
  keep_years <- as.integer(strsplit(requested_years, ",", fixed = TRUE)[[1]])
  keep <- years %in% keep_years
  files <- files[keep]; years <- years[keep]
  if (!length(files)) stop("No Provider-and-Service files match MEDICARE_REALIZED_CARE_YEARS", call. = FALSE)
}
prefix <- Sys.getenv("MEDICARE_REALIZED_CARE_PREFIX", unset = "medicare_realized_care_2013_2023")

crosswalk <- urps_medicare_service_crosswalk()
codes <- paste(sprintf("'%s'", crosswalk$hcpcs), collapse = ", ")
con <- dbConnect(duckdb(), dbdir = ":memory:")
on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

read_one_year <- function(path, year) {
  message("Scanning Medicare Provider-and-Service PUF for ", year, "...")
  quoted <- DBI::dbQuoteString(con, normalizePath(path))
  sql <- paste0(
    "SELECT Rndrng_NPI, Rndrng_Prvdr_State_Abrvtn, Rndrng_Prvdr_Type, ",
    "HCPCS_Cd, Place_Of_Srvc, Tot_Srvcs ",
    "FROM read_csv_auto(", quoted, ", header = true) ",
    "WHERE HCPCS_Cd IN (", codes, ")"
  )
  out <- dbGetQuery(con, sql)
  out$year <- year
  message("  retained ", format(nrow(out), big.mark = ","), " procedure lines")
  out
}

matched <- bind_rows(Map(read_one_year, files, years))
if (!nrow(matched)) stop("No procedure lines matched the URPS crosswalk", call. = FALSE)
realized <- aggregate_medicare_realized_care(matched, crosswalk = crosswalk)

artifact <- file.path(output_dir, paste0(prefix, ".rds"))
write_artifact_with_provenance(
  realized, artifact,
  inputs = list(files = basename(files), years = years, crosswalk = crosswalk),
  code_paths = c("R/46-medicare_capacity.R", "scripts/plot_medicare_realized_care.R"),
  source = "CMS Medicare Provider-and-Service PUF; procedure-specific URPS HCPCS only",
  extra = list(payer_scope = "Medicare fee-for-service", estimand = attr(realized, "caveat"))
)

national <- realized |>
  group_by(year, service) |>
  summarise(billed_services = sum(billed_services), .groups = "drop")

fig <- ggplot(national, aes(year, billed_services)) +
  geom_line(linewidth = .8, colour = "#007C91") +
  geom_point(size = 1.8, colour = "#007C91") +
  facet_wrap(~ service, scales = "free_y", ncol = 3, labeller = label_wrap_gen(18)) +
  scale_x_continuous(breaks = sort(unique(national$year))) +
  scale_y_continuous(labels = scales::label_number(big.mark = ",")) +
  labs(
    title = "Observed Medicare FFS procedure use in the URPS service basket",
    subtitle = "Procedure-specific HCPCS only; each panel has its own y-axis",
    x = NULL, y = "Billed services",
    caption = paste(
      "Realized Medicare FFS use, not latent all-payer demand.",
      "Provider-and-Service PUF suppresses low-volume lines and has no diagnosis or beneficiary-age field."
    )
  ) +
  theme_minimal(base_size = 12) +
  theme(panel.grid.minor = element_blank(), strip.text = element_text(face = "bold"),
        plot.title = element_text(face = "bold"), plot.caption = element_text(hjust = 0))

figure <- file.path(output_dir, paste0(prefix, ".png"))
ggsave(figure, fig, width = 11, height = 8, dpi = 300, bg = "white")
utils::write.csv(national, file.path(output_dir, paste0(prefix, "_national.csv")), row.names = FALSE)
message("Wrote ", artifact, " and ", figure)
