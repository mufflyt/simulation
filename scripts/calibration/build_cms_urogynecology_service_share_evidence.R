#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  if (!requireNamespace("urpssim", quietly = TRUE)) {
    pkgload::load_all(".", quiet = TRUE)
  } else {
    library(urpssim)
  }
})

frozen_roster_sha256 <- paste0(
  "fbdd8332a8de6f4870b65c83cefccfec",
  "3990ccca912d53165c3333c09934132c"
)

provider_path <- Sys.getenv(
  "URPS_CMS_PROVIDER_SERVICE",
  file.path(
    "data-raw", "cms_psps",
    "PHY_R26_P05_V10_D24_Prov_Svc.csv"
  )
)
geography_path <- Sys.getenv(
  "URPS_CMS_GEOGRAPHY_SERVICE",
  file.path(
    "data-raw", "cms_psps",
    "MUP_PHY_R26_P05_V10_D24_Geo.csv"
  )
)
roster_path <- Sys.getenv(
  "URPS_LINKAGE_ROSTER_2024",
  file.path(
    "data-raw", "urps_roster",
    "urps_linkage_roster_2024.csv"
  )
)
provider_type_path <- Sys.getenv(
  "URPS_CMS_PROVIDER_TYPE_MAP",
  file.path(
    "scripts", "validation", "mappings",
    "cms_provider_type_class.csv"
  )
)
output_dir <- Sys.getenv(
  "URPS_SERVICE_SHARE_OUTPUT_DIR",
  file.path("artifacts", "service_shares")
)

inputs <- base::c(
  provider_service = provider_path,
  geography_service = geography_path,
  linkage_roster = roster_path,
  provider_type_map = provider_type_path
)
missing_inputs <- inputs[!base::file.exists(inputs)]
if (base::length(missing_inputs) > 0L) {
  base::stop(
    "Missing CMS service-share input(s):\n  ",
    base::paste(missing_inputs, collapse = "\n  "),
    call. = FALSE
  )
}

observed_roster_sha <- digest::digest(
  file = roster_path,
  algo = "sha256"
)
if (!base::identical(observed_roster_sha, frozen_roster_sha256)) {
  base::stop(
    "Frozen 2024 linkage roster SHA-256 mismatch.\nExpected: ",
    frozen_roster_sha256,
    "\nObserved: ", observed_roster_sha,
    call. = FALSE
  )
}

registry <- urogynecology_service_share_registry()
hcpcs_keep <- registry$hcpcs
base::message(
  "Reading CMS Provider and Service evidence for ",
  scales::comma(base::length(hcpcs_keep)),
  " frozen HCPCS codes."
)
provider_service <- readr::read_csv(
  provider_path,
  col_select = dplyr::all_of(base::c(
    "Rndrng_NPI", "Rndrng_Prvdr_Type", "HCPCS_Cd", "Tot_Srvcs"
  )),
  show_col_types = FALSE,
  progress = interactive()
) |>
  dplyr::filter(.data$HCPCS_Cd %in% hcpcs_keep)

base::message("Reading CMS national Geography denominators.")
geography_service <- readr::read_csv(
  geography_path,
  col_select = dplyr::all_of(base::c(
    "Rndrng_Prvdr_Geo_Lvl", "HCPCS_Cd", "Tot_Srvcs"
  )),
  show_col_types = FALSE,
  progress = interactive()
) |>
  dplyr::filter(
    .data$Rndrng_Prvdr_Geo_Lvl == "National",
    .data$HCPCS_Cd %in% hcpcs_keep
  )

roster <- readr::read_csv(
  roster_path,
  show_col_types = FALSE,
  progress = FALSE
)
if (!"npi" %in% base::names(roster)) {
  npi_candidate <- base::intersect(
    base::names(roster),
    base::c("NPI", "rendering_npi", "Rndrng_NPI")
  )
  if (base::length(npi_candidate) != 1L) {
    base::stop("Linkage roster must contain an `npi` column.", call. = FALSE)
  }
  roster <- roster |>
    dplyr::rename(npi = dplyr::all_of(npi_candidate[[1L]]))
}

provider_type_map <- readr::read_csv(
  provider_type_path,
  comment = "#",
  show_col_types = FALSE,
  progress = FALSE
)

evidence <- build_cms_service_share_evidence(
  provider_service = provider_service,
  geography_service = geography_service,
  roster = roster,
  provider_type_map = provider_type_map,
  service_registry = registry,
  workload = urps_service_workload()
)

evidence$provenance$file_sha256 <- stats::setNames(
  base::vapply(
    inputs,
    function(path) digest::digest(file = path, algo = "sha256"),
    FUN.VALUE = base::character(1)
  ),
  base::names(inputs)
)
evidence$provenance$prespec <-
  "docs/PRESPEC_URPS_SHARE.md @ faf72dc"
evidence$provenance$created_at <- base::format(
  base::Sys.time(),
  "%Y-%m-%dT%H:%M:%S%z"
)

base::dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
bundle_path <- base::file.path(
  output_dir,
  base::paste0("cms_service_share_evidence_", timestamp, ".rds")
)
service_path <- base::file.path(
  output_dir,
  base::paste0("cms_service_share_bounds_", timestamp, ".csv")
)
aggregate_path <- base::file.path(
  output_dir,
  base::paste0("cms_service_share_aggregate_", timestamp, ".csv")
)

base::saveRDS(evidence, bundle_path)
readr::write_csv(evidence$service_bounds, service_path)
readr::write_csv(evidence$aggregate_bounds, aggregate_path)

base::message("Saved CMS evidence bundle: ", base::normalizePath(bundle_path))
base::message("Saved service bounds: ", base::normalizePath(service_path))
base::message("Saved aggregate bounds: ", base::normalizePath(aggregate_path))
