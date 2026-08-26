# scripts/calibration/fit_chia_medicare_bridge.R ----------------------------
#
# Fit the CHIA all-payer <-> Medicare FFS workload bridge and project national
# all-payer provider-year workload from Medicare-observed URPS. Assumes the
# package is loaded (library(urpssim) or devtools::load_all()).
#
# It reads two external sources that are NOT vendored (and cannot run in CI):
# point it at the real files via URPS_CHIA_PATH / URPS_MEDICARE_PATH, and
# optionally a canonical URPS roster via URPS_ROSTER_PATH to restrict both
# sources to board-certified URPS NPIs before fitting.

base::message("fit_chia_medicare_bridge.R: starting.")

timestamp <- base::format(Sys.time(), "%Y%m%d_%H%M%S")
artifact_dir <- file.path("artifacts", "calibration")
dir.create(artifact_dir, recursive = TRUE, showWarnings = FALSE)

chia_path <- Sys.getenv("URPS_CHIA_PATH", unset = "data-raw/chia/cadish.parquet")
medicare_path <- Sys.getenv(
  "URPS_MEDICARE_PATH",
  unset = "../alternative_payments/Data/medicare_ffs.parquet"
)
roster_path <- Sys.getenv("URPS_ROSTER_PATH", unset = "")

base::message("CHIA source: ", chia_path)
base::message("Medicare source: ", medicare_path)

chia_claims_tbl <- read_claims_source(chia_path)
medicare_claims_tbl <- read_claims_source(medicare_path)

base::message("CHIA columns: ",
              base::paste(base::names(chia_claims_tbl), collapse = ", "))
base::message("Medicare columns: ",
              base::paste(base::names(medicare_claims_tbl), collapse = ", "))

# Optional: restrict both sources to canonical URPS NPIs. The cleanest design --
# same physicians, same years, MA all-payer numerator vs Medicare denominator.
if (nzchar(roster_path) && file.exists(roster_path)) {
  urps_roster <- readr::read_csv(roster_path, show_col_types = FALSE)
  chia_claims_tbl <- filter_claims_to_urps(chia_claims_tbl, urps_roster)
  medicare_claims_tbl <- filter_claims_to_urps(medicare_claims_tbl, urps_roster)
}

chia_provider_year_tbl <- build_chia_provider_year(chia_tbl = chia_claims_tbl)
medicare_provider_year_tbl <- build_medicare_provider_year(
  medicare_tbl = medicare_claims_tbl
)

ma_overlap_tbl <- join_chia_medicare_overlap(
  chia_provider_year = chia_provider_year_tbl,
  medicare_provider_year = medicare_provider_year_tbl
)

bridge_fit <- fit_chia_medicare_bridge(ma_overlap_tbl)
print(summary(bridge_fit$model))
print(bridge_fit$ratio_summary)

national_allpayer_tbl <- predict_allpayer_from_medicare(
  bridge_fit = bridge_fit,
  medicare_provider_year = medicare_provider_year_tbl
)

age_curve_tbl <- estimate_workload_age_curve(national_allpayer_tbl)
print(age_curve_tbl)

overlap_path <- file.path(artifact_dir, paste0("chia_medicare_overlap_", timestamp, ".csv"))
ratio_path <- file.path(artifact_dir, paste0("chia_medicare_ratio_summary_", timestamp, ".csv"))
national_path <- file.path(artifact_dir, paste0("national_allpayer_provider_year_", timestamp, ".csv"))
age_curve_path <- file.path(artifact_dir, paste0("urps_empirical_age_workload_curve_", timestamp, ".csv"))

readr::write_csv(ma_overlap_tbl, overlap_path)
readr::write_csv(bridge_fit$ratio_summary, ratio_path)
readr::write_csv(national_allpayer_tbl, national_path)
readr::write_csv(age_curve_tbl, age_curve_path)

base::message("Saved overlap: ", normalizePath(overlap_path, mustWork = FALSE))
base::message("Saved ratio summary: ", normalizePath(ratio_path, mustWork = FALSE))
base::message("Saved national provider-years: ", normalizePath(national_path, mustWork = FALSE))
base::message("Saved age curve: ", normalizePath(age_curve_path, mustWork = FALSE))

base::message(
  paste(
    "fit_chia_medicare_bridge.R: this is delivered-workload calibration, not an",
    "adequacy estimate -- claims do not observe patients who never entered care."
  )
)
base::message("fit_chia_medicare_bridge.R: complete.")
