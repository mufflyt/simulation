# scripts/calibration/build_chia_cadr_capacity_evidence.R --------------------
#
# Assemble the empirical URPS capacity-evidence bundle from Massachusetts CHIA
# case-mix (linked to NPI via BORIM), CADR per-treated-patient intensity, and
# the fielded Lizeth/Rabice access anchor. Assumes the package is loaded
# (library(urpssim) or devtools::load_all()).
#
# Not vendored and cannot run in CI: it needs the CHIA case-mix file(s), the
# BORIM extract, the CADR workload artifact, and the Lizeth REDCAP export. Point
# it at them via MA_CHIA_CASEMIX (+ MA_BORIM_CSV), then run from the repo root.

base::message("build_chia_cadr_capacity_evidence.R: starting.")

chia_paths <- Sys.getenv("MA_CHIA_CASEMIX", unset = "")
chia_paths <- chia_paths[nzchar(chia_paths)]
if (length(chia_paths) == 0L) {
  base::stop(
    "Set MA_CHIA_CASEMIX to one CHIA case-mix CSV before running.",
    call. = FALSE
  )
}

# The existing Lizeth pipeline supplies the fielded access target.
lizeth_anchor <- NULL
lizeth_dir <- Sys.getenv("LIZETH_DIR", unset = "../lizeth")
if (dir.exists(lizeth_dir)) {
  lizeth_path <- find_lizeth_redcap(lizeth_dir)
  lizeth_raw <- readr::read_csv(lizeth_path, show_col_types = FALSE, progress = FALSE)
  lizeth_parsed <- parse_lizeth_physician_information(lizeth_raw)
  lizeth_calls <- prepare_lizeth_access(lizeth_parsed)
  lizeth_anchor <- estimate_lizeth_access_anchor(lizeth_calls)
}

capacity_evidence <- build_empirical_capacity_evidence(
  chia_paths = chia_paths,
  lizeth_anchor = lizeth_anchor
)

print(capacity_evidence$status)
print(capacity_evidence$chia_summary)
print(capacity_evidence$cadr_workload)

save_empirical_capacity_evidence(capacity_evidence)

# Before a fitted, validated access inverse is supplied, this MUST stay
# unresolved: workload evidence alone does not identify unmet need.
print(empirical_capacity_status(capacity_evidence))

# Once the inverse fit exists (e.g. the Lizeth/Rabice wait inverse), pass its
# validated estimate here rather than editing capacity_status() by hand:
#   access_fit <- list(adequacy = 0.82, lower = 0.74, upper = 0.91,
#                      method = "lizeth_catchment_wait_inverse_v1",
#                      validation_passed = TRUE)
#   print(empirical_capacity_status(capacity_evidence, access_fit))

base::message("build_chia_cadr_capacity_evidence.R: complete.")
