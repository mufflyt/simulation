# Fielded Lizeth URPS access anchor -- end-to-end runner.
#
# Lizeth is a national mystery-caller study of access to URPS care. This script
# reads the most recent labeled REDCap export, constructs call-level access
# outcomes, and reports the empirical base-year ACCESS anchor
# (appointment obtainment, wait-time distribution, insurance and scenario
# strata).
#
# IMPORTANT: this measures realized access, not latent productive capacity. It
# does NOT invert wait time into an adequacy ratio and does NOT flip
# capacity_status()$resolved to TRUE. See ?build_lizeth_access_anchor and
# ?capacity_status_with_lizeth.
#
# Requires the Lizeth repository checked out alongside this one (default
# ../lizeth). Run from the package root:
#   Rscript scripts/calibrate_lizeth_access_anchor.R

pkgload::load_all(".", quiet = TRUE)

lizeth_calibration <- build_lizeth_access_anchor(
  lizeth_dir = "../lizeth"
)

print(lizeth_calibration$anchor$overall)
print(lizeth_calibration$anchor$by_insurance)
print(lizeth_calibration$anchor$by_scenario)
print(lizeth_calibration$evidence)
print(lizeth_calibration$capacity_status)

base::cat(
  lizeth_calibration$anchor$summary_sentence,
  "\n"
)
