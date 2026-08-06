# The mufflyaccess Contract Pin ----
#
# A VERSION NUMBER IS NOT AN IDENTITY, and this repository has the receipts.
# Two materially different mufflyaccess builds both reported 0.10.0 during a
# single working session: one exported 56 functions and lacked
# `urps_retirement_status()`, the other exported 98 and provided it. The
# difference was not cosmetic -- it changed how `n_retired` is served (integer
# zeros vs NA), which is the field the back-test's attrition guard reads.
#
# With `Remotes: mufflyt/mufflyaccess` unpinned, a fresh install resolves HEAD,
# so two machines can install "0.10.0" months apart and disagree about what the
# contract says. The version-based check that would normally catch this cannot:
# both builds satisfy `packageVersion("mufflyaccess") >= "0.10.0"`.
#
# DESCRIPTION now pins the commit. This module makes the pin CHECKABLE at run
# time and, more importantly, checks CAPABILITY rather than version -- because
# the pin only constrains a fresh install, and nothing stops an older build
# already sitting in a library from being loaded.

#' Commit the contract is pinned to in DESCRIPTION
#' @export
MUFFLYACCESS_PINNED_SHA <- "b1f33acf918d71031ed7cbed79cb76d525503d4c"

#' Contract functions this package calls
#'
#' Derived by grepping `mufflyaccess::` across R/ and scripts/, then frozen
#' here. An installed build missing any of these is not merely older -- it
#' cannot run this package, whatever version number it reports.
#'
#' `mc_weighted_ci` and `urps_projection_schema` are deliberately ABSENT: both
#' appear only in roxygen prose, never in a call. A frozen list padded with
#' documentation mentions would demand capabilities the package does not use,
#' and reject a build that could run it perfectly well.
#'
#' @export
MUFFLYACCESS_REQUIRED_EXPORTS <- c(
  "calculate_replacement_gap",
  "get_canonical_bands", "get_primary_access_band",
  "pfd_prevalence", "rurality_from_ruca", "safe_divide", "safe_percent",
  "urps_count", "urps_counts_long", "urps_entry_counts", "urps_lineage",
  "URPS_PROJECTION_CONTRACT_VERSION",
  "urps_provenance", "urps_retired_values", "urps_retirement_status",
  "urps_scenario_ids", "URPS_SCENARIO_REGISTRY_VERSION", "urps_scenarios",
  "validate_urps_projection"
)

#' Identity of the installed contract build
#'
#' @return List with `version`, `sha`, `sha_matches_pin`, `n_exports`,
#'   `missing_exports`, `usable`.
#' @export
mufflyaccess_build <- function() {
  if (!requireNamespace("mufflyaccess", quietly = TRUE)) {
    return(list(installed = FALSE, usable = FALSE,
                detail = "mufflyaccess is not installed"))
  }
  d <- utils::packageDescription("mufflyaccess")
  sha <- d$RemoteSha %||% NA_character_
  ex <- getNamespaceExports("mufflyaccess")
  missing <- setdiff(MUFFLYACCESS_REQUIRED_EXPORTS, ex)

  list(
    installed = TRUE,
    version = d$Version %||% NA_character_,
    sha = sha,
    # NA rather than FALSE when the build carries no RemoteSha: a local or
    # CRAN-style install is unidentified, not mismatched.
    sha_matches_pin = if (is.na(sha)) NA else identical(sha, MUFFLYACCESS_PINNED_SHA),
    remote_ref = d$RemoteRef %||% NA_character_,
    n_exports = length(ex),
    missing_exports = missing,
    usable = length(missing) == 0L
  )
}

#' Refuse to run against a contract build that cannot support this package
#'
#' Checks CAPABILITY first and identity second. A build missing a required
#' export fails regardless of what version it claims; a build whose commit
#' differs from the pin warns, because it may be newer rather than broken.
#'
#' @param mode Reproducibility mode; strict errors on a capability failure.
#' @return (Invisibly) TRUE when the build can support this package.
#' @export
assert_mufflyaccess_contract <- function(mode = resolve_reproducibility_mode()) {
  b <- mufflyaccess_build()
  if (!isTRUE(b$installed)) {
    if (identical(mode, "strict")) stop(b$detail, call. = FALSE)
    .msg_warn(b$detail)
    return(invisible(FALSE))
  }

  if (!isTRUE(b$usable)) {
    msg <- sprintf(paste(
      "The installed mufflyaccess build reports version %s but does not export:",
      "%s. A version number is not an identity -- two builds have reported",
      "0.10.0 with 56 and 98 exports respectively. Install the pinned commit:",
      "remotes::install_github('mufflyt/mufflyaccess@%s')."),
      b$version, paste(b$missing_exports, collapse = ", "),
      MUFFLYACCESS_PINNED_SHA)
    if (identical(mode, "strict")) stop(msg, call. = FALSE)
    .msg_warn(msg)
    return(invisible(FALSE))
  }

  if (isFALSE(b$sha_matches_pin)) {
    # A WARNING, not an error. The installed build satisfies every capability
    # this package needs; it is simply not the commit DESCRIPTION pins, which
    # may mean it is newer. Failing here would block legitimate upgrades.
    .msg_info(sprintf(paste(
      "mufflyaccess build %s differs from the pinned commit %s, but exports",
      "everything this package requires. Update Remotes: in DESCRIPTION if the",
      "newer contract is intended."),
      substr(b$sha, 1, 12), substr(MUFFLYACCESS_PINNED_SHA, 1, 12)))
  }
  invisible(TRUE)
}
