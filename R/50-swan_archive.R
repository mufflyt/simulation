# SWAN archive loader -------------------------------------------------------
#
# The SWAN files are 2.6 GB sitting outside the repository, on a removable drive,
# under a path that resolves through SIMULATION_DATA_ROOT. Three things go wrong
# with an input like that, and none of them announce themselves:
#
#  * the drive is not mounted, so the path resolves to somewhere that does not
#    exist and the failure surfaces deep inside a reshape;
#  * a second copy of the archive exists somewhere and is silently a different
#    vintage -- SWAN files were regenerated repeatedly (merged_longitudinal_data,
#    raw_merged_longitudinal_data, comprehensive_swan_dataset_corrected,
#    comprehensive_swan_dataset_final_working all coexist in the same folder);
#  * a fit gets published and nobody can say afterwards which bytes produced it.
#
# So reading the archive goes through one gate that resolves the path, hashes the
# file, checks the hash against a shipped reference, and staples the result to
# the returned object. The provenance then rides along into the DMDM panel and
# out through swan_panel_fit_caveats(), so a fitted transition set can be traced
# back to a specific file on a specific disk.
#
# Hashing cost is proportional to file size: swan_all_visits.rds (28 MB) hashes
# in about 0.4 s, so verification is on by default. merge_all_swan_rda_files_output.rds
# is 1.8 GB and takes roughly a minute; pass verify = FALSE when that is not worth
# it, and accept that the returned provenance then records `verified = FALSE`.

#' Reference checksums for the validated SWAN archive files
#'
#' SHA-256 of the archive as extracted from the Dropbox export on 2026-08-03 and
#' used for the fitted UI transitions. A file that hashes differently is a
#' different vintage, whatever it is named.
#'
#' @format Named character vector; names are bare file names.
#' @export
SWAN_ARCHIVE_SHA256 <- c(
  swan_all_visits.rds = "9ca085dd516de07a9bd0ab009760cdb36f151137a40be726dd6d7c1bdd209405"
)

#' Expected sizes in bytes, as a cheap pre-check before hashing
#'
#' @format Named numeric vector; names are bare file names.
#' @export
SWAN_ARCHIVE_BYTES <- c(
  swan_all_visits.rds = 29537163
)

#' Load a SWAN archive file with verified provenance
#'
#' Resolves the file beneath the external data root (or an explicitly approved
#' local copy), hashes it, checks the hash against [SWAN_ARCHIVE_SHA256], reads
#' it, and attaches a `swan_archive_provenance` attribute.
#'
#' A checksum mismatch means the bytes are not the ones the shipped reference was
#' taken from. That is treated the way every other provenance failure in this
#' package is treated: strict mode stops, relaxed mode warns loudly and proceeds,
#' so an exploratory session is not blocked but cannot mistake the file for the
#' validated one.
#'
#' @param file Bare file name beneath the SWAN directory. Defaults to
#'   `"swan_all_visits.rds"`, the only file carrying every covariate the DMDM
#'   panel needs.
#' @param path Full path to an approved local copy, bypassing [swan_path()].
#'   Recorded in the provenance so a run off a non-canonical copy is visible.
#' @param verify Hash the file and compare against the reference. `TRUE` by
#'   default; set `FALSE` only for the multi-gigabyte files where the cost is not
#'   worth it, which records `verified = FALSE`.
#' @param expected_sha256 Override the shipped reference, for a file this package
#'   does not ship a checksum for.
#' @param mode Reproducibility mode; strict errors on a mismatch.
#' @param verbose Narrate resolution, size, hash and verification.
#' @return The object read from the `.rds`, carrying a `swan_archive_provenance`
#'   attribute (`path`, `file`, `bytes`, `modified`, `sha256`, `verified`,
#'   `reference_source`).
#' @seealso [swan_archive_provenance()], [build_swan_dmdm_panel()],
#'   [swan_dmdm_panel_from_archive()]
#' @export
load_swan_archive <- function(file = "swan_all_visits.rds",
                              path = NULL,
                              verify = TRUE,
                              expected_sha256 = NULL,
                              mode = resolve_reproducibility_mode(),
                              verbose = TRUE) {
  resolved <- if (!is.null(path)) path else swan_path(file)
  from_override <- !is.null(path)

  if (!file.exists(resolved)) {
    # Distinguish "drive not plugged in" from "file genuinely absent": they need
    # different fixes and the generic not-found message sends people to edit
    # config when the real answer is to attach a disk.
    root <- simulation_data_root()
    root_missing <- !dir.exists(root)
    stop(sprintf(paste0(
      "SWAN archive not found: %s\n",
      if (root_missing)
        "  The external data root itself does not exist (%s).\n  If it is a removable drive, mount it; otherwise set SIMULATION_DATA_ROOT.\n"
      else
        "  The data root exists (%s) but this file is not under it.\n  Check the file name, or point SIMULATION_DATA_ROOT at the archive.\n"),
      resolved, root), call. = FALSE)
  }

  info <- file.info(resolved)
  reference <- if (!is.null(expected_sha256)) expected_sha256 else
    unname(SWAN_ARCHIVE_SHA256[basename(resolved)])
  reference_source <- if (!is.null(expected_sha256)) "caller" else
    if (!is.na(reference) && length(reference)) "SWAN_ARCHIVE_SHA256" else "none"

  if (isTRUE(verbose)) {
    message("load_swan_archive(): ", resolved)
    message("  ", format(info$size, big.mark = ","), " bytes, modified ", format(info$mtime))
    if (from_override) message("  NOTE: approved local copy, not the resolved swan_path()")
  }

  # Size is a free pre-check: a differing size guarantees a differing hash, and
  # saying so before a minute of hashing is more useful than saying it after.
  expected_bytes <- unname(SWAN_ARCHIVE_BYTES[basename(resolved)])
  if (length(expected_bytes) && !is.na(expected_bytes) && info$size != expected_bytes) {
    msg <- sprintf(paste("SWAN archive %s is %s bytes; the validated archive is %s.",
                         "This is a different file."),
                   basename(resolved), format(info$size, big.mark = ","),
                   format(expected_bytes, big.mark = ","))
    if (identical(mode, "strict")) stop(msg, call. = FALSE)
    .msg_warn(msg)
  }

  observed <- NA_character_
  verified <- FALSE
  if (isTRUE(verify)) {
    observed <- digest::digest(file = resolved, algo = "sha256")
    if (identical(reference_source, "none") || is.na(reference)) {
      .msg_warn(sprintf(paste(
        "No reference checksum for '%s': recorded its SHA-256 (%s) but could not",
        "verify it. Add it to SWAN_ARCHIVE_SHA256 once the file is validated."),
        basename(resolved), substr(observed, 1, 12)))
    } else if (!identical(observed, reference)) {
      msg <- sprintf(paste(
        "SWAN archive checksum mismatch for %s.\n  expected %s\n  observed %s\n",
        " These are different bytes than the validated archive; any fit from them",
        "is not the fit the reference describes."),
        basename(resolved), reference, observed)
      if (identical(mode, "strict")) stop(msg, call. = FALSE)
      .msg_warn(msg)
    } else {
      verified <- TRUE
      if (isTRUE(verbose)) message("  sha256 verified against ", reference_source)
    }
  } else if (isTRUE(verbose)) {
    message("  verify = FALSE: provenance will record verified = FALSE")
  }

  obj <- readRDS(resolved)
  attr(obj, "swan_archive_provenance") <- list(
    path = resolved,
    file = basename(resolved),
    approved_local_copy = from_override,
    bytes = as.numeric(info$size),
    modified = info$mtime,
    sha256 = observed,
    verified = verified,
    reference_source = reference_source,
    n_rows = if (is.data.frame(obj)) nrow(obj) else NA_integer_,
    n_cols = if (is.data.frame(obj)) ncol(obj) else NA_integer_
  )
  if (isTRUE(verbose) && is.data.frame(obj)) {
    message("  read ", format(nrow(obj), big.mark = ","), " x ",
            format(ncol(obj), big.mark = ","))
  }
  obj
}

#' Provenance recorded by [load_swan_archive()]
#'
#' @param x An object returned by [load_swan_archive()], or a panel built from
#'   one by [build_swan_dmdm_panel()].
#' @return The provenance list, or `NULL` when the object did not come through
#'   the archive loader.
#' @export
swan_archive_provenance <- function(x) {
  p <- attr(x, "swan_archive_provenance")
  if (!is.null(p)) return(p)
  # A panel carries the archive provenance nested inside its own.
  panel_prov <- attr(x, "swan_dmdm_provenance")
  if (!is.null(panel_prov)) return(panel_prov$archive)
  NULL
}

#' Load the SWAN archive and build the DMDM panel in one step
#'
#' Convenience wrapper over [load_swan_archive()] and [build_swan_dmdm_panel()]
#' that keeps the archive provenance attached to the panel, so
#' [swan_panel_fit_caveats()] can name the exact file a fit came from.
#'
#' @param file,path,verify,expected_sha256 Passed to [load_swan_archive()].
#' @param ... Passed to [build_swan_dmdm_panel()] (`visits`, `conditions`, ...).
#' @param verbose Narrate both steps.
#' @return The panel from [build_swan_dmdm_panel()], whose
#'   `swan_dmdm_provenance` attribute carries an `archive` element.
#' @export
swan_dmdm_panel_from_archive <- function(file = "swan_all_visits.rds",
                                         path = NULL,
                                         verify = TRUE,
                                         expected_sha256 = NULL,
                                         ...,
                                         verbose = TRUE) {
  archive <- load_swan_archive(file = file, path = path, verify = verify,
                               expected_sha256 = expected_sha256, verbose = verbose)
  build_swan_dmdm_panel(archive, verbose = verbose, ...)
}
