# Canonical Drive-Time Isochrone Import ----
#
# The isochrones are the last missing input of the geographic access layer.
# Everything else -- tract population, tract centroids, provider coordinates
# (98.9% of baseline), the demand and supply machinery, the validation gate --
# is already present or wired. They are expensive to generate, exist in
# mufflyt/isochrones, and are imported rather than recomputed.
#
# WHY THIS FILE IS A CONTRACT AND NOT A `file.copy()`. The source repository
# contains, side by side with the canonical artifacts:
#
#   * artifacts/quarantine/contaminated_prod_cache_20260607_111432/
#   * .../archived_unusable/ and .../isochrones_180min_consolidated_backup_may8_broken.rds
#   * a README describing a SUPERSEDED March merge with ~10x the locations
#   * a downstream run (native_seam_tracts, July 22-23) carrying a SUCCESS
#     marker whose own RUN_MANIFEST records 9 succeeded / 10 failed steps,
#     "Chain of Custody: INTACT (0 verified, 0 broken)", and an earlier FAILED
#     marker showing it resumed past missing receipts for steps 2.5, 3, 4 and 6
#     -- exactly the steps producing cohort, isochrones and access.
#
# Sorted by filename or by date, the wrong artifact wins. Sorted by recorded
# provenance, the right one does. So identity is asserted against the registry
# and the checksums, and nothing else is accepted.
#
# WHAT IS DELIBERATELY REFUSED. See ISOCHRONE_REFUSED_ARTIFACTS. Each entry is a
# thing a reasonable person might import, with the reason it is not imported.

#' Canonical isochrone run, as recorded in both registries
#'
#' The local `artifacts/isochrones/ISOCHRONE_REGISTRY.json` and the S3
#' `isochrone_registry/current.json` were read independently and agree on every
#' field below. Generated 2026-05-08, uploaded 2026-05-29; 2026-06-06 is the date
#' the run was declared active, NOT the artifact vintage.
#' @export
ISOCHRONE_CANONICAL_RUN_ID <- "20260508_ec2_r6i"

#' @rdname ISOCHRONE_CANONICAL_RUN_ID
#' @export
ISOCHRONE_CANONICAL_BANDS <- c(30L, 60L, 120L, 180L)

#' @rdname ISOCHRONE_CANONICAL_RUN_ID
#' @export
ISOCHRONE_CANONICAL_VALHALLA <- "3.7.0-680c8f2b7"

#' @rdname ISOCHRONE_CANONICAL_RUN_ID
#' @export
ISOCHRONE_CANONICAL_COHORT <- list(
  n_npis = 7616L,
  sha256 = "5f6fe125fba04806216f91e5b80f2a8708b64c4daf809462950682148ca59529",
  file = "step_2.5_final_cohort.rds"
)

# SHA-256 of each band file, transcribed from the source SHA256SUMS.txt and
# verified against the artifacts on 2026-08-08 (all four OK). These are the
# identity of the data; a file that does not match one of them is not the
# canonical artifact whatever it is called.
#' @rdname ISOCHRONE_CANONICAL_RUN_ID
#' @export
ISOCHRONE_CANONICAL_SHA256 <- c(
  "30"  = "a9c65ae33f1188887c44527aa947a6c1149d56cd3b0fac0acea0ec678d2b282a",
  "60"  = "1750c221cff2c19b36f70e17c93d6f14cb459dc082bf5c096bbc1e7d812feb9b",
  "120" = "722524930422947aece121dc3e5489d8c78fd8dc51549bc695727ab5a1540272",
  "180" = "4be728add8ea5ac33c701adc14fd8d916a65feb4ecd570d7ec24e6a2ad0b59f6"
)

#' Artifacts that exist, look plausible, and are NOT imported
#'
#' Recorded so the refusal is a decision on the record rather than an omission
#' someone later "fixes".
#' @export
ISOCHRONE_REFUSED_ARTIFACTS <- tibble::tribble(
  ~artifact, ~why_refused,
  "native_seam_tracts/ (downstream_results/20260614_214343_ac587845, Jul 22-23)",
  paste("Newest and most tempting -- it is the computed access surface by tract",
        "with RUCA for 2020-2023. Its PIPELINE_SUCCESS.json says exit 0 while its",
        "own RUN_MANIFEST.md records 9 succeeded / 10 failed steps and 'Chain of",
        "Custody: INTACT (0 verified, 0 broken)', which is vacuous. An earlier",
        "PIPELINE_FAILED.json shows a resume past missing receipts for steps 2.5,",
        "3, 4 and 6. Not importable until that contradiction is explained."),

  "abu_isochrones/abu_iso_20260715_052420 (Jul 15)",
  paste("A genuine 29-physician gap-fill for uncovered ABU/FPMRS providers, on a",
        "DIFFERENT Valhalla build (3.7.0-ae2c62ecf). Its own manifest requires",
        "schema normalisation first: inner bands carry +2 QA columns",
        "(nesting_clip_failed, nesting_was_clipped) vs canonical 41. Worth doing",
        "as its own reconciliation against the 15 baseline providers still",
        "lacking coordinates -- deliberately NOT folded into this import, so a",
        "clean provenance chain is not contaminated by a merge."),

  "supplemental_isochrones/ (Jun 20)",
  "Supplement covering 30/60/120 only -- no 180 band, so it cannot complete a four-band set.",

  "artifacts/isochrones/README.md",
  paste("Stale. Dated 2026-03-09 and describes a superseded merge of 40,562",
        "locations (~10x the active run's ~3,911/band). The registry and",
        "SHA256SUMS are authoritative; do not size or validate against it."),

  "supplemental_npi_location_map.rds (Mar 4)",
  "Sits beside the canonical files but is absent from SHA256SUMS.txt, so it is unverifiable."
)

ISOCHRONE_BAND_FILE <- function(band) {
  sprintf("isochrones_%dmin_consolidated.rds", as.integer(band))
}

#' Directory holding the canonical isochrone artifacts
#'
#' Resolved from `SIMULATION_ISOCHRONE_ROOT`, then `isochrones_root:` in
#' `config/paths.local.yml`, then `config/paths.yml`. Referenced in place rather
#' than copied: the set is ~1.6 GB, checksum verification makes in-place safe,
#' and duplicating it would create a second thing to keep true.
#'
#' @return Path to the directory, or NA when none is configured or present.
#' @family isochrone import
#' @concept geography
#' @export
isochrone_source_dir <- function() {
  env <- Sys.getenv("SIMULATION_ISOCHRONE_ROOT", unset = "")
  if (nzchar(env)) return(path.expand(env))

  for (cfg in c(file.path(getwd(), "config", "paths.local.yml"),
                file.path(getwd(), "config", "paths.yml"))) {
    if (!file.exists(cfg)) next
    y <- tryCatch(yaml::read_yaml(cfg), error = function(e) NULL)
    root <- y$isochrones_root
    if (!is.null(root) && nzchar(root)) return(path.expand(root))
  }
  NA_character_
}

#' Verify the canonical isochrone set without throwing
#'
#' Checks identity, not merely existence: the registry's `active_run_id`, the
#' band set, and the SHA-256 of every band file. Returns a report so a caller can
#' decide; [assert_canonical_isochrones()] is the fail-closed form.
#'
#' `verify_checksums = FALSE` skips the ~1.6 GB hash, which takes minutes. Use it
#' only for a fast presence check; it cannot establish identity.
#'
#' @param dir Source directory; defaults to [isochrone_source_dir()].
#' @param verify_checksums Hash every band file (default TRUE).
#' @return List with `ok`, `reason`, `dir`, `run_id`, `bands`, and per-file `files`.
#' @family isochrone import
#' @concept geography
#' @export
verify_canonical_isochrones <- function(dir = isochrone_source_dir(),
                                        verify_checksums = TRUE) {
  fail <- function(reason, ...) {
    c(list(ok = FALSE, reason = reason, dir = dir), list(...))
  }
  if (is.na(dir) || !nzchar(dir)) {
    return(fail(paste(
      "No isochrone source configured. Set SIMULATION_ISOCHRONE_ROOT, or add",
      "isochrones_root: to config/paths.local.yml. The canonical set is run",
      ISOCHRONE_CANONICAL_RUN_ID, "in the mufflyt/isochrones repository.")))
  }
  if (!dir.exists(dir)) {
    return(fail(sprintf("Isochrone source directory does not exist: %s", dir)))
  }

  # Registry first, filenames never. A directory of correctly-named files from
  # the wrong run is the failure this ordering prevents.
  reg_path <- file.path(dir, "ISOCHRONE_REGISTRY.json")
  run_id <- NA_character_
  if (file.exists(reg_path)) {
    reg <- tryCatch(jsonlite::fromJSON(reg_path, simplifyVector = FALSE),
                    error = function(e) NULL)
    if (is.null(reg)) return(fail(sprintf("Unreadable registry: %s", reg_path)))
    run_id <- reg$active_run_id %||% NA_character_
    if (!identical(run_id, ISOCHRONE_CANONICAL_RUN_ID)) {
      return(fail(sprintf(paste(
        "Registry active_run_id is '%s' but this package is pinned to '%s'. A",
        "newer run may be legitimate, but it must be reviewed and the pin moved",
        "deliberately -- recency is not provenance."),
        run_id, ISOCHRONE_CANONICAL_RUN_ID), run_id = run_id))
    }
  } else {
    return(fail(sprintf(paste(
      "No ISOCHRONE_REGISTRY.json in %s. Identity cannot be established from",
      "filenames alone, and this directory sits beside quarantined and",
      "superseded artifacts."), dir)))
  }

  files <- lapply(as.character(ISOCHRONE_CANONICAL_BANDS), function(b) {
    p <- file.path(dir, ISOCHRONE_BAND_FILE(b))
    present <- file.exists(p)
    got <- if (present && isTRUE(verify_checksums)) {
      tryCatch(as.character(digest::digest(p, algo = "sha256", file = TRUE)),
               error = function(e) NA_character_)
    } else NA_character_
    want <- unname(ISOCHRONE_CANONICAL_SHA256[[b]])
    list(band = as.integer(b), path = p, present = present,
         expected_sha256 = want, observed_sha256 = got,
         sha256_ok = if (isTRUE(verify_checksums)) identical(got, want) else NA)
  })
  names(files) <- as.character(ISOCHRONE_CANONICAL_BANDS)

  missing <- vapply(files, function(f) !f$present, logical(1))
  if (any(missing)) {
    return(fail(sprintf("Missing band file(s): %s",
                        paste(vapply(files[missing], function(f) basename(f$path),
                                     character(1)), collapse = ", ")),
                run_id = run_id, files = files))
  }
  if (isTRUE(verify_checksums)) {
    bad <- vapply(files, function(f) !isTRUE(f$sha256_ok), logical(1))
    if (any(bad)) {
      return(fail(sprintf(paste(
        "SHA-256 mismatch on band(s) %s. The file is not the canonical artifact,",
        "whatever it is named -- the source tree contains quarantined and broken",
        "copies under similar names."),
        paste(names(files)[bad], collapse = ", ")), run_id = run_id, files = files))
    }
  }

  list(ok = TRUE, reason = NA_character_, dir = dir, run_id = run_id,
       bands = ISOCHRONE_CANONICAL_BANDS, files = files,
       valhalla_version = ISOCHRONE_CANONICAL_VALHALLA,
       cohort = ISOCHRONE_CANONICAL_COHORT,
       checksums_verified = isTRUE(verify_checksums))
}

#' Fail closed unless the canonical isochrone set is present and verified
#'
#' @param dir Source directory.
#' @param mode Reproducibility mode; strict errors, relaxed warns.
#' @param verify_checksums Passed to [verify_canonical_isochrones()].
#' @return (Invisibly) the verification report.
#' @family isochrone import
#' @concept geography
#' @export
assert_canonical_isochrones <- function(dir = isochrone_source_dir(),
                                        mode = resolve_reproducibility_mode(),
                                        verify_checksums = TRUE) {
  v <- verify_canonical_isochrones(dir, verify_checksums = verify_checksums)
  if (isTRUE(v$ok)) return(invisible(v))

  msg <- paste0(
    "Canonical drive-time isochrones unavailable: ", v$reason,
    " Refusing to substitute another artifact -- see ISOCHRONE_REFUSED_ARTIFACTS",
    " for the ones that exist and why each is not used."
  )
  if (identical(mode, "strict")) stop(msg, call. = FALSE)
  .msg_warn(msg)
  invisible(v)
}
