# CMS service-share frozen-source guard -------------------------------------

#' Verify a CMS service-share file against its canonical SHA-256
#'
#' Reads the expected checksum from `config/canonical_sources.yml` and
#' verifies the file actually supplied to the service-share pipeline. This
#' deliberately verifies the resolved file contents rather than the path so
#' mounted or environment-overridden copies remain valid when byte-identical.
#'
#' @param path Path to the CMS file to verify.
#' @param source_name Canonical source key under `sources:`.
#' @param config_path Canonical source registry path.
#'
#' @return The observed SHA-256, invisibly.
#' @keywords internal
.cms_verify_canonical_sha256 <- function(
    path,
    source_name,
    config_path = .canonical_config_path()) {
  base::message(
    "Verifying frozen CMS service-share source: ", source_name, "."
  )

  if (!base::file.exists(path)) {
    base::stop(
      "CMS service-share source file does not exist: ", path,
      call. = FALSE
    )
  }
  if (!base::file.exists(config_path)) {
    base::stop(
      "Canonical source registry not found: ", config_path,
      call. = FALSE
    )
  }

  registry <- yaml::read_yaml(config_path)
  sources <- registry[["sources"]]
  if (base::is.null(sources)) {
    sources <- registry
  }
  entry <- sources[[source_name]]
  if (base::is.null(entry) || !base::is.list(entry)) {
    base::stop(
      "Canonical CMS source is not registered: ", source_name,
      call. = FALSE
    )
  }

  expected_sha <- entry[["sha256"]]
  valid_sha <- base::is.character(expected_sha) &&
    base::length(expected_sha) == 1L &&
    base::nzchar(expected_sha) &&
    base::grepl("^[0-9A-Fa-f]{64}$", expected_sha)
  if (!valid_sha) {
    base::stop(
      "Canonical CMS source has no valid SHA-256: ", source_name,
      call. = FALSE
    )
  }

  expected_sha <- base::tolower(expected_sha)
  observed_sha <- digest::digest(file = path, algo = "sha256")
  if (!base::identical(observed_sha, expected_sha)) {
    base::stop(
      "Canonical CMS source SHA-256 mismatch for ", source_name,
      ".\nExpected: ", expected_sha,
      "\nObserved: ", observed_sha,
      call. = FALSE
    )
  }

  base::message(
    "Verified frozen CMS source ", source_name,
    " (SHA-256 ", base::substr(observed_sha, 1L, 12L), "...)."
  )
  base::invisible(observed_sha)
}
