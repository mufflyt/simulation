################################################################################
# R/calibration-anchor_integrity.R
# Verify calibration anchors against their recorded SHA-256.
#
# config/calibration_targets.yml carries a sha256 for every anchor. Until this
# file existed nothing checked them, which is the worst of both worlds: the
# field looks like an integrity guarantee and provides none. An anchor edited
# between runs -- by hand, by a re-ingest, by a merge -- would change every
# calibration scalar downstream with nothing to say so.
#
# Anchors marked `status: missing` are not yet acquired (the HCUP Central
# Distributor pull is licensed); they are reported, not failed, so the check can
# be wired into a render without blocking on data that does not exist yet.
################################################################################

#' Verify calibration anchor files against their recorded checksums
#'
#' @param config Path to the calibration targets YAML.
#' @param root Repository root; anchor paths are resolved against it.
#' @param strict If TRUE, a mismatch or an unhashed present file is an error.
#'   Missing (`status: missing`) anchors never error.
#' @return Invisibly, a data.frame with one row per anchor: `anchor`, `path`,
#'   `state` (one of "ok", "mismatch", "unhashed", "absent", "missing_declared").
#' @examples
#' \dontrun{ verify_calibration_anchors() }
#' @export
verify_calibration_anchors <- function(config = "config/calibration_targets.yml",
                                       root = ".", strict = TRUE) {
  cfg <- yaml::read_yaml(file.path(root, config))
  anchors <- cfg$anchors
  out <- do.call(rbind, lapply(names(anchors), function(nm) {
    a <- anchors[[nm]]
    f <- file.path(root, a$path)
    declared <- if (is.null(a$sha256)) "" else a$sha256
    state <-
      if (identical(a$status, "missing") && !file.exists(f)) "missing_declared"
      else if (!file.exists(f))                              "absent"
      else if (!nzchar(declared))                            "unhashed"
      else if (identical(unname(tools::md5sum(f)), NA_character_)) "absent"
      else {
        actual <- digest::digest(file = f, algo = "sha256")
        if (identical(actual, declared)) "ok" else "mismatch"
      }
    data.frame(anchor = nm, path = a$path, state = state, stringsAsFactors = FALSE)
  }))

  bad <- out$state %in% c("mismatch", "absent", "unhashed")
  for (i in seq_len(nrow(out))) {
    base::message(sprintf("  %-28s %-46s %s", out$anchor[i], out$path[i], out$state[i]))
  }
  if (strict && any(bad)) {
    stop("calibration anchor integrity failed for: ",
         paste(out$anchor[bad], collapse = ", "),
         ". A 'mismatch' means the anchor changed since its checksum was recorded; ",
         "every calibration scalar computed from it is suspect until reconciled.",
         call. = FALSE)
  }
  invisible(out)
}
