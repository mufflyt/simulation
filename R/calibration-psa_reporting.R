# PSA reporting: summarise a run, and freeze it with provenance ----
#
# `run_psa()` returns draws; these turn draws into the two things a reader
# actually needs, and do it in a way that cannot go stale silently.
#
# WHY THE ARTIFACT CARRIES ITS INPUTS. A PSA result is only interpretable
# against the input distributions that produced it. A frozen RDS of draws, with
# no record of the `psa_uniform(...)` specs behind them, cannot be distinguished
# from a run under different assumptions -- and the natural failure is silent:
# someone widens an input, re-runs nothing, and reads the old file. So the
# artifact is written through write_artifact_with_provenance() with the PSA
# input specs, the model inputs, and the deficit definition all in the
# fingerprint. Reading it back with different ones fails closed.
#
# WHY THE DEFICIT DEFINITION IS PART OF THAT FINGERPRINT. `p_deficit` is
# meaningless without knowing which side of the threshold counts as a deficit. A
# gap where negative means "short of demand" and one where positive means it are
# both common, and mixing them silently inverts the headline probability.

#' Summarise one PSA output
#'
#' @param psa A [run_psa()] result.
#' @param output Output column to summarise. Defaults to the first.
#' @param deficit_direction Which side of `deficit_threshold` counts as a
#'   deficit. `"negative"` (the default) means values below the threshold.
#' @param deficit_threshold Threshold separating deficit from adequacy.
#' @param ci Central interval width.
#' @return A one-row tibble of `output`, `n_draws`, `n_complete`, `n_failed`,
#'   `mean`, `median`, `sd`, `lower`, `upper`, `ci`, `p_deficit`, and the deficit
#'   definition used.
#'
#' @importFrom stats median quantile sd
#' @family psa reporting
#' @concept calibration
#' @export
psa_outcome_summary <- function(psa,
                                output = psa$output_names[1],
                                deficit_direction = c("negative", "positive"),
                                deficit_threshold = 0,
                                ci = 0.95) {
  deficit_direction <- match.arg(deficit_direction)
  if (!output %in% names(psa$draws)) {
    stop("psa_outcome_summary(): '", output, "' is not a column of psa$draws.",
         call. = FALSE)
  }
  if (!(ci > 0 && ci < 1)) {
    stop("psa_outcome_summary(): ci must be between 0 and 1.", call. = FALSE)
  }

  v <- psa$draws[[output]]
  ok <- v[is.finite(v)]
  if (!length(ok)) {
    stop("psa_outcome_summary(): every draw of '", output, "' is non-finite; ",
         "there is nothing to summarise.", call. = FALSE)
  }
  a <- (1 - ci) / 2
  q <- stats::quantile(ok, c(a, 1 - a), names = FALSE)

  # NA draws are excluded from the probability rather than counted as adequate.
  # Treating a failed evaluation as "not a deficit" biases the headline downward
  # exactly when the model is least stable.
  p_def <- if (identical(deficit_direction, "negative")) {
    mean(ok < deficit_threshold)
  } else {
    mean(ok > deficit_threshold)
  }

  tibble::tibble(
    output            = output,
    n_draws           = as.integer(psa$n),
    n_complete        = length(ok),
    n_failed          = as.integer(psa$n) - length(ok),
    mean              = mean(ok),
    median            = stats::median(ok),
    sd                = stats::sd(ok),
    lower             = q[1],
    upper             = q[2],
    ci                = ci,
    p_deficit         = p_def,
    deficit_direction = deficit_direction,
    deficit_threshold = deficit_threshold
  )
}

#' Write a PSA run as a provenance-guarded artifact plus readable tables
#'
#' Freezes the run itself as the artifact (so `draws` round-trip exactly) and
#' writes the summary, PRCC, and SRRC tables beside it as CSV.
#'
#' The provenance fingerprint covers the model inputs, the PSA input specs, the
#' chosen output, and the deficit definition. Reading the artifact back with any
#' of those changed fails rather than returning a stale object.
#'
#' @param psa A [run_psa()] result.
#' @param output Output column to report. Defaults to the first.
#' @param output_dir Directory to write into; created if absent.
#' @param prefix Filename prefix for every file written.
#' @param inputs Model inputs to record in the provenance fingerprint.
#' @param code_paths Source files to fingerprint alongside the inputs.
#' @param deficit_direction,deficit_threshold Deficit definition, recorded in the
#'   fingerprint because `p_deficit` is meaningless without it.
#' @param write_plot Also write a tornado plot PNG. Requires ggplot2.
#' @param mode Reproducibility mode passed through to the artifact writer.
#' @return A list of the paths written: `artifact`, `draws`, `summary`, `prcc`,
#'   `srrc`, and `plot` (NA when not written).
#'
#' @importFrom utils write.csv
#' @family psa reporting
#' @concept calibration
#' @export
write_psa_report <- function(psa,
                             output = psa$output_names[1],
                             output_dir = "outputs",
                             prefix = "psa",
                             inputs = NULL,
                             code_paths = character(0),
                             deficit_direction = c("negative", "positive"),
                             deficit_threshold = 0,
                             write_plot = TRUE,
                             mode = resolve_reproducibility_mode()) {
  deficit_direction <- match.arg(deficit_direction)
  if (!output %in% names(psa$draws)) {
    stop("write_psa_report(): '", output, "' is not a column of psa$draws.",
         call. = FALSE)
  }
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  path <- function(suffix, ext) file.path(output_dir, paste0(prefix, suffix, ext))

  summary_tbl <- psa_outcome_summary(psa, output, deficit_direction,
                                     deficit_threshold)
  prcc <- psa_prcc(psa, output)
  srrc <- psa_srrc(psa, output)

  artifact_path <- path("_psa", ".rds")
  # The deficit definition travels INSIDE the fingerprint, not beside it: a run
  # re-read under a flipped direction must fail, not silently invert p_deficit.
  write_artifact_with_provenance(
    object = psa,
    artifact_path = artifact_path,
    inputs = list(model_inputs = inputs, psa_inputs = psa$inputs,
                  output = output, deficit_direction = deficit_direction,
                  deficit_threshold = deficit_threshold),
    code_paths = code_paths,
    mode = mode
  )

  draws_path <- path("_draws", ".csv")
  summary_path <- path("_summary", ".csv")
  prcc_path <- path("_prcc", ".csv")
  srrc_path <- path("_srrc", ".csv")
  utils::write.csv(psa$draws, draws_path, row.names = FALSE)
  utils::write.csv(summary_tbl, summary_path, row.names = FALSE)
  utils::write.csv(prcc, prcc_path, row.names = FALSE)
  utils::write.csv(srrc, srrc_path, row.names = FALSE)

  plot_path <- NA_character_
  if (isTRUE(write_plot)) {
    if (!requireNamespace("ggplot2", quietly = TRUE)) {
      .msg_warn("write_psa_report(): ggplot2 is not installed; skipping the ",
                "tornado plot. Every table was still written.")
    } else {
      plot_path <- path("_tornado", ".png")
      p <- plot_psa_tornado(psa_tornado(psa, output))
      ggplot2::ggsave(plot_path, p, width = 7, height = 5, dpi = 150)
    }
  }

  list(artifact = artifact_path, draws = draws_path, summary = summary_path,
       prcc = prcc_path, srrc = srrc_path, plot = plot_path)
}
