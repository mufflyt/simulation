# Probabilistic Sensitivity Analysis (global PSA + variance decomposition) ----
#
# A model-agnostic PSA engine: sample every uncertain input JOINTLY (Latin
# Hypercube), evaluate the model once per draw, and decompose which inputs drive
# the spread of the output. This is the reproducible, global replacement for a
# one-input-at-a-time sensitivity table.
#
#   run_psa(inputs, evaluate, n)   -> the draw x (inputs, outputs) table
#   psa_prcc(psa, output)          -> Partial Rank Correlation Coefficients
#   psa_srrc(psa, output)          -> Standardised Rank Regression Coefficients
#                                     + each input's share of explained variance
#   psa_tornado(psa, output)       -> ranked tidy table for a tornado plot
#   plot_psa_tornado(tornado)      -> ggplot horizontal tornado (Suggests ggplot2)
#
# PRCC (Blower & Dowlatabadi 1994) is the standard global index for a monotone
# model: the partial correlation of rank(input) with rank(output), controlling
# for the other ranked inputs. It is signed (direction of effect) and robust to
# nonlinear-but-monotone responses. SRRC gives an approximate variance share.
#
# The engine is decoupled from the workforce model: `evaluate` is any function
# mapping a named list of inputs to a numeric scalar (or named numeric vector of
# several outputs). psa_workforce_gap() wires the workforce orchestrator as the
# default evaluator.

# ---- Input distribution specs ---------------------------------------------

#' Define a uniform PSA input
#' @param name Input name (matches the `evaluate` argument).
#' @param min,max Range.
#' @return A `psa_input` spec.
#' @family psa
#' @concept calibration
#' @export
psa_uniform <- function(name, min, max) {
  structure(list(name = name, type = "uniform", min = min, max = max),
            class = "psa_input")
}

#' Define a normal PSA input
#' @param name Input name.
#' @param mean,sd Normal parameters.
#' @return A `psa_input` spec.
#' @examples
#' psa_normal("retirement_age", mean = 65, sd = 2)
#' @family psa
#' @concept calibration
#' @export
psa_normal <- function(name, mean, sd) {
  structure(list(name = name, type = "normal", mean = mean, sd = sd),
            class = "psa_input")
}

#' Define a triangular PSA input (min / mode / max) -- the common PSA default
#' @param name Input name.
#' @param min,mode,max Triangular parameters.
#' @return A `psa_input` spec.
#' @examples
#' psa_triangular("entrants", min = 45, mode = 55, max = 70)
#' @family psa
#' @concept calibration
#' @export
psa_triangular <- function(name, min, mode, max) {
  assertthat::assert_that(min <= mode, mode <= max)
  structure(list(name = name, type = "triangular", min = min, mode = mode, max = max),
            class = "psa_input")
}

#' Define a discrete PSA input (unordered or ordered categories)
#' @param name Input name.
#' @param values Character/numeric vector of allowed values.
#' @param probs Optional selection probabilities (default uniform).
#' @return A `psa_input` spec.
#' @family psa
#' @concept calibration
#' @export
psa_discrete <- function(name, values, probs = NULL) {
  if (is.null(probs)) probs <- rep(1 / length(values), length(values))
  assertthat::assert_that(length(values) == length(probs),
                          abs(sum(probs) - 1) < 1e-8)
  structure(list(name = name, type = "discrete", values = values, probs = probs),
            class = "psa_input")
}

# Inverse-CDF for one input at LHS uniforms u in (0,1). Discrete inputs return
# their category index (1..k) as the numeric coordinate + the mapped value.
.psa_quantile <- function(input, u) {
  switch(input$type,
    uniform    = input$min + u * (input$max - input$min),
    normal     = stats::qnorm(u, input$mean, input$sd),
    triangular = {
      fc <- (input$mode - input$min) / (input$max - input$min)
      ifelse(u < fc,
             input$min + sqrt(u * (input$max - input$min) * (input$mode - input$min)),
             input$max - sqrt((1 - u) * (input$max - input$min) * (input$max - input$mode)))
    },
    discrete   = {
      cutp <- cumsum(input$probs)
      idx <- findInterval(u, cutp[-length(cutp)]) + 1L
      idx
    },
    stop(sprintf("Unknown psa_input type '%s'", input$type), call. = FALSE)
  )
}

# ---- Latin Hypercube sampling ---------------------------------------------

#' Latin Hypercube sample of a set of PSA inputs
#'
#' Each input's marginal is stratified into `n` equal-probability bins; inputs
#' are permuted independently so joint draws are near-orthogonal.
#'
#' @param inputs List of `psa_input()` specs.
#' @param n Number of draws.
#' @param seed RNG seed (the caller's stream is restored on exit).
#' @return Tibble with `n` rows: one numeric column per input (discrete inputs
#'   hold the category index) plus `<name>_value` columns for discrete inputs.
#' @family psa
#' @concept calibration
#' @export
psa_sample <- function(inputs, n, seed = 20260801L) {
  old <- if (exists(".Random.seed", envir = .GlobalEnv)) get(".Random.seed", envir = .GlobalEnv) else NULL
  on.exit(if (!is.null(old)) assign(".Random.seed", old, envir = .GlobalEnv))
  set.seed(seed)

  cols <- list()
  for (inp in inputs) {
    # Stratified LHS uniforms: one point per (1/n) bin, jittered within the bin.
    u <- (sample.int(n) - stats::runif(n)) / n
    coord <- .psa_quantile(inp, u)
    cols[[inp$name]] <- coord
    if (inp$type == "discrete") {
      cols[[paste0(inp$name, "_value")]] <- inp$values[coord]
    }
  }
  tibble::as_tibble(cols)
}

# ---- Run the PSA ----------------------------------------------------------

#' Run a probabilistic sensitivity analysis
#'
#' Samples the inputs jointly (LHS) and evaluates `evaluate` once per draw.
#'
#' @param inputs List of `psa_input()` specs.
#' @param evaluate Function taking a named list of input values (discrete inputs
#'   passed as their mapped value) and returning a numeric scalar, or a named
#'   numeric vector of several outputs.
#' @param n Number of Monte-Carlo draws.
#' @param seed RNG seed.
#' @param verbose Logical progress.
#' @return List: `draws` (tibble of inputs + outputs), `inputs`, `n`, `seed`,
#'   `n_failed` (evaluations that errored -> NA).
#' @family psa
#' @concept calibration
#' @export
run_psa <- function(inputs, evaluate, n = 500, seed = 20260801L, verbose = TRUE) {
  assertthat::assert_that(is.function(evaluate))
  samp <- psa_sample(inputs, n, seed)

  if (verbose) .msg_info(sprintf("PSA: %d draws over %d inputs", n, length(inputs)))

  n_failed <- 0L
  out_list <- vector("list", n)
  for (i in seq_len(n)) {
    if (verbose && i %% 100 == 0) .msg_info(sprintf("  draw %d/%d", i, n))
    params <- list()
    for (inp in inputs) {
      params[[inp$name]] <- if (inp$type == "discrete") {
        inp$values[samp[[inp$name]][i]]
      } else {
        samp[[inp$name]][i]
      }
    }
    res <- tryCatch(evaluate(params), error = function(e) {
      .msg_debug(sprintf("PSA draw %d failed: %s", i, conditionMessage(e)))
      NA_real_
    })
    if (all(is.na(res))) n_failed <- n_failed + 1L
    out_list[[i]] <- res
  }

  # Normalise outputs to a named tibble (support scalar or named-vector evaluate).
  first_ok <- out_list[[which(!vapply(out_list, function(x) all(is.na(x)), logical(1)))[1]]]
  out_names <- if (!is.null(names(first_ok))) names(first_ok) else "output"
  out_mat <- do.call(rbind, lapply(out_list, function(x) {
    v <- rep(NA_real_, length(out_names)); v[seq_along(x)] <- as.numeric(x); v
  }))
  colnames(out_mat) <- out_names

  draws <- dplyr::bind_cols(tibble::tibble(.iter = seq_len(n)), samp,
                            tibble::as_tibble(as.data.frame(out_mat)))
  if (n_failed > 0) .msg_warn(sprintf("PSA: %d/%d evaluations failed (NA)", n_failed, n))

  list(draws = draws, inputs = inputs, n = n, seed = seed, n_failed = n_failed,
       output_names = out_names)
}

# ---- Sensitivity indices --------------------------------------------------

.psa_input_names <- function(psa) vapply(psa$inputs, function(i) i$name, character(1))

#' Partial Rank Correlation Coefficients (global sensitivity)
#'
#' For each input, the partial correlation of its rank with the output's rank,
#' controlling for all other inputs' ranks. Signed and robust for monotone
#' models. NA output rows are dropped.
#'
#' @param psa A [run_psa()] result.
#' @param output Output column name (default the first output).
#' @return Tibble `input`, `prcc`, `p_value`, ordered by descending |prcc|.
#' @family psa
#' @concept calibration
#' @export
psa_prcc <- function(psa, output = psa$output_names[1]) {
  vars <- .psa_input_names(psa)
  d <- psa$draws[stats::complete.cases(psa$draws[c(vars, output)]), , drop = FALSE]
  n <- nrow(d)
  # Degenerate cases are undefined, not zero: too few complete draws to residualise
  # (df would be <= 0), or a zero-variance output (every rank tied -> the partial
  # correlation is meaningless, and residualising a constant leaves only
  # floating-point noise that reads as a spurious non-zero PRCC). Return NA, never
  # a fabricated sensitivity for an output that does not vary.
  if (n < 3L || stats::sd(d[[output]]) == 0) {
    return(tibble::tibble(input = vars, prcc = NA_real_, p_value = NA_real_))
  }
  R <- as.data.frame(lapply(d[vars], rank))
  ry <- rank(d[[output]])

  res <- lapply(vars, function(v) {
    # A constant input has no rank variance -> its partial correlation is undefined.
    if (stats::sd(d[[v]]) == 0) {
      return(tibble::tibble(input = v, prcc = NA_real_, p_value = NA_real_))
    }
    others <- setdiff(vars, v)
    # Residualise rank(v) and rank(y) on the other ranked inputs.
    if (length(others) > 0) {
      ex <- stats::residuals(stats::lm(R[[v]] ~ ., data = R[others]))
      ey <- stats::residuals(stats::lm(ry ~ ., data = R[others]))
    } else {
      ex <- R[[v]] - mean(R[[v]]); ey <- ry - mean(ry)
    }
    r <- suppressWarnings(stats::cor(ex, ey))
    k <- length(others)
    # t-test on the partial correlation.
    df <- n - k - 2
    tval <- if (is.na(r) || abs(r) >= 1 || df <= 0) NA_real_ else r * sqrt(df / (1 - r^2))
    p <- if (is.na(tval)) NA_real_ else 2 * stats::pt(-abs(tval), df)
    tibble::tibble(input = v, prcc = r, p_value = p)
  })
  out <- dplyr::bind_rows(res)
  out[order(-abs(out$prcc)), ]
}

#' Standardised Rank Regression Coefficients + variance shares
#'
#' Fits rank(output) ~ rank(inputs); standardised coefficients approximate each
#' input's contribution, and `var_share` (coef^2, renormalised to the model R^2)
#' estimates the fraction of output-rank variance each input explains -- the
#' natural magnitude axis for a tornado.
#'
#' @param psa A [run_psa()] result.
#' @param output Output column name.
#' @return List: `coefficients` (tibble `input`, `srrc`, `var_share`) ordered by
#'   descending |srrc|, and `model_r2`.
#' @family psa
#' @concept calibration
#' @export
psa_srrc <- function(psa, output = psa$output_names[1]) {
  vars <- .psa_input_names(psa)
  d <- psa$draws[stats::complete.cases(psa$draws[c(vars, output)]), , drop = FALSE]
  n <- nrow(d)
  na_out <- list(
    coefficients = tibble::tibble(input = vars, srrc = NA_real_, var_share = NA_real_),
    model_r2 = NA_real_)
  # Degenerate: too few complete draws, or a zero-variance output. Standardising a
  # constant output divides by zero and would crash lm(); degrade to NA instead.
  if (n < 3L || stats::sd(d[[output]]) == 0) return(na_out)
  z <- function(x) { r <- rank(x); (r - mean(r)) / stats::sd(r) }
  # A constant input has zero rank variance (z would be NaN and break the fit), so
  # it cannot enter the regression; it is reported back with an NA share.
  keep <- vars[vapply(d[vars], function(x) stats::sd(x) > 0, logical(1))]
  if (length(keep) == 0) return(na_out)
  Z <- as.data.frame(lapply(d[keep], z)); Z$.y <- z(d[[output]])
  fit <- stats::lm(.y ~ ., data = Z)
  r2 <- summary(fit)$r.squared
  co <- stats::coef(fit)[-1]
  denom <- sum(co^2)
  var_share <- if (denom == 0) rep(0, length(co)) else (co^2) / denom * r2
  fitted_tbl <- tibble::tibble(input = names(co), srrc = unname(co),
                               var_share = unname(var_share))
  dropped <- setdiff(vars, keep)
  if (length(dropped)) {
    fitted_tbl <- dplyr::bind_rows(
      fitted_tbl,
      tibble::tibble(input = dropped, srrc = NA_real_, var_share = NA_real_))
  }
  out <- fitted_tbl[order(-abs(fitted_tbl$srrc)), ]   # NA (dropped inputs) sort last
  list(coefficients = out, model_r2 = r2)
}

#' Tidy tornado table (ranked sensitivity indices)
#'
#' @param psa A [run_psa()] result.
#' @param output Output column name.
#' @param method "prcc" (default) or "srrc".
#' @return Tibble `input`, `index`, `direction` ordered by descending magnitude.
#' @family psa
#' @concept calibration
#' @export
psa_tornado <- function(psa, output = psa$output_names[1], method = c("prcc", "srrc")) {
  method <- match.arg(method)
  tab <- if (method == "prcc") {
    p <- psa_prcc(psa, output)
    tibble::tibble(input = p$input, index = p$prcc)
  } else {
    s <- psa_srrc(psa, output)$coefficients
    tibble::tibble(input = s$input, index = s$srrc)
  }
  tab$direction <- ifelse(tab$index >= 0, "increases", "decreases")
  tab$method <- method
  tab[order(-abs(tab$index)), ]
}

#' Horizontal tornado plot of PSA sensitivity indices
#'
#' @param tornado A [psa_tornado()] table.
#' @param title Plot title.
#' @return A ggplot object (requires ggplot2).
#' @family psa
#' @concept calibration
#' @export
plot_psa_tornado <- function(tornado, title = "PSA sensitivity (tornado)") {
  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    stop("plot_psa_tornado() needs the 'ggplot2' package (Suggests).", call. = FALSE)
  }
  d <- tornado
  d$input <- factor(d$input, levels = d$input[order(abs(d$index))])
  lab <- if (identical(d$method[1], "prcc")) "PRCC" else "SRRC"
  ggplot2::ggplot(d, ggplot2::aes(x = .data$index, y = .data$input, fill = .data$direction)) +
    ggplot2::geom_col() +
    ggplot2::geom_vline(xintercept = 0, linewidth = 0.4) +
    ggplot2::labs(title = title, x = lab, y = NULL, fill = NULL) +
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::theme(plot.title = ggplot2::element_text(size = ggplot2::rel(1.1)),
                   plot.title.position = "plot")
}
