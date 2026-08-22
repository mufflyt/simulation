# Urogynecology service-share calibration ----------------------------------

.service_share_require_counts <- function(events) {
  required <- base::c(
    "service", "condition", "year", "provider_group", "service_events"
  )
  missing <- base::setdiff(required, base::names(events))
  if (base::length(missing) > 0L) {
    base::stop(
      "Service-share events are missing: ",
      base::paste(missing, collapse = ", "),
      ".",
      call. = FALSE
    )
  }

  invalid_group <- base::setdiff(
    base::unique(base::as.character(events$provider_group)),
    provider_routing_groups()
  )
  if (base::length(invalid_group) > 0L) {
    base::stop(
      "Unrecognized provider_group: ",
      base::paste(invalid_group, collapse = ", "),
      ".",
      call. = FALSE
    )
  }

  counts <- base::as.numeric(events$service_events)
  if (base::any(!base::is.finite(counts)) || base::any(counts < 0)) {
    base::stop("service_events must be finite and nonnegative.",
      call. = FALSE)
  }
  base::invisible(TRUE)
}


.service_share_counts <- function(events) {
  .service_share_require_counts(events)
  events |>
    dplyr::transmute(
      service = base::as.character(.data$service),
      condition = base::as.character(.data$condition),
      year = base::as.integer(.data$year),
      provider_group = base::as.character(.data$provider_group),
      service_events = base::as.numeric(.data$service_events)
    ) |>
    dplyr::group_by(
      .data$service,
      .data$condition,
      .data$year,
      .data$provider_group
    ) |>
    dplyr::summarise(
      service_events = base::sum(.data$service_events),
      .groups = "drop"
    )
}


.service_share_complete_cell <- function(data, groups) {
  data |>
    dplyr::select(.data$provider_group, .data$service_events) |>
    tidyr::complete(
      provider_group = groups,
      fill = base::list(service_events = 0)
    ) |>
    dplyr::arrange(base::match(.data$provider_group, groups))
}


.service_share_prior <- function(training, groups, pseudo_count = 0.5) {
  prior <- training |>
    dplyr::group_by(.data$provider_group) |>
    dplyr::summarise(
      service_events = base::sum(.data$service_events),
      .groups = "drop"
    ) |>
    tidyr::complete(
      provider_group = groups,
      fill = base::list(service_events = 0)
    ) |>
    dplyr::arrange(base::match(.data$provider_group, groups)) |>
    dplyr::mutate(
      prior_count = .data$service_events + pseudo_count,
      prior_mean = .data$prior_count / base::sum(.data$prior_count)
    )
  prior
}


#' Select service-share concentration using a held-out year
#'
#' The concentration controls how strongly sparse service-condition cells borrow
#' the service-level provider composition. It is selected independently for each
#' service by minimizing held-out multinomial cross-entropy. This replaces the
#' historical fixed `prior_strength = 20` in the calibrated production path.
#'
#' @param events Provider-group event counts by service, condition, and year.
#' @param alpha_grid Positive candidate concentrations.
#'
#' @return A list with `selected` and all candidate `scores`.
#' @keywords internal
select_service_share_concentration <- function(
    events,
    alpha_grid = base::c(0.5, 1, 2, 5, 10, 20, 50, 100)) {
  counts <- .service_share_counts(events)
  if (!base::is.numeric(alpha_grid) || base::length(alpha_grid) < 1L ||
      base::any(!base::is.finite(alpha_grid)) ||
      base::any(alpha_grid <= 0)) {
    base::stop("alpha_grid must contain positive finite values.",
      call. = FALSE)
  }
  alpha_grid <- base::sort(base::unique(base::as.numeric(alpha_grid)))
  groups <- provider_routing_groups()

  years_per_service <- counts |>
    dplyr::distinct(.data$service, .data$year) |>
    dplyr::count(.data$service, name = "year_n")
  insufficient <- years_per_service |>
    dplyr::filter(.data$year_n < 2L)
  if (base::nrow(insufficient) > 0L) {
    base::stop(
      "Calibrated service shares require at least two years per service. ",
      "Insufficient: ",
      base::paste(insufficient$service, collapse = ", "),
      ".",
      call. = FALSE
    )
  }

  base::message(
    "Selecting service-share concentration from ",
    scales::comma(base::length(alpha_grid)),
    " candidates using leave-latest-year-out scoring."
  )

  scores <- purrr::map_dfr(
    base::unique(counts$service),
    function(service_name) {
      service_counts <- counts |>
        dplyr::filter(.data$service == service_name)
      holdout_year <- base::max(service_counts$year)
      training <- service_counts |>
        dplyr::filter(.data$year < holdout_year)
      holdout <- service_counts |>
        dplyr::filter(.data$year == holdout_year)
      prior <- .service_share_prior(training, groups)

      conditions <- base::unique(holdout$condition)
      purrr::map_dfr(alpha_grid, function(alpha_value) {
        condition_score <- purrr::map_dfr(
          conditions,
          function(condition_name) {
            training_cell <- training |>
              dplyr::filter(.data$condition == condition_name) |>
              .service_share_complete_cell(groups)
            holdout_cell <- holdout |>
              dplyr::filter(.data$condition == condition_name) |>
              .service_share_complete_cell(groups)

            prediction <- (
              training_cell$service_events +
                alpha_value * prior$prior_mean
            ) / (
              base::sum(training_cell$service_events) + alpha_value
            )
            prediction <- base::pmax(prediction, 1e-15)
            log_score <- base::sum(
              holdout_cell$service_events * base::log(prediction)
            )
            tibble::tibble(
              log_score = log_score,
              holdout_events = base::sum(holdout_cell$service_events)
            )
          }
        )
        total_events <- base::sum(condition_score$holdout_events)
        tibble::tibble(
          service = service_name,
          holdout_year = holdout_year,
          alpha = alpha_value,
          log_score = base::sum(condition_score$log_score),
          holdout_events = total_events,
          cross_entropy = -base::sum(condition_score$log_score) /
            total_events
        )
      })
    }
  )

  selected <- scores |>
    dplyr::arrange(.data$service, .data$cross_entropy, .data$alpha) |>
    dplyr::group_by(.data$service) |>
    dplyr::slice_head(n = 1L) |>
    dplyr::ungroup() |>
    dplyr::transmute(
      .data$service,
      .data$holdout_year,
      selected_alpha = .data$alpha,
      .data$log_score,
      .data$holdout_events,
      .data$cross_entropy
    )

  base::message(
    "Selected held-out concentrations for ",
    scales::comma(base::nrow(selected)),
    " services."
  )
  base::list(selected = selected, scores = scores)
}


.service_share_rdirichlet <- function(alpha, draws) {
  if (base::any(!base::is.finite(alpha)) || base::any(alpha <= 0)) {
    base::stop("Dirichlet alpha values must be positive and finite.",
      call. = FALSE)
  }
  gamma_values <- stats::rgamma(
    draws * base::length(alpha),
    shape = base::rep(alpha, each = draws),
    rate = 1
  )
  gamma_matrix <- base::matrix(
    gamma_values,
    nrow = draws,
    ncol = base::length(alpha)
  )
  gamma_matrix / base::rowSums(gamma_matrix)
}


#' Draw joint provider compositions for every service cell
#'
#' @param events Provider-group event counts.
#' @param selected_alpha Selected concentration table.
#' @param draws Number of joint Monte Carlo draws.
#' @param seed Random seed.
#'
#' @return Long tibble of joint provider-share draws.
#' @keywords internal
draw_service_share_composition <- function(
    events,
    selected_alpha,
    draws = 500L,
    seed = 20260822L) {
  counts <- .service_share_counts(events)
  groups <- provider_routing_groups()
  required_alpha <- base::c("service", "selected_alpha")
  missing_alpha <- base::setdiff(required_alpha, base::names(selected_alpha))
  if (base::length(missing_alpha) > 0L) {
    base::stop(
      "selected_alpha is missing: ",
      base::paste(missing_alpha, collapse = ", "),
      ".",
      call. = FALSE
    )
  }
  if (!base::is.numeric(draws) || base::length(draws) != 1L ||
      !base::is.finite(draws) || draws < 1) {
    base::stop("draws must be a positive integer.", call. = FALSE)
  }
  draws <- base::as.integer(draws)
  base::set.seed(base::as.integer(seed))

  service_priors <- purrr::map_dfr(
    base::unique(counts$service),
    function(service_name) {
      .service_share_prior(
        counts |>
          dplyr::filter(.data$service == service_name),
        groups
      ) |>
        dplyr::mutate(service = service_name, .before = 1)
    }
  )

  cells <- counts |>
    dplyr::distinct(.data$service, .data$condition, .data$year)

  purrr::pmap_dfr(
    cells,
    function(service, condition, year) {
      cell <- counts |>
        dplyr::filter(
          .data$service == service,
          .data$condition == condition,
          .data$year == year
        ) |>
        .service_share_complete_cell(groups)
      prior <- service_priors |>
        dplyr::filter(.data$service == service) |>
        dplyr::arrange(base::match(.data$provider_group, groups))
      alpha_row <- selected_alpha |>
        dplyr::filter(.data$service == service)
      if (base::nrow(alpha_row) != 1L) {
        base::stop(
          "Expected one selected alpha for service: ", service, ".",
          call. = FALSE
        )
      }
      posterior_alpha <- cell$service_events +
        alpha_row$selected_alpha[[1L]] * prior$prior_mean
      matrix <- .service_share_rdirichlet(posterior_alpha, draws)
      base::colnames(matrix) <- groups

      tibble::as_tibble(matrix) |>
        dplyr::mutate(draw_id = dplyr::row_number(), .before = 1) |>
        tidyr::pivot_longer(
          cols = -dplyr::all_of("draw_id"),
          names_to = "provider_group",
          values_to = "share"
        ) |>
        dplyr::mutate(
          service = service,
          condition = condition,
          year = base::as.integer(year),
          cell_events = base::sum(cell$service_events),
          selected_alpha = alpha_row$selected_alpha[[1L]],
          .before = 1
        )
    }
  )
}


.service_share_draw_physician_fraction <- function(share_draws) {
  physician_groups <- base::c(
    "urps", "general_obgyn", "general_urology", "primary_care", "other"
  )
  latest <- share_draws |>
    dplyr::group_by(.data$service) |>
    dplyr::filter(.data$year == base::max(.data$year)) |>
    dplyr::ungroup() |>
    dplyr::mutate(weighted_share = .data$share * .data$cell_events) |>
    dplyr::group_by(.data$service, .data$draw_id, .data$provider_group) |>
    dplyr::summarise(
      routed_events = base::sum(.data$weighted_share),
      .groups = "drop"
    )

  latest |>
    dplyr::group_by(.data$service, .data$draw_id) |>
    dplyr::summarise(
      urps_events = base::sum(
        .data$routed_events[.data$provider_group == "urps"]
      ),
      physician_events = base::sum(
        .data$routed_events[
          .data$provider_group %in% physician_groups
        ]
      ),
      urps_given_physician = dplyr::if_else(
        .data$physician_events > 0,
        .data$urps_events / .data$physician_events,
        NA_real_
      ),
      .groups = "drop"
    )
}


.service_share_score_sources <- function(
    share_draws,
    cms_evidence = NULL,
    chia_evidence = NULL,
    baseline_transport_sd = 0.05) {
  model_fraction <- .service_share_draw_physician_fraction(share_draws)
  draw_ids <- base::sort(base::unique(share_draws$draw_id))
  cms_detail <- tibble::tibble()
  chia_detail <- tibble::tibble()

  if (!base::is.null(cms_evidence)) {
    bounds <- cms_evidence$service_bounds |>
      dplyr::select(
        .data$service,
        cms_lower = .data$lower_bound,
        cms_upper = .data$upper_bound
      ) |>
      dplyr::distinct(.data$service, .keep_all = TRUE)
    cms_detail <- model_fraction |>
      dplyr::inner_join(bounds, by = "service") |>
      dplyr::mutate(
        distance = dplyr::case_when(
          .data$urps_given_physician < .data$cms_lower ~
            .data$cms_lower - .data$urps_given_physician,
          .data$urps_given_physician > .data$cms_upper ~
            .data$urps_given_physician - .data$cms_upper,
          TRUE ~ 0
        ),
        interval_sd = base::pmax(
          (.data$cms_upper - .data$cms_lower) / 4,
          0.02
        ),
        log_weight_component = -0.5 *
          (.data$distance / .data$interval_sd)^2
      )
  }

  if (!base::is.null(chia_evidence)) {
    chia_summary <- chia_evidence$physician_share |>
      dplyr::group_by(.data$service) |>
      dplyr::summarise(
        chia_urps_events = base::sum(.data$urps_events, na.rm = TRUE),
        chia_physician_events = base::sum(
          .data$physician_events,
          na.rm = TRUE
        ),
        chia_share = dplyr::if_else(
          .data$chia_physician_events > 0,
          .data$chia_urps_events / .data$chia_physician_events,
          NA_real_
        ),
        .groups = "drop"
      )

    if (!base::is.null(cms_evidence)) {
      transport <- compare_chia_to_cms_service_share_evidence(
        chia_evidence,
        cms_evidence,
        baseline_transport_sd = baseline_transport_sd
      ) |>
        dplyr::select(.data$service, .data$transport_sd)
    } else {
      transport <- chia_summary |>
        dplyr::transmute(
          .data$service,
          transport_sd = baseline_transport_sd
        )
    }

    chia_detail <- model_fraction |>
      dplyr::inner_join(chia_summary, by = "service") |>
      dplyr::left_join(transport, by = "service") |>
      dplyr::mutate(
        transport_sd = dplyr::coalesce(
          .data$transport_sd,
          baseline_transport_sd
        ),
        residual = .data$urps_given_physician - .data$chia_share,
        log_weight_component = -0.5 *
          (.data$residual / .data$transport_sd)^2 -
          base::log(.data$transport_sd)
      )
  }

  cms_weight <- if (base::nrow(cms_detail) > 0L) {
    cms_detail |>
      dplyr::group_by(.data$draw_id) |>
      dplyr::summarise(
        cms_log_weight = base::sum(.data$log_weight_component),
        .groups = "drop"
      )
  } else {
    tibble::tibble(draw_id = draw_ids, cms_log_weight = 0)
  }

  chia_weight <- if (base::nrow(chia_detail) > 0L) {
    chia_detail |>
      dplyr::group_by(.data$draw_id) |>
      dplyr::summarise(
        chia_log_weight = base::sum(.data$log_weight_component),
        .groups = "drop"
      )
  } else {
    tibble::tibble(draw_id = draw_ids, chia_log_weight = 0)
  }

  draw_weights <- tibble::tibble(draw_id = draw_ids) |>
    dplyr::left_join(cms_weight, by = "draw_id") |>
    dplyr::left_join(chia_weight, by = "draw_id") |>
    dplyr::mutate(
      cms_log_weight = dplyr::coalesce(.data$cms_log_weight, 0),
      chia_log_weight = dplyr::coalesce(.data$chia_log_weight, 0),
      log_weight = .data$cms_log_weight + .data$chia_log_weight
    )
  max_log_weight <- base::max(draw_weights$log_weight)
  raw_weight <- base::exp(draw_weights$log_weight - max_log_weight)
  draw_weights$weight <- raw_weight / base::sum(raw_weight)

  base::list(
    cms = cms_detail,
    chia = chia_detail,
    draw_weights = draw_weights
  )
}


#' Calibrate a joint urogynecology provider service-share model
#'
#' @param events Provider-group service counts by condition and year.
#' @param cms_evidence Optional CMS partial-identification evidence.
#' @param chia_evidence Optional CHIA transport evidence.
#' @param alpha_grid Candidate concentrations for held-out selection.
#' @param draws Number of joint draws retained in the calibration bundle.
#' @param seed Master random seed.
#'
#' @return A validated service-share calibration bundle.
#' @keywords internal
calibrate_service_share_model <- function(
    events,
    cms_evidence = NULL,
    chia_evidence = NULL,
    alpha_grid = base::c(0.5, 1, 2, 5, 10, 20, 50, 100),
    draws = 500L,
    seed = 20260822L) {
  base::message("Calibrating joint urogynecology service shares.")
  concentration <- select_service_share_concentration(events, alpha_grid)
  raw_draws <- draw_service_share_composition(
    events,
    selected_alpha = concentration$selected,
    draws = draws,
    seed = seed
  )
  source_fit <- .service_share_score_sources(
    raw_draws,
    cms_evidence = cms_evidence,
    chia_evidence = chia_evidence
  )

  has_external_evidence <- !base::is.null(cms_evidence) ||
    !base::is.null(chia_evidence)
  if (has_external_evidence) {
    base::set.seed(base::as.integer(seed) + 100000L)
    sampled_draw_ids <- base::sample(
      source_fit$draw_weights$draw_id,
      size = base::as.integer(draws),
      replace = TRUE,
      prob = source_fit$draw_weights$weight
    )
    share_draws <- purrr::map2_dfr(
      sampled_draw_ids,
      base::seq_along(sampled_draw_ids),
      function(source_draw_id, new_draw_id) {
        raw_draws |>
          dplyr::filter(.data$draw_id == source_draw_id) |>
          dplyr::mutate(
            source_draw_id = source_draw_id,
            draw_id = base::as.integer(new_draw_id),
            .after = "draw_id"
          )
      }
    )
  } else {
    share_draws <- raw_draws |>
      dplyr::mutate(source_draw_id = .data$draw_id, .after = "draw_id")
  }

  event_hash <- digest::digest(
    .service_share_counts(events),
    algo = "sha256",
    serialize = TRUE
  )
  cms_hash <- if (base::is.null(cms_evidence)) NA_character_ else {
    digest::digest(cms_evidence, algo = "sha256", serialize = TRUE)
  }
  chia_hash <- if (base::is.null(chia_evidence)) NA_character_ else {
    digest::digest(chia_evidence, algo = "sha256", serialize = TRUE)
  }

  bundle <- base::list(
    share_draws = share_draws,
    selected_alpha = concentration$selected,
    holdout_scores = concentration$scores,
    source_fit = source_fit,
    provenance = base::list(
      events_sha256 = event_hash,
      cms_evidence_sha256 = cms_hash,
      chia_evidence_sha256 = chia_hash,
      git_sha = base::Sys.getenv("GITHUB_SHA", unset = NA_character_),
      created_at = base::format(
        base::Sys.time(),
        "%Y-%m-%dT%H:%M:%S%z"
      )
    ),
    config = base::list(
      seed = base::as.integer(seed),
      draws = base::as.integer(draws),
      alpha_grid = base::as.numeric(alpha_grid),
      projection_policy = "carry_forward_latest",
      provider_groups = provider_routing_groups(),
      cms_used = !base::is.null(cms_evidence),
      chia_used = !base::is.null(chia_evidence)
    ),
    valid = TRUE
  )
  validate_service_share_bundle(bundle)
  base::message(
    "Service-share calibration complete: ",
    scales::comma(base::n_distinct(bundle$share_draws$service)),
    " services, ",
    scales::comma(base::n_distinct(bundle$share_draws$draw_id)),
    " joint draws."
  )
  bundle
}


#' Validate a service-share calibration bundle
#'
#' @param bundle Calibration bundle.
#' @param tolerance Tolerance on compositional sums.
#'
#' @return `TRUE`, invisibly.
#' @keywords internal
validate_service_share_bundle <- function(bundle, tolerance = 1e-8) {
  required_bundle <- base::c(
    "share_draws", "selected_alpha", "holdout_scores", "source_fit",
    "provenance", "config", "valid"
  )
  missing_bundle <- base::setdiff(required_bundle, base::names(bundle))
  if (base::length(missing_bundle) > 0L) {
    base::stop(
      "Service-share bundle is missing: ",
      base::paste(missing_bundle, collapse = ", "),
      ".",
      call. = FALSE
    )
  }
  if (!base::isTRUE(bundle$valid)) {
    base::stop("Service-share bundle is not marked valid.", call. = FALSE)
  }

  draws <- bundle$share_draws
  required_draws <- base::c(
    "service", "condition", "year", "draw_id", "provider_group", "share",
    "cell_events", "selected_alpha"
  )
  missing_draws <- base::setdiff(required_draws, base::names(draws))
  if (base::length(missing_draws) > 0L) {
    base::stop(
      "share_draws is missing: ",
      base::paste(missing_draws, collapse = ", "),
      ".",
      call. = FALSE
    )
  }

  if (base::any(!base::is.finite(draws$share)) ||
      base::any(draws$share < -tolerance) ||
      base::any(draws$share > 1 + tolerance)) {
    base::stop("Every service-share draw must have share in [0, 1].",
      call. = FALSE)
  }
  invalid_groups <- base::setdiff(
    base::unique(draws$provider_group),
    provider_routing_groups()
  )
  if (base::length(invalid_groups) > 0L) {
    base::stop(
      "Invalid provider_group in calibrated bundle: ",
      base::paste(invalid_groups, collapse = ", "),
      ".",
      call. = FALSE
    )
  }

  sums <- draws |>
    dplyr::group_by(
      .data$service,
      .data$condition,
      .data$year,
      .data$draw_id
    ) |>
    dplyr::summarise(
      share_sum = base::sum(.data$share),
      .groups = "drop"
    )
  if (base::any(base::abs(sums$share_sum - 1) > tolerance)) {
    base::stop("Calibrated provider shares do not sum to one.",
      call. = FALSE)
  }

  alpha <- bundle$selected_alpha$selected_alpha
  if (base::any(!base::is.finite(alpha)) || base::any(alpha <= 0)) {
    base::stop("Selected alpha values must be positive and finite.",
      call. = FALSE)
  }
  base::invisible(TRUE)
}
