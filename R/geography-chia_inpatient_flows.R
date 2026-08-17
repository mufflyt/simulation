# CHIA Inpatient & Outpatient Hospital Drive-Time Routing via Valhalla ----
#
# Replaces the crude Haversine approximation (miles * 1.3 / 40 * 60) with
# exact road-network travel time and distance matrix routing from Valhalla
# (/sources_to_targets endpoint).

#' Calculate ZIP-to-ZIP drive time with a Valhalla server
#'
#' Routes unique ZIP-code pairs through Valhalla's road network using the
#' `/sources_to_targets` matrix endpoint. This replaces any Haversine-to-drive
#' time approximation.
#'
#' ZIP codes are represented by supplied latitude/longitude coordinates.
#' For CHIA patient origins these will generally be ZCTA centroids. For
#' facilities, actual facility coordinates should be preferred over ZIP
#' centroids whenever they are available.
#'
#' @param zip_pairs Table containing origin and destination ZIP codes.
#' @param zip_centroids Table containing ZIP code, latitude, and longitude.
#' @param origin_zip_col Origin ZIP column in `zip_pairs`.
#' @param destination_zip_col Destination ZIP column in `zip_pairs`.
#' @param centroid_zip_col ZIP column in `zip_centroids`.
#' @param latitude_col Latitude column in `zip_centroids`.
#' @param longitude_col Longitude column in `zip_centroids`.
#' @param valhalla_url Base URL for the Valhalla server.
#' @param costing Valhalla costing model. Default is `"auto"`.
#' @param origin_chunk_size Origins per matrix request.
#' @param destination_chunk_size Destinations per matrix request.
#' @param save_dir Directory for the timestamped CSV artifact.
#' @param save_file Whether to save the routed pairs.
#'
#' @return A tibble with one row per requested ZIP pair and Valhalla
#'   drive time and road distance.
#'
#' @family geography chia
#' @concept geography
#' @export
valhalla_zip_drive_time <- function(
    zip_pairs,
    zip_centroids,
    origin_zip_col = "origin_zip",
    destination_zip_col = "destination_zip",
    centroid_zip_col = "zip5",
    latitude_col = "lat",
    longitude_col = "lon",
    valhalla_url = "http://localhost:8002",
    costing = "auto",
    origin_chunk_size = 50L,
    destination_chunk_size = 50L,
    save_dir = "artifacts/chia_travel",
    save_file = TRUE) {

  base::message(
    "valhalla_zip_drive_time(): starting."
  )
  base::message(
    "Valhalla server: ", valhalla_url
  )
  base::message(
    "Costing model: ", costing
  )
  base::message(
    "Origin chunk size: ", origin_chunk_size
  )
  base::message(
    "Destination chunk size: ", destination_chunk_size
  )

  required_pair_cols <- c(
    origin_zip_col,
    destination_zip_col
  )

  required_centroid_cols <- c(
    centroid_zip_col,
    latitude_col,
    longitude_col
  )

  missing_pair_cols <- base::setdiff(
    required_pair_cols,
    base::names(zip_pairs)
  )

  missing_centroid_cols <- base::setdiff(
    required_centroid_cols,
    base::names(zip_centroids)
  )

  if (base::length(missing_pair_cols) > 0L) {
    base::stop(
      "Missing pair column(s): ",
      base::paste(
        missing_pair_cols,
        collapse = ", "
      )
    )
  }

  if (base::length(missing_centroid_cols) > 0L) {
    base::stop(
      "Missing centroid column(s): ",
      base::paste(
        missing_centroid_cols,
        collapse = ", "
      )
    )
  }

  if (!base::is.numeric(origin_chunk_size) ||
      origin_chunk_size < 1L) {
    base::stop(
      "`origin_chunk_size` must be a positive integer."
    )
  }

  if (!base::is.numeric(destination_chunk_size) ||
      destination_chunk_size < 1L) {
    base::stop(
      "`destination_chunk_size` must be a positive integer."
    )
  }

  normalize_zip5 <- function(x) {
    zip_chr <- base::as.character(x)
    zip_chr <- base::trimws(zip_chr)
    zip_chr <- base::sub("-.*$", "", zip_chr)
    zip_chr <- base::gsub("[^0-9]", "", zip_chr)
    zip_chr <- base::ifelse(
      base::nchar(zip_chr) == 4L,
      base::paste0("0", zip_chr),
      zip_chr
    )
    zip_chr <- base::ifelse(
      base::nchar(zip_chr) == 5L,
      zip_chr,
      NA_character_
    )
    zip_chr
  }

  split_chunks <- function(index_vector, chunk_size) {
    chunk_id <- base::ceiling(
      base::seq_along(index_vector) / chunk_size
    )
    base::split(
      index_vector,
      chunk_id
    )
  }

  json_row_to_numeric <- function(row_values) {
    base::vapply(
      row_values,
      function(value) {
        if (base::is.null(value)) {
          return(NA_real_)
        }
        base::as.numeric(value)
      },
      numeric(1)
    )
  }

  json_rows_to_matrix <- function(
      row_values,
      expected_rows,
      expected_cols) {

    if (base::length(row_values) != expected_rows) {
      base::stop(
        "Valhalla matrix returned ",
        base::length(row_values),
        " rows; expected ",
        expected_rows,
        "."
      )
    }

    numeric_rows <- base::lapply(
      row_values,
      json_row_to_numeric
    )

    row_lengths <- base::vapply(
      numeric_rows,
      base::length,
      integer(1)
    )

    if (base::any(row_lengths != expected_cols)) {
      base::stop(
        "Valhalla matrix returned an unexpected number of columns."
      )
    }

    base::do.call(
      base::rbind,
      numeric_rows
    )
  }

  base::message("Normalizing requested ZIP pairs.")

  requested_pairs <- zip_pairs |>
    dplyr::transmute(
      origin_zip = normalize_zip5(.data[[origin_zip_col]]),
      destination_zip = normalize_zip5(.data[[destination_zip_col]])
    ) |>
    dplyr::filter(
      !base::is.na(.data$origin_zip),
      !base::is.na(.data$destination_zip)
    ) |>
    dplyr::distinct()

  base::message(
    "Unique requested ZIP pairs: ",
    base::format(base::nrow(requested_pairs), big.mark = ",")
  )

  if (base::nrow(requested_pairs) == 0L) {
    base::stop("No valid ZIP-to-ZIP pairs remain after normalization.")
  }

  base::message("Preparing ZIP-coordinate reference.")

  centroid_reference <- zip_centroids |>
    dplyr::transmute(
      zip5 = normalize_zip5(.data[[centroid_zip_col]]),
      latitude = base::as.numeric(.data[[latitude_col]]),
      longitude = base::as.numeric(.data[[longitude_col]])
    ) |>
    dplyr::filter(!base::is.na(.data$zip5)) |>
    dplyr::distinct(.data$zip5, .keep_all = TRUE)

  duplicate_zip_count <- zip_centroids |>
    dplyr::transmute(zip5 = normalize_zip5(.data[[centroid_zip_col]])) |>
    dplyr::filter(!base::is.na(.data$zip5)) |>
    dplyr::count(.data$zip5) |>
    dplyr::filter(.data$n > 1L) |>
    base::nrow()

  base::message(
    "ZIPs with duplicate centroid rows before deduplication: ",
    duplicate_zip_count
  )

  origins <- requested_pairs |>
    dplyr::distinct(.data$origin_zip) |>
    dplyr::left_join(centroid_reference, by = c("origin_zip" = "zip5")) |>
    dplyr::rename(
      origin_lat = .data$latitude,
      origin_lon = .data$longitude
    )

  destinations <- requested_pairs |>
    dplyr::distinct(.data$destination_zip) |>
    dplyr::left_join(centroid_reference, by = c("destination_zip" = "zip5")) |>
    dplyr::rename(
      destination_lat = .data$latitude,
      destination_lon = .data$longitude
    )

  missing_origins <- origins |>
    dplyr::filter(base::is.na(.data$origin_lat) | base::is.na(.data$origin_lon))

  missing_destinations <- destinations |>
    dplyr::filter(base::is.na(.data$destination_lat) | base::is.na(.data$destination_lon))

  if (base::nrow(missing_origins) > 0L) {
    base::message("Missing origin coordinates: ", base::nrow(missing_origins))
  }

  if (base::nrow(missing_destinations) > 0L) {
    base::message("Missing destination coordinates: ", base::nrow(missing_destinations))
  }

  origins_routeable <- origins |>
    dplyr::filter(base::is.finite(.data$origin_lat), base::is.finite(.data$origin_lon)) |>
    dplyr::arrange(.data$origin_zip)

  destinations_routeable <- destinations |>
    dplyr::filter(base::is.finite(.data$destination_lat), base::is.finite(.data$destination_lon)) |>
    dplyr::arrange(.data$destination_zip)

  base::message("Routeable origin ZIPs: ", base::format(base::nrow(origins_routeable), big.mark = ","))
  base::message("Routeable destination ZIPs: ", base::format(base::nrow(destinations_routeable), big.mark = ","))

  if (base::nrow(origins_routeable) == 0L || base::nrow(destinations_routeable) == 0L) {
    base::stop("No routeable origin/destination coordinates remain.")
  }

  origin_chunks <- split_chunks(
    base::seq_len(base::nrow(origins_routeable)),
    base::as.integer(origin_chunk_size)
  )

  destination_chunks <- split_chunks(
    base::seq_len(base::nrow(destinations_routeable)),
    base::as.integer(destination_chunk_size)
  )

  n_requests <- base::length(origin_chunks) * base::length(destination_chunks)
  base::message("Valhalla matrix requests required: ", base::format(n_requests, big.mark = ","))

  matrix_blocks <- base::list()
  block_number <- 0L

  for (origin_index in base::seq_along(origin_chunks)) {
    source_block <- origins_routeable[origin_chunks[[origin_index]], , drop = FALSE]

    for (destination_index in base::seq_along(destination_chunks)) {
      target_block <- destinations_routeable[destination_chunks[[destination_index]], , drop = FALSE]
      block_number <- block_number + 1L

      base::message(
        "Routing matrix block ", block_number, " of ", n_requests, ": ",
        base::nrow(source_block), " origins x ", base::nrow(target_block), " destinations."
      )

      source_locations <- base::lapply(
        base::seq_len(base::nrow(source_block)),
        function(index) {
          base::list(
            lat = source_block$origin_lat[[index]],
            lon = source_block$origin_lon[[index]]
          )
        }
      )

      target_locations <- base::lapply(
        base::seq_len(base::nrow(target_block)),
        function(index) {
          base::list(
            lat = target_block$destination_lat[[index]],
            lon = target_block$destination_lon[[index]]
          )
        }
      )

      request_body <- base::list(
        sources = source_locations,
        targets = target_locations,
        costing = costing,
        units = "miles",
        verbose = FALSE
      )

      request_url <- base::paste0(
        base::sub("/+$", "", valhalla_url),
        "/sources_to_targets"
      )

      valhalla_response <- base::tryCatch(
        {
          httr2::request(request_url) |>
            httr2::req_headers(`Content-Type` = "application/json") |>
            httr2::req_body_json(request_body, auto_unbox = TRUE) |>
            httr2::req_timeout(120) |>
            httr2::req_retry(
              max_tries = 3L,
              backoff = function(tries) 2 ^ tries
            ) |>
            httr2::req_perform()
        },
        error = function(error_condition) {
          base::stop(
            "Valhalla request failed for block ", block_number, ": ",
            base::conditionMessage(error_condition)
          )
        }
      )

      response_body <- httr2::resp_body_json(valhalla_response, simplifyVector = FALSE)

      if (base::is.null(response_body$sources_to_targets)) {
        base::stop("Valhalla response does not contain `sources_to_targets`.")
      }

      duration_rows <- response_body$sources_to_targets$durations
      distance_rows <- response_body$sources_to_targets$distances

      duration_matrix <- json_rows_to_matrix(
        duration_rows,
        expected_rows = base::nrow(source_block),
        expected_cols = base::nrow(target_block)
      )

      distance_matrix <- json_rows_to_matrix(
        distance_rows,
        expected_rows = base::nrow(source_block),
        expected_cols = base::nrow(target_block)
      )

      block_routes <- tibble::tibble(
        source_index = base::rep(base::seq_len(base::nrow(source_block)), each = base::nrow(target_block)),
        target_index = base::rep(base::seq_len(base::nrow(target_block)), times = base::nrow(source_block)),
        duration_sec = base::as.numeric(base::t(duration_matrix)),
        distance_miles = base::as.numeric(base::t(distance_matrix))
      ) |>
        dplyr::mutate(
          origin_zip = source_block$origin_zip[.data$source_index],
          destination_zip = target_block$destination_zip[.data$target_index]
        ) |>
        dplyr::select(
          .data$origin_zip,
          .data$destination_zip,
          .data$duration_sec,
          .data$distance_miles
        )

      matrix_blocks[[block_number]] <- block_routes
    }
  }

  base::message("Combining Valhalla matrix blocks.")

  routed_matrix <- dplyr::bind_rows(matrix_blocks) |>
    dplyr::distinct(.data$origin_zip, .data$destination_zip, .keep_all = TRUE) |>
    dplyr::mutate(
      drive_minutes = .data$duration_sec / 60,
      drive_miles = .data$distance_miles,
      route_status = dplyr::if_else(
        base::is.finite(.data$drive_minutes),
        "routed",
        "unreachable"
      ),
      drive_time_band = dplyr::case_when(
        !base::is.finite(.data$drive_minutes) ~ "unreachable",
        .data$drive_minutes <= 30            ~ "00-30",
        .data$drive_minutes <= 60            ~ "31-60",
        .data$drive_minutes <= 120           ~ "61-120",
        .data$drive_minutes <= 180           ~ "121-180",
        TRUE                                 ~ ">180"
      ),
      routing_engine = "Valhalla",
      costing = costing
    )

  base::message("Restricting matrix to requested ZIP pairs.")

  routed_pairs <- requested_pairs |>
    dplyr::left_join(
      routed_matrix,
      by = c("origin_zip", "destination_zip")
    ) |>
    dplyr::mutate(
      route_status = dplyr::case_when(
        base::is.na(.data$route_status) ~ "missing_coordinate_or_route",
        TRUE ~ .data$route_status
      )
    )

  n_routed <- routed_pairs |>
    dplyr::filter(.data$route_status == "routed") |>
    base::nrow()

  n_unrouted <- base::nrow(routed_pairs) - n_routed
  routed_pct <- 100 * n_routed / base::nrow(routed_pairs)

  base::message("Requested pairs: ", base::format(base::nrow(routed_pairs), big.mark = ","))
  base::message("Successfully routed: ", base::format(n_routed, big.mark = ","), " (", base::sprintf("%.1f%%", routed_pct), ").")
  base::message("Not routed: ", base::format(n_unrouted, big.mark = ","))

  if (n_routed > 0L) {
    drive_median <- stats::median(routed_pairs$drive_minutes, na.rm = TRUE)
    drive_p25 <- base::unname(stats::quantile(routed_pairs$drive_minutes, probs = 0.25, na.rm = TRUE))
    drive_p75 <- base::unname(stats::quantile(routed_pairs$drive_minutes, probs = 0.75, na.rm = TRUE))
    drive_mean <- base::mean(routed_pairs$drive_minutes, na.rm = TRUE)
    drive_sd <- stats::sd(routed_pairs$drive_minutes, na.rm = TRUE)

    base::message("Drive time mean (SD): ", base::sprintf("%.1f (%.1f) min", drive_mean, drive_sd))
    base::message("Drive time median (p25, p75): ", base::sprintf("%.1f (%.1f, %.1f) min", drive_median, drive_p25, drive_p75))
  }

  saved_path <- NA_character_

  if (base::isTRUE(save_file)) {
    base::dir.create(save_dir, recursive = TRUE, showWarnings = FALSE)
    timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
    saved_path <- base::file.path(save_dir, base::paste0("valhalla_zip_drive_times_", timestamp, ".csv"))

    base::message("Saving routed ZIP pairs.")
    readr::write_csv(routed_pairs, saved_path)
    base::message("Saved file: ", base::normalizePath(saved_path, mustWork = TRUE))
  }

  base::message("valhalla_zip_drive_time(): complete.")
  base::attr(routed_pairs, "saved_path") <- saved_path
  routed_pairs
}

#' Build empirical inpatient surgical travel kernel from CHIA patient-to-hospital routes
#'
#' Estimates empirical drive-time band shares and distance-decay weights for
#' major inpatient pelvic reconstructive surgery.
#'
#' @param routed_pairs Output table from [valhalla_zip_drive_time()] with `drive_minutes`.
#' @param save_dir Directory for saving the empirical travel kernel artifact.
#'
#' @return A list containing `band_shares` (empirical shares), `decay_weights`
#'   (normalized decay weights for E2SFCA), and output `saved_path`.
#'
#' @family geography chia
#' @concept geography
#' @export
build_chia_surgical_travel_kernel <- function(
    routed_pairs,
    save_dir = "artifacts/chia_travel") {

  base::message("build_chia_surgical_travel_kernel(): starting.")

  timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
  base::dir.create(save_dir, recursive = TRUE, showWarnings = FALSE)

  valid_routes <- routed_pairs |>
    dplyr::filter(is.finite(drive_minutes), drive_minutes >= 0)

  if (nrow(valid_routes) == 0) {
    base::warning("No valid routed pairs provided. Returning default decay weights.")
    shares <- tibble::tibble(
      drive_time_band = c("00-30", "31-60", "61-120", "121-180", ">180"),
      count = c(50, 30, 15, 4, 1),
      share = c(0.50, 0.30, 0.15, 0.04, 0.01),
      decay_weight = c(1.00, 0.60, 0.30, 0.08, 0.00)
    )
  } else {
    shares <- valid_routes |>
      dplyr::mutate(
        drive_time_band = dplyr::case_when(
          drive_minutes <= 30  ~ "00-30",
          drive_minutes <= 60  ~ "31-60",
          drive_minutes <= 120 ~ "61-120",
          drive_minutes <= 180 ~ "121-180",
          TRUE                 ~ ">180"
        )
      ) |>
      dplyr::count(drive_time_band, name = "count") |>
      dplyr::mutate(
        share = count / sum(count),
        decay_weight = round(share / max(share), 4)
      )
  }

  weights_vec <- stats::setNames(shares$decay_weight, shares$drive_time_band)

  saved_path <- base::file.path(save_dir, paste0("chia_urps_inpatient_travel_weights_", timestamp, ".csv"))
  readr::write_csv(shares, saved_path)
  base::message("Saved empirical travel weights artifact: ", saved_path)

  list(
    band_shares = shares,
    decay_weights = weights_vec,
    saved_path = saved_path
  )
}

