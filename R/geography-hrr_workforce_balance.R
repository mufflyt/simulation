# HRR workforce supply-demand balance -----------------------------------
#
# Note: HRSA HWSM models demand at the county level and aggregates to state/national.
# This module implements an HRSA-inspired subnational balance analysis using the 306
# Dartmouth Hospital Referral Regions (HRRs).

URPS_EXPECTED_HRR_N <- 306L
URPS_HRR_SHORTAGE_THRESHOLD <- 0.20


#' Validate a Hospital Referral Region reference table
#'
#' @param hrr_reference_tbl One row per HRR with `hrr_code` and `hrr_name`.
#' @param expected_hrr_n Expected number of HRRs.
#'
#' @return Validated HRR reference table.
#' @family provider geography
#' @concept geography
#' @export
validate_hrr_reference <- function(
    hrr_reference_tbl,
    expected_hrr_n = URPS_EXPECTED_HRR_N) {

  if (!base::is.data.frame(hrr_reference_tbl)) {
    base::stop(
      "`hrr_reference_tbl` must be a data frame.",
      call. = FALSE
    )
  }

  required_cols <- c(
    "hrr_code",
    "hrr_name"
  )

  missing_cols <- base::setdiff(
    required_cols,
    base::names(hrr_reference_tbl)
  )

  if (base::length(missing_cols) > 0L) {
    base::stop(
      "HRR reference is missing: ",
      base::paste(
        missing_cols,
        collapse = ", "
      ),
      call. = FALSE
    )
  }

  reference_tbl <- hrr_reference_tbl |>
    dplyr::transmute(
      hrr_code = base::as.character(
        .data$hrr_code
      ),
      hrr_name = base::as.character(
        .data$hrr_name
      )
    ) |>
    dplyr::distinct()

  duplicate_tbl <- reference_tbl |>
    dplyr::count(
      .data$hrr_code,
      name = "row_n"
    ) |>
    dplyr::filter(
      .data$row_n > 1L
    )

  if (base::nrow(duplicate_tbl) > 0L) {
    base::stop(
      "HRR reference contains duplicated HRR codes.",
      call. = FALSE
    )
  }

  if (base::any(
    base::is.na(reference_tbl$hrr_code) |
      reference_tbl$hrr_code == ""
  )) {
    base::stop(
      "HRR codes may not be missing.",
      call. = FALSE
    )
  }

  if (!base::is.null(expected_hrr_n)) {
    if (base::nrow(reference_tbl) !=
        base::as.integer(expected_hrr_n)) {
      base::stop(
        "Expected ",
        expected_hrr_n,
        " HRRs but found ",
        base::nrow(reference_tbl),
        ".",
        call. = FALSE
      )
    }
  }

  base::message(
    "[hrr-balance] HRR reference validated: ",
    base::format(
      base::nrow(reference_tbl),
      big.mark = ","
    ),
    " regions."
  )

  reference_tbl
}


#' Aggregate provider FTE to HRR-year
#'
#' @param provider_roster Provider-level table.
#' @param fte_col Provider FTE column.
#' @param year_col Optional year column.
#'
#' @return HRR-year supply table.
#' @keywords internal
.aggregate_hrr_supply <- function(
    provider_roster,
    fte_col,
    year_col) {

  required_cols <- c(
    "hrr_code",
    fte_col
  )

  if (!base::is.null(year_col)) {
    required_cols <- c(
      required_cols,
      year_col
    )
  }

  missing_cols <- base::setdiff(
    required_cols,
    base::names(provider_roster)
  )

  if (base::length(missing_cols) > 0L) {
    base::stop(
      "Provider roster is missing: ",
      base::paste(
        missing_cols,
        collapse = ", "
      ),
      call. = FALSE
    )
  }

  supply_tbl <- provider_roster |>
    dplyr::transmute(
      hrr_code = base::as.character(
        .data$hrr_code
      ),
      year = if (base::is.null(year_col)) {
        0L
      } else {
        base::as.integer(
          .data[[year_col]]
        )
      },
      provider_id = if (
        "provider_id" %in% base::names(provider_roster)
      ) {
        base::as.character(
          .data$provider_id
        )
      } else {
        base::as.character(
          base::seq_len(
            base::nrow(provider_roster)
          )
        )
      },
      supply_fte = base::as.numeric(
        .data[[fte_col]]
      )
    )

  if (base::any(
    supply_tbl$supply_fte < 0,
    na.rm = TRUE
  )) {
    base::stop(
      "Provider FTE cannot be negative.",
      call. = FALSE
    )
  }

  supply_tbl |>
    dplyr::filter(
      !base::is.na(.data$hrr_code)
    ) |>
    dplyr::group_by(
      .data$year,
      .data$hrr_code
    ) |>
    dplyr::summarise(
      supply_fte = base::sum(
        .data$supply_fte,
        na.rm = TRUE
      ),
      provider_headcount =
        dplyr::n_distinct(
          .data$provider_id
        ),
      .groups = "drop"
    )
}


#' Aggregate modeled workforce demand to HRR-year
#'
#' @param demand_tbl Demand table containing HRR and demand FTE.
#' @param demand_col Demand FTE column.
#' @param year_col Optional year column.
#'
#' @return HRR-year demand table.
#' @keywords internal
.aggregate_hrr_demand <- function(
    demand_tbl,
    demand_col,
    year_col) {

  required_cols <- c(
    "hrr_code",
    demand_col
  )

  if (!base::is.null(year_col)) {
    required_cols <- c(
      required_cols,
      year_col
    )
  }

  missing_cols <- base::setdiff(
    required_cols,
    base::names(demand_tbl)
  )

  if (base::length(missing_cols) > 0L) {
    base::stop(
      "Demand table is missing: ",
      base::paste(
        missing_cols,
        collapse = ", "
      ),
      call. = FALSE
    )
  }

  demand_work_tbl <- demand_tbl |>
    dplyr::transmute(
      hrr_code = base::as.character(
        .data$hrr_code
      ),
      year = if (base::is.null(year_col)) {
        0L
      } else {
        base::as.integer(
          .data[[year_col]]
        )
      },
      demand_fte = base::as.numeric(
        .data[[demand_col]]
      )
    )

  if (base::any(
    demand_work_tbl$demand_fte < 0,
    na.rm = TRUE
  )) {
    base::stop(
      "Demand FTE cannot be negative.",
      call. = FALSE
    )
  }

  demand_work_tbl |>
    dplyr::filter(
      !base::is.na(.data$hrr_code)
    ) |>
    dplyr::group_by(
      .data$year,
      .data$hrr_code
    ) |>
    dplyr::summarise(
      demand_fte = base::sum(
        .data$demand_fte,
        na.rm = TRUE
      ),
      .groups = "drop"
    )
}


#' Aggregate workforce balance across Hospital Referral Regions
#'
#' @description
#' Implements a five-step HRR spatial accounting framework:
#'
#' 1. Validate the complete HRR geography.
#' 2. Aggregate active provider FTE to HRR-year.
#' 3. Aggregate modeled demand FTE to HRR-year.
#' 4. Complete every HRR-year combination and reconcile national totals.
#' 5. Calculate local adequacy, deficit, and shortage classifications.
#'
#' A shortage of 20 percent means:
#'
#' `supply_fte < 0.80 * demand_fte`
#'
#' or equivalently:
#'
#' `deficit_fraction >= 0.20`.
#'
#' @param provider_roster Provider-level workforce table.
#' @param hrr_demand_tbl Demand table.
#' @param hrr_reference_tbl Complete HRR reference.
#' @param provider_fte_col Provider FTE column.
#' @param demand_fte_col Demand FTE column.
#' @param provider_year_col Optional provider year column.
#' @param demand_year_col Optional demand year column.
#' @param shortage_threshold Fractional shortage threshold.
#' @param expected_hrr_n Expected HRR count.
#'
#' @return HRR-year workforce balance table.
#' @family provider geography
#' @concept geography
#' @export
aggregate_hrr_workforce_balance <- function(
    provider_roster,
    hrr_demand_tbl,
    hrr_reference_tbl,
    provider_fte_col = "fte",
    demand_fte_col = "demand_fte",
    provider_year_col = NULL,
    demand_year_col = NULL,
    shortage_threshold =
      URPS_HRR_SHORTAGE_THRESHOLD,
    expected_hrr_n =
      URPS_EXPECTED_HRR_N) {

  if (!base::is.numeric(shortage_threshold) ||
      base::length(shortage_threshold) != 1L ||
      !base::is.finite(shortage_threshold) ||
      shortage_threshold < 0 ||
      shortage_threshold >= 1) {
    base::stop(
      "`shortage_threshold` must be in [0, 1).",
      call. = FALSE
    )
  }

  base::message(
    "[hrr-balance] Starting HRR workforce balance."
  )

  # Step 1: complete geographic universe.
  reference_tbl <- validate_hrr_reference(
    hrr_reference_tbl = hrr_reference_tbl,
    expected_hrr_n = expected_hrr_n
  )

  valid_hrr <- reference_tbl$hrr_code

  provider_bad <- base::setdiff(
    base::unique(
      base::as.character(
        provider_roster$hrr_code
      )
    ),
    valid_hrr
  )

  provider_bad <- provider_bad[
    !base::is.na(provider_bad)
  ]

  if (base::length(provider_bad) > 0L) {
    base::stop(
      "Provider roster contains unknown HRR code(s): ",
      base::paste(
        provider_bad,
        collapse = ", "
      ),
      call. = FALSE
    )
  }

  demand_bad <- base::setdiff(
    base::unique(
      base::as.character(
        hrr_demand_tbl$hrr_code
      )
    ),
    valid_hrr
  )

  demand_bad <- demand_bad[
    !base::is.na(demand_bad)
  ]

  if (base::length(demand_bad) > 0L) {
    base::stop(
      "Demand table contains unknown HRR code(s): ",
      base::paste(
        demand_bad,
        collapse = ", "
      ),
      call. = FALSE
    )
  }

  # Step 2: provider FTE.
  supply_tbl <- .aggregate_hrr_supply(
    provider_roster = provider_roster,
    fte_col = provider_fte_col,
    year_col = provider_year_col
  )

  # Step 3: demand FTE.
  demand_tbl_agg <- .aggregate_hrr_demand(
    demand_tbl = hrr_demand_tbl,
    demand_col = demand_fte_col,
    year_col = demand_year_col
  )

  supply_years <- base::unique(
    supply_tbl$year
  )

  demand_years <- base::unique(
    demand_tbl_agg$year
  )

  years <- base::sort(
    base::union(
      supply_years,
      demand_years
    )
  )

  if (base::length(years) == 0L) {
    years <- 0L
  }

  # Step 4: explicitly complete every HRR-year.
  hrr_year_grid <- tidyr::crossing(
    year = years,
    reference_tbl
  )

  balance_tbl <- hrr_year_grid |>
    dplyr::left_join(
      supply_tbl,
      by = c(
        "year",
        "hrr_code"
      )
    ) |>
    dplyr::left_join(
      demand_tbl_agg,
      by = c(
        "year",
        "hrr_code"
      )
    ) |>
    dplyr::mutate(
      supply_fte = dplyr::coalesce(
        .data$supply_fte,
        0
      ),
      provider_headcount =
        dplyr::coalesce(
          .data$provider_headcount,
          0L
        ),
      demand_fte = dplyr::coalesce(
        .data$demand_fte,
        0
      )
    )

  expected_supply <- base::sum(
    supply_tbl$supply_fte,
    na.rm = TRUE
  )

  observed_supply <- base::sum(
    balance_tbl$supply_fte,
    na.rm = TRUE
  )

  expected_demand <- base::sum(
    demand_tbl_agg$demand_fte,
    na.rm = TRUE
  )

  observed_demand <- base::sum(
    balance_tbl$demand_fte,
    na.rm = TRUE
  )

  if (!base::isTRUE(
    base::all.equal(
      expected_supply,
      observed_supply,
      tolerance = 1e-10
    )
  )) {
    base::stop(
      "HRR aggregation failed the national supply reconciliation.",
      call. = FALSE
    )
  }

  if (!base::isTRUE(
    base::all.equal(
      expected_demand,
      observed_demand,
      tolerance = 1e-10
    )
  )) {
    base::stop(
      "HRR aggregation failed the national demand reconciliation.",
      call. = FALSE
    )
  }

  # Step 5: adequacy and shortage classification.
  balance_tbl <- balance_tbl |>
    dplyr::mutate(
      gap_fte =
        .data$supply_fte -
        .data$demand_fte,

      deficit_fte =
        base::pmax(
          .data$demand_fte -
            .data$supply_fte,
          0
        ),

      surplus_fte =
        base::pmax(
          .data$supply_fte -
            .data$demand_fte,
          0
        ),

      adequacy_ratio = dplyr::case_when(
        .data$demand_fte > 0 ~
          .data$supply_fte /
          .data$demand_fte,

        .data$demand_fte == 0 &
          .data$supply_fte == 0 ~
          NA_real_,

        .data$demand_fte == 0 &
          .data$supply_fte > 0 ~
          Inf
      ),

      deficit_fraction = dplyr::case_when(
        .data$demand_fte > 0 ~
          .data$deficit_fte /
          .data$demand_fte,

        TRUE ~
          0
      ),

      shortage_20pct =
        .data$demand_fte > 0 &
        .data$deficit_fraction >=
          shortage_threshold,

      shortage_severity = dplyr::case_when(
        .data$demand_fte <= 0 ~
          "no_demand",

        .data$deficit_fraction < 0.10 ~
          "adequate",

        .data$deficit_fraction < 0.20 ~
          "mild_shortage",

        .data$deficit_fraction < 0.40 ~
          "moderate_shortage",

        .data$deficit_fraction < 0.60 ~
          "severe_shortage",

        TRUE ~
          "critical_shortage"
      ),

      shortage_severity = base::factor(
        .data$shortage_severity,
        levels = c(
          "no_demand",
          "adequate",
          "mild_shortage",
          "moderate_shortage",
          "severe_shortage",
          "critical_shortage"
        ),
        ordered = TRUE
      )
    )

  shortage_n <- base::sum(
    balance_tbl$shortage_20pct,
    na.rm = TRUE
  )

  base::message(
    "[hrr-balance] HRR-year cells evaluated: ",
    base::format(
      base::nrow(balance_tbl),
      big.mark = ","
    )
  )

  base::message(
    "[hrr-balance] Cells with >= ",
    scales::percent(
      shortage_threshold,
      accuracy = 1
    ),
    " deficit: ",
    base::format(
      shortage_n,
      big.mark = ","
    )
  )

  balance_tbl
}


#' Summarize HRR workforce imbalance
#'
#' @param hrr_balance_tbl Output from
#'   [aggregate_hrr_workforce_balance()].
#'
#' @return One row per simulation year.
#' @family provider geography
#' @concept geography
#' @export
summarize_hrr_workforce_balance <- function(
    hrr_balance_tbl) {

  required_cols <- c(
    "year",
    "supply_fte",
    "demand_fte",
    "deficit_fte",
    "surplus_fte",
    "shortage_20pct"
  )

  missing_cols <- base::setdiff(
    required_cols,
    base::names(hrr_balance_tbl)
  )

  if (base::length(missing_cols) > 0L) {
    base::stop(
      "HRR balance table is missing: ",
      base::paste(
        missing_cols,
        collapse = ", "
      ),
      call. = FALSE
    )
  }

  hrr_balance_tbl |>
    dplyr::group_by(
      .data$year
    ) |>
    dplyr::summarise(
      hrr_n = dplyr::n(),

      supply_fte = base::sum(
        .data$supply_fte,
        na.rm = TRUE
      ),

      demand_fte = base::sum(
        .data$demand_fte,
        na.rm = TRUE
      ),

      national_gap_fte =
        .data$supply_fte -
        .data$demand_fte,

      geographic_deficit_fte =
        base::sum(
          .data$deficit_fte,
          na.rm = TRUE
        ),

      geographic_surplus_fte =
        base::sum(
          .data$surplus_fte,
          na.rm = TRUE
        ),

      shortage_hrr_n = base::sum(
        .data$shortage_20pct,
        na.rm = TRUE
      ),

      shortage_hrr_pct =
        .data$shortage_hrr_n /
        .data$hrr_n,

      .groups = "drop"
    )
}
