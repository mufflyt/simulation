#' Deconstruct surgical workload and model APP delegation
#'
#' Uses CMS global-package percentages to allocate total work RVUs for
#' accounting and CMS physician-time assumptions to estimate workload. The
#' RVU allocation is not a claim that CMS publishes component-specific work
#' RVUs. Initial intake is modeled separately from the surgical global package.
#'
#' @param cpt_volume One row per CPT/HCPCS code and simulation period.
#' @param pfs_reference CMS Physician Fee Schedule reference table.
#' @param delegation_policy Delegation assumptions by workload phase.
#' @param capacity_parameters Named list of annual surgeon capacity inputs.
#' @param save_directory Optional directory for timestamped CSV files.
#'
#' @return A named list containing component, summary, and capacity tibbles.
#' @family workload decomposition
#' @concept demand
#' @export
deconstruct_workload_rvus <- function(
    cpt_volume,
    pfs_reference,
    delegation_policy = tibble::tribble(
      ~phase, ~app_share, ~surgeon_rework_share,
      "initial_intake", 0.80, 0.10,
      "pre_service", 0.50, 0.15,
      "intra_service", 0.00, 0.00,
      "post_service", 0.90, 0.10
    ),
    capacity_parameters = list(
      annual_surgeon_or_minutes = 92e3,
      annual_surgeon_clinic_minutes = 86e3,
      intake_minutes_per_case = 30,
      app_time_multiplier = 1.00
    ),
    save_directory = NULL) {
  base::message("deconstruct_workload_rvus(): starting")
  base::message(
    "CPT volume rows: ",
    scales::comma(base::nrow(cpt_volume))
  )
  base::message(
    "PFS reference rows: ",
    scales::comma(base::nrow(pfs_reference))
  )

  if (!base::is.data.frame(cpt_volume)) {
    base::stop("`cpt_volume` must be a data frame.")
  }
  if (!base::is.data.frame(pfs_reference)) {
    base::stop("`pfs_reference` must be a data frame.")
  }
  if (!base::is.data.frame(delegation_policy)) {
    base::stop("`delegation_policy` must be a data frame.")
  }

  volume_names <- c("year", "hcpcs", "case_volume")
  pfs_names <- c(
    "year",
    "hcpcs",
    "work_rvu",
    "global_days",
    "pre_op_pct",
    "intra_op_pct",
    "post_op_pct",
    "pre_service_minutes",
    "intra_service_minutes",
    "post_service_minutes"
  )
  policy_names <- c(
    "phase",
    "app_share",
    "surgeon_rework_share"
  )
  check_names <- function(table_object, required_names, object_name) {
    absent_names <- base::setdiff(required_names, names(table_object))
    if (base::length(absent_names) > 0L) {
      base::stop(
        object_name,
        " is missing: ",
        base::paste(absent_names, collapse = ", ")
      )
    }
  }
  check_names(cpt_volume, volume_names, "`cpt_volume`")
  check_names(pfs_reference, pfs_names, "`pfs_reference`")
  check_names(
    delegation_policy,
    policy_names,
    "`delegation_policy`"
  )

  required_phases <- c(
    "initial_intake",
    "pre_service",
    "intra_service",
    "post_service"
  )
  if (!base::setequal(delegation_policy$phase, required_phases)) {
    base::stop(
      "`delegation_policy$phase` must contain each required phase once."
    )
  }
  if (base::anyDuplicated(delegation_policy$phase) > 0L) {
    base::stop("Delegation phases must be unique.")
  }
  if (base::any(
    delegation_policy$app_share < 0 |
      delegation_policy$app_share > 1 |
      delegation_policy$surgeon_rework_share < 0 |
      delegation_policy$surgeon_rework_share > 1
  )) {
    base::stop("Delegation shares must lie between zero and one.")
  }
  intra_share <- delegation_policy |>
    dplyr::filter(.data$phase == "intra_service") |>
    dplyr::pull(.data$app_share)
  if (intra_share != 0) {
    base::stop(
      "Primary-surgeon intra-service time cannot be delegated to an APP."
    )
  }

  percentage_names <- c(
    "pre_op_pct",
    "intra_op_pct",
    "post_op_pct"
  )
  if (base::any(
    !base::is.finite(base::as.matrix(pfs_reference[percentage_names]))
  )) {
    base::stop("Global-package percentages must be finite.")
  }
  if (base::any(
    base::as.matrix(pfs_reference[percentage_names]) < 0 |
      base::as.matrix(pfs_reference[percentage_names]) > 1
  )) {
    base::stop("Global-package percentages must use proportions from 0 to 1.")
  }
  percentage_sum <- base::rowSums(pfs_reference[percentage_names])
  global_rows <- base::as.character(pfs_reference$global_days) %in%
    c("010", "090", "10", "90")
  if (base::any(base::abs(percentage_sum[global_rows] - 1) > 0.02)) {
    base::stop(
      "Pre-, intra-, and post-operative percentages must sum to one ",
      "within rounding tolerance for 10- and 90-day global codes."
    )
  }
  if (base::any(cpt_volume$case_volume < 0, na.rm = TRUE)) {
    base::stop("`case_volume` cannot be negative.")
  }

  key_duplicates <- pfs_reference |>
    dplyr::count(.data$year, .data$hcpcs, name = "key_count") |>
    dplyr::filter(.data$key_count > 1)
  if (base::nrow(key_duplicates) > 0L) {
    base::stop("`pfs_reference` has duplicate year-HCPCS keys.")
  }

  base::message("Joining annual CPT volume to the CMS PFS reference")
  surgery_table <- cpt_volume |>
    dplyr::left_join(
      pfs_reference,
      by = c("year", "hcpcs"),
      relationship = "many-to-one"
    )
  unmatched_codes <- surgery_table |>
    dplyr::filter(base::is.na(.data$work_rvu)) |>
    dplyr::distinct(.data$year, .data$hcpcs)
  if (base::nrow(unmatched_codes) > 0L) {
    base::stop(
      "PFS match failed for ",
      scales::comma(base::nrow(unmatched_codes)),
      " year-HCPCS combinations."
    )
  }

  intake_minutes <- capacity_parameters$intake_minutes_per_case
  app_multiplier <- capacity_parameters$app_time_multiplier
  if (!base::is.numeric(intake_minutes) ||
      base::length(intake_minutes) != 1L ||
      !base::is.finite(intake_minutes) ||
      intake_minutes < 0) {
    base::stop("`intake_minutes_per_case` must be one nonnegative number.")
  }
  if (!base::is.numeric(app_multiplier) ||
      base::length(app_multiplier) != 1L ||
      !base::is.finite(app_multiplier) ||
      app_multiplier <= 0) {
    base::stop("`app_time_multiplier` must be one positive number.")
  }

  base::message("Expanding each surgical code into workload phases")
  component_table <- dplyr::bind_rows(
    surgery_table |>
      dplyr::transmute(
        year = .data$year,
        hcpcs = .data$hcpcs,
        case_volume = .data$case_volume,
        global_days = .data$global_days,
        work_rvu = .data$work_rvu,
        phase = "initial_intake",
        phase_pct = 0,
        minutes_per_case = intake_minutes,
        phase_work_rvu_per_case = 0
      ),
    surgery_table |>
      tidyr::pivot_longer(
        cols = dplyr::all_of(percentage_names),
        names_to = "percentage_field",
        values_to = "phase_pct"
      ) |>
      dplyr::mutate(
        phase = dplyr::recode(
          .data$percentage_field,
          pre_op_pct = "pre_service",
          intra_op_pct = "intra_service",
          post_op_pct = "post_service"
        ),
        minutes_per_case = dplyr::case_when(
          .data$phase == "pre_service" ~ .data$pre_service_minutes,
          .data$phase == "intra_service" ~ .data$intra_service_minutes,
          .data$phase == "post_service" ~ .data$post_service_minutes,
          TRUE ~ NA_real_
        ),
        phase_work_rvu_per_case = .data$work_rvu * .data$phase_pct
      ) |>
      dplyr::select(
        .data$year,
        .data$hcpcs,
        .data$case_volume,
        .data$global_days,
        .data$work_rvu,
        .data$phase,
        .data$phase_pct,
        .data$minutes_per_case,
        .data$phase_work_rvu_per_case
      )
  )
  if (base::any(base::is.na(component_table$minutes_per_case))) {
    base::stop(
      "CMS time fields contain missing values after phase expansion."
    )
  }

  base::message("Applying phase-specific APP delegation assumptions")
  component_table <- component_table |>
    dplyr::left_join(
      delegation_policy,
      by = "phase",
      relationship = "many-to-one"
    ) |>
    dplyr::mutate(
      gross_minutes = .data$case_volume * .data$minutes_per_case,
      delegated_minutes = .data$gross_minutes * .data$app_share,
      surgeon_rework_minutes = .data$delegated_minutes *
        .data$surgeon_rework_share,
      surgeon_minutes_after = .data$gross_minutes -
        .data$delegated_minutes + .data$surgeon_rework_minutes,
      app_minutes_after = .data$delegated_minutes * app_multiplier,
      surgeon_minutes_freed = .data$gross_minutes -
        .data$surgeon_minutes_after,
      total_work_rvus = .data$case_volume *
        .data$phase_work_rvu_per_case,
      rvu_treatment = dplyr::if_else(
        .data$phase == "initial_intake",
        "separate task; no surgical global RVU allocated",
        "accounting allocation using CMS global percentage"
      )
    )

  base::message("Summarizing workload by year and phase")
  workload_summary <- component_table |>
    dplyr::group_by(.data$year, .data$phase) |>
    dplyr::summarise(
      case_volume = base::sum(.data$case_volume),
      total_work_rvus = base::sum(.data$total_work_rvus),
      gross_surgeon_hours = base::sum(.data$gross_minutes) / 60,
      surgeon_hours_after = base::sum(
        .data$surgeon_minutes_after
      ) / 60,
      app_hours_after = base::sum(.data$app_minutes_after) / 60,
      surgeon_hours_freed = base::sum(
        .data$surgeon_minutes_freed
      ) / 60,
      .groups = "drop"
    )

  required_capacity <- c(
    "annual_surgeon_or_minutes",
    "annual_surgeon_clinic_minutes"
  )
  absent_capacity <- base::setdiff(
    required_capacity,
    names(capacity_parameters)
  )
  if (base::length(absent_capacity) > 0L) {
    base::stop(
      "`capacity_parameters` is missing: ",
      base::paste(absent_capacity, collapse = ", ")
    )
  }
  or_capacity <- capacity_parameters$annual_surgeon_or_minutes
  clinic_capacity <- capacity_parameters$annual_surgeon_clinic_minutes
  if (base::any(!base::is.finite(c(or_capacity, clinic_capacity))) ||
      base::any(c(or_capacity, clinic_capacity) <= 0)) {
    base::stop("Annual OR and clinic capacity must be positive and finite.")
  }

  base::message("Calculating OR- and clinic-constrained surgical throughput")
  capacity_summary <- component_table |>
    dplyr::group_by(.data$year) |>
    dplyr::summarise(
      observed_cases = base::sum(
        dplyr::if_else(
          .data$phase == "intra_service",
          .data$case_volume,
          0
        )
      ),
      weighted_intra_minutes = stats::weighted.mean(
        .data$minutes_per_case[.data$phase == "intra_service"],
        w = .data$case_volume[.data$phase == "intra_service"]
      ),
      weighted_clinic_minutes_before = base::sum(
        .data$gross_minutes[.data$phase != "intra_service"]
      ) / base::sum(
        .data$case_volume[.data$phase == "intra_service"]
      ),
      weighted_clinic_minutes_after = base::sum(
        .data$surgeon_minutes_after[
          .data$phase != "intra_service"
        ]
      ) / base::sum(
        .data$case_volume[.data$phase == "intra_service"]
      ),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      or_limited_cases = base::floor(
        or_capacity / .data$weighted_intra_minutes
      ),
      clinic_limited_cases_before = base::floor(
        clinic_capacity / .data$weighted_clinic_minutes_before
      ),
      clinic_limited_cases_after = base::floor(
        clinic_capacity / .data$weighted_clinic_minutes_after
      ),
      baseline_capacity = base::pmin(
        .data$or_limited_cases,
        .data$clinic_limited_cases_before
      ),
      delegated_capacity = base::pmin(
        .data$or_limited_cases,
        .data$clinic_limited_cases_after
      ),
      additional_case_capacity = base::pmax(
        .data$delegated_capacity - .data$baseline_capacity,
        0
      ),
      capacity_change_pct = dplyr::if_else(
        .data$baseline_capacity > 0,
        .data$additional_case_capacity / .data$baseline_capacity,
        NA_real_
      ),
      binding_constraint_after = dplyr::if_else(
        .data$or_limited_cases <= .data$clinic_limited_cases_after,
        "operating_room",
        "clinic"
      )
    )

  year_range <- base::range(component_table$year, na.rm = TRUE)
  total_freed_hours <- base::sum(
    component_table$surgeon_minutes_freed,
    na.rm = TRUE
  ) / 60
  total_added_cases <- base::sum(
    capacity_summary$additional_case_capacity,
    na.rm = TRUE
  )
  direction_text <- if (total_added_cases > 0) {
    "increased"
  } else {
    "did not increase"
  }
  summary_sentence <- base::paste0(
    "From ",
    year_range[[1]],
    " through ",
    year_range[[2]],
    ", modeled APP delegation freed ",
    scales::comma(total_freed_hours, accuracy = 0.1),
    " surgeon hours and ",
    direction_text,
    " annual surgical capacity by a summed ",
    scales::comma(total_added_cases),
    " cases across modeled years; no p-value applies because this is a ",
    "deterministic capacity scenario."
  )
  base::message(summary_sentence)

  saved_files <- character(0)
  if (!base::is.null(save_directory)) {
    if (!base::dir.exists(save_directory)) {
      base::dir.create(save_directory, recursive = TRUE)
    }
    timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
    component_path <- base::file.path(
      save_directory,
      base::paste0("workload_components_", timestamp, ".csv")
    )
    summary_path <- base::file.path(
      save_directory,
      base::paste0("workload_summary_", timestamp, ".csv")
    )
    capacity_path <- base::file.path(
      save_directory,
      base::paste0("workload_capacity_", timestamp, ".csv")
    )
    readr::write_csv(component_table, component_path)
    readr::write_csv(workload_summary, summary_path)
    readr::write_csv(capacity_summary, capacity_path)
    saved_files <- c(component_path, summary_path, capacity_path)
    base::message(
      "Saved files: ",
      base::paste(saved_files, collapse = "; ")
    )
  }

  base::message("deconstruct_workload_rvus(): complete")
  list(
    components = component_table,
    workload_summary = workload_summary,
    capacity_summary = capacity_summary,
    summary_sentence = summary_sentence,
    saved_files = saved_files,
    interpretation = base::paste(
      "RVUs are allocated for accounting with CMS global percentages.",
      "Capacity changes arise from modeled surgeon minutes, not new RVUs."
    )
  )
}
