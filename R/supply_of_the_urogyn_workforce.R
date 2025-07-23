# SUPPLY of UROGYN WORKFORCE
# The main function coordinates these helper functions:
#
# process_census_population_data() - Reads and processes Excel files
# calculate_workforce_demand() - Converts population to FTE demand
# simulate_workforce_scenarios() - Creates multiple supply scenarios
# calculate_workforce_gaps() - Computes supply-demand gaps
# create_workforce_visualizations() - Generates publication-ready plots
# generate_summary_statistics() - Creates key metrics summary

# General Functions ----
#' Analyze Urogynecology Workforce Supply and Demand
#'
#' This function performs a comprehensive analysis of urogynecology workforce
#' supply and demand projections, including multiple scenarios for training
#' and retirement rates. It processes census population data, simulates
#' workforce supply under different scenarios, and calculates workforce gaps.
#'
#' @param census_file_path Character. Full path to the Census Excel file
#'   containing population projections (np2023-t2.xlsx format).
#' @param initial_workforce_count Numeric. Starting number of urogynecologists
#'   in the workforce. Default is 1169.
#' @param baseline_new_fellows Numeric. Annual number of new fellows entering
#'   the workforce under status quo conditions. Default is 55.
#' @param baseline_retirements Numeric. Annual number of retirements under
#'   status quo conditions. Default is 55.
#' @param visits_per_woman_annually Numeric. Expected number of urogynecology
#'   visits per woman aged 65+ per year. Default is 1.5.
#' @param hours_per_provider_yearly Numeric. Total working hours per provider
#'   per year. Default is 1728 (36 hours/week * 48 weeks).
#' @param output_directory Character. Directory path where output files should
#'   be saved. Default is current working directory.
#' @param create_visualizations Logical. Whether to generate and save
#'   visualization plots. Default is TRUE.
#' @param apply_demand_smoothing Logical. Whether to apply lowess smoothing
#'   to demand projections to remove unrealistic fluctuations. Default is TRUE.
#' @param smoothing_span Numeric. Span parameter for lowess smoothing
#'   (0-1, higher = more smoothing). Default is 0.3.
#' @param verbose Logical. Whether to enable detailed logging output.
#'   Default is TRUE.
#'
#' @return A list containing:
#'   \item{workforce_projections}{Data frame with supply projections by scenario}
#'   \item{demand_projections}{Data frame with demand projections by year}
#'   \item{workforce_gap_analysis}{Data frame showing supply-demand gaps}
#'   \item{summary_statistics}{List of key summary metrics}
#'
#' @examples
#' # Basic analysis with default parameters (includes smoothing)
#' census_path <- "/path/to/census/np2023-t2.xlsx"
#' workforce_results <- analyze_urogynecology_workforce(
#'   census_file_path = census_path,
#'   initial_workforce_count = 1169,
#'   baseline_new_fellows = 55,
#'   baseline_retirements = 55,
#'   visits_per_woman_annually = 1.5,
#'   hours_per_provider_yearly = 1728,
#'   output_directory = "./workforce_analysis",
#'   create_visualizations = TRUE,
#'   apply_demand_smoothing = TRUE,
#'   smoothing_span = 0.3,
#'   verbose = TRUE
#' )
#'
#' # Analysis with no smoothing to see raw census fluctuations
#' raw_data_results <- analyze_urogynecology_workforce(
#'   census_file_path = census_path,
#'   initial_workforce_count = 1200,
#'   baseline_new_fellows = 65,
#'   baseline_retirements = 50,
#'   visits_per_woman_annually = 1.8,
#'   hours_per_provider_yearly = 1800,
#'   output_directory = "./raw_data_analysis",
#'   create_visualizations = TRUE,
#'   apply_demand_smoothing = FALSE,
#'   smoothing_span = 0.3,
#'   verbose = FALSE
#' )
#'
#' # Conservative analysis with heavy smoothing for long-term planning
#' conservative_results <- analyze_urogynecology_workforce(
#'   census_file_path = census_path,
#'   initial_workforce_count = 1100,
#'   baseline_new_fellows = 50,
#'   baseline_retirements = 60,
#'   visits_per_woman_annually = 1.2,
#'   hours_per_provider_yearly = 1650,
#'   output_directory = "./conservative_analysis",
#'   create_visualizations = FALSE,
#'   apply_demand_smoothing = TRUE,
#'   smoothing_span = 0.5,
#'   verbose = TRUE
#' )
#'
#' @importFrom readxl read_excel
#' @importFrom dplyr filter mutate select pivot_longer bind_rows left_join
#' @importFrom dplyr group_by summarise case_when pull arrange
#' @importFrom tidyr pivot_longer
#' @importFrom tibble tibble
#' @importFrom purrr map_dfr
#' @importFrom ggplot2 ggplot aes geom_line geom_hline labs theme_minimal
#' @importFrom ggplot2 scale_x_continuous annotate ggsave
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_warn log_error log_debug log_threshold
#' @importFrom stats lowess
#' @export
analyze_urogynecology_workforce <- function(census_file_path,
                                            initial_workforce_count = 1169,
                                            baseline_new_fellows = 55,
                                            baseline_retirements = 55,
                                            visits_per_woman_annually = 1.5,
                                            hours_per_provider_yearly = 1728,
                                            output_directory = ".",
                                            create_visualizations = TRUE,
                                            apply_demand_smoothing = TRUE,
                                            smoothing_span = 0.3,
                                            verbose = TRUE) {

  # Configure logging level based on verbose parameter
  if (verbose) {
    logger::log_threshold(logger::DEBUG)
  } else {
    logger::log_threshold(logger::INFO)
  }

  logger::log_info("Starting urogynecology workforce analysis")
  logger::log_debug("Input parameters: census_file_path = {census_file_path}")
  logger::log_debug("initial_workforce_count = {initial_workforce_count}")
  logger::log_debug("baseline_new_fellows = {baseline_new_fellows}")
  logger::log_debug("baseline_retirements = {baseline_retirements}")
  logger::log_debug("visits_per_woman_annually = {visits_per_woman_annually}")
  logger::log_debug("hours_per_provider_yearly = {hours_per_provider_yearly}")
  logger::log_debug("output_directory = {output_directory}")
  logger::log_debug("create_visualizations = {create_visualizations}")
  logger::log_debug("apply_demand_smoothing = {apply_demand_smoothing}")
  logger::log_debug("smoothing_span = {smoothing_span}")

  # Input validation with assertthat
  assertthat::assert_that(is.character(census_file_path),
                          msg = "census_file_path must be a character string")
  assertthat::assert_that(file.exists(census_file_path),
                          msg = "Census file does not exist at specified path")
  assertthat::assert_that(is.numeric(initial_workforce_count),
                          initial_workforce_count > 0,
                          msg = "initial_workforce_count must be positive number")
  assertthat::assert_that(is.numeric(baseline_new_fellows),
                          baseline_new_fellows >= 0,
                          msg = "baseline_new_fellows must be non-negative")
  assertthat::assert_that(is.numeric(baseline_retirements),
                          baseline_retirements >= 0,
                          msg = "baseline_retirements must be non-negative")
  assertthat::assert_that(is.numeric(visits_per_woman_annually),
                          visits_per_woman_annually > 0,
                          msg = "visits_per_woman_annually must be positive")
  assertthat::assert_that(is.numeric(hours_per_provider_yearly),
                          hours_per_provider_yearly > 0,
                          msg = "hours_per_provider_yearly must be positive")
  assertthat::assert_that(is.character(output_directory),
                          msg = "output_directory must be a character string")
  assertthat::assert_that(is.logical(create_visualizations),
                          msg = "create_visualizations must be logical")
  assertthat::assert_that(is.logical(apply_demand_smoothing),
                          msg = "apply_demand_smoothing must be logical")
  assertthat::assert_that(is.numeric(smoothing_span),
                          smoothing_span > 0, smoothing_span <= 1,
                          msg = "smoothing_span must be between 0 and 1")
  assertthat::assert_that(is.logical(verbose),
                          msg = "verbose must be logical")

  logger::log_info("Input validation completed successfully")

  # Create output directory if it doesn't exist
  if (!dir.exists(output_directory)) {
    dir.create(output_directory, recursive = TRUE)
    logger::log_info("Created output directory: {output_directory}")
  }

  # Process census population data
  logger::log_info("Processing census population projections")
  population_projections <- process_census_population_data(
    census_file_path = census_file_path,
    verbose = verbose
  )
  logger::log_info("Census data processing completed: {nrow(population_projections)} years of data")

  # Calculate demand projections
  logger::log_info("Calculating workforce demand projections")
  demand_analysis <- calculate_workforce_demand(
    population_data = population_projections,
    visits_per_woman_annually = visits_per_woman_annually,
    hours_per_provider_yearly = hours_per_provider_yearly,
    apply_smoothing = apply_demand_smoothing,
    smoothing_span = smoothing_span,
    verbose = verbose
  )
  logger::log_info("Demand calculations completed")

  # Generate supply scenarios
  logger::log_info("Simulating workforce supply scenarios")
  supply_projections <- simulate_workforce_scenarios(
    projection_years = population_projections$year_value,
    initial_workforce_count = initial_workforce_count,
    baseline_new_fellows = baseline_new_fellows,
    baseline_retirements = baseline_retirements,
    verbose = verbose
  )
  logger::log_info("Supply scenario simulations completed: {nrow(supply_projections)} projections")

  # Calculate workforce gaps
  logger::log_info("Analyzing workforce supply-demand gaps")
  gap_analysis <- calculate_workforce_gaps(
    supply_data = supply_projections,
    demand_data = demand_analysis,
    verbose = verbose
  )
  logger::log_info("Gap analysis completed")

  # Generate visualizations if requested
  visualization_paths <- NULL
  if (create_visualizations) {
    logger::log_info("Creating workforce analysis visualizations")
    visualization_paths <- create_workforce_visualizations(
      supply_data = supply_projections,
      demand_data = demand_analysis,
      gap_data = gap_analysis,
      output_directory = output_directory,
      verbose = verbose
    )
    logger::log_info("Visualizations saved to: {paste(visualization_paths, collapse = ', ')}")
  }

  # Generate summary statistics
  logger::log_info("Calculating summary statistics")
  summary_stats <- generate_summary_statistics(
    supply_data = supply_projections,
    demand_data = demand_analysis,
    gap_data = gap_analysis,
    verbose = verbose
  )

  # Compile final results
  final_analysis_results <- list(
    workforce_projections = supply_projections,
    demand_projections = demand_analysis,
    workforce_gap_analysis = gap_analysis,
    summary_statistics = summary_stats,
    visualization_file_paths = visualization_paths
  )

  logger::log_info("Workforce analysis completed successfully")
  logger::log_info("Results include {nrow(supply_projections)} supply projections and {nrow(demand_analysis)} demand projections")

  return(final_analysis_results)
}

#' Process Census Population Projection Data
#'
#' @param census_file_path Character path to census Excel file
#' @param verbose Logical for detailed logging
#' @return Data frame with processed population projections
#' @noRd
process_census_population_data <- function(census_file_path, verbose = TRUE) {

  logger::log_debug("Reading census Excel file from: {census_file_path}")

  # Read the census Excel file
  raw_census_data <- readxl::read_excel(
    path = census_file_path,
    sheet = "Main series (thousands)",
    skip = 3
  )

  assertthat::assert_that(is.data.frame(raw_census_data),
                          msg = "Failed to read census data as data frame")

  logger::log_debug("Raw census data dimensions: {nrow(raw_census_data)} x {ncol(raw_census_data)}")

  # Process column names using second row as headers
  column_names <- c("age_group_category", as.character(as.numeric(raw_census_data[2, -1])))
  names(raw_census_data) <- column_names
  processed_census_data <- raw_census_data[-c(1:2), ]

  logger::log_debug("Processed column names and removed header rows")

  # Filter for 65+ population
  population_65_plus <- processed_census_data %>%
    dplyr::filter(age_group_category == ".65 years and over") %>%
    tidyr::pivot_longer(
      cols = -age_group_category,
      names_to = "year_value",
      values_to = "population_thousands"
    ) %>%
    dplyr::mutate(
      year_value = as.integer(year_value),
      population_65_plus_total = as.numeric(population_thousands) * 1000
    ) %>%
    dplyr::select(year_value, population_65_plus_total)

  assertthat::assert_that(nrow(population_65_plus) > 0,
                          msg = "No 65+ population data found")

  logger::log_debug("Filtered and processed 65+ population data: {nrow(population_65_plus)} years")

  return(population_65_plus)
}

#' Calculate Workforce Demand Based on Population
#'
#' @param population_data Data frame with population projections
#' @param visits_per_woman_annually Numeric visits per woman per year
#' @param hours_per_provider_yearly Numeric hours per provider per year
#' @param apply_smoothing Logical whether to apply lowess smoothing
#' @param smoothing_span Numeric span for lowess smoothing (0-1)
#' @param verbose Logical for detailed logging
#' @return Data frame with demand calculations
#' @noRd
calculate_workforce_demand <- function(population_data,
                                       visits_per_woman_annually,
                                       hours_per_provider_yearly,
                                       apply_smoothing = TRUE,
                                       smoothing_span = 0.3,
                                       verbose = TRUE) {

  assertthat::assert_that(is.data.frame(population_data),
                          msg = "population_data must be a data frame")

  logger::log_debug("Calculating demand for {nrow(population_data)} years of population data")

  # Calculate raw demand
  demand_calculations <- population_data %>%
    dplyr::mutate(
      total_annual_visits = population_65_plus_total * visits_per_woman_annually,
      required_workforce_fte_raw = total_annual_visits / hours_per_provider_yearly
    )

  # Apply smoothing if requested
  if (apply_smoothing) {
    logger::log_info("Applying lowess smoothing to demand projections (span = {smoothing_span})")

    # Sort by year for proper smoothing
    demand_sorted <- demand_calculations %>%
      dplyr::arrange(year_value)

    # Apply lowess smoothing
    smoothed_demand <- stats::lowess(
      x = demand_sorted$year_value,
      y = demand_sorted$required_workforce_fte_raw,
      f = smoothing_span
    )

    # Add smoothed values back to data frame
    demand_calculations <- demand_sorted %>%
      dplyr::mutate(
        required_workforce_fte = smoothed_demand$y,
        smoothing_applied = TRUE
      ) %>%
      dplyr::arrange(year_value)

    # Log smoothing impact
    raw_range <- range(demand_calculations$required_workforce_fte_raw, na.rm = TRUE)
    smooth_range <- range(demand_calculations$required_workforce_fte, na.rm = TRUE)
    logger::log_debug("Raw demand range: {round(raw_range[1])} to {round(raw_range[2])} FTE")
    logger::log_debug("Smoothed demand range: {round(smooth_range[1])} to {round(smooth_range[2])} FTE")

  } else {
    logger::log_info("Using raw demand projections (no smoothing applied)")
    demand_calculations <- demand_calculations %>%
      dplyr::mutate(
        required_workforce_fte = required_workforce_fte_raw,
        smoothing_applied = FALSE
      )
  }

  logger::log_debug("Demand calculation completed")
  logger::log_debug("Year range: {min(demand_calculations$year_value)} to {max(demand_calculations$year_value)}")
  logger::log_debug("Average annual demand: {round(mean(demand_calculations$required_workforce_fte), 1)} FTE")

  return(demand_calculations)
}
#' Simulate Multiple Workforce Supply Scenarios
#'
#' @param projection_years Vector of years for projections
#' @param initial_workforce_count Numeric starting workforce size
#' @param baseline_new_fellows Numeric annual new fellows
#' @param baseline_retirements Numeric annual retirements
#' @param verbose Logical for detailed logging
#' @return Data frame with supply scenarios
#' @noRd
simulate_workforce_scenarios <- function(projection_years,
                                         initial_workforce_count,
                                         baseline_new_fellows,
                                         baseline_retirements,
                                         verbose = TRUE) {

  logger::log_debug("Simulating supply scenarios for {length(projection_years)} years")

  # Define scenario parameters
  scenario_configurations <- list(
    "Status Quo" = list(new_fellows = baseline_new_fellows, retirements = baseline_retirements),
    "Enhanced Training" = list(new_fellows = round(baseline_new_fellows * 1.1), retirements = baseline_retirements),
    "Delayed Retirement" = list(new_fellows = baseline_new_fellows, retirements = round(baseline_retirements * 0.73)),
    "Early Retirement" = list(new_fellows = baseline_new_fellows, retirements = round(baseline_retirements * 1.27))
  )

  logger::log_debug("Configured {length(scenario_configurations)} supply scenarios")

  # Generate scenarios
  all_supply_scenarios <- purrr::map_dfr(names(scenario_configurations), function(scenario_name) {

    scenario_params <- scenario_configurations[[scenario_name]]
    logger::log_debug("Processing scenario: {scenario_name}")

    simulate_single_supply_scenario(
      projection_years = projection_years,
      annual_new_fellows = scenario_params$new_fellows,
      annual_retirements = scenario_params$retirements,
      initial_workforce_count = initial_workforce_count,
      scenario_label = scenario_name
    )
  })

  logger::log_debug("All supply scenarios completed: {nrow(all_supply_scenarios)} total projections")

  return(all_supply_scenarios)
}

#' Simulate Single Workforce Supply Scenario
#'
#' @param projection_years Vector of projection years
#' @param annual_new_fellows Numeric new fellows per year
#' @param annual_retirements Numeric retirements per year
#' @param initial_workforce_count Numeric starting workforce
#' @param scenario_label Character scenario name
#' @return Data frame with single scenario projection
#' @noRd
simulate_single_supply_scenario <- function(projection_years,
                                            annual_new_fellows,
                                            annual_retirements,
                                            initial_workforce_count,
                                            scenario_label) {

  workforce_by_year <- numeric(length(projection_years))
  workforce_by_year[1] <- initial_workforce_count

  # Simulate year-over-year changes
  for (year_index in 2:length(projection_years)) {
    workforce_by_year[year_index] <- workforce_by_year[year_index - 1] +
      annual_new_fellows - annual_retirements
  }

  scenario_projection <- tibble::tibble(
    year_value = projection_years,
    workforce_fte_supply = workforce_by_year,
    supply_scenario_name = scenario_label
  )

  return(scenario_projection)
}

#' Calculate Workforce Supply-Demand Gaps
#'
#' @param supply_data Data frame with supply projections
#' @param demand_data Data frame with demand projections
#' @param verbose Logical for detailed logging
#' @return Data frame with gap analysis
#' @noRd
calculate_workforce_gaps <- function(supply_data, demand_data, verbose = TRUE) {

  assertthat::assert_that(is.data.frame(supply_data),
                          msg = "supply_data must be a data frame")
  assertthat::assert_that(is.data.frame(demand_data),
                          msg = "demand_data must be a data frame")

  logger::log_debug("Calculating workforce gaps by joining supply and demand data")

  # Check for duplicates in demand data
  demand_year_counts <- demand_data %>%
    dplyr::count(year_value, name = "n_demand_rows")

  duplicate_demand_years <- demand_year_counts %>%
    dplyr::filter(n_demand_rows > 1)

  if (nrow(duplicate_demand_years) > 0) {
    logger::log_warn("Found {nrow(duplicate_demand_years)} years with duplicate demand projections")
    logger::log_debug("Duplicate years: {paste(duplicate_demand_years$year_value, collapse = ', ')}")

    # Remove duplicates, keeping first occurrence
    demand_data <- demand_data %>%
      dplyr::distinct(year_value, .keep_all = TRUE)

    logger::log_info("Removed duplicate demand projections, keeping first occurrence for each year")
  }

  # Check for duplicates in supply data by year and scenario
  supply_check <- supply_data %>%
    dplyr::count(year_value, supply_scenario_name, name = "n_supply_rows")

  duplicate_supply <- supply_check %>%
    dplyr::filter(n_supply_rows > 1)

  if (nrow(duplicate_supply) > 0) {
    logger::log_warn("Found duplicates in supply data: {nrow(duplicate_supply)} year-scenario combinations")

    # Remove duplicates
    supply_data <- supply_data %>%
      dplyr::distinct(year_value, supply_scenario_name, .keep_all = TRUE)

    logger::log_info("Removed duplicate supply projections")
  }

  # Log data dimensions before join
  logger::log_debug("Supply data: {nrow(supply_data)} rows, {length(unique(supply_data$year_value))} unique years, {length(unique(supply_data$supply_scenario_name))} scenarios")
  logger::log_debug("Demand data: {nrow(demand_data)} rows, {length(unique(demand_data$year_value))} unique years")

  # Perform the join with explicit relationship specification
  gap_analysis <- supply_data %>%
    dplyr::left_join(
      demand_data,
      by = "year_value",
      relationship = "many-to-one"  # Many supply rows (scenarios) to one demand row per year
    ) %>%
    dplyr::mutate(
      workforce_gap_fte = workforce_fte_supply - required_workforce_fte,
      gap_percentage = (workforce_gap_fte / required_workforce_fte) * 100
    ) %>%
    dplyr::filter(!is.na(workforce_gap_fte))

  # Log results
  logger::log_debug("Gap analysis completed for {nrow(gap_analysis)} data points")
  logger::log_debug("Years with gap data: {min(gap_analysis$year_value, na.rm = TRUE)} to {max(gap_analysis$year_value, na.rm = TRUE)}")

  # Check for any remaining data quality issues
  missing_demand_years <- supply_data %>%
    dplyr::anti_join(demand_data, by = "year_value") %>%
    dplyr::distinct(year_value) %>%
    dplyr::pull(year_value)

  if (length(missing_demand_years) > 0) {
    logger::log_warn("Supply data includes {length(missing_demand_years)} years without matching demand data: {paste(head(missing_demand_years, 5), collapse = ', ')}")
  }

  return(gap_analysis)
}

#' Create Workforce Analysis Visualizations
#'
#' @param supply_data Data frame with supply projections
#' @param demand_data Data frame with demand projections
#' @param gap_data Data frame with gap analysis
#' @param output_directory Character output directory path
#' @param verbose Logical for detailed logging
#' @return Vector of created file paths
#' @noRd
create_workforce_visualizations <- function(supply_data, demand_data, gap_data,
                                            output_directory, verbose = TRUE) {

  logger::log_debug("Creating workforce analysis visualizations")

  created_file_paths <- character()

  # Supply vs Demand plot
  supply_demand_plot <- ggplot2::ggplot() +
    ggplot2::geom_line(
      data = supply_data,
      ggplot2::aes(x = year_value, y = workforce_fte_supply, color = supply_scenario_name),
      size = 1.2
    ) +
    ggplot2::geom_line(
      data = demand_data,
      ggplot2::aes(x = year_value, y = required_workforce_fte),
      color = "darkred", size = 1.2
    ) +
    ggplot2::labs(
      title = "Projected Urogynecology Workforce Supply vs Demand",
      y = "Full-Time Equivalent Providers",
      x = "Year",
      color = "Supply Scenario"
    ) +
    ggplot2::theme_minimal()

  supply_demand_file <- file.path(output_directory, "workforce_supply_demand.png")
  ggplot2::ggsave(supply_demand_file, supply_demand_plot, width = 12, height = 8)
  created_file_paths <- c(created_file_paths, supply_demand_file)
  logger::log_debug("Saved supply vs demand plot: {supply_demand_file}")

  # Workforce gap plot
  gap_plot <- ggplot2::ggplot(gap_data, ggplot2::aes(x = year_value, y = workforce_gap_fte,
                                                     color = supply_scenario_name)) +
    ggplot2::geom_line(size = 1.2) +
    ggplot2::geom_hline(yintercept = 0, linetype = "dashed", color = "black") +
    ggplot2::labs(
      title = "Projected Workforce Supply Gap by Scenario",
      y = "FTE Surplus (+) / Deficit (-)",
      x = "Year",
      color = "Supply Scenario"
    ) +
    ggplot2::theme_minimal()

  gap_file <- file.path(output_directory, "workforce_gap_analysis.png")
  ggplot2::ggsave(gap_file, gap_plot, width = 12, height = 8)
  created_file_paths <- c(created_file_paths, gap_file)
  logger::log_debug("Saved workforce gap plot: {gap_file}")

  return(created_file_paths)
}

#' Generate Summary Statistics for Workforce Analysis
#'
#' @param supply_data Data frame with supply projections
#' @param demand_data Data frame with demand projections
#' @param gap_data Data frame with gap analysis
#' @param verbose Logical for detailed logging
#' @return List of summary statistics
#' @noRd
generate_summary_statistics <- function(supply_data, demand_data, gap_data, verbose = TRUE) {

  logger::log_debug("Generating summary statistics")

  # Calculate key metrics by scenario
  scenario_summaries <- gap_data %>%
    dplyr::group_by(supply_scenario_name) %>%
    dplyr::summarise(
      years_with_shortage = sum(workforce_gap_fte < 0, na.rm = TRUE),
      max_shortage_fte = min(workforce_gap_fte, na.rm = TRUE),
      max_surplus_fte = max(workforce_gap_fte, na.rm = TRUE),
      avg_gap_percentage = mean(gap_percentage, na.rm = TRUE),
      .groups = "drop"
    )

  # Overall demand projections
  demand_summary <- demand_data %>%
    dplyr::summarise(
      min_demand_fte = min(required_workforce_fte, na.rm = TRUE),
      max_demand_fte = max(required_workforce_fte, na.rm = TRUE),
      demand_growth_rate = (max(required_workforce_fte, na.rm = TRUE) -
                              min(required_workforce_fte, na.rm = TRUE)) /
        min(required_workforce_fte, na.rm = TRUE) * 100
    )

  summary_statistics <- list(
    scenario_analysis = scenario_summaries,
    demand_projections = demand_summary,
    projection_period = list(
      start_year = min(gap_data$year_value, na.rm = TRUE),
      end_year = max(gap_data$year_value, na.rm = TRUE),
      total_years = length(unique(gap_data$year_value))
    )
  )

  logger::log_debug("Summary statistics generation completed")

  return(summary_statistics)
}

# Function at 1904 with confidence intervals
#' Process Census Population Data with Multiple Scenarios
#'
#' @param census_file_path Character path to census Excel file
#' @param verbose Logical for detailed logging
#' @return Data frame with processed population projections
#' @noRd
process_census_population_data_enhanced <- function(census_file_path, verbose = TRUE) {

  logger::log_debug("Reading census Excel file from: {census_file_path}")

  # Read the census Excel file
  raw_census_data <- readxl::read_excel(
    path = census_file_path,
    sheet = "Main series (thousands)",
    skip = 3
  )

  assertthat::assert_that(is.data.frame(raw_census_data),
                          msg = "Failed to read census data as data frame")

  logger::log_debug("Raw census data dimensions: {nrow(raw_census_data)} x {ncol(raw_census_data)}")

  # Process column names using second row as headers
  column_names <- c("age_group_category", as.character(as.numeric(raw_census_data[2, -1])))
  names(raw_census_data) <- column_names
  processed_census_data <- raw_census_data[-c(1:2), ]

  logger::log_debug("Processed column names and removed header rows")

  # Find all 65+ related rows (there might be multiple scenarios)
  age_65_plus_rows <- processed_census_data %>%
    dplyr::filter(grepl("65 years and over", age_group_category, ignore.case = TRUE))

  logger::log_debug("Found {nrow(age_65_plus_rows)} rows with 65+ population data")

  if (nrow(age_65_plus_rows) == 0) {
    stop("No 65+ population data found in census file")
  }

  # Create detailed scenario labels for different projections
  if (nrow(age_65_plus_rows) == 1) {
    scenario_labels <- "Main Projection\n(Middle Series - Current demographic trends)"
  } else if (nrow(age_65_plus_rows) == 2) {
    scenario_labels <- c("Main Projection\n(Middle Series - Current demographic trends)",
                         "Alternative Scenario\n(Modified demographic assumptions)")
  } else if (nrow(age_65_plus_rows) == 3) {
    scenario_labels <- c("Low Growth Scenario\n(Lower fertility & immigration rates)",
                         "Main Projection\n(Middle Series - Current demographic trends)",
                         "High Growth Scenario\n(Higher fertility & immigration rates)")
  } else if (nrow(age_65_plus_rows) == 4) {
    scenario_labels <- c("Low Growth Scenario\n(Lower fertility & immigration rates)",
                         "Main Projection\n(Middle Series - Current demographic trends)",
                         "High Growth Scenario\n(Higher fertility & immigration rates)",
                         "Zero Net International Migration\n(No immigration gains/losses)")
  } else {
    # For more than 4 scenarios, use detailed descriptive labels
    scenario_labels <- c(
      "Low Fertility/Mortality Scenario\n(Slower population aging)",
      "Main Projection\n(Middle Series - Current demographic trends)",
      "High Fertility/Mortality Scenario\n(Faster population aging)",
      "Zero Net International Migration\n(No immigration gains/losses)",
      "High International Migration\n(50% above current immigration levels)",
      "Low International Migration\n(50% below current immigration levels)"
    )[1:nrow(age_65_plus_rows)]
  }

  # Process all scenarios
  all_scenarios <- purrr::map2_dfr(1:nrow(age_65_plus_rows), scenario_labels, function(i, label) {
    age_65_plus_rows[i, ] %>%
      tidyr::pivot_longer(
        cols = -age_group_category,
        names_to = "year_value",
        values_to = "population_thousands"
      ) %>%
      dplyr::mutate(
        year_value = as.integer(year_value),
        population_65_plus_total = as.numeric(population_thousands) * 1000,
        projection_scenario = label
      ) %>%
      dplyr::select(year_value, population_65_plus_total, projection_scenario)
  })

  logger::log_debug("Processed {nrow(all_scenarios)} total population projection data points")
  logger::log_debug("Scenarios: {paste(unique(all_scenarios$projection_scenario), collapse = ', ')}")

  return(all_scenarios)
}

#' Create Population Visualization with Individual Colored Confidence Bands
#'
#' @param population_data Data frame with population scenarios
#' @param output_path Character file path for saving plot
#' @param add_confidence_bands Logical whether to add individual confidence bands
#' @param band_alpha Numeric alpha level for confidence bands (0-1)
#' @param confidence_percent Numeric percentage for confidence band width (e.g., 10 = ±10%)
#' @return ggplot object
create_population_visualization <- function(population_data,
                                            output_path = NULL,
                                            add_confidence_bands = TRUE,
                                            band_alpha = 0.2,
                                            confidence_percent = 8) {

  # Define colors for different scenarios
  n_scenarios <- length(unique(population_data$projection_scenario))

  if (n_scenarios == 1) {
    colors <- "darkblue"
  } else {
    colors <- RColorBrewer::brewer.pal(min(n_scenarios, 8), "Set2")
  }

  # Create named color vector
  scenario_names <- unique(population_data$projection_scenario)
  names(colors) <- scenario_names[1:length(colors)]

  # Start building the plot
  pop_plot <- ggplot2::ggplot(population_data,
                              ggplot2::aes(x = year_value,
                                           y = population_65_plus_total))

  # Add individual confidence bands if requested
  if (add_confidence_bands && n_scenarios >= 1) {

    # Create confidence bands for each scenario
    for (scenario in scenario_names) {
      scenario_data <- population_data %>%
        dplyr::filter(projection_scenario == scenario) %>%
        dplyr::mutate(
          lower_bound = population_65_plus_total * (1 - confidence_percent/100),
          upper_bound = population_65_plus_total * (1 + confidence_percent/100)
        )

      # Get the color for this scenario
      scenario_color <- colors[scenario]

      # Add the confidence ribbon for this scenario
      pop_plot <- pop_plot +
        ggplot2::geom_ribbon(
          data = scenario_data,
          ggplot2::aes(x = year_value,
                       ymin = lower_bound,
                       ymax = upper_bound),
          fill = scenario_color,
          alpha = band_alpha,
          inherit.aes = FALSE
        )
    }
  }

  # Add the scenario lines (on top of bands)
  pop_plot <- pop_plot +
    ggplot2::geom_line(ggplot2::aes(color = projection_scenario), size = 1.2) +
    ggplot2::labs(
      title = "U.S. Women Age 65+ Population Projections",
      subtitle = if(add_confidence_bands) {
        paste0("Census Bureau Projections with ±", confidence_percent, "% Uncertainty Bands")
      } else {
        "Census Bureau Population Projections by Demographic Scenario"
      },
      x = "Year",
      y = "Population (Millions)",
      color = "Projection\nScenario"
    ) +
    ggplot2::theme_minimal() +
    ggplot2::scale_y_continuous(
      labels = function(x) paste0(round(x/1e6, 1), "M"),
      breaks = scales::pretty_breaks(n = 6)
    ) +
    ggplot2::scale_x_continuous(breaks = seq(2020, 2100, by = 10)) +
    ggplot2::scale_color_manual(values = colors) +
    ggplot2::theme(
      legend.position = "right",
      legend.title = ggplot2::element_text(size = 10, face = "bold"),
      legend.text = ggplot2::element_text(size = 8, lineheight = 1.2),
      legend.key.width = ggplot2::unit(1.5, "cm"),
      legend.key.height = ggplot2::unit(1.2, "cm"),
      legend.margin = ggplot2::margin(l = 20),
      plot.title = ggplot2::element_text(size = 14, face = "bold"),
      plot.subtitle = ggplot2::element_text(size = 11),
      axis.title = ggplot2::element_text(size = 11),
      axis.text = ggplot2::element_text(size = 10),
      plot.margin = ggplot2::margin(t = 20, r = 20, b = 20, l = 20)
    )

  # Add annotation for confidence bands if present
  if (add_confidence_bands) {
    pop_plot <- pop_plot +
      ggplot2::annotate(
        "text",
        x = 2030,
        y = max(population_data$population_65_plus_total) * 0.95,
        label = paste0("Colored bands show\n±", confidence_percent,
                       "% uncertainty\n(α = ", band_alpha, ")"),
        size = 3,
        hjust = 0,
        color = "gray50",
        fontface = "italic"
      )
  }

  # Save plot if path provided (wider for detailed legend)
  if (!is.null(output_path)) {
    ggplot2::ggsave(output_path, pop_plot, width = 14, height = 8, dpi = 300)
    cat("Population visualization saved to:", output_path, "\n")
  }

  return(pop_plot)
}

# Usage example to fix your terrible graph:
library(tidyverse)
library(RColorBrewer)
library(stringr)

# Create the improved population data
census_file_path <- "/Users/tylermuffly/Dropbox (Personal)/tyler/data-raw/np2023-t2.xlsx"
enhanced_pop_data <- process_census_population_data_enhanced(census_file_path, verbose = TRUE)

# Smart relabeling based on actual population values in 2075 (not arbitrary order)
scenario_ranking <- enhanced_pop_data %>%
  dplyr::filter(year_value == 2075) %>%
  dplyr::arrange(population_65_plus_total) %>%
  dplyr::mutate(
    rank_order = row_number(),
    total_scenarios = n()
  )

# Create proper labels based on population ranking
enhanced_pop_data <- enhanced_pop_data %>%
  dplyr::left_join(scenario_ranking %>% dplyr::select(projection_scenario, rank_order, total_scenarios),
                   by = "projection_scenario") %>%
  dplyr::mutate(
    corrected_scenario = dplyr::case_when(
      total_scenarios == 1 ~ "Main Projection\n(Middle Series - Current demographic trends)",
      total_scenarios == 2 & rank_order == 1 ~ "Low Growth Scenario\n(Lower fertility & immigration rates)",
      total_scenarios == 2 & rank_order == 2 ~ "High Growth Scenario\n(Higher fertility & immigration rates)",
      total_scenarios == 3 & rank_order == 1 ~ "Low Growth Scenario\n(Lower fertility & immigration rates)",
      total_scenarios == 3 & rank_order == 2 ~ "Main Projection\n(Middle Series - Current demographic trends)",
      total_scenarios == 3 & rank_order == 3 ~ "High Growth Scenario\n(Higher fertility & immigration rates)",
      total_scenarios >= 4 & rank_order == 1 ~ "Low Growth Scenario\n(Lower fertility & immigration rates)",
      total_scenarios >= 4 & rank_order == 2 ~ "Main Projection\n(Middle Series - Current demographic trends)",
      total_scenarios >= 4 & rank_order == 3 ~ "High Growth Scenario\n(Higher fertility & immigration rates)",
      total_scenarios >= 4 & rank_order == 4 ~ "Zero Net International Migration\n(No immigration gains/losses)",
      TRUE ~ paste0("Scenario ", rank_order, "\n(Population rank: ", rank_order, ")")
    )
  ) %>%
  dplyr::select(-projection_scenario, -rank_order, -total_scenarios) %>%
  dplyr::rename(projection_scenario = corrected_scenario)

# Create the much better visualization
better_pop_plot <- create_population_visualization(
  population_data = enhanced_pop_data,
  output_path = "./improved_population_projections.png"
)

# Display the plot
print(better_pop_plot)

# Check what scenarios we actually have
cat("Population projection scenarios found:\n")
print(table(enhanced_pop_data$projection_scenario))

# Summary statistics by scenario
scenario_summary <- enhanced_pop_data %>%
  group_by(projection_scenario) %>%
  summarise(
    min_year = min(year_value),
    max_year = max(year_value),
    pop_2025 = population_65_plus_total[year_value == 2025] / 1e6,
    pop_2050 = population_65_plus_total[year_value == 2050] / 1e6,
    pop_2075 = population_65_plus_total[year_value == 2075] / 1e6,
    .groups = "drop"
  )

print("Population by scenario (millions):")
print(scenario_summary)

# Create population plot with individual colored confidence bands
better_pop_plot <- create_population_visualization(
  population_data = enhanced_pop_data,
  output_path = "./individual_colored_bands.png",
  add_confidence_bands = TRUE,     # Show individual bands
  band_alpha = 0.2,               # 20% transparency (light bands)
  confidence_percent = 8          # ±8% uncertainty around each line
)

print(better_pop_plot)

# Try different band widths
narrow_bands <- create_population_visualization(
  population_data = enhanced_pop_data,
  add_confidence_bands = TRUE,
  band_alpha = 0.25,
  confidence_percent = 5          # ±5% narrower bands
); narrow_bands

wide_bands <- create_population_visualization(
  population_data = enhanced_pop_data,
  add_confidence_bands = TRUE,
  band_alpha = 0.15,
  confidence_percent = 12         # ±12% wider bands
); wide_bands


####
# ACTION JACKSON NOW ----
####

# Set the path to your census file
census_file_path <- "/Users/tylermuffly/Dropbox (Personal)/tyler/data-raw/np2023-t2.xlsx"
# =============================================================================
# WORKFORCE ANALYSIS FUNCTIONS - USAGE GUIDE
# =============================================================================

# -----------------------------------------------------------------------------
# STEP 1: INSTALL AND LOAD REQUIRED PACKAGES ----
# -----------------------------------------------------------------------------

# Install packages if you haven't already
# install.packages(c(
#   "tidyverse", "readxl", "ggplot2", "dplyr", "tidyr",
#   "tibble", "purrr", "assertthat", "logger"
# ))

# Load the required libraries
library(tidyverse)
library(readxl)
library(assertthat)
library(logger)
library(tibble)
library(purrr)

# -----------------------------------------------------------------------------
# STEP 2: SOURCE THE FUNCTIONS -----
# -----------------------------------------------------------------------------

# Option A: Save the functions to a file and source it
# Save the function code to "workforce_functions.R" then:
# source("workforce_functions.R")

# Option B: If you're building an R package, the functions would be available after
# devtools::load_all() or library(yourpackagename)

# -----------------------------------------------------------------------------
# STEP 3: PREPARE YOUR DATA ----
# -----------------------------------------------------------------------------

# You need the Census population projection Excel file
# Download from: https://www.census.gov/data/datasets/2023/demo/popproj/2023-popproj.html
# Look for "np2023-t2.xlsx" - Table 2 with population by age and sex

# Set the path to your census file (update this to your actual path!)
# census_file_path <- "/path/to/your/np2023-t2.xlsx"
# Example: census_file_path <- "/Users/yourusername/Documents/np2023-t2.xlsx"

# Create output directory for results
output_dir <- "./workforce_analysis_results"
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

# -----------------------------------------------------------------------------
# STEP 4: BASIC USAGE - DEFAULT ANALYSIS -----
# -----------------------------------------------------------------------------

# function at 1921 ----
#####
#' Process Census Population Projection Data (FIXED VERSION)
#'
#' @param census_file_path Character path to census Excel file
#' @param verbose Logical for detailed logging
#' @return Data frame with processed population projections
#' @noRd
process_census_population_data <- function(census_file_path, verbose = TRUE) {

  logger::log_debug("Reading census Excel file from: {census_file_path}")

  # Read the census Excel file
  raw_census_data <- readxl::read_excel(
    path = census_file_path,
    sheet = "Main series (thousands)",
    skip = 3
  )

  assertthat::assert_that(is.data.frame(raw_census_data),
                          msg = "Failed to read census data as data frame")

  logger::log_debug("Raw census data dimensions: {nrow(raw_census_data)} x {ncol(raw_census_data)}")

  # Process column names using second row as headers
  column_names <- c("age_group_category", as.character(as.numeric(raw_census_data[2, -1])))
  names(raw_census_data) <- column_names
  processed_census_data <- raw_census_data[-c(1:2), ]

  logger::log_debug("Processed column names and removed header rows")

  # Filter for 65+ population
  population_65_plus <- processed_census_data %>%
    dplyr::filter(age_group_category == ".65 years and over") %>%
    tidyr::pivot_longer(
      cols = -age_group_category,
      names_to = "year_value",
      values_to = "population_thousands"
    ) %>%
    dplyr::mutate(
      year_value = as.integer(year_value),
      population_65_plus_total = as.numeric(population_thousands) * 1000
    ) %>%
    dplyr::select(year_value, population_65_plus_total)

  # CRITICAL FIX: Remove duplicates by keeping only the first occurrence per year
  # This selects the main/primary population projection when multiple scenarios exist
  population_65_plus <- population_65_plus %>%
    dplyr::group_by(year_value) %>%
    dplyr::slice_head(n = 1) %>%  # Keep only first row per year
    dplyr::ungroup()

  assertthat::assert_that(nrow(population_65_plus) > 0,
                          msg = "No 65+ population data found")

  # Verify we now have unique years
  duplicate_years <- population_65_plus %>%
    dplyr::count(year_value) %>%
    dplyr::filter(n > 1)

  if (nrow(duplicate_years) > 0) {
    logger::log_warn("Still have duplicate years after filtering: {paste(duplicate_years$year_value, collapse = ', ')}")
  } else {
    logger::log_debug("Successfully removed duplicates: {nrow(population_65_plus)} unique years")
  }

  logger::log_debug("Filtered and processed 65+ population data: {nrow(population_65_plus)} years")

  return(population_65_plus)
}
#####

# Run the analysis with default parameters (smoothing enabled by default)
# Re-run with the fixed function
basic_analysis_fixed <- analyze_urogynecology_workforce(
  census_file_path = census_file_path,
  output_directory = paste0(output_dir, "_fixed"),
  apply_demand_smoothing = TRUE,
  smoothing_span = 0.3,
  verbose = TRUE
)

# Verify the fix worked
cat("AFTER FIX - Demand per year:\n")
print(table(table(basic_analysis_fixed$demand_projections$year_value)))

cat("AFTER FIX - Supply per year-scenario:\n")
supply_check_fixed <- basic_analysis_fixed$workforce_projections %>%
  count(year_value, supply_scenario_name) %>%
  pull(n)
print(table(supply_check_fixed))

# Check a specific year - should now show only 1 row
basic_analysis_fixed$demand_projections %>%
  filter(year_value == 2022)

# Check summary stats - should be more reasonable now
basic_analysis_fixed$summary_statistics$scenario_analysis

# Let's see what these numbers mean
cat("Current assumptions:\n")
cat("- Current workforce:", basic_analysis_fixed$workforce_projections$workforce_fte_supply[1], "FTE\n")
cat("- Current demand:", round(basic_analysis_fixed$demand_projections$required_workforce_fte[1]), "FTE\n")
cat("- Population 65+:", scales::comma(basic_analysis_fixed$demand_projections$population_65_plus_total[1]), "\n")
cat("- Visits per woman:", 1.5, "per year\n")
cat("- Hours per provider:", 1728, "per year\n")

# What if we adjust assumptions?
cat("\nWhat if only 10% of 65+ women need urogynecology care?\n")
adjusted_demand <- basic_analysis_fixed$demand_projections$required_workforce_fte[1] * 0.1
cat("Adjusted demand:", round(adjusted_demand), "FTE\n")
cat("Gap with 10% rate:", round(1169 - adjusted_demand), "FTE\n")

# Option 2: Sensitivity Analysis
# Test different assumptions
sensitivity_test <- analyze_urogynecology_workforce(
  census_file_path = census_file_path,
  visits_per_woman_annually = 0.15,  # 10x lower visit rate
  hours_per_provider_yearly = 2000,   # Higher productivity
  verbose = FALSE
)

#  Parameter Validation
# Test different realistic scenarios
realistic_low <- 0.10   # 10% of 65+ women, 1 visit/year
realistic_mid <- 0.15   # 15% of 65+ women, 1 visit/year
realistic_high <- 0.25  # 25% of 65+ women, 1 visit/year

# The function will log progress to console like:
# INFO [timestamp] Starting urogynecology workforce analysis
# DEBUG [timestamp] Input parameters: census_file_path = /path/to/file
# INFO [timestamp] Processing census population projections
# ... etc.

# Function at 1941 ----
####
#' Analyze Urogynecology Workforce Supply and Demand
#'
#' This function performs a comprehensive analysis of urogynecology workforce
#' supply and demand projections, including multiple scenarios for training
#' and retirement rates. It processes census population data, simulates
#' workforce supply under different scenarios, and calculates workforce gaps.
#'
#' @param census_file_path Character. Full path to the Census Excel file
#'   containing population projections (np2023-t2.xlsx format).
#' @param initial_workforce_count Numeric. Starting number of urogynecologists
#'   in the workforce. Default is 1169.
#' @param baseline_new_fellows Numeric. Annual number of new fellows entering
#'   the workforce under status quo conditions. Default is 55.
#' @param baseline_retirements Numeric. Annual number of retirements under
#'   status quo conditions. Default is 55.
#' @param visits_per_woman_annually Numeric. Expected number of urogynecology
#'   visits per woman aged 65+ per year. Default is 1.5.
#' @param hours_per_provider_yearly Numeric. Total working hours per provider
#'   per year. Default is 1728 (36 hours/week * 48 weeks).
#' @param output_directory Character. Directory path where output files should
#'   be saved. Default is current working directory.
#' @param create_visualizations Logical. Whether to generate and save
#'   visualization plots. Default is TRUE.
#' @param apply_demand_smoothing Logical. Whether to apply lowess smoothing
#'   to demand projections to remove unrealistic fluctuations. Default is TRUE.
#' @param smoothing_span Numeric. Span parameter for lowess smoothing
#'   (0-1, higher = more smoothing). Default is 0.3.
#' @param verbose Logical. Whether to enable detailed logging output.
#'   Default is TRUE.
#'
#' @return A list containing:
#'   \item{workforce_projections}{Data frame with supply projections by scenario}
#'   \item{demand_projections}{Data frame with demand projections by year}
#'   \item{workforce_gap_analysis}{Data frame showing supply-demand gaps}
#'   \item{summary_statistics}{List of key summary metrics}
#'
#' @examples
#' # Basic analysis with default parameters (includes smoothing)
#' census_path <- "/path/to/census/np2023-t2.xlsx"
#' workforce_results <- analyze_urogynecology_workforce(
#'   census_file_path = census_path,
#'   initial_workforce_count = 1169,
#'   baseline_new_fellows = 55,
#'   baseline_retirements = 55,
#'   visits_per_woman_annually = 1.5,
#'   hours_per_provider_yearly = 1728,
#'   output_directory = "./workforce_analysis",
#'   create_visualizations = TRUE,
#'   apply_demand_smoothing = TRUE,
#'   smoothing_span = 0.3,
#'   verbose = TRUE
#' )
#'
#' # Analysis with no smoothing to see raw census fluctuations
#' raw_data_results <- analyze_urogynecology_workforce(
#'   census_file_path = census_path,
#'   initial_workforce_count = 1200,
#'   baseline_new_fellows = 65,
#'   baseline_retirements = 50,
#'   visits_per_woman_annually = 1.8,
#'   hours_per_provider_yearly = 1800,
#'   output_directory = "./raw_data_analysis",
#'   create_visualizations = TRUE,
#'   apply_demand_smoothing = FALSE,
#'   smoothing_span = 0.3,
#'   verbose = FALSE
#' )
#'
#' # Conservative analysis with heavy smoothing for long-term planning
#' conservative_results <- analyze_urogynecology_workforce(
#'   census_file_path = census_path,
#'   initial_workforce_count = 1100,
#'   baseline_new_fellows = 50,
#'   baseline_retirements = 60,
#'   visits_per_woman_annually = 1.2,
#'   hours_per_provider_yearly = 1650,
#'   output_directory = "./conservative_analysis",
#'   create_visualizations = FALSE,
#'   apply_demand_smoothing = TRUE,
#'   smoothing_span = 0.5,
#'   verbose = TRUE
#' )
#'
#' @importFrom readxl read_excel
#' @importFrom dplyr filter mutate select pivot_longer bind_rows left_join
#' @importFrom dplyr group_by summarise case_when pull arrange
#' @importFrom tidyr pivot_longer
#' @importFrom tibble tibble
#' @importFrom purrr map_dfr
#' @importFrom ggplot2 ggplot aes geom_line geom_hline labs theme_minimal
#' @importFrom ggplot2 scale_x_continuous annotate ggsave scale_y_continuous
#' @importFrom scales comma
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_warn log_error log_debug log_threshold
#' @importFrom stats lowess
#' @importFrom stringr str_detect
#' @export
analyze_urogynecology_workforce <- function(census_file_path,
                                            initial_workforce_count = 1169,
                                            baseline_new_fellows = 55,
                                            baseline_retirements = 55,
                                            visits_per_woman_annually = 1.5,
                                            hours_per_provider_yearly = 1728,
                                            output_directory = ".",
                                            create_visualizations = TRUE,
                                            apply_demand_smoothing = TRUE,
                                            smoothing_span = 0.3,
                                            verbose = TRUE) {

  # Configure logging level based on verbose parameter
  if (verbose) {
    logger::log_threshold(logger::DEBUG)
  } else {
    logger::log_threshold(logger::INFO)
  }

  logger::log_info("Starting urogynecology workforce analysis")
  logger::log_debug("Input parameters: census_file_path = {census_file_path}")
  logger::log_debug("initial_workforce_count = {initial_workforce_count}")
  logger::log_debug("baseline_new_fellows = {baseline_new_fellows}")
  logger::log_debug("baseline_retirements = {baseline_retirements}")
  logger::log_debug("visits_per_woman_annually = {visits_per_woman_annually}")
  logger::log_debug("hours_per_provider_yearly = {hours_per_provider_yearly}")
  logger::log_debug("output_directory = {output_directory}")
  logger::log_debug("create_visualizations = {create_visualizations}")
  logger::log_debug("apply_demand_smoothing = {apply_demand_smoothing}")
  logger::log_debug("smoothing_span = {smoothing_span}")

  # Input validation with assertthat
  assertthat::assert_that(is.character(census_file_path),
                          msg = "census_file_path must be a character string")
  assertthat::assert_that(file.exists(census_file_path),
                          msg = "Census file does not exist at specified path")
  assertthat::assert_that(is.numeric(initial_workforce_count),
                          initial_workforce_count > 0,
                          msg = "initial_workforce_count must be positive number")
  assertthat::assert_that(is.numeric(baseline_new_fellows),
                          baseline_new_fellows >= 0,
                          msg = "baseline_new_fellows must be non-negative")
  assertthat::assert_that(is.numeric(baseline_retirements),
                          baseline_retirements >= 0,
                          msg = "baseline_retirements must be non-negative")
  assertthat::assert_that(is.numeric(visits_per_woman_annually),
                          visits_per_woman_annually > 0,
                          msg = "visits_per_woman_annually must be positive")
  assertthat::assert_that(is.numeric(hours_per_provider_yearly),
                          hours_per_provider_yearly > 0,
                          msg = "hours_per_provider_yearly must be positive")
  assertthat::assert_that(is.character(output_directory),
                          msg = "output_directory must be a character string")
  assertthat::assert_that(is.logical(create_visualizations),
                          msg = "create_visualizations must be logical")
  assertthat::assert_that(is.logical(apply_demand_smoothing),
                          msg = "apply_demand_smoothing must be logical")
  assertthat::assert_that(is.numeric(smoothing_span),
                          smoothing_span > 0, smoothing_span <= 1,
                          msg = "smoothing_span must be between 0 and 1")
  assertthat::assert_that(is.logical(verbose),
                          msg = "verbose must be logical")

  logger::log_info("Input validation completed successfully")

  # Create output directory if it doesn't exist
  if (!dir.exists(output_directory)) {
    dir.create(output_directory, recursive = TRUE)
    logger::log_info("Created output directory: {output_directory}")
  }

  # Process census population data
  logger::log_info("Processing census population projections")
  population_projections_all <- process_census_population_data(
    census_file_path = census_file_path,
    verbose = verbose
  )

  # Check if we have multiple scenarios or just one dataset
  if ("projection_scenario" %in% names(population_projections_all)) {
    # Multiple scenarios - select only the Main Projection
    population_projections <- population_projections_all %>%
      dplyr::filter(stringr::str_detect(projection_scenario, "Main Projection|Scenario 1")) %>%
      dplyr::select(year_value, population_65_plus_total)  # Remove scenario column
    logger::log_info("Using Main Projection scenario only (filtered from multiple scenarios)")
  } else {
    # Single scenario dataset - use as-is
    population_projections <- population_projections_all
    logger::log_info("Using single population projection dataset")
  }

  logger::log_info("Census data processing completed: {nrow(population_projections)} years of data")

  # Calculate demand projections
  logger::log_info("Calculating workforce demand projections")
  demand_analysis <- calculate_workforce_demand(
    population_data = population_projections,
    visits_per_woman_annually = visits_per_woman_annually,
    hours_per_provider_yearly = hours_per_provider_yearly,
    apply_smoothing = apply_demand_smoothing,
    smoothing_span = smoothing_span,
    verbose = verbose
  )
  logger::log_info("Demand calculations completed")

  # Generate supply scenarios
  logger::log_info("Simulating workforce supply scenarios")
  supply_projections <- simulate_workforce_scenarios(
    projection_years = population_projections$year_value,
    initial_workforce_count = initial_workforce_count,
    baseline_new_fellows = baseline_new_fellows,
    baseline_retirements = baseline_retirements,
    verbose = verbose
  )
  logger::log_info("Supply scenario simulations completed: {nrow(supply_projections)} projections")

  # Calculate workforce gaps
  logger::log_info("Analyzing workforce supply-demand gaps")
  gap_analysis <- calculate_workforce_gaps(
    supply_data = supply_projections,
    demand_data = demand_analysis,
    verbose = verbose
  )
  logger::log_info("Gap analysis completed")

  # Generate visualizations if requested
  visualization_paths <- NULL
  if (create_visualizations) {
    logger::log_info("Creating workforce analysis visualizations")
    visualization_paths <- create_workforce_visualizations(
      supply_data = supply_projections,
      demand_data = demand_analysis,
      gap_data = gap_analysis,
      output_directory = output_directory,
      verbose = verbose
    )
    logger::log_info("Visualizations saved to: {paste(visualization_paths, collapse = ', ')}")
  }

  # Generate summary statistics
  logger::log_info("Calculating summary statistics")
  summary_stats <- generate_summary_statistics(
    supply_data = supply_projections,
    demand_data = demand_analysis,
    gap_data = gap_analysis,
    verbose = verbose
  )

  # Compile final results
  final_analysis_results <- list(
    workforce_projections = supply_projections,
    demand_projections = demand_analysis,
    workforce_gap_analysis = gap_analysis,
    summary_statistics = summary_stats,
    visualization_file_paths = visualization_paths
  )

  logger::log_info("Workforce analysis completed successfully")
  logger::log_info("Results include {nrow(supply_projections)} supply projections and {nrow(demand_analysis)} demand projections")

  return(final_analysis_results)
}

#' Process Census Population Projection Data
#'
#' @param census_file_path Character path to census Excel file
#' @param verbose Logical for detailed logging
#' @return Data frame with processed population projections
#' @noRd
process_census_population_data <- function(census_file_path, verbose = TRUE) {

  logger::log_debug("Reading census Excel file from: {census_file_path}")

  # Read the census Excel file
  raw_census_data <- readxl::read_excel(
    path = census_file_path,
    sheet = "Main series (thousands)",
    skip = 3
  )

  assertthat::assert_that(is.data.frame(raw_census_data),
                          msg = "Failed to read census data as data frame")

  logger::log_debug("Raw census data dimensions: {nrow(raw_census_data)} x {ncol(raw_census_data)}")

  # Process column names using second row as headers
  column_names <- c("age_group_category", as.character(as.numeric(raw_census_data[2, -1])))
  names(raw_census_data) <- column_names
  processed_census_data <- raw_census_data[-c(1:2), ]

  logger::log_debug("Processed column names and removed header rows")

  # Filter for 65+ population
  population_65_plus <- processed_census_data %>%
    dplyr::filter(age_group_category == ".65 years and over") %>%
    tidyr::pivot_longer(
      cols = -age_group_category,
      names_to = "year_value",
      values_to = "population_thousands"
    ) %>%
    dplyr::mutate(
      year_value = as.integer(year_value),
      population_65_plus_total = as.numeric(population_thousands) * 1000
    ) %>%
    dplyr::select(year_value, population_65_plus_total)

  assertthat::assert_that(nrow(population_65_plus) > 0,
                          msg = "No 65+ population data found")

  logger::log_debug("Filtered and processed 65+ population data: {nrow(population_65_plus)} years")

  return(population_65_plus)
}

#' Calculate Workforce Demand Based on Population
#'
#' @param population_data Data frame with population projections
#' @param visits_per_woman_annually Numeric visits per woman per year
#' @param hours_per_provider_yearly Numeric hours per provider per year
#' @param apply_smoothing Logical whether to apply lowess smoothing
#' @param smoothing_span Numeric span for lowess smoothing (0-1)
#' @param verbose Logical for detailed logging
#' @return Data frame with demand calculations
#' @noRd
calculate_workforce_demand <- function(population_data,
                                       visits_per_woman_annually,
                                       hours_per_provider_yearly,
                                       apply_smoothing = TRUE,
                                       smoothing_span = 0.3,
                                       verbose = TRUE) {

  assertthat::assert_that(is.data.frame(population_data),
                          msg = "population_data must be a data frame")

  logger::log_debug("Calculating demand for {nrow(population_data)} years of population data")

  # Calculate raw demand
  demand_calculations <- population_data %>%
    dplyr::mutate(
      total_annual_visits = population_65_plus_total * visits_per_woman_annually,
      required_workforce_fte_raw = total_annual_visits / hours_per_provider_yearly
    )

  # Apply smoothing if requested
  if (apply_smoothing) {
    logger::log_info("Applying lowess smoothing to demand projections (span = {smoothing_span})")

    # Sort by year for proper smoothing
    demand_sorted <- demand_calculations %>%
      dplyr::arrange(year_value)

    # Apply lowess smoothing
    smoothed_demand <- stats::lowess(
      x = demand_sorted$year_value,
      y = demand_sorted$required_workforce_fte_raw,
      f = smoothing_span
    )

    # Add smoothed values back to data frame
    demand_calculations <- demand_sorted %>%
      dplyr::mutate(
        required_workforce_fte = smoothed_demand$y,
        smoothing_applied = TRUE
      ) %>%
      dplyr::arrange(year_value)

    # Log smoothing impact
    raw_range <- range(demand_calculations$required_workforce_fte_raw, na.rm = TRUE)
    smooth_range <- range(demand_calculations$required_workforce_fte, na.rm = TRUE)
    logger::log_debug("Raw demand range: {round(raw_range[1])} to {round(raw_range[2])} FTE")
    logger::log_debug("Smoothed demand range: {round(smooth_range[1])} to {round(smooth_range[2])} FTE")

  } else {
    logger::log_info("Using raw demand projections (no smoothing applied)")
    demand_calculations <- demand_calculations %>%
      dplyr::mutate(
        required_workforce_fte = required_workforce_fte_raw,
        smoothing_applied = FALSE
      )
  }

  logger::log_debug("Demand calculation completed")
  logger::log_debug("Year range: {min(demand_calculations$year_value)} to {max(demand_calculations$year_value)}")
  logger::log_debug("Average annual demand: {round(mean(demand_calculations$required_workforce_fte), 1)} FTE")

  return(demand_calculations)
}

#' Simulate Multiple Workforce Supply Scenarios
#'
#' @param projection_years Vector of years for projections
#' @param initial_workforce_count Numeric starting workforce size
#' @param baseline_new_fellows Numeric annual new fellows
#' @param baseline_retirements Numeric annual retirements
#' @param verbose Logical for detailed logging
#' @return Data frame with supply scenarios
#' @noRd
simulate_workforce_scenarios <- function(projection_years,
                                         initial_workforce_count,
                                         baseline_new_fellows,
                                         baseline_retirements,
                                         verbose = TRUE) {

  logger::log_debug("Simulating supply scenarios for {length(projection_years)} years")

  # Define scenario parameters with detailed descriptions
  scenario_configurations <- list(
    "Status Quo\n(55 new fellows/year, 55 retirements/year)" = list(
      new_fellows = baseline_new_fellows,
      retirements = baseline_retirements
    ),
    "Enhanced Training\n(10% more fellows: 61/year, 55 retirements/year)" = list(
      new_fellows = round(baseline_new_fellows * 1.1),
      retirements = baseline_retirements
    ),
    "Delayed Retirement\n(55 new fellows/year, 27% fewer retirements: 40/year)" = list(
      new_fellows = baseline_new_fellows,
      retirements = round(baseline_retirements * 0.73)
    ),
    "Early Retirement\n(55 new fellows/year, 27% more retirements: 70/year)" = list(
      new_fellows = baseline_new_fellows,
      retirements = round(baseline_retirements * 1.27)
    )
  )

  logger::log_debug("Configured {length(scenario_configurations)} supply scenarios")

  # Generate scenarios
  all_supply_scenarios <- purrr::map_dfr(names(scenario_configurations), function(scenario_name) {

    scenario_params <- scenario_configurations[[scenario_name]]
    logger::log_debug("Processing scenario: {scenario_name}")

    simulate_single_supply_scenario(
      projection_years = projection_years,
      annual_new_fellows = scenario_params$new_fellows,
      annual_retirements = scenario_params$retirements,
      initial_workforce_count = initial_workforce_count,
      scenario_label = scenario_name
    )
  })

  logger::log_debug("All supply scenarios completed: {nrow(all_supply_scenarios)} total projections")

  return(all_supply_scenarios)
}

#' Simulate Single Workforce Supply Scenario
#'
#' @param projection_years Vector of projection years
#' @param annual_new_fellows Numeric new fellows per year
#' @param annual_retirements Numeric retirements per year
#' @param initial_workforce_count Numeric starting workforce
#' @param scenario_label Character scenario name
#' @return Data frame with single scenario projection
#' @noRd
simulate_single_supply_scenario <- function(projection_years,
                                            annual_new_fellows,
                                            annual_retirements,
                                            initial_workforce_count,
                                            scenario_label) {

  workforce_by_year <- numeric(length(projection_years))
  workforce_by_year[1] <- initial_workforce_count

  # Simulate year-over-year changes
  for (year_index in 2:length(projection_years)) {
    workforce_by_year[year_index] <- workforce_by_year[year_index - 1] +
      annual_new_fellows - annual_retirements
  }

  scenario_projection <- tibble::tibble(
    year_value = projection_years,
    workforce_fte_supply = workforce_by_year,
    supply_scenario_name = scenario_label
  )

  return(scenario_projection)
}

#' Calculate Workforce Supply-Demand Gaps
#'
#' @param supply_data Data frame with supply projections
#' @param demand_data Data frame with demand projections
#' @param verbose Logical for detailed logging
#' @return Data frame with gap analysis
#' @noRd
calculate_workforce_gaps <- function(supply_data, demand_data, verbose = TRUE) {

  assertthat::assert_that(is.data.frame(supply_data),
                          msg = "supply_data must be a data frame")
  assertthat::assert_that(is.data.frame(demand_data),
                          msg = "demand_data must be a data frame")

  logger::log_debug("Calculating workforce gaps by joining supply and demand data")

  gap_analysis <- supply_data %>%
    dplyr::left_join(demand_data, by = "year_value") %>%
    dplyr::mutate(
      workforce_gap_fte = workforce_fte_supply - required_workforce_fte,
      gap_percentage = (workforce_gap_fte / required_workforce_fte) * 100
    ) %>%
    dplyr::filter(!is.na(workforce_gap_fte))

  logger::log_debug("Gap analysis completed for {nrow(gap_analysis)} data points")

  return(gap_analysis)
}

#' Create Workforce Analysis Visualizations
#'
#' @param supply_data Data frame with supply projections
#' @param demand_data Data frame with demand projections
#' @param gap_data Data frame with gap analysis
#' @param output_directory Character output directory path
#' @param verbose Logical for detailed logging
#' @return Vector of created file paths
#' @noRd
create_workforce_visualizations <- function(supply_data, demand_data, gap_data,
                                            output_directory, verbose = TRUE) {

  logger::log_debug("Creating workforce analysis visualizations")

  created_file_paths <- character()

  # Supply vs Demand plot with smoothing note
  smoothing_note <- if("smoothing_applied" %in% names(demand_data) &&
                       any(demand_data$smoothing_applied, na.rm = TRUE)) {
    "Demand projections smoothed using lowess"
  } else {
    "Raw demand projections (no smoothing)"
  }

  supply_demand_plot <- ggplot2::ggplot() +
    ggplot2::geom_line(
      data = supply_data,
      ggplot2::aes(x = year_value, y = workforce_fte_supply, color = supply_scenario_name),
      size = 1.2
    ) +
    ggplot2::geom_line(
      data = demand_data,
      ggplot2::aes(x = year_value, y = required_workforce_fte),
      color = "darkred", size = 1.2
    ) +
    ggplot2::labs(
      title = "Projected Urogynecology Workforce Supply vs Demand",
      subtitle = smoothing_note,
      y = "Full-Time Equivalent Providers",
      x = "Year",
      color = "Supply Scenario"
    ) +
    ggplot2::theme_minimal() +
    ggplot2::scale_y_continuous(labels = scales::comma) +
    ggplot2::scale_x_continuous(breaks = seq(2020, 2100, by = 10))

  supply_demand_file <- file.path(output_directory, "workforce_supply_demand.png")
  ggplot2::ggsave(supply_demand_file, supply_demand_plot, width = 12, height = 8)
  created_file_paths <- c(created_file_paths, supply_demand_file)
  logger::log_debug("Saved supply vs demand plot: {supply_demand_file}")

  # Workforce gap plot with improved formatting for detailed scenario names
  gap_plot <- ggplot2::ggplot(gap_data, ggplot2::aes(x = year_value, y = workforce_gap_fte,
                                                     color = supply_scenario_name)) +
    ggplot2::geom_line(size = 1.2) +
    ggplot2::geom_hline(yintercept = 0, linetype = "dashed", color = "black") +
    ggplot2::labs(
      title = "Projected Workforce Supply Gap by Policy Scenario",
      y = "FTE Surplus (+) / Deficit (-)",
      x = "Year",
      color = "Policy\nScenario"
    ) +
    ggplot2::theme_minimal() +
    ggplot2::scale_y_continuous(labels = scales::comma) +
    ggplot2::scale_x_continuous(breaks = seq(2020, 2100, by = 10)) +
    ggplot2::theme(
      legend.position = "right",
      legend.title = ggplot2::element_text(size = 10, face = "bold"),
      legend.text = ggplot2::element_text(size = 7, lineheight = 1.1),
      legend.key.width = ggplot2::unit(1.8, "cm"),
      legend.key.height = ggplot2::unit(1.5, "cm"),
      legend.margin = ggplot2::margin(l = 20),
      plot.margin = ggplot2::margin(t = 20, r = 25, b = 20, l = 20)
    )

  gap_file <- file.path(output_directory, "workforce_gap_analysis.png")
  ggplot2::ggsave(gap_file, gap_plot, width = 12, height = 8)
  created_file_paths <- c(created_file_paths, gap_file)
  logger::log_debug("Saved workforce gap plot: {gap_file}")

  return(created_file_paths)
}

#' Generate Summary Statistics for Workforce Analysis
#'
#' @param supply_data Data frame with supply projections
#' @param demand_data Data frame with demand projections
#' @param gap_data Data frame with gap analysis
#' @param verbose Logical for detailed logging
#' @return List of summary statistics
#' @noRd
generate_summary_statistics <- function(supply_data, demand_data, gap_data, verbose = TRUE) {

  logger::log_debug("Generating summary statistics")

  # Calculate key metrics by scenario
  scenario_summaries <- gap_data %>%
    dplyr::group_by(supply_scenario_name) %>%
    dplyr::summarise(
      years_with_shortage = sum(workforce_gap_fte < 0, na.rm = TRUE),
      max_shortage_fte = min(workforce_gap_fte, na.rm = TRUE),
      max_surplus_fte = max(workforce_gap_fte, na.rm = TRUE),
      avg_gap_percentage = mean(gap_percentage, na.rm = TRUE),
      .groups = "drop"
    )

  # Overall demand projections
  demand_summary <- demand_data %>%
    dplyr::summarise(
      min_demand_fte = min(required_workforce_fte, na.rm = TRUE),
      max_demand_fte = max(required_workforce_fte, na.rm = TRUE),
      demand_growth_rate = (max(required_workforce_fte, na.rm = TRUE) -
                              min(required_workforce_fte, na.rm = TRUE)) /
        min(required_workforce_fte, na.rm = TRUE) * 100
    )

  summary_statistics <- list(
    scenario_analysis = scenario_summaries,
    demand_projections = demand_summary,
    projection_period = list(
      start_year = min(gap_data$year_value, na.rm = TRUE),
      end_year = max(gap_data$year_value, na.rm = TRUE),
      total_years = length(unique(gap_data$year_value))
    )
  )

  logger::log_debug("Summary statistics generation completed")

  return(summary_statistics)
}
####

# Re-run with the enhanced scenario names
detailed_analysis <- analyze_urogynecology_workforce(
  census_file_path = census_file_path,
  visits_per_woman_annually = 0.15,
  output_directory = paste0(output_dir, "_detailed"),
  verbose = TRUE
)

# Check the new detailed scenario names
detailed_analysis$summary_statistics$scenario_analysis

# Look at the enhanced workforce projections
detailed_analysis$workforce_projections %>%
  filter(year_value %in% c(2022, 2040, 2100)) %>%
  select(year_value, workforce_fte_supply, supply_scenario_name)

# -----------------------------------------------------------------------------
# STEP 5: EXAMINE THE RESULTS
# -----------------------------------------------------------------------------

# The function returns a list with 5 components:
str(basic_analysis)

# Access workforce supply projections by scenario
workforce_supply <- basic_analysis$workforce_projections
head(workforce_supply)
#   year_value workforce_fte_supply supply_scenario_name
# 1       2022                 1169           Status Quo
# 2       2023                 1169     Enhanced Training
# 3       2024                 1169    Delayed Retirement
# ...

# Access demand projections
workforce_demand <- basic_analysis$demand_projections
head(workforce_demand)
#   year_value population_65_plus_total total_annual_visits required_workforce_fte
# 1       2022                 56100000            84150000               48697.92
# 2       2023                 57200000            85800000               49652.78
# ...

# Access gap analysis (supply minus demand)
workforce_gaps <- basic_analysis$workforce_gap_analysis
head(workforce_gaps)
#   year_value workforce_fte_supply supply_scenario_name population_65_plus_total
# 1       2022                 1169           Status Quo                 56100000
# 2       2023                 1169           Status Quo                 57200000
# ...  workforce_gap_fte gap_percentage
# 1          -47528.92        -97.60
# 2          -48483.78        -97.65

# Access summary statistics
summary_stats <- basic_analysis$summary_statistics
summary_stats$scenario_analysis
summary_stats$demand_projections

# Check where visualizations were saved
basic_analysis$visualization_file_paths
# [1] "./workforce_analysis_results/workforce_supply_demand.png"
# [2] "./workforce_analysis_results/workforce_gap_analysis.png"

# -----------------------------------------------------------------------------
# STEP 6: UNDERSTANDING AND CONTROLLING DEMAND SMOOTHING
# -----------------------------------------------------------------------------

# Your issue: Raw census data often has unrealistic year-to-year fluctuations
# Solution: Lowess smoothing removes artifacts while preserving long-term trends

# Compare raw vs smoothed demand projections
raw_demand_analysis <- analyze_urogynecology_workforce(
  census_file_path = census_file_path,
  output_directory = paste0(output_dir, "/raw_demand"),
  apply_demand_smoothing = FALSE,  # Turn OFF smoothing
  create_visualizations = TRUE,
  verbose = TRUE
)

smoothed_demand_analysis <- analyze_urogynecology_workforce(
  census_file_path = census_file_path,
  output_directory = paste0(output_dir, "/smoothed_demand"),
  apply_demand_smoothing = TRUE,   # Turn ON smoothing
  smoothing_span = 0.3,           # Default smoothing
  create_visualizations = TRUE,
  verbose = TRUE
)

# Compare the two approaches
comparison_plot <- ggplot() +
  geom_line(data = raw_demand_analysis$demand_projections,
            aes(x = year_value, y = required_workforce_fte),
            color = "red", alpha = 0.7, size = 0.8) +
  geom_line(data = smoothed_demand_analysis$demand_projections,
            aes(x = year_value, y = required_workforce_fte),
            color = "darkred", size = 1.2) +
  labs(title = "Raw vs Smoothed Demand Projections",
       subtitle = "Red = Raw census data, Dark Red = Lowess smoothed",
       y = "Required FTE", x = "Year") +
  theme_minimal()

print(comparison_plot)

# Different smoothing levels
light_smoothing <- analyze_urogynecology_workforce(
  census_file_path = census_file_path,
  apply_demand_smoothing = TRUE,
  smoothing_span = 0.1,           # Light smoothing (more responsive)
  create_visualizations = FALSE,
  verbose = FALSE
)

heavy_smoothing <- analyze_urogynecology_workforce(
  census_file_path = census_file_path,
  apply_demand_smoothing = TRUE,
  smoothing_span = 0.7,           # Heavy smoothing (more stable)
  create_visualizations = FALSE,
  verbose = FALSE
)

# Compare smoothing levels
smoothing_comparison <- ggplot() +
  geom_line(data = light_smoothing$demand_projections,
            aes(x = year_value, y = required_workforce_fte),
            color = "orange", size = 1, linetype = "dashed") +
  geom_line(data = smoothed_demand_analysis$demand_projections,
            aes(x = year_value, y = required_workforce_fte),
            color = "darkred", size = 1.2) +
  geom_line(data = heavy_smoothing$demand_projections,
            aes(x = year_value, y = required_workforce_fte),
            color = "navy", size = 1, linetype = "dotted") +
  labs(title = "Effect of Different Smoothing Levels",
       subtitle = "Orange = Light (0.1), Red = Medium (0.3), Navy = Heavy (0.7)",
       y = "Required FTE", x = "Year") +
  theme_minimal()

print(smoothing_comparison)

# -----------------------------------------------------------------------------
# STEP 7: CUSTOMIZED ANALYSIS SCENARIOS
# -----------------------------------------------------------------------------

# Scenario 1: Optimistic training expansion
optimistic_analysis <- analyze_urogynecology_workforce(
  census_file_path = census_file_path,
  initial_workforce_count = 1200,        # Start with more providers
  baseline_new_fellows = 75,             # Increase training by 36%
  baseline_retirements = 45,             # Reduce retirements
  visits_per_woman_annually = 1.3,       # Lower demand assumption
  hours_per_provider_yearly = 1800,      # Higher productivity
  apply_demand_smoothing = TRUE,          # Use smoothed projections
  smoothing_span = 0.4,                  # Slightly more smoothing
  output_directory = paste0(output_dir, "/optimistic"),
  create_visualizations = TRUE,
  verbose = TRUE
)

# Scenario 2: Conservative/pessimistic analysis
conservative_analysis <- analyze_urogynecology_workforce(
  census_file_path = census_file_path,
  initial_workforce_count = 1100,        # Start with fewer providers
  baseline_new_fellows = 45,             # Reduced training
  baseline_retirements = 65,             # Increased retirements
  visits_per_woman_annually = 2.0,       # Higher demand per woman
  hours_per_provider_yearly = 1600,      # Lower productivity
  apply_demand_smoothing = TRUE,          # Use smoothed projections
  smoothing_span = 0.2,                  # Less smoothing for conservative estimates
  output_directory = paste0(output_dir, "/conservative"),
  create_visualizations = TRUE,
  verbose = FALSE  # Less verbose logging
)

# Scenario 3: Focus on specific time period with targeted smoothing
recent_analysis <- analyze_urogynecology_workforce(
  census_file_path = census_file_path,
  initial_workforce_count = 1169,
  baseline_new_fellows = 60,
  baseline_retirements = 50,
  visits_per_woman_annually = 1.5,
  hours_per_provider_yearly = 1728,
  apply_demand_smoothing = TRUE,
  smoothing_span = 0.3,
  output_directory = paste0(output_dir, "/recent_focus"),
  create_visualizations = TRUE,
  verbose = TRUE
)

# -----------------------------------------------------------------------------
# STEP 8: WORKING WITH THE RESULTS
# -----------------------------------------------------------------------------

# Compare scenarios side by side
comparison_data <- bind_rows(
  basic_analysis$workforce_gap_analysis %>%
    mutate(analysis_type = "Basic"),
  optimistic_analysis$workforce_gap_analysis %>%
    mutate(analysis_type = "Optimistic"),
  conservative_analysis$workforce_gap_analysis %>%
    mutate(analysis_type = "Conservative")
)

# Create custom comparison plot
library(ggplot2)
ggplot(comparison_data,
       aes(x = year_value, y = workforce_gap_fte,
           color = paste(analysis_type, supply_scenario_name))) +
  geom_line(size = 1) +
  geom_hline(yintercept = 0, linetype = "dashed") +
  labs(title = "Workforce Gap Comparison Across Analysis Types",
       x = "Year",
       y = "FTE Surplus (+) / Deficit (-)",
       color = "Analysis & Scenario") +
  theme_minimal() +
  theme(legend.position = "bottom")

# Find years with shortages for each scenario
shortage_years <- basic_analysis$workforce_gap_analysis %>%
  filter(workforce_gap_fte < 0) %>%
  group_by(supply_scenario_name) %>%
  summarise(
    first_shortage_year = min(year_value),
    last_shortage_year = max(year_value),
    years_with_shortage = n(),
    worst_shortage_fte = min(workforce_gap_fte),
    .groups = "drop"
  )

print(shortage_years)

# Export results to CSV for further analysis
write_csv(basic_analysis$workforce_projections,
          file.path(output_dir, "supply_projections.csv"))
write_csv(basic_analysis$demand_projections,
          file.path(output_dir, "demand_projections.csv"))
write_csv(basic_analysis$workforce_gap_analysis,
          file.path(output_dir, "gap_analysis.csv"))

# -----------------------------------------------------------------------------
# STEP 9: SENSITIVITY ANALYSIS
# -----------------------------------------------------------------------------

# Test different visit rates with smoothing
visit_rates <- c(1.0, 1.2, 1.5, 1.8, 2.0)
sensitivity_results <- list()

for(i in seq_along(visit_rates)) {
  rate <- visit_rates[i]

  sensitivity_results[[paste0("visits_", rate)]] <- analyze_urogynecology_workforce(
    census_file_path = census_file_path,
    visits_per_woman_annually = rate,
    apply_demand_smoothing = TRUE,      # Use smoothing for cleaner comparisons
    smoothing_span = 0.3,
    output_directory = paste0(output_dir, "/sensitivity_visits_", rate),
    create_visualizations = FALSE,      # Skip plots for sensitivity analysis
    verbose = FALSE
  )
}

# Test different smoothing parameters
smoothing_spans <- c(0.1, 0.2, 0.3, 0.5, 0.7)
smoothing_sensitivity <- list()

for(i in seq_along(smoothing_spans)) {
  span <- smoothing_spans[i]

  smoothing_sensitivity[[paste0("span_", span)]] <- analyze_urogynecology_workforce(
    census_file_path = census_file_path,
    apply_demand_smoothing = TRUE,
    smoothing_span = span,
    output_directory = paste0(output_dir, "/smoothing_span_", span),
    create_visualizations = FALSE,
    verbose = FALSE
  )
}

# Extract key metrics from sensitivity analysis
sensitivity_summary <- map_dfr(names(sensitivity_results), function(scenario_name) {
  result <- sensitivity_results[[scenario_name]]

  # Get status quo scenario gap in 2040
  gap_2040 <- result$workforce_gap_analysis %>%
    filter(supply_scenario_name == "Status Quo", year_value == 2040) %>%
    pull(workforce_gap_fte)

  tibble(
    scenario = scenario_name,
    gap_2040_fte = gap_2040,
    visit_rate = as.numeric(str_extract(scenario_name, "[0-9.]+"))
  )
})

# Plot sensitivity results (now with smoothed data)
ggplot(sensitivity_summary, aes(x = visit_rate, y = gap_2040_fte)) +
  geom_line(size = 1.2) +
  geom_point(size = 3) +
  geom_hline(yintercept = 0, linetype = "dashed") +
  labs(title = "Sensitivity to Visit Rate Assumptions (Smoothed Demand)",
       subtitle = "2040 Workforce Gap - Status Quo Scenario",
       x = "Visits per Woman per Year",
       y = "FTE Gap (Surplus + / Deficit -)") +
  theme_minimal()

# -----------------------------------------------------------------------------
# STEP 10: QUICK DIAGNOSTIC CHECKS
# -----------------------------------------------------------------------------

# Check if your census file is formatted correctly
test_census_read <- function(file_path) {
  tryCatch({
    raw_data <- read_excel(file_path, sheet = "Main series (thousands)", skip = 3)
    cat("✓ File readable\n")
    cat("✓ Dimensions:", nrow(raw_data), "x", ncol(raw_data), "\n")

    # Check for 65+ data
    has_65_plus <- any(grepl("65 years and over", raw_data[[1]], ignore.case = TRUE))
    if(has_65_plus) {
      cat("✓ Contains 65+ population data\n")
    } else {
      cat("✗ Missing 65+ population data\n")
    }

    return(TRUE)
  }, error = function(e) {
    cat("✗ Error reading file:", e$message, "\n")
    return(FALSE)
  })
}

# Test your census file
test_census_read(census_file_path)

# -----------------------------------------------------------------------------
# STEP 11: TROUBLESHOOTING COMMON ISSUES
# -----------------------------------------------------------------------------

# Issue 1: File path problems
if(!file.exists(census_file_path)) {
  cat("Census file not found. Check your path:\n")
  cat("Current path:", census_file_path, "\n")
  cat("Working directory:", getwd(), "\n")
  cat("Files in current directory:", paste(list.files(), collapse = ", "), "\n")
}

# Issue 2: Package conflicts
# If you get namespace conflicts, use explicit package prefixes:
# dplyr::filter() instead of filter()
# readxl::read_excel() instead of read_excel()

# Issue 3: Memory issues with large census files
# If you get memory errors, you can process subsets of years:
# This would require modifying the functions to accept year ranges

# Issue 4: Logging not appearing
# Make sure logger threshold is set correctly:
logger::log_threshold(logger::DEBUG)  # For detailed logs
logger::log_threshold(logger::INFO)   # For basic logs

# Issue 5: Smoothing too aggressive or not enough
# Adjust smoothing_span parameter:
# - Lower values (0.1-0.2): Light smoothing, preserves more variation
# - Medium values (0.3-0.4): Balanced smoothing (recommended)
# - Higher values (0.5-0.7): Heavy smoothing, very stable trends

# Issue 6: Still seeing unrealistic jumps even with smoothing
# This might indicate data quality issues in the census file
# Try examining the raw population data:
test_pop_data <- process_census_population_data(census_file_path, verbose = TRUE)
plot(test_pop_data$year_value, test_pop_data$population_65_plus_total,
     type = "l", main = "Raw Population Data")

# -----------------------------------------------------------------------------
# STEP 12: EXAMPLE OUTPUT INTERPRETATION
# -----------------------------------------------------------------------------

# The results tell you:
# 1. workforce_projections: How many providers we'll have under each scenario
# 2. demand_projections: How many providers we'll need based on population growth
# 3. workforce_gap_analysis: The difference (positive = surplus, negative = shortage)
# 4. summary_statistics: Key metrics for decision making

cat("Example interpretation:\n")
cat("If the Status Quo scenario shows a gap of -45,000 FTE in 2040,\n")
cat("this means we'll be short about 45,000 full-time equivalent providers\n")
cat("to meet the projected demand from the 65+ population.\n")
