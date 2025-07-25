# v4 Merge SWAN .rda Files from Directory ----
#' Merge SWAN .rda Files from Directory
#'
#' This function discovers, loads, and intelligently merges all .rda files from
#' a specified SWAN data directory. It handles different column structures,
#' identifies optimal merge keys, and creates a comprehensive merged dataset
#' suitable for analysis.
#'
#' @param swan_data_directory_path Character string specifying the directory path
#'   containing SWAN .rda data files to be loaded and merged
#' @param merge_strategy Character string specifying the merge approach:
#'   "intelligent" (default), "by_key", "row_bind", or "largest_only"
#' @param output_merged_file_path Character string specifying the path where
#'   the merged dataset should be saved (default: NULL, no file output)
#' @param preferred_key_columns Character vector of preferred column names to use
#'   as merge keys, in order of preference (default: SWAN standard keys)
#' @param create_source_tracking Logical value indicating whether to add a
#'   source file tracking column (default: TRUE)
#' @param verbose_logging Logical value controlling detailed logging output
#'   (default: FALSE)
#'
#' @return A data frame containing the merged SWAN dataset from all .rda files
#'
#' @examples
#' # Example 1: Basic merge with intelligent strategy (SWANID-based)
#' swan_directory <- "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/"
#' merged_swan_dataset <- merge_swan_rda_files_comprehensive(
#'   swan_data_directory_path = swan_directory,
#'   merge_strategy = "intelligent",
#'   output_merged_file_path = NULL,
#'   create_source_tracking = TRUE,
#'   verbose_logging = TRUE
#' )
#' # Output: Complete longitudinal dataset merged by SWANID with all visits
#'
#' # Example 2: Force SWANID merge with comprehensive visit data
#' merged_with_output <- merge_swan_rda_files_comprehensive(
#'   swan_data_directory_path = swan_directory,
#'   merge_strategy = "by_key",
#'   output_merged_file_path = "complete_swan_longitudinal.csv",
#'   preferred_key_columns = c("SWANID", "ARCHID", "participant_id"),
#'   create_source_tracking = TRUE,
#'   verbose_logging = TRUE
#' )
#' # Output: Full longitudinal merge with AGE0, AGE1, AGE2... BMI0, BMI1, BMI2...
#'
#' # Example 3: Comprehensive longitudinal dataset with all visit variables
#' longitudinal_dataset <- merge_swan_rda_files_comprehensive(
#'   swan_data_directory_path = swan_directory,
#'   merge_strategy = "by_key",
#'   output_merged_file_path = "swan_all_visits.rds",
#'   preferred_key_columns = c("SWANID"),
#'   create_source_tracking = FALSE,
#'   verbose_logging = FALSE
#' )
#' # Output: Clean longitudinal dataset with visit-specific variables
#'
#' @importFrom assertthat assert_that
#' @importFrom dplyr full_join bind_rows mutate select arrange
#' @importFrom logger log_info log_warn log_error
#' @importFrom readr write_csv
#' @importFrom stringr str_detect str_extract str_trim
#' @importFrom purrr map_dfr
#'
#' @export
merge_swan_rda_files_comprehensive <- function(swan_data_directory_path,
                                               merge_strategy = "intelligent",
                                               output_merged_file_path = NULL,
                                               preferred_key_columns = c("SWANID", "swanid", "ARCHID", "archid", "ID", "id",
                                                                         "participant_id", "PARTICIPANT_ID",
                                                                         "subject_id", "SUBJECT_ID"),
                                               create_source_tracking = TRUE,
                                               verbose_logging = FALSE) {

  # Initialize logging
  initialize_merger_logging(verbose_mode = verbose_logging)

  # Log function entry and parameters
  logger::log_info("Starting SWAN .rda files merge process")
  logger::log_info("Directory path: {swan_data_directory_path}")
  logger::log_info("Merge strategy: {merge_strategy}")
  logger::log_info("Output file path: {ifelse(is.null(output_merged_file_path), 'None (memory only)', output_merged_file_path)}")
  logger::log_info("Source tracking enabled: {create_source_tracking}")
  logger::log_info("Verbose logging: {verbose_logging}")

  # Validate inputs
  validate_merger_inputs(
    directory_path = swan_data_directory_path,
    strategy = merge_strategy,
    output_path = output_merged_file_path,
    key_columns = preferred_key_columns
  )

  # Discover and load all .rda files
  loaded_datasets_collection <- discover_and_load_swan_files(
    directory_path = swan_data_directory_path,
    verbose_mode = verbose_logging
  )

  # Analyze merge opportunities
  merge_analysis_results <- analyze_merge_opportunities_comprehensive(
    datasets_collection = loaded_datasets_collection,
    preferred_keys = preferred_key_columns,
    verbose_mode = verbose_logging
  )

  # Execute merge strategy
  merged_comprehensive_dataset <- execute_merge_strategy_comprehensive(
    datasets_collection = loaded_datasets_collection,
    merge_analysis = merge_analysis_results,
    strategy = merge_strategy,
    track_sources = create_source_tracking,
    verbose_mode = verbose_logging
  )

  # Save output file if requested
  if (!is.null(output_merged_file_path)) {
    save_merged_dataset_to_file(
      merged_dataset = merged_comprehensive_dataset,
      file_path = output_merged_file_path,
      verbose_mode = verbose_logging
    )
  }

  # Log completion
  logger::log_info("SWAN .rda files merge completed successfully")
  logger::log_info("Final merged dataset: {nrow(merged_comprehensive_dataset)} rows, {ncol(merged_comprehensive_dataset)} columns")

  return(merged_comprehensive_dataset)
}

#' @noRd
initialize_merger_logging <- function(verbose_mode) {
  if (verbose_mode) {
    logger::log_threshold(logger::INFO)
  } else {
    logger::log_threshold(logger::WARN)
  }
}

#' @noRd
validate_merger_inputs <- function(directory_path, strategy, output_path, key_columns) {

  logger::log_info("Validating merger input parameters")

  # Validate directory exists
  assertthat::assert_that(dir.exists(directory_path),
                          msg = paste("Directory does not exist:", directory_path))

  # Validate merge strategy
  valid_strategies <- c("intelligent", "by_key", "row_bind", "largest_only")
  assertthat::assert_that(strategy %in% valid_strategies,
                          msg = paste("Invalid merge strategy. Must be one of:", paste(valid_strategies, collapse = ", ")))

  # Validate output path if provided
  if (!is.null(output_path)) {
    assertthat::assert_that(is.character(output_path),
                            msg = "Output file path must be a character string")

    # Check if output directory exists
    output_directory <- dirname(output_path)
    if (!dir.exists(output_directory)) {
      logger::log_warn("Output directory does not exist and will be created: {output_directory}")
    }
  }

  # Validate key columns if provided
  if (!is.null(key_columns)) {
    assertthat::assert_that(is.character(key_columns),
                            msg = "Preferred key columns must be a character vector")
  }

  logger::log_info("Input validation completed successfully")
}

#' @noRd
discover_and_load_swan_files <- function(directory_path, verbose_mode) {

  logger::log_info("Discovering .rda files in directory: {directory_path}")

  # Find all .rda files
  rda_file_paths <- list.files(path = directory_path,
                               pattern = "\\.rda$",
                               full.names = TRUE,
                               recursive = FALSE)

  assertthat::assert_that(length(rda_file_paths) > 0,
                          msg = paste("No .rda files found in directory:", directory_path))

  logger::log_info("Found {length(rda_file_paths)} .rda files to process")

  # Load each file with comprehensive information
  datasets_collection <- list()

  for (i in seq_along(rda_file_paths)) {
    current_file_path <- rda_file_paths[i]
    current_file_name <- basename(current_file_path)

    logger::log_info("Loading file {i}/{length(rda_file_paths)}: {current_file_name}")

    # Load file information
    file_info <- load_single_rda_file_comprehensive(
      file_path = current_file_path,
      file_name = current_file_name,
      verbose_mode = verbose_mode
    )

    if (!is.null(file_info)) {
      datasets_collection[[current_file_name]] <- file_info
    }
  }

  assertthat::assert_that(length(datasets_collection) > 0,
                          msg = "No valid datasets were loaded from .rda files")

  logger::log_info("Successfully loaded {length(datasets_collection)} datasets")

  return(datasets_collection)
}

#' @noRd
load_single_rda_file_comprehensive <- function(file_path, file_name, verbose_mode) {

  # Create environment for loading
  loading_environment <- new.env()

  tryCatch({
    # Load the .rda file
    load(file_path, envir = loading_environment)

    # Get loaded objects
    loaded_object_names <- ls(loading_environment)

    if (length(loaded_object_names) == 0) {
      logger::log_warn("  No objects found in {file_name}")
      return(NULL)
    }

    # Handle single vs multiple objects
    if (length(loaded_object_names) == 1) {
      dataset_object <- get(loaded_object_names[1], envir = loading_environment)
    } else {
      # Multiple objects - find largest data frame
      logger::log_warn("  Multiple objects in {file_name}: {paste(loaded_object_names, collapse = ', ')}")

      largest_dataset <- NULL
      largest_size <- 0

      for (obj_name in loaded_object_names) {
        obj <- get(obj_name, envir = loading_environment)
        if (is.data.frame(obj) && nrow(obj) > largest_size) {
          largest_dataset <- obj
          largest_size <- nrow(obj)
        }
      }

      dataset_object <- largest_dataset
    }

    # Validate it's a data frame
    if (!is.data.frame(dataset_object)) {
      logger::log_warn("  Loaded object from {file_name} is not a data frame")
      return(NULL)
    }

    # Create comprehensive file information
    file_info <- list(
      dataset = dataset_object,
      file_name = file_name,
      file_path = file_path,
      file_size_mb = round(file.size(file_path) / 1024 / 1024, 2),
      row_count = nrow(dataset_object),
      column_count = ncol(dataset_object),
      column_names = names(dataset_object),
      loaded_objects = loaded_object_names
    )

    if (verbose_mode) {
      logger::log_info("    Rows: {file_info$row_count}, Columns: {file_info$column_count}")
      logger::log_info("    File size: {file_info$file_size_mb} MB")

      # Show sample of columns
      sample_columns <- head(file_info$column_names, 8)
      logger::log_info("    Sample columns: {paste(sample_columns, collapse = ', ')}")
    }

    return(file_info)

  }, error = function(e) {
    logger::log_error("  Error loading {file_name}: {e$message}")
    return(NULL)
  })
}

#' @noRd
analyze_merge_opportunities_comprehensive <- function(datasets_collection, preferred_keys, verbose_mode) {

  logger::log_info("Analyzing merge opportunities across datasets")

  # Extract all unique column names
  all_columns_across_files <- unique(unlist(lapply(datasets_collection, function(x) x$column_names)))

  # Find available key columns
  available_key_columns <- intersect(preferred_keys, all_columns_across_files)

  if (verbose_mode) {
    logger::log_info("Total unique columns across all files: {length(all_columns_across_files)}")
    logger::log_info("Available preferred key columns: {paste(available_key_columns, collapse = ', ')}")
  }

  # Analyze key column coverage
  key_analysis <- list()

  for (key_col in available_key_columns) {
    files_with_key <- character(0)

    for (file_name in names(datasets_collection)) {
      if (key_col %in% datasets_collection[[file_name]]$column_names) {
        files_with_key <- c(files_with_key, file_name)
      }
    }

    key_analysis[[key_col]] <- list(
      files_with_key = files_with_key,
      coverage_count = length(files_with_key),
      coverage_percentage = round(length(files_with_key) / length(datasets_collection) * 100, 1)
    )

    if (verbose_mode && length(files_with_key) > 1) {
      logger::log_info("Key '{key_col}': {length(files_with_key)} files ({key_analysis[[key_col]]$coverage_percentage}%)")
    }
  }

  # Analyze column overlap for row binding potential
  column_overlap_analysis <- analyze_column_overlap_patterns(datasets_collection, verbose_mode)

  # Determine best merge strategies
  best_key_column <- NULL
  best_key_coverage <- 0

  if (length(available_key_columns) > 0) {
    coverage_counts <- sapply(key_analysis, function(x) x$coverage_count)
    best_key_index <- which.max(coverage_counts)
    best_key_column <- names(key_analysis)[best_key_index]
    best_key_coverage <- coverage_counts[best_key_index]
  }

  merge_analysis <- list(
    available_keys = available_key_columns,
    key_analysis = key_analysis,
    best_key_column = best_key_column,
    best_key_coverage = best_key_coverage,
    column_overlap = column_overlap_analysis,
    total_files = length(datasets_collection)
  )

  if (verbose_mode) {
    if (!is.null(best_key_column)) {
      logger::log_info("Best merge key: '{best_key_column}' (covers {best_key_coverage}/{merge_analysis$total_files} files)")
    } else {
      logger::log_info("No suitable merge keys found - will consider alternative strategies")
    }
  }

  return(merge_analysis)
}

#' @noRd
analyze_column_overlap_patterns <- function(datasets_collection, verbose_mode) {

  # Find groups of files with identical column structures
  column_signature_groups <- list()

  for (file_name in names(datasets_collection)) {
    columns <- sort(datasets_collection[[file_name]]$column_names)
    column_signature <- paste(columns, collapse = "|")

    if (column_signature %in% names(column_signature_groups)) {
      column_signature_groups[[column_signature]] <- c(column_signature_groups[[column_signature]], file_name)
    } else {
      column_signature_groups[[column_signature]] <- file_name
    }
  }

  # Identify largest compatible group
  group_sizes <- sapply(column_signature_groups, length)
  largest_group_index <- which.max(group_sizes)
  largest_compatible_group <- column_signature_groups[[largest_group_index]]

  if (verbose_mode) {
    logger::log_info("Column structure analysis:")
    logger::log_info("  {length(column_signature_groups)} unique column structures found")
    logger::log_info("  Largest compatible group: {length(largest_compatible_group)} files")

    if (length(largest_compatible_group) > 1) {
      logger::log_info("  Compatible files: {paste(largest_compatible_group, collapse = ', ')}")
    }
  }

  return(list(
    signature_groups = column_signature_groups,
    largest_compatible_group = largest_compatible_group,
    largest_group_size = length(largest_compatible_group)
  ))
}

#' @noRd
execute_merge_strategy_comprehensive <- function(datasets_collection, merge_analysis, strategy, track_sources, verbose_mode) {

  logger::log_info("Executing merge strategy: {strategy}")

  if (strategy == "intelligent") {
    # Choose best strategy based on analysis
    if (!is.null(merge_analysis$best_key_column) && merge_analysis$best_key_coverage >= 2) {
      logger::log_info("Intelligent strategy: Using key-based merge with '{merge_analysis$best_key_column}'")
      merged_dataset <- execute_key_based_merge(datasets_collection, merge_analysis, track_sources, verbose_mode)
    } else if (merge_analysis$column_overlap$largest_group_size >= 2) {
      logger::log_info("Intelligent strategy: Using row binding for compatible datasets")
      merged_dataset <- execute_row_binding_merge(datasets_collection, merge_analysis, track_sources, verbose_mode)
    } else {
      logger::log_info("Intelligent strategy: Using largest dataset approach")
      merged_dataset <- execute_largest_dataset_approach(datasets_collection, track_sources, verbose_mode)
    }

  } else if (strategy == "by_key") {
    if (!is.null(merge_analysis$best_key_column)) {
      merged_dataset <- execute_key_based_merge(datasets_collection, merge_analysis, track_sources, verbose_mode)
    } else {
      logger::log_warn("No suitable key columns found - falling back to largest dataset")
      merged_dataset <- execute_largest_dataset_approach(datasets_collection, track_sources, verbose_mode)
    }

  } else if (strategy == "row_bind") {
    merged_dataset <- execute_row_binding_merge(datasets_collection, merge_analysis, track_sources, verbose_mode)

  } else if (strategy == "largest_only") {
    merged_dataset <- execute_largest_dataset_approach(datasets_collection, track_sources, verbose_mode)
  }

  return(merged_dataset)
}

#' @noRd
execute_key_based_merge <- function(datasets_collection, merge_analysis, track_sources, verbose_mode) {

  key_column <- merge_analysis$best_key_column
  mergeable_files <- merge_analysis$key_analysis[[key_column]]$files_with_key

  logger::log_info("Executing key-based merge using '{key_column}' on {length(mergeable_files)} files")

  # Start with largest dataset that has the key
  mergeable_datasets <- datasets_collection[mergeable_files]
  dataset_sizes <- sapply(mergeable_datasets, function(x) x$row_count)
  largest_dataset_name <- names(dataset_sizes)[which.max(dataset_sizes)]

  merged_dataset <- mergeable_datasets[[largest_dataset_name]]$dataset

  if (track_sources) {
    merged_dataset$source_file_primary <- largest_dataset_name
  }

  logger::log_info("Starting with largest dataset: {largest_dataset_name} ({nrow(merged_dataset)} rows)")

  # Merge remaining datasets
  remaining_files <- setdiff(mergeable_files, largest_dataset_name)

  for (file_name in remaining_files) {
    current_dataset <- datasets_collection[[file_name]]$dataset

    logger::log_info("Merging: {file_name}")

    # Add source tracking to current dataset if requested
    if (track_sources) {
      current_dataset$source_file_secondary <- file_name
    }

    # Perform full join
    merged_dataset <- dplyr::full_join(
      merged_dataset,
      current_dataset,
      by = key_column,
      suffix = c("", paste0("_", gsub("\\.rda$", "", file_name)))
    )

    logger::log_info("  After merge: {nrow(merged_dataset)} rows, {ncol(merged_dataset)} columns")
  }

  return(merged_dataset)
}

#' @noRd
execute_row_binding_merge <- function(datasets_collection, merge_analysis, track_sources, verbose_mode) {

  compatible_files <- merge_analysis$column_overlap$largest_compatible_group

  logger::log_info("Executing row binding merge on {length(compatible_files)} compatible files")

  if (length(compatible_files) < 2) {
    logger::log_warn("Not enough compatible files for row binding - using largest dataset")
    return(execute_largest_dataset_approach(datasets_collection, track_sources, verbose_mode))
  }

  # Prepare datasets for row binding
  datasets_for_binding <- list()

  for (file_name in compatible_files) {
    current_dataset <- datasets_collection[[file_name]]$dataset

    if (track_sources) {
      current_dataset$source_file <- file_name
    }

    datasets_for_binding[[file_name]] <- current_dataset

    if (verbose_mode) {
      logger::log_info("  Adding to row bind: {file_name} ({nrow(current_dataset)} rows)")
    }
  }

  # Perform row binding
  merged_dataset <- dplyr::bind_rows(datasets_for_binding)

  logger::log_info("Row binding completed: {nrow(merged_dataset)} total rows from {length(compatible_files)} files")

  return(merged_dataset)
}

#' @noRd
execute_largest_dataset_approach <- function(datasets_collection, track_sources, verbose_mode) {

  logger::log_info("Using largest dataset approach")

  # Find largest dataset by row count
  dataset_sizes <- sapply(datasets_collection, function(x) x$row_count)
  largest_dataset_name <- names(dataset_sizes)[which.max(dataset_sizes)]
  largest_dataset <- datasets_collection[[largest_dataset_name]]$dataset

  if (track_sources) {
    largest_dataset$source_file <- largest_dataset_name
  }

  logger::log_info("Selected largest dataset: {largest_dataset_name} ({nrow(largest_dataset)} rows, {ncol(largest_dataset)} columns)")

  if (verbose_mode) {
    # Show what files were not included
    excluded_files <- setdiff(names(datasets_collection), largest_dataset_name)
    if (length(excluded_files) > 0) {
      logger::log_info("Excluded files: {paste(excluded_files, collapse = ', ')}")
    }
  }

  return(largest_dataset)
}

#' @noRd
save_merged_dataset_to_file <- function(merged_dataset, file_path, verbose_mode) {

  logger::log_info("Saving merged dataset to file: {file_path}")

  # Create directory if it doesn't exist
  output_directory <- dirname(file_path)
  if (!dir.exists(output_directory)) {
    dir.create(output_directory, recursive = TRUE)
    logger::log_info("Created output directory: {output_directory}")
  }

  # Determine file format and save accordingly
  file_extension <- tolower(tools::file_ext(file_path))

  tryCatch({
    if (file_extension == "csv") {
      readr::write_csv(merged_dataset, file_path)
    } else if (file_extension == "rds") {
      saveRDS(merged_dataset, file_path)
    } else if (file_extension == "rda" || file_extension == "rdata") {
      save(merged_dataset, file = file_path)
    } else {
      # Default to CSV
      readr::write_csv(merged_dataset, file_path)
      logger::log_warn("Unknown file extension, saved as CSV")
    }

    logger::log_info("Dataset saved successfully to: {file_path}")

  }, error = function(e) {
    logger::log_error("Error saving file: {e$message}")
  })
}

# Run ----
# Test the updated merger that should find SWANID
raw_merged_longitudinal_data <- merge_swan_rda_files_comprehensive(
  swan_data_directory_path = "data/SWAN/",
  merge_strategy = "by_key",
  preferred_key_columns = c("SWANID", "swanid"),
  create_source_tracking = FALSE,
  verbose_logging = TRUE
)

readr::write_rds(raw_merged_longitudinal_data, "data/SWAN/raw_merged_longitudinal_data.rds")

merged_longitudinal_data <- raw_merged_longitudinal_data %>%
  dplyr::select(
    # Static (baseline) variables
    SWANID, HYSTERE, CHILDREN, WORK, MARITALGP, RACE, AGE, STATUS,
    HORMEVER, SMOKER, SMOKING, NUM_CIG, BMI,
    AGE_R_HYST, AGE_R_OOPH, AGE_R_LMP, NUMCHILD,
    AGE0, LANGUAG0, PREPAID0, MEDICAR0, NOINSUR0, SMOKERE0, INVOLEA0,
    INCOME0, WORKLOA0, BMI0, STATUS0,

    # Longitudinal variables (visits 1–10)
    dplyr::matches("^AGE[1-9]?$"),  # AGE1 to AGE9
    AGE10,
    dplyr::matches("^HYSTERE[1-9]?$"), HYSTERE10,
    dplyr::matches("^OOPHORE[1-9]?$"), OOPHORE10,
    dplyr::matches("^MARITAL[1-9]?$"), MARITAL10,
    dplyr::matches("^STATUS[1-9]?$"), STATUS10,
    dplyr::matches("^INSURAN[4-9]?$"), INSURAN10,
    dplyr::matches("^SMOKERE[1-9]?$"), SMOKERE10,
    dplyr::matches("^WORKPHY[1-4]?$"),
    dplyr::matches("^INVOLEA[1-6]?$"), INVOLEA10,
    dplyr::matches("^WORKLOA[1-9]?$"), WORKLOA10,
    dplyr::matches("^INCOME[1-9]?$"), INCOME10,
    dplyr::matches("^BMI[1-9]?$"), BMI10,

    # UI-specific leak variables
    DAYSLEA0, AMTLEAK0,
    DAYSLEA1, AMTLEAK1,
    DAYSLEA2, AMTLEAK2,
    DAYSLEA3, AMTLEAK3,
    DAYSLEA4, AMTLEAK4,
    DAYSLEA5, DAYSLEA6,
    LEKDAYS7, LEKAMNT7,
    LEKDAYS8, LEKAMNT8,
    LEKDAYS9, LEKAMNT9,
    DAYSLEA10, LEKAMNT10
  ) %>%
  mutate(across(c(BMI, BMI0, BMI1, BMI2, BMI3, BMI4, BMI5, BMI6, BMI7, BMI8, BMI9, BMI10), ~ (round(., digits = 1))))

readr::write_rds(merged_longitudinal_data, "data/SWAN/merged_longitudinal_data.rds")
