
# v4 #' Merge SWAN .rda Files from Directory ----
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
merged_longitudinal_data <- merge_swan_rda_files_comprehensive(
  swan_data_directory_path = "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/",
  merge_strategy = "by_key",
  preferred_key_columns = c("SWANID", "swanid"),
  create_source_tracking = FALSE,
  verbose_logging = TRUE
)

merged_longitudinal_data <- merged_longitudinal_data %>%
  select(SWANID, HYSTERE, CHILDREN, WORK, MARITALGP, RACE, AGE, STATUS, HORMEVER, SMOKER, SMOKING, NUM_CIG, BMI, AGE_R_HYST, AGE_R_OOPH, AGE_R_LMP, NUMCHILD, AGE0, LANGUAG0, PREPAID0, MEDICAR0, NOINSUR0, SMOKERE0, INVOLEA0, INCOME0, WORKLOA0, BMI0, STATUS0, AGE1, HYSTERE1, OOPHORE1, MARITAL1, SMOKERE1, WORKPHY1, INVOLEA1, WORKLOA1, INCOME1, SMOKER1, BMI1, STATUS1, AGE2, HYSTERE2, OOPHORE2, MARITAL2, STATUS2, SMOKERE2, WORKPHY2, INVOLEA2, WORKLOA2, INCOME2, BMI2, AGE3, HYSTERE3, OOPHORE3, MARITAL3, STATUS3, SMOKERE3, WORKPHY3, INVOLEA3, WORKLOA3, INCOME3, BMI3, AGE4, HYSTERE4, OOPHORE4, MARITAL4, INSURAN4, SMOKERE4, WORKPHY4, INVOLEA4, WORKLOA4, INCOME4, BMI4, STATUS4, AGE5, HYSTERE5, OOPHORE5, MARITAL5, STATUS5, INSURAN5, SMOKERE5, INVOLEA5, WORKLOA5, INCOME5, BMI5, AGE6, HYSTERE6, OOPHORE6, MARITAL6, STATUS6, INSURAN6, SMOKERE6, INVOLEA6, WORKLOA6, INCOME6, BMI6, AGE7, HYSTERE7, OOPHORE7, MARITAL7, STATUS7, INSURAN7, SMOKERE7, WORKLOA7, INCOME7, BMI7, AGE8, HYSTERE8, OOPHORE8, MARITAL8, INSURAN8, SMOKERE8, WORKLOA8, INCOME8, BMI8, STATUS8, AGE9, HYSTERE9, OOPHORE9, MARITAL9, INSURAN9, SMOKERE9, WORKLOA9, INCOME9, BMI9, STATUS9, AGE10, HYSTERE10, OOPHORE10, MARITAL10, STATUS10, INSURAN10, SMOKERE10, INVOLEA10, WORKLOA10, INCOME10, BMI10) %>%
  arrange(SWANID) %>%
  exploratory::reorder_cols(SWANID, AGE0, AGE, CHILDREN, NUMCHILD, RACE, HYSTERE, WORK, MARITALGP, STATUS, HORMEVER, SMOKER, SMOKING, NUM_CIG, BMI, AGE_R_HYST, AGE_R_OOPH, AGE_R_LMP, LANGUAG0, PREPAID0, MEDICAR0, NOINSUR0, SMOKERE0, INVOLEA0, INCOME0, WORKTRB0, WORKLOA0, BMI0, STATUS0, AGE1, HYSTERE1, OOPHORE1, MARITAL1, SMOKERE1, WORKPHY1, INVOLEA1, WORKTRB1, WORKLOA1, INCOME1, SMOKER1, BMI1, STATUS1, AGE2, HYSTERE2, OOPHORE2, MARITAL2, STATUS2, SMOKERE2, WORKPHY2, INVOLEA2, WORKTRB2, WORKLOA2, INCOME2, LANGSAB2, LANGSAC2, BMI2, AGE3, HYSTERE3, OOPHORE3, MARITAL3, STATUS3, LANGSAA3, SMOKERE3, WORKTIR3, WORKPHY3, INVOLEA3, WORKTRB3, WORKLOA3, INCOME3, LANGSAB3, LANGSAC3, BMI3, AGE4, HYSTERE4, OOPHORE4, MARITAL4, LANGSAA4, INSURAN4, SMOKERE4, WORKPHY4, INVOLEA4, WORKTRB4, WORKLOA4, INCOME4, LANGSAB4, BMI4, STATUS4, AGE5, HYSTERE5, OOPHORE5, MARITAL5, STATUS5, LANGSAA5, INSURAN5, SMOKERE5, INVOLEA5, WORKTRB5, WORKLOA5, INCOME5, LANGSAB5, BMI5, AGE6, HYSTERE6, OOPHORE6, MARITAL6, STATUS6, LANGSAA6, INSURAN6, SMOKERE6, WORKTIR6, INVOLEA6, WORKTRB6, WORKLOA6, INCOME6, LANGSAB6, BMI6, AGE7, HYSTERE7, OOPHORE7, MARITAL7, STATUS7, LANGSAA7, INSURAN7, SMOKERE7, WORKTRB7, WORKLOA7, INCOME7, BMI7, AGE8, HYSTERE8, OOPHORE8, MARITAL8, LANGSAA8, INSURAN8, SMOKERE8, WORKTRB8, WORKLOA8, INCOME8, LANGSAB8, BMI8, STATUS8, AGE9, HYSTERE9, OOPHORE9, MARITAL9, LANGSAA9, INSURAN9, SMOKERE9, WORKTRB9, WORKLOA9, WORKTIR9, INCOME9, BMI9, STATUS9, AGE10, HYSTERE10, OOPHORE10, MARITAL10, STATUS10, LANGSAA10, INSURAN10, SMOKERE10, INVOLEA10, WORKTRB10, WORKLOA10, INCOME10, LANGSAB10, BMI10) %>%
  mutate(across(c(BMI, BMI0, BMI1, BMI2, BMI3, BMI4, BMI5, BMI6, BMI7, BMI8, BMI9, BMI10), ~ (round(., digits = 1))))

readr::write_rds(merged_longitudinal_data, "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/merged_longitudinal_data.rds")



#' # Read in SWAN files
#'
#'
#' # version 7 Create Comprehensive SWAN Dataset with Visit-Specific Variables ----
#' #' Create Comprehensive SWAN Dataset with Visit-Specific Variables
#' #'
#' #' This function creates a comprehensive dataset from SWAN study files, extracting
#' #' baseline demographics and visit-specific variables including urinary incontinence
#' #' measures (INVOLEA/LEKINVO) and other longitudinal health indicators.
#' #'
#' #' @param swan_data_directory Character. Path to directory containing SWAN .rda files
#' #' @param output_format Character. Either "long" or "wide" format for final dataset
#' #' @param verbose Logical. Enable detailed logging output to console
#' #' @param include_visit_demographics Logical. Include visit-specific demographic
#' #'   variables (AGE, BMI, STATUS, etc.) in addition to baseline
#' #' @param filter_valid_responses Logical. Remove rows with missing INVOLEA responses
#' #'
#' #' @return tibble. Comprehensive SWAN dataset with baseline and visit-specific variables
#' #'
#' #' @examples
#' #' # Example 1: Basic long format with visit demographics
#' #' swan_long_comprehensive <- create_comprehensive_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "long",
#' #'   verbose = TRUE,
#' #'   include_visit_demographics = TRUE,
#' #'   filter_valid_responses = TRUE
#' #' )
#' #'
#' #' # Example 2: Wide format for analysis, no filtering
#' #' swan_wide_complete <- create_comprehensive_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "wide",
#' #'   verbose = FALSE,
#' #'   include_visit_demographics = TRUE,
#' #'   filter_valid_responses = FALSE
#' #' )
#' #'
#' #' # Example 3: Minimal dataset with only baseline + INVOLEA
#' #' swan_minimal <- create_comprehensive_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "long",
#' #'   verbose = TRUE,
#' #'   include_visit_demographics = FALSE,
#' #'   filter_valid_responses = TRUE
#' #' )
#' #'
#' #' @importFrom dplyr filter select mutate arrange group_by summarise
#' #'   left_join full_join distinct coalesce case_when all_of any_of ends_with
#' #' @importFrom tidyr pivot_longer pivot_wider
#' #' @importFrom stringr str_detect str_extract str_replace_all str_starts
#' #'   str_remove str_match
#' #' @importFrom purrr map_dfr map_chr map_lgl keep walk compact map
#' #' @importFrom assertthat assert_that
#' #' @importFrom logger log_info log_warn log_error
#' #' @importFrom tibble tibble as_tibble
#' #' @importFrom readr parse_number
#' #' @importFrom glue glue
#' #'
#' #' @export
#' create_comprehensive_swan_dataset <- function(swan_data_directory,
#'                                               output_format = "long",
#'                                               verbose = TRUE,
#'                                               include_visit_demographics = TRUE,
#'                                               filter_valid_responses = TRUE) {
#'
#'   # Input validation with assertthat
#'   if (verbose) {
#'     logger::log_info("Creating comprehensive SWAN dataset with visit-specific variables")
#'     logger::log_info("Function inputs - directory: {swan_data_directory}")
#'     logger::log_info("Function inputs - format: {output_format}, verbose: {verbose}")
#'     logger::log_info("Function inputs - include_visit_demographics: {include_visit_demographics}")
#'     logger::log_info("Function inputs - filter_valid_responses: {filter_valid_responses}")
#'   }
#'
#'   assertthat::assert_that(
#'     is.character(swan_data_directory),
#'     msg = "swan_data_directory must be a character string path"
#'   )
#'   assertthat::assert_that(
#'     dir.exists(swan_data_directory),
#'     msg = glue::glue("Directory does not exist: {swan_data_directory}")
#'   )
#'   assertthat::assert_that(
#'     output_format %in% c("long", "wide"),
#'     msg = "output_format must be 'long' or 'wide'"
#'   )
#'   assertthat::assert_that(
#'     is.logical(verbose),
#'     msg = "verbose must be TRUE or FALSE"
#'   )
#'   assertthat::assert_that(
#'     is.logical(include_visit_demographics),
#'     msg = "include_visit_demographics must be TRUE or FALSE"
#'   )
#'   assertthat::assert_that(
#'     is.logical(filter_valid_responses),
#'     msg = "filter_valid_responses must be TRUE or FALSE"
#'   )
#'
#'   # Step 1: Extract baseline demographics
#'   baseline_participant_demographics <- extract_baseline_participant_demographics(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Baseline demographics extracted: {nrow(baseline_participant_demographics)} participants")
#'   }
#'
#'   # Step 2: Extract comprehensive longitudinal data
#'   longitudinal_health_measurements <- extract_comprehensive_longitudinal_data(
#'     directory_path = swan_data_directory,
#'     include_visit_demographics = include_visit_demographics,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Longitudinal data extracted: {nrow(longitudinal_health_measurements)} observations")
#'   }
#'
#'   # Step 3: Merge datasets
#'   merged_comprehensive_dataset <- merge_baseline_with_longitudinal_data(
#'     baseline_data = baseline_participant_demographics,
#'     longitudinal_data = longitudinal_health_measurements,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Data merged: {nrow(merged_comprehensive_dataset)} total observations")
#'   }
#'
#'   # Step 4: Apply filtering if requested
#'   if (filter_valid_responses) {
#'     filtered_comprehensive_dataset <- apply_response_filtering(
#'       merged_data = merged_comprehensive_dataset,
#'       verbose = verbose
#'     )
#'   } else {
#'     filtered_comprehensive_dataset <- merged_comprehensive_dataset
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("After filtering: {nrow(filtered_comprehensive_dataset)} observations")
#'   }
#'
#'   # Step 5: Format final output
#'   final_comprehensive_dataset <- format_comprehensive_output(
#'     merged_data = filtered_comprehensive_dataset,
#'     format = output_format,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Comprehensive SWAN dataset creation completed")
#'     logger::log_info("Final dataset dimensions: {nrow(final_comprehensive_dataset)} rows × {ncol(final_comprehensive_dataset)} columns")
#'     logger::log_info("Output file path: Not saved to file (returned as object)")
#'   }
#'
#'   return(final_comprehensive_dataset)
#' }
#'
#' #' Extract baseline demographics from SWAN screener/baseline files
#' #' @noRd
#' extract_baseline_participant_demographics <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting baseline participant demographics from SWAN files")
#'   }
#'
#'   # Find baseline/screener files using known ICPSR numbers
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   # Known baseline file patterns from SWAN documentation
#'   baseline_file_identifiers <- c("28762", "04368", "baseline", "screener")
#'
#'   baseline_candidate_files <- purrr::keep(all_rda_files, function(file_path) {
#'     file_name <- tolower(basename(file_path))
#'     any(stringr::str_detect(file_name, baseline_file_identifiers))
#'   })
#'
#'   if (verbose) {
#'     logger::log_info("Found {length(baseline_candidate_files)} potential baseline files")
#'     purrr::walk(baseline_candidate_files, ~logger::log_info("  Candidate file: {basename(.x)}"))
#'   }
#'
#'   # Process each baseline file
#'   baseline_dataset_list <- purrr::map(baseline_candidate_files, function(file_path) {
#'     process_single_baseline_file(file_path, verbose)
#'   })
#'
#'   # Remove NULL results and combine
#'   baseline_dataset_list <- purrr::compact(baseline_dataset_list)
#'
#'   if (length(baseline_dataset_list) == 0) {
#'     logger::log_error("No valid baseline demographic data found in any files")
#'     stop("No baseline demographic data could be extracted")
#'   }
#'
#'   # Combine multiple baseline sources
#'   combined_baseline_demographics <- combine_multiple_baseline_sources(
#'     baseline_dataset_list,
#'     verbose
#'   )
#'
#'   return(combined_baseline_demographics)
#' }
#'
#' #' Process a single baseline file to extract demographics
#' #' @noRd
#' process_single_baseline_file <- function(file_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Processing baseline file: {basename(file_path)}")
#'   }
#'
#'   # Load the file safely
#'   file_environment <- new.env()
#'
#'   tryCatch({
#'     load(file_path, envir = file_environment)
#'   }, error = function(e) {
#'     if (verbose) {
#'       logger::log_error("Failed to load {basename(file_path)}: {e$message}")
#'     }
#'     return(NULL)
#'   })
#'
#'   if (length(ls(file_environment)) == 0) {
#'     if (verbose) {
#'       logger::log_warn("No datasets found in {basename(file_path)}")
#'     }
#'     return(NULL)
#'   }
#'
#'   raw_baseline_dataset <- file_environment[[ls(file_environment)[1]]]
#'
#'   # Identify demographic variables
#'   demographic_variable_mapping <- identify_baseline_demographic_variables(
#'     raw_baseline_dataset
#'   )
#'
#'   if (is.null(demographic_variable_mapping$participant_id)) {
#'     if (verbose) {
#'       logger::log_warn("No participant ID found in {basename(file_path)}")
#'     }
#'     return(NULL)
#'   }
#'
#'   # Extract and standardize demographics
#'   standardized_baseline_demographics <- extract_standardized_demographics(
#'     dataset = raw_baseline_dataset,
#'     variable_mapping = demographic_variable_mapping,
#'     source_file = basename(file_path),
#'     verbose = verbose
#'   )
#'
#'   return(standardized_baseline_demographics)
#' }
#'
#' #' Identify demographic variables in baseline dataset
#' #' @noRd
#' identify_baseline_demographic_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Participant ID patterns
#'   participant_id_patterns <- c("^SWANID$", "^ARCHID$", "^swanid$", "^archid$")
#'   participant_id_variable <- find_matching_variable(variable_names, participant_id_patterns)
#'
#'   # Demographic variable patterns (baseline variables often end with 0)
#'   age_patterns <- c("^AGE0?$", "^age0?$", "AGE.*0$")
#'   bmi_patterns <- c("^BMI0?$", "^bmi0?$", "BMI.*0$")
#'   race_patterns <- c("^RACE$", "^race$", "^ETHNIC")
#'   smoking_patterns <- c("^SMOKER0?$", "^SMOKING0?$", "^SMOKE")
#'   parity_patterns <- c("^NUMCHILD$", "^CHILDREN$", "^PARITY")
#'   menopausal_patterns <- c("^STATUS0?$", "^MENO")
#'   education_patterns <- c("^DEGREE$", "^EDUCATION", "^EDUC")
#'   marital_patterns <- c("^MARITAL", "^MARITALGP")
#'
#'   return(list(
#'     participant_id = participant_id_variable,
#'     age = find_matching_variable(variable_names, age_patterns),
#'     bmi = find_matching_variable(variable_names, bmi_patterns),
#'     race = find_matching_variable(variable_names, race_patterns),
#'     smoking = find_matching_variable(variable_names, smoking_patterns),
#'     parity = find_matching_variable(variable_names, parity_patterns),
#'     menopausal = find_matching_variable(variable_names, menopausal_patterns),
#'     education = find_matching_variable(variable_names, education_patterns),
#'     marital = find_matching_variable(variable_names, marital_patterns)
#'   ))
#' }
#'
#' #' Find first matching variable from patterns
#' #' @noRd
#' find_matching_variable <- function(variable_names, patterns) {
#'   for (pattern in patterns) {
#'     matches <- variable_names[stringr::str_detect(variable_names, pattern)]
#'     if (length(matches) > 0) {
#'       return(matches[1])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' Extract and standardize demographic variables
#' #' @noRd
#' extract_standardized_demographics <- function(dataset, variable_mapping,
#'                                               source_file, verbose) {
#'
#'   # Start with participant ID
#'   standardized_demographics <- tibble::tibble(
#'     swan_participant_id = as.character(dataset[[variable_mapping$participant_id]])
#'   )
#'
#'   # Add demographic variables with type-safe conversion
#'   if (!is.null(variable_mapping$age)) {
#'     standardized_demographics$baseline_age_years <-
#'       convert_to_numeric_safely(dataset[[variable_mapping$age]])
#'   }
#'
#'   if (!is.null(variable_mapping$bmi)) {
#'     standardized_demographics$baseline_bmi_kg_m2 <-
#'       convert_to_numeric_safely(dataset[[variable_mapping$bmi]])
#'   }
#'
#'   if (!is.null(variable_mapping$race)) {
#'     standardized_demographics$baseline_race_ethnicity <-
#'       standardize_race_ethnicity_variable(dataset[[variable_mapping$race]])
#'   }
#'
#'   if (!is.null(variable_mapping$smoking)) {
#'     standardized_demographics$baseline_smoking_status <-
#'       standardize_smoking_status_variable(dataset[[variable_mapping$smoking]])
#'   }
#'
#'   if (!is.null(variable_mapping$parity)) {
#'     standardized_demographics$baseline_parity_count <-
#'       convert_to_numeric_safely(dataset[[variable_mapping$parity]])
#'   }
#'
#'   if (!is.null(variable_mapping$menopausal)) {
#'     standardized_demographics$baseline_menopausal_stage <-
#'       standardize_menopausal_status_variable(dataset[[variable_mapping$menopausal]])
#'   }
#'
#'   if (!is.null(variable_mapping$education)) {
#'     standardized_demographics$baseline_education_level <-
#'       standardize_education_variable(dataset[[variable_mapping$education]])
#'   }
#'
#'   if (!is.null(variable_mapping$marital)) {
#'     standardized_demographics$baseline_marital_status <-
#'       standardize_marital_status_variable(dataset[[variable_mapping$marital]])
#'   }
#'
#'   standardized_demographics$source_file <- source_file
#'
#'   if (verbose) {
#'     valid_participant_count <- sum(!is.na(standardized_demographics$swan_participant_id))
#'     demographic_variable_count <- ncol(standardized_demographics) - 2  # exclude ID and source
#'     logger::log_info("  Extracted {valid_participant_count} participants with {demographic_variable_count} demographic variables")
#'   }
#'
#'   return(standardized_demographics)
#' }
#'
#' #' Extract comprehensive longitudinal data including visit-specific variables
#' #' @noRd
#' extract_comprehensive_longitudinal_data <- function(directory_path,
#'                                                     include_visit_demographics,
#'                                                     verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting comprehensive longitudinal data from all SWAN files")
#'   }
#'
#'   # Find all RDA files
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   # Process each file for longitudinal variables
#'   longitudinal_dataset_list <- purrr::map(all_rda_files, function(file_path) {
#'     process_single_longitudinal_file(
#'       file_path = file_path,
#'       include_visit_demographics = include_visit_demographics,
#'       verbose = verbose
#'     )
#'   })
#'
#'   # Remove NULL results
#'   longitudinal_dataset_list <- purrr::compact(longitudinal_dataset_list)
#'
#'   if (length(longitudinal_dataset_list) == 0) {
#'     logger::log_error("No longitudinal data found in any files")
#'     stop("No longitudinal data could be extracted")
#'   }
#'
#'   # Combine all longitudinal datasets
#'   combined_longitudinal_data <- purrr::map_dfr(longitudinal_dataset_list, ~.x)
#'
#'   # Remove duplicate observations (same participant, visit, variable)
#'   deduped_longitudinal_data <- combined_longitudinal_data %>%
#'     dplyr::distinct(
#'       swan_participant_id,
#'       visit_number,
#'       incontinence_source_variable,
#'       .keep_all = TRUE
#'     )
#'
#'   if (verbose) {
#'     unique_participant_count <- length(unique(deduped_longitudinal_data$swan_participant_id))
#'     visit_range <- paste(
#'       min(deduped_longitudinal_data$visit_number, na.rm = TRUE), "to",
#'       max(deduped_longitudinal_data$visit_number, na.rm = TRUE)
#'     )
#'     logger::log_info("Comprehensive longitudinal data extracted: {nrow(deduped_longitudinal_data)} observations")
#'     logger::log_info("Visit range: {visit_range}")
#'     logger::log_info("Unique participants with longitudinal data: {unique_participant_count}")
#'   }
#'
#'   return(deduped_longitudinal_data)
#' }
#'
#' #' Process single file for longitudinal variables
#' #' @noRd
#' process_single_longitudinal_file <- function(file_path, include_visit_demographics, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Scanning {basename(file_path)} for longitudinal variables")
#'   }
#'
#'   # Load file safely
#'   file_environment <- new.env()
#'
#'   tryCatch({
#'     load(file_path, envir = file_environment)
#'   }, error = function(e) {
#'     if (verbose) {
#'       logger::log_warn("Failed to load {basename(file_path)}: {e$message}")
#'     }
#'     return(NULL)
#'   })
#'
#'   if (length(ls(file_environment)) == 0) return(NULL)
#'
#'   raw_longitudinal_dataset <- file_environment[[ls(file_environment)[1]]]
#'
#'   # Look for incontinence variables (INVOLEA/LEKINVO)
#'   incontinence_variable_names <- identify_incontinence_variables(raw_longitudinal_dataset)
#'
#'   if (length(incontinence_variable_names) == 0) {
#'     return(NULL)  # No incontinence variables in this file
#'   }
#'
#'   if (verbose) {
#'     incontinence_vars_string <- paste(incontinence_variable_names, collapse = ", ")
#'     logger::log_info("  Found {length(incontinence_variable_names)} incontinence variables: {incontinence_vars_string}")
#'   }
#'
#'   # Extract longitudinal data
#'   extracted_longitudinal_data <- extract_longitudinal_measurements(
#'     dataset = raw_longitudinal_dataset,
#'     incontinence_variables = incontinence_variable_names,
#'     include_visit_demographics = include_visit_demographics,
#'     source_file = basename(file_path),
#'     verbose = verbose
#'   )
#'
#'   return(extracted_longitudinal_data)
#' }
#'
#' #' Identify incontinence variables (INVOLEA/LEKINVO) in dataset
#' #' @noRd
#' identify_incontinence_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Look for INVOLEA and LEKINVO patterns
#'   incontinence_patterns <- c("INVOLEA\\d*", "LEKINVO\\d*", "involea\\d*", "lekinvo\\d*")
#'
#'   incontinence_variables <- c()
#'   for (pattern in incontinence_patterns) {
#'     matching_variables <- variable_names[stringr::str_detect(variable_names, pattern)]
#'     incontinence_variables <- c(incontinence_variables, matching_variables)
#'   }
#'
#'   return(unique(incontinence_variables))
#' }
#'
#' #' Extract longitudinal measurements including visit-specific demographics
#' #' @noRd
#' extract_longitudinal_measurements <- function(dataset, incontinence_variables,
#'                                               include_visit_demographics,
#'                                               source_file, verbose) {
#'
#'   # Identify core variables
#'   participant_id_variable <- identify_participant_id_variable(dataset)
#'   visit_number_variable <- identify_visit_number_variable(dataset)
#'
#'   if (is.null(participant_id_variable)) {
#'     if (verbose) {
#'       logger::log_warn("No participant ID found in {source_file}")
#'     }
#'     return(NULL)
#'   }
#'
#'   # Start with core columns
#'   core_column_names <- c(participant_id_variable)
#'   if (!is.null(visit_number_variable)) {
#'     core_column_names <- c(core_column_names, visit_number_variable)
#'   }
#'
#'   # Identify ALL visit-specific demographic variables if requested
#'   visit_specific_variables <- list()
#'
#'   if (include_visit_demographics) {
#'     visit_specific_variables <- identify_visit_specific_demographic_variables(dataset)
#'
#'     if (verbose && length(visit_specific_variables) > 0) {
#'       total_visit_vars <- sum(lengths(visit_specific_variables))
#'       visit_vars_string <- paste(names(visit_specific_variables), collapse = ", ")
#'       logger::log_info("  Visit-specific demographics found: {visit_vars_string} (total: {total_visit_vars} variables)")
#'     }
#'   }
#'
#'   # Combine all variables to extract - flatten the visit-specific variables list
#'   all_visit_specific_vars <- unlist(visit_specific_variables)
#'   all_variables_to_extract <- c(
#'     core_column_names,
#'     all_visit_specific_vars,  # Now includes ALL visit-specific variables
#'     incontinence_variables
#'   )
#'
#'   # Extract data
#'   longitudinal_measurements <- tibble::as_tibble(dataset) %>%
#'     dplyr::select(dplyr::all_of(all_variables_to_extract))
#'
#'   # Standardize core column names
#'   names(longitudinal_measurements)[1] <- "swan_participant_id"
#'   if (!is.null(visit_number_variable)) {
#'     names(longitudinal_measurements)[2] <- "visit_number_from_file"
#'   } else {
#'     longitudinal_measurements$visit_number_from_file <- NA_real_
#'   }
#'
#'   # Convert incontinence variables to long format first
#'   incontinence_long_format <- longitudinal_measurements %>%
#'     tidyr::pivot_longer(
#'       cols = dplyr::all_of(incontinence_variables),
#'       names_to = "incontinence_source_variable",
#'       values_to = "incontinence_raw_response"
#'     ) %>%
#'     dplyr::mutate(
#'       swan_participant_id = as.character(swan_participant_id),
#'       # Extract visit number from incontinence variable name
#'       visit_number = extract_visit_number_from_variable(incontinence_source_variable),
#'       incontinence_status = standardize_incontinence_response(incontinence_raw_response),
#'       years_since_baseline = visit_number,
#'       source_file = source_file
#'     ) %>%
#'     dplyr::select(-visit_number_from_file) %>%
#'     dplyr::filter(!is.na(incontinence_raw_response))  # Remove missing responses
#'
#'   # Now add visit-specific demographic variables that match the visit number
#'   if (include_visit_demographics && length(visit_specific_variables) > 0) {
#'
#'     for (demographic_type in names(visit_specific_variables)) {
#'       variable_names_for_this_type <- visit_specific_variables[[demographic_type]]
#'
#'       # Create a mapping of visit numbers to values for this demographic type
#'       visit_demographic_mapping <- tibble::tibble()
#'
#'       for (var_name in variable_names_for_this_type) {
#'         # Extract visit number from variable name (e.g., AGE3 -> 3, BMI0 -> 0)
#'         var_visit_number <- extract_visit_number_from_variable(var_name)
#'
#'         # Get the values for this variable
#'         var_values <- dataset[[var_name]]
#'
#'         # Create temporary mapping
#'         temp_mapping <- tibble::tibble(
#'           swan_participant_id = as.character(dataset[[participant_id_variable]]),
#'           visit_number = var_visit_number,
#'           !!paste0("visit_", demographic_type) := var_values
#'         ) %>%
#'           dplyr::filter(!is.na(.data[[paste0("visit_", demographic_type)]]))
#'
#'         # Combine with existing mapping
#'         if (nrow(visit_demographic_mapping) == 0) {
#'           visit_demographic_mapping <- temp_mapping
#'         } else {
#'           visit_demographic_mapping <- dplyr::bind_rows(visit_demographic_mapping, temp_mapping)
#'         }
#'       }
#'
#'       # Merge this demographic variable with the incontinence data
#'       if (nrow(visit_demographic_mapping) > 0) {
#'         incontinence_long_format <- incontinence_long_format %>%
#'           dplyr::left_join(
#'             visit_demographic_mapping,
#'             by = c("swan_participant_id", "visit_number")
#'           )
#'       }
#'     }
#'   }
#'
#'   # Log results
#'   if (verbose && nrow(incontinence_long_format) > 0) {
#'     unique_visits <- sort(unique(incontinence_long_format$visit_number))
#'     visits_string <- paste(unique_visits, collapse = ", ")
#'     logger::log_info("  Extracted visits for {source_file}: {visits_string}")
#'     logger::log_info("  Total observations in {source_file}: {nrow(incontinence_long_format)}")
#'
#'     # Log visit-specific variables preserved
#'     visit_specific_columns <- names(incontinence_long_format)[stringr::str_starts(names(incontinence_long_format), "visit_")]
#'     if (length(visit_specific_columns) > 0) {
#'       visit_vars_preserved <- paste(stringr::str_remove(visit_specific_columns, "^visit_"), collapse = ", ")
#'       logger::log_info("  Visit-specific variables preserved: {visit_vars_preserved}")
#'
#'       # Sample a few values to verify they're changing across visits
#'       if ("visit_bmi_kg_m2" %in% names(incontinence_long_format)) {
#'         sample_participant <- incontinence_long_format$swan_participant_id[1]
#'         sample_bmi_values <- incontinence_long_format %>%
#'           dplyr::filter(swan_participant_id == sample_participant) %>%
#'           dplyr::select(visit_number, visit_bmi_kg_m2) %>%
#'           dplyr::arrange(visit_number)
#'
#'         if (nrow(sample_bmi_values) > 1) {
#'           bmi_range <- paste(round(range(sample_bmi_values$visit_bmi_kg_m2, na.rm = TRUE), 1), collapse = " to ")
#'           logger::log_info("  Sample BMI range for participant {sample_participant}: {bmi_range}")
#'         }
#'       }
#'     }
#'   }
#'
#'   return(incontinence_long_format)
#' }
#'
#' #' Identify visit-specific demographic variables in dataset
#' #' @noRd
#' identify_visit_specific_demographic_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'   visit_specific_vars <- list()
#'
#'   # AGE variables - match only single digit visit numbers (AGE0, AGE1, AGE2, ..., AGE9)
#'   # Also match AGE10 specifically, but exclude AGE11, AGE21, etc.
#'   age_variables <- variable_names[stringr::str_detect(variable_names, "^AGE(\\d|10)$")]
#'   if (length(age_variables) > 0) {
#'     visit_specific_vars[["age_years"]] <- age_variables
#'   }
#'
#'   # BMI variables - match only single digit visit numbers (BMI0, BMI1, BMI2, ..., BMI9, BMI10)
#'   bmi_variables <- variable_names[stringr::str_detect(variable_names, "^BMI(\\d|10)$")]
#'   if (length(bmi_variables) > 0) {
#'     visit_specific_vars[["bmi_kg_m2"]] <- bmi_variables
#'   }
#'
#'   # Weight variables - match only single digit visit numbers
#'   weight_variables <- variable_names[stringr::str_detect(variable_names, "^WEIGHT(\\d|10)$")]
#'   if (length(weight_variables) > 0) {
#'     visit_specific_vars[["weight_kg"]] <- weight_variables
#'   }
#'
#'   # Height variables - match only single digit visit numbers
#'   height_variables <- variable_names[stringr::str_detect(variable_names, "^HEIGHT(\\d|10)$")]
#'   if (length(height_variables) > 0) {
#'     visit_specific_vars[["height_cm"]] <- height_variables
#'   }
#'
#'   # Smoking variables - match only single digit visit numbers
#'   smoking_variables <- variable_names[stringr::str_detect(variable_names, "^SMOKER?E?(\\d|10)$")]
#'   if (length(smoking_variables) > 0) {
#'     visit_specific_vars[["smoking_status"]] <- smoking_variables
#'   }
#'
#'   # Menopausal status variables - match only single digit visit numbers
#'   menopause_variables <- variable_names[stringr::str_detect(variable_names, "^STATUS(\\d|10)$")]
#'   if (length(menopause_variables) > 0) {
#'     visit_specific_vars[["menopausal_stage"]] <- menopause_variables
#'   }
#'
#'   # Marital status variables - match only single digit visit numbers
#'   marital_variables <- variable_names[stringr::str_detect(variable_names, "^MARITALGP(\\d|10)$")]
#'   if (length(marital_variables) > 0) {
#'     visit_specific_vars[["marital_status"]] <- marital_variables
#'   }
#'
#'   # Parity variables - match only single digit visit numbers
#'   parity_variables <- variable_names[stringr::str_detect(variable_names, "^NUMCHILD(\\d|10)$")]
#'   if (length(parity_variables) > 0) {
#'     visit_specific_vars[["parity_count"]] <- parity_variables
#'   }
#'
#'   return(visit_specific_vars)
#' }
#'
#' #' Extract visit number from variable name (e.g., INVOLEA9 -> 9, AGE1 -> 1)
#' #' @noRd
#' extract_visit_number_from_variable <- function(variable_names) {
#'
#'   # For SWAN variables, extract the visit number more carefully
#'   visit_numbers <- character(length(variable_names))
#'
#'   for (i in seq_along(variable_names)) {
#'     var_name <- variable_names[i]
#'
#'     # For incontinence variables (INVOLEA0, LEKINVO7, etc.)
#'     if (stringr::str_detect(var_name, "^(INVOLEA|LEKINVO)")) {
#'       visit_num <- stringr::str_extract(var_name, "\\d+$")
#'       visit_numbers[i] <- visit_num
#'     }
#'     # For demographic variables (AGE1, BMI1, STATUS1, etc.)
#'     else if (stringr::str_detect(var_name, "^(AGE|BMI|WEIGHT|HEIGHT|STATUS|SMOKER|MARITALGP|NUMCHILD)")) {
#'       # Extract only the last number, and only if it's 0-10 (valid visit numbers)
#'       potential_visit <- stringr::str_extract(var_name, "(\\d|10)$")
#'       if (!is.na(potential_visit) && as.numeric(potential_visit) <= 10) {
#'         visit_numbers[i] <- potential_visit
#'       } else {
#'         visit_numbers[i] <- NA_character_
#'       }
#'     }
#'     else {
#'       # For other variables, extract trailing digits
#'       visit_num <- stringr::str_extract(var_name, "\\d+$")
#'       visit_numbers[i] <- visit_num
#'     }
#'   }
#'
#'   # Convert to numeric
#'   visit_numbers_numeric <- as.numeric(visit_numbers)
#'
#'   # Replace any NAs with 0 (baseline)
#'   visit_numbers_numeric[is.na(visit_numbers_numeric)] <- 0
#'
#'   return(visit_numbers_numeric)
#' }
#'
#' #' Standardize incontinence response values
#' #' @noRd
#' standardize_incontinence_response <- function(response_values) {
#'
#'   dplyr::case_when(
#'     # Standard Yes responses
#'     response_values %in% c("(2) Yes", "2", 2, "Yes", "YES", "yes") ~ 1,
#'     # Standard No responses
#'     response_values %in% c("(1) No", "1", 1, "No", "NO", "no") ~ 0,
#'     # Extended formats from later visits
#'     response_values %in% c("(2) 2: Yes", "(2)2: Yes", "2: Yes") ~ 1,
#'     response_values %in% c("(1) 1: No", "(1)1: No", "1: No") ~ 0,
#'     TRUE ~ NA_real_
#'   )
#' }
#'
#' #' Helper function to identify participant ID column
#' #' @noRd
#' identify_participant_id_variable <- function(dataset) {
#'   participant_id_patterns <- c("^SWANID$", "^ARCHID$", "^swanid$", "^archid$")
#'   for (pattern in participant_id_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' Helper function to identify visit number column
#' #' @noRd
#' identify_visit_number_variable <- function(dataset) {
#'   visit_patterns <- c("^VISIT$", "^visit$", "^Visit$", "^VISITNO$")
#'   for (pattern in visit_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' Safe numeric conversion
#' #' @noRd
#' convert_to_numeric_safely <- function(variable_values) {
#'   suppressWarnings(as.numeric(as.character(variable_values)))
#' }
#'
#' #' Standardize race/ethnicity variable
#' #' @noRd
#' standardize_race_ethnicity_variable <- function(race_values) {
#'   dplyr::case_when(
#'     stringr::str_detect(as.character(race_values), "White|Caucasian|4") ~ "White",
#'     stringr::str_detect(as.character(race_values), "Black|African|1") ~ "Black",
#'     stringr::str_detect(as.character(race_values), "Hispanic|5") ~ "Hispanic",
#'     stringr::str_detect(as.character(race_values), "Chinese|2") ~ "Chinese",
#'     stringr::str_detect(as.character(race_values), "Japanese|3") ~ "Japanese",
#'     TRUE ~ "Other"
#'   )
#' }
#'
#' #' Standardize smoking status variable
#' #' @noRd
#' standardize_smoking_status_variable <- function(smoking_values) {
#'   dplyr::case_when(
#'     smoking_values %in% c(1, "(1) Never smoked", "Never") ~ "Never",
#'     smoking_values %in% c(2, "(2) Past smoker", "Past") ~ "Past",
#'     smoking_values %in% c(3, "(3) Current smoker", "Current") ~ "Current",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Standardize menopausal status variable
#' #' @noRd
#' standardize_menopausal_status_variable <- function(status_values) {
#'   dplyr::case_when(
#'     status_values %in% c(1, "(1) Post by BSO") ~ "Post BSO",
#'     status_values %in% c(2, "(2) Natural Post", "(2) Postmenopausal") ~ "Natural Post",
#'     status_values %in% c(3, "(3) Late Peri", "(3) Late perimenopausal") ~ "Late Peri",
#'     status_values %in% c(4, "(4) Early Peri", "(4) Early perimenopausal") ~ "Early Peri",
#'     status_values %in% c(5, "(5) Pre", "(5) Premenopausal") ~ "Pre",
#'     status_values %in% c(6, "(6) Pregnant/breastfeeding") ~ "Pregnant/Breastfeeding",
#'     status_values %in% c(7, "(7) Unknown due to HT use") ~ "Unknown HT",
#'     status_values %in% c(8, "(8) Unknown due to hysterectomy") ~ "Unknown Hysterectomy",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Standardize education variable
#' #' @noRd
#' standardize_education_variable <- function(education_values) {
#'   dplyr::case_when(
#'     education_values %in% c(1, "(1) Less than high school") ~ "Less than HS",
#'     education_values %in% c(2, "(2) High school") ~ "High School",
#'     education_values %in% c(3, "(3) Some college") ~ "Some College",
#'     education_values %in% c(4, "(4) College graduate") ~ "College Graduate",
#'     education_values %in% c(5, "(5) Graduate school") ~ "Graduate School",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Standardize marital status variable
#' #' @noRd
#' standardize_marital_status_variable <- function(marital_values) {
#'   dplyr::case_when(
#'     marital_values %in% c(1, "(1) Married", "Married") ~ "Married",
#'     marital_values %in% c(2, "(2) Single", "Single") ~ "Single",
#'     marital_values %in% c(3, "(3) Divorced/Separated", "Divorced") ~ "Divorced/Separated",
#'     marital_values %in% c(4, "(4) Widowed", "Widowed") ~ "Widowed",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Combine multiple baseline sources with type-safe merging
#' #' @noRd
#' combine_multiple_baseline_sources <- function(baseline_dataset_list, verbose) {
#'
#'   if (length(baseline_dataset_list) == 1) {
#'     return(baseline_dataset_list[[1]])
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Combining {length(baseline_dataset_list)} baseline sources")
#'   }
#'
#'   # Find the most complete dataset (most non-missing participant IDs)
#'   dataset_completeness_scores <- purrr::map_dbl(baseline_dataset_list, function(baseline_dataset) {
#'     sum(!is.na(baseline_dataset$swan_participant_id))
#'   })
#'
#'   primary_baseline_dataset <- baseline_dataset_list[[which.max(dataset_completeness_scores)]]
#'
#'   if (verbose) {
#'     logger::log_info("Primary baseline dataset: {primary_baseline_dataset$source_file[1]} with {max(dataset_completeness_scores)} participants")
#'   }
#'
#'   # Merge additional datasets
#'   for (dataset_index in seq_along(baseline_dataset_list)) {
#'     if (dataset_index == which.max(dataset_completeness_scores)) next
#'
#'     secondary_baseline_dataset <- baseline_dataset_list[[dataset_index]]
#'
#'     if (verbose) {
#'       logger::log_info("Merging secondary baseline: {secondary_baseline_dataset$source_file[1]}")
#'     }
#'
#'     # Harmonize data types before merging
#'     secondary_baseline_dataset <- harmonize_baseline_data_types(
#'       secondary_baseline_dataset,
#'       primary_baseline_dataset,
#'       verbose
#'     )
#'
#'     primary_baseline_dataset <- primary_baseline_dataset %>%
#'       dplyr::full_join(
#'         secondary_baseline_dataset,
#'         by = "swan_participant_id",
#'         suffix = c("", "_secondary")
#'       )
#'
#'     # Smart coalescing - only coalesce variables that exist in both datasets
#'     baseline_variables_to_merge <- c(
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_parity_count",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_menopausal_stage",
#'       "baseline_education_level",
#'       "baseline_marital_status"
#'     )
#'
#'     # Check which variables actually exist with _secondary suffix
#'     available_columns <- names(primary_baseline_dataset)
#'
#'     for (baseline_variable in baseline_variables_to_merge) {
#'       secondary_variable_name <- paste0(baseline_variable, "_secondary")
#'
#'       if (baseline_variable %in% available_columns && secondary_variable_name %in% available_columns) {
#'         if (verbose) {
#'           logger::log_info("  Coalescing {baseline_variable}")
#'         }
#'
#'         # Apply appropriate coalescing based on data type
#'         if (stringr::str_detect(baseline_variable, "age|bmi|parity|count")) {
#'           # Numeric variables
#'           primary_baseline_dataset[[baseline_variable]] <- apply_safe_numeric_coalesce(
#'             primary_baseline_dataset[[baseline_variable]],
#'             primary_baseline_dataset[[secondary_variable_name]]
#'           )
#'         } else {
#'           # Character variables
#'           primary_baseline_dataset[[baseline_variable]] <- apply_safe_character_coalesce(
#'             primary_baseline_dataset[[baseline_variable]],
#'             primary_baseline_dataset[[secondary_variable_name]]
#'           )
#'         }
#'       } else if (verbose) {
#'         logger::log_info("  Skipping {baseline_variable} - not available in secondary dataset")
#'       }
#'     }
#'
#'     # Remove all secondary columns
#'     primary_baseline_dataset <- primary_baseline_dataset %>%
#'       dplyr::select(-dplyr::ends_with("_secondary"))
#'   }
#'
#'   if (verbose) {
#'     final_participant_count <- sum(!is.na(primary_baseline_dataset$swan_participant_id))
#'     logger::log_info("Combined baseline dataset: {final_participant_count} total participants")
#'   }
#'
#'   return(primary_baseline_dataset)
#' }
#'
#' #' Harmonize data types between baseline datasets
#' #' @noRd
#' harmonize_baseline_data_types <- function(secondary_dataset, primary_dataset, verbose) {
#'
#'   baseline_column_names <- names(secondary_dataset)[stringr::str_starts(names(secondary_dataset), "baseline_")]
#'
#'   for (column_name in baseline_column_names) {
#'     if (column_name %in% names(primary_dataset)) {
#'
#'       primary_data_type <- class(primary_dataset[[column_name]])[1]
#'       secondary_data_type <- class(secondary_dataset[[column_name]])[1]
#'
#'       if (primary_data_type != secondary_data_type) {
#'         if (verbose) {
#'           logger::log_info("Harmonizing {column_name}: {secondary_data_type} -> {primary_data_type}")
#'         }
#'
#'         # Convert secondary to match primary type
#'         if (primary_data_type %in% c("numeric", "double")) {
#'           secondary_dataset[[column_name]] <- convert_to_numeric_safely(secondary_dataset[[column_name]])
#'         } else if (primary_data_type == "character") {
#'           secondary_dataset[[column_name]] <- as.character(secondary_dataset[[column_name]])
#'         } else if (primary_data_type == "logical") {
#'           secondary_dataset[[column_name]] <- as.logical(secondary_dataset[[column_name]])
#'         }
#'       }
#'     }
#'   }
#'
#'   return(secondary_dataset)
#' }
#'
#' #' Type-safe numeric coalesce
#' #' @noRd
#' apply_safe_numeric_coalesce <- function(primary_values, secondary_values) {
#'   # Convert both to numeric if needed
#'   primary_numeric <- if (is.numeric(primary_values)) primary_values else convert_to_numeric_safely(primary_values)
#'   secondary_numeric <- if (is.numeric(secondary_values)) secondary_values else convert_to_numeric_safely(secondary_values)
#'   dplyr::coalesce(primary_numeric, secondary_numeric)
#' }
#'
#' #' Type-safe character coalesce
#' #' @noRd
#' apply_safe_character_coalesce <- function(primary_values, secondary_values) {
#'   # Convert both to character if needed
#'   primary_character <- if (is.character(primary_values)) primary_values else as.character(primary_values)
#'   secondary_character <- if (is.character(secondary_values)) secondary_values else as.character(secondary_values)
#'   dplyr::coalesce(primary_character, secondary_character)
#' }
#'
#' #' Merge baseline demographics with longitudinal data
#' #' @noRd
#' merge_baseline_with_longitudinal_data <- function(baseline_data, longitudinal_data, verbose) {
#'
#'   if (verbose) {
#'     baseline_participant_count <- length(unique(baseline_data$swan_participant_id))
#'     longitudinal_observation_count <- nrow(longitudinal_data)
#'     logger::log_info("Merging baseline demographics with longitudinal measurements")
#'     logger::log_info("Baseline participants: {baseline_participant_count}")
#'     logger::log_info("Longitudinal observations: {longitudinal_observation_count}")
#'   }
#'
#'   # Merge datasets
#'   merged_comprehensive_data <- longitudinal_data %>%
#'     dplyr::left_join(
#'       baseline_data %>% dplyr::select(-source_file),
#'       by = "swan_participant_id"
#'     )
#'
#'   # Add derived time variables
#'   merged_comprehensive_data <- merged_comprehensive_data %>%
#'     dplyr::mutate(
#'       # Calculate current age at each visit
#'       current_age_years = baseline_age_years + years_since_baseline,
#'       # Add visit timing categories
#'       visit_timing_category = dplyr::case_when(
#'         visit_number == 0 ~ "Baseline",
#'         visit_number %in% 1:3 ~ "Early Follow-up",
#'         visit_number %in% 4:7 ~ "Mid Follow-up",
#'         visit_number >= 8 ~ "Late Follow-up",
#'         TRUE ~ "Unknown"
#'       )
#'     ) %>%
#'     dplyr::arrange(swan_participant_id, visit_number)
#'
#'   if (verbose) {
#'     merged_observation_count <- nrow(merged_comprehensive_data)
#'     participants_with_both_data <- length(unique(merged_comprehensive_data$swan_participant_id[!is.na(merged_comprehensive_data$baseline_age_years)]))
#'     logger::log_info("Merged comprehensive dataset: {merged_observation_count} observations")
#'     logger::log_info("Participants with both baseline + longitudinal data: {participants_with_both_data}")
#'   }
#'
#'   return(merged_comprehensive_data)
#' }
#'
#' #' Apply filtering for valid responses
#' #' @noRd
#' apply_response_filtering <- function(merged_data, verbose) {
#'
#'   if (verbose) {
#'     original_observation_count <- nrow(merged_data)
#'     logger::log_info("Applying response filtering to remove invalid/missing incontinence responses")
#'   }
#'
#'   filtered_data <- merged_data %>%
#'     dplyr::filter(
#'       !is.na(incontinence_status),
#'       !is.na(swan_participant_id),
#'       !is.na(visit_number)
#'     )
#'
#'   if (verbose) {
#'     filtered_observation_count <- nrow(filtered_data)
#'     observations_removed <- original_observation_count - filtered_observation_count
#'     logger::log_info("Filtering completed: {filtered_observation_count} observations retained")
#'     logger::log_info("Observations removed: {observations_removed}")
#'   }
#'
#'   return(filtered_data)
#' }
#'
#' #' Format comprehensive output dataset
#' #' @noRd
#' format_comprehensive_output <- function(merged_data, format, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Formatting comprehensive output as: {format}")
#'     all_column_names <- paste(names(merged_data), collapse = ", ")
#'     logger::log_info("Available columns: {all_column_names}")
#'   }
#'
#'   if (format == "long") {
#'     # Identify visit-specific columns
#'     visit_specific_column_names <- names(merged_data)[stringr::str_starts(names(merged_data), "visit_")]
#'
#'     if (verbose && length(visit_specific_column_names) > 0) {
#'       visit_vars_string <- paste(visit_specific_column_names, collapse = ", ")
#'       logger::log_info("Visit-specific columns preserved: {visit_vars_string}")
#'     } else if (verbose) {
#'       logger::log_warn("No visit-specific columns found in final dataset")
#'     }
#'
#'     # Build comprehensive column selection
#'     baseline_demographic_columns <- c(
#'       "swan_participant_id",
#'       "visit_number",
#'       "years_since_baseline",
#'       "current_age_years",
#'       "visit_timing_category",
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_parity_count",
#'       "baseline_menopausal_stage",
#'       "baseline_education_level",
#'       "baseline_marital_status"
#'     )
#'
#'     incontinence_measurement_columns <- c(
#'       "incontinence_status",
#'       "incontinence_raw_response",
#'       "incontinence_source_variable"
#'     )
#'
#'     # Combine all columns, keeping only those that exist
#'     all_desired_columns <- c(
#'       baseline_demographic_columns,
#'       incontinence_measurement_columns,
#'       visit_specific_column_names
#'     )
#'
#'     columns_that_exist_in_data <- intersect(all_desired_columns, names(merged_data))
#'
#'     if (verbose) {
#'       logger::log_info("Selecting {length(columns_that_exist_in_data)} columns for long format output")
#'     }
#'
#'     # Create final long format dataset
#'     final_comprehensive_dataset <- merged_data %>%
#'       dplyr::select(dplyr::all_of(columns_that_exist_in_data)) %>%
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         incontinence_source_variable,
#'         .keep_all = TRUE
#'       ) %>%
#'       dplyr::arrange(swan_participant_id, visit_number)
#'
#'   } else {
#'     # Wide format conversion
#'     visit_specific_column_names <- names(merged_data)[stringr::str_starts(names(merged_data), "visit_")]
#'
#'     baseline_columns_for_wide <- c(
#'       "swan_participant_id",
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_parity_count",
#'       "baseline_menopausal_stage",
#'       "baseline_education_level",
#'       "baseline_marital_status"
#'     )
#'
#'     measurement_columns_for_wide <- c(
#'       "visit_number",
#'       "incontinence_status",
#'       "current_age_years"
#'     )
#'
#'     all_columns_for_wide <- c(
#'       baseline_columns_for_wide,
#'       measurement_columns_for_wide,
#'       visit_specific_column_names
#'     )
#'
#'     columns_that_exist_for_wide <- intersect(all_columns_for_wide, names(merged_data))
#'
#'     if (verbose) {
#'       logger::log_info("Converting to wide format with {length(columns_that_exist_for_wide)} base columns")
#'     }
#'
#'     final_comprehensive_dataset <- merged_data %>%
#'       dplyr::select(dplyr::all_of(columns_that_exist_for_wide)) %>%
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         .keep_all = TRUE
#'       ) %>%
#'       tidyr::pivot_wider(
#'         names_from = visit_number,
#'         values_from = c(
#'           dplyr::any_of(visit_specific_column_names),
#'           incontinence_status,
#'           current_age_years
#'         ),
#'         names_sep = "_visit_"
#'       )
#'   }
#'
#'   if (verbose) {
#'     final_row_count <- nrow(final_comprehensive_dataset)
#'     final_column_count <- ncol(final_comprehensive_dataset)
#'     unique_participant_count <- length(unique(final_comprehensive_dataset$swan_participant_id))
#'
#'     logger::log_info("Final comprehensive dataset formatting completed")
#'     logger::log_info("Dimensions: {final_row_count} rows × {final_column_count} columns")
#'     logger::log_info("Unique participants: {unique_participant_count}")
#'
#'     if (format == "long") {
#'       final_visit_specific_count <- sum(stringr::str_starts(names(final_comprehensive_dataset), "visit_"))
#'       visit_range_min <- min(final_comprehensive_dataset$visit_number, na.rm = TRUE)
#'       visit_range_max <- max(final_comprehensive_dataset$visit_number, na.rm = TRUE)
#'       logger::log_info("Visit-specific variables in final dataset: {final_visit_specific_count}")
#'       logger::log_info("Visit range: {visit_range_min} to {visit_range_max}")
#'     }
#'   }
#'
#'   return(final_comprehensive_dataset)
#' }
#'
#' # Run -----
#' comprehensive_swan_dataset_corrected <- create_comprehensive_swan_dataset(
#'   swan_data_directory = swan_data_directory,
#'   output_format = "long",
#'   verbose = TRUE,
#'   include_visit_demographics = TRUE,
#'   filter_valid_responses = TRUE
#' )
#'
#'
#' write_rds(comprehensive_swan_dataset_corrected, "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/comprehensive_swan_dataset_corrected.rds")
#'
#' # V9 Create Comprehensive SWAN Dataset with Visit-Specific Variables ----
#' #' Create Comprehensive SWAN Dataset with Visit-Specific Variables
#' #'
#' #' This function creates a comprehensive dataset from SWAN study files, extracting
#' #' baseline demographics and visit-specific variables including urinary incontinence
#' #' measures (INVOLEA/LEKINVO) and other longitudinal health indicators.
#' #'
#' #' @param swan_data_directory Character. Path to directory containing SWAN .rda files
#' #' @param output_format Character. Either "long" or "wide" format for final dataset
#' #' @param verbose Logical. Enable detailed logging output to console
#' #' @param include_visit_demographics Logical. Include visit-specific demographic
#' #'   variables (AGE, BMI, STATUS, etc.) in addition to baseline
#' #' @param filter_valid_responses Logical. Remove rows with missing INVOLEA responses
#' #'
#' #' @return tibble. Comprehensive SWAN dataset with baseline and visit-specific variables
#' #'
#' #' @examples
#' #' # Example 1: Basic long format with visit demographics
#' #' swan_long_comprehensive <- create_comprehensive_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "long",
#' #'   verbose = TRUE,
#' #'   include_visit_demographics = TRUE,
#' #'   filter_valid_responses = TRUE
#' #' )
#' #'
#' #' # Example 2: Wide format for analysis, no filtering
#' #' swan_wide_complete <- create_comprehensive_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "wide",
#' #'   verbose = FALSE,
#' #'   include_visit_demographics = TRUE,
#' #'   filter_valid_responses = FALSE
#' #' )
#' #'
#' #' # Example 3: Minimal dataset with only baseline + INVOLEA
#' #' swan_minimal <- create_comprehensive_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "long",
#' #'   verbose = TRUE,
#' #'   include_visit_demographics = FALSE,
#' #'   filter_valid_responses = TRUE
#' #' )
#' #'
#' #' @importFrom dplyr filter select mutate arrange group_by summarise
#' #'   left_join full_join distinct coalesce case_when all_of any_of ends_with
#' #' @importFrom tidyr pivot_longer pivot_wider
#' #' @importFrom stringr str_detect str_extract str_replace_all str_starts
#' #'   str_remove str_match
#' #' @importFrom purrr map_dfr map_chr map_lgl keep walk compact map
#' #' @importFrom assertthat assert_that
#' #' @importFrom logger log_info log_warn log_error
#' #' @importFrom tibble tibble as_tibble
#' #' @importFrom readr parse_number
#' #' @importFrom glue glue
#' #'
#' #' @export
#' create_comprehensive_swan_dataset <- function(swan_data_directory,
#'                                               output_format = "long",
#'                                               verbose = TRUE,
#'                                               include_visit_demographics = TRUE,
#'                                               filter_valid_responses = TRUE) {
#'
#'   # Input validation with assertthat
#'   if (verbose) {
#'     logger::log_info("Creating comprehensive SWAN dataset with visit-specific variables")
#'     logger::log_info("Function inputs - directory: {swan_data_directory}")
#'     logger::log_info("Function inputs - format: {output_format}, verbose: {verbose}")
#'     logger::log_info("Function inputs - include_visit_demographics: {include_visit_demographics}")
#'     logger::log_info("Function inputs - filter_valid_responses: {filter_valid_responses}")
#'   }
#'
#'   assertthat::assert_that(
#'     is.character(swan_data_directory),
#'     msg = "swan_data_directory must be a character string path"
#'   )
#'   assertthat::assert_that(
#'     dir.exists(swan_data_directory),
#'     msg = glue::glue("Directory does not exist: {swan_data_directory}")
#'   )
#'   assertthat::assert_that(
#'     output_format %in% c("long", "wide"),
#'     msg = "output_format must be 'long' or 'wide'"
#'   )
#'   assertthat::assert_that(
#'     is.logical(verbose),
#'     msg = "verbose must be TRUE or FALSE"
#'   )
#'   assertthat::assert_that(
#'     is.logical(include_visit_demographics),
#'     msg = "include_visit_demographics must be TRUE or FALSE"
#'   )
#'   assertthat::assert_that(
#'     is.logical(filter_valid_responses),
#'     msg = "filter_valid_responses must be TRUE or FALSE"
#'   )
#'
#'   # Step 1: Extract baseline demographics
#'   baseline_participant_demographics <- extract_baseline_participant_demographics(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Baseline demographics extracted: {nrow(baseline_participant_demographics)} participants")
#'   }
#'
#'   # Step 2: Extract comprehensive longitudinal data
#'   longitudinal_health_measurements <- extract_comprehensive_longitudinal_data(
#'     directory_path = swan_data_directory,
#'     include_visit_demographics = include_visit_demographics,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Longitudinal data extracted: {nrow(longitudinal_health_measurements)} observations")
#'   }
#'
#'   # Step 3: Merge datasets
#'   merged_comprehensive_dataset <- merge_baseline_with_longitudinal_data(
#'     baseline_data = baseline_participant_demographics,
#'     longitudinal_data = longitudinal_health_measurements,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Data merged: {nrow(merged_comprehensive_dataset)} total observations")
#'   }
#'
#'   # Step 4: Apply filtering if requested
#'   if (filter_valid_responses) {
#'     filtered_comprehensive_dataset <- apply_response_filtering(
#'       merged_data = merged_comprehensive_dataset,
#'       verbose = verbose
#'     )
#'   } else {
#'     filtered_comprehensive_dataset <- merged_comprehensive_dataset
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("After filtering: {nrow(filtered_comprehensive_dataset)} observations")
#'   }
#'
#'   # Step 5: Format final output
#'   final_comprehensive_dataset <- format_comprehensive_output(
#'     merged_data = filtered_comprehensive_dataset,
#'     format = output_format,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Comprehensive SWAN dataset creation completed")
#'     logger::log_info("Final dataset dimensions: {nrow(final_comprehensive_dataset)} rows × {ncol(final_comprehensive_dataset)} columns")
#'     logger::log_info("Output file path: Not saved to file (returned as object)")
#'   }
#'
#'   return(final_comprehensive_dataset)
#' }
#'
#' #' Extract baseline demographics from SWAN screener/baseline files
#' #' @noRd
#' extract_baseline_participant_demographics <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting baseline participant demographics from SWAN files")
#'   }
#'
#'   # Find baseline/screener files using known ICPSR numbers
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   # Known baseline file patterns from SWAN documentation
#'   baseline_file_identifiers <- c("28762", "04368", "baseline", "screener")
#'
#'   baseline_candidate_files <- purrr::keep(all_rda_files, function(file_path) {
#'     file_name <- tolower(basename(file_path))
#'     any(stringr::str_detect(file_name, baseline_file_identifiers))
#'   })
#'
#'   if (verbose) {
#'     logger::log_info("Found {length(baseline_candidate_files)} potential baseline files")
#'     purrr::walk(baseline_candidate_files, ~logger::log_info("  Candidate file: {basename(.x)}"))
#'   }
#'
#'   # Process each baseline file
#'   baseline_dataset_list <- purrr::map(baseline_candidate_files, function(file_path) {
#'     process_single_baseline_file(file_path, verbose)
#'   })
#'
#'   # Remove NULL results and combine
#'   baseline_dataset_list <- purrr::compact(baseline_dataset_list)
#'
#'   if (length(baseline_dataset_list) == 0) {
#'     logger::log_error("No valid baseline demographic data found in any files")
#'     stop("No baseline demographic data could be extracted")
#'   }
#'
#'   # Combine multiple baseline sources
#'   combined_baseline_demographics <- combine_multiple_baseline_sources(
#'     baseline_dataset_list,
#'     verbose
#'   )
#'
#'   return(combined_baseline_demographics)
#' }
#'
#' #' Process a single baseline file to extract demographics
#' #' @noRd
#' process_single_baseline_file <- function(file_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Processing baseline file: {basename(file_path)}")
#'   }
#'
#'   # Load the file safely
#'   file_environment <- new.env()
#'
#'   tryCatch({
#'     load(file_path, envir = file_environment)
#'   }, error = function(e) {
#'     if (verbose) {
#'       logger::log_error("Failed to load {basename(file_path)}: {e$message}")
#'     }
#'     return(NULL)
#'   })
#'
#'   if (length(ls(file_environment)) == 0) {
#'     if (verbose) {
#'       logger::log_warn("No datasets found in {basename(file_path)}")
#'     }
#'     return(NULL)
#'   }
#'
#'   raw_baseline_dataset <- file_environment[[ls(file_environment)[1]]]
#'
#'   # Identify demographic variables
#'   demographic_variable_mapping <- identify_baseline_demographic_variables(
#'     raw_baseline_dataset
#'   )
#'
#'   if (is.null(demographic_variable_mapping$participant_id)) {
#'     if (verbose) {
#'       logger::log_warn("No participant ID found in {basename(file_path)}")
#'     }
#'     return(NULL)
#'   }
#'
#'   # Extract and standardize demographics
#'   standardized_baseline_demographics <- extract_standardized_demographics(
#'     dataset = raw_baseline_dataset,
#'     variable_mapping = demographic_variable_mapping,
#'     source_file = basename(file_path),
#'     verbose = verbose
#'   )
#'
#'   return(standardized_baseline_demographics)
#' }
#'
#' #' Identify demographic variables in baseline dataset
#' #' @noRd
#' identify_baseline_demographic_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Participant ID patterns
#'   participant_id_patterns <- c("^SWANID$", "^ARCHID$", "^swanid$", "^archid$")
#'   participant_id_variable <- find_matching_variable(variable_names, participant_id_patterns)
#'
#'   # Demographic variable patterns - updated to match actual SWAN variable names
#'   age_patterns <- c("^AGE0?$", "^age0?$", "AGE.*0$")
#'   bmi_patterns <- c("^BMI0?$", "^bmi0?$", "BMI.*0$")
#'   race_patterns <- c("^RACE$", "^race$", "^ETHNIC")
#'
#'   # Updated smoking patterns to match SWAN codebook
#'   smoking_patterns <- c("^SMOKER0?$", "^SMOKING0?$", "^SMOKE_R$", "^SMOKED$")
#'
#'   # Updated parity patterns to match SWAN codebook
#'   parity_patterns <- c("^NUMCHILD$", "^CHILDREN$", "^PARITY", "^NUM_CHILD$")
#'
#'   # Updated menopausal patterns to match SWAN codebook
#'   menopausal_patterns <- c("^STATUS0?$", "^MENO", "^MEN_FLAG$")
#'
#'   education_patterns <- c("^DEGREE$", "^EDUCATION", "^EDUC")
#'   marital_patterns <- c("^MARITAL", "^MARITALGP")
#'
#'   return(list(
#'     participant_id = participant_id_variable,
#'     age = find_matching_variable(variable_names, age_patterns),
#'     bmi = find_matching_variable(variable_names, bmi_patterns),
#'     race = find_matching_variable(variable_names, race_patterns),
#'     smoking = find_matching_variable(variable_names, smoking_patterns),
#'     parity = find_matching_variable(variable_names, parity_patterns),
#'     menopausal = find_matching_variable(variable_names, menopausal_patterns),
#'     education = find_matching_variable(variable_names, education_patterns),
#'     marital = find_matching_variable(variable_names, marital_patterns)
#'   ))
#' }
#'
#' #' Find first matching variable from patterns
#' #' @noRd
#' find_matching_variable <- function(variable_names, patterns) {
#'   for (pattern in patterns) {
#'     matches <- variable_names[stringr::str_detect(variable_names, pattern)]
#'     if (length(matches) > 0) {
#'       return(matches[1])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' Extract and standardize demographic variables
#' #' @noRd
#' extract_standardized_demographics <- function(dataset, variable_mapping,
#'                                               source_file, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("  Variable mapping for {source_file}:")
#'     for (var_type in names(variable_mapping)) {
#'       var_name <- variable_mapping[[var_type]]
#'       if (!is.null(var_name)) {
#'         logger::log_info("    {var_type}: {var_name}")
#'       } else {
#'         logger::log_info("    {var_type}: NOT FOUND")
#'       }
#'     }
#'   }
#'
#'   # Start with participant ID
#'   standardized_demographics <- tibble::tibble(
#'     swan_participant_id = as.character(dataset[[variable_mapping$participant_id]])
#'   )
#'
#'   # Add demographic variables with type-safe conversion
#'   if (!is.null(variable_mapping$age)) {
#'     standardized_demographics$baseline_age_years <-
#'       convert_to_numeric_safely(dataset[[variable_mapping$age]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_age_years))
#'       logger::log_info("    Age variable {variable_mapping$age}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   if (!is.null(variable_mapping$bmi)) {
#'     standardized_demographics$baseline_bmi_kg_m2 <-
#'       convert_to_numeric_safely(dataset[[variable_mapping$bmi]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_bmi_kg_m2))
#'       logger::log_info("    BMI variable {variable_mapping$bmi}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   if (!is.null(variable_mapping$race)) {
#'     standardized_demographics$baseline_race_ethnicity <-
#'       standardize_race_ethnicity_variable(dataset[[variable_mapping$race]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_race_ethnicity))
#'       logger::log_info("    Race variable {variable_mapping$race}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   if (!is.null(variable_mapping$smoking)) {
#'     standardized_demographics$baseline_smoking_status <-
#'       standardize_smoking_status_variable(dataset[[variable_mapping$smoking]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_smoking_status))
#'       logger::log_info("    Smoking variable {variable_mapping$smoking}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   if (!is.null(variable_mapping$parity)) {
#'     standardized_demographics$baseline_parity_count <-
#'       convert_to_numeric_safely(dataset[[variable_mapping$parity]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_parity_count))
#'       logger::log_info("    Parity variable {variable_mapping$parity}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   if (!is.null(variable_mapping$menopausal)) {
#'     standardized_demographics$baseline_menopausal_stage <-
#'       standardize_menopausal_status_variable(dataset[[variable_mapping$menopausal]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_menopausal_stage))
#'       logger::log_info("    Menopausal variable {variable_mapping$menopausal}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   if (!is.null(variable_mapping$education)) {
#'     standardized_demographics$baseline_education_level <-
#'       standardize_education_variable(dataset[[variable_mapping$education]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_education_level))
#'       logger::log_info("    Education variable {variable_mapping$education}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   if (!is.null(variable_mapping$marital)) {
#'     standardized_demographics$baseline_marital_status <-
#'       standardize_marital_status_variable(dataset[[variable_mapping$marital]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_marital_status))
#'       logger::log_info("    Marital variable {variable_mapping$marital}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   standardized_demographics$source_file <- source_file
#'
#'   if (verbose) {
#'     valid_participant_count <- sum(!is.na(standardized_demographics$swan_participant_id))
#'     demographic_variable_count <- ncol(standardized_demographics) - 2  # exclude ID and source
#'     logger::log_info("  Extracted {valid_participant_count} participants with {demographic_variable_count} demographic variables")
#'   }
#'
#'   return(standardized_demographics)
#' }
#'
#' #' Extract comprehensive longitudinal data including visit-specific variables
#' #' @noRd
#' extract_comprehensive_longitudinal_data <- function(directory_path,
#'                                                     include_visit_demographics,
#'                                                     verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting comprehensive longitudinal data from all SWAN files")
#'   }
#'
#'   # Find all RDA files
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   # Process each file for longitudinal variables
#'   longitudinal_dataset_list <- purrr::map(all_rda_files, function(file_path) {
#'     process_single_longitudinal_file(
#'       file_path = file_path,
#'       include_visit_demographics = include_visit_demographics,
#'       verbose = verbose
#'     )
#'   })
#'
#'   # Remove NULL results
#'   longitudinal_dataset_list <- purrr::compact(longitudinal_dataset_list)
#'
#'   if (length(longitudinal_dataset_list) == 0) {
#'     logger::log_error("No longitudinal data found in any files")
#'     stop("No longitudinal data could be extracted")
#'   }
#'
#'   # Combine all longitudinal datasets
#'   combined_longitudinal_data <- purrr::map_dfr(longitudinal_dataset_list, ~.x)
#'
#'   # Remove duplicate observations (same participant, visit, variable)
#'   deduped_longitudinal_data <- combined_longitudinal_data %>%
#'     dplyr::distinct(
#'       swan_participant_id,
#'       visit_number,
#'       incontinence_source_variable,
#'       .keep_all = TRUE
#'     )
#'
#'   if (verbose) {
#'     unique_participant_count <- length(unique(deduped_longitudinal_data$swan_participant_id))
#'     visit_range <- paste(
#'       min(deduped_longitudinal_data$visit_number, na.rm = TRUE), "to",
#'       max(deduped_longitudinal_data$visit_number, na.rm = TRUE)
#'     )
#'     logger::log_info("Comprehensive longitudinal data extracted: {nrow(deduped_longitudinal_data)} observations")
#'     logger::log_info("Visit range: {visit_range}")
#'     logger::log_info("Unique participants with longitudinal data: {unique_participant_count}")
#'   }
#'
#'   return(deduped_longitudinal_data)
#' }
#'
#' #' Process single file for longitudinal variables
#' #' @noRd
#' process_single_longitudinal_file <- function(file_path, include_visit_demographics, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Scanning {basename(file_path)} for longitudinal variables")
#'   }
#'
#'   # Load file safely
#'   file_environment <- new.env()
#'
#'   tryCatch({
#'     load(file_path, envir = file_environment)
#'   }, error = function(e) {
#'     if (verbose) {
#'       logger::log_warn("Failed to load {basename(file_path)}: {e$message}")
#'     }
#'     return(NULL)
#'   })
#'
#'   if (length(ls(file_environment)) == 0) return(NULL)
#'
#'   raw_longitudinal_dataset <- file_environment[[ls(file_environment)[1]]]
#'
#'   # Look for incontinence variables (INVOLEA/LEKINVO)
#'   incontinence_variable_names <- identify_incontinence_variables(raw_longitudinal_dataset)
#'
#'   if (length(incontinence_variable_names) == 0) {
#'     return(NULL)  # No incontinence variables in this file
#'   }
#'
#'   if (verbose) {
#'     incontinence_vars_string <- paste(incontinence_variable_names, collapse = ", ")
#'     logger::log_info("  Found {length(incontinence_variable_names)} incontinence variables: {incontinence_vars_string}")
#'   }
#'
#'   # Extract longitudinal data
#'   extracted_longitudinal_data <- extract_longitudinal_measurements(
#'     dataset = raw_longitudinal_dataset,
#'     incontinence_variables = incontinence_variable_names,
#'     include_visit_demographics = include_visit_demographics,
#'     source_file = basename(file_path),
#'     verbose = verbose
#'   )
#'
#'   return(extracted_longitudinal_data)
#' }
#'
#' #' Identify incontinence variables (INVOLEA/LEKINVO) in dataset
#' #' @noRd
#' identify_incontinence_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Look for INVOLEA and LEKINVO patterns
#'   incontinence_patterns <- c("INVOLEA\\d*", "LEKINVO\\d*", "involea\\d*", "lekinvo\\d*")
#'
#'   incontinence_variables <- c()
#'   for (pattern in incontinence_patterns) {
#'     matching_variables <- variable_names[stringr::str_detect(variable_names, pattern)]
#'     incontinence_variables <- c(incontinence_variables, matching_variables)
#'   }
#'
#'   return(unique(incontinence_variables))
#' }
#'
#' #' Extract longitudinal measurements including visit-specific demographics
#' #' @noRd
#' extract_longitudinal_measurements <- function(dataset, incontinence_variables,
#'                                               include_visit_demographics,
#'                                               source_file, verbose) {
#'
#'   # Identify core variables
#'   participant_id_variable <- identify_participant_id_variable(dataset)
#'   visit_number_variable <- identify_visit_number_variable(dataset)
#'
#'   if (is.null(participant_id_variable)) {
#'     if (verbose) {
#'       logger::log_warn("No participant ID found in {source_file}")
#'     }
#'     return(NULL)
#'   }
#'
#'   # Start with core columns
#'   core_column_names <- c(participant_id_variable)
#'   if (!is.null(visit_number_variable)) {
#'     core_column_names <- c(core_column_names, visit_number_variable)
#'   }
#'
#'   # Identify ALL visit-specific demographic variables if requested
#'   visit_specific_variables <- list()
#'
#'   if (include_visit_demographics) {
#'     visit_specific_variables <- identify_visit_specific_demographic_variables(dataset)
#'
#'     if (verbose && length(visit_specific_variables) > 0) {
#'       total_visit_vars <- sum(lengths(visit_specific_variables))
#'       visit_vars_string <- paste(names(visit_specific_variables), collapse = ", ")
#'       logger::log_info("  Visit-specific demographics found: {visit_vars_string} (total: {total_visit_vars} variables)")
#'     }
#'   }
#'
#'   # Combine all variables to extract - flatten the visit-specific variables list
#'   all_visit_specific_vars <- unlist(visit_specific_variables)
#'   all_variables_to_extract <- c(
#'     core_column_names,
#'     all_visit_specific_vars,  # Now includes ALL visit-specific variables
#'     incontinence_variables
#'   )
#'
#'   # Extract data
#'   longitudinal_measurements <- tibble::as_tibble(dataset) %>%
#'     dplyr::select(dplyr::all_of(all_variables_to_extract))
#'
#'   # Standardize core column names
#'   names(longitudinal_measurements)[1] <- "swan_participant_id"
#'   if (!is.null(visit_number_variable)) {
#'     names(longitudinal_measurements)[2] <- "visit_number_from_file"
#'   } else {
#'     longitudinal_measurements$visit_number_from_file <- NA_real_
#'   }
#'
#'   # Convert incontinence variables to long format first
#'   incontinence_long_format <- longitudinal_measurements %>%
#'     tidyr::pivot_longer(
#'       cols = dplyr::all_of(incontinence_variables),
#'       names_to = "incontinence_source_variable",
#'       values_to = "incontinence_raw_response"
#'     ) %>%
#'     dplyr::mutate(
#'       swan_participant_id = as.character(swan_participant_id),
#'       # Extract visit number from incontinence variable name
#'       visit_number = extract_visit_number_from_variable(incontinence_source_variable),
#'       incontinence_status = standardize_incontinence_response(incontinence_raw_response),
#'       years_since_baseline = visit_number,
#'       source_file = source_file
#'     ) %>%
#'     dplyr::select(-visit_number_from_file) %>%
#'     dplyr::filter(!is.na(incontinence_raw_response))  # Remove missing responses
#'
#'   # Now add visit-specific demographic variables that match the visit number
#'   if (include_visit_demographics && length(visit_specific_variables) > 0) {
#'
#'     for (demographic_type in names(visit_specific_variables)) {
#'       variable_names_for_this_type <- visit_specific_variables[[demographic_type]]
#'
#'       # Create a mapping of visit numbers to values for this demographic type
#'       visit_demographic_mapping <- tibble::tibble()
#'
#'       for (var_name in variable_names_for_this_type) {
#'         # Extract visit number from variable name (e.g., AGE3 -> 3, BMI0 -> 0)
#'         var_visit_number <- extract_visit_number_from_variable(var_name)
#'
#'         # Get the values for this variable
#'         var_values <- dataset[[var_name]]
#'
#'         # Create temporary mapping
#'         temp_mapping <- tibble::tibble(
#'           swan_participant_id = as.character(dataset[[participant_id_variable]]),
#'           visit_number = var_visit_number,
#'           !!paste0("visit_", demographic_type) := var_values
#'         ) %>%
#'           dplyr::filter(!is.na(.data[[paste0("visit_", demographic_type)]]))
#'
#'         # Combine with existing mapping
#'         if (nrow(visit_demographic_mapping) == 0) {
#'           visit_demographic_mapping <- temp_mapping
#'         } else {
#'           visit_demographic_mapping <- dplyr::bind_rows(visit_demographic_mapping, temp_mapping)
#'         }
#'       }
#'
#'       # Merge this demographic variable with the incontinence data
#'       if (nrow(visit_demographic_mapping) > 0) {
#'         incontinence_long_format <- incontinence_long_format %>%
#'           dplyr::left_join(
#'             visit_demographic_mapping,
#'             by = c("swan_participant_id", "visit_number")
#'           )
#'       }
#'     }
#'   }
#'
#'   # Log results
#'   if (verbose && nrow(incontinence_long_format) > 0) {
#'     unique_visits <- sort(unique(incontinence_long_format$visit_number))
#'     visits_string <- paste(unique_visits, collapse = ", ")
#'     logger::log_info("  Extracted visits for {source_file}: {visits_string}")
#'     logger::log_info("  Total observations in {source_file}: {nrow(incontinence_long_format)}")
#'
#'     # Log visit-specific variables preserved
#'     visit_specific_columns <- names(incontinence_long_format)[stringr::str_starts(names(incontinence_long_format), "visit_")]
#'     if (length(visit_specific_columns) > 0) {
#'       visit_vars_preserved <- paste(stringr::str_remove(visit_specific_columns, "^visit_"), collapse = ", ")
#'       logger::log_info("  Visit-specific variables preserved: {visit_vars_preserved}")
#'
#'       # Sample a few values to verify they're changing across visits
#'       if ("visit_bmi_kg_m2" %in% names(incontinence_long_format)) {
#'         sample_participant <- incontinence_long_format$swan_participant_id[1]
#'         sample_bmi_values <- incontinence_long_format %>%
#'           dplyr::filter(swan_participant_id == sample_participant) %>%
#'           dplyr::select(visit_number, visit_bmi_kg_m2) %>%
#'           dplyr::arrange(visit_number)
#'
#'         if (nrow(sample_bmi_values) > 1) {
#'           bmi_range <- paste(round(range(sample_bmi_values$visit_bmi_kg_m2, na.rm = TRUE), 1), collapse = " to ")
#'           logger::log_info("  Sample BMI range for participant {sample_participant}: {bmi_range}")
#'         }
#'       }
#'     }
#'   }
#'
#'   return(incontinence_long_format)
#' }
#'
#' #' Identify visit-specific demographic variables in dataset
#' #' @noRd
#' identify_visit_specific_demographic_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'   visit_specific_vars <- list()
#'
#'   # AGE variables - match only single digit visit numbers (AGE0, AGE1, AGE2, ..., AGE9)
#'   # Also match AGE10 specifically, but exclude AGE11, AGE21, etc.
#'   age_variables <- variable_names[stringr::str_detect(variable_names, "^AGE(\\d|10)$")]
#'   if (length(age_variables) > 0) {
#'     visit_specific_vars[["age_years"]] <- age_variables
#'   }
#'
#'   # BMI variables - match only single digit visit numbers (BMI0, BMI1, BMI2, ..., BMI9, BMI10)
#'   bmi_variables <- variable_names[stringr::str_detect(variable_names, "^BMI(\\d|10)$")]
#'   if (length(bmi_variables) > 0) {
#'     visit_specific_vars[["bmi_kg_m2"]] <- bmi_variables
#'   }
#'
#'   # Weight variables - match only single digit visit numbers
#'   weight_variables <- variable_names[stringr::str_detect(variable_names, "^WEIGHT(\\d|10)$")]
#'   if (length(weight_variables) > 0) {
#'     visit_specific_vars[["weight_kg"]] <- weight_variables
#'   }
#'
#'   # Height variables - match only single digit visit numbers
#'   height_variables <- variable_names[stringr::str_detect(variable_names, "^HEIGHT(\\d|10)$")]
#'   if (length(height_variables) > 0) {
#'     visit_specific_vars[["height_cm"]] <- height_variables
#'   }
#'
#'   # Smoking variables - match only single digit visit numbers
#'   smoking_variables <- variable_names[stringr::str_detect(variable_names, "^SMOKER?E?(\\d|10)$")]
#'   if (length(smoking_variables) > 0) {
#'     visit_specific_vars[["smoking_status"]] <- smoking_variables
#'   }
#'
#'   # Menopausal status variables - match only single digit visit numbers
#'   menopause_variables <- variable_names[stringr::str_detect(variable_names, "^STATUS(\\d|10)$")]
#'   if (length(menopause_variables) > 0) {
#'     visit_specific_vars[["menopausal_stage"]] <- menopause_variables
#'   }
#'
#'   # Marital status variables - match only single digit visit numbers
#'   marital_variables <- variable_names[stringr::str_detect(variable_names, "^MARITALGP(\\d|10)$")]
#'   if (length(marital_variables) > 0) {
#'     visit_specific_vars[["marital_status"]] <- marital_variables
#'   }
#'
#'   # Parity variables - match only single digit visit numbers
#'   parity_variables <- variable_names[stringr::str_detect(variable_names, "^NUMCHILD(\\d|10)$")]
#'   if (length(parity_variables) > 0) {
#'     visit_specific_vars[["parity_count"]] <- parity_variables
#'   }
#'
#'   return(visit_specific_vars)
#' }
#'
#' #' Extract visit number from variable name (e.g., INVOLEA9 -> 9, AGE1 -> 1)
#' #' @noRd
#' extract_visit_number_from_variable <- function(variable_names) {
#'
#'   # For SWAN variables, extract the visit number more carefully
#'   visit_numbers <- character(length(variable_names))
#'
#'   for (i in seq_along(variable_names)) {
#'     var_name <- variable_names[i]
#'
#'     # For incontinence variables (INVOLEA0, LEKINVO7, etc.)
#'     if (stringr::str_detect(var_name, "^(INVOLEA|LEKINVO)")) {
#'       visit_num <- stringr::str_extract(var_name, "\\d+$")
#'       visit_numbers[i] <- visit_num
#'     }
#'     # For demographic variables (AGE1, BMI1, STATUS1, etc.)
#'     else if (stringr::str_detect(var_name, "^(AGE|BMI|WEIGHT|HEIGHT|STATUS|SMOKER|MARITALGP|NUMCHILD)")) {
#'       # Extract only the last number, and only if it's 0-10 (valid visit numbers)
#'       potential_visit <- stringr::str_extract(var_name, "(\\d|10)$")
#'       if (!is.na(potential_visit) && as.numeric(potential_visit) <= 10) {
#'         visit_numbers[i] <- potential_visit
#'       } else {
#'         visit_numbers[i] <- NA_character_
#'       }
#'     }
#'     else {
#'       # For other variables, extract trailing digits
#'       visit_num <- stringr::str_extract(var_name, "\\d+$")
#'       visit_numbers[i] <- visit_num
#'     }
#'   }
#'
#'   # Convert to numeric
#'   visit_numbers_numeric <- as.numeric(visit_numbers)
#'
#'   # Replace any NAs with 0 (baseline)
#'   visit_numbers_numeric[is.na(visit_numbers_numeric)] <- 0
#'
#'   return(visit_numbers_numeric)
#' }
#'
#' #' Standardize incontinence response values
#' #' @noRd
#' standardize_incontinence_response <- function(response_values) {
#'
#'   dplyr::case_when(
#'     # Standard Yes responses
#'     response_values %in% c("(2) Yes", "2", 2, "Yes", "YES", "yes") ~ 1,
#'     # Standard No responses
#'     response_values %in% c("(1) No", "1", 1, "No", "NO", "no") ~ 0,
#'     # Extended formats from later visits
#'     response_values %in% c("(2) 2: Yes", "(2)2: Yes", "2: Yes") ~ 1,
#'     response_values %in% c("(1) 1: No", "(1)1: No", "1: No") ~ 0,
#'     TRUE ~ NA_real_
#'   )
#' }
#'
#' #' Helper function to identify participant ID column
#' #' @noRd
#' identify_participant_id_variable <- function(dataset) {
#'   participant_id_patterns <- c("^SWANID$", "^ARCHID$", "^swanid$", "^archid$")
#'   for (pattern in participant_id_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' Helper function to identify visit number column
#' #' @noRd
#' identify_visit_number_variable <- function(dataset) {
#'   visit_patterns <- c("^VISIT$", "^visit$", "^Visit$", "^VISITNO$")
#'   for (pattern in visit_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' Safe numeric conversion
#' #' @noRd
#' convert_to_numeric_safely <- function(variable_values) {
#'   suppressWarnings(as.numeric(as.character(variable_values)))
#' }
#'
#' #' Standardize race/ethnicity variable
#' #' @noRd
#' standardize_race_ethnicity_variable <- function(race_values) {
#'   dplyr::case_when(
#'     stringr::str_detect(as.character(race_values), "White|Caucasian|4") ~ "White",
#'     stringr::str_detect(as.character(race_values), "Black|African|1") ~ "Black",
#'     stringr::str_detect(as.character(race_values), "Hispanic|5") ~ "Hispanic",
#'     stringr::str_detect(as.character(race_values), "Chinese|2") ~ "Chinese",
#'     stringr::str_detect(as.character(race_values), "Japanese|3") ~ "Japanese",
#'     TRUE ~ "Other"
#'   )
#' }
#'
#' #' Standardize smoking status variable
#' #' @noRd
#' standardize_smoking_status_variable <- function(smoking_values) {
#'   dplyr::case_when(
#'     smoking_values %in% c(1, "(1) Never smoked", "Never") ~ "Never",
#'     smoking_values %in% c(2, "(2) Past smoker", "Past") ~ "Past",
#'     smoking_values %in% c(3, "(3) Current smoker", "Current") ~ "Current",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Standardize menopausal status variable
#' #' @noRd
#' standardize_menopausal_status_variable <- function(status_values) {
#'   dplyr::case_when(
#'     status_values %in% c(1, "(1) Post by BSO") ~ "Post BSO",
#'     status_values %in% c(2, "(2) Natural Post", "(2) Postmenopausal") ~ "Natural Post",
#'     status_values %in% c(3, "(3) Late Peri", "(3) Late perimenopausal") ~ "Late Peri",
#'     status_values %in% c(4, "(4) Early Peri", "(4) Early perimenopausal") ~ "Early Peri",
#'     status_values %in% c(5, "(5) Pre", "(5) Premenopausal") ~ "Pre",
#'     status_values %in% c(6, "(6) Pregnant/breastfeeding") ~ "Pregnant/Breastfeeding",
#'     status_values %in% c(7, "(7) Unknown due to HT use") ~ "Unknown HT",
#'     status_values %in% c(8, "(8) Unknown due to hysterectomy") ~ "Unknown Hysterectomy",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Standardize education variable
#' #' @noRd
#' standardize_education_variable <- function(education_values) {
#'   dplyr::case_when(
#'     education_values %in% c(1, "(1) Less than high school") ~ "Less than HS",
#'     education_values %in% c(2, "(2) High school") ~ "High School",
#'     education_values %in% c(3, "(3) Some college") ~ "Some College",
#'     education_values %in% c(4, "(4) College graduate") ~ "College Graduate",
#'     education_values %in% c(5, "(5) Graduate school") ~ "Graduate School",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Standardize marital status variable
#' #' @noRd
#' standardize_marital_status_variable <- function(marital_values) {
#'   dplyr::case_when(
#'     marital_values %in% c(1, "(1) Married", "Married") ~ "Married",
#'     marital_values %in% c(2, "(2) Single", "Single") ~ "Single",
#'     marital_values %in% c(3, "(3) Divorced/Separated", "Divorced") ~ "Divorced/Separated",
#'     marital_values %in% c(4, "(4) Widowed", "Widowed") ~ "Widowed",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Combine multiple baseline sources with type-safe merging
#' #' @noRd
#' combine_multiple_baseline_sources <- function(baseline_dataset_list, verbose) {
#'
#'   if (length(baseline_dataset_list) == 1) {
#'     return(baseline_dataset_list[[1]])
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Combining {length(baseline_dataset_list)} baseline sources")
#'   }
#'
#'   # Find the most complete dataset (most non-missing participant IDs)
#'   dataset_completeness_scores <- purrr::map_dbl(baseline_dataset_list, function(baseline_dataset) {
#'     sum(!is.na(baseline_dataset$swan_participant_id))
#'   })
#'
#'   primary_baseline_dataset <- baseline_dataset_list[[which.max(dataset_completeness_scores)]]
#'
#'   if (verbose) {
#'     logger::log_info("Primary baseline dataset: {primary_baseline_dataset$source_file[1]} with {max(dataset_completeness_scores)} participants")
#'   }
#'
#'   # Merge additional datasets
#'   for (dataset_index in seq_along(baseline_dataset_list)) {
#'     if (dataset_index == which.max(dataset_completeness_scores)) next
#'
#'     secondary_baseline_dataset <- baseline_dataset_list[[dataset_index]]
#'
#'     if (verbose) {
#'       logger::log_info("Merging secondary baseline: {secondary_baseline_dataset$source_file[1]}")
#'     }
#'
#'     # Harmonize data types before merging
#'     secondary_baseline_dataset <- harmonize_baseline_data_types(
#'       secondary_baseline_dataset,
#'       primary_baseline_dataset,
#'       verbose
#'     )
#'
#'     primary_baseline_dataset <- primary_baseline_dataset %>%
#'       dplyr::full_join(
#'         secondary_baseline_dataset,
#'         by = "swan_participant_id",
#'         suffix = c("", "_secondary")
#'       )
#'
#'     # Smart coalescing - only coalesce variables that exist in both datasets
#'     baseline_variables_to_merge <- c(
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_parity_count",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_menopausal_stage",
#'       "baseline_education_level",
#'       "baseline_marital_status"
#'     )
#'
#'     # Check which variables actually exist with _secondary suffix
#'     available_columns <- names(primary_baseline_dataset)
#'
#'     for (baseline_variable in baseline_variables_to_merge) {
#'       secondary_variable_name <- paste0(baseline_variable, "_secondary")
#'
#'       if (baseline_variable %in% available_columns && secondary_variable_name %in% available_columns) {
#'         if (verbose) {
#'           logger::log_info("  Coalescing {baseline_variable}")
#'         }
#'
#'         # Apply appropriate coalescing based on data type
#'         if (stringr::str_detect(baseline_variable, "age|bmi|parity|count")) {
#'           # Numeric variables
#'           primary_baseline_dataset[[baseline_variable]] <- apply_safe_numeric_coalesce(
#'             primary_baseline_dataset[[baseline_variable]],
#'             primary_baseline_dataset[[secondary_variable_name]]
#'           )
#'         } else {
#'           # Character variables
#'           primary_baseline_dataset[[baseline_variable]] <- apply_safe_character_coalesce(
#'             primary_baseline_dataset[[baseline_variable]],
#'             primary_baseline_dataset[[secondary_variable_name]]
#'           )
#'         }
#'       } else if (verbose) {
#'         logger::log_info("  Skipping {baseline_variable} - not available in secondary dataset")
#'       }
#'     }
#'
#'     # Remove all secondary columns
#'     primary_baseline_dataset <- primary_baseline_dataset %>%
#'       dplyr::select(-dplyr::ends_with("_secondary"))
#'   }
#'
#'   if (verbose) {
#'     final_participant_count <- sum(!is.na(primary_baseline_dataset$swan_participant_id))
#'     logger::log_info("Combined baseline dataset: {final_participant_count} total participants")
#'   }
#'
#'   return(primary_baseline_dataset)
#' }
#'
#' #' Harmonize data types between baseline datasets
#' #' @noRd
#' harmonize_baseline_data_types <- function(secondary_dataset, primary_dataset, verbose) {
#'
#'   baseline_column_names <- names(secondary_dataset)[stringr::str_starts(names(secondary_dataset), "baseline_")]
#'
#'   for (column_name in baseline_column_names) {
#'     if (column_name %in% names(primary_dataset)) {
#'
#'       primary_data_type <- class(primary_dataset[[column_name]])[1]
#'       secondary_data_type <- class(secondary_dataset[[column_name]])[1]
#'
#'       if (primary_data_type != secondary_data_type) {
#'         if (verbose) {
#'           logger::log_info("Harmonizing {column_name}: {secondary_data_type} -> {primary_data_type}")
#'         }
#'
#'         # Convert secondary to match primary type
#'         if (primary_data_type %in% c("numeric", "double")) {
#'           secondary_dataset[[column_name]] <- convert_to_numeric_safely(secondary_dataset[[column_name]])
#'         } else if (primary_data_type == "character") {
#'           secondary_dataset[[column_name]] <- as.character(secondary_dataset[[column_name]])
#'         } else if (primary_data_type == "logical") {
#'           secondary_dataset[[column_name]] <- as.logical(secondary_dataset[[column_name]])
#'         }
#'       }
#'     }
#'   }
#'
#'   return(secondary_dataset)
#' }
#'
#' #' Type-safe numeric coalesce
#' #' @noRd
#' apply_safe_numeric_coalesce <- function(primary_values, secondary_values) {
#'   # Convert both to numeric if needed
#'   primary_numeric <- if (is.numeric(primary_values)) primary_values else convert_to_numeric_safely(primary_values)
#'   secondary_numeric <- if (is.numeric(secondary_values)) secondary_values else convert_to_numeric_safely(secondary_values)
#'   dplyr::coalesce(primary_numeric, secondary_numeric)
#' }
#'
#' #' Type-safe character coalesce
#' #' @noRd
#' apply_safe_character_coalesce <- function(primary_values, secondary_values) {
#'   # Convert both to character if needed
#'   primary_character <- if (is.character(primary_values)) primary_values else as.character(primary_values)
#'   secondary_character <- if (is.character(secondary_values)) secondary_values else as.character(secondary_values)
#'   dplyr::coalesce(primary_character, secondary_character)
#' }
#'
#' #' Merge baseline demographics with longitudinal data
#' #' @noRd
#' merge_baseline_with_longitudinal_data <- function(baseline_data, longitudinal_data, verbose) {
#'
#'   if (verbose) {
#'     baseline_participant_count <- length(unique(baseline_data$swan_participant_id))
#'     longitudinal_observation_count <- nrow(longitudinal_data)
#'     logger::log_info("Merging baseline demographics with longitudinal measurements")
#'     logger::log_info("Baseline participants: {baseline_participant_count}")
#'     logger::log_info("Longitudinal observations: {longitudinal_observation_count}")
#'   }
#'
#'   # Merge datasets
#'   merged_comprehensive_data <- longitudinal_data %>%
#'     dplyr::left_join(
#'       baseline_data %>% dplyr::select(-source_file),
#'       by = "swan_participant_id"
#'     )
#'
#'   # Add derived time variables
#'   merged_comprehensive_data <- merged_comprehensive_data %>%
#'     dplyr::mutate(
#'       # Calculate current age at each visit
#'       current_age_years = baseline_age_years + years_since_baseline,
#'       # Add visit timing categories
#'       visit_timing_category = dplyr::case_when(
#'         visit_number == 0 ~ "Baseline",
#'         visit_number %in% 1:3 ~ "Early Follow-up",
#'         visit_number %in% 4:7 ~ "Mid Follow-up",
#'         visit_number >= 8 ~ "Late Follow-up",
#'         TRUE ~ "Unknown"
#'       )
#'     ) %>%
#'     dplyr::arrange(swan_participant_id, visit_number)
#'
#'   if (verbose) {
#'     merged_observation_count <- nrow(merged_comprehensive_data)
#'     participants_with_both_data <- length(unique(merged_comprehensive_data$swan_participant_id[!is.na(merged_comprehensive_data$baseline_age_years)]))
#'     logger::log_info("Merged comprehensive dataset: {merged_observation_count} observations")
#'     logger::log_info("Participants with both baseline + longitudinal data: {participants_with_both_data}")
#'   }
#'
#'   return(merged_comprehensive_data)
#' }
#'
#' #' Apply filtering for valid responses
#' #' @noRd
#' apply_response_filtering <- function(merged_data, verbose) {
#'
#'   if (verbose) {
#'     original_observation_count <- nrow(merged_data)
#'     logger::log_info("Applying response filtering to remove invalid/missing incontinence responses")
#'   }
#'
#'   filtered_data <- merged_data %>%
#'     dplyr::filter(
#'       !is.na(incontinence_status),
#'       !is.na(swan_participant_id),
#'       !is.na(visit_number)
#'     )
#'
#'   if (verbose) {
#'     filtered_observation_count <- nrow(filtered_data)
#'     observations_removed <- original_observation_count - filtered_observation_count
#'     logger::log_info("Filtering completed: {filtered_observation_count} observations retained")
#'     logger::log_info("Observations removed: {observations_removed}")
#'   }
#'
#'   return(filtered_data)
#' }
#'
#' #' Format comprehensive output dataset
#' #' @noRd
#' format_comprehensive_output <- function(merged_data, format, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Formatting comprehensive output as: {format}")
#'     all_column_names <- paste(names(merged_data), collapse = ", ")
#'     logger::log_info("Available columns: {all_column_names}")
#'   }
#'
#'   if (format == "long") {
#'     # Identify visit-specific columns
#'     visit_specific_column_names <- names(merged_data)[stringr::str_starts(names(merged_data), "visit_")]
#'
#'     if (verbose && length(visit_specific_column_names) > 0) {
#'       visit_vars_string <- paste(visit_specific_column_names, collapse = ", ")
#'       logger::log_info("Visit-specific columns preserved: {visit_vars_string}")
#'     } else if (verbose) {
#'       logger::log_warn("No visit-specific columns found in final dataset")
#'     }
#'
#'     # Build comprehensive column selection
#'     baseline_demographic_columns <- c(
#'       "swan_participant_id",
#'       "visit_number",
#'       "years_since_baseline",
#'       "current_age_years",
#'       "visit_timing_category",
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_parity_count",
#'       "baseline_menopausal_stage",
#'       "baseline_education_level",
#'       "baseline_marital_status"
#'     )
#'
#'     incontinence_measurement_columns <- c(
#'       "incontinence_status",
#'       "incontinence_raw_response",
#'       "incontinence_source_variable"
#'     )
#'
#'     # Combine all columns, keeping only those that exist
#'     all_desired_columns <- c(
#'       baseline_demographic_columns,
#'       incontinence_measurement_columns,
#'       visit_specific_column_names
#'     )
#'
#'     columns_that_exist_in_data <- intersect(all_desired_columns, names(merged_data))
#'
#'     if (verbose) {
#'       logger::log_info("Selecting {length(columns_that_exist_in_data)} columns for long format output")
#'     }
#'
#'     # Create final long format dataset
#'     final_comprehensive_dataset <- merged_data %>%
#'       dplyr::select(dplyr::all_of(columns_that_exist_in_data)) %>%
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         incontinence_source_variable,
#'         .keep_all = TRUE
#'       ) %>%
#'       dplyr::arrange(swan_participant_id, visit_number)
#'
#'   } else {
#'     # Wide format conversion
#'     visit_specific_column_names <- names(merged_data)[stringr::str_starts(names(merged_data), "visit_")]
#'
#'     baseline_columns_for_wide <- c(
#'       "swan_participant_id",
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_parity_count",
#'       "baseline_menopausal_stage",
#'       "baseline_education_level",
#'       "baseline_marital_status"
#'     )
#'
#'     measurement_columns_for_wide <- c(
#'       "visit_number",
#'       "incontinence_status",
#'       "current_age_years"
#'     )
#'
#'     all_columns_for_wide <- c(
#'       baseline_columns_for_wide,
#'       measurement_columns_for_wide,
#'       visit_specific_column_names
#'     )
#'
#'     columns_that_exist_for_wide <- intersect(all_columns_for_wide, names(merged_data))
#'
#'     if (verbose) {
#'       logger::log_info("Converting to wide format with {length(columns_that_exist_for_wide)} base columns")
#'     }
#'
#'     final_comprehensive_dataset <- merged_data %>%
#'       dplyr::select(dplyr::all_of(columns_that_exist_for_wide)) %>%
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         .keep_all = TRUE
#'       ) %>%
#'       tidyr::pivot_wider(
#'         names_from = visit_number,
#'         values_from = c(
#'           dplyr::any_of(visit_specific_column_names),
#'           incontinence_status,
#'           current_age_years
#'         ),
#'         names_sep = "_visit_"
#'       )
#'   }
#'
#'   if (verbose) {
#'     final_row_count <- nrow(final_comprehensive_dataset)
#'     final_column_count <- ncol(final_comprehensive_dataset)
#'     unique_participant_count <- length(unique(final_comprehensive_dataset$swan_participant_id))
#'
#'     logger::log_info("Final comprehensive dataset formatting completed")
#'     logger::log_info("Dimensions: {final_row_count} rows × {final_column_count} columns")
#'     logger::log_info("Unique participants: {unique_participant_count}")
#'
#'     if (format == "long") {
#'       final_visit_specific_count <- sum(stringr::str_starts(names(final_comprehensive_dataset), "visit_"))
#'       visit_range_min <- min(final_comprehensive_dataset$visit_number, na.rm = TRUE)
#'       visit_range_max <- max(final_comprehensive_dataset$visit_number, na.rm = TRUE)
#'       logger::log_info("Visit-specific variables in final dataset: {final_visit_specific_count}")
#'       logger::log_info("Visit range: {visit_range_min} to {visit_range_max}")
#'     }
#'   }
#'
#'   return(final_comprehensive_dataset)
#' }
#'
#' comprehensive_swan_dataset_final <- create_comprehensive_swan_dataset(
#'   swan_data_directory = swan_data_directory,
#'   output_format = "long",
#'   verbose = TRUE,
#'   include_visit_demographics = TRUE,
#'   filter_valid_responses = TRUE
#' )
#'
#' # version 10, Create Comprehensive SWAN Dataset with Visit-Specific Variables
#' #' Create Comprehensive SWAN Dataset with Visit-Specific Variables
#' #'
#' #' This function creates a comprehensive dataset from SWAN study files, extracting
#' #' baseline demographics and visit-specific variables including urinary incontinence
#' #' measures (INVOLEA/LEKINVO) and other longitudinal health indicators.
#' #'
#' #' @param swan_data_directory Character. Path to directory containing SWAN .rda files
#' #' @param output_format Character. Either "long" or "wide" format for final dataset
#' #' @param verbose Logical. Enable detailed logging output to console
#' #' @param include_visit_demographics Logical. Include visit-specific demographic
#' #'   variables (AGE, BMI, STATUS, etc.) in addition to baseline
#' #' @param filter_valid_responses Logical. Remove rows with missing INVOLEA responses
#' #'
#' #' @return tibble. Comprehensive SWAN dataset with baseline and visit-specific variables
#' #'
#' #' @examples
#' #' # Example 1: Basic long format with visit demographics
#' #' swan_long_comprehensive <- create_comprehensive_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "long",
#' #'   verbose = TRUE,
#' #'   include_visit_demographics = TRUE,
#' #'   filter_valid_responses = TRUE
#' #' )
#' #'
#' #' # Example 2: Wide format for analysis, no filtering
#' #' swan_wide_complete <- create_comprehensive_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "wide",
#' #'   verbose = FALSE,
#' #'   include_visit_demographics = TRUE,
#' #'   filter_valid_responses = FALSE
#' #' )
#' #'
#' #' # Example 3: Minimal dataset with only baseline + INVOLEA
#' #' swan_minimal <- create_comprehensive_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "long",
#' #'   verbose = TRUE,
#' #'   include_visit_demographics = FALSE,
#' #'   filter_valid_responses = TRUE
#' #' )
#' #'
#' #' @importFrom dplyr filter select mutate arrange group_by summarise
#' #'   left_join full_join distinct coalesce case_when all_of any_of ends_with
#' #' @importFrom tidyr pivot_longer pivot_wider
#' #' @importFrom stringr str_detect str_extract str_replace_all str_starts
#' #'   str_remove str_match
#' #' @importFrom purrr map_dfr map_chr map_lgl keep walk compact map
#' #' @importFrom assertthat assert_that
#' #' @importFrom logger log_info log_warn log_error
#' #' @importFrom tibble tibble as_tibble
#' #' @importFrom readr parse_number
#' #' @importFrom glue glue
#' #'
#' #' @export
#' create_comprehensive_swan_dataset <- function(swan_data_directory,
#'                                               output_format = "long",
#'                                               verbose = TRUE,
#'                                               include_visit_demographics = TRUE,
#'                                               filter_valid_responses = TRUE) {
#'
#'   # Input validation with assertthat
#'   if (verbose) {
#'     logger::log_info("Creating comprehensive SWAN dataset with visit-specific variables")
#'     logger::log_info("Function inputs - directory: {swan_data_directory}")
#'     logger::log_info("Function inputs - format: {output_format}, verbose: {verbose}")
#'     logger::log_info("Function inputs - include_visit_demographics: {include_visit_demographics}")
#'     logger::log_info("Function inputs - filter_valid_responses: {filter_valid_responses}")
#'   }
#'
#'   assertthat::assert_that(
#'     is.character(swan_data_directory),
#'     msg = "swan_data_directory must be a character string path"
#'   )
#'   assertthat::assert_that(
#'     dir.exists(swan_data_directory),
#'     msg = glue::glue("Directory does not exist: {swan_data_directory}")
#'   )
#'   assertthat::assert_that(
#'     output_format %in% c("long", "wide"),
#'     msg = "output_format must be 'long' or 'wide'"
#'   )
#'   assertthat::assert_that(
#'     is.logical(verbose),
#'     msg = "verbose must be TRUE or FALSE"
#'   )
#'   assertthat::assert_that(
#'     is.logical(include_visit_demographics),
#'     msg = "include_visit_demographics must be TRUE or FALSE"
#'   )
#'   assertthat::assert_that(
#'     is.logical(filter_valid_responses),
#'     msg = "filter_valid_responses must be TRUE or FALSE"
#'   )
#'
#'   # Step 1: Extract baseline demographics
#'   baseline_participant_demographics <- extract_baseline_participant_demographics(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Baseline demographics extracted: {nrow(baseline_participant_demographics)} participants")
#'   }
#'
#'   # Step 2: Extract comprehensive longitudinal data
#'   longitudinal_health_measurements <- extract_comprehensive_longitudinal_data(
#'     directory_path = swan_data_directory,
#'     include_visit_demographics = include_visit_demographics,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Longitudinal data extracted: {nrow(longitudinal_health_measurements)} observations")
#'   }
#'
#'   # Step 3: Merge datasets
#'   merged_comprehensive_dataset <- merge_baseline_with_longitudinal_data(
#'     baseline_data = baseline_participant_demographics,
#'     longitudinal_data = longitudinal_health_measurements,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Data merged: {nrow(merged_comprehensive_dataset)} total observations")
#'   }
#'
#'   # Step 4: Apply filtering if requested
#'   if (filter_valid_responses) {
#'     filtered_comprehensive_dataset <- apply_response_filtering(
#'       merged_data = merged_comprehensive_dataset,
#'       verbose = verbose
#'     )
#'   } else {
#'     filtered_comprehensive_dataset <- merged_comprehensive_dataset
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("After filtering: {nrow(filtered_comprehensive_dataset)} observations")
#'   }
#'
#'   # Step 5: Format final output
#'   final_comprehensive_dataset <- format_comprehensive_output(
#'     merged_data = filtered_comprehensive_dataset,
#'     format = output_format,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Comprehensive SWAN dataset creation completed")
#'     logger::log_info("Final dataset dimensions: {nrow(final_comprehensive_dataset)} rows × {ncol(final_comprehensive_dataset)} columns")
#'     logger::log_info("Output file path: Not saved to file (returned as object)")
#'   }
#'
#'   return(final_comprehensive_dataset)
#' }
#'
#' #' Extract baseline demographics from SWAN screener/baseline files
#' #' @noRd
#' extract_baseline_participant_demographics <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting baseline participant demographics from SWAN files")
#'   }
#'
#'   # Find baseline/screener files using known ICPSR numbers
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   # Known baseline file patterns from SWAN documentation
#'   baseline_file_identifiers <- c("28762", "04368", "baseline", "screener")
#'
#'   baseline_candidate_files <- purrr::keep(all_rda_files, function(file_path) {
#'     file_name <- tolower(basename(file_path))
#'     any(stringr::str_detect(file_name, baseline_file_identifiers))
#'   })
#'
#'   if (verbose) {
#'     logger::log_info("Found {length(baseline_candidate_files)} potential baseline files")
#'     purrr::walk(baseline_candidate_files, ~logger::log_info("  Candidate file: {basename(.x)}"))
#'   }
#'
#'   # Process each baseline file
#'   baseline_dataset_list <- purrr::map(baseline_candidate_files, function(file_path) {
#'     process_single_baseline_file(file_path, verbose)
#'   })
#'
#'   # Remove NULL results and combine
#'   baseline_dataset_list <- purrr::compact(baseline_dataset_list)
#'
#'   if (length(baseline_dataset_list) == 0) {
#'     logger::log_error("No valid baseline demographic data found in any files")
#'     stop("No baseline demographic data could be extracted")
#'   }
#'
#'   # Combine multiple baseline sources
#'   combined_baseline_demographics <- combine_multiple_baseline_sources(
#'     baseline_dataset_list,
#'     verbose
#'   )
#'
#'   return(combined_baseline_demographics)
#' }
#'
#' #' Process a single baseline file to extract demographics
#' #' @noRd
#' process_single_baseline_file <- function(file_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Processing baseline file: {basename(file_path)}")
#'   }
#'
#'   # Load the file safely
#'   file_environment <- new.env()
#'
#'   tryCatch({
#'     load(file_path, envir = file_environment)
#'   }, error = function(e) {
#'     if (verbose) {
#'       logger::log_error("Failed to load {basename(file_path)}: {e$message}")
#'     }
#'     return(NULL)
#'   })
#'
#'   if (length(ls(file_environment)) == 0) {
#'     if (verbose) {
#'       logger::log_warn("No datasets found in {basename(file_path)}")
#'     }
#'     return(NULL)
#'   }
#'
#'   raw_baseline_dataset <- file_environment[[ls(file_environment)[1]]]
#'
#'   # Identify demographic variables
#'   demographic_variable_mapping <- identify_baseline_demographic_variables(
#'     raw_baseline_dataset
#'   )
#'
#'   if (is.null(demographic_variable_mapping$participant_id)) {
#'     if (verbose) {
#'       logger::log_warn("No participant ID found in {basename(file_path)}")
#'     }
#'     return(NULL)
#'   }
#'
#'   # Extract and standardize demographics
#'   standardized_baseline_demographics <- extract_standardized_demographics(
#'     dataset = raw_baseline_dataset,
#'     variable_mapping = demographic_variable_mapping,
#'     source_file = basename(file_path),
#'     verbose = verbose
#'   )
#'
#'   return(standardized_baseline_demographics)
#' }
#'
#' #' Identify demographic variables in baseline dataset
#' #' @noRd
#' identify_baseline_demographic_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Participant ID patterns
#'   participant_id_patterns <- c("^SWANID$", "^ARCHID$", "^swanid$", "^archid$")
#'   participant_id_variable <- find_matching_variable(variable_names, participant_id_patterns)
#'
#'   # Demographic variable patterns - updated to match actual SWAN variable names
#'   age_patterns <- c("^AGE0?$", "^age0?$", "AGE.*0$")
#'   bmi_patterns <- c("^BMI0?$", "^bmi0?$", "BMI.*0$")
#'   race_patterns <- c("^RACE$", "^race$", "^ETHNIC")
#'
#'   # Updated smoking patterns based on actual SWAN variables found
#'   smoking_patterns <- c(
#'     "^SMOKER$", "^SMOKED$", "^SMOKE_R$",  # Screener variables
#'     "^SMOKERE0$", "^SMOKENO0$", "^SMOKEYR0$"  # Baseline variables
#'   )
#'
#'   # Updated parity patterns based on actual SWAN variables found
#'   parity_patterns <- c(
#'     "^NUMCHILD$", "^CHILDREN$",  # Screener variables
#'     "^CHILDMO0$", "^CHILDIL0$"   # Baseline variables - but these might not be parity
#'   )
#'
#'   # For parity, let's also try the screener variables that actually have data
#'   # Since NUMCHILD in screener has 0 values, let's try CHILDREN
#'   if ("CHILDREN" %in% variable_names) {
#'     # Check if CHILDREN has more data than NUMCHILD
#'     children_data <- sum(!is.na(dataset[["CHILDREN"]]))
#'     numchild_data <- if ("NUMCHILD" %in% variable_names) sum(!is.na(dataset[["NUMCHILD"]])) else 0
#'
#'     if (children_data > numchild_data) {
#'       parity_patterns <- c("^CHILDREN$", parity_patterns)
#'     }
#'   }
#'
#'   # For smoking, let's prioritize variables that actually have data
#'   # Check which smoking variable has the most data
#'   smoking_candidates <- variable_names[stringr::str_detect(variable_names, paste(smoking_patterns, collapse = "|"))]
#'   if (length(smoking_candidates) > 0) {
#'     smoking_data_counts <- sapply(smoking_candidates, function(var) {
#'       sum(!is.na(dataset[[var]]))
#'     })
#'     # Reorder patterns to put the variable with most data first
#'     best_smoking_var <- names(smoking_data_counts)[which.max(smoking_data_counts)]
#'     smoking_patterns <- c(paste0("^", best_smoking_var, "$"), smoking_patterns)
#'   }
#'
#'   # Updated menopausal patterns to match SWAN codebook
#'   menopausal_patterns <- c("^STATUS0?$", "^MENO", "^MEN_FLAG$")
#'
#'   education_patterns <- c("^DEGREE$", "^EDUCATION", "^EDUC")
#'   marital_patterns <- c("^MARITAL", "^MARITALGP")
#'
#'   return(list(
#'     participant_id = participant_id_variable,
#'     age = find_matching_variable(variable_names, age_patterns),
#'     bmi = find_matching_variable(variable_names, bmi_patterns),
#'     race = find_matching_variable(variable_names, race_patterns),
#'     smoking = find_matching_variable(variable_names, smoking_patterns),
#'     parity = find_matching_variable(variable_names, parity_patterns),
#'     menopausal = find_matching_variable(variable_names, menopausal_patterns),
#'     education = find_matching_variable(variable_names, education_patterns),
#'     marital = find_matching_variable(variable_names, marital_patterns)
#'   ))
#' }
#'
#' #' Find first matching variable from patterns
#' #' @noRd
#' find_matching_variable <- function(variable_names, patterns) {
#'   for (pattern in patterns) {
#'     matches <- variable_names[stringr::str_detect(variable_names, pattern)]
#'     if (length(matches) > 0) {
#'       return(matches[1])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' Extract and standardize demographic variables
#' #' @noRd
#' extract_standardized_demographics <- function(dataset, variable_mapping,
#'                                               source_file, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("  Variable mapping for {source_file}:")
#'     for (var_type in names(variable_mapping)) {
#'       var_name <- variable_mapping[[var_type]]
#'       if (!is.null(var_name)) {
#'         logger::log_info("    {var_type}: {var_name}")
#'       } else {
#'         logger::log_info("    {var_type}: NOT FOUND")
#'       }
#'     }
#'   }
#'
#'   # Start with participant ID
#'   standardized_demographics <- tibble::tibble(
#'     swan_participant_id = as.character(dataset[[variable_mapping$participant_id]])
#'   )
#'
#'   # Add demographic variables with type-safe conversion
#'   if (!is.null(variable_mapping$age)) {
#'     standardized_demographics$baseline_age_years <-
#'       convert_to_numeric_safely(dataset[[variable_mapping$age]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_age_years))
#'       logger::log_info("    Age variable {variable_mapping$age}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   if (!is.null(variable_mapping$bmi)) {
#'     standardized_demographics$baseline_bmi_kg_m2 <-
#'       convert_to_numeric_safely(dataset[[variable_mapping$bmi]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_bmi_kg_m2))
#'       logger::log_info("    BMI variable {variable_mapping$bmi}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   if (!is.null(variable_mapping$race)) {
#'     standardized_demographics$baseline_race_ethnicity <-
#'       standardize_race_ethnicity_variable(dataset[[variable_mapping$race]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_race_ethnicity))
#'       logger::log_info("    Race variable {variable_mapping$race}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   if (!is.null(variable_mapping$smoking)) {
#'     standardized_demographics$baseline_smoking_status <-
#'       standardize_smoking_status_variable(dataset[[variable_mapping$smoking]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_smoking_status))
#'       logger::log_info("    Smoking variable {variable_mapping$smoking}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   if (!is.null(variable_mapping$parity)) {
#'     standardized_demographics$baseline_parity_count <-
#'       convert_to_numeric_safely(dataset[[variable_mapping$parity]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_parity_count))
#'       logger::log_info("    Parity variable {variable_mapping$parity}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   if (!is.null(variable_mapping$menopausal)) {
#'     standardized_demographics$baseline_menopausal_stage <-
#'       standardize_menopausal_status_variable(dataset[[variable_mapping$menopausal]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_menopausal_stage))
#'       logger::log_info("    Menopausal variable {variable_mapping$menopausal}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   if (!is.null(variable_mapping$education)) {
#'     standardized_demographics$baseline_education_level <-
#'       standardize_education_variable(dataset[[variable_mapping$education]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_education_level))
#'       logger::log_info("    Education variable {variable_mapping$education}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   if (!is.null(variable_mapping$marital)) {
#'     standardized_demographics$baseline_marital_status <-
#'       standardize_marital_status_variable(dataset[[variable_mapping$marital]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_marital_status))
#'       logger::log_info("    Marital variable {variable_mapping$marital}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   standardized_demographics$source_file <- source_file
#'
#'   if (verbose) {
#'     valid_participant_count <- sum(!is.na(standardized_demographics$swan_participant_id))
#'     demographic_variable_count <- ncol(standardized_demographics) - 2  # exclude ID and source
#'     logger::log_info("  Extracted {valid_participant_count} participants with {demographic_variable_count} demographic variables")
#'   }
#'
#'   return(standardized_demographics)
#' }
#'
#' #' Extract comprehensive longitudinal data including visit-specific variables
#' #' @noRd
#' extract_comprehensive_longitudinal_data <- function(directory_path,
#'                                                     include_visit_demographics,
#'                                                     verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting comprehensive longitudinal data from all SWAN files")
#'   }
#'
#'   # Find all RDA files
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   # Process each file for longitudinal variables
#'   longitudinal_dataset_list <- purrr::map(all_rda_files, function(file_path) {
#'     process_single_longitudinal_file(
#'       file_path = file_path,
#'       include_visit_demographics = include_visit_demographics,
#'       verbose = verbose
#'     )
#'   })
#'
#'   # Remove NULL results
#'   longitudinal_dataset_list <- purrr::compact(longitudinal_dataset_list)
#'
#'   if (length(longitudinal_dataset_list) == 0) {
#'     logger::log_error("No longitudinal data found in any files")
#'     stop("No longitudinal data could be extracted")
#'   }
#'
#'   # Combine all longitudinal datasets
#'   combined_longitudinal_data <- purrr::map_dfr(longitudinal_dataset_list, ~.x)
#'
#'   # Remove duplicate observations (same participant, visit, variable)
#'   deduped_longitudinal_data <- combined_longitudinal_data %>%
#'     dplyr::distinct(
#'       swan_participant_id,
#'       visit_number,
#'       incontinence_source_variable,
#'       .keep_all = TRUE
#'     )
#'
#'   if (verbose) {
#'     unique_participant_count <- length(unique(deduped_longitudinal_data$swan_participant_id))
#'     visit_range <- paste(
#'       min(deduped_longitudinal_data$visit_number, na.rm = TRUE), "to",
#'       max(deduped_longitudinal_data$visit_number, na.rm = TRUE)
#'     )
#'     logger::log_info("Comprehensive longitudinal data extracted: {nrow(deduped_longitudinal_data)} observations")
#'     logger::log_info("Visit range: {visit_range}")
#'     logger::log_info("Unique participants with longitudinal data: {unique_participant_count}")
#'   }
#'
#'   return(deduped_longitudinal_data)
#' }
#'
#' #' Process single file for longitudinal variables
#' #' @noRd
#' process_single_longitudinal_file <- function(file_path, include_visit_demographics, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Scanning {basename(file_path)} for longitudinal variables")
#'   }
#'
#'   # Load file safely
#'   file_environment <- new.env()
#'
#'   tryCatch({
#'     load(file_path, envir = file_environment)
#'   }, error = function(e) {
#'     if (verbose) {
#'       logger::log_warn("Failed to load {basename(file_path)}: {e$message}")
#'     }
#'     return(NULL)
#'   })
#'
#'   if (length(ls(file_environment)) == 0) return(NULL)
#'
#'   raw_longitudinal_dataset <- file_environment[[ls(file_environment)[1]]]
#'
#'   # Look for incontinence variables (INVOLEA/LEKINVO)
#'   incontinence_variable_names <- identify_incontinence_variables(raw_longitudinal_dataset)
#'
#'   if (length(incontinence_variable_names) == 0) {
#'     return(NULL)  # No incontinence variables in this file
#'   }
#'
#'   if (verbose) {
#'     incontinence_vars_string <- paste(incontinence_variable_names, collapse = ", ")
#'     logger::log_info("  Found {length(incontinence_variable_names)} incontinence variables: {incontinence_vars_string}")
#'   }
#'
#'   # Extract longitudinal data
#'   extracted_longitudinal_data <- extract_longitudinal_measurements(
#'     dataset = raw_longitudinal_dataset,
#'     incontinence_variables = incontinence_variable_names,
#'     include_visit_demographics = include_visit_demographics,
#'     source_file = basename(file_path),
#'     verbose = verbose
#'   )
#'
#'   return(extracted_longitudinal_data)
#' }
#'
#' #' Identify incontinence variables (INVOLEA/LEKINVO) in dataset
#' #' @noRd
#' identify_incontinence_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Look for INVOLEA and LEKINVO patterns
#'   incontinence_patterns <- c("INVOLEA\\d*", "LEKINVO\\d*", "involea\\d*", "lekinvo\\d*")
#'
#'   incontinence_variables <- c()
#'   for (pattern in incontinence_patterns) {
#'     matching_variables <- variable_names[stringr::str_detect(variable_names, pattern)]
#'     incontinence_variables <- c(incontinence_variables, matching_variables)
#'   }
#'
#'   return(unique(incontinence_variables))
#' }
#'
#' #' Extract longitudinal measurements including visit-specific demographics
#' #' @noRd
#' extract_longitudinal_measurements <- function(dataset, incontinence_variables,
#'                                               include_visit_demographics,
#'                                               source_file, verbose) {
#'
#'   # Identify core variables
#'   participant_id_variable <- identify_participant_id_variable(dataset)
#'   visit_number_variable <- identify_visit_number_variable(dataset)
#'
#'   if (is.null(participant_id_variable)) {
#'     if (verbose) {
#'       logger::log_warn("No participant ID found in {source_file}")
#'     }
#'     return(NULL)
#'   }
#'
#'   # Start with core columns
#'   core_column_names <- c(participant_id_variable)
#'   if (!is.null(visit_number_variable)) {
#'     core_column_names <- c(core_column_names, visit_number_variable)
#'   }
#'
#'   # Identify ALL visit-specific demographic variables if requested
#'   visit_specific_variables <- list()
#'
#'   if (include_visit_demographics) {
#'     visit_specific_variables <- identify_visit_specific_demographic_variables(dataset)
#'
#'     if (verbose && length(visit_specific_variables) > 0) {
#'       total_visit_vars <- sum(lengths(visit_specific_variables))
#'       visit_vars_string <- paste(names(visit_specific_variables), collapse = ", ")
#'       logger::log_info("  Visit-specific demographics found: {visit_vars_string} (total: {total_visit_vars} variables)")
#'     }
#'   }
#'
#'   # Combine all variables to extract - flatten the visit-specific variables list
#'   all_visit_specific_vars <- unlist(visit_specific_variables)
#'   all_variables_to_extract <- c(
#'     core_column_names,
#'     all_visit_specific_vars,  # Now includes ALL visit-specific variables
#'     incontinence_variables
#'   )
#'
#'   # Extract data
#'   longitudinal_measurements <- tibble::as_tibble(dataset) %>%
#'     dplyr::select(dplyr::all_of(all_variables_to_extract))
#'
#'   # Standardize core column names
#'   names(longitudinal_measurements)[1] <- "swan_participant_id"
#'   if (!is.null(visit_number_variable)) {
#'     names(longitudinal_measurements)[2] <- "visit_number_from_file"
#'   } else {
#'     longitudinal_measurements$visit_number_from_file <- NA_real_
#'   }
#'
#'   # Convert incontinence variables to long format first
#'   incontinence_long_format <- longitudinal_measurements %>%
#'     tidyr::pivot_longer(
#'       cols = dplyr::all_of(incontinence_variables),
#'       names_to = "incontinence_source_variable",
#'       values_to = "incontinence_raw_response"
#'     ) %>%
#'     dplyr::mutate(
#'       swan_participant_id = as.character(swan_participant_id),
#'       # Extract visit number from incontinence variable name
#'       visit_number = extract_visit_number_from_variable(incontinence_source_variable),
#'       incontinence_status = standardize_incontinence_response(incontinence_raw_response),
#'       years_since_baseline = visit_number,
#'       source_file = source_file
#'     ) %>%
#'     dplyr::select(-visit_number_from_file) %>%
#'     dplyr::filter(!is.na(incontinence_raw_response))  # Remove missing responses
#'
#'   # Now add visit-specific demographic variables that match the visit number
#'   if (include_visit_demographics && length(visit_specific_variables) > 0) {
#'
#'     for (demographic_type in names(visit_specific_variables)) {
#'       variable_names_for_this_type <- visit_specific_variables[[demographic_type]]
#'
#'       # Create a mapping of visit numbers to values for this demographic type
#'       visit_demographic_mapping <- tibble::tibble()
#'
#'       for (var_name in variable_names_for_this_type) {
#'         # Extract visit number from variable name (e.g., AGE3 -> 3, BMI0 -> 0)
#'         var_visit_number <- extract_visit_number_from_variable(var_name)
#'
#'         # Get the values for this variable
#'         var_values <- dataset[[var_name]]
#'
#'         # Create temporary mapping
#'         temp_mapping <- tibble::tibble(
#'           swan_participant_id = as.character(dataset[[participant_id_variable]]),
#'           visit_number = var_visit_number,
#'           !!paste0("visit_", demographic_type) := var_values
#'         ) %>%
#'           dplyr::filter(!is.na(.data[[paste0("visit_", demographic_type)]]))
#'
#'         # Combine with existing mapping
#'         if (nrow(visit_demographic_mapping) == 0) {
#'           visit_demographic_mapping <- temp_mapping
#'         } else {
#'           visit_demographic_mapping <- dplyr::bind_rows(visit_demographic_mapping, temp_mapping)
#'         }
#'       }
#'
#'       # Merge this demographic variable with the incontinence data
#'       if (nrow(visit_demographic_mapping) > 0) {
#'         incontinence_long_format <- incontinence_long_format %>%
#'           dplyr::left_join(
#'             visit_demographic_mapping,
#'             by = c("swan_participant_id", "visit_number")
#'           )
#'       }
#'     }
#'   }
#'
#'   # Log results
#'   if (verbose && nrow(incontinence_long_format) > 0) {
#'     unique_visits <- sort(unique(incontinence_long_format$visit_number))
#'     visits_string <- paste(unique_visits, collapse = ", ")
#'     logger::log_info("  Extracted visits for {source_file}: {visits_string}")
#'     logger::log_info("  Total observations in {source_file}: {nrow(incontinence_long_format)}")
#'
#'     # Log visit-specific variables preserved
#'     visit_specific_columns <- names(incontinence_long_format)[stringr::str_starts(names(incontinence_long_format), "visit_")]
#'     if (length(visit_specific_columns) > 0) {
#'       visit_vars_preserved <- paste(stringr::str_remove(visit_specific_columns, "^visit_"), collapse = ", ")
#'       logger::log_info("  Visit-specific variables preserved: {visit_vars_preserved}")
#'
#'       # Sample a few values to verify they're changing across visits
#'       if ("visit_bmi_kg_m2" %in% names(incontinence_long_format)) {
#'         sample_participant <- incontinence_long_format$swan_participant_id[1]
#'         sample_bmi_values <- incontinence_long_format %>%
#'           dplyr::filter(swan_participant_id == sample_participant) %>%
#'           dplyr::select(visit_number, visit_bmi_kg_m2) %>%
#'           dplyr::arrange(visit_number)
#'
#'         if (nrow(sample_bmi_values) > 1) {
#'           bmi_range <- paste(round(range(sample_bmi_values$visit_bmi_kg_m2, na.rm = TRUE), 1), collapse = " to ")
#'           logger::log_info("  Sample BMI range for participant {sample_participant}: {bmi_range}")
#'         }
#'       }
#'     }
#'   }
#'
#'   return(incontinence_long_format)
#' }
#'
#' #' Identify visit-specific demographic variables in dataset
#' #' @noRd
#' identify_visit_specific_demographic_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'   visit_specific_vars <- list()
#'
#'   # AGE variables - match only single digit visit numbers (AGE0, AGE1, AGE2, ..., AGE9)
#'   # Also match AGE10 specifically, but exclude AGE11, AGE21, etc.
#'   age_variables <- variable_names[stringr::str_detect(variable_names, "^AGE(\\d|10)$")]
#'   if (length(age_variables) > 0) {
#'     visit_specific_vars[["age_years"]] <- age_variables
#'   }
#'
#'   # BMI variables - match only single digit visit numbers (BMI0, BMI1, BMI2, ..., BMI9, BMI10)
#'   bmi_variables <- variable_names[stringr::str_detect(variable_names, "^BMI(\\d|10)$")]
#'   if (length(bmi_variables) > 0) {
#'     visit_specific_vars[["bmi_kg_m2"]] <- bmi_variables
#'   }
#'
#'   # Weight variables - match only single digit visit numbers
#'   weight_variables <- variable_names[stringr::str_detect(variable_names, "^WEIGHT(\\d|10)$")]
#'   if (length(weight_variables) > 0) {
#'     visit_specific_vars[["weight_kg"]] <- weight_variables
#'   }
#'
#'   # Height variables - match only single digit visit numbers
#'   height_variables <- variable_names[stringr::str_detect(variable_names, "^HEIGHT(\\d|10)$")]
#'   if (length(height_variables) > 0) {
#'     visit_specific_vars[["height_cm"]] <- height_variables
#'   }
#'
#'   # Smoking variables - match only single digit visit numbers
#'   smoking_variables <- variable_names[stringr::str_detect(variable_names, "^SMOKER?E?(\\d|10)$")]
#'   if (length(smoking_variables) > 0) {
#'     visit_specific_vars[["smoking_status"]] <- smoking_variables
#'   }
#'
#'   # Menopausal status variables - match only single digit visit numbers
#'   menopause_variables <- variable_names[stringr::str_detect(variable_names, "^STATUS(\\d|10)$")]
#'   if (length(menopause_variables) > 0) {
#'     visit_specific_vars[["menopausal_stage"]] <- menopause_variables
#'   }
#'
#'   # Marital status variables - match only single digit visit numbers
#'   marital_variables <- variable_names[stringr::str_detect(variable_names, "^MARITALGP(\\d|10)$")]
#'   if (length(marital_variables) > 0) {
#'     visit_specific_vars[["marital_status"]] <- marital_variables
#'   }
#'
#'   # Parity variables - match only single digit visit numbers
#'   parity_variables <- variable_names[stringr::str_detect(variable_names, "^NUMCHILD(\\d|10)$")]
#'   if (length(parity_variables) > 0) {
#'     visit_specific_vars[["parity_count"]] <- parity_variables
#'   }
#'
#'   return(visit_specific_vars)
#' }
#'
#' #' Extract visit number from variable name (e.g., INVOLEA9 -> 9, AGE1 -> 1)
#' #' @noRd
#' extract_visit_number_from_variable <- function(variable_names) {
#'
#'   # For SWAN variables, extract the visit number more carefully
#'   visit_numbers <- character(length(variable_names))
#'
#'   for (i in seq_along(variable_names)) {
#'     var_name <- variable_names[i]
#'
#'     # For incontinence variables (INVOLEA0, LEKINVO7, etc.)
#'     if (stringr::str_detect(var_name, "^(INVOLEA|LEKINVO)")) {
#'       visit_num <- stringr::str_extract(var_name, "\\d+$")
#'       visit_numbers[i] <- visit_num
#'     }
#'     # For demographic variables (AGE1, BMI1, STATUS1, etc.)
#'     else if (stringr::str_detect(var_name, "^(AGE|BMI|WEIGHT|HEIGHT|STATUS|SMOKER|MARITALGP|NUMCHILD)")) {
#'       # Extract only the last number, and only if it's 0-10 (valid visit numbers)
#'       potential_visit <- stringr::str_extract(var_name, "(\\d|10)$")
#'       if (!is.na(potential_visit) && as.numeric(potential_visit) <= 10) {
#'         visit_numbers[i] <- potential_visit
#'       } else {
#'         visit_numbers[i] <- NA_character_
#'       }
#'     }
#'     else {
#'       # For other variables, extract trailing digits
#'       visit_num <- stringr::str_extract(var_name, "\\d+$")
#'       visit_numbers[i] <- visit_num
#'     }
#'   }
#'
#'   # Convert to numeric
#'   visit_numbers_numeric <- as.numeric(visit_numbers)
#'
#'   # Replace any NAs with 0 (baseline)
#'   visit_numbers_numeric[is.na(visit_numbers_numeric)] <- 0
#'
#'   return(visit_numbers_numeric)
#' }
#'
#' #' Standardize incontinence response values
#' #' @noRd
#' standardize_incontinence_response <- function(response_values) {
#'
#'   dplyr::case_when(
#'     # Standard Yes responses
#'     response_values %in% c("(2) Yes", "2", 2, "Yes", "YES", "yes") ~ 1,
#'     # Standard No responses
#'     response_values %in% c("(1) No", "1", 1, "No", "NO", "no") ~ 0,
#'     # Extended formats from later visits
#'     response_values %in% c("(2) 2: Yes", "(2)2: Yes", "2: Yes") ~ 1,
#'     response_values %in% c("(1) 1: No", "(1)1: No", "1: No") ~ 0,
#'     TRUE ~ NA_real_
#'   )
#' }
#'
#' #' Helper function to identify participant ID column
#' #' @noRd
#' identify_participant_id_variable <- function(dataset) {
#'   participant_id_patterns <- c("^SWANID$", "^ARCHID$", "^swanid$", "^archid$")
#'   for (pattern in participant_id_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' Helper function to identify visit number column
#' #' @noRd
#' identify_visit_number_variable <- function(dataset) {
#'   visit_patterns <- c("^VISIT$", "^visit$", "^Visit$", "^VISITNO$")
#'   for (pattern in visit_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' Safe numeric conversion
#' #' @noRd
#' convert_to_numeric_safely <- function(variable_values) {
#'   suppressWarnings(as.numeric(as.character(variable_values)))
#' }
#'
#' #' Standardize race/ethnicity variable
#' #' @noRd
#' standardize_race_ethnicity_variable <- function(race_values) {
#'   dplyr::case_when(
#'     stringr::str_detect(as.character(race_values), "White|Caucasian|4") ~ "White",
#'     stringr::str_detect(as.character(race_values), "Black|African|1") ~ "Black",
#'     stringr::str_detect(as.character(race_values), "Hispanic|5") ~ "Hispanic",
#'     stringr::str_detect(as.character(race_values), "Chinese|2") ~ "Chinese",
#'     stringr::str_detect(as.character(race_values), "Japanese|3") ~ "Japanese",
#'     TRUE ~ "Other"
#'   )
#' }
#'
#' #' Standardize smoking status variable
#' #' @noRd
#' standardize_smoking_status_variable <- function(smoking_values) {
#'   dplyr::case_when(
#'     smoking_values %in% c(1, "(1) Never smoked", "Never") ~ "Never",
#'     smoking_values %in% c(2, "(2) Past smoker", "Past") ~ "Past",
#'     smoking_values %in% c(3, "(3) Current smoker", "Current") ~ "Current",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Standardize menopausal status variable
#' #' @noRd
#' standardize_menopausal_status_variable <- function(status_values) {
#'   dplyr::case_when(
#'     status_values %in% c(1, "(1) Post by BSO") ~ "Post BSO",
#'     status_values %in% c(2, "(2) Natural Post", "(2) Postmenopausal") ~ "Natural Post",
#'     status_values %in% c(3, "(3) Late Peri", "(3) Late perimenopausal") ~ "Late Peri",
#'     status_values %in% c(4, "(4) Early Peri", "(4) Early perimenopausal") ~ "Early Peri",
#'     status_values %in% c(5, "(5) Pre", "(5) Premenopausal") ~ "Pre",
#'     status_values %in% c(6, "(6) Pregnant/breastfeeding") ~ "Pregnant/Breastfeeding",
#'     status_values %in% c(7, "(7) Unknown due to HT use") ~ "Unknown HT",
#'     status_values %in% c(8, "(8) Unknown due to hysterectomy") ~ "Unknown Hysterectomy",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Standardize education variable
#' #' @noRd
#' standardize_education_variable <- function(education_values) {
#'   dplyr::case_when(
#'     education_values %in% c(1, "(1) Less than high school") ~ "Less than HS",
#'     education_values %in% c(2, "(2) High school") ~ "High School",
#'     education_values %in% c(3, "(3) Some college") ~ "Some College",
#'     education_values %in% c(4, "(4) College graduate") ~ "College Graduate",
#'     education_values %in% c(5, "(5) Graduate school") ~ "Graduate School",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Standardize marital status variable
#' #' @noRd
#' standardize_marital_status_variable <- function(marital_values) {
#'   dplyr::case_when(
#'     marital_values %in% c(1, "(1) Married", "Married") ~ "Married",
#'     marital_values %in% c(2, "(2) Single", "Single") ~ "Single",
#'     marital_values %in% c(3, "(3) Divorced/Separated", "Divorced") ~ "Divorced/Separated",
#'     marital_values %in% c(4, "(4) Widowed", "Widowed") ~ "Widowed",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Combine multiple baseline sources with type-safe merging
#' #' @noRd
#' combine_multiple_baseline_sources <- function(baseline_dataset_list, verbose) {
#'
#'   if (length(baseline_dataset_list) == 1) {
#'     return(baseline_dataset_list[[1]])
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Combining {length(baseline_dataset_list)} baseline sources")
#'   }
#'
#'   # Find the most complete dataset (most non-missing participant IDs)
#'   dataset_completeness_scores <- purrr::map_dbl(baseline_dataset_list, function(baseline_dataset) {
#'     sum(!is.na(baseline_dataset$swan_participant_id))
#'   })
#'
#'   primary_baseline_dataset <- baseline_dataset_list[[which.max(dataset_completeness_scores)]]
#'
#'   if (verbose) {
#'     logger::log_info("Primary baseline dataset: {primary_baseline_dataset$source_file[1]} with {max(dataset_completeness_scores)} participants")
#'   }
#'
#'   # Merge additional datasets
#'   for (dataset_index in seq_along(baseline_dataset_list)) {
#'     if (dataset_index == which.max(dataset_completeness_scores)) next
#'
#'     secondary_baseline_dataset <- baseline_dataset_list[[dataset_index]]
#'
#'     if (verbose) {
#'       logger::log_info("Merging secondary baseline: {secondary_baseline_dataset$source_file[1]}")
#'     }
#'
#'     # Harmonize data types before merging
#'     secondary_baseline_dataset <- harmonize_baseline_data_types(
#'       secondary_baseline_dataset,
#'       primary_baseline_dataset,
#'       verbose
#'     )
#'
#'     primary_baseline_dataset <- primary_baseline_dataset %>%
#'       dplyr::full_join(
#'         secondary_baseline_dataset,
#'         by = "swan_participant_id",
#'         suffix = c("", "_secondary")
#'       )
#'
#'     # Smart coalescing - only coalesce variables that exist in both datasets
#'     baseline_variables_to_merge <- c(
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_parity_count",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_menopausal_stage",
#'       "baseline_education_level",
#'       "baseline_marital_status"
#'     )
#'
#'     # Check which variables actually exist with _secondary suffix
#'     available_columns <- names(primary_baseline_dataset)
#'
#'     for (baseline_variable in baseline_variables_to_merge) {
#'       secondary_variable_name <- paste0(baseline_variable, "_secondary")
#'
#'       if (baseline_variable %in% available_columns && secondary_variable_name %in% available_columns) {
#'         if (verbose) {
#'           logger::log_info("  Coalescing {baseline_variable}")
#'         }
#'
#'         # Apply appropriate coalescing based on data type
#'         if (stringr::str_detect(baseline_variable, "age|bmi|parity|count")) {
#'           # Numeric variables
#'           primary_baseline_dataset[[baseline_variable]] <- apply_safe_numeric_coalesce(
#'             primary_baseline_dataset[[baseline_variable]],
#'             primary_baseline_dataset[[secondary_variable_name]]
#'           )
#'         } else {
#'           # Character variables
#'           primary_baseline_dataset[[baseline_variable]] <- apply_safe_character_coalesce(
#'             primary_baseline_dataset[[baseline_variable]],
#'             primary_baseline_dataset[[secondary_variable_name]]
#'           )
#'         }
#'       } else if (verbose) {
#'         logger::log_info("  Skipping {baseline_variable} - not available in secondary dataset")
#'       }
#'     }
#'
#'     # Remove all secondary columns
#'     primary_baseline_dataset <- primary_baseline_dataset %>%
#'       dplyr::select(-dplyr::ends_with("_secondary"))
#'   }
#'
#'   if (verbose) {
#'     final_participant_count <- sum(!is.na(primary_baseline_dataset$swan_participant_id))
#'     logger::log_info("Combined baseline dataset: {final_participant_count} total participants")
#'   }
#'
#'   return(primary_baseline_dataset)
#' }
#'
#' #' Harmonize data types between baseline datasets
#' #' @noRd
#' harmonize_baseline_data_types <- function(secondary_dataset, primary_dataset, verbose) {
#'
#'   baseline_column_names <- names(secondary_dataset)[stringr::str_starts(names(secondary_dataset), "baseline_")]
#'
#'   for (column_name in baseline_column_names) {
#'     if (column_name %in% names(primary_dataset)) {
#'
#'       primary_data_type <- class(primary_dataset[[column_name]])[1]
#'       secondary_data_type <- class(secondary_dataset[[column_name]])[1]
#'
#'       if (primary_data_type != secondary_data_type) {
#'         if (verbose) {
#'           logger::log_info("Harmonizing {column_name}: {secondary_data_type} -> {primary_data_type}")
#'         }
#'
#'         # Convert secondary to match primary type
#'         if (primary_data_type %in% c("numeric", "double")) {
#'           secondary_dataset[[column_name]] <- convert_to_numeric_safely(secondary_dataset[[column_name]])
#'         } else if (primary_data_type == "character") {
#'           secondary_dataset[[column_name]] <- as.character(secondary_dataset[[column_name]])
#'         } else if (primary_data_type == "logical") {
#'           secondary_dataset[[column_name]] <- as.logical(secondary_dataset[[column_name]])
#'         }
#'       }
#'     }
#'   }
#'
#'   return(secondary_dataset)
#' }
#'
#' #' Type-safe numeric coalesce
#' #' @noRd
#' apply_safe_numeric_coalesce <- function(primary_values, secondary_values) {
#'   # Convert both to numeric if needed
#'   primary_numeric <- if (is.numeric(primary_values)) primary_values else convert_to_numeric_safely(primary_values)
#'   secondary_numeric <- if (is.numeric(secondary_values)) secondary_values else convert_to_numeric_safely(secondary_values)
#'   dplyr::coalesce(primary_numeric, secondary_numeric)
#' }
#'
#' #' Type-safe character coalesce
#' #' @noRd
#' apply_safe_character_coalesce <- function(primary_values, secondary_values) {
#'   # Convert both to character if needed
#'   primary_character <- if (is.character(primary_values)) primary_values else as.character(primary_values)
#'   secondary_character <- if (is.character(secondary_values)) secondary_values else as.character(secondary_values)
#'   dplyr::coalesce(primary_character, secondary_character)
#' }
#'
#' #' Merge baseline demographics with longitudinal data
#' #' @noRd
#' merge_baseline_with_longitudinal_data <- function(baseline_data, longitudinal_data, verbose) {
#'
#'   if (verbose) {
#'     baseline_participant_count <- length(unique(baseline_data$swan_participant_id))
#'     longitudinal_observation_count <- nrow(longitudinal_data)
#'     logger::log_info("Merging baseline demographics with longitudinal measurements")
#'     logger::log_info("Baseline participants: {baseline_participant_count}")
#'     logger::log_info("Longitudinal observations: {longitudinal_observation_count}")
#'   }
#'
#'   # Merge datasets
#'   merged_comprehensive_data <- longitudinal_data %>%
#'     dplyr::left_join(
#'       baseline_data %>% dplyr::select(-source_file),
#'       by = "swan_participant_id"
#'     )
#'
#'   # Add derived time variables
#'   merged_comprehensive_data <- merged_comprehensive_data %>%
#'     dplyr::mutate(
#'       # Calculate current age at each visit
#'       current_age_years = baseline_age_years + years_since_baseline,
#'       # Add visit timing categories
#'       visit_timing_category = dplyr::case_when(
#'         visit_number == 0 ~ "Baseline",
#'         visit_number %in% 1:3 ~ "Early Follow-up",
#'         visit_number %in% 4:7 ~ "Mid Follow-up",
#'         visit_number >= 8 ~ "Late Follow-up",
#'         TRUE ~ "Unknown"
#'       )
#'     ) %>%
#'     dplyr::arrange(swan_participant_id, visit_number)
#'
#'   if (verbose) {
#'     merged_observation_count <- nrow(merged_comprehensive_data)
#'     participants_with_both_data <- length(unique(merged_comprehensive_data$swan_participant_id[!is.na(merged_comprehensive_data$baseline_age_years)]))
#'     logger::log_info("Merged comprehensive dataset: {merged_observation_count} observations")
#'     logger::log_info("Participants with both baseline + longitudinal data: {participants_with_both_data}")
#'   }
#'
#'   return(merged_comprehensive_data)
#' }
#'
#' #' Apply filtering for valid responses
#' #' @noRd
#' apply_response_filtering <- function(merged_data, verbose) {
#'
#'   if (verbose) {
#'     original_observation_count <- nrow(merged_data)
#'     logger::log_info("Applying response filtering to remove invalid/missing incontinence responses")
#'   }
#'
#'   filtered_data <- merged_data %>%
#'     dplyr::filter(
#'       !is.na(incontinence_status),
#'       !is.na(swan_participant_id),
#'       !is.na(visit_number)
#'     )
#'
#'   if (verbose) {
#'     filtered_observation_count <- nrow(filtered_data)
#'     observations_removed <- original_observation_count - filtered_observation_count
#'     logger::log_info("Filtering completed: {filtered_observation_count} observations retained")
#'     logger::log_info("Observations removed: {observations_removed}")
#'   }
#'
#'   return(filtered_data)
#' }
#'
#' #' Format comprehensive output dataset
#' #' @noRd
#' format_comprehensive_output <- function(merged_data, format, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Formatting comprehensive output as: {format}")
#'     all_column_names <- paste(names(merged_data), collapse = ", ")
#'     logger::log_info("Available columns: {all_column_names}")
#'   }
#'
#'   if (format == "long") {
#'     # Identify visit-specific columns
#'     visit_specific_column_names <- names(merged_data)[stringr::str_starts(names(merged_data), "visit_")]
#'
#'     if (verbose && length(visit_specific_column_names) > 0) {
#'       visit_vars_string <- paste(visit_specific_column_names, collapse = ", ")
#'       logger::log_info("Visit-specific columns preserved: {visit_vars_string}")
#'     } else if (verbose) {
#'       logger::log_warn("No visit-specific columns found in final dataset")
#'     }
#'
#'     # Build comprehensive column selection
#'     baseline_demographic_columns <- c(
#'       "swan_participant_id",
#'       "visit_number",
#'       "years_since_baseline",
#'       "current_age_years",
#'       "visit_timing_category",
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_parity_count",
#'       "baseline_menopausal_stage",
#'       "baseline_education_level",
#'       "baseline_marital_status"
#'     )
#'
#'     incontinence_measurement_columns <- c(
#'       "incontinence_status",
#'       "incontinence_raw_response",
#'       "incontinence_source_variable"
#'     )
#'
#'     # Combine all columns, keeping only those that exist
#'     all_desired_columns <- c(
#'       baseline_demographic_columns,
#'       incontinence_measurement_columns,
#'       visit_specific_column_names
#'     )
#'
#'     columns_that_exist_in_data <- intersect(all_desired_columns, names(merged_data))
#'
#'     if (verbose) {
#'       logger::log_info("Selecting {length(columns_that_exist_in_data)} columns for long format output")
#'     }
#'
#'     # Create final long format dataset
#'     final_comprehensive_dataset <- merged_data %>%
#'       dplyr::select(dplyr::all_of(columns_that_exist_in_data)) %>%
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         incontinence_source_variable,
#'         .keep_all = TRUE
#'       ) %>%
#'       dplyr::arrange(swan_participant_id, visit_number)
#'
#'   } else {
#'     # Wide format conversion
#'     visit_specific_column_names <- names(merged_data)[stringr::str_starts(names(merged_data), "visit_")]
#'
#'     baseline_columns_for_wide <- c(
#'       "swan_participant_id",
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_parity_count",
#'       "baseline_menopausal_stage",
#'       "baseline_education_level",
#'       "baseline_marital_status"
#'     )
#'
#'     measurement_columns_for_wide <- c(
#'       "visit_number",
#'       "incontinence_status",
#'       "current_age_years"
#'     )
#'
#'     all_columns_for_wide <- c(
#'       baseline_columns_for_wide,
#'       measurement_columns_for_wide,
#'       visit_specific_column_names
#'     )
#'
#'     columns_that_exist_for_wide <- intersect(all_columns_for_wide, names(merged_data))
#'
#'     if (verbose) {
#'       logger::log_info("Converting to wide format with {length(columns_that_exist_for_wide)} base columns")
#'     }
#'
#'     final_comprehensive_dataset <- merged_data %>%
#'       dplyr::select(dplyr::all_of(columns_that_exist_for_wide)) %>%
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         .keep_all = TRUE
#'       ) %>%
#'       tidyr::pivot_wider(
#'         names_from = visit_number,
#'         values_from = c(
#'           dplyr::any_of(visit_specific_column_names),
#'           incontinence_status,
#'           current_age_years
#'         ),
#'         names_sep = "_visit_"
#'       )
#'   }
#'
#'   if (verbose) {
#'     final_row_count <- nrow(final_comprehensive_dataset)
#'     final_column_count <- ncol(final_comprehensive_dataset)
#'     unique_participant_count <- length(unique(final_comprehensive_dataset$swan_participant_id))
#'
#'     logger::log_info("Final comprehensive dataset formatting completed")
#'     logger::log_info("Dimensions: {final_row_count} rows × {final_column_count} columns")
#'     logger::log_info("Unique participants: {unique_participant_count}")
#'
#'     if (format == "long") {
#'       final_visit_specific_count <- sum(stringr::str_starts(names(final_comprehensive_dataset), "visit_"))
#'       visit_range_min <- min(final_comprehensive_dataset$visit_number, na.rm = TRUE)
#'       visit_range_max <- max(final_comprehensive_dataset$visit_number, na.rm = TRUE)
#'       logger::log_info("Visit-specific variables in final dataset: {final_visit_specific_count}")
#'       logger::log_info("Visit range: {visit_range_min} to {visit_range_max}")
#'     }
#'   }
#'
#'   return(final_comprehensive_dataset)
#' }
#'
#' # Run ----
#'
#'
#' # Version 16 Create Comprehensive SWAN Dataset with Visit-Specific Variables ----
#' #' Create Comprehensive SWAN Dataset with Visit-Specific Variables
#' #'
#' #' This function creates a comprehensive dataset from SWAN study files, extracting
#' #' baseline demographics and visit-specific variables including urinary incontinence
#' #' measures (INVOLEA/LEKINVO) and other longitudinal health indicators.
#' #'
#' #' @param swan_data_directory Character. Path to directory containing SWAN .rda files
#' #' @param output_format Character. Either "long" or "wide" format for final dataset
#' #' @param verbose Logical. Enable detailed logging output to console
#' #' @param include_visit_demographics Logical. Include visit-specific demographic
#' #'   variables (AGE, BMI, STATUS, etc.) in addition to baseline
#' #' @param filter_valid_responses Logical. Remove rows with missing INVOLEA responses
#' #'
#' #' @return tibble. Comprehensive SWAN dataset with baseline and visit-specific variables
#' #'
#' #' @examples
#' #' # Example 1: Basic long format with visit demographics
#' #' swan_long_comprehensive <- create_comprehensive_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "long",
#' #'   verbose = TRUE,
#' #'   include_visit_demographics = TRUE,
#' #'   filter_valid_responses = TRUE
#' #' )
#' #'
#' #' # Example 2: Wide format for analysis, no filtering
#' #' swan_wide_complete <- create_comprehensive_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "wide",
#' #'   verbose = FALSE,
#' #'   include_visit_demographics = TRUE,
#' #'   filter_valid_responses = FALSE
#' #' )
#' #'
#' #' # Example 3: Minimal dataset with only baseline + INVOLEA
#' #' swan_minimal <- create_comprehensive_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "long",
#' #'   verbose = TRUE,
#' #'   include_visit_demographics = FALSE,
#' #'   filter_valid_responses = TRUE
#' #' )
#' #'
#' #' @importFrom dplyr filter select mutate arrange group_by summarise
#' #'   left_join full_join distinct coalesce case_when all_of any_of ends_with
#' #' @importFrom tidyr pivot_longer pivot_wider
#' #' @importFrom stringr str_detect str_extract str_replace_all str_starts
#' #'   str_remove str_match
#' #' @importFrom purrr map_dfr map_chr map_lgl keep walk compact map
#' #' @importFrom assertthat assert_that
#' #' @importFrom logger log_info log_warn log_error
#' #' @importFrom tibble tibble as_tibble
#' #' @importFrom readr parse_number
#' #' @importFrom glue glue
#' #'
#' #' @export
#' create_comprehensive_swan_dataset <- function(swan_data_directory,
#'                                               output_format = "long",
#'                                               verbose = TRUE,
#'                                               include_visit_demographics = TRUE,
#'                                               filter_valid_responses = TRUE) {
#'
#'   # Input validation with assertthat
#'   if (verbose) {
#'     logger::log_info("Creating comprehensive SWAN dataset with visit-specific variables")
#'     logger::log_info("Function inputs - directory: {swan_data_directory}")
#'     logger::log_info("Function inputs - format: {output_format}, verbose: {verbose}")
#'     logger::log_info("Function inputs - include_visit_demographics: {include_visit_demographics}")
#'     logger::log_info("Function inputs - filter_valid_responses: {filter_valid_responses}")
#'   }
#'
#'   assertthat::assert_that(
#'     is.character(swan_data_directory),
#'     msg = "swan_data_directory must be a character string path"
#'   )
#'   assertthat::assert_that(
#'     dir.exists(swan_data_directory),
#'     msg = glue::glue("Directory does not exist: {swan_data_directory}")
#'   )
#'   assertthat::assert_that(
#'     output_format %in% c("long", "wide"),
#'     msg = "output_format must be 'long' or 'wide'"
#'   )
#'   assertthat::assert_that(
#'     is.logical(verbose),
#'     msg = "verbose must be TRUE or FALSE"
#'   )
#'   assertthat::assert_that(
#'     is.logical(include_visit_demographics),
#'     msg = "include_visit_demographics must be TRUE or FALSE"
#'   )
#'   assertthat::assert_that(
#'     is.logical(filter_valid_responses),
#'     msg = "filter_valid_responses must be TRUE or FALSE"
#'   )
#'
#'   # Step 1: Extract baseline demographics
#'   baseline_participant_demographics <- extract_baseline_participant_demographics(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Baseline demographics extracted: {nrow(baseline_participant_demographics)} participants")
#'   }
#'
#'   # Step 2: Extract comprehensive longitudinal data
#'   longitudinal_health_measurements <- extract_comprehensive_longitudinal_data(
#'     directory_path = swan_data_directory,
#'     include_visit_demographics = include_visit_demographics,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Longitudinal data extracted: {nrow(longitudinal_health_measurements)} observations")
#'   }
#'
#'   # Step 3: Merge datasets
#'   merged_comprehensive_dataset <- merge_baseline_with_longitudinal_data(
#'     baseline_data = baseline_participant_demographics,
#'     longitudinal_data = longitudinal_health_measurements,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Data merged: {nrow(merged_comprehensive_dataset)} total observations")
#'   }
#'
#'   # Step 4: Apply filtering if requested
#'   if (filter_valid_responses) {
#'     filtered_comprehensive_dataset <- apply_response_filtering(
#'       merged_data = merged_comprehensive_dataset,
#'       verbose = verbose
#'     )
#'   } else {
#'     filtered_comprehensive_dataset <- merged_comprehensive_dataset
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("After filtering: {nrow(filtered_comprehensive_dataset)} observations")
#'   }
#'
#'   # Step 5: Format final output
#'   final_comprehensive_dataset <- format_comprehensive_output(
#'     merged_data = filtered_comprehensive_dataset,
#'     format = output_format,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Comprehensive SWAN dataset creation completed")
#'     logger::log_info("Final dataset dimensions: {nrow(final_comprehensive_dataset)} rows × {ncol(final_comprehensive_dataset)} columns")
#'     logger::log_info("Output file path: Not saved to file (returned as object)")
#'   }
#'
#'   return(final_comprehensive_dataset)
#' }
#'
#' #' Extract baseline demographics from SWAN screener/baseline files
#' #' @noRd
#' extract_baseline_participant_demographics <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting baseline participant demographics from SWAN files")
#'   }
#'
#'   # Find baseline/screener files using known ICPSR numbers
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   # Known baseline file patterns from SWAN documentation
#'   baseline_file_identifiers <- c("28762", "04368", "baseline", "screener")
#'
#'   baseline_candidate_files <- purrr::keep(all_rda_files, function(file_path) {
#'     file_name <- tolower(basename(file_path))
#'     any(stringr::str_detect(file_name, baseline_file_identifiers))
#'   })
#'
#'   if (verbose) {
#'     logger::log_info("Found {length(baseline_candidate_files)} potential baseline files")
#'     purrr::walk(baseline_candidate_files, ~logger::log_info("  Candidate file: {basename(.x)}"))
#'   }
#'
#'   # Process each baseline file
#'   baseline_dataset_list <- purrr::map(baseline_candidate_files, function(file_path) {
#'     process_single_baseline_file(file_path, verbose)
#'   })
#'
#'   # Remove NULL results and combine
#'   baseline_dataset_list <- purrr::compact(baseline_dataset_list)
#'
#'   if (length(baseline_dataset_list) == 0) {
#'     logger::log_error("No valid baseline demographic data found in any files")
#'     stop("No baseline demographic data could be extracted")
#'   }
#'
#'   # Combine multiple baseline sources
#'   combined_baseline_demographics <- combine_multiple_baseline_sources(
#'     baseline_dataset_list,
#'     verbose
#'   )
#'
#'   return(combined_baseline_demographics)
#' }
#'
#' #' Process a single baseline file to extract demographics
#' #' @noRd
#' process_single_baseline_file <- function(file_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Processing baseline file: {basename(file_path)}")
#'   }
#'
#'   # Load the file safely
#'   file_environment <- new.env()
#'
#'   tryCatch({
#'     load(file_path, envir = file_environment)
#'   }, error = function(e) {
#'     if (verbose) {
#'       logger::log_error("Failed to load {basename(file_path)}: {e$message}")
#'     }
#'     return(NULL)
#'   })
#'
#'   if (length(ls(file_environment)) == 0) {
#'     if (verbose) {
#'       logger::log_warn("No datasets found in {basename(file_path)}")
#'     }
#'     return(NULL)
#'   }
#'
#'   raw_baseline_dataset <- file_environment[[ls(file_environment)[1]]]
#'
#'   # Identify demographic variables
#'   demographic_variable_mapping <- identify_baseline_demographic_variables(
#'     raw_baseline_dataset
#'   )
#'
#'   if (is.null(demographic_variable_mapping$participant_id)) {
#'     if (verbose) {
#'       logger::log_warn("No participant ID found in {basename(file_path)}")
#'     }
#'     return(NULL)
#'   }
#'
#'   # Extract and standardize demographics
#'   standardized_baseline_demographics <- extract_standardized_demographics(
#'     dataset = raw_baseline_dataset,
#'     variable_mapping = demographic_variable_mapping,
#'     source_file = basename(file_path),
#'     verbose = verbose
#'   )
#'
#'   return(standardized_baseline_demographics)
#' }
#'
#' #' Identify demographic variables in baseline dataset
#' #' @noRd
#' identify_baseline_demographic_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Participant ID patterns
#'   participant_id_patterns <- c("^SWANID$", "^ARCHID$", "^swanid$", "^archid$")
#'   participant_id_variable <- find_matching_variable(variable_names, participant_id_patterns)
#'
#'   # Demographic variable patterns - updated based on actual data exploration
#'   age_patterns <- c("^AGE0?$", "^age0?$", "AGE.*0$")
#'   bmi_patterns <- c("^BMI0?$", "^bmi0?$", "BMI.*0$")
#'   race_patterns <- c("^RACE$", "^race$", "^ETHNIC")
#'
#'   # Smoking patterns based on actual SWAN variables with data
#'   # Prioritize variables with the most non-missing values
#'   smoking_patterns <- c(
#'     "^SMOKED$",      # Screener: 15,998 non-missing values (best)
#'     "^SMOKER$",      # Screener: 15,994 non-missing values
#'     "^SMOKERE0$",    # Baseline: 3,273 non-missing values
#'     "^SMOKE_R$",     # Screener: 7,610 non-missing values
#'     "^SMOKENO0$",    # Baseline: 1,394 non-missing values
#'     "^SMOKEYR0$"     # Baseline: 525 non-missing values
#'   )
#'
#'   # Parity patterns based on actual SWAN variables with data
#'   # Both NUMCHILD and CHILDREN have identical counts, prefer NUMCHILD
#'   parity_patterns <- c(
#'     "^NUMCHILD$",    # Screener: 16,115 non-missing values (best)
#'     "^CHILDREN$",    # Screener: 16,115 non-missing values (identical)
#'     "^CHILDMO0$",    # Baseline: 3,283 non-missing values (not parity, but backup)
#'     "^CHILDIL0$"     # Baseline: 3,232 non-missing values (not parity, but backup)
#'   )
#'
#'   # Menopausal status patterns
#'   menopausal_patterns <- c("^STATUS0?$", "^MENO", "^MEN_FLAG$")
#'
#'   education_patterns <- c("^DEGREE$", "^EDUCATION", "^EDUC")
#'   marital_patterns <- c("^MARITAL", "^MARITALGP")
#'
#'   return(list(
#'     participant_id = participant_id_variable,
#'     age = find_matching_variable(variable_names, age_patterns),
#'     bmi = find_matching_variable(variable_names, bmi_patterns),
#'     race = find_matching_variable(variable_names, race_patterns),
#'     smoking = find_matching_variable(variable_names, smoking_patterns),
#'     parity = find_matching_variable(variable_names, parity_patterns),
#'     menopausal = find_matching_variable(variable_names, menopausal_patterns),
#'     education = find_matching_variable(variable_names, education_patterns),
#'     marital = find_matching_variable(variable_names, marital_patterns)
#'   ))
#' }
#'
#' #' Find first matching variable from patterns
#' #' @noRd
#' find_matching_variable <- function(variable_names, patterns) {
#'   for (pattern in patterns) {
#'     matches <- variable_names[stringr::str_detect(variable_names, pattern)]
#'     if (length(matches) > 0) {
#'       return(matches[1])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' Extract and standardize demographic variables
#' #' @noRd
#' extract_standardized_demographics <- function(dataset, variable_mapping,
#'                                               source_file, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("  Variable mapping for {source_file}:")
#'     for (var_type in names(variable_mapping)) {
#'       var_name <- variable_mapping[[var_type]]
#'       if (!is.null(var_name)) {
#'         logger::log_info("    {var_type}: {var_name}")
#'       } else {
#'         logger::log_info("    {var_type}: NOT FOUND")
#'       }
#'     }
#'   }
#'
#'   # Start with participant ID
#'   standardized_demographics <- tibble::tibble(
#'     swan_participant_id = as.character(dataset[[variable_mapping$participant_id]])
#'   )
#'
#'   # Add demographic variables with type-safe conversion
#'   if (!is.null(variable_mapping$age)) {
#'     standardized_demographics$baseline_age_years <-
#'       convert_to_numeric_safely(dataset[[variable_mapping$age]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_age_years))
#'       logger::log_info("    Age variable {variable_mapping$age}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   if (!is.null(variable_mapping$bmi)) {
#'     standardized_demographics$baseline_bmi_kg_m2 <-
#'       convert_to_numeric_safely(dataset[[variable_mapping$bmi]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_bmi_kg_m2))
#'       logger::log_info("    BMI variable {variable_mapping$bmi}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   if (!is.null(variable_mapping$race)) {
#'     standardized_demographics$baseline_race_ethnicity <-
#'       standardize_race_ethnicity_variable(dataset[[variable_mapping$race]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_race_ethnicity))
#'       logger::log_info("    Race variable {variable_mapping$race}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   if (!is.null(variable_mapping$smoking)) {
#'     standardized_demographics$baseline_smoking_status <-
#'       standardize_smoking_status_variable(dataset[[variable_mapping$smoking]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_smoking_status))
#'       logger::log_info("    Smoking variable {variable_mapping$smoking}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   if (!is.null(variable_mapping$parity)) {
#'     standardized_demographics$baseline_parity_count <-
#'       convert_to_numeric_safely(dataset[[variable_mapping$parity]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_parity_count))
#'       logger::log_info("    Parity variable {variable_mapping$parity}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   if (!is.null(variable_mapping$menopausal)) {
#'     standardized_demographics$baseline_menopausal_stage <-
#'       standardize_menopausal_status_variable(dataset[[variable_mapping$menopausal]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_menopausal_stage))
#'       logger::log_info("    Menopausal variable {variable_mapping$menopausal}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   if (!is.null(variable_mapping$education)) {
#'     standardized_demographics$baseline_education_level <-
#'       standardize_education_variable(dataset[[variable_mapping$education]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_education_level))
#'       logger::log_info("    Education variable {variable_mapping$education}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   if (!is.null(variable_mapping$marital)) {
#'     standardized_demographics$baseline_marital_status <-
#'       standardize_marital_status_variable(dataset[[variable_mapping$marital]])
#'
#'     if (verbose) {
#'       non_na_count <- sum(!is.na(standardized_demographics$baseline_marital_status))
#'       logger::log_info("    Marital variable {variable_mapping$marital}: {non_na_count} non-missing values")
#'     }
#'   }
#'
#'   standardized_demographics$source_file <- source_file
#'
#'   if (verbose) {
#'     valid_participant_count <- sum(!is.na(standardized_demographics$swan_participant_id))
#'     demographic_variable_count <- ncol(standardized_demographics) - 2  # exclude ID and source
#'     logger::log_info("  Extracted {valid_participant_count} participants with {demographic_variable_count} demographic variables")
#'   }
#'
#'   return(standardized_demographics)
#' }
#'
#' #' Extract comprehensive longitudinal data including visit-specific variables
#' #' @noRd
#' extract_comprehensive_longitudinal_data <- function(directory_path,
#'                                                     include_visit_demographics,
#'                                                     verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting comprehensive longitudinal data from all SWAN files")
#'   }
#'
#'   # Find all RDA files
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   # Process each file for longitudinal variables
#'   longitudinal_dataset_list <- purrr::map(all_rda_files, function(file_path) {
#'     process_single_longitudinal_file(
#'       file_path = file_path,
#'       include_visit_demographics = include_visit_demographics,
#'       verbose = verbose
#'     )
#'   })
#'
#'   # Remove NULL results
#'   longitudinal_dataset_list <- purrr::compact(longitudinal_dataset_list)
#'
#'   if (length(longitudinal_dataset_list) == 0) {
#'     logger::log_error("No longitudinal data found in any files")
#'     stop("No longitudinal data could be extracted")
#'   }
#'
#'   # Combine all longitudinal datasets
#'   combined_longitudinal_data <- purrr::map_dfr(longitudinal_dataset_list, ~.x)
#'
#'   # Remove duplicate observations (same participant, visit, variable)
#'   deduped_longitudinal_data <- combined_longitudinal_data %>%
#'     dplyr::distinct(
#'       swan_participant_id,
#'       visit_number,
#'       incontinence_source_variable,
#'       .keep_all = TRUE
#'     )
#'
#'   if (verbose) {
#'     unique_participant_count <- length(unique(deduped_longitudinal_data$swan_participant_id))
#'     visit_range <- paste(
#'       min(deduped_longitudinal_data$visit_number, na.rm = TRUE), "to",
#'       max(deduped_longitudinal_data$visit_number, na.rm = TRUE)
#'     )
#'     logger::log_info("Comprehensive longitudinal data extracted: {nrow(deduped_longitudinal_data)} observations")
#'     logger::log_info("Visit range: {visit_range}")
#'     logger::log_info("Unique participants with longitudinal data: {unique_participant_count}")
#'   }
#'
#'   return(deduped_longitudinal_data)
#' }
#'
#' #' Process single file for longitudinal variables
#' #' @noRd
#' process_single_longitudinal_file <- function(file_path, include_visit_demographics, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Scanning {basename(file_path)} for longitudinal variables")
#'   }
#'
#'   # Load file safely
#'   file_environment <- new.env()
#'
#'   tryCatch({
#'     load(file_path, envir = file_environment)
#'   }, error = function(e) {
#'     if (verbose) {
#'       logger::log_warn("Failed to load {basename(file_path)}: {e$message}")
#'     }
#'     return(NULL)
#'   })
#'
#'   if (length(ls(file_environment)) == 0) return(NULL)
#'
#'   raw_longitudinal_dataset <- file_environment[[ls(file_environment)[1]]]
#'
#'   # Look for incontinence variables (INVOLEA/LEKINVO)
#'   incontinence_variable_names <- identify_incontinence_variables(raw_longitudinal_dataset)
#'
#'   if (length(incontinence_variable_names) == 0) {
#'     return(NULL)  # No incontinence variables in this file
#'   }
#'
#'   if (verbose) {
#'     incontinence_vars_string <- paste(incontinence_variable_names, collapse = ", ")
#'     logger::log_info("  Found {length(incontinence_variable_names)} incontinence variables: {incontinence_vars_string}")
#'   }
#'
#'   # Extract longitudinal data
#'   extracted_longitudinal_data <- extract_longitudinal_measurements(
#'     dataset = raw_longitudinal_dataset,
#'     incontinence_variables = incontinence_variable_names,
#'     include_visit_demographics = include_visit_demographics,
#'     source_file = basename(file_path),
#'     verbose = verbose
#'   )
#'
#'   return(extracted_longitudinal_data)
#' }
#'
#' #' Identify incontinence variables (INVOLEA/LEKINVO) in dataset
#' #' @noRd
#' identify_incontinence_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Look for INVOLEA and LEKINVO patterns
#'   incontinence_patterns <- c("INVOLEA\\d*", "LEKINVO\\d*", "involea\\d*", "lekinvo\\d*")
#'
#'   incontinence_variables <- c()
#'   for (pattern in incontinence_patterns) {
#'     matching_variables <- variable_names[stringr::str_detect(variable_names, pattern)]
#'     incontinence_variables <- c(incontinence_variables, matching_variables)
#'   }
#'
#'   return(unique(incontinence_variables))
#' }
#'
#' #' Extract longitudinal measurements including visit-specific demographics
#' #' @noRd
#' extract_longitudinal_measurements <- function(dataset, incontinence_variables,
#'                                               include_visit_demographics,
#'                                               source_file, verbose) {
#'
#'   # Identify core variables
#'   participant_id_variable <- identify_participant_id_variable(dataset)
#'   visit_number_variable <- identify_visit_number_variable(dataset)
#'
#'   if (is.null(participant_id_variable)) {
#'     if (verbose) {
#'       logger::log_warn("No participant ID found in {source_file}")
#'     }
#'     return(NULL)
#'   }
#'
#'   # Start with core columns
#'   core_column_names <- c(participant_id_variable)
#'   if (!is.null(visit_number_variable)) {
#'     core_column_names <- c(core_column_names, visit_number_variable)
#'   }
#'
#'   # Identify ALL visit-specific demographic variables if requested
#'   visit_specific_variables <- list()
#'
#'   if (include_visit_demographics) {
#'     visit_specific_variables <- identify_visit_specific_demographic_variables(dataset)
#'
#'     if (verbose && length(visit_specific_variables) > 0) {
#'       total_visit_vars <- sum(lengths(visit_specific_variables))
#'       visit_vars_string <- paste(names(visit_specific_variables), collapse = ", ")
#'       logger::log_info("  Visit-specific demographics found: {visit_vars_string} (total: {total_visit_vars} variables)")
#'     }
#'   }
#'
#'   # Combine all variables to extract - flatten the visit-specific variables list
#'   all_visit_specific_vars <- unlist(visit_specific_variables)
#'   all_variables_to_extract <- c(
#'     core_column_names,
#'     all_visit_specific_vars,  # Now includes ALL visit-specific variables
#'     incontinence_variables
#'   )
#'
#'   # Extract data
#'   longitudinal_measurements <- tibble::as_tibble(dataset) %>%
#'     dplyr::select(dplyr::all_of(all_variables_to_extract))
#'
#'   # Standardize core column names
#'   names(longitudinal_measurements)[1] <- "swan_participant_id"
#'   if (!is.null(visit_number_variable)) {
#'     names(longitudinal_measurements)[2] <- "visit_number_from_file"
#'   } else {
#'     longitudinal_measurements$visit_number_from_file <- NA_real_
#'   }
#'
#'   # Convert incontinence variables to long format first
#'   incontinence_long_format <- longitudinal_measurements %>%
#'     tidyr::pivot_longer(
#'       cols = dplyr::all_of(incontinence_variables),
#'       names_to = "incontinence_source_variable",
#'       values_to = "incontinence_raw_response"
#'     ) %>%
#'     dplyr::mutate(
#'       swan_participant_id = as.character(swan_participant_id),
#'       # Extract visit number from incontinence variable name
#'       visit_number = extract_visit_number_from_variable(incontinence_source_variable),
#'       incontinence_status = standardize_incontinence_response(incontinence_raw_response),
#'       years_since_baseline = visit_number,
#'       source_file = source_file
#'     ) %>%
#'     dplyr::select(-visit_number_from_file) %>%
#'     dplyr::filter(!is.na(incontinence_raw_response))  # Remove missing responses
#'
#'   # Now add visit-specific demographic variables that match the visit number
#'   if (include_visit_demographics && length(visit_specific_variables) > 0) {
#'
#'     for (demographic_type in names(visit_specific_variables)) {
#'       variable_names_for_this_type <- visit_specific_variables[[demographic_type]]
#'
#'       # Create a mapping of visit numbers to values for this demographic type
#'       visit_demographic_mapping <- tibble::tibble()
#'
#'       for (var_name in variable_names_for_this_type) {
#'         # Extract visit number from variable name (e.g., AGE3 -> 3, BMI0 -> 0)
#'         var_visit_number <- extract_visit_number_from_variable(var_name)
#'
#'         # Get the values for this variable
#'         var_values <- dataset[[var_name]]
#'
#'         # Create temporary mapping
#'         temp_mapping <- tibble::tibble(
#'           swan_participant_id = as.character(dataset[[participant_id_variable]]),
#'           visit_number = var_visit_number,
#'           !!paste0("visit_", demographic_type) := var_values
#'         ) %>%
#'           dplyr::filter(!is.na(.data[[paste0("visit_", demographic_type)]]))
#'
#'         # Combine with existing mapping
#'         if (nrow(visit_demographic_mapping) == 0) {
#'           visit_demographic_mapping <- temp_mapping
#'         } else {
#'           visit_demographic_mapping <- dplyr::bind_rows(visit_demographic_mapping, temp_mapping)
#'         }
#'       }
#'
#'       # Merge this demographic variable with the incontinence data
#'       if (nrow(visit_demographic_mapping) > 0) {
#'         incontinence_long_format <- incontinence_long_format %>%
#'           dplyr::left_join(
#'             visit_demographic_mapping,
#'             by = c("swan_participant_id", "visit_number")
#'           )
#'       }
#'     }
#'   }
#'
#'   # Log results
#'   if (verbose && nrow(incontinence_long_format) > 0) {
#'     unique_visits <- sort(unique(incontinence_long_format$visit_number))
#'     visits_string <- paste(unique_visits, collapse = ", ")
#'     logger::log_info("  Extracted visits for {source_file}: {visits_string}")
#'     logger::log_info("  Total observations in {source_file}: {nrow(incontinence_long_format)}")
#'
#'     # Log visit-specific variables preserved
#'     visit_specific_columns <- names(incontinence_long_format)[stringr::str_starts(names(incontinence_long_format), "visit_")]
#'     if (length(visit_specific_columns) > 0) {
#'       visit_vars_preserved <- paste(stringr::str_remove(visit_specific_columns, "^visit_"), collapse = ", ")
#'       logger::log_info("  Visit-specific variables preserved: {visit_vars_preserved}")
#'
#'       # Sample a few values to verify they're changing across visits
#'       if ("visit_bmi_kg_m2" %in% names(incontinence_long_format)) {
#'         sample_participant <- incontinence_long_format$swan_participant_id[1]
#'         sample_bmi_values <- incontinence_long_format %>%
#'           dplyr::filter(swan_participant_id == sample_participant) %>%
#'           dplyr::select(visit_number, visit_bmi_kg_m2) %>%
#'           dplyr::arrange(visit_number)
#'
#'         if (nrow(sample_bmi_values) > 1) {
#'           bmi_range <- paste(round(range(sample_bmi_values$visit_bmi_kg_m2, na.rm = TRUE), 1), collapse = " to ")
#'           logger::log_info("  Sample BMI range for participant {sample_participant}: {bmi_range}")
#'         }
#'       }
#'     }
#'   }
#'
#'   return(incontinence_long_format)
#' }
#'
#' #' Identify visit-specific demographic variables in dataset
#' #' @noRd
#' identify_visit_specific_demographic_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'   visit_specific_vars <- list()
#'
#'   # AGE variables - match only single digit visit numbers (AGE0, AGE1, AGE2, ..., AGE9)
#'   # Also match AGE10 specifically, but exclude AGE11, AGE21, etc.
#'   age_variables <- variable_names[stringr::str_detect(variable_names, "^AGE(\\d|10)$")]
#'   if (length(age_variables) > 0) {
#'     visit_specific_vars[["age_years"]] <- age_variables
#'   }
#'
#'   # BMI variables - match only single digit visit numbers (BMI0, BMI1, BMI2, ..., BMI9, BMI10)
#'   bmi_variables <- variable_names[stringr::str_detect(variable_names, "^BMI(\\d|10)$")]
#'   if (length(bmi_variables) > 0) {
#'     visit_specific_vars[["bmi_kg_m2"]] <- bmi_variables
#'   }
#'
#'   # Weight variables - match only single digit visit numbers
#'   weight_variables <- variable_names[stringr::str_detect(variable_names, "^WEIGHT(\\d|10)$")]
#'   if (length(weight_variables) > 0) {
#'     visit_specific_vars[["weight_kg"]] <- weight_variables
#'   }
#'
#'   # Height variables - match only single digit visit numbers
#'   height_variables <- variable_names[stringr::str_detect(variable_names, "^HEIGHT(\\d|10)$")]
#'   if (length(height_variables) > 0) {
#'     visit_specific_vars[["height_cm"]] <- height_variables
#'   }
#'
#'   # Smoking variables - match only single digit visit numbers
#'   smoking_variables <- variable_names[stringr::str_detect(variable_names, "^SMOKER?E?(\\d|10)$")]
#'   if (length(smoking_variables) > 0) {
#'     visit_specific_vars[["smoking_status"]] <- smoking_variables
#'   }
#'
#'   # Menopausal status variables - match only single digit visit numbers
#'   menopause_variables <- variable_names[stringr::str_detect(variable_names, "^STATUS(\\d|10)$")]
#'   if (length(menopause_variables) > 0) {
#'     visit_specific_vars[["menopausal_stage"]] <- menopause_variables
#'   }
#'
#'   # Marital status variables - match only single digit visit numbers
#'   marital_variables <- variable_names[stringr::str_detect(variable_names, "^MARITALGP(\\d|10)$")]
#'   if (length(marital_variables) > 0) {
#'     visit_specific_vars[["marital_status"]] <- marital_variables
#'   }
#'
#'   # Parity variables - match only single digit visit numbers
#'   parity_variables <- variable_names[stringr::str_detect(variable_names, "^NUMCHILD(\\d|10)$")]
#'   if (length(parity_variables) > 0) {
#'     visit_specific_vars[["parity_count"]] <- parity_variables
#'   }
#'
#'   return(visit_specific_vars)
#' }
#'
#' #' Extract visit number from variable name (e.g., INVOLEA9 -> 9, AGE1 -> 1)
#' #' @noRd
#' extract_visit_number_from_variable <- function(variable_names) {
#'
#'   # For SWAN variables, extract the visit number more carefully
#'   visit_numbers <- character(length(variable_names))
#'
#'   for (i in seq_along(variable_names)) {
#'     var_name <- variable_names[i]
#'
#'     # For incontinence variables (INVOLEA0, LEKINVO7, etc.)
#'     if (stringr::str_detect(var_name, "^(INVOLEA|LEKINVO)")) {
#'       visit_num <- stringr::str_extract(var_name, "\\d+$")
#'       visit_numbers[i] <- visit_num
#'     }
#'     # For demographic variables (AGE1, BMI1, STATUS1, etc.)
#'     else if (stringr::str_detect(var_name, "^(AGE|BMI|WEIGHT|HEIGHT|STATUS|SMOKER|MARITALGP|NUMCHILD)")) {
#'       # Extract only the last number, and only if it's 0-10 (valid visit numbers)
#'       potential_visit <- stringr::str_extract(var_name, "(\\d|10)$")
#'       if (!is.na(potential_visit) && as.numeric(potential_visit) <= 10) {
#'         visit_numbers[i] <- potential_visit
#'       } else {
#'         visit_numbers[i] <- NA_character_
#'       }
#'     }
#'     else {
#'       # For other variables, extract trailing digits
#'       visit_num <- stringr::str_extract(var_name, "\\d+$")
#'       visit_numbers[i] <- visit_num
#'     }
#'   }
#'
#'   # Convert to numeric
#'   visit_numbers_numeric <- as.numeric(visit_numbers)
#'
#'   # Replace any NAs with 0 (baseline)
#'   visit_numbers_numeric[is.na(visit_numbers_numeric)] <- 0
#'
#'   return(visit_numbers_numeric)
#' }
#'
#' #' Standardize incontinence response values
#' #' @noRd
#' standardize_incontinence_response <- function(response_values) {
#'
#'   dplyr::case_when(
#'     # Standard Yes responses
#'     response_values %in% c("(2) Yes", "2", 2, "Yes", "YES", "yes") ~ 1,
#'     # Standard No responses
#'     response_values %in% c("(1) No", "1", 1, "No", "NO", "no") ~ 0,
#'     # Extended formats from later visits
#'     response_values %in% c("(2) 2: Yes", "(2)2: Yes", "2: Yes") ~ 1,
#'     response_values %in% c("(1) 1: No", "(1)1: No", "1: No") ~ 0,
#'     TRUE ~ NA_real_
#'   )
#' }
#'
#' #' Helper function to identify participant ID column
#' #' @noRd
#' identify_participant_id_variable <- function(dataset) {
#'   participant_id_patterns <- c("^SWANID$", "^ARCHID$", "^swanid$", "^archid$")
#'   for (pattern in participant_id_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' Helper function to identify visit number column
#' #' @noRd
#' identify_visit_number_variable <- function(dataset) {
#'   visit_patterns <- c("^VISIT$", "^visit$", "^Visit$", "^VISITNO$")
#'   for (pattern in visit_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' Safe numeric conversion
#' #' @noRd
#' convert_to_numeric_safely <- function(variable_values) {
#'   suppressWarnings(as.numeric(as.character(variable_values)))
#' }
#'
#' #' Standardize race/ethnicity variable
#' #' @noRd
#' standardize_race_ethnicity_variable <- function(race_values) {
#'   dplyr::case_when(
#'     stringr::str_detect(as.character(race_values), "White|Caucasian|4") ~ "White",
#'     stringr::str_detect(as.character(race_values), "Black|African|1") ~ "Black",
#'     stringr::str_detect(as.character(race_values), "Hispanic|5") ~ "Hispanic",
#'     stringr::str_detect(as.character(race_values), "Chinese|2") ~ "Chinese",
#'     stringr::str_detect(as.character(race_values), "Japanese|3") ~ "Japanese",
#'     TRUE ~ "Other"
#'   )
#' }
#'
#' #' Standardize smoking status variable
#' #' @noRd
#' standardize_smoking_status_variable <- function(smoking_values) {
#'   dplyr::case_when(
#'     smoking_values %in% c(1, "(1) Never smoked", "Never") ~ "Never",
#'     smoking_values %in% c(2, "(2) Past smoker", "Past") ~ "Past",
#'     smoking_values %in% c(3, "(3) Current smoker", "Current") ~ "Current",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Standardize menopausal status variable that handles factors and numeric coding
#' #' @noRd
#' standardize_menopausal_status_variable <- function(status_values) {
#'   # Convert factors to character first
#'   if (is.factor(status_values)) {
#'     status_values <- as.character(status_values)
#'   }
#'
#'   # Handle numeric coding
#'   numeric_values <- suppressWarnings(as.numeric(status_values))
#'
#'   dplyr::case_when(
#'     # Standard text responses
#'     status_values %in% c("(1) Post by BSO", "Post by BSO") ~ "Post BSO",
#'     status_values %in% c("(2) Natural Post", "(2) Postmenopausal", "Natural Post") ~ "Natural Post",
#'     status_values %in% c("(3) Late Peri", "(3) Late perimenopausal", "Late Peri") ~ "Late Peri",
#'     status_values %in% c("(4) Early Peri", "(4) Early perimenopausal", "Early Peri") ~ "Early Peri",
#'     status_values %in% c("(5) Pre", "(5) Premenopausal", "Pre") ~ "Pre",
#'     status_values %in% c("(6) Pregnant/breastfeeding", "Pregnant/Breastfeeding") ~ "Pregnant/Breastfeeding",
#'     status_values %in% c("(7) Unknown due to HT use", "Unknown HT") ~ "Unknown HT",
#'     status_values %in% c("(8) Unknown due to hysterectomy", "Unknown Hysterectomy") ~ "Unknown Hysterectomy",
#'     # Numeric coding (SWAN format)
#'     numeric_values == 1 ~ "Post BSO",
#'     numeric_values == 2 ~ "Natural Post",
#'     numeric_values == 3 ~ "Late Peri",
#'     numeric_values == 4 ~ "Early Peri",
#'     numeric_values == 5 ~ "Pre",
#'     numeric_values == 6 ~ "Pregnant/Breastfeeding",
#'     numeric_values == 7 ~ "Unknown HT",
#'     numeric_values == 8 ~ "Unknown Hysterectomy",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Standardize education variable
#' #' @noRd
#' standardize_education_variable <- function(education_values) {
#'   dplyr::case_when(
#'     education_values %in% c(1, "(1) Less than high school") ~ "Less than HS",
#'     education_values %in% c(2, "(2) High school") ~ "High School",
#'     education_values %in% c(3, "(3) Some college") ~ "Some College",
#'     education_values %in% c(4, "(4) College graduate") ~ "College Graduate",
#'     education_values %in% c(5, "(5) Graduate school") ~ "Graduate School",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Standardize marital status variable
#' #' @noRd
#' standardize_marital_status_variable <- function(marital_values) {
#'   dplyr::case_when(
#'     marital_values %in% c(1, "(1) Married", "Married") ~ "Married",
#'     marital_values %in% c(2, "(2) Single", "Single") ~ "Single",
#'     marital_values %in% c(3, "(3) Divorced/Separated", "Divorced") ~ "Divorced/Separated",
#'     marital_values %in% c(4, "(4) Widowed", "Widowed") ~ "Widowed",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Combine multiple baseline sources with type-safe merging
#' #' @noRd
#' combine_multiple_baseline_sources <- function(baseline_dataset_list, verbose) {
#'
#'   if (length(baseline_dataset_list) == 1) {
#'     return(baseline_dataset_list[[1]])
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Combining {length(baseline_dataset_list)} baseline sources")
#'   }
#'
#'   # Find the most complete dataset (most non-missing participant IDs)
#'   dataset_completeness_scores <- purrr::map_dbl(baseline_dataset_list, function(baseline_dataset) {
#'     sum(!is.na(baseline_dataset$swan_participant_id))
#'   })
#'
#'   primary_baseline_dataset <- baseline_dataset_list[[which.max(dataset_completeness_scores)]]
#'
#'   if (verbose) {
#'     logger::log_info("Primary baseline dataset: {primary_baseline_dataset$source_file[1]} with {max(dataset_completeness_scores)} participants")
#'   }
#'
#'   # Merge additional datasets
#'   for (dataset_index in seq_along(baseline_dataset_list)) {
#'     if (dataset_index == which.max(dataset_completeness_scores)) next
#'
#'     secondary_baseline_dataset <- baseline_dataset_list[[dataset_index]]
#'
#'     if (verbose) {
#'       logger::log_info("Merging secondary baseline: {secondary_baseline_dataset$source_file[1]}")
#'     }
#'
#'     # Harmonize data types before merging
#'     secondary_baseline_dataset <- harmonize_baseline_data_types(
#'       secondary_baseline_dataset,
#'       primary_baseline_dataset,
#'       verbose
#'     )
#'
#'     primary_baseline_dataset <- primary_baseline_dataset %>%
#'       dplyr::full_join(
#'         secondary_baseline_dataset,
#'         by = "swan_participant_id",
#'         suffix = c("", "_secondary")
#'       )
#'
#'     # Smart coalescing - only coalesce variables that exist in both datasets
#'     baseline_variables_to_merge <- c(
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_parity_count",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_menopausal_stage",
#'       "baseline_education_level",
#'       "baseline_marital_status"
#'     )
#'
#'     # Check which variables actually exist with _secondary suffix
#'     available_columns <- names(primary_baseline_dataset)
#'
#'     for (baseline_variable in baseline_variables_to_merge) {
#'       secondary_variable_name <- paste0(baseline_variable, "_secondary")
#'
#'       if (baseline_variable %in% available_columns && secondary_variable_name %in% available_columns) {
#'         if (verbose) {
#'           logger::log_info("  Coalescing {baseline_variable}")
#'         }
#'
#'         # Apply appropriate coalescing based on data type
#'         if (stringr::str_detect(baseline_variable, "age|bmi|parity|count")) {
#'           # Numeric variables
#'           primary_baseline_dataset[[baseline_variable]] <- apply_safe_numeric_coalesce(
#'             primary_baseline_dataset[[baseline_variable]],
#'             primary_baseline_dataset[[secondary_variable_name]]
#'           )
#'         } else {
#'           # Character variables
#'           primary_baseline_dataset[[baseline_variable]] <- apply_safe_character_coalesce(
#'             primary_baseline_dataset[[baseline_variable]],
#'             primary_baseline_dataset[[secondary_variable_name]]
#'           )
#'         }
#'       } else if (verbose) {
#'         logger::log_info("  Skipping {baseline_variable} - not available in secondary dataset")
#'       }
#'     }
#'
#'     # Remove all secondary columns
#'     primary_baseline_dataset <- primary_baseline_dataset %>%
#'       dplyr::select(-dplyr::ends_with("_secondary"))
#'   }
#'
#'   if (verbose) {
#'     final_participant_count <- sum(!is.na(primary_baseline_dataset$swan_participant_id))
#'     logger::log_info("Combined baseline dataset: {final_participant_count} total participants")
#'   }
#'
#'   return(primary_baseline_dataset)
#' }
#'
#' #' Harmonize data types between baseline datasets
#' #' @noRd
#' harmonize_baseline_data_types <- function(secondary_dataset, primary_dataset, verbose) {
#'
#'   baseline_column_names <- names(secondary_dataset)[stringr::str_starts(names(secondary_dataset), "baseline_")]
#'
#'   for (column_name in baseline_column_names) {
#'     if (column_name %in% names(primary_dataset)) {
#'
#'       primary_data_type <- class(primary_dataset[[column_name]])[1]
#'       secondary_data_type <- class(secondary_dataset[[column_name]])[1]
#'
#'       if (primary_data_type != secondary_data_type) {
#'         if (verbose) {
#'           logger::log_info("Harmonizing {column_name}: {secondary_data_type} -> {primary_data_type}")
#'         }
#'
#'         # Convert secondary to match primary type
#'         if (primary_data_type %in% c("numeric", "double")) {
#'           secondary_dataset[[column_name]] <- convert_to_numeric_safely(secondary_dataset[[column_name]])
#'         } else if (primary_data_type == "character") {
#'           secondary_dataset[[column_name]] <- as.character(secondary_dataset[[column_name]])
#'         } else if (primary_data_type == "logical") {
#'           secondary_dataset[[column_name]] <- as.logical(secondary_dataset[[column_name]])
#'         }
#'       }
#'     }
#'   }
#'
#'   return(secondary_dataset)
#' }
#'
#' #' Type-safe numeric coalesce
#' #' @noRd
#' apply_safe_numeric_coalesce <- function(primary_values, secondary_values) {
#'   # Convert both to numeric if needed
#'   primary_numeric <- if (is.numeric(primary_values)) primary_values else convert_to_numeric_safely(primary_values)
#'   secondary_numeric <- if (is.numeric(secondary_values)) secondary_values else convert_to_numeric_safely(secondary_values)
#'   dplyr::coalesce(primary_numeric, secondary_numeric)
#' }
#'
#' #' Type-safe character coalesce
#' #' @noRd
#' apply_safe_character_coalesce <- function(primary_values, secondary_values) {
#'   # Convert both to character if needed
#'   primary_character <- if (is.character(primary_values)) primary_values else as.character(primary_values)
#'   secondary_character <- if (is.character(secondary_values)) secondary_values else as.character(secondary_values)
#'   dplyr::coalesce(primary_character, secondary_character)
#' }
#'
#' #' Merge baseline demographics with longitudinal data
#' #' @noRd
#' merge_baseline_with_longitudinal_data <- function(baseline_data, longitudinal_data, verbose) {
#'
#'   if (verbose) {
#'     baseline_participant_count <- length(unique(baseline_data$swan_participant_id))
#'     longitudinal_observation_count <- nrow(longitudinal_data)
#'     logger::log_info("Merging baseline demographics with longitudinal measurements")
#'     logger::log_info("Baseline participants: {baseline_participant_count}")
#'     logger::log_info("Longitudinal observations: {longitudinal_observation_count}")
#'   }
#'
#'   # Merge datasets
#'   merged_comprehensive_data <- longitudinal_data %>%
#'     dplyr::left_join(
#'       baseline_data %>% dplyr::select(-source_file),
#'       by = "swan_participant_id"
#'     )
#'
#'   # Add derived time variables
#'   merged_comprehensive_data <- merged_comprehensive_data %>%
#'     dplyr::mutate(
#'       # Calculate current age at each visit
#'       current_age_years = baseline_age_years + years_since_baseline,
#'       # Add visit timing categories
#'       visit_timing_category = dplyr::case_when(
#'         visit_number == 0 ~ "Baseline",
#'         visit_number %in% 1:3 ~ "Early Follow-up",
#'         visit_number %in% 4:7 ~ "Mid Follow-up",
#'         visit_number >= 8 ~ "Late Follow-up",
#'         TRUE ~ "Unknown"
#'       )
#'     ) %>%
#'     dplyr::arrange(swan_participant_id, visit_number)
#'
#'   if (verbose) {
#'     merged_observation_count <- nrow(merged_comprehensive_data)
#'     participants_with_both_data <- length(unique(merged_comprehensive_data$swan_participant_id[!is.na(merged_comprehensive_data$baseline_age_years)]))
#'     logger::log_info("Merged comprehensive dataset: {merged_observation_count} observations")
#'     logger::log_info("Participants with both baseline + longitudinal data: {participants_with_both_data}")
#'   }
#'
#'   return(merged_comprehensive_data)
#' }
#'
#' #' Apply filtering for valid responses
#' #' @noRd
#' apply_response_filtering <- function(merged_data, verbose) {
#'
#'   if (verbose) {
#'     original_observation_count <- nrow(merged_data)
#'     logger::log_info("Applying response filtering to remove invalid/missing incontinence responses")
#'   }
#'
#'   filtered_data <- merged_data %>%
#'     dplyr::filter(
#'       !is.na(incontinence_status),
#'       !is.na(swan_participant_id),
#'       !is.na(visit_number)
#'     )
#'
#'   if (verbose) {
#'     filtered_observation_count <- nrow(filtered_data)
#'     observations_removed <- original_observation_count - filtered_observation_count
#'     logger::log_info("Filtering completed: {filtered_observation_count} observations retained")
#'     logger::log_info("Observations removed: {observations_removed}")
#'   }
#'
#'   return(filtered_data)
#' }
#'
#' #' Format comprehensive output dataset
#' #' @noRd
#' format_comprehensive_output <- function(merged_data, format, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Formatting comprehensive output as: {format}")
#'     all_column_names <- paste(names(merged_data), collapse = ", ")
#'     logger::log_info("Available columns: {all_column_names}")
#'   }
#'
#'   if (format == "long") {
#'     # Identify visit-specific columns
#'     visit_specific_column_names <- names(merged_data)[stringr::str_starts(names(merged_data), "visit_")]
#'
#'     if (verbose && length(visit_specific_column_names) > 0) {
#'       visit_vars_string <- paste(visit_specific_column_names, collapse = ", ")
#'       logger::log_info("Visit-specific columns preserved: {visit_vars_string}")
#'     } else if (verbose) {
#'       logger::log_warn("No visit-specific columns found in final dataset")
#'     }
#'
#'     # Build comprehensive column selection
#'     baseline_demographic_columns <- c(
#'       "swan_participant_id",
#'       "visit_number",
#'       "years_since_baseline",
#'       "current_age_years",
#'       "visit_timing_category",
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_parity_count",
#'       "baseline_menopausal_stage",
#'       "baseline_education_level",
#'       "baseline_marital_status"
#'     )
#'
#'     incontinence_measurement_columns <- c(
#'       "incontinence_status",
#'       "incontinence_raw_response",
#'       "incontinence_source_variable"
#'     )
#'
#'     # Combine all columns, keeping only those that exist
#'     all_desired_columns <- c(
#'       baseline_demographic_columns,
#'       incontinence_measurement_columns,
#'       visit_specific_column_names
#'     )
#'
#'     columns_that_exist_in_data <- intersect(all_desired_columns, names(merged_data))
#'
#'     if (verbose) {
#'       logger::log_info("Selecting {length(columns_that_exist_in_data)} columns for long format output")
#'     }
#'
#'     # Create final long format dataset
#'     final_comprehensive_dataset <- merged_data %>%
#'       dplyr::select(dplyr::all_of(columns_that_exist_in_data)) %>%
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         incontinence_source_variable,
#'         .keep_all = TRUE
#'       ) %>%
#'       dplyr::arrange(swan_participant_id, visit_number)
#'
#'   } else {
#'     # Wide format conversion
#'     visit_specific_column_names <- names(merged_data)[stringr::str_starts(names(merged_data), "visit_")]
#'
#'     baseline_columns_for_wide <- c(
#'       "swan_participant_id",
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_parity_count",
#'       "baseline_menopausal_stage",
#'       "baseline_education_level",
#'       "baseline_marital_status"
#'     )
#'
#'     measurement_columns_for_wide <- c(
#'       "visit_number",
#'       "incontinence_status",
#'       "current_age_years"
#'     )
#'
#'     all_columns_for_wide <- c(
#'       baseline_columns_for_wide,
#'       measurement_columns_for_wide,
#'       visit_specific_column_names
#'     )
#'
#'     columns_that_exist_for_wide <- intersect(all_columns_for_wide, names(merged_data))
#'
#'     if (verbose) {
#'       logger::log_info("Converting to wide format with {length(columns_that_exist_for_wide)} base columns")
#'     }
#'
#'     final_comprehensive_dataset <- merged_data %>%
#'       dplyr::select(dplyr::all_of(columns_that_exist_for_wide)) %>%
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         .keep_all = TRUE
#'       ) %>%
#'       tidyr::pivot_wider(
#'         names_from = visit_number,
#'         values_from = c(
#'           dplyr::any_of(visit_specific_column_names),
#'           incontinence_status,
#'           current_age_years
#'         ),
#'         names_sep = "_visit_"
#'       )
#'   }
#'
#'   if (verbose) {
#'     final_row_count <- nrow(final_comprehensive_dataset)
#'     final_column_count <- ncol(final_comprehensive_dataset)
#'     unique_participant_count <- length(unique(final_comprehensive_dataset$swan_participant_id))
#'
#'     logger::log_info("Final comprehensive dataset formatting completed")
#'     logger::log_info("Dimensions: {final_row_count} rows × {final_column_count} columns")
#'     logger::log_info("Unique participants: {unique_participant_count}")
#'
#'     if (format == "long") {
#'       final_visit_specific_count <- sum(stringr::str_starts(names(final_comprehensive_dataset), "visit_"))
#'       visit_range_min <- min(final_comprehensive_dataset$visit_number, na.rm = TRUE)
#'       visit_range_max <- max(final_comprehensive_dataset$visit_number, na.rm = TRUE)
#'       logger::log_info("Visit-specific variables in final dataset: {final_visit_specific_count}")
#'       logger::log_info("Visit range: {visit_range_min} to {visit_range_max}")
#'     }
#'   }
#'
#'   return(final_comprehensive_dataset)
#' }
#'
#' # Run ----
#' comprehensive_swan_dataset_final_working <- create_comprehensive_swan_dataset(
#'   swan_data_directory = swan_data_directory,
#'   output_format = "long",
#'   verbose = TRUE,
#'   include_visit_demographics = TRUE,
#'   filter_valid_responses = TRUE
#' )
#'
#' write_rds(comprehensive_swan_dataset_final_working, "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/comprehensive_swan_dataset_final_working.rds")
#'
#' # Timoth Dall R Data Conversion Debugging in Project
#' # Back at it #' Process SWAN Dataset with Comprehensive Variable Transformations at 1205 ----
#' #' Process SWAN Dataset with Comprehensive Variable Transformations
#' #'
#' #' This function processes SWAN (Study of Women's Health Across the Nation)
#' #' datasets by standardizing and transforming key variables including smoking
#' #' status, parity, and other categorical variables into numeric formats suitable
#' #' for analysis.
#' #'
#' #' @param swan_dataset A data frame containing SWAN study data with variables
#' #'   to be processed
#' #' @param smoking_variable_name Character string specifying the column name
#' #'   containing smoking status data (default: "baseline_smoking_status")
#' #' @param parity_variable_name Character string specifying the column name
#' #'   containing parity/children count data (default: "baseline_parity_count")
#' #' @param output_directory Character string specifying the directory path
#' #'   where processed files should be saved (default: "processed_data")
#' #' @param create_output_directory Logical value indicating whether to create
#' #'   the output directory if it doesn't exist (default: TRUE)
#' #' @param verbose Logical value controlling the amount of logging output
#' #'   (default: FALSE)
#' #'
#' #' @return A data frame containing the processed SWAN dataset with transformed
#' #'   variables
#' #'
#' #' @examples
#' #' # Example 1: Basic usage with default parameters
#' #' swan_test_dataset <- data.frame(
#' #'   participant_id = c("S001", "S002", "S003"),
#' #'   baseline_smoking_status = c("(1) No", "(2) Yes", "(1) No"),
#' #'   baseline_parity_count = c("(0) No children", "(2) 2 children", "(1) 1 child"),
#' #'   stringsAsFactors = FALSE
#' #' )
#' #' processed_swan_data <- process_swan_dataset_comprehensive(
#' #'   swan_dataset = swan_test_dataset,
#' #'   smoking_variable_name = "baseline_smoking_status",
#' #'   parity_variable_name = "baseline_parity_count",
#' #'   output_directory = "analysis_output",
#' #'   create_output_directory = TRUE,
#' #'   verbose = TRUE
#' #' )
#' #' # Output: Processed data with smoking_status_numeric and parity_count_numeric
#' #'
#' #' # Example 2: Processing with custom variable names and no file output
#' #' custom_swan_data <- data.frame(
#' #'   study_id = c("SWAN_001", "SWAN_002", "SWAN_003", "SWAN_004"),
#' #'   smoke_history = c("(1) No", "(2) Yes", "(2) Yes", "(1) No"),
#' #'   children_count = c("(3) 3 children", "(0) No children", "(2) 2 children",
#' #'                      "(1) 1 child"),
#' #'   stringsAsFactors = FALSE
#' #' )
#' #' transformed_swan_data <- process_swan_dataset_comprehensive(
#' #'   swan_dataset = custom_swan_data,
#' #'   smoking_variable_name = "smoke_history",
#' #'   parity_variable_name = "children_count",
#' #'   output_directory = "temp_processing",
#' #'   create_output_directory = FALSE,
#' #'   verbose = TRUE
#' #' )
#' #' # Output: Data with standardized numeric variables for analysis
#' #'
#' #' # Example 3: Comprehensive processing with full logging enabled
#' #' large_swan_dataset <- data.frame(
#' #'   participant_identifier = paste0("PARTICIPANT_", 1:10),
#' #'   baseline_smoking_status = sample(c("(1) No", "(2) Yes"), 10, replace = TRUE),
#' #'   baseline_parity_count = sample(c("(0) No children", "(1) 1 child",
#' #'                                   "(2) 2 children", "(3) 3 children"),
#' #'                                  10, replace = TRUE),
#' #'   visit_number = rep(c("00", "01", "02"), length.out = 10),
#' #'   stringsAsFactors = FALSE
#' #' )
#' #' comprehensive_processed_data <- process_swan_dataset_comprehensive(
#' #'   swan_dataset = large_swan_dataset,
#' #'   smoking_variable_name = "baseline_smoking_status",
#' #'   parity_variable_name = "baseline_parity_count",
#' #'   output_directory = "comprehensive_analysis",
#' #'   create_output_directory = TRUE,
#' #'   verbose = TRUE
#' #' )
#' #' # Output: Fully processed dataset with extensive logging and file output
#' #'
#' #' @importFrom assertthat assert_that
#' #' @importFrom dplyr mutate select filter arrange case_when
#' #' @importFrom logger log_info log_warn log_error
#' #' @importFrom readr write_csv
#' #' @importFrom stringr str_extract str_trim str_detect
#' #' @importFrom tidyr replace_na
#' #'
#' #' @export
#' process_swan_dataset_comprehensive <- function(swan_dataset,
#'                                                smoking_variable_name = "baseline_smoking_status",
#'                                                parity_variable_name = "baseline_parity_count",
#'                                                output_directory = "processed_data",
#'                                                create_output_directory = TRUE,
#'                                                verbose = FALSE) {
#'
#'   # Initialize logging based on verbose setting
#'   initialize_comprehensive_logging(verbose_mode = verbose)
#'
#'   # Log function entry and input parameters
#'   logger::log_info("Starting comprehensive SWAN dataset processing")
#'   logger::log_info("Input dataset dimensions: {nrow(swan_dataset)} rows, {ncol(swan_dataset)} columns")
#'   logger::log_info("Smoking variable name: {smoking_variable_name}")
#'   logger::log_info("Parity variable name: {parity_variable_name}")
#'   logger::log_info("Output directory specified: {output_directory}")
#'   logger::log_info("Create output directory flag: {create_output_directory}")
#'   logger::log_info("Verbose logging enabled: {verbose}")
#'
#'   # Validate input parameters comprehensively
#'   validated_parameters <- validate_swan_processing_inputs(
#'     input_dataset = swan_dataset,
#'     smoking_column_name = smoking_variable_name,
#'     parity_column_name = parity_variable_name,
#'     output_path = output_directory,
#'     create_directory_flag = create_output_directory
#'   )
#'
#'   # Process smoking status variable transformation
#'   logger::log_info("Beginning smoking status variable transformation")
#'   transformed_dataset_smoking <- transform_smoking_status_variable(
#'     input_dataset = validated_parameters$dataset,
#'     smoking_column_identifier = smoking_variable_name,
#'     verbose_logging = verbose
#'   )
#'   logger::log_info("Smoking status transformation completed successfully")
#'
#'   # Process parity count variable transformation
#'   logger::log_info("Beginning parity count variable transformation")
#'   transformed_dataset_complete <- transform_parity_count_variable(
#'     input_dataset = transformed_dataset_smoking,
#'     parity_column_identifier = parity_variable_name,
#'     verbose_logging = verbose
#'   )
#'   logger::log_info("Parity count transformation completed successfully")
#'
#'   # Generate comprehensive summary statistics
#'   transformation_summary_statistics <- generate_transformation_summary_statistics(
#'     original_dataset = swan_dataset,
#'     processed_dataset = transformed_dataset_complete,
#'     smoking_variable = smoking_variable_name,
#'     parity_variable = parity_variable_name
#'   )
#'
#'   # Handle output directory creation and file writing
#'   if (create_output_directory) {
#'     output_file_information <- manage_output_directory_and_files(
#'       output_directory_path = output_directory,
#'       processed_dataset = transformed_dataset_complete,
#'       summary_statistics = transformation_summary_statistics,
#'       verbose_logging = verbose
#'     )
#'     logger::log_info("Output files written to: {output_file_information$directory_path}")
#'     logger::log_info("Processed dataset file: {output_file_information$dataset_file_path}")
#'     logger::log_info("Summary statistics file: {output_file_information$summary_file_path}")
#'   }
#'
#'   # Log final processing completion
#'   logger::log_info("SWAN dataset processing completed successfully")
#'   logger::log_info("Final dataset dimensions: {nrow(transformed_dataset_complete)} rows, {ncol(transformed_dataset_complete)} columns")
#'
#'   return(transformed_dataset_complete)
#' }
#'
#' #' @noRd
#' initialize_comprehensive_logging <- function(verbose_mode) {
#'   if (verbose_mode) {
#'     logger::log_threshold(logger::INFO)
#'   } else {
#'     logger::log_threshold(logger::WARN)
#'   }
#' }
#'
#' #' @noRd
#' validate_swan_processing_inputs <- function(input_dataset, smoking_column_name,
#'                                             parity_column_name, output_path,
#'                                             create_directory_flag) {
#'
#'   logger::log_info("Validating input parameters and dataset structure")
#'
#'   # Validate dataset is data frame
#'   assertthat::assert_that(is.data.frame(input_dataset),
#'                           msg = "Input dataset must be a data frame")
#'
#'   # Validate dataset has rows
#'   assertthat::assert_that(nrow(input_dataset) > 0,
#'                           msg = "Input dataset must contain at least one row")
#'
#'   # Validate smoking column exists
#'   assertthat::assert_that(smoking_column_name %in% names(input_dataset),
#'                           msg = paste("Smoking variable", smoking_column_name, "not found in dataset"))
#'
#'   # Validate parity column exists
#'   assertthat::assert_that(parity_column_name %in% names(input_dataset),
#'                           msg = paste("Parity variable", parity_column_name, "not found in dataset"))
#'
#'   # Validate output path is character
#'   assertthat::assert_that(is.character(output_path),
#'                           msg = "Output directory path must be a character string")
#'
#'   # Validate create directory flag is logical
#'   assertthat::assert_that(is.logical(create_directory_flag),
#'                           msg = "Create directory flag must be logical (TRUE/FALSE)")
#'
#'   logger::log_info("Input validation completed successfully")
#'
#'   return(list(
#'     dataset = input_dataset,
#'     smoking_column = smoking_column_name,
#'     parity_column = parity_column_name,
#'     output_directory = output_path,
#'     create_directory = create_directory_flag
#'   ))
#' }
#'
#' #' @noRd
#' transform_smoking_status_variable <- function(input_dataset, smoking_column_identifier, verbose_logging) {
#'
#'   logger::log_info("Starting smoking status variable transformation")
#'
#'   # Extract the smoking column for processing
#'   smoking_status_vector <- input_dataset[[smoking_column_identifier]]
#'   logger::log_info("Original smoking status values: {length(unique(smoking_status_vector))} unique values")
#'
#'   if (verbose_logging) {
#'     unique_smoking_values <- unique(smoking_status_vector)
#'     for (i in seq_along(unique_smoking_values)) {
#'       logger::log_info("Unique smoking value {i}: '{unique_smoking_values[i]}'")
#'     }
#'   }
#'
#'   # Apply smoking status standardization
#'   standardized_smoking_values <- standardize_smoking_status_with_logging(
#'     smoking_status_data = smoking_status_vector,
#'     verbose_mode = verbose_logging
#'   )
#'
#'   # Add the new column to the dataset
#'   transformed_dataset <- input_dataset %>%
#'     dplyr::mutate(smoking_status_numeric = standardized_smoking_values)
#'
#'   logger::log_info("Smoking status transformation: {sum(!is.na(standardized_smoking_values))} successful conversions out of {length(standardized_smoking_values)} total values")
#'
#'   return(transformed_dataset)
#' }
#'
#' #' @noRd
#' transform_parity_count_variable <- function(input_dataset, parity_column_identifier, verbose_logging) {
#'
#'   logger::log_info("Starting parity count variable transformation")
#'
#'   # Extract the parity column for processing
#'   parity_count_vector <- input_dataset[[parity_column_identifier]]
#'   logger::log_info("Original parity count values: {length(unique(parity_count_vector))} unique values")
#'
#'   if (verbose_logging) {
#'     unique_parity_values <- unique(parity_count_vector)
#'     for (i in seq_along(unique_parity_values)) {
#'       logger::log_info("Unique parity value {i}: '{unique_parity_values[i]}'")
#'     }
#'   }
#'
#'   # Apply parity count standardization
#'   standardized_parity_values <- standardize_parity_count_with_logging(
#'     parity_count_data = parity_count_vector,
#'     verbose_mode = verbose_logging
#'   )
#'
#'   # Add the new column to the dataset
#'   transformed_dataset <- input_dataset %>%
#'     dplyr::mutate(parity_count_numeric = standardized_parity_values)
#'
#'   logger::log_info("Parity count transformation: {sum(!is.na(standardized_parity_values))} successful conversions out of {length(standardized_parity_values)} total values")
#'
#'   return(transformed_dataset)
#' }
#'
#' #' @noRd
#' standardize_smoking_status_with_logging <- function(smoking_status_data, verbose_mode) {
#'
#'   logger::log_info("Applying smoking status standardization rules")
#'
#'   # Convert to character and clean whitespace
#'   cleaned_smoking_data <- stringr::str_trim(as.character(smoking_status_data))
#'
#'   # Apply case-when transformation with comprehensive pattern matching
#'   standardized_values <- dplyr::case_when(
#'     # Handle missing or empty values
#'     is.na(cleaned_smoking_data) | cleaned_smoking_data == "" ~ NA_real_,
#'
#'     # Handle "No" patterns - various formats
#'     stringr::str_detect(cleaned_smoking_data, "(?i)\\(1\\).*no") ~ 0,
#'     stringr::str_detect(cleaned_smoking_data, "(?i)^no$") ~ 0,
#'     stringr::str_detect(cleaned_smoking_data, "(?i)never") ~ 0,
#'     stringr::str_detect(cleaned_smoking_data, "(?i)non.?smoker") ~ 0,
#'
#'     # Handle "Yes" patterns - various formats
#'     stringr::str_detect(cleaned_smoking_data, "(?i)\\(2\\).*yes") ~ 1,
#'     stringr::str_detect(cleaned_smoking_data, "(?i)^yes$") ~ 1,
#'     stringr::str_detect(cleaned_smoking_data, "(?i)current") ~ 1,
#'     stringr::str_detect(cleaned_smoking_data, "(?i)smoker") ~ 1,
#'
#'     # Handle numeric codes directly
#'     cleaned_smoking_data == "0" ~ 0,
#'     cleaned_smoking_data == "1" ~ 1,
#'     cleaned_smoking_data == "2" ~ 1,
#'
#'     # Default to NA for unrecognized patterns
#'     TRUE ~ NA_real_
#'   )
#'
#'   if (verbose_mode) {
#'     conversion_success_count <- sum(!is.na(standardized_values))
#'     total_value_count <- length(standardized_values)
#'     logger::log_info("Smoking status conversion success rate: {conversion_success_count}/{total_value_count} ({round(100*conversion_success_count/total_value_count, 1)}%)")
#'
#'     # Log any unrecognized patterns
#'     unrecognized_patterns <- unique(cleaned_smoking_data[is.na(standardized_values) & !is.na(cleaned_smoking_data)])
#'     if (length(unrecognized_patterns) > 0) {
#'       logger::log_warn("Unrecognized smoking status patterns found:")
#'       for (pattern in unrecognized_patterns) {
#'         logger::log_warn("  Unrecognized pattern: '{pattern}'")
#'       }
#'     }
#'   }
#'
#'   return(standardized_values)
#' }
#'
#' #' @noRd
#' standardize_parity_count_with_logging <- function(parity_count_data, verbose_mode) {
#'
#'   logger::log_info("Applying parity count standardization rules")
#'
#'   # Convert to character and clean whitespace
#'   cleaned_parity_data <- stringr::str_trim(as.character(parity_count_data))
#'
#'   # Extract numeric values using comprehensive pattern matching
#'   extracted_numeric_values <- stringr::str_extract(cleaned_parity_data, "\\(([0-9]+)\\)|^([0-9]+)$")
#'   extracted_numeric_values <- stringr::str_extract(extracted_numeric_values, "[0-9]+")
#'
#'   # Convert to numeric, handling special cases
#'   standardized_values <- dplyr::case_when(
#'     # Handle missing or empty values
#'     is.na(cleaned_parity_data) | cleaned_parity_data == "" ~ NA_real_,
#'
#'     # Handle "no children" patterns explicitly
#'     stringr::str_detect(cleaned_parity_data, "(?i)no.?children") ~ 0,
#'     stringr::str_detect(cleaned_parity_data, "(?i)\\(0\\)") ~ 0,
#'
#'     # Handle numeric extraction results
#'     !is.na(extracted_numeric_values) ~ as.numeric(extracted_numeric_values),
#'
#'     # Handle direct numeric strings
#'     stringr::str_detect(cleaned_parity_data, "^[0-9]+$") ~ as.numeric(cleaned_parity_data),
#'
#'     # Default to NA for unrecognized patterns
#'     TRUE ~ NA_real_
#'   )
#'
#'   if (verbose_mode) {
#'     conversion_success_count <- sum(!is.na(standardized_values))
#'     total_value_count <- length(standardized_values)
#'     logger::log_info("Parity count conversion success rate: {conversion_success_count}/{total_value_count} ({round(100*conversion_success_count/total_value_count, 1)}%)")
#'
#'     # Log any unrecognized patterns
#'     unrecognized_patterns <- unique(cleaned_parity_data[is.na(standardized_values) & !is.na(cleaned_parity_data)])
#'     if (length(unrecognized_patterns) > 0) {
#'       logger::log_warn("Unrecognized parity count patterns found:")
#'       for (pattern in unrecognized_patterns) {
#'         logger::log_warn("  Unrecognized pattern: '{pattern}'")
#'       }
#'     }
#'   }
#'
#'   return(standardized_values)
#' }
#'
#' #' @noRd
#' generate_transformation_summary_statistics <- function(original_dataset, processed_dataset,
#'                                                        smoking_variable, parity_variable) {
#'
#'   logger::log_info("Generating comprehensive transformation summary statistics")
#'
#'   # Calculate smoking status transformation statistics
#'   original_smoking_values <- original_dataset[[smoking_variable]]
#'   processed_smoking_values <- processed_dataset$smoking_status_numeric
#'
#'   smoking_transformation_stats <- list(
#'     original_unique_count = length(unique(original_smoking_values, useNA = "always")),
#'     processed_unique_count = length(unique(processed_smoking_values, useNA = "always")),
#'     conversion_success_rate = sum(!is.na(processed_smoking_values)) / length(processed_smoking_values),
#'     value_distribution = table(processed_smoking_values, useNA = "always")
#'   )
#'
#'   # Calculate parity count transformation statistics
#'   original_parity_values <- original_dataset[[parity_variable]]
#'   processed_parity_values <- processed_dataset$parity_count_numeric
#'
#'   parity_transformation_stats <- list(
#'     original_unique_count = length(unique(original_parity_values, useNA = "always")),
#'     processed_unique_count = length(unique(processed_parity_values, useNA = "always")),
#'     conversion_success_rate = sum(!is.na(processed_parity_values)) / length(processed_parity_values),
#'     value_distribution = table(processed_parity_values, useNA = "always"),
#'     summary_statistics = summary(processed_parity_values)
#'   )
#'
#'   comprehensive_summary <- list(
#'     dataset_dimensions = list(
#'       original_rows = nrow(original_dataset),
#'       original_columns = ncol(original_dataset),
#'       processed_rows = nrow(processed_dataset),
#'       processed_columns = ncol(processed_dataset)
#'     ),
#'     smoking_transformation = smoking_transformation_stats,
#'     parity_transformation = parity_transformation_stats,
#'     processing_timestamp = Sys.time()
#'   )
#'
#'   logger::log_info("Summary statistics generation completed")
#'
#'   return(comprehensive_summary)
#' }
#'
#' #' @noRd
#' manage_output_directory_and_files <- function(output_directory_path, processed_dataset,
#'                                               summary_statistics, verbose_logging) {
#'
#'   logger::log_info("Managing output directory and file writing operations")
#'
#'   # Create output directory if it doesn't exist
#'   if (!dir.exists(output_directory_path)) {
#'     dir.create(output_directory_path, recursive = TRUE)
#'     logger::log_info("Created output directory: {output_directory_path}")
#'   } else {
#'     logger::log_info("Output directory already exists: {output_directory_path}")
#'   }
#'
#'   # Generate timestamp for file naming
#'   current_timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
#'
#'   # Define file paths
#'   processed_dataset_file_path <- file.path(output_directory_path,
#'                                            paste0("processed_swan_dataset_", current_timestamp, ".csv"))
#'   summary_statistics_file_path <- file.path(output_directory_path,
#'                                             paste0("transformation_summary_", current_timestamp, ".txt"))
#'
#'   # Write processed dataset to CSV
#'   readr::write_csv(processed_dataset, processed_dataset_file_path)
#'   logger::log_info("Processed dataset written to: {processed_dataset_file_path}")
#'
#'   # Write summary statistics to text file
#'   capture.output({
#'     cat("SWAN Dataset Transformation Summary\n")
#'     cat("==================================\n\n")
#'     cat("Processing Timestamp:", as.character(summary_statistics$processing_timestamp), "\n\n")
#'     cat("Dataset Dimensions:\n")
#'     cat("  Original:", summary_statistics$dataset_dimensions$original_rows, "rows x",
#'         summary_statistics$dataset_dimensions$original_columns, "columns\n")
#'     cat("  Processed:", summary_statistics$dataset_dimensions$processed_rows, "rows x",
#'         summary_statistics$dataset_dimensions$processed_columns, "columns\n\n")
#'     cat("Smoking Status Transformation:\n")
#'     cat("  Conversion Success Rate:", round(summary_statistics$smoking_transformation$conversion_success_rate * 100, 2), "%\n")
#'     cat("  Value Distribution:\n")
#'     print(summary_statistics$smoking_transformation$value_distribution)
#'     cat("\nParity Count Transformation:\n")
#'     cat("  Conversion Success Rate:", round(summary_statistics$parity_transformation$conversion_success_rate * 100, 2), "%\n")
#'     cat("  Value Distribution:\n")
#'     print(summary_statistics$parity_transformation$value_distribution)
#'     cat("  Summary Statistics:\n")
#'     print(summary_statistics$parity_transformation$summary_statistics)
#'   }, file = summary_statistics_file_path)
#'
#'   logger::log_info("Summary statistics written to: {summary_statistics_file_path}")
#'
#'   return(list(
#'     directory_path = output_directory_path,
#'     dataset_file_path = processed_dataset_file_path,
#'     summary_file_path = summary_statistics_file_path
#'   ))
#' }
#'
#' # Testing
#' # Test with your exact data patterns
#' smoking_test <- c("(1) No", "(1) No", "(2) Yes", "(2) Yes", "(1) No",
#'                   "(2) Yes", "(2) Yes", "(2) Yes", "(2) Yes", "(1) No")
#' parity_test <- c("(3) 3 children", "(3) 3 children", "(0) No children",
#'                  "(2) 2 children", "(2) 2 children")
#'
#' # Create test dataset
#' test_data <- data.frame(
#'   baseline_smoking_status = smoking_test[1:5],
#'   baseline_parity_count = parity_test,
#'   stringsAsFactors = FALSE
#' )
#'
#' # Process with full logging
#' processed_data <- process_swan_dataset_comprehensive(
#'   swan_dataset = test_data,
#'   verbose = TRUE
#' )
#'
#' process_swan_dataset_comprehensive(swan_data_directory,
#'                                                smoking_variable_name = "baseline_smoking_status",
#'                                                parity_variable_name = "baseline_parity_count",
#'                                                output_directory = "processed_data",
#'                                                create_output_directory = TRUE,
#'                                                verbose = FALSE)
#'
#' # version 7 #' Process SWAN Dataset with Comprehensive Variable Transformations , function at 1214 ----
#' #' Process SWAN Dataset with Comprehensive Variable Transformations
#' #'
#' #' This function processes SWAN (Study of Women's Health Across the Nation)
#' #' datasets by loading and merging all .rda files from a specified directory,
#' #' then standardizing and transforming key variables including smoking
#' #' status, parity, and other categorical variables into numeric formats suitable
#' #' for analysis.
#' #'
#' #' @param swan_data_directory Character string specifying the directory path
#' #'   containing SWAN .rda data files to be loaded and merged
#' #' @param smoking_variable_name Character string specifying the column name
#' #'   containing smoking status data (default: "baseline_smoking_status")
#' #' @param parity_variable_name Character string specifying the column name
#' #'   containing parity/children count data (default: "baseline_parity_count")
#' #' @param output_directory Character string specifying the directory path
#' #'   where processed files should be saved (default: "processed_data")
#' #' @param create_output_directory Logical value indicating whether to create
#' #'   the output directory if it doesn't exist (default: TRUE)
#' #' @param verbose Logical value controlling the amount of logging output
#' #'   (default: FALSE)
#' #'
#' #' @return A data frame containing the processed SWAN dataset with transformed
#' #'   variables
#' #'
#' #' @examples
#' #' # Example 1: Basic usage processing all .rda files in SWAN directory
#' #' swan_directory_path <- "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/"
#' #' processed_swan_data <- process_swan_dataset_comprehensive(
#' #'   swan_data_directory = swan_directory_path,
#' #'   smoking_variable_name = "baseline_smoking_status",
#' #'   parity_variable_name = "baseline_parity_count",
#' #'   output_directory = "analysis_output",
#' #'   create_output_directory = TRUE,
#' #'   verbose = TRUE
#' #' )
#' #' # Output: Merged and processed data from all .rda files with transformations
#' #'
#' #' # Example 2: Processing with custom variable names and directory structure
#' #' custom_swan_directory <- "/path/to/swan/data/files/"
#' #' transformed_swan_data <- process_swan_dataset_comprehensive(
#' #'   swan_data_directory = custom_swan_directory,
#' #'   smoking_variable_name = "smoke_status_baseline",
#' #'   parity_variable_name = "children_count_baseline",
#' #'   output_directory = "temp_processing",
#' #'   create_output_directory = FALSE,
#' #'   verbose = TRUE
#' #' )
#' #' # Output: Comprehensive dataset merged from directory with custom variables
#' #'
#' #' # Example 3: Full processing with extensive logging for production analysis
#' #' production_swan_directory <- "~/data/SWAN_study_files/"
#' #' comprehensive_processed_data <- process_swan_dataset_comprehensive(
#' #'   swan_data_directory = production_swan_directory,
#' #'   smoking_variable_name = "baseline_smoking_status",
#' #'   parity_variable_name = "baseline_parity_count",
#' #'   output_directory = "comprehensive_analysis",
#' #'   create_output_directory = TRUE,
#' #'   verbose = TRUE
#' #' )
#' #' # Output: Production-ready merged dataset with full logging and file outputs
#' #'
#' #' @importFrom assertthat assert_that
#' #' @importFrom dplyr mutate select filter arrange case_when full_join bind_rows
#' #' @importFrom logger log_info log_warn log_error
#' #' @importFrom readr write_csv
#' #' @importFrom stringr str_extract str_trim str_detect
#' #' @importFrom tidyr replace_na
#' #'
#' #' @export
#' process_swan_dataset_comprehensive <- function(swan_data_directory,
#'                                                smoking_variable_name = "baseline_smoking_status",
#'                                                parity_variable_name = "baseline_parity_count",
#'                                                output_directory = "processed_data",
#'                                                create_output_directory = TRUE,
#'                                                verbose = FALSE) {
#'
#'   # Initialize logging based on verbose setting
#'   initialize_comprehensive_logging(verbose_mode = verbose)
#'
#'   # Log function entry and input parameters
#'   logger::log_info("Starting comprehensive SWAN dataset processing from directory")
#'   logger::log_info("SWAN data directory: {swan_data_directory}")
#'   logger::log_info("Smoking variable name: {smoking_variable_name}")
#'   logger::log_info("Parity variable name: {parity_variable_name}")
#'   logger::log_info("Output directory specified: {output_directory}")
#'   logger::log_info("Create output directory flag: {create_output_directory}")
#'   logger::log_info("Verbose logging enabled: {verbose}")
#'
#'   # Load and merge all .rda files from the directory
#'   logger::log_info("Beginning .rda file discovery and loading process")
#'   merged_swan_dataset <- load_and_merge_swan_rda_files(
#'     directory_path = swan_data_directory,
#'     verbose_logging = verbose
#'   )
#'
#'   # Validate input parameters comprehensively
#'   validated_parameters <- validate_swan_processing_inputs(
#'     input_dataset = merged_swan_dataset,
#'     smoking_column_name = smoking_variable_name,
#'     parity_column_name = parity_variable_name,
#'     output_path = output_directory,
#'     create_directory_flag = create_output_directory
#'   )
#'
#'   # Process smoking status variable transformation
#'   logger::log_info("Beginning smoking status variable transformation")
#'   transformed_dataset_smoking <- transform_smoking_status_variable(
#'     input_dataset = validated_parameters$dataset,
#'     smoking_column_identifier = smoking_variable_name,
#'     verbose_logging = verbose
#'   )
#'   logger::log_info("Smoking status transformation completed successfully")
#'
#'   # Process parity count variable transformation
#'   logger::log_info("Beginning parity count variable transformation")
#'   transformed_dataset_complete <- transform_parity_count_variable(
#'     input_dataset = transformed_dataset_smoking,
#'     parity_column_identifier = parity_variable_name,
#'     verbose_logging = verbose
#'   )
#'   logger::log_info("Parity count transformation completed successfully")
#'
#'   # Generate comprehensive summary statistics
#'   transformation_summary_statistics <- generate_transformation_summary_statistics(
#'     original_dataset = merged_swan_dataset,
#'     processed_dataset = transformed_dataset_complete,
#'     smoking_variable = smoking_variable_name,
#'     parity_variable = parity_variable_name
#'   )
#'
#'   # Handle output directory creation and file writing
#'   if (create_output_directory) {
#'     output_file_information <- manage_output_directory_and_files(
#'       output_directory_path = output_directory,
#'       processed_dataset = transformed_dataset_complete,
#'       summary_statistics = transformation_summary_statistics,
#'       verbose_logging = verbose
#'     )
#'     logger::log_info("Output files written to: {output_file_information$directory_path}")
#'     logger::log_info("Processed dataset file: {output_file_information$dataset_file_path}")
#'     logger::log_info("Summary statistics file: {output_file_information$summary_file_path}")
#'   }
#'
#'   # Log final processing completion
#'   logger::log_info("SWAN dataset processing completed successfully")
#'   logger::log_info("Final dataset dimensions: {nrow(transformed_dataset_complete)} rows, {ncol(transformed_dataset_complete)} columns")
#'
#'   return(transformed_dataset_complete)
#' }
#'
#' #' @noRd
#' load_and_merge_swan_rda_files <- function(directory_path, verbose_logging) {
#'
#'   logger::log_info("Discovering .rda files in directory: {directory_path}")
#'
#'   # Validate directory exists
#'   assertthat::assert_that(dir.exists(directory_path),
#'                           msg = paste("Directory does not exist:", directory_path))
#'
#'   # Find all .rda files in the directory
#'   rda_file_paths <- list.files(path = directory_path,
#'                                pattern = "\\.rda$",
#'                                full.names = TRUE,
#'                                recursive = FALSE)
#'
#'   assertthat::assert_that(length(rda_file_paths) > 0,
#'                           msg = paste("No .rda files found in directory:", directory_path))
#'
#'   logger::log_info("Found {length(rda_file_paths)} .rda files to process")
#'
#'   if (verbose_logging) {
#'     for (i in seq_along(rda_file_paths)) {
#'       file_name <- basename(rda_file_paths[i])
#'       file_size <- file.size(rda_file_paths[i])
#'       logger::log_info("  File {i}: {file_name} ({round(file_size/1024/1024, 2)} MB)")
#'     }
#'   }
#'
#'   # Load each .rda file and store datasets
#'   loaded_datasets_list <- list()
#'
#'   for (i in seq_along(rda_file_paths)) {
#'     current_file_path <- rda_file_paths[i]
#'     current_file_name <- basename(current_file_path)
#'
#'     logger::log_info("Loading file {i}/{length(rda_file_paths)}: {current_file_name}")
#'
#'     # Create a new environment for loading
#'     loading_environment <- new.env()
#'
#'     # Load the .rda file into the environment
#'     load(current_file_path, envir = loading_environment)
#'
#'     # Get the loaded object names
#'     loaded_object_names <- ls(loading_environment)
#'
#'     if (length(loaded_object_names) == 1) {
#'       # Single object - use it directly
#'       current_dataset <- get(loaded_object_names[1], envir = loading_environment)
#'
#'       if (is.data.frame(current_dataset)) {
#'         logger::log_info("  Successfully loaded dataset with {nrow(current_dataset)} rows, {ncol(current_dataset)} columns")
#'         loaded_datasets_list[[current_file_name]] <- current_dataset
#'       } else {
#'         logger::log_warn("  Skipping {current_file_name}: loaded object is not a data frame")
#'       }
#'     } else if (length(loaded_object_names) > 1) {
#'       logger::log_warn("  Multiple objects found in {current_file_name}: {paste(loaded_object_names, collapse = ', ')}")
#'
#'       # Try to find the largest data frame
#'       largest_dataset <- NULL
#'       largest_size <- 0
#'
#'       for (obj_name in loaded_object_names) {
#'         obj <- get(obj_name, envir = loading_environment)
#'         if (is.data.frame(obj) && nrow(obj) > largest_size) {
#'           largest_dataset <- obj
#'           largest_size <- nrow(obj)
#'         }
#'       }
#'
#'       if (!is.null(largest_dataset)) {
#'         logger::log_info("  Using largest data frame with {nrow(largest_dataset)} rows, {ncol(largest_dataset)} columns")
#'         loaded_datasets_list[[current_file_name]] <- largest_dataset
#'       } else {
#'         logger::log_warn("  No data frames found in {current_file_name}")
#'       }
#'     } else {
#'       logger::log_warn("  No objects found in {current_file_name}")
#'     }
#'   }
#'
#'   assertthat::assert_that(length(loaded_datasets_list) > 0,
#'                           msg = "No valid data frames were loaded from .rda files")
#'
#'   logger::log_info("Successfully loaded {length(loaded_datasets_list)} datasets")
#'
#'   # Merge datasets based on common columns
#'   merged_comprehensive_dataset <- merge_swan_datasets_intelligently(
#'     datasets_list = loaded_datasets_list,
#'     verbose_logging = verbose_logging
#'   )
#'
#'   return(merged_comprehensive_dataset)
#' }
#'
#' #' @noRd
#' merge_swan_datasets_intelligently <- function(datasets_list, verbose_logging) {
#'
#'   logger::log_info("Beginning intelligent dataset merging process")
#'
#'   # Identify potential key columns for merging
#'   all_column_names <- unique(unlist(lapply(datasets_list, names)))
#'
#'   # Common SWAN key columns to look for
#'   potential_key_columns <- c("ARCHID", "archid", "ID", "id", "participant_id",
#'                              "PARTICIPANT_ID", "subject_id", "SUBJECT_ID")
#'
#'   # Find which key columns exist across datasets
#'   available_key_columns <- intersect(potential_key_columns, all_column_names)
#'
#'   if (length(available_key_columns) == 0) {
#'     logger::log_warn("No standard key columns found. Attempting row binding instead of merging.")
#'
#'     # If no common keys, try to row bind datasets with same column structure
#'     merged_dataset_by_rows <- attempt_row_binding_datasets(datasets_list, verbose_logging)
#'     return(merged_dataset_by_rows)
#'   }
#'
#'   # Use the first available key column
#'   primary_key_column <- available_key_columns[1]
#'   logger::log_info("Using '{primary_key_column}' as primary merge key")
#'
#'   # Start with the first dataset
#'   dataset_names <- names(datasets_list)
#'   merged_comprehensive_dataset <- datasets_list[[1]]
#'   logger::log_info("Starting merge with dataset: {dataset_names[1]} ({nrow(merged_comprehensive_dataset)} rows)")
#'
#'   # Merge subsequent datasets
#'   for (i in 2:length(datasets_list)) {
#'     current_dataset_name <- dataset_names[i]
#'     current_dataset <- datasets_list[[i]]
#'
#'     if (primary_key_column %in% names(current_dataset)) {
#'       logger::log_info("Merging dataset {i}/{length(datasets_list)}: {current_dataset_name}")
#'
#'       # Perform full outer join to preserve all data
#'       merged_comprehensive_dataset <- dplyr::full_join(
#'         merged_comprehensive_dataset,
#'         current_dataset,
#'         by = primary_key_column,
#'         suffix = c("", paste0("_", gsub("\\.rda$", "", current_dataset_name)))
#'       )
#'
#'       logger::log_info("  Merge completed. Result: {nrow(merged_comprehensive_dataset)} rows, {ncol(merged_comprehensive_dataset)} columns")
#'     } else {
#'       logger::log_warn("  Skipping {current_dataset_name}: key column '{primary_key_column}' not found")
#'     }
#'   }
#'
#'   logger::log_info("Dataset merging completed successfully")
#'   logger::log_info("Final merged dataset: {nrow(merged_comprehensive_dataset)} rows, {ncol(merged_comprehensive_dataset)} columns")
#'
#'   return(merged_comprehensive_dataset)
#' }
#'
#' #' @noRd
#' attempt_row_binding_datasets <- function(datasets_list, verbose_logging) {
#'
#'   logger::log_info("Attempting to combine datasets using row binding")
#'
#'   # Check if all datasets have compatible column structures
#'   first_dataset_columns <- names(datasets_list[[1]])
#'   compatible_datasets <- list()
#'
#'   for (i in seq_along(datasets_list)) {
#'     current_dataset_name <- names(datasets_list)[i]
#'     current_dataset <- datasets_list[[i]]
#'     current_columns <- names(current_dataset)
#'
#'     if (setequal(current_columns, first_dataset_columns)) {
#'       compatible_datasets[[current_dataset_name]] <- current_dataset
#'       logger::log_info("  Dataset {current_dataset_name}: compatible for row binding")
#'     } else {
#'       logger::log_warn("  Dataset {current_dataset_name}: incompatible column structure, skipping")
#'     }
#'   }
#'
#'   if (length(compatible_datasets) > 0) {
#'     # Combine compatible datasets
#'     combined_dataset <- dplyr::bind_rows(compatible_datasets, .id = "source_file")
#'     logger::log_info("Row binding completed: {nrow(combined_dataset)} total rows from {length(compatible_datasets)} datasets")
#'     return(combined_dataset)
#'   } else {
#'     # If no compatible datasets, return the largest one
#'     largest_dataset_index <- which.max(sapply(datasets_list, nrow))
#'     largest_dataset <- datasets_list[[largest_dataset_index]]
#'     logger::log_warn("No compatible datasets for merging. Returning largest dataset: {names(datasets_list)[largest_dataset_index]}")
#'     return(largest_dataset)
#'   }
#' }
#'
#' #' @noRd
#' initialize_comprehensive_logging <- function(verbose_mode) {
#'   if (verbose_mode) {
#'     logger::log_threshold(logger::INFO)
#'   } else {
#'     logger::log_threshold(logger::WARN)
#'   }
#' }
#'
#' #' @noRd
#' validate_swan_processing_inputs <- function(input_dataset, smoking_column_name,
#'                                             parity_column_name, output_path,
#'                                             create_directory_flag) {
#'
#'   logger::log_info("Validating input parameters and dataset structure")
#'
#'   # Validate dataset is data frame
#'   assertthat::assert_that(is.data.frame(input_dataset),
#'                           msg = "Input dataset must be a data frame")
#'
#'   # Validate dataset has rows
#'   assertthat::assert_that(nrow(input_dataset) > 0,
#'                           msg = "Input dataset must contain at least one row")
#'
#'   # Validate smoking column exists
#'   assertthat::assert_that(smoking_column_name %in% names(input_dataset),
#'                           msg = paste("Smoking variable", smoking_column_name, "not found in dataset"))
#'
#'   # Validate parity column exists
#'   assertthat::assert_that(parity_column_name %in% names(input_dataset),
#'                           msg = paste("Parity variable", parity_column_name, "not found in dataset"))
#'
#'   # Validate output path is character
#'   assertthat::assert_that(is.character(output_path),
#'                           msg = "Output directory path must be a character string")
#'
#'   # Validate create directory flag is logical
#'   assertthat::assert_that(is.logical(create_directory_flag),
#'                           msg = "Create directory flag must be logical (TRUE/FALSE)")
#'
#'   logger::log_info("Input validation completed successfully")
#'
#'   return(list(
#'     dataset = input_dataset,
#'     smoking_column = smoking_column_name,
#'     parity_column = parity_column_name,
#'     output_directory = output_path,
#'     create_directory = create_directory_flag
#'   ))
#' }
#'
#' #' @noRd
#' transform_smoking_status_variable <- function(input_dataset, smoking_column_identifier, verbose_logging) {
#'
#'   logger::log_info("Starting smoking status variable transformation")
#'
#'   # Extract the smoking column for processing
#'   smoking_status_vector <- input_dataset[[smoking_column_identifier]]
#'   logger::log_info("Original smoking status values: {length(unique(smoking_status_vector))} unique values")
#'
#'   if (verbose_logging) {
#'     unique_smoking_values <- unique(smoking_status_vector)
#'     for (i in seq_along(unique_smoking_values)) {
#'       logger::log_info("Unique smoking value {i}: '{unique_smoking_values[i]}'")
#'     }
#'   }
#'
#'   # Apply smoking status standardization
#'   standardized_smoking_values <- standardize_smoking_status_with_logging(
#'     smoking_status_data = smoking_status_vector,
#'     verbose_mode = verbose_logging
#'   )
#'
#'   # Add the new column to the dataset
#'   transformed_dataset <- input_dataset %>%
#'     dplyr::mutate(smoking_status_numeric = standardized_smoking_values)
#'
#'   logger::log_info("Smoking status transformation: {sum(!is.na(standardized_smoking_values))} successful conversions out of {length(standardized_smoking_values)} total values")
#'
#'   return(transformed_dataset)
#' }
#'
#' #' @noRd
#' transform_parity_count_variable <- function(input_dataset, parity_column_identifier, verbose_logging) {
#'
#'   logger::log_info("Starting parity count variable transformation")
#'
#'   # Extract the parity column for processing
#'   parity_count_vector <- input_dataset[[parity_column_identifier]]
#'   logger::log_info("Original parity count values: {length(unique(parity_count_vector))} unique values")
#'
#'   if (verbose_logging) {
#'     unique_parity_values <- unique(parity_count_vector)
#'     for (i in seq_along(unique_parity_values)) {
#'       logger::log_info("Unique parity value {i}: '{unique_parity_values[i]}'")
#'     }
#'   }
#'
#'   # Apply parity count standardization
#'   standardized_parity_values <- standardize_parity_count_with_logging(
#'     parity_count_data = parity_count_vector,
#'     verbose_mode = verbose_logging
#'   )
#'
#'   # Add the new column to the dataset
#'   transformed_dataset <- input_dataset %>%
#'     dplyr::mutate(parity_count_numeric = standardized_parity_values)
#'
#'   logger::log_info("Parity count transformation: {sum(!is.na(standardized_parity_values))} successful conversions out of {length(standardized_parity_values)} total values")
#'
#'   return(transformed_dataset)
#' }
#'
#' #' @noRd
#' standardize_smoking_status_with_logging <- function(smoking_status_data, verbose_mode) {
#'
#'   logger::log_info("Applying smoking status standardization rules")
#'
#'   # Convert to character and clean whitespace
#'   cleaned_smoking_data <- stringr::str_trim(as.character(smoking_status_data))
#'
#'   # Apply case-when transformation with comprehensive pattern matching
#'   standardized_values <- dplyr::case_when(
#'     # Handle missing or empty values
#'     is.na(cleaned_smoking_data) | cleaned_smoking_data == "" ~ NA_real_,
#'
#'     # Handle "No" patterns - various formats
#'     stringr::str_detect(cleaned_smoking_data, "(?i)\\(1\\).*no") ~ 0,
#'     stringr::str_detect(cleaned_smoking_data, "(?i)^no$") ~ 0,
#'     stringr::str_detect(cleaned_smoking_data, "(?i)never") ~ 0,
#'     stringr::str_detect(cleaned_smoking_data, "(?i)non.?smoker") ~ 0,
#'
#'     # Handle "Yes" patterns - various formats
#'     stringr::str_detect(cleaned_smoking_data, "(?i)\\(2\\).*yes") ~ 1,
#'     stringr::str_detect(cleaned_smoking_data, "(?i)^yes$") ~ 1,
#'     stringr::str_detect(cleaned_smoking_data, "(?i)current") ~ 1,
#'     stringr::str_detect(cleaned_smoking_data, "(?i)smoker") ~ 1,
#'
#'     # Handle numeric codes directly
#'     cleaned_smoking_data == "0" ~ 0,
#'     cleaned_smoking_data == "1" ~ 1,
#'     cleaned_smoking_data == "2" ~ 1,
#'
#'     # Default to NA for unrecognized patterns
#'     TRUE ~ NA_real_
#'   )
#'
#'   if (verbose_mode) {
#'     conversion_success_count <- sum(!is.na(standardized_values))
#'     total_value_count <- length(standardized_values)
#'     logger::log_info("Smoking status conversion success rate: {conversion_success_count}/{total_value_count} ({round(100*conversion_success_count/total_value_count, 1)}%)")
#'
#'     # Log any unrecognized patterns
#'     unrecognized_patterns <- unique(cleaned_smoking_data[is.na(standardized_values) & !is.na(cleaned_smoking_data)])
#'     if (length(unrecognized_patterns) > 0) {
#'       logger::log_warn("Unrecognized smoking status patterns found:")
#'       for (pattern in unrecognized_patterns) {
#'         logger::log_warn("  Unrecognized pattern: '{pattern}'")
#'       }
#'     }
#'   }
#'
#'   return(standardized_values)
#' }
#'
#' #' @noRd
#' standardize_parity_count_with_logging <- function(parity_count_data, verbose_mode) {
#'
#'   logger::log_info("Applying parity count standardization rules")
#'
#'   # Convert to character and clean whitespace
#'   cleaned_parity_data <- stringr::str_trim(as.character(parity_count_data))
#'
#'   # Extract numeric values using comprehensive pattern matching
#'   extracted_numeric_values <- stringr::str_extract(cleaned_parity_data, "\\(([0-9]+)\\)|^([0-9]+)$")
#'   extracted_numeric_values <- stringr::str_extract(extracted_numeric_values, "[0-9]+")
#'
#'   # Convert to numeric, handling special cases
#'   standardized_values <- dplyr::case_when(
#'     # Handle missing or empty values
#'     is.na(cleaned_parity_data) | cleaned_parity_data == "" ~ NA_real_,
#'
#'     # Handle "no children" patterns explicitly
#'     stringr::str_detect(cleaned_parity_data, "(?i)no.?children") ~ 0,
#'     stringr::str_detect(cleaned_parity_data, "(?i)\\(0\\)") ~ 0,
#'
#'     # Handle numeric extraction results
#'     !is.na(extracted_numeric_values) ~ as.numeric(extracted_numeric_values),
#'
#'     # Handle direct numeric strings
#'     stringr::str_detect(cleaned_parity_data, "^[0-9]+$") ~ as.numeric(cleaned_parity_data),
#'
#'     # Default to NA for unrecognized patterns
#'     TRUE ~ NA_real_
#'   )
#'
#'   if (verbose_mode) {
#'     conversion_success_count <- sum(!is.na(standardized_values))
#'     total_value_count <- length(standardized_values)
#'     logger::log_info("Parity count conversion success rate: {conversion_success_count}/{total_value_count} ({round(100*conversion_success_count/total_value_count, 1)}%)")
#'
#'     # Log any unrecognized patterns
#'     unrecognized_patterns <- unique(cleaned_parity_data[is.na(standardized_values) & !is.na(cleaned_parity_data)])
#'     if (length(unrecognized_patterns) > 0) {
#'       logger::log_warn("Unrecognized parity count patterns found:")
#'       for (pattern in unrecognized_patterns) {
#'         logger::log_warn("  Unrecognized pattern: '{pattern}'")
#'       }
#'     }
#'   }
#'
#'   return(standardized_values)
#' }
#'
#' #' @noRd
#' generate_transformation_summary_statistics <- function(original_dataset, processed_dataset,
#'                                                        smoking_variable, parity_variable) {
#'
#'   logger::log_info("Generating comprehensive transformation summary statistics")
#'
#'   # Calculate smoking status transformation statistics
#'   original_smoking_values <- original_dataset[[smoking_variable]]
#'   processed_smoking_values <- processed_dataset$smoking_status_numeric
#'
#'   smoking_transformation_stats <- list(
#'     original_unique_count = length(unique(original_smoking_values, useNA = "always")),
#'     processed_unique_count = length(unique(processed_smoking_values, useNA = "always")),
#'     conversion_success_rate = sum(!is.na(processed_smoking_values)) / length(processed_smoking_values),
#'     value_distribution = table(processed_smoking_values, useNA = "always")
#'   )
#'
#'   # Calculate parity count transformation statistics
#'   original_parity_values <- original_dataset[[parity_variable]]
#'   processed_parity_values <- processed_dataset$parity_count_numeric
#'
#'   parity_transformation_stats <- list(
#'     original_unique_count = length(unique(original_parity_values, useNA = "always")),
#'     processed_unique_count = length(unique(processed_parity_values, useNA = "always")),
#'     conversion_success_rate = sum(!is.na(processed_parity_values)) / length(processed_parity_values),
#'     value_distribution = table(processed_parity_values, useNA = "always"),
#'     summary_statistics = summary(processed_parity_values)
#'   )
#'
#'   comprehensive_summary <- list(
#'     dataset_dimensions = list(
#'       original_rows = nrow(original_dataset),
#'       original_columns = ncol(original_dataset),
#'       processed_rows = nrow(processed_dataset),
#'       processed_columns = ncol(processed_dataset)
#'     ),
#'     smoking_transformation = smoking_transformation_stats,
#'     parity_transformation = parity_transformation_stats,
#'     processing_timestamp = Sys.time()
#'   )
#'
#'   logger::log_info("Summary statistics generation completed")
#'
#'   return(comprehensive_summary)
#' }
#'
#' #' @noRd
#' manage_output_directory_and_files <- function(output_directory_path, processed_dataset,
#'                                               summary_statistics, verbose_logging) {
#'
#'   logger::log_info("Managing output directory and file writing operations")
#'
#'   # Create output directory if it doesn't exist
#'   if (!dir.exists(output_directory_path)) {
#'     dir.create(output_directory_path, recursive = TRUE)
#'     logger::log_info("Created output directory: {output_directory_path}")
#'   } else {
#'     logger::log_info("Output directory already exists: {output_directory_path}")
#'   }
#'
#'   # Generate timestamp for file naming
#'   current_timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
#'
#'   # Define file paths
#'   processed_dataset_file_path <- file.path(output_directory_path,
#'                                            paste0("processed_swan_dataset_", current_timestamp, ".csv"))
#'   summary_statistics_file_path <- file.path(output_directory_path,
#'                                             paste0("transformation_summary_", current_timestamp, ".txt"))
#'
#'   # Write processed dataset to CSV
#'   readr::write_csv(processed_dataset, processed_dataset_file_path)
#'   logger::log_info("Processed dataset written to: {processed_dataset_file_path}")
#'
#'   # Write summary statistics to text file
#'   capture.output({
#'     cat("SWAN Dataset Transformation Summary\n")
#'     cat("==================================\n\n")
#'     cat("Processing Timestamp:", as.character(summary_statistics$processing_timestamp), "\n\n")
#'     cat("Dataset Dimensions:\n")
#'     cat("  Original:", summary_statistics$dataset_dimensions$original_rows, "rows x",
#'         summary_statistics$dataset_dimensions$original_columns, "columns\n")
#'     cat("  Processed:", summary_statistics$dataset_dimensions$processed_rows, "rows x",
#'         summary_statistics$dataset_dimensions$processed_columns, "columns\n\n")
#'     cat("Smoking Status Transformation:\n")
#'     cat("  Conversion Success Rate:", round(summary_statistics$smoking_transformation$conversion_success_rate * 100, 2), "%\n")
#'     cat("  Value Distribution:\n")
#'     print(summary_statistics$smoking_transformation$value_distribution)
#'     cat("\nParity Count Transformation:\n")
#'     cat("  Conversion Success Rate:", round(summary_statistics$parity_transformation$conversion_success_rate * 100, 2), "%\n")
#'     cat("  Value Distribution:\n")
#'     print(summary_statistics$parity_transformation$value_distribution)
#'     cat("  Summary Statistics:\n")
#'     print(summary_statistics$parity_transformation$summary_statistics)
#'   }, file = summary_statistics_file_path)
#'
#'   logger::log_info("Summary statistics written to: {summary_statistics_file_path}")
#'
#'   return(list(
#'     directory_path = output_directory_path,
#'     dataset_file_path = processed_dataset_file_path,
#'     summary_file_path = summary_statistics_file_path
#'   ))
#' }
#'
#' # Run ----
#' # Process all .rda files in your SWAN directory
#' processed_data <- process_swan_dataset_comprehensive(
#'   swan_data_directory = "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/",
#'   smoking_variable_name = "baseline_smoking_status",
#'   parity_variable_name = "baseline_parity_count",
#'   output_directory = "merged_swan_analysis",
#'   create_output_directory = TRUE,
#'   verbose = TRUE
#' )
#'
#'
#' #' # Merge SWAN .rda Files from Directory -----
#' #' #' Merge SWAN .rda Files from Directory
#' #' #'
#' #' #' This function discovers, loads, and intelligently merges all .rda files from
#' #' #' a specified SWAN data directory. It handles different column structures,
#' #' #' identifies optimal merge keys, and creates a comprehensive merged dataset
#' #' #' suitable for analysis.
#' #' #'
#' #' #' @param swan_data_directory_path Character string specifying the directory path
#' #' #'   containing SWAN .rda data files to be loaded and merged
#' #' #' @param merge_strategy Character string specifying the merge approach:
#' #' #'   "intelligent" (default), "by_key", "row_bind", or "largest_only"
#' #' #' @param output_merged_file_path Character string specifying the path where
#' #' #'   the merged dataset should be saved (default: NULL, no file output)
#' #' #' @param preferred_key_columns Character vector of preferred column names to use
#' #' #'   as merge keys, in order of preference (default: SWAN standard keys)
#' #' #' @param create_source_tracking Logical value indicating whether to add a
#' #' #'   source file tracking column (default: TRUE)
#' #' #' @param verbose_logging Logical value controlling detailed logging output
#' #' #'   (default: FALSE)
#' #' #'
#' #' #' @return A data frame containing the merged SWAN dataset from all .rda files
#' #' #'
#' #' #' @examples
#' #' #' # Example 1: Basic merge with intelligent strategy
#' #' #' swan_directory <- "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/"
#' #' #' merged_swan_dataset <- merge_swan_rda_files_comprehensive(
#' #' #'   swan_data_directory_path = swan_directory,
#' #' #'   merge_strategy = "intelligent",
#' #' #'   output_merged_file_path = NULL,
#' #' #'   create_source_tracking = TRUE,
#' #' #'   verbose_logging = TRUE
#' #' #' )
#' #' #' # Output: Intelligently merged dataset from all .rda files
#' #' #'
#' #' #' # Example 2: Merge using specific key column with file output
#' #' #' merged_with_output <- merge_swan_rda_files_comprehensive(
#' #' #'   swan_data_directory_path = swan_directory,
#' #' #'   merge_strategy = "by_key",
#' #' #'   output_merged_file_path = "merged_swan_data.csv",
#' #' #'   preferred_key_columns = c("ARCHID", "archid", "participant_id"),
#' #' #'   create_source_tracking = TRUE,
#' #' #'   verbose_logging = TRUE
#' #' #' )
#' #' #' # Output: Key-based merge with file saved to disk
#' #' #'
#' #' #' # Example 3: Row binding strategy for compatible datasets
#' #' #' row_bound_dataset <- merge_swan_rda_files_comprehensive(
#' #' #'   swan_data_directory_path = swan_directory,
#' #' #'   merge_strategy = "row_bind",
#' #' #'   output_merged_file_path = "swan_combined_rows.rds",
#' #' #'   preferred_key_columns = NULL,
#' #' #'   create_source_tracking = TRUE,
#' #' #'   verbose_logging = FALSE
#' #' #' )
#' #' #' # Output: Row-bound dataset with source tracking
#' #' #'
#' #' #' @importFrom assertthat assert_that
#' #' #' @importFrom dplyr full_join bind_rows mutate select arrange
#' #' #' @importFrom logger log_info log_warn log_error
#' #' #' @importFrom readr write_csv
#' #' #' @importFrom stringr str_detect str_extract str_trim
#' #' #' @importFrom purrr map_dfr
#' #' #'
#' #' #' @export
#' #' merge_swan_rda_files_comprehensive <- function(swan_data_directory_path,
#' #'                                                merge_strategy = "intelligent",
#' #'                                                output_merged_file_path = NULL,
#' #'                                                preferred_key_columns = c("ARCHID", "archid", "ID", "id",
#' #'                                                                          "participant_id", "PARTICIPANT_ID",
#' #'                                                                          "subject_id", "SUBJECT_ID"),
#' #'                                                create_source_tracking = TRUE,
#' #'                                                verbose_logging = FALSE) {
#' #'
#' #'   # Initialize logging
#' #'   initialize_merger_logging(verbose_mode = verbose_logging)
#' #'
#' #'   # Log function entry and parameters
#' #'   logger::log_info("Starting SWAN .rda files merge process")
#' #'   logger::log_info("Directory path: {swan_data_directory_path}")
#' #'   logger::log_info("Merge strategy: {merge_strategy}")
#' #'   logger::log_info("Output file path: {ifelse(is.null(output_merged_file_path), 'None (memory only)', output_merged_file_path)}")
#' #'   logger::log_info("Source tracking enabled: {create_source_tracking}")
#' #'   logger::log_info("Verbose logging: {verbose_logging}")
#' #'
#' #'   # Validate inputs
#' #'   validate_merger_inputs(
#' #'     directory_path = swan_data_directory_path,
#' #'     strategy = merge_strategy,
#' #'     output_path = output_merged_file_path,
#' #'     key_columns = preferred_key_columns
#' #'   )
#' #'
#' #'   # Discover and load all .rda files
#' #'   loaded_datasets_collection <- discover_and_load_swan_files(
#' #'     directory_path = swan_data_directory_path,
#' #'     verbose_mode = verbose_logging
#' #'   )
#' #'
#' #'   # Analyze merge opportunities
#' #'   merge_analysis_results <- analyze_merge_opportunities_comprehensive(
#' #'     datasets_collection = loaded_datasets_collection,
#' #'     preferred_keys = preferred_key_columns,
#' #'     verbose_mode = verbose_logging
#' #'   )
#' #'
#' #'   # Execute merge strategy
#' #'   merged_comprehensive_dataset <- execute_merge_strategy_comprehensive(
#' #'     datasets_collection = loaded_datasets_collection,
#' #'     merge_analysis = merge_analysis_results,
#' #'     strategy = merge_strategy,
#' #'     track_sources = create_source_tracking,
#' #'     verbose_mode = verbose_logging
#' #'   )
#' #'
#' #'   # Save output file if requested
#' #'   if (!is.null(output_merged_file_path)) {
#' #'     save_merged_dataset_to_file(
#' #'       merged_dataset = merged_comprehensive_dataset,
#' #'       file_path = output_merged_file_path,
#' #'       verbose_mode = verbose_logging
#' #'     )
#' #'   }
#' #'
#' #'   # Log completion
#' #'   logger::log_info("SWAN .rda files merge completed successfully")
#' #'   logger::log_info("Final merged dataset: {nrow(merged_comprehensive_dataset)} rows, {ncol(merged_comprehensive_dataset)} columns")
#' #'
#' #'   return(merged_comprehensive_dataset)
#' #' }
#' #'
#' #' #' @noRd
#' #' initialize_merger_logging <- function(verbose_mode) {
#' #'   if (verbose_mode) {
#' #'     logger::log_threshold(logger::INFO)
#' #'   } else {
#' #'     logger::log_threshold(logger::WARN)
#' #'   }
#' #' }
#' #'
#' #' #' @noRd
#' #' validate_merger_inputs <- function(directory_path, strategy, output_path, key_columns) {
#' #'
#' #'   logger::log_info("Validating merger input parameters")
#' #'
#' #'   # Validate directory exists
#' #'   assertthat::assert_that(dir.exists(directory_path),
#' #'                           msg = paste("Directory does not exist:", directory_path))
#' #'
#' #'   # Validate merge strategy
#' #'   valid_strategies <- c("intelligent", "by_key", "row_bind", "largest_only")
#' #'   assertthat::assert_that(strategy %in% valid_strategies,
#' #'                           msg = paste("Invalid merge strategy. Must be one of:", paste(valid_strategies, collapse = ", ")))
#' #'
#' #'   # Validate output path if provided
#' #'   if (!is.null(output_path)) {
#' #'     assertthat::assert_that(is.character(output_path),
#' #'                             msg = "Output file path must be a character string")
#' #'
#' #'     # Check if output directory exists
#' #'     output_directory <- dirname(output_path)
#' #'     if (!dir.exists(output_directory)) {
#' #'       logger::log_warn("Output directory does not exist and will be created: {output_directory}")
#' #'     }
#' #'   }
#' #'
#' #'   # Validate key columns if provided
#' #'   if (!is.null(key_columns)) {
#' #'     assertthat::assert_that(is.character(key_columns),
#' #'                             msg = "Preferred key columns must be a character vector")
#' #'   }
#' #'
#' #'   logger::log_info("Input validation completed successfully")
#' #' }
#' #'
#' #' #' @noRd
#' #' discover_and_load_swan_files <- function(directory_path, verbose_mode) {
#' #'
#' #'   logger::log_info("Discovering .rda files in directory: {directory_path}")
#' #'
#' #'   # Find all .rda files
#' #'   rda_file_paths <- list.files(path = directory_path,
#' #'                                pattern = "\\.rda$",
#' #'                                full.names = TRUE,
#' #'                                recursive = FALSE)
#' #'
#' #'   assertthat::assert_that(length(rda_file_paths) > 0,
#' #'                           msg = paste("No .rda files found in directory:", directory_path))
#' #'
#' #'   logger::log_info("Found {length(rda_file_paths)} .rda files to process")
#' #'
#' #'   # Load each file with comprehensive information
#' #'   datasets_collection <- list()
#' #'
#' #'   for (i in seq_along(rda_file_paths)) {
#' #'     current_file_path <- rda_file_paths[i]
#' #'     current_file_name <- basename(current_file_path)
#' #'
#' #'     logger::log_info("Loading file {i}/{length(rda_file_paths)}: {current_file_name}")
#' #'
#' #'     # Load file information
#' #'     file_info <- load_single_rda_file_comprehensive(
#' #'       file_path = current_file_path,
#' #'       file_name = current_file_name,
#' #'       verbose_mode = verbose_mode
#' #'     )
#' #'
#' #'     if (!is.null(file_info)) {
#' #'       datasets_collection[[current_file_name]] <- file_info
#' #'     }
#' #'   }
#' #'
#' #'   assertthat::assert_that(length(datasets_collection) > 0,
#' #'                           msg = "No valid datasets were loaded from .rda files")
#' #'
#' #'   logger::log_info("Successfully loaded {length(datasets_collection)} datasets")
#' #'
#' #'   return(datasets_collection)
#' #' }
#' #'
#' #' #' @noRd
#' #' load_single_rda_file_comprehensive <- function(file_path, file_name, verbose_mode) {
#' #'
#' #'   # Create environment for loading
#' #'   loading_environment <- new.env()
#' #'
#' #'   tryCatch({
#' #'     # Load the .rda file
#' #'     load(file_path, envir = loading_environment)
#' #'
#' #'     # Get loaded objects
#' #'     loaded_object_names <- ls(loading_environment)
#' #'
#' #'     if (length(loaded_object_names) == 0) {
#' #'       logger::log_warn("  No objects found in {file_name}")
#' #'       return(NULL)
#' #'     }
#' #'
#' #'     # Handle single vs multiple objects
#' #'     if (length(loaded_object_names) == 1) {
#' #'       dataset_object <- get(loaded_object_names[1], envir = loading_environment)
#' #'     } else {
#' #'       # Multiple objects - find largest data frame
#' #'       logger::log_warn("  Multiple objects in {file_name}: {paste(loaded_object_names, collapse = ', ')}")
#' #'
#' #'       largest_dataset <- NULL
#' #'       largest_size <- 0
#' #'
#' #'       for (obj_name in loaded_object_names) {
#' #'         obj <- get(obj_name, envir = loading_environment)
#' #'         if (is.data.frame(obj) && nrow(obj) > largest_size) {
#' #'           largest_dataset <- obj
#' #'           largest_size <- nrow(obj)
#' #'         }
#' #'       }
#' #'
#' #'       dataset_object <- largest_dataset
#' #'     }
#' #'
#' #'     # Validate it's a data frame
#' #'     if (!is.data.frame(dataset_object)) {
#' #'       logger::log_warn("  Loaded object from {file_name} is not a data frame")
#' #'       return(NULL)
#' #'     }
#' #'
#' #'     # Create comprehensive file information
#' #'     file_info <- list(
#' #'       dataset = dataset_object,
#' #'       file_name = file_name,
#' #'       file_path = file_path,
#' #'       file_size_mb = round(file.size(file_path) / 1024 / 1024, 2),
#' #'       row_count = nrow(dataset_object),
#' #'       column_count = ncol(dataset_object),
#' #'       column_names = names(dataset_object),
#' #'       loaded_objects = loaded_object_names
#' #'     )
#' #'
#' #'     if (verbose_mode) {
#' #'       logger::log_info("    Rows: {file_info$row_count}, Columns: {file_info$column_count}")
#' #'       logger::log_info("    File size: {file_info$file_size_mb} MB")
#' #'
#' #'       # Show sample of columns
#' #'       sample_columns <- head(file_info$column_names, 8)
#' #'       logger::log_info("    Sample columns: {paste(sample_columns, collapse = ', ')}")
#' #'     }
#' #'
#' #'     return(file_info)
#' #'
#' #'   }, error = function(e) {
#' #'     logger::log_error("  Error loading {file_name}: {e$message}")
#' #'     return(NULL)
#' #'   })
#' #' }
#' #'
#' #' #' @noRd
#' #' analyze_merge_opportunities_comprehensive <- function(datasets_collection, preferred_keys, verbose_mode) {
#' #'
#' #'   logger::log_info("Analyzing merge opportunities across datasets")
#' #'
#' #'   # Extract all unique column names
#' #'   all_columns_across_files <- unique(unlist(lapply(datasets_collection, function(x) x$column_names)))
#' #'
#' #'   # Find available key columns
#' #'   available_key_columns <- intersect(preferred_keys, all_columns_across_files)
#' #'
#' #'   if (verbose_mode) {
#' #'     logger::log_info("Total unique columns across all files: {length(all_columns_across_files)}")
#' #'     logger::log_info("Available preferred key columns: {paste(available_key_columns, collapse = ', ')}")
#' #'   }
#' #'
#' #'   # Analyze key column coverage
#' #'   key_analysis <- list()
#' #'
#' #'   for (key_col in available_key_columns) {
#' #'     files_with_key <- character(0)
#' #'
#' #'     for (file_name in names(datasets_collection)) {
#' #'       if (key_col %in% datasets_collection[[file_name]]$column_names) {
#' #'         files_with_key <- c(files_with_key, file_name)
#' #'       }
#' #'     }
#' #'
#' #'     key_analysis[[key_col]] <- list(
#' #'       files_with_key = files_with_key,
#' #'       coverage_count = length(files_with_key),
#' #'       coverage_percentage = round(length(files_with_key) / length(datasets_collection) * 100, 1)
#' #'     )
#' #'
#' #'     if (verbose_mode && length(files_with_key) > 1) {
#' #'       logger::log_info("Key '{key_col}': {length(files_with_key)} files ({key_analysis[[key_col]]$coverage_percentage}%)")
#' #'     }
#' #'   }
#' #'
#' #'   # Analyze column overlap for row binding potential
#' #'   column_overlap_analysis <- analyze_column_overlap_patterns(datasets_collection, verbose_mode)
#' #'
#' #'   # Determine best merge strategies
#' #'   best_key_column <- NULL
#' #'   best_key_coverage <- 0
#' #'
#' #'   if (length(available_key_columns) > 0) {
#' #'     coverage_counts <- sapply(key_analysis, function(x) x$coverage_count)
#' #'     best_key_index <- which.max(coverage_counts)
#' #'     best_key_column <- names(key_analysis)[best_key_index]
#' #'     best_key_coverage <- coverage_counts[best_key_index]
#' #'   }
#' #'
#' #'   merge_analysis <- list(
#' #'     available_keys = available_key_columns,
#' #'     key_analysis = key_analysis,
#' #'     best_key_column = best_key_column,
#' #'     best_key_coverage = best_key_coverage,
#' #'     column_overlap = column_overlap_analysis,
#' #'     total_files = length(datasets_collection)
#' #'   )
#' #'
#' #'   if (verbose_mode) {
#' #'     if (!is.null(best_key_column)) {
#' #'       logger::log_info("Best merge key: '{best_key_column}' (covers {best_key_coverage}/{merge_analysis$total_files} files)")
#' #'     } else {
#' #'       logger::log_info("No suitable merge keys found - will consider alternative strategies")
#' #'     }
#' #'   }
#' #'
#' #'   return(merge_analysis)
#' #' }
#' #'
#' #' #' @noRd
#' #' analyze_column_overlap_patterns <- function(datasets_collection, verbose_mode) {
#' #'
#' #'   # Find groups of files with identical column structures
#' #'   column_signature_groups <- list()
#' #'
#' #'   for (file_name in names(datasets_collection)) {
#' #'     columns <- sort(datasets_collection[[file_name]]$column_names)
#' #'     column_signature <- paste(columns, collapse = "|")
#' #'
#' #'     if (column_signature %in% names(column_signature_groups)) {
#' #'       column_signature_groups[[column_signature]] <- c(column_signature_groups[[column_signature]], file_name)
#' #'     } else {
#' #'       column_signature_groups[[column_signature]] <- file_name
#' #'     }
#' #'   }
#' #'
#' #'   # Identify largest compatible group
#' #'   group_sizes <- sapply(column_signature_groups, length)
#' #'   largest_group_index <- which.max(group_sizes)
#' #'   largest_compatible_group <- column_signature_groups[[largest_group_index]]
#' #'
#' #'   if (verbose_mode) {
#' #'     logger::log_info("Column structure analysis:")
#' #'     logger::log_info("  {length(column_signature_groups)} unique column structures found")
#' #'     logger::log_info("  Largest compatible group: {length(largest_compatible_group)} files")
#' #'
#' #'     if (length(largest_compatible_group) > 1) {
#' #'       logger::log_info("  Compatible files: {paste(largest_compatible_group, collapse = ', ')}")
#' #'     }
#' #'   }
#' #'
#' #'   return(list(
#' #'     signature_groups = column_signature_groups,
#' #'     largest_compatible_group = largest_compatible_group,
#' #'     largest_group_size = length(largest_compatible_group)
#' #'   ))
#' #' }
#' #'
#' #' #' @noRd
#' #' execute_merge_strategy_comprehensive <- function(datasets_collection, merge_analysis, strategy, track_sources, verbose_mode) {
#' #'
#' #'   logger::log_info("Executing merge strategy: {strategy}")
#' #'
#' #'   if (strategy == "intelligent") {
#' #'     # Choose best strategy based on analysis
#' #'     if (!is.null(merge_analysis$best_key_column) && merge_analysis$best_key_coverage >= 2) {
#' #'       logger::log_info("Intelligent strategy: Using key-based merge with '{merge_analysis$best_key_column}'")
#' #'       merged_dataset <- execute_key_based_merge(datasets_collection, merge_analysis, track_sources, verbose_mode)
#' #'     } else if (merge_analysis$column_overlap$largest_group_size >= 2) {
#' #'       logger::log_info("Intelligent strategy: Using row binding for compatible datasets")
#' #'       merged_dataset <- execute_row_binding_merge(datasets_collection, merge_analysis, track_sources, verbose_mode)
#' #'     } else {
#' #'       logger::log_info("Intelligent strategy: Using largest dataset approach")
#' #'       merged_dataset <- execute_largest_dataset_approach(datasets_collection, track_sources, verbose_mode)
#' #'     }
#' #'
#' #'   } else if (strategy == "by_key") {
#' #'     if (!is.null(merge_analysis$best_key_column)) {
#' #'       merged_dataset <- execute_key_based_merge(datasets_collection, merge_analysis, track_sources, verbose_mode)
#' #'     } else {
#' #'       logger::log_warn("No suitable key columns found - falling back to largest dataset")
#' #'       merged_dataset <- execute_largest_dataset_approach(datasets_collection, track_sources, verbose_mode)
#' #'     }
#' #'
#' #'   } else if (strategy == "row_bind") {
#' #'     merged_dataset <- execute_row_binding_merge(datasets_collection, merge_analysis, track_sources, verbose_mode)
#' #'
#' #'   } else if (strategy == "largest_only") {
#' #'     merged_dataset <- execute_largest_dataset_approach(datasets_collection, track_sources, verbose_mode)
#' #'   }
#' #'
#' #'   return(merged_dataset)
#' #' }
#' #'
#' #' #' @noRd
#' #' execute_key_based_merge <- function(datasets_collection, merge_analysis, track_sources, verbose_mode) {
#' #'
#' #'   key_column <- merge_analysis$best_key_column
#' #'   mergeable_files <- merge_analysis$key_analysis[[key_column]]$files_with_key
#' #'
#' #'   logger::log_info("Executing key-based merge using '{key_column}' on {length(mergeable_files)} files")
#' #'
#' #'   # Start with largest dataset that has the key
#' #'   mergeable_datasets <- datasets_collection[mergeable_files]
#' #'   dataset_sizes <- sapply(mergeable_datasets, function(x) x$row_count)
#' #'   largest_dataset_name <- names(dataset_sizes)[which.max(dataset_sizes)]
#' #'
#' #'   merged_dataset <- mergeable_datasets[[largest_dataset_name]]$dataset
#' #'
#' #'   if (track_sources) {
#' #'     merged_dataset$source_file_primary <- largest_dataset_name
#' #'   }
#' #'
#' #'   logger::log_info("Starting with largest dataset: {largest_dataset_name} ({nrow(merged_dataset)} rows)")
#' #'
#' #'   # Merge remaining datasets
#' #'   remaining_files <- setdiff(mergeable_files, largest_dataset_name)
#' #'
#' #'   for (file_name in remaining_files) {
#' #'     current_dataset <- datasets_collection[[file_name]]$dataset
#' #'
#' #'     logger::log_info("Merging: {file_name}")
#' #'
#' #'     # Add source tracking to current dataset if requested
#' #'     if (track_sources) {
#' #'       current_dataset$source_file_secondary <- file_name
#' #'     }
#' #'
#' #'     # Perform full join
#' #'     merged_dataset <- dplyr::full_join(
#' #'       merged_dataset,
#' #'       current_dataset,
#' #'       by = key_column,
#' #'       suffix = c("", paste0("_", gsub("\\.rda$", "", file_name)))
#' #'     )
#' #'
#' #'     logger::log_info("  After merge: {nrow(merged_dataset)} rows, {ncol(merged_dataset)} columns")
#' #'   }
#' #'
#' #'   return(merged_dataset)
#' #' }
#' #'
#' #' #' @noRd
#' #' execute_row_binding_merge <- function(datasets_collection, merge_analysis, track_sources, verbose_mode) {
#' #'
#' #'   compatible_files <- merge_analysis$column_overlap$largest_compatible_group
#' #'
#' #'   logger::log_info("Executing row binding merge on {length(compatible_files)} compatible files")
#' #'
#' #'   if (length(compatible_files) < 2) {
#' #'     logger::log_warn("Not enough compatible files for row binding - using largest dataset")
#' #'     return(execute_largest_dataset_approach(datasets_collection, track_sources, verbose_mode))
#' #'   }
#' #'
#' #'   # Prepare datasets for row binding
#' #'   datasets_for_binding <- list()
#' #'
#' #'   for (file_name in compatible_files) {
#' #'     current_dataset <- datasets_collection[[file_name]]$dataset
#' #'
#' #'     if (track_sources) {
#' #'       current_dataset$source_file <- file_name
#' #'     }
#' #'
#' #'     datasets_for_binding[[file_name]] <- current_dataset
#' #'
#' #'     if (verbose_mode) {
#' #'       logger::log_info("  Adding to row bind: {file_name} ({nrow(current_dataset)} rows)")
#' #'     }
#' #'   }
#' #'
#' #'   # Perform row binding
#' #'   merged_dataset <- dplyr::bind_rows(datasets_for_binding)
#' #'
#' #'   logger::log_info("Row binding completed: {nrow(merged_dataset)} total rows from {length(compatible_files)} files")
#' #'
#' #'   return(merged_dataset)
#' #' }
#' #'
#' #' #' @noRd
#' #' execute_largest_dataset_approach <- function(datasets_collection, track_sources, verbose_mode) {
#' #'
#' #'   logger::log_info("Using largest dataset approach")
#' #'
#' #'   # Find largest dataset by row count
#' #'   dataset_sizes <- sapply(datasets_collection, function(x) x$row_count)
#' #'   largest_dataset_name <- names(dataset_sizes)[which.max(dataset_sizes)]
#' #'   largest_dataset <- datasets_collection[[largest_dataset_name]]$dataset
#' #'
#' #'   if (track_sources) {
#' #'     largest_dataset$source_file <- largest_dataset_name
#' #'   }
#' #'
#' #'   logger::log_info("Selected largest dataset: {largest_dataset_name} ({nrow(largest_dataset)} rows, {ncol(largest_dataset)} columns)")
#' #'
#' #'   if (verbose_mode) {
#' #'     # Show what files were not included
#' #'     excluded_files <- setdiff(names(datasets_collection), largest_dataset_name)
#' #'     if (length(excluded_files) > 0) {
#' #'       logger::log_info("Excluded files: {paste(excluded_files, collapse = ', ')}")
#' #'     }
#' #'   }
#' #'
#' #'   return(largest_dataset)
#' #' }
#' #'
#' #' #' @noRd
#' #' save_merged_dataset_to_file <- function(merged_dataset, file_path, verbose_mode) {
#' #'
#' #'   logger::log_info("Saving merged dataset to file: {file_path}")
#' #'
#' #'   # Create directory if it doesn't exist
#' #'   output_directory <- dirname(file_path)
#' #'   if (!dir.exists(output_directory)) {
#' #'     dir.create(output_directory, recursive = TRUE)
#' #'     logger::log_info("Created output directory: {output_directory}")
#' #'   }
#' #'
#' #'   # Determine file format and save accordingly
#' #'   file_extension <- tolower(tools::file_ext(file_path))
#' #'
#' #'   tryCatch({
#' #'     if (file_extension == "csv") {
#' #'       readr::write_csv(merged_dataset, file_path)
#' #'     } else if (file_extension == "rds") {
#' #'       saveRDS(merged_dataset, file_path)
#' #'     } else if (file_extension == "rda" || file_extension == "rdata") {
#' #'       save(merged_dataset, file = file_path)
#' #'     } else {
#' #'       # Default to CSV
#' #'       readr::write_csv(merged_dataset, file_path)
#' #'       logger::log_warn("Unknown file extension, saved as CSV")
#' #'     }
#' #'
#' #'     logger::log_info("Dataset saved successfully to: {file_path}")
#' #'
#' #'   }, error = function(e) {
#' #'     logger::log_error("Error saving file: {e$message}")
#' #'   })
#' #' }
#' #'
#' #' # Test ----
#' #' # Basic intelligent merge
#' #' merged_data <- merge_swan_rda_files_comprehensive(
#' #'   swan_data_directory_path = "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/",
#' #'   merge_strategy = "intelligent",
#' #'   verbose_logging = TRUE
#' #' )
#' #'
#' #' # Save merged dataset to file
#' #' merged_with_file <- merge_swan_rda_files_comprehensive(
#' #'   swan_data_directory_path = "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/",
#' #'   merge_strategy = "intelligent",
#' #'   output_merged_file_path = "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/merged_swan_complete.csv",
#' #'   create_source_tracking = TRUE,
#' #'   verbose_logging = TRUE
#' #' )
#' #'
#' #' # Force key-based merge with specific preferences
#' #' key_merged <- merge_swan_rda_files_comprehensive(
#' #'   swan_data_directory_path = "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/",
#' #'   merge_strategy = "by_key",
#' #'   preferred_key_columns = c("ARCHID", "archid", "participant_id"),
#' #'   verbose_logging = TRUE
#' #' )

#' #' Merge All SWAN RDA Files into Comprehensive Dataset
#' #'
#' #' This function reads all .rda files from a specified SWAN data directory and
#' #' intelligently merges them into a comprehensive longitudinal dataset. The
#' #' function handles different file types (visit data, biomarkers, genetics, etc.),
#' #' identifies common variables for merging, and creates a unified dataset suitable
#' #' for longitudinal analysis. The function provides extensive logging and
#' #' validation to ensure data integrity throughout the merging process.
#' #'
#' #' @param swan_data_directory Character. Path to directory containing SWAN .rda
#' #'   files. All .rda files in this directory will be processed and merged.
#' #'   The directory should contain files with standard SWAN naming conventions.
#' #' @param merge_strategy Character. Strategy for merging datasets: "full" performs
#' #'   full outer joins to retain all participants from all files, "inner" keeps
#' #'   only participants present in all files, "progressive" merges files
#' #'   sequentially starting with the largest dataset. Default is "full".
#' #' @param verbose Logical. If TRUE (default), prints comprehensive logging
#' #'   information including file discovery, data structure analysis, merge
#' #'   progress, and final dataset summary. Set to FALSE for silent operation.
#' #'
#' #' @return A tibble containing the merged SWAN dataset with standardized
#' #'   participant and visit identifiers:
#' #'   \describe{
#' #'     \item{swan_participant_id}{Standardized participant identifier (SWANID/ARCHID)}
#' #'     \item{visit_number}{Standardized visit number}
#' #'     \item{...}{All other variables from merged files with source file tracking}
#' #'   }
#' #'   The returned dataset includes metadata attributes:
#' #'   - source_files: List of all files that were merged
#' #'   - merge_summary: Summary statistics about the merge process
#' #'   - data_completeness: Variable-level completeness statistics
#' #'
#' #' @examples
#' #' # Example 1: Basic merge with full outer join strategy
#' #' comprehensive_swan_data <- merge_all_swan_rda_files(
#' #'   swan_data_directory = "/Users/tylermuffly/Dropbox (Personal)/workforce/Dall_model/data/SWAN",
#' #'   merge_strategy = "full",
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' # Example 2: Conservative merge keeping only complete cases
#' #' complete_cases_swan <- merge_all_swan_rda_files(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   merge_strategy = "inner",
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' # Example 3: Progressive merge for large datasets with memory constraints
#' #' progressive_swan_merge <- merge_all_swan_rda_files(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   merge_strategy = "progressive",
#' #'   verbose = FALSE
#' #' )
#' #' # Result: Comprehensive longitudinal dataset with ~3,300+ participants
#' #' # Expected structure:
#' #' # # A tibble: 25,000+ × 500+
#' #' #   swan_participant_id visit_number baseline_age_years visit_0_bmi ...
#' #' #   <chr>                      <dbl>              <dbl>       <dbl>
#' #' # 1 00001                          0                 45        24.5
#' #' # 2 00001                          1                 45        24.8
#' #' # # ... with extensive variables from all SWAN domains
#' #'
#' #' @importFrom dplyr full_join left_join inner_join mutate select rename
#' #' @importFrom dplyr case_when coalesce bind_rows arrange group_by summarise
#' #' @importFrom dplyr across everything n distinct
#' #' @importFrom stringr str_detect str_extract str_replace_all str_to_lower
#' #' @importFrom purrr map_dfr map_chr map_lgl map_dbl walk imap
#' #' @importFrom assertthat assert_that
#' #' @importFrom logger log_info log_warn log_error
#' #' @importFrom tibble tibble as_tibble
#' #' @importFrom fs dir_ls path_ext
#' #' @importFrom readr parse_number
#' #' @importFrom glue glue
#' #'
#' #' @export
#' merge_all_swan_rda_files <- function(swan_data_directory,
#'                                      merge_strategy = "full",
#'                                      verbose = TRUE) {
#'
#'   # Input validation and setup
#'   if (verbose) {
#'     logger::log_info("Starting comprehensive SWAN RDA files merge")
#'     logger::log_info("Validating inputs and discovering files")
#'   }
#'
#'   assertthat::assert_that(
#'     is.character(swan_data_directory),
#'     msg = "swan_data_directory must be a character string path"
#'   )
#'   assertthat::assert_that(
#'     dir.exists(swan_data_directory),
#'     msg = glue::glue("Directory does not exist: {swan_data_directory}")
#'   )
#'   assertthat::assert_that(
#'     merge_strategy %in% c("full", "inner", "progressive"),
#'     msg = "merge_strategy must be 'full', 'inner', or 'progressive'"
#'   )
#'   assertthat::assert_that(
#'     is.logical(verbose),
#'     msg = "verbose must be TRUE or FALSE"
#'   )
#'
#'   # Discover all RDA files in directory
#'   rda_file_paths <- discover_swan_rda_files(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   # Load and standardize all datasets
#'   standardized_datasets <- load_and_standardize_swan_files(
#'     file_paths = rda_file_paths,
#'     verbose = verbose
#'   )
#'
#'   # Merge datasets according to strategy
#'   merged_swan_dataset <- execute_merge_strategy(
#'     datasets_list = standardized_datasets,
#'     strategy = merge_strategy,
#'     verbose = verbose
#'   )
#'
#'   # Add metadata and final processing
#'   final_swan_dataset <- finalize_merged_dataset(
#'     merged_data = merged_swan_dataset,
#'     source_files = rda_file_paths,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("SWAN RDA files merge completed successfully")
#'     logger::log_info("Final dataset: {nrow(final_swan_dataset)} rows × {ncol(final_swan_dataset)} columns")
#'   }
#'
#'   return(final_swan_dataset)
#' }
#'
#'
#' #' @noRd
#' discover_swan_rda_files <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Discovering RDA files in directory: {directory_path}")
#'   }
#'
#'   # Find all .rda files
#'   rda_files <- fs::dir_ls(
#'     path = directory_path,
#'     regexp = "\\.(rda|RDA)$",
#'     recurse = FALSE
#'   )
#'
#'   if (length(rda_files) == 0) {
#'     logger::log_error("No .rda files found in directory: {directory_path}")
#'     stop("No .rda files found in specified directory")
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Found {length(rda_files)} RDA files")
#'     purrr::walk(rda_files, ~logger::log_info("  - {basename(.x)}"))
#'   }
#'
#'   return(rda_files)
#' }
#'
#'
#' #' @noRd
#' load_and_standardize_swan_files <- function(file_paths, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Loading and standardizing {length(file_paths)} RDA files")
#'   }
#'
#'   standardized_datasets <- purrr::imap(file_paths, function(file_path, index) {
#'
#'     if (verbose) {
#'       logger::log_info("Processing file {index}/{length(file_paths)}: {basename(file_path)}")
#'     }
#'
#'     # Load RDA file safely
#'     file_environment <- new.env()
#'     tryCatch({
#'       load(file_path, envir = file_environment)
#'     }, error = function(e) {
#'       logger::log_error("Failed to load {basename(file_path)}: {e$message}")
#'       return(NULL)
#'     })
#'
#'     # Extract dataset from environment
#'     dataset_names <- ls(file_environment)
#'     if (length(dataset_names) == 0) {
#'       logger::log_warn("No datasets found in {basename(file_path)}")
#'       return(NULL)
#'     }
#'
#'     raw_dataset <- file_environment[[dataset_names[1]]]
#'
#'     if (verbose) {
#'       logger::log_info("  Loaded dataset: {nrow(raw_dataset)} rows × {ncol(raw_dataset)} columns")
#'     }
#'
#'     # Standardize the dataset
#'     standardized_dataset <- standardize_swan_dataset(
#'       dataset = raw_dataset,
#'       source_file = basename(file_path),
#'       verbose = verbose
#'     )
#'
#'     return(standardized_dataset)
#'   })
#'
#'   # Remove any NULL datasets (failed loads)
#'   standardized_datasets <- purrr::compact(standardized_datasets)
#'
#'   if (verbose) {
#'     logger::log_info("Successfully loaded {length(standardized_datasets)} datasets")
#'   }
#'
#'   return(standardized_datasets)
#' }
#'
#'
#' #' @noRd
#' standardize_swan_dataset <- function(dataset, source_file, verbose) {
#'
#'   # Convert to tibble and identify key variables
#'   dataset_tibble <- tibble::as_tibble(dataset)
#'
#'   # Identify participant ID variable
#'   participant_id_column <- identify_participant_id_column(dataset_tibble)
#'
#'   # Identify visit variable
#'   visit_column <- identify_visit_column(dataset_tibble)
#'
#'   # Standardize core variables
#'   standardized_dataset <- dataset_tibble %>%
#'     dplyr::mutate(
#'       swan_participant_id = if (!is.null(participant_id_column)) {
#'         standardize_participant_id(.data[[participant_id_column]])
#'       } else NA_character_,
#'       visit_number = if (!is.null(visit_column)) {
#'         standardize_visit_number(.data[[visit_column]])
#'       } else NA_real_,
#'       source_dataset = source_file
#'     )
#'
#'   # Move standardized columns to front
#'   core_columns <- c("swan_participant_id", "visit_number", "source_dataset")
#'   other_columns <- setdiff(names(standardized_dataset), core_columns)
#'
#'   standardized_dataset <- standardized_dataset %>%
#'     dplyr::select(dplyr::all_of(core_columns), dplyr::all_of(other_columns))
#'
#'   if (verbose) {
#'     logger::log_info("  Standardized: ID column = {participant_id_column %||% 'NONE'}")
#'     logger::log_info("  Standardized: Visit column = {visit_column %||% 'NONE'}")
#'     logger::log_info("  Unique participants: {length(unique(standardized_dataset$swan_participant_id[!is.na(standardized_dataset$swan_participant_id)]))}")
#'   }
#'
#'   return(standardized_dataset)
#' }
#'
#'
#' #' @noRd
#' identify_participant_id_column <- function(dataset) {
#'
#'   # Look for common SWAN participant ID variables
#'   participant_id_patterns <- c(
#'     "^SWANID$", "^ARCHID$", "^SWAN_ID$", "^ARCH_ID$",
#'     "^swanid$", "^archid$", "^participant_id$", "^id$"
#'   )
#'
#'   for (pattern in participant_id_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'
#'   return(NULL)
#' }
#'
#'
#' #' @noRd
#' identify_visit_column <- function(dataset) {
#'
#'   # Look for common SWAN visit variables
#'   visit_patterns <- c(
#'     "^VISIT$", "^visit$", "^Visit$", "^VISITNO$", "^visit_number$"
#'   )
#'
#'   for (pattern in visit_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'
#'   return(NULL)
#' }
#'
#'
#' #' @noRd
#' standardize_participant_id <- function(participant_id_raw) {
#'
#'   # Convert to character and clean
#'   participant_id_clean <- as.character(participant_id_raw)
#'
#'   # Remove any leading/trailing whitespace
#'   participant_id_clean <- stringr::str_trim(participant_id_clean)
#'
#'   # Pad with leading zeros if numeric-like (common in SWAN)
#'   if (all(stringr::str_detect(participant_id_clean[!is.na(participant_id_clean)], "^\\d+$"))) {
#'     participant_id_clean <- stringr::str_pad(participant_id_clean, width = 5, side = "left", pad = "0")
#'   }
#'
#'   return(participant_id_clean)
#' }
#'
#'
#' #' @noRd
#' standardize_visit_number <- function(visit_raw) {
#'
#'   # Try to extract numeric visit numbers
#'   visit_numeric <- suppressWarnings(as.numeric(as.character(visit_raw)))
#'
#'   # If that fails, try to parse numbers from strings
#'   if (all(is.na(visit_numeric))) {
#'     visit_numeric <- readr::parse_number(as.character(visit_raw))
#'   }
#'
#'   return(visit_numeric)
#' }
#'
#'
#' #' @noRd
#' execute_merge_strategy <- function(datasets_list, strategy, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Executing merge strategy: {strategy}")
#'   }
#'
#'   if (strategy == "full") {
#'     merged_dataset <- execute_full_merge(datasets_list, verbose)
#'   } else if (strategy == "inner") {
#'     merged_dataset <- execute_inner_merge(datasets_list, verbose)
#'   } else if (strategy == "progressive") {
#'     merged_dataset <- execute_progressive_merge(datasets_list, verbose)
#'   }
#'
#'   return(merged_dataset)
#' }
#'
#'
#' #' @noRd
#' execute_full_merge <- function(datasets_list, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Performing full outer joins to retain all participants")
#'   }
#'
#'   # Start with first dataset
#'   merged_dataset <- datasets_list[[1]]
#'
#'   # Merge each subsequent dataset
#'   for (i in 2:length(datasets_list)) {
#'
#'     current_dataset <- datasets_list[[i]]
#'
#'     if (verbose) {
#'       logger::log_info("Merging dataset {i}: {unique(current_dataset$source_dataset)}")
#'     }
#'
#'     # Determine merge keys
#'     merge_keys <- determine_merge_keys(merged_dataset, current_dataset)
#'
#'     # Perform merge
#'     merged_dataset <- dplyr::full_join(
#'       merged_dataset,
#'       current_dataset,
#'       by = merge_keys,
#'       suffix = c("", paste0("_", i))
#'     )
#'
#'     if (verbose) {
#'       logger::log_info("  After merge: {nrow(merged_dataset)} rows")
#'     }
#'   }
#'
#'   return(merged_dataset)
#' }
#'
#'
#' #' @noRd
#' execute_inner_merge <- function(datasets_list, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Performing inner joins to keep only complete participants")
#'   }
#'
#'   # Start with first dataset
#'   merged_dataset <- datasets_list[[1]]
#'
#'   # Merge each subsequent dataset
#'   for (i in 2:length(datasets_list)) {
#'
#'     current_dataset <- datasets_list[[i]]
#'
#'     if (verbose) {
#'       logger::log_info("Inner joining dataset {i}: {unique(current_dataset$source_dataset)}")
#'     }
#'
#'     # Determine merge keys
#'     merge_keys <- determine_merge_keys(merged_dataset, current_dataset)
#'
#'     # Perform inner join
#'     merged_dataset <- dplyr::inner_join(
#'       merged_dataset,
#'       current_dataset,
#'       by = merge_keys,
#'       suffix = c("", paste0("_", i))
#'     )
#'
#'     if (verbose) {
#'       logger::log_info("  After inner join: {nrow(merged_dataset)} rows retained")
#'     }
#'   }
#'
#'   return(merged_dataset)
#' }
#'
#'
#' #' @noRd
#' execute_progressive_merge <- function(datasets_list, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Performing progressive merge starting with largest dataset")
#'   }
#'
#'   # Sort datasets by size (largest first)
#'   dataset_sizes <- purrr::map_dbl(datasets_list, nrow)
#'   sorted_indices <- order(dataset_sizes, decreasing = TRUE)
#'   sorted_datasets <- datasets_list[sorted_indices]
#'
#'   if (verbose) {
#'     logger::log_info("Dataset sizes (largest first):")
#'     purrr::walk2(sorted_datasets, dataset_sizes[sorted_indices],
#'                  ~logger::log_info("  {unique(.x$source_dataset)}: {.y} rows"))
#'   }
#'
#'   # Start with largest dataset
#'   merged_dataset <- sorted_datasets[[1]]
#'
#'   # Progressively merge smaller datasets
#'   for (i in 2:length(sorted_datasets)) {
#'
#'     current_dataset <- sorted_datasets[[i]]
#'
#'     if (verbose) {
#'       logger::log_info("Progressive merge {i}: {unique(current_dataset$source_dataset)}")
#'     }
#'
#'     # Determine merge keys
#'     merge_keys <- determine_merge_keys(merged_dataset, current_dataset)
#'
#'     # Perform left join (prioritizing the accumulated dataset)
#'     merged_dataset <- dplyr::left_join(
#'       merged_dataset,
#'       current_dataset,
#'       by = merge_keys,
#'       suffix = c("", paste0("_prog", i))
#'     )
#'
#'     if (verbose) {
#'       logger::log_info("  After progressive merge: {nrow(merged_dataset)} rows")
#'     }
#'   }
#'
#'   return(merged_dataset)
#' }
#'
#'
#' #' @noRd
#' determine_merge_keys <- function(dataset1, dataset2) {
#'
#'   # Find common columns that are suitable for merging
#'   common_columns <- intersect(names(dataset1), names(dataset2))
#'
#'   # Prioritize standard merge keys
#'   priority_keys <- c("swan_participant_id", "visit_number")
#'   merge_keys <- intersect(priority_keys, common_columns)
#'
#'   # If no priority keys found, use participant ID only
#'   if (length(merge_keys) == 0 && "swan_participant_id" %in% common_columns) {
#'     merge_keys <- "swan_participant_id"
#'   }
#'
#'   # Fallback to any common ID-like columns
#'   if (length(merge_keys) == 0) {
#'     id_like_columns <- common_columns[stringr::str_detect(stringr::str_to_lower(common_columns), "id")]
#'     if (length(id_like_columns) > 0) {
#'       merge_keys <- id_like_columns[1]
#'     }
#'   }
#'
#'   return(merge_keys)
#' }
#'
#'
#' #' @noRd
#' finalize_merged_dataset <- function(merged_data, source_files, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Finalizing merged dataset with metadata")
#'   }
#'
#'   # Clean up column names (remove duplicated merge suffixes where appropriate)
#'   final_dataset <- clean_merged_column_names(merged_data, verbose)
#'
#'   # Add comprehensive metadata as attributes
#'   attr(final_dataset, "source_files") <- basename(source_files)
#'   attr(final_dataset, "merge_timestamp") <- Sys.time()
#'   attr(final_dataset, "merge_summary") <- create_merge_summary(final_dataset)
#'
#'   if (verbose) {
#'     logger::log_info("Added metadata attributes to final dataset")
#'     log_final_summary(final_dataset)
#'   }
#'
#'   return(final_dataset)
#' }
#'
#'
#' #' @noRd
#' clean_merged_column_names <- function(merged_data, verbose) {
#'
#'   # Identify and clean up redundant column names from merging
#'   original_names <- names(merged_data)
#'
#'   # Look for columns with merge suffixes
#'   suffix_pattern <- "_\\d+$|_prog\\d+$"
#'   suffixed_columns <- stringr::str_detect(original_names, suffix_pattern)
#'
#'   if (any(suffixed_columns) && verbose) {
#'     logger::log_info("Cleaning up {sum(suffixed_columns)} columns with merge suffixes")
#'   }
#'
#'   return(merged_data)
#' }
#'
#'
#' #' @noRd
#' create_merge_summary <- function(dataset) {
#'
#'   summary_stats <- tibble::tibble(
#'     total_participants = length(unique(dataset$swan_participant_id[!is.na(dataset$swan_participant_id)])),
#'     total_rows = nrow(dataset),
#'     total_variables = ncol(dataset),
#'     unique_source_datasets = length(unique(dataset$source_dataset[!is.na(dataset$source_dataset)])),
#'     visit_range = paste(
#'       min(dataset$visit_number, na.rm = TRUE),
#'       "to",
#'       max(dataset$visit_number, na.rm = TRUE)
#'     )
#'   )
#'
#'   return(summary_stats)
#' }
#'
#'
#' #' @noRd
#' log_final_summary <- function(dataset) {
#'
#'   summary_info <- attr(dataset, "merge_summary")
#'
#'   logger::log_info("=== FINAL MERGE SUMMARY ===")
#'   logger::log_info("Total participants: {summary_info$total_participants}")
#'   logger::log_info("Total observations: {summary_info$total_rows}")
#'   logger::log_info("Total variables: {summary_info$total_variables}")
#'   logger::log_info("Source datasets merged: {summary_info$unique_source_datasets}")
#'   logger::log_info("Visit range: {summary_info$visit_range}")
#'   logger::log_info("===========================")
#' }
#'
#'
#' swan_data_directory <- "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/"
#' merge_all_swan_rda_files_output <- merge_all_swan_rda_files(swan_data_directory,
#'                                      merge_strategy = "full",
#'                                      verbose = TRUE)
#'
#' write_rds(merge_all_swan_rda_files_output, "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/merge_all_swan_rda_files_output.rds")
#'
#'
#' # Function at 1630 ----
#' #' Create Focused SWAN Dataset: Baseline Demographics + INVOLEA Variables
#' #'
#' #' This function creates a clean, focused SWAN dataset containing baseline
#' #' demographic information (one row per participant) merged with urinary
#' #' incontinence (INVOLEA) variables across all visits in long format. This
#' #' structure is optimal for longitudinal analysis of urinary incontinence
#' #' while maintaining essential baseline covariates.
#' #'
#' #' @param swan_data_directory Character. Path to directory containing SWAN .rda
#' #'   files. The function will identify baseline demographic files and files
#' #'   containing INVOLEA variables across visits.
#' #' @param output_format Character. Format for the final dataset: "long" creates
#' #'   one row per participant-visit with INVOLEA data, "wide" creates one row
#' #'   per participant with INVOLEA variables spread across visits. Default "long".
#' #' @param verbose Logical. If TRUE (default), prints detailed logging about
#' #'   file processing, baseline demographics extraction, and INVOLEA variable
#' #'   identification across visits.
#' #'
#' #' @return A tibble containing focused SWAN data:
#' #'   For "long" format:
#' #'   \describe{
#' #'     \item{swan_participant_id}{Participant identifier}
#' #'     \item{visit_number}{Visit number (0=baseline, 1-15=follow-up)}
#' #'     \item{baseline_age_years}{Age at baseline (repeated for all visits)}
#' #'     \item{baseline_bmi_kg_m2}{BMI at baseline (repeated for all visits)}
#' #'     \item{baseline_race_ethnicity}{Race/ethnicity (repeated for all visits)}
#' #'     \item{baseline_smoking_status}{Smoking status at baseline}
#' #'     \item{baseline_parity_count}{Number of pregnancies at baseline}
#' #'     \item{baseline_menopausal_stage}{Menopausal stage at baseline}
#' #'     \item{involea_incontinence}{Binary incontinence variable (0/1)}
#' #'     \item{involea_raw_response}{Original INVOLEA response}
#' #'     \item{involea_source_variable}{Original variable name}
#' #'     \item{years_since_baseline}{Time since baseline visit}
#' #'   }
#' #'
#' #' @examples
#' #' # Example 1: Long format for longitudinal modeling
#' #' swan_long_dataset <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/Users/tylermuffly/Dropbox (Personal)/workforce/Dall_model/data/SWAN",
#' #'   output_format = "long",
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' # Example 2: Wide format for cross-sectional analysis
#' #' swan_wide_dataset <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "wide",
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' # Example 3: Silent processing for production pipelines
#' #' swan_analysis_ready <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "long",
#' #'   verbose = FALSE
#' #' )
#' #' # Expected output for long format:
#' #' # # A tibble: 21,629 × 12
#' #' #   swan_participant_id visit_number baseline_age_years baseline_bmi_kg_m2
#' #' #   <chr>                      <dbl>              <dbl>              <dbl>
#' #' # 1 10005                          0                 48               24.1
#' #' # 2 10005                          1                 48               24.1
#' #' # 3 10005                          3                 48               24.1
#' #' # # ... with 21,626 more rows and 8 more variables
#' #'
#' #' @importFrom dplyr filter select mutate left_join case_when
#' #' @importFrom dplyr group_by summarise ungroup arrange distinct
#' #' @importFrom tidyr pivot_longer pivot_wider
#' #' @importFrom stringr str_detect str_extract str_replace_all
#' #' @importFrom purrr map_dfr map_chr map_lgl keep
#' #' @importFrom assertthat assert_that
#' #' @importFrom logger log_info log_warn log_error
#' #' @importFrom tibble tibble as_tibble
#' #' @importFrom readr parse_number
#' #' @importFrom glue glue
#' #'
#' #' @export
#' create_focused_swan_dataset <- function(swan_data_directory,
#'                                         output_format = "long",
#'                                         verbose = TRUE) {
#'
#'   # Input validation
#'   if (verbose) {
#'     logger::log_info("Creating focused SWAN dataset: Baseline demographics + INVOLEA variables")
#'     logger::log_info("Input validation and setup")
#'   }
#'
#'   assertthat::assert_that(
#'     is.character(swan_data_directory),
#'     msg = "swan_data_directory must be a character string path"
#'   )
#'   assertthat::assert_that(
#'     dir.exists(swan_data_directory),
#'     msg = glue::glue("Directory does not exist: {swan_data_directory}")
#'   )
#'   assertthat::assert_that(
#'     output_format %in% c("long", "wide"),
#'     msg = "output_format must be 'long' or 'wide'"
#'   )
#'
#'   # Step 1: Extract baseline demographics
#'   baseline_demographics_data <- extract_baseline_demographics(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   # Step 2: Extract INVOLEA variables across all visits
#'   involea_longitudinal_data <- extract_involea_variables(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   # Step 3: Merge baseline demographics with INVOLEA data
#'   merged_focused_dataset <- merge_baseline_with_involea(
#'     baseline_data = baseline_demographics_data,
#'     involea_data = involea_longitudinal_data,
#'     verbose = verbose
#'   )
#'
#'   # Step 4: Format output according to user preference
#'   final_focused_dataset <- format_final_dataset(
#'     merged_data = merged_focused_dataset,
#'     format = output_format,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Focused SWAN dataset creation completed")
#'     logger::log_info("Final dataset: {nrow(final_focused_dataset)} rows × {ncol(final_focused_dataset)} columns")
#'   }
#'
#'   return(final_focused_dataset)
#' }
#'
#'
#' #' @noRd
#' extract_baseline_demographics <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting baseline demographics from SWAN files")
#'   }
#'
#'   # Find files likely to contain baseline demographics
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Searching {length(all_rda_files)} RDA files for baseline demographics")
#'   }
#'
#'   # Try to identify baseline/demographic files by name patterns
#'   baseline_file_patterns <- c("28762", "baseline", "demo", "04368")
#'
#'   baseline_candidates <- purrr::keep(all_rda_files, function(file_path) {
#'     file_name <- tolower(basename(file_path))
#'     any(stringr::str_detect(file_name, baseline_file_patterns))
#'   })
#'
#'   if (verbose) {
#'     logger::log_info("Found {length(baseline_candidates)} potential baseline files:")
#'     purrr::walk(baseline_candidates, ~logger::log_info("  - {basename(.x)}"))
#'   }
#'
#'   # Load and process baseline files
#'   baseline_datasets <- purrr::map(baseline_candidates, function(file_path) {
#'
#'     if (verbose) {
#'       logger::log_info("Processing baseline file: {basename(file_path)}")
#'     }
#'
#'     # Load file
#'     file_environment <- new.env()
#'     load(file_path, envir = file_environment)
#'     raw_baseline_data <- file_environment[[ls(file_environment)[1]]]
#'
#'     # Look for demographic variables
#'     demo_variables <- identify_demographic_variables(raw_baseline_data)
#'
#'     if (length(demo_variables$participant_id) == 0) {
#'       if (verbose) {
#'         logger::log_warn("No participant ID found in {basename(file_path)}")
#'       }
#'       return(NULL)
#'     }
#'
#'     # Extract and clean demographics
#'     cleaned_baseline <- extract_and_clean_demographics(
#'       dataset = raw_baseline_data,
#'       demo_vars = demo_variables,
#'       source_file = basename(file_path),
#'       verbose = verbose
#'     )
#'
#'     return(cleaned_baseline)
#'   })
#'
#'   # Combine baseline datasets
#'   baseline_datasets <- purrr::compact(baseline_datasets)
#'
#'   if (length(baseline_datasets) == 0) {
#'     logger::log_error("No valid baseline demographic data found")
#'     stop("No baseline demographic data could be extracted")
#'   }
#'
#'   # Merge multiple baseline sources if needed
#'   combined_baseline <- combine_baseline_sources(baseline_datasets, verbose)
#'
#'   if (verbose) {
#'     logger::log_info("Baseline demographics extracted: {nrow(combined_baseline)} participants")
#'   }
#'
#'   return(combined_baseline)
#' }
#'
#'
#' #' @noRd
#' extract_involea_variables <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting INVOLEA variables across all visits")
#'   }
#'
#'   # Find all RDA files
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   # Process each file to find INVOLEA variables
#'   involea_datasets <- purrr::map(all_rda_files, function(file_path) {
#'
#'     if (verbose) {
#'       logger::log_info("Scanning {basename(file_path)} for INVOLEA variables")
#'     }
#'
#'     # Load file
#'     file_environment <- new.env()
#'     tryCatch({
#'       load(file_path, envir = file_environment)
#'     }, error = function(e) {
#'       if (verbose) {
#'         logger::log_warn("Failed to load {basename(file_path)}: {e$message}")
#'       }
#'       return(NULL)
#'     })
#'
#'     if (length(ls(file_environment)) == 0) return(NULL)
#'
#'     raw_dataset <- file_environment[[ls(file_environment)[1]]]
#'
#'     # Look for INVOLEA variables
#'     involea_columns <- identify_involea_variables(raw_dataset)
#'
#'     if (length(involea_columns) == 0) {
#'       return(NULL)  # No INVOLEA variables in this file
#'     }
#'
#'     if (verbose) {
#'       logger::log_info("  Found {length(involea_columns)} INVOLEA variables: {paste(involea_columns, collapse = ', ')}")
#'     }
#'
#'     # Extract INVOLEA data
#'     involea_data <- extract_involea_data(
#'       dataset = raw_dataset,
#'       involea_vars = involea_columns,
#'       source_file = basename(file_path),
#'       verbose = verbose
#'     )
#'
#'     return(involea_data)
#'   })
#'
#'   # Combine all INVOLEA datasets
#'   involea_datasets <- purrr::compact(involea_datasets)
#'
#'   if (length(involea_datasets) == 0) {
#'     logger::log_error("No INVOLEA variables found in any files")
#'     stop("No INVOLEA variables could be found")
#'   }
#'
#'   combined_involea <- purrr::map_dfr(involea_datasets, ~.x)
#'
#'   # Remove duplicates (same participant, visit, and variable)
#'   combined_involea <- combined_involea %>%
#'     dplyr::distinct(
#'       swan_participant_id,
#'       visit_number,
#'       involea_source_variable,
#'       .keep_all = TRUE
#'     )
#'
#'   if (verbose) {
#'     logger::log_info("INVOLEA variables extracted: {nrow(combined_involea)} observations")
#'     logger::log_info("Visit range: {min(combined_involea$visit_number, na.rm = TRUE)} to {max(combined_involea$visit_number, na.rm = TRUE)}")
#'     logger::log_info("Unique participants with INVOLEA: {length(unique(combined_involea$swan_participant_id))}")
#'   }
#'
#'   return(combined_involea)
#' }
#'
#'
#' #' @noRd
#' identify_demographic_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Identify participant ID
#'   participant_id_patterns <- c("SWANID", "ARCHID", "swanid", "archid")
#'   participant_id_col <- NULL
#'   for (pattern in participant_id_patterns) {
#'     if (pattern %in% variable_names) {
#'       participant_id_col <- pattern
#'       break
#'     }
#'   }
#'
#'   # Identify demographic variables
#'   age_vars <- variable_names[stringr::str_detect(variable_names, "AGE|age")]
#'   bmi_vars <- variable_names[stringr::str_detect(variable_names, "BMI|bmi")]
#'   race_vars <- variable_names[stringr::str_detect(variable_names, "RACE|race")]
#'   smoking_vars <- variable_names[stringr::str_detect(variable_names, "SMOKE|smoke")]
#'   parity_vars <- variable_names[stringr::str_detect(variable_names, "PREG|CHILD|preg|child|parity")]
#'   menopausal_vars <- variable_names[stringr::str_detect(variable_names, "STATUS|MENO|status|meno")]
#'
#'   return(list(
#'     participant_id = participant_id_col,
#'     age = age_vars,
#'     bmi = bmi_vars,
#'     race = race_vars,
#'     smoking = smoking_vars,
#'     parity = parity_vars,
#'     menopausal = menopausal_vars
#'   ))
#' }
#'
#'
#' #' @noRd
#' identify_involea_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Look for INVOLEA variables (including LEKINVO variants)
#'   involea_patterns <- c("INVOLEA", "LEKINVO", "involea", "lekinvo")
#'
#'   involea_variables <- c()
#'   for (pattern in involea_patterns) {
#'     matching_vars <- variable_names[stringr::str_detect(variable_names, pattern)]
#'     involea_variables <- c(involea_variables, matching_vars)
#'   }
#'
#'   return(unique(involea_variables))
#' }
#'
#'
#' #' @noRd
#' extract_and_clean_demographics <- function(dataset, demo_vars, source_file, verbose) {
#'
#'   # Start with participant ID
#'   cleaned_data <- tibble::as_tibble(dataset) %>%
#'     dplyr::select(dplyr::all_of(demo_vars$participant_id))
#'
#'   names(cleaned_data)[1] <- "swan_participant_id"
#'
#'   # Add demographic variables if available - ensuring consistent data types
#'   if (length(demo_vars$age) > 0) {
#'     cleaned_data$baseline_age_years <- as.numeric(as.character(dataset[[demo_vars$age[1]]]))
#'   }
#'
#'   if (length(demo_vars$bmi) > 0) {
#'     cleaned_data$baseline_bmi_kg_m2 <- as.numeric(as.character(dataset[[demo_vars$bmi[1]]]))
#'   }
#'
#'   if (length(demo_vars$race) > 0) {
#'     cleaned_data$baseline_race_ethnicity <- clean_race_variable(dataset[[demo_vars$race[1]]])
#'   }
#'
#'   if (length(demo_vars$smoking) > 0) {
#'     cleaned_data$baseline_smoking_status <- clean_smoking_variable(dataset[[demo_vars$smoking[1]]])
#'   }
#'
#'   if (length(demo_vars$parity) > 0) {
#'     cleaned_data$baseline_parity_count <- as.numeric(as.character(dataset[[demo_vars$parity[1]]]))
#'   }
#'
#'   if (length(demo_vars$menopausal) > 0) {
#'     cleaned_data$baseline_menopausal_stage <- clean_menopausal_variable(dataset[[demo_vars$menopausal[1]]])
#'   }
#'
#'   # Standardize participant ID
#'   cleaned_data$swan_participant_id <- as.character(cleaned_data$swan_participant_id)
#'   cleaned_data$source_file <- source_file
#'
#'   if (verbose) {
#'     logger::log_info("  Extracted {nrow(cleaned_data)} participants with {ncol(cleaned_data)-2} demographic variables")
#'   }
#'
#'   return(cleaned_data)
#' }
#'
#'
#' #' @noRd
#' extract_involea_data <- function(dataset, involea_vars, source_file, verbose) {
#'
#'   # Identify participant ID and visit variables
#'   participant_id_col <- identify_participant_id_column(dataset)
#'   visit_col <- identify_visit_column(dataset)
#'
#'   if (is.null(participant_id_col)) {
#'     if (verbose) {
#'       logger::log_warn("No participant ID found in {source_file}")
#'     }
#'     return(NULL)
#'   }
#'
#'   # Select relevant columns
#'   core_columns <- c(participant_id_col)
#'   if (!is.null(visit_col)) {
#'     core_columns <- c(core_columns, visit_col)
#'   }
#'
#'   involea_dataset <- tibble::as_tibble(dataset) %>%
#'     dplyr::select(dplyr::all_of(c(core_columns, involea_vars)))
#'
#'   # Standardize column names
#'   names(involea_dataset)[1] <- "swan_participant_id"
#'   if (!is.null(visit_col)) {
#'     names(involea_dataset)[2] <- "visit_number_raw"
#'   } else {
#'     involea_dataset$visit_number_raw <- NA_real_
#'   }
#'
#'   # Convert to long format
#'   involea_long <- involea_dataset %>%
#'     tidyr::pivot_longer(
#'       cols = dplyr::all_of(involea_vars),
#'       names_to = "involea_source_variable",
#'       values_to = "involea_raw_response"
#'     ) %>%
#'     dplyr::mutate(
#'       swan_participant_id = as.character(swan_participant_id),
#'       # Extract visit number from variable name or use existing visit
#'       visit_number = dplyr::case_when(
#'         !is.na(visit_number_raw) ~ as.numeric(visit_number_raw),
#'         TRUE ~ extract_visit_number_from_variable_name(involea_source_variable)
#'       ),
#'       involea_incontinence = clean_involea_response(involea_raw_response),
#'       source_file = source_file
#'     ) %>%
#'     dplyr::select(-visit_number_raw) %>%
#'     dplyr::filter(!is.na(involea_raw_response))  # Remove missing responses
#'
#'   return(involea_long)
#' }
#'
#'
#' #' @noRd
#' extract_visit_number_from_variable_name <- function(variable_names) {
#'
#'   # Extract numeric part from variable names more carefully
#'   visit_numbers <- purrr::map_dbl(variable_names, function(var_name) {
#'
#'     # Look for patterns like INVOLEA0, INVOLEA1, LEKINVO7, etc.
#'     if (stringr::str_detect(var_name, "INVOLEA\\d+")) {
#'       # Extract number after INVOLEA
#'       num <- stringr::str_extract(var_name, "(?<=INVOLEA)\\d+")
#'       return(as.numeric(num))
#'     } else if (stringr::str_detect(var_name, "LEKINVO\\d+")) {
#'       # Extract number after LEKINVO
#'       num <- stringr::str_extract(var_name, "(?<=LEKINVO)\\d+")
#'       return(as.numeric(num))
#'     } else {
#'       # Fall back to parse_number
#'       num <- readr::parse_number(var_name)
#'       return(if_else(is.na(num), 0, num))
#'     }
#'   })
#'
#'   return(visit_numbers)
#' }
#'
#'
#' #' @noRd
#' clean_involea_response <- function(response_raw) {
#'
#'   dplyr::case_when(
#'     response_raw %in% c("(2) Yes", "2", 2, "Yes", "YES", "yes") ~ 1,
#'     response_raw %in% c("(1) No", "1", 1, "No", "NO", "no") ~ 0,
#'     TRUE ~ NA_real_
#'   )
#' }
#'
#'
#' #' @noRd
#' combine_baseline_sources <- function(baseline_datasets, verbose) {
#'
#'   if (length(baseline_datasets) == 1) {
#'     return(baseline_datasets[[1]])
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Combining {length(baseline_datasets)} baseline sources")
#'   }
#'
#'   # Start with the most complete dataset
#'   dataset_completeness <- purrr::map_dbl(baseline_datasets, function(ds) {
#'     sum(!is.na(ds$swan_participant_id))
#'   })
#'
#'   primary_dataset <- baseline_datasets[[which.max(dataset_completeness)]]
#'
#'   # Add data from other sources
#'   for (i in seq_along(baseline_datasets)) {
#'     if (i == which.max(dataset_completeness)) next
#'
#'     secondary_dataset <- baseline_datasets[[i]]
#'
#'     # Harmonize data types before merging
#'     secondary_dataset <- harmonize_data_types(secondary_dataset, primary_dataset, verbose)
#'
#'     primary_dataset <- primary_dataset %>%
#'       dplyr::full_join(
#'         secondary_dataset,
#'         by = "swan_participant_id",
#'         suffix = c("", "_secondary")
#'       ) %>%
#'       dplyr::mutate(
#'         # Use primary data preferentially with type-safe coalesce
#'         baseline_age_years = safe_coalesce_numeric(
#'           baseline_age_years,
#'           baseline_age_years_secondary
#'         ),
#'         baseline_bmi_kg_m2 = safe_coalesce_numeric(
#'           baseline_bmi_kg_m2,
#'           baseline_bmi_kg_m2_secondary
#'         ),
#'         baseline_parity_count = safe_coalesce_numeric(
#'           baseline_parity_count,
#'           baseline_parity_count_secondary
#'         ),
#'         baseline_smoking_status = safe_coalesce_character(
#'           baseline_smoking_status,
#'           baseline_smoking_status_secondary
#'         ),
#'         baseline_race_ethnicity = safe_coalesce_character(
#'           baseline_race_ethnicity,
#'           baseline_race_ethnicity_secondary
#'         ),
#'         baseline_menopausal_stage = safe_coalesce_character(
#'           baseline_menopausal_stage,
#'           baseline_menopausal_stage_secondary
#'         )
#'       ) %>%
#'       dplyr::select(-dplyr::ends_with("_secondary"))
#'   }
#'
#'   return(primary_dataset)
#' }
#'
#'
#' #' @noRd
#' harmonize_data_types <- function(secondary_dataset, primary_dataset, verbose) {
#'
#'   # Get common baseline columns
#'   baseline_columns <- names(secondary_dataset)[stringr::str_starts(names(secondary_dataset), "baseline_")]
#'
#'   for (col_name in baseline_columns) {
#'     if (col_name %in% names(primary_dataset)) {
#'
#'       primary_type <- class(primary_dataset[[col_name]])[1]
#'       secondary_type <- class(secondary_dataset[[col_name]])[1]
#'
#'       if (primary_type != secondary_type) {
#'         if (verbose) {
#'           logger::log_info("Harmonizing {col_name}: {secondary_type} -> {primary_type}")
#'         }
#'
#'         # Convert secondary to match primary type
#'         if (primary_type == "numeric" || primary_type == "double") {
#'           secondary_dataset[[col_name]] <- as.numeric(as.character(secondary_dataset[[col_name]]))
#'         } else if (primary_type == "character") {
#'           secondary_dataset[[col_name]] <- as.character(secondary_dataset[[col_name]])
#'         } else if (primary_type == "logical") {
#'           secondary_dataset[[col_name]] <- as.logical(secondary_dataset[[col_name]])
#'         }
#'       }
#'     }
#'   }
#'
#'   return(secondary_dataset)
#' }
#'
#'
#' #' @noRd
#' safe_coalesce_numeric <- function(x, y) {
#'   # Convert both to numeric if needed
#'   x_numeric <- if (is.numeric(x)) x else as.numeric(as.character(x))
#'   y_numeric <- if (is.numeric(y)) y else as.numeric(as.character(y))
#'   dplyr::coalesce(x_numeric, y_numeric)
#' }
#'
#'
#' #' @noRd
#' safe_coalesce_character <- function(x, y) {
#'   # Convert both to character if needed
#'   x_char <- if (is.character(x)) x else as.character(x)
#'   y_char <- if (is.character(y)) y else as.character(y)
#'   dplyr::coalesce(x_char, y_char)
#' }
#'
#'
#' #' @noRd
#' merge_baseline_with_involea <- function(baseline_data, involea_data, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Merging baseline demographics with INVOLEA data")
#'     logger::log_info("Baseline participants: {length(unique(baseline_data$swan_participant_id))}")
#'     logger::log_info("INVOLEA observations: {nrow(involea_data)}")
#'   }
#'
#'   # Merge datasets
#'   merged_data <- involea_data %>%
#'     dplyr::left_join(
#'       baseline_data %>% dplyr::select(-source_file),
#'       by = "swan_participant_id"
#'     )
#'
#'   # Add time variables
#'   merged_data <- merged_data %>%
#'     dplyr::mutate(
#'       years_since_baseline = visit_number
#'     ) %>%
#'     dplyr::arrange(swan_participant_id, visit_number)
#'
#'   if (verbose) {
#'     logger::log_info("Merged dataset: {nrow(merged_data)} observations")
#'     logger::log_info("Participants with both baseline + INVOLEA: {length(unique(merged_data$swan_participant_id[!is.na(merged_data$baseline_age_years)]))}")
#'   }
#'
#'   return(merged_data)
#' }
#'
#'
#' #' @noRd
#' format_final_dataset <- function(merged_data, format, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Formatting final dataset as: {format}")
#'   }
#'
#'   if (format == "long") {
#'     # Clean up and ensure one row per participant-visit
#'     final_data <- merged_data %>%
#'       dplyr::select(
#'         swan_participant_id,
#'         visit_number,
#'         years_since_baseline,
#'         baseline_age_years,
#'         baseline_bmi_kg_m2,
#'         baseline_race_ethnicity,
#'         baseline_smoking_status,
#'         baseline_parity_count,
#'         baseline_menopausal_stage,
#'         involea_incontinence,
#'         involea_raw_response,
#'         involea_source_variable
#'       ) %>%
#'       # Remove any remaining duplicates and keep the first occurrence
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         .keep_all = TRUE
#'       ) %>%
#'       dplyr::arrange(swan_participant_id, visit_number)
#'
#'   } else {
#'     # Convert to wide format
#'     final_data <- merged_data %>%
#'       dplyr::select(
#'         swan_participant_id,
#'         visit_number,
#'         baseline_age_years,
#'         baseline_bmi_kg_m2,
#'         baseline_race_ethnicity,
#'         baseline_smoking_status,
#'         baseline_parity_count,
#'         baseline_menopausal_stage,
#'         involea_incontinence
#'       ) %>%
#'       # Remove duplicates before pivoting
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         .keep_all = TRUE
#'       ) %>%
#'       tidyr::pivot_wider(
#'         names_from = visit_number,
#'         values_from = involea_incontinence,
#'         names_prefix = "involea_visit_"
#'       )
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Final formatted dataset: {nrow(final_data)} rows")
#'     logger::log_info("Unique participants: {length(unique(final_data$swan_participant_id))}")
#'     if (format == "long") {
#'       logger::log_info("Visit range: {min(final_data$visit_number, na.rm = TRUE)} to {max(final_data$visit_number, na.rm = TRUE)}")
#'     }
#'   }
#'
#'   return(final_data)
#' }
#'
#'
#' # Helper functions from previous code
#' #' @noRd
#' identify_participant_id_column <- function(dataset) {
#'   participant_id_patterns <- c("^SWANID$", "^ARCHID$", "^swanid$", "^archid$")
#'   for (pattern in participant_id_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' @noRd
#' identify_visit_column <- function(dataset) {
#'   visit_patterns <- c("^VISIT$", "^visit$", "^Visit$", "^VISITNO$")
#'   for (pattern in visit_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' @noRd
#' clean_race_variable <- function(race_raw) {
#'   dplyr::case_when(
#'     stringr::str_detect(as.character(race_raw), "White|Caucasian") ~ "White",
#'     stringr::str_detect(as.character(race_raw), "Black|African") ~ "Black",
#'     stringr::str_detect(as.character(race_raw), "Hispanic") ~ "Hispanic",
#'     stringr::str_detect(as.character(race_raw), "Chinese") ~ "Chinese",
#'     stringr::str_detect(as.character(race_raw), "Japanese") ~ "Japanese",
#'     TRUE ~ "Other"
#'   )
#' }
#'
#' #' @noRd
#' clean_smoking_variable <- function(smoking_raw) {
#'   dplyr::case_when(
#'     smoking_raw %in% c(1, "(1) Never smoked") ~ "Never",
#'     smoking_raw %in% c(2, "(2) Past smoker") ~ "Past",
#'     smoking_raw %in% c(3, "(3) Current smoker") ~ "Current",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' @noRd
#' clean_menopausal_variable <- function(status_raw) {
#'   dplyr::case_when(
#'     status_raw %in% c(2, "(2) Postmenopausal") ~ "Post",
#'     status_raw %in% c(3, "(3) Late perimenopausal") ~ "Late Peri",
#'     status_raw %in% c(4, "(4) Early perimenopausal") ~ "Early Peri",
#'     status_raw %in% c(5, "(5) Premenopausal") ~ "Pre",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' swan_data_directory <- "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/"
#' create_focused_swan_dataset(swan_data_directory,
#'                                         output_format = "long",
#'                                         verbose = TRUE)
#'
#'
#' # Debug script -----
#' # Debug the visit number extraction issue
#'
#' library(tidyverse)
#' library(stringr)
#'
#' # Test the variable names we're seeing
#' test_variables <- c("INVOLEA0", "INVOLEA1", "INVOLEA2", "INVOLEA3", "INVOLEA4",
#'                     "INVOLEA5", "INVOLEA6", "LEKINVO7", "LEKINVO8", "LEKINVO9", "INVOLEA10")
#'
#' cat("Testing visit number extraction:\n")
#'
#' # Method 1: Simple regex to extract number at end
#' method1 <- str_extract(test_variables, "\\d+$")
#' cat("Method 1 (number at end):\n")
#' for(i in seq_along(test_variables)) {
#'   cat(sprintf("  %s -> %s\n", test_variables[i], method1[i]))
#' }
#'
#' # Method 2: Using parse_number
#' method2 <- readr::parse_number(test_variables)
#' cat("\nMethod 2 (parse_number):\n")
#' for(i in seq_along(test_variables)) {
#'   cat(sprintf("  %s -> %s\n", test_variables[i], method2[i]))
#' }
#'
#' # Let's test what your actual data looks like
#' swan_directory <- "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/"
#'
#' # Load one file to see the actual structure
#' cat("\nChecking actual data structure:\n")
#'
#' # Load the first INVOLEA file
#' env <- new.env()
#' load(file.path(swan_directory, "28762-0001-Data.rda"), envir = env)
#' data <- env[[ls(env)[1]]]
#'
#' involea_vars <- grep("INVOLEA", names(data), value = TRUE)
#' cat("INVOLEA variables found in 28762:\n")
#' for(var in involea_vars) {
#'   cat(sprintf("  %s\n", var))
#' }
#'
#' # Test extraction on actual variables
#' if(length(involea_vars) > 0) {
#'   actual_visits <- str_extract(involea_vars, "\\d+$")
#'   cat("\nExtracted visit numbers:\n")
#'   for(i in seq_along(involea_vars)) {
#'     cat(sprintf("  %s -> %s\n", involea_vars[i], actual_visits[i]))
#'   }
#' }
#'
#' # Quick check of the data structure
#' cat("\nSample of participant IDs and INVOLEA0 values:\n")
#' if("INVOLEA0" %in% names(data) && "SWANID" %in% names(data)) {
#'   sample_data <- data %>%
#'     select(SWANID, INVOLEA0) %>%
#'     slice_head(n = 10)
#'   print(sample_data)
#' }
#'
#' # Test the simplified approach directly
#' test_extract_visits <- function(var_names) {
#'   visits <- str_extract(var_names, "\\d+$")
#'   as.numeric(visits)
#' }
#'
#' cat("\nTesting simplified extraction:\n")
#' test_vars <- c("INVOLEA0", "INVOLEA1", "LEKINVO7", "INVOLEA10")
#' extracted <- test_extract_visits(test_vars)
#' for(i in seq_along(test_vars)) {
#'   cat(sprintf("  %s -> %d\n", test_vars[i], extracted[i]))
#' }
#'
#' cat("\nRange of extracted visits:", min(extracted, na.rm = TRUE), "to", max(extracted, na.rm = TRUE), "\n")
#'
#'
#' # Then try the main function again
#' create_focused_swan_dataset(swan_data_directory, output_format = "long", verbose = TRUE)
#'
#'
#' # Function at 1648 -----
#' #' Create Focused SWAN Dataset: Baseline Demographics + INVOLEA Variables
#' #'
#' #' This function creates a clean, focused SWAN dataset containing baseline
#' #' demographic information (one row per participant) merged with urinary
#' #' incontinence (INVOLEA) variables across all visits in long format. This
#' #' structure is optimal for longitudinal analysis of urinary incontinence
#' #' while maintaining essential baseline covariates.
#' #'
#' #' @param swan_data_directory Character. Path to directory containing SWAN .rda
#' #'   files. The function will identify baseline demographic files and files
#' #'   containing INVOLEA variables across visits.
#' #' @param output_format Character. Format for the final dataset: "long" creates
#' #'   one row per participant-visit with INVOLEA data, "wide" creates one row
#' #'   per participant with INVOLEA variables spread across visits. Default "long".
#' #' @param verbose Logical. If TRUE (default), prints detailed logging about
#' #'   file processing, baseline demographics extraction, and INVOLEA variable
#' #'   identification across visits.
#' #'
#' #' @return A tibble containing focused SWAN data:
#' #'   For "long" format:
#' #'   \describe{
#' #'     \item{swan_participant_id}{Participant identifier}
#' #'     \item{visit_number}{Visit number (0=baseline, 1-15=follow-up)}
#' #'     \item{baseline_age_years}{Age at baseline (repeated for all visits)}
#' #'     \item{baseline_bmi_kg_m2}{BMI at baseline (repeated for all visits)}
#' #'     \item{baseline_race_ethnicity}{Race/ethnicity (repeated for all visits)}
#' #'     \item{baseline_smoking_status}{Smoking status at baseline}
#' #'     \item{baseline_parity_count}{Number of pregnancies at baseline}
#' #'     \item{baseline_menopausal_stage}{Menopausal stage at baseline}
#' #'     \item{involea_incontinence}{Binary incontinence variable (0/1)}
#' #'     \item{involea_raw_response}{Original INVOLEA response}
#' #'     \item{involea_source_variable}{Original variable name}
#' #'     \item{years_since_baseline}{Time since baseline visit}
#' #'   }
#' #'
#' #' @examples
#' #' # Example 1: Long format for longitudinal modeling
#' #' swan_long_dataset <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/Users/tylermuffly/Dropbox (Personal)/workforce/Dall_model/data/SWAN",
#' #'   output_format = "long",
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' # Example 2: Wide format for cross-sectional analysis
#' #' swan_wide_dataset <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "wide",
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' # Example 3: Silent processing for production pipelines
#' #' swan_analysis_ready <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "long",
#' #'   verbose = FALSE
#' #' )
#' #' # Expected output for long format:
#' #' # # A tibble: 21,629 × 12
#' #' #   swan_participant_id visit_number baseline_age_years baseline_bmi_kg_m2
#' #' #   <chr>                      <dbl>              <dbl>              <dbl>
#' #' # 1 10005                          0                 48               24.1
#' #' # 2 10005                          1                 48               24.1
#' #' # 3 10005                          3                 48               24.1
#' #' # # ... with 21,626 more rows and 8 more variables
#' #'
#' #' @importFrom dplyr filter select mutate left_join case_when
#' #' @importFrom dplyr group_by summarise ungroup arrange distinct
#' #' @importFrom tidyr pivot_longer pivot_wider
#' #' @importFrom stringr str_detect str_extract str_replace_all
#' #' @importFrom purrr map_dfr map_chr map_lgl keep
#' #' @importFrom assertthat assert_that
#' #' @importFrom logger log_info log_warn log_error
#' #' @importFrom tibble tibble as_tibble
#' #' @importFrom readr parse_number
#' #' @importFrom glue glue
#' #'
#' #' @export
#' create_focused_swan_dataset <- function(swan_data_directory,
#'                                         output_format = "long",
#'                                         verbose = TRUE) {
#'
#'   # Input validation
#'   if (verbose) {
#'     logger::log_info("Creating focused SWAN dataset: Baseline demographics + INVOLEA variables")
#'     logger::log_info("Input validation and setup")
#'   }
#'
#'   assertthat::assert_that(
#'     is.character(swan_data_directory),
#'     msg = "swan_data_directory must be a character string path"
#'   )
#'   assertthat::assert_that(
#'     dir.exists(swan_data_directory),
#'     msg = glue::glue("Directory does not exist: {swan_data_directory}")
#'   )
#'   assertthat::assert_that(
#'     output_format %in% c("long", "wide"),
#'     msg = "output_format must be 'long' or 'wide'"
#'   )
#'
#'   # Step 1: Extract baseline demographics
#'   baseline_demographics_data <- extract_baseline_demographics(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   # Step 2: Extract INVOLEA variables across all visits
#'   involea_longitudinal_data <- extract_involea_variables(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   # Step 3: Merge baseline demographics with INVOLEA data
#'   merged_focused_dataset <- merge_baseline_with_involea(
#'     baseline_data = baseline_demographics_data,
#'     involea_data = involea_longitudinal_data,
#'     verbose = verbose
#'   )
#'
#'   # Step 4: Format output according to user preference
#'   final_focused_dataset <- format_final_dataset(
#'     merged_data = merged_focused_dataset,
#'     format = output_format,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Focused SWAN dataset creation completed")
#'     logger::log_info("Final dataset: {nrow(final_focused_dataset)} rows × {ncol(final_focused_dataset)} columns")
#'   }
#'
#'   return(final_focused_dataset)
#' }
#'
#'
#' #' @noRd
#' extract_baseline_demographics <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting baseline demographics from SWAN files")
#'   }
#'
#'   # Find files likely to contain baseline demographics
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Searching {length(all_rda_files)} RDA files for baseline demographics")
#'   }
#'
#'   # Try to identify baseline/demographic files by name patterns
#'   baseline_file_patterns <- c("28762", "baseline", "demo", "04368")
#'
#'   baseline_candidates <- purrr::keep(all_rda_files, function(file_path) {
#'     file_name <- tolower(basename(file_path))
#'     any(stringr::str_detect(file_name, baseline_file_patterns))
#'   })
#'
#'   if (verbose) {
#'     logger::log_info("Found {length(baseline_candidates)} potential baseline files:")
#'     purrr::walk(baseline_candidates, ~logger::log_info("  - {basename(.x)}"))
#'   }
#'
#'   # Load and process baseline files
#'   baseline_datasets <- purrr::map(baseline_candidates, function(file_path) {
#'
#'     if (verbose) {
#'       logger::log_info("Processing baseline file: {basename(file_path)}")
#'     }
#'
#'     # Load file
#'     file_environment <- new.env()
#'     load(file_path, envir = file_environment)
#'     raw_baseline_data <- file_environment[[ls(file_environment)[1]]]
#'
#'     # Look for demographic variables
#'     demo_variables <- identify_demographic_variables(raw_baseline_data)
#'
#'     if (length(demo_variables$participant_id) == 0) {
#'       if (verbose) {
#'         logger::log_warn("No participant ID found in {basename(file_path)}")
#'       }
#'       return(NULL)
#'     }
#'
#'     # Extract and clean demographics
#'     cleaned_baseline <- extract_and_clean_demographics(
#'       dataset = raw_baseline_data,
#'       demo_vars = demo_variables,
#'       source_file = basename(file_path),
#'       verbose = verbose
#'     )
#'
#'     return(cleaned_baseline)
#'   })
#'
#'   # Combine baseline datasets
#'   baseline_datasets <- purrr::compact(baseline_datasets)
#'
#'   if (length(baseline_datasets) == 0) {
#'     logger::log_error("No valid baseline demographic data found")
#'     stop("No baseline demographic data could be extracted")
#'   }
#'
#'   # Merge multiple baseline sources if needed
#'   combined_baseline <- combine_baseline_sources(baseline_datasets, verbose)
#'
#'   if (verbose) {
#'     logger::log_info("Baseline demographics extracted: {nrow(combined_baseline)} participants")
#'   }
#'
#'   return(combined_baseline)
#' }
#'
#'
#' #' @noRd
#' extract_involea_variables <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting INVOLEA variables across all visits")
#'   }
#'
#'   # Find all RDA files
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   # Process each file to find INVOLEA variables
#'   involea_datasets <- purrr::map(all_rda_files, function(file_path) {
#'
#'     if (verbose) {
#'       logger::log_info("Scanning {basename(file_path)} for INVOLEA variables")
#'     }
#'
#'     # Load file
#'     file_environment <- new.env()
#'     tryCatch({
#'       load(file_path, envir = file_environment)
#'     }, error = function(e) {
#'       if (verbose) {
#'         logger::log_warn("Failed to load {basename(file_path)}: {e$message}")
#'       }
#'       return(NULL)
#'     })
#'
#'     if (length(ls(file_environment)) == 0) return(NULL)
#'
#'     raw_dataset <- file_environment[[ls(file_environment)[1]]]
#'
#'     # Look for INVOLEA variables
#'     involea_columns <- identify_involea_variables(raw_dataset)
#'
#'     if (length(involea_columns) == 0) {
#'       return(NULL)  # No INVOLEA variables in this file
#'     }
#'
#'     if (verbose) {
#'       logger::log_info("  Found {length(involea_columns)} INVOLEA variables: {paste(involea_columns, collapse = ', ')}")
#'     }
#'
#'     # Extract INVOLEA data
#'     involea_data <- extract_involea_data(
#'       dataset = raw_dataset,
#'       involea_vars = involea_columns,
#'       source_file = basename(file_path),
#'       verbose = verbose
#'     )
#'
#'     return(involea_data)
#'   })
#'
#'   # Combine all INVOLEA datasets
#'   involea_datasets <- purrr::compact(involea_datasets)
#'
#'   if (length(involea_datasets) == 0) {
#'     logger::log_error("No INVOLEA variables found in any files")
#'     stop("No INVOLEA variables could be found")
#'   }
#'
#'   combined_involea <- purrr::map_dfr(involea_datasets, ~.x)
#'
#'   # Remove duplicates (same participant, visit, and variable)
#'   combined_involea <- combined_involea %>%
#'     dplyr::distinct(
#'       swan_participant_id,
#'       visit_number,
#'       involea_source_variable,
#'       .keep_all = TRUE
#'     )
#'
#'   if (verbose) {
#'     logger::log_info("INVOLEA variables extracted: {nrow(combined_involea)} observations")
#'     logger::log_info("Visit range: {min(combined_involea$visit_number, na.rm = TRUE)} to {max(combined_involea$visit_number, na.rm = TRUE)}")
#'     logger::log_info("Unique participants with INVOLEA: {length(unique(combined_involea$swan_participant_id))}")
#'   }
#'
#'   return(combined_involea)
#' }
#'
#'
#' #' @noRd
#' identify_demographic_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Identify participant ID
#'   participant_id_patterns <- c("SWANID", "ARCHID", "swanid", "archid")
#'   participant_id_col <- NULL
#'   for (pattern in participant_id_patterns) {
#'     if (pattern %in% variable_names) {
#'       participant_id_col <- pattern
#'       break
#'     }
#'   }
#'
#'   # Identify demographic variables
#'   age_vars <- variable_names[stringr::str_detect(variable_names, "AGE|age")]
#'   bmi_vars <- variable_names[stringr::str_detect(variable_names, "BMI|bmi")]
#'   race_vars <- variable_names[stringr::str_detect(variable_names, "RACE|race")]
#'   smoking_vars <- variable_names[stringr::str_detect(variable_names, "SMOKE|smoke")]
#'   parity_vars <- variable_names[stringr::str_detect(variable_names, "PREG|CHILD|preg|child|parity")]
#'   menopausal_vars <- variable_names[stringr::str_detect(variable_names, "STATUS|MENO|status|meno")]
#'
#'   return(list(
#'     participant_id = participant_id_col,
#'     age = age_vars,
#'     bmi = bmi_vars,
#'     race = race_vars,
#'     smoking = smoking_vars,
#'     parity = parity_vars,
#'     menopausal = menopausal_vars
#'   ))
#' }
#'
#'
#' #' @noRd
#' identify_involea_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Look for INVOLEA variables (including LEKINVO variants)
#'   involea_patterns <- c("INVOLEA", "LEKINVO", "involea", "lekinvo")
#'
#'   involea_variables <- c()
#'   for (pattern in involea_patterns) {
#'     matching_vars <- variable_names[stringr::str_detect(variable_names, pattern)]
#'     involea_variables <- c(involea_variables, matching_vars)
#'   }
#'
#'   return(unique(involea_variables))
#' }
#'
#'
#' #' @noRd
#' extract_and_clean_demographics <- function(dataset, demo_vars, source_file, verbose) {
#'
#'   # Start with participant ID
#'   cleaned_data <- tibble::as_tibble(dataset) %>%
#'     dplyr::select(dplyr::all_of(demo_vars$participant_id))
#'
#'   names(cleaned_data)[1] <- "swan_participant_id"
#'
#'   # Add demographic variables if available - ensuring consistent data types
#'   if (length(demo_vars$age) > 0) {
#'     cleaned_data$baseline_age_years <- as.numeric(as.character(dataset[[demo_vars$age[1]]]))
#'   }
#'
#'   if (length(demo_vars$bmi) > 0) {
#'     cleaned_data$baseline_bmi_kg_m2 <- as.numeric(as.character(dataset[[demo_vars$bmi[1]]]))
#'   }
#'
#'   if (length(demo_vars$race) > 0) {
#'     cleaned_data$baseline_race_ethnicity <- clean_race_variable(dataset[[demo_vars$race[1]]])
#'   }
#'
#'   if (length(demo_vars$smoking) > 0) {
#'     cleaned_data$baseline_smoking_status <- clean_smoking_variable(dataset[[demo_vars$smoking[1]]])
#'   }
#'
#'   if (length(demo_vars$parity) > 0) {
#'     cleaned_data$baseline_parity_count <- as.numeric(as.character(dataset[[demo_vars$parity[1]]]))
#'   }
#'
#'   if (length(demo_vars$menopausal) > 0) {
#'     cleaned_data$baseline_menopausal_stage <- clean_menopausal_variable(dataset[[demo_vars$menopausal[1]]])
#'   }
#'
#'   # Standardize participant ID
#'   cleaned_data$swan_participant_id <- as.character(cleaned_data$swan_participant_id)
#'   cleaned_data$source_file <- source_file
#'
#'   if (verbose) {
#'     logger::log_info("  Extracted {nrow(cleaned_data)} participants with {ncol(cleaned_data)-2} demographic variables")
#'   }
#'
#'   return(cleaned_data)
#' }
#'
#'
#' #' @noRd
#' extract_involea_data <- function(dataset, involea_vars, source_file, verbose) {
#'
#'   # Identify participant ID and visit variables
#'   participant_id_col <- identify_participant_id_column(dataset)
#'   visit_col <- identify_visit_column(dataset)
#'
#'   if (is.null(participant_id_col)) {
#'     if (verbose) {
#'       logger::log_warn("No participant ID found in {source_file}")
#'     }
#'     return(NULL)
#'   }
#'
#'   # Select relevant columns
#'   core_columns <- c(participant_id_col)
#'   if (!is.null(visit_col)) {
#'     core_columns <- c(core_columns, visit_col)
#'   }
#'
#'   involea_dataset <- tibble::as_tibble(dataset) %>%
#'     dplyr::select(dplyr::all_of(c(core_columns, involea_vars)))
#'
#'   # Standardize column names
#'   names(involea_dataset)[1] <- "swan_participant_id"
#'   if (!is.null(visit_col)) {
#'     names(involea_dataset)[2] <- "visit_number_raw"
#'   } else {
#'     involea_dataset$visit_number_raw <- NA_real_
#'   }
#'
#'   # Convert to long format
#'   involea_long <- involea_dataset %>%
#'     tidyr::pivot_longer(
#'       cols = dplyr::all_of(involea_vars),
#'       names_to = "involea_source_variable",
#'       values_to = "involea_raw_response"
#'     ) %>%
#'     dplyr::mutate(
#'       swan_participant_id = as.character(swan_participant_id),
#'       # Extract visit number from variable name or use existing visit
#'       visit_number = dplyr::case_when(
#'         !is.na(visit_number_raw) ~ as.numeric(visit_number_raw),
#'         TRUE ~ extract_visit_number_from_variable_name(involea_source_variable)
#'       ),
#'       involea_incontinence = clean_involea_response(involea_raw_response),
#'       source_file = source_file
#'     ) %>%
#'     dplyr::select(-visit_number_raw) %>%
#'     dplyr::filter(!is.na(involea_raw_response))  # Remove missing responses
#'
#'   return(involea_long)
#' }
#'
#'
#' #' @noRd
#' extract_visit_number_from_variable_name <- function(variable_names) {
#'
#'   # Simple and robust extraction of numbers at end of variable names
#'   visit_numbers <- stringr::str_extract(variable_names, "\\d+$")
#'   visit_numbers <- as.numeric(visit_numbers)
#'
#'   # Replace any NAs with 0 (baseline)
#'   visit_numbers[is.na(visit_numbers)] <- 0
#'
#'   return(visit_numbers)
#' }
#'
#'
#' #' @noRd
#' clean_involea_response <- function(response_raw) {
#'
#'   dplyr::case_when(
#'     response_raw %in% c("(2) Yes", "2", 2, "Yes", "YES", "yes") ~ 1,
#'     response_raw %in% c("(1) No", "1", 1, "No", "NO", "no") ~ 0,
#'     TRUE ~ NA_real_
#'   )
#' }
#'
#'
#' #' @noRd
#' combine_baseline_sources <- function(baseline_datasets, verbose) {
#'
#'   if (length(baseline_datasets) == 1) {
#'     return(baseline_datasets[[1]])
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Combining {length(baseline_datasets)} baseline sources")
#'   }
#'
#'   # Start with the most complete dataset
#'   dataset_completeness <- purrr::map_dbl(baseline_datasets, function(ds) {
#'     sum(!is.na(ds$swan_participant_id))
#'   })
#'
#'   primary_dataset <- baseline_datasets[[which.max(dataset_completeness)]]
#'
#'   # Add data from other sources
#'   for (i in seq_along(baseline_datasets)) {
#'     if (i == which.max(dataset_completeness)) next
#'
#'     secondary_dataset <- baseline_datasets[[i]]
#'
#'     # Harmonize data types before merging
#'     secondary_dataset <- harmonize_data_types(secondary_dataset, primary_dataset, verbose)
#'
#'     primary_dataset <- primary_dataset %>%
#'       dplyr::full_join(
#'         secondary_dataset,
#'         by = "swan_participant_id",
#'         suffix = c("", "_secondary")
#'       ) %>%
#'       dplyr::mutate(
#'         # Use primary data preferentially with type-safe coalesce
#'         baseline_age_years = safe_coalesce_numeric(
#'           baseline_age_years,
#'           baseline_age_years_secondary
#'         ),
#'         baseline_bmi_kg_m2 = safe_coalesce_numeric(
#'           baseline_bmi_kg_m2,
#'           baseline_bmi_kg_m2_secondary
#'         ),
#'         baseline_parity_count = safe_coalesce_numeric(
#'           baseline_parity_count,
#'           baseline_parity_count_secondary
#'         ),
#'         baseline_smoking_status = safe_coalesce_character(
#'           baseline_smoking_status,
#'           baseline_smoking_status_secondary
#'         ),
#'         baseline_race_ethnicity = safe_coalesce_character(
#'           baseline_race_ethnicity,
#'           baseline_race_ethnicity_secondary
#'         ),
#'         baseline_menopausal_stage = safe_coalesce_character(
#'           baseline_menopausal_stage,
#'           baseline_menopausal_stage_secondary
#'         )
#'       ) %>%
#'       dplyr::select(-dplyr::ends_with("_secondary"))
#'   }
#'
#'   return(primary_dataset)
#' }
#'
#'
#' #' @noRd
#' harmonize_data_types <- function(secondary_dataset, primary_dataset, verbose) {
#'
#'   # Get common baseline columns
#'   baseline_columns <- names(secondary_dataset)[stringr::str_starts(names(secondary_dataset), "baseline_")]
#'
#'   for (col_name in baseline_columns) {
#'     if (col_name %in% names(primary_dataset)) {
#'
#'       primary_type <- class(primary_dataset[[col_name]])[1]
#'       secondary_type <- class(secondary_dataset[[col_name]])[1]
#'
#'       if (primary_type != secondary_type) {
#'         if (verbose) {
#'           logger::log_info("Harmonizing {col_name}: {secondary_type} -> {primary_type}")
#'         }
#'
#'         # Convert secondary to match primary type
#'         if (primary_type == "numeric" || primary_type == "double") {
#'           secondary_dataset[[col_name]] <- as.numeric(as.character(secondary_dataset[[col_name]]))
#'         } else if (primary_type == "character") {
#'           secondary_dataset[[col_name]] <- as.character(secondary_dataset[[col_name]])
#'         } else if (primary_type == "logical") {
#'           secondary_dataset[[col_name]] <- as.logical(secondary_dataset[[col_name]])
#'         }
#'       }
#'     }
#'   }
#'
#'   return(secondary_dataset)
#' }
#'
#'
#' #' @noRd
#' safe_coalesce_numeric <- function(x, y) {
#'   # Convert both to numeric if needed
#'   x_numeric <- if (is.numeric(x)) x else as.numeric(as.character(x))
#'   y_numeric <- if (is.numeric(y)) y else as.numeric(as.character(y))
#'   dplyr::coalesce(x_numeric, y_numeric)
#' }
#'
#'
#' #' @noRd
#' safe_coalesce_character <- function(x, y) {
#'   # Convert both to character if needed
#'   x_char <- if (is.character(x)) x else as.character(x)
#'   y_char <- if (is.character(y)) y else as.character(y)
#'   dplyr::coalesce(x_char, y_char)
#' }
#'
#'
#' #' @noRd
#' merge_baseline_with_involea <- function(baseline_data, involea_data, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Merging baseline demographics with INVOLEA data")
#'     logger::log_info("Baseline participants: {length(unique(baseline_data$swan_participant_id))}")
#'     logger::log_info("INVOLEA observations: {nrow(involea_data)}")
#'   }
#'
#'   # Merge datasets
#'   merged_data <- involea_data %>%
#'     dplyr::left_join(
#'       baseline_data %>% dplyr::select(-source_file),
#'       by = "swan_participant_id"
#'     )
#'
#'   # Add time variables
#'   merged_data <- merged_data %>%
#'     dplyr::mutate(
#'       years_since_baseline = visit_number
#'     ) %>%
#'     dplyr::arrange(swan_participant_id, visit_number)
#'
#'   if (verbose) {
#'     logger::log_info("Merged dataset: {nrow(merged_data)} observations")
#'     logger::log_info("Participants with both baseline + INVOLEA: {length(unique(merged_data$swan_participant_id[!is.na(merged_data$baseline_age_years)]))}")
#'   }
#'
#'   return(merged_data)
#' }
#'
#'
#' #' @noRd
#' format_final_dataset <- function(merged_data, format, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Formatting final dataset as: {format}")
#'   }
#'
#'   if (format == "long") {
#'     # Clean up but preserve all participant-visit combinations
#'     final_data <- merged_data %>%
#'       dplyr::select(
#'         swan_participant_id,
#'         visit_number,
#'         years_since_baseline,
#'         baseline_age_years,
#'         baseline_bmi_kg_m2,
#'         baseline_race_ethnicity,
#'         baseline_smoking_status,
#'         baseline_parity_count,
#'         baseline_menopausal_stage,
#'         involea_incontinence,
#'         involea_raw_response,
#'         involea_source_variable
#'       ) %>%
#'       # Only remove true duplicates (same participant, visit, AND response)
#'       # but keep different visits for the same participant
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         involea_source_variable,
#'         .keep_all = TRUE
#'       ) %>%
#'       dplyr::arrange(swan_participant_id, visit_number)
#'
#'   } else {
#'     # Convert to wide format
#'     final_data <- merged_data %>%
#'       dplyr::select(
#'         swan_participant_id,
#'         visit_number,
#'         baseline_age_years,
#'         baseline_bmi_kg_m2,
#'         baseline_race_ethnicity,
#'         baseline_smoking_status,
#'         baseline_parity_count,
#'         baseline_menopausal_stage,
#'         involea_incontinence
#'       ) %>%
#'       # Only remove true duplicates before pivoting
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         .keep_all = TRUE
#'       ) %>%
#'       tidyr::pivot_wider(
#'         names_from = visit_number,
#'         values_from = involea_incontinence,
#'         names_prefix = "involea_visit_"
#'       )
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Final formatted dataset: {nrow(final_data)} rows")
#'     logger::log_info("Unique participants: {length(unique(final_data$swan_participant_id))}")
#'     if (format == "long") {
#'       logger::log_info("Visit range: {min(final_data$visit_number, na.rm = TRUE)} to {max(final_data$visit_number, na.rm = TRUE)}")
#'     }
#'   }
#'
#'   return(final_data)
#' }
#'
#'
#' # Helper functions from previous code
#' #' @noRd
#' identify_participant_id_column <- function(dataset) {
#'   participant_id_patterns <- c("^SWANID$", "^ARCHID$", "^swanid$", "^archid$")
#'   for (pattern in participant_id_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' @noRd
#' identify_visit_column <- function(dataset) {
#'   visit_patterns <- c("^VISIT$", "^visit$", "^Visit$", "^VISITNO$")
#'   for (pattern in visit_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' @noRd
#' clean_race_variable <- function(race_raw) {
#'   dplyr::case_when(
#'     stringr::str_detect(as.character(race_raw), "White|Caucasian") ~ "White",
#'     stringr::str_detect(as.character(race_raw), "Black|African") ~ "Black",
#'     stringr::str_detect(as.character(race_raw), "Hispanic") ~ "Hispanic",
#'     stringr::str_detect(as.character(race_raw), "Chinese") ~ "Chinese",
#'     stringr::str_detect(as.character(race_raw), "Japanese") ~ "Japanese",
#'     TRUE ~ "Other"
#'   )
#' }
#'
#' #' @noRd
#' clean_smoking_variable <- function(smoking_raw) {
#'   dplyr::case_when(
#'     smoking_raw %in% c(1, "(1) Never smoked") ~ "Never",
#'     smoking_raw %in% c(2, "(2) Past smoker") ~ "Past",
#'     smoking_raw %in% c(3, "(3) Current smoker") ~ "Current",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' @noRd
#' clean_menopausal_variable <- function(status_raw) {
#'   dplyr::case_when(
#'     status_raw %in% c(2, "(2) Postmenopausal") ~ "Post",
#'     status_raw %in% c(3, "(3) Late perimenopausal") ~ "Late Peri",
#'     status_raw %in% c(4, "(4) Early perimenopausal") ~ "Early Peri",
#'     status_raw %in% c(5, "(5) Premenopausal") ~ "Pre",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' create_focused_swan_dataset(swan_data_directory,
#'                                         output_format = "long",
#'                                         verbose = TRUE)
#'
#' # Diagnostic -----
#' # Diagnose the visit number extraction issue
#'
#' library(tidyverse)
#' library(stringr)
#'
#' # Load one file and manually test the extraction
#' swan_directory <- "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/"
#'
#' # Test each file manually
#' files_to_test <- c("28762-0001-Data.rda", "29221-0001-Data.rda", "29401-0001-Data.rda")
#'
#' for (file_name in files_to_test) {
#'   cat("\n=== Testing", file_name, "===\n")
#'
#'   # Load file
#'   env <- new.env()
#'   load(file.path(swan_directory, file_name), envir = env)
#'   data <- env[[ls(env)[1]]]
#'
#'   # Find INVOLEA/LEKINVO variables
#'   involea_vars <- grep("INVOLEA|LEKINVO", names(data), value = TRUE)
#'   cat("INVOLEA variables found:", paste(involea_vars, collapse = ", "), "\n")
#'
#'   # Test visit extraction
#'   if (length(involea_vars) > 0) {
#'     extracted_visits <- str_extract(involea_vars, "\\d+$")
#'     extracted_visits_numeric <- as.numeric(extracted_visits)
#'
#'     for (i in seq_along(involea_vars)) {
#'       cat(sprintf("  %s -> %s (numeric: %d)\n",
#'                   involea_vars[i],
#'                   extracted_visits[i],
#'                   extracted_visits_numeric[i]))
#'     }
#'
#'     # Check a sample of the actual data
#'     if ("SWANID" %in% names(data) || "ARCHID" %in% names(data)) {
#'       id_col <- if ("SWANID" %in% names(data)) "SWANID" else "ARCHID"
#'
#'       sample_data <- data %>%
#'         select(!!sym(id_col), all_of(involea_vars)) %>%
#'         slice_head(n = 5)
#'
#'       cat("Sample data:\n")
#'       print(sample_data)
#'
#'       # Test the pivot_longer operation
#'       long_data <- sample_data %>%
#'         pivot_longer(
#'           cols = all_of(involea_vars),
#'           names_to = "source_variable",
#'           values_to = "response"
#'         ) %>%
#'         mutate(
#'           visit_extracted = str_extract(source_variable, "\\d+$"),
#'           visit_numeric = as.numeric(visit_extracted)
#'         )
#'
#'       cat("After pivot_longer:\n")
#'       print(long_data)
#'
#'       cat("Unique visits extracted:", paste(sort(unique(long_data$visit_numeric)), collapse = ", "), "\n")
#'     }
#'   }
#' }
#'
#' # Test the specific extraction function
#' extract_visit_number_from_variable_name <- function(variable_names) {
#'   visit_numbers <- stringr::str_extract(variable_names, "\\d+$")
#'   visit_numbers <- as.numeric(visit_numbers)
#'   visit_numbers[is.na(visit_numbers)] <- 0
#'   return(visit_numbers)
#' }
#'
#' cat("\n=== Testing extraction function directly ===\n")
#' test_vars <- c("INVOLEA0", "INVOLEA1", "INVOLEA2", "LEKINVO7", "INVOLEA10")
#' extracted <- extract_visit_number_from_variable_name(test_vars)
#'
#' for (i in seq_along(test_vars)) {
#'   cat(sprintf("%s -> %d\n", test_vars[i], extracted[i]))
#' }
#'
#' cat("Range:", min(extracted), "to", max(extracted), "\n")
#'
#'
#' # Function at 1653 -----
#' #' Create Focused SWAN Dataset: Baseline Demographics + INVOLEA Variables
#' #'
#' #' This function creates a clean, focused SWAN dataset containing baseline
#' #' demographic information (one row per participant) merged with urinary
#' #' incontinence (INVOLEA) variables across all visits in long format. This
#' #' structure is optimal for longitudinal analysis of urinary incontinence
#' #' while maintaining essential baseline covariates.
#' #'
#' #' @param swan_data_directory Character. Path to directory containing SWAN .rda
#' #'   files. The function will identify baseline demographic files and files
#' #'   containing INVOLEA variables across visits.
#' #' @param output_format Character. Format for the final dataset: "long" creates
#' #'   one row per participant-visit with INVOLEA data, "wide" creates one row
#' #'   per participant with INVOLEA variables spread across visits. Default "long".
#' #' @param verbose Logical. If TRUE (default), prints detailed logging about
#' #'   file processing, baseline demographics extraction, and INVOLEA variable
#' #'   identification across visits.
#' #'
#' #' @return A tibble containing focused SWAN data:
#' #'   For "long" format:
#' #'   \describe{
#' #'     \item{swan_participant_id}{Participant identifier}
#' #'     \item{visit_number}{Visit number (0=baseline, 1-15=follow-up)}
#' #'     \item{baseline_age_years}{Age at baseline (repeated for all visits)}
#' #'     \item{baseline_bmi_kg_m2}{BMI at baseline (repeated for all visits)}
#' #'     \item{baseline_race_ethnicity}{Race/ethnicity (repeated for all visits)}
#' #'     \item{baseline_smoking_status}{Smoking status at baseline}
#' #'     \item{baseline_parity_count}{Number of pregnancies at baseline}
#' #'     \item{baseline_menopausal_stage}{Menopausal stage at baseline}
#' #'     \item{involea_incontinence}{Binary incontinence variable (0/1)}
#' #'     \item{involea_raw_response}{Original INVOLEA response}
#' #'     \item{involea_source_variable}{Original variable name}
#' #'     \item{years_since_baseline}{Time since baseline visit}
#' #'   }
#' #'
#' #' @examples
#' #' # Example 1: Long format for longitudinal modeling
#' #' swan_long_dataset <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/Users/tylermuffly/Dropbox (Personal)/workforce/Dall_model/data/SWAN",
#' #'   output_format = "long",
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' # Example 2: Wide format for cross-sectional analysis
#' #' swan_wide_dataset <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "wide",
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' # Example 3: Silent processing for production pipelines
#' #' swan_analysis_ready <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "long",
#' #'   verbose = FALSE
#' #' )
#' #' # Expected output for long format:
#' #' # # A tibble: 21,629 × 12
#' #' #   swan_participant_id visit_number baseline_age_years baseline_bmi_kg_m2
#' #' #   <chr>                      <dbl>              <dbl>              <dbl>
#' #' # 1 10005                          0                 48               24.1
#' #' # 2 10005                          1                 48               24.1
#' #' # 3 10005                          3                 48               24.1
#' #' # # ... with 21,626 more rows and 8 more variables
#' #'
#' #' @importFrom dplyr filter select mutate left_join case_when
#' #' @importFrom dplyr group_by summarise ungroup arrange distinct
#' #' @importFrom tidyr pivot_longer pivot_wider
#' #' @importFrom stringr str_detect str_extract str_replace_all
#' #' @importFrom purrr map_dfr map_chr map_lgl keep
#' #' @importFrom assertthat assert_that
#' #' @importFrom logger log_info log_warn log_error
#' #' @importFrom tibble tibble as_tibble
#' #' @importFrom readr parse_number
#' #' @importFrom glue glue
#' #'
#' #' @export
#' create_focused_swan_dataset <- function(swan_data_directory,
#'                                         output_format = "long",
#'                                         verbose = TRUE) {
#'
#'   # Input validation
#'   if (verbose) {
#'     logger::log_info("Creating focused SWAN dataset: Baseline demographics + INVOLEA variables")
#'     logger::log_info("Input validation and setup")
#'   }
#'
#'   assertthat::assert_that(
#'     is.character(swan_data_directory),
#'     msg = "swan_data_directory must be a character string path"
#'   )
#'   assertthat::assert_that(
#'     dir.exists(swan_data_directory),
#'     msg = glue::glue("Directory does not exist: {swan_data_directory}")
#'   )
#'   assertthat::assert_that(
#'     output_format %in% c("long", "wide"),
#'     msg = "output_format must be 'long' or 'wide'"
#'   )
#'
#'   # Step 1: Extract baseline demographics
#'   baseline_demographics_data <- extract_baseline_demographics(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   # Step 2: Extract INVOLEA variables across all visits
#'   involea_longitudinal_data <- extract_involea_variables(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   # Step 3: Merge baseline demographics with INVOLEA data
#'   merged_focused_dataset <- merge_baseline_with_involea(
#'     baseline_data = baseline_demographics_data,
#'     involea_data = involea_longitudinal_data,
#'     verbose = verbose
#'   )
#'
#'   # Step 4: Format output according to user preference
#'   final_focused_dataset <- format_final_dataset(
#'     merged_data = merged_focused_dataset,
#'     format = output_format,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Focused SWAN dataset creation completed")
#'     logger::log_info("Final dataset: {nrow(final_focused_dataset)} rows × {ncol(final_focused_dataset)} columns")
#'   }
#'
#'   return(final_focused_dataset)
#' }
#'
#'
#' #' @noRd
#' extract_baseline_demographics <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting baseline demographics from SWAN files")
#'   }
#'
#'   # Find files likely to contain baseline demographics
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Searching {length(all_rda_files)} RDA files for baseline demographics")
#'   }
#'
#'   # Try to identify baseline/demographic files by name patterns
#'   baseline_file_patterns <- c("28762", "baseline", "demo", "04368")
#'
#'   baseline_candidates <- purrr::keep(all_rda_files, function(file_path) {
#'     file_name <- tolower(basename(file_path))
#'     any(stringr::str_detect(file_name, baseline_file_patterns))
#'   })
#'
#'   if (verbose) {
#'     logger::log_info("Found {length(baseline_candidates)} potential baseline files:")
#'     purrr::walk(baseline_candidates, ~logger::log_info("  - {basename(.x)}"))
#'   }
#'
#'   # Load and process baseline files
#'   baseline_datasets <- purrr::map(baseline_candidates, function(file_path) {
#'
#'     if (verbose) {
#'       logger::log_info("Processing baseline file: {basename(file_path)}")
#'     }
#'
#'     # Load file
#'     file_environment <- new.env()
#'     load(file_path, envir = file_environment)
#'     raw_baseline_data <- file_environment[[ls(file_environment)[1]]]
#'
#'     # Look for demographic variables
#'     demo_variables <- identify_demographic_variables(raw_baseline_data)
#'
#'     if (length(demo_variables$participant_id) == 0) {
#'       if (verbose) {
#'         logger::log_warn("No participant ID found in {basename(file_path)}")
#'       }
#'       return(NULL)
#'     }
#'
#'     # Extract and clean demographics
#'     cleaned_baseline <- extract_and_clean_demographics(
#'       dataset = raw_baseline_data,
#'       demo_vars = demo_variables,
#'       source_file = basename(file_path),
#'       verbose = verbose
#'     )
#'
#'     return(cleaned_baseline)
#'   })
#'
#'   # Combine baseline datasets
#'   baseline_datasets <- purrr::compact(baseline_datasets)
#'
#'   if (length(baseline_datasets) == 0) {
#'     logger::log_error("No valid baseline demographic data found")
#'     stop("No baseline demographic data could be extracted")
#'   }
#'
#'   # Merge multiple baseline sources if needed
#'   combined_baseline <- combine_baseline_sources(baseline_datasets, verbose)
#'
#'   if (verbose) {
#'     logger::log_info("Baseline demographics extracted: {nrow(combined_baseline)} participants")
#'   }
#'
#'   return(combined_baseline)
#' }
#'
#'
#' #' @noRd
#' extract_involea_variables <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting INVOLEA variables across all visits")
#'   }
#'
#'   # Find all RDA files
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   # Process each file to find INVOLEA variables
#'   involea_datasets <- purrr::map(all_rda_files, function(file_path) {
#'
#'     if (verbose) {
#'       logger::log_info("Scanning {basename(file_path)} for INVOLEA variables")
#'     }
#'
#'     # Load file
#'     file_environment <- new.env()
#'     tryCatch({
#'       load(file_path, envir = file_environment)
#'     }, error = function(e) {
#'       if (verbose) {
#'         logger::log_warn("Failed to load {basename(file_path)}: {e$message}")
#'       }
#'       return(NULL)
#'     })
#'
#'     if (length(ls(file_environment)) == 0) return(NULL)
#'
#'     raw_dataset <- file_environment[[ls(file_environment)[1]]]
#'
#'     # Look for INVOLEA variables
#'     involea_columns <- identify_involea_variables(raw_dataset)
#'
#'     if (length(involea_columns) == 0) {
#'       return(NULL)  # No INVOLEA variables in this file
#'     }
#'
#'     if (verbose) {
#'       logger::log_info("  Found {length(involea_columns)} INVOLEA variables: {paste(involea_columns, collapse = ', ')}")
#'     }
#'
#'     # Extract INVOLEA data
#'     involea_data <- extract_involea_data(
#'       dataset = raw_dataset,
#'       involea_vars = involea_columns,
#'       source_file = basename(file_path),
#'       verbose = verbose
#'     )
#'
#'     return(involea_data)
#'   })
#'
#'   # Combine all INVOLEA datasets
#'   involea_datasets <- purrr::compact(involea_datasets)
#'
#'   if (length(involea_datasets) == 0) {
#'     logger::log_error("No INVOLEA variables found in any files")
#'     stop("No INVOLEA variables could be found")
#'   }
#'
#'   combined_involea <- purrr::map_dfr(involea_datasets, ~.x)
#'
#'   # Remove duplicates (same participant, visit, and variable)
#'   combined_involea <- combined_involea %>%
#'     dplyr::distinct(
#'       swan_participant_id,
#'       visit_number,
#'       involea_source_variable,
#'       .keep_all = TRUE
#'     )
#'
#'   if (verbose) {
#'     logger::log_info("INVOLEA variables extracted: {nrow(combined_involea)} observations")
#'     logger::log_info("Visit range: {min(combined_involea$visit_number, na.rm = TRUE)} to {max(combined_involea$visit_number, na.rm = TRUE)}")
#'     logger::log_info("Unique participants with INVOLEA: {length(unique(combined_involea$swan_participant_id))}")
#'   }
#'
#'   return(combined_involea)
#' }
#'
#'
#' #' @noRd
#' identify_demographic_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Identify participant ID
#'   participant_id_patterns <- c("SWANID", "ARCHID", "swanid", "archid")
#'   participant_id_col <- NULL
#'   for (pattern in participant_id_patterns) {
#'     if (pattern %in% variable_names) {
#'       participant_id_col <- pattern
#'       break
#'     }
#'   }
#'
#'   # Identify demographic variables
#'   age_vars <- variable_names[stringr::str_detect(variable_names, "AGE|age")]
#'   bmi_vars <- variable_names[stringr::str_detect(variable_names, "BMI|bmi")]
#'   race_vars <- variable_names[stringr::str_detect(variable_names, "RACE|race")]
#'   smoking_vars <- variable_names[stringr::str_detect(variable_names, "SMOKE|smoke")]
#'   parity_vars <- variable_names[stringr::str_detect(variable_names, "PREG|CHILD|preg|child|parity")]
#'   menopausal_vars <- variable_names[stringr::str_detect(variable_names, "STATUS|MENO|status|meno")]
#'
#'   return(list(
#'     participant_id = participant_id_col,
#'     age = age_vars,
#'     bmi = bmi_vars,
#'     race = race_vars,
#'     smoking = smoking_vars,
#'     parity = parity_vars,
#'     menopausal = menopausal_vars
#'   ))
#' }
#'
#'
#' #' @noRd
#' identify_involea_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Look for INVOLEA variables (including LEKINVO variants)
#'   involea_patterns <- c("INVOLEA", "LEKINVO", "involea", "lekinvo")
#'
#'   involea_variables <- c()
#'   for (pattern in involea_patterns) {
#'     matching_vars <- variable_names[stringr::str_detect(variable_names, pattern)]
#'     involea_variables <- c(involea_variables, matching_vars)
#'   }
#'
#'   return(unique(involea_variables))
#' }
#'
#'
#' #' @noRd
#' extract_and_clean_demographics <- function(dataset, demo_vars, source_file, verbose) {
#'
#'   # Start with participant ID
#'   cleaned_data <- tibble::as_tibble(dataset) %>%
#'     dplyr::select(dplyr::all_of(demo_vars$participant_id))
#'
#'   names(cleaned_data)[1] <- "swan_participant_id"
#'
#'   # Add demographic variables if available - ensuring consistent data types
#'   if (length(demo_vars$age) > 0) {
#'     cleaned_data$baseline_age_years <- as.numeric(as.character(dataset[[demo_vars$age[1]]]))
#'   }
#'
#'   if (length(demo_vars$bmi) > 0) {
#'     cleaned_data$baseline_bmi_kg_m2 <- as.numeric(as.character(dataset[[demo_vars$bmi[1]]]))
#'   }
#'
#'   if (length(demo_vars$race) > 0) {
#'     cleaned_data$baseline_race_ethnicity <- clean_race_variable(dataset[[demo_vars$race[1]]])
#'   }
#'
#'   if (length(demo_vars$smoking) > 0) {
#'     cleaned_data$baseline_smoking_status <- clean_smoking_variable(dataset[[demo_vars$smoking[1]]])
#'   }
#'
#'   if (length(demo_vars$parity) > 0) {
#'     cleaned_data$baseline_parity_count <- as.numeric(as.character(dataset[[demo_vars$parity[1]]]))
#'   }
#'
#'   if (length(demo_vars$menopausal) > 0) {
#'     cleaned_data$baseline_menopausal_stage <- clean_menopausal_variable(dataset[[demo_vars$menopausal[1]]])
#'   }
#'
#'   # Standardize participant ID
#'   cleaned_data$swan_participant_id <- as.character(cleaned_data$swan_participant_id)
#'   cleaned_data$source_file <- source_file
#'
#'   if (verbose) {
#'     logger::log_info("  Extracted {nrow(cleaned_data)} participants with {ncol(cleaned_data)-2} demographic variables")
#'   }
#'
#'   return(cleaned_data)
#' }
#'
#'
#' #' @noRd
#' extract_involea_data <- function(dataset, involea_vars, source_file, verbose) {
#'
#'   # Identify participant ID and visit variables
#'   participant_id_col <- identify_participant_id_column(dataset)
#'   visit_col <- identify_visit_column(dataset)
#'
#'   if (is.null(participant_id_col)) {
#'     if (verbose) {
#'       logger::log_warn("No participant ID found in {source_file}")
#'     }
#'     return(NULL)
#'   }
#'
#'   # Select relevant columns
#'   core_columns <- c(participant_id_col)
#'   if (!is.null(visit_col)) {
#'     core_columns <- c(core_columns, visit_col)
#'   }
#'
#'   involea_dataset <- tibble::as_tibble(dataset) %>%
#'     dplyr::select(dplyr::all_of(c(core_columns, involea_vars)))
#'
#'   # Standardize column names
#'   names(involea_dataset)[1] <- "swan_participant_id"
#'   if (!is.null(visit_col)) {
#'     names(involea_dataset)[2] <- "visit_number_from_file"
#'   } else {
#'     involea_dataset$visit_number_from_file <- NA_real_
#'   }
#'
#'   # Convert to long format
#'   involea_long <- involea_dataset %>%
#'     tidyr::pivot_longer(
#'       cols = dplyr::all_of(involea_vars),
#'       names_to = "involea_source_variable",
#'       values_to = "involea_raw_response"
#'     ) %>%
#'     dplyr::mutate(
#'       swan_participant_id = as.character(swan_participant_id),
#'       # ALWAYS extract visit number from variable name since SWAN files
#'       # have one variable per visit per file
#'       visit_number = extract_visit_number_from_variable_name(involea_source_variable),
#'       involea_incontinence = clean_involea_response(involea_raw_response),
#'       source_file = source_file
#'     ) %>%
#'     dplyr::select(-visit_number_from_file) %>%
#'     dplyr::filter(!is.na(involea_raw_response))  # Remove missing responses
#'
#'   # Debug logging to see what visit numbers we actually extracted
#'   if (verbose && nrow(involea_long) > 0) {
#'     unique_visits <- sort(unique(involea_long$visit_number))
#'     logger::log_info("  Extracted visits for {source_file}: {paste(unique_visits, collapse = ', ')}")
#'     logger::log_info("  Observations in {source_file}: {nrow(involea_long)}")
#'   }
#'
#'   return(involea_long)
#' }
#'
#'
#' #' @noRd
#' extract_visit_number_from_variable_name <- function(variable_names) {
#'
#'   # Simple and robust extraction of numbers at end of variable names
#'   visit_numbers <- stringr::str_extract(variable_names, "\\d+$")
#'   visit_numbers <- as.numeric(visit_numbers)
#'
#'   # Replace any NAs with 0 (baseline)
#'   visit_numbers[is.na(visit_numbers)] <- 0
#'
#'   return(visit_numbers)
#' }
#'
#'
#' #' @noRd
#' clean_involea_response <- function(response_raw) {
#'
#'   dplyr::case_when(
#'     response_raw %in% c("(2) Yes", "2", 2, "Yes", "YES", "yes") ~ 1,
#'     response_raw %in% c("(1) No", "1", 1, "No", "NO", "no") ~ 0,
#'     TRUE ~ NA_real_
#'   )
#' }
#'
#'
#' #' @noRd
#' combine_baseline_sources <- function(baseline_datasets, verbose) {
#'
#'   if (length(baseline_datasets) == 1) {
#'     return(baseline_datasets[[1]])
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Combining {length(baseline_datasets)} baseline sources")
#'   }
#'
#'   # Start with the most complete dataset
#'   dataset_completeness <- purrr::map_dbl(baseline_datasets, function(ds) {
#'     sum(!is.na(ds$swan_participant_id))
#'   })
#'
#'   primary_dataset <- baseline_datasets[[which.max(dataset_completeness)]]
#'
#'   # Add data from other sources
#'   for (i in seq_along(baseline_datasets)) {
#'     if (i == which.max(dataset_completeness)) next
#'
#'     secondary_dataset <- baseline_datasets[[i]]
#'
#'     # Harmonize data types before merging
#'     secondary_dataset <- harmonize_data_types(secondary_dataset, primary_dataset, verbose)
#'
#'     primary_dataset <- primary_dataset %>%
#'       dplyr::full_join(
#'         secondary_dataset,
#'         by = "swan_participant_id",
#'         suffix = c("", "_secondary")
#'       ) %>%
#'       dplyr::mutate(
#'         # Use primary data preferentially with type-safe coalesce
#'         baseline_age_years = safe_coalesce_numeric(
#'           baseline_age_years,
#'           baseline_age_years_secondary
#'         ),
#'         baseline_bmi_kg_m2 = safe_coalesce_numeric(
#'           baseline_bmi_kg_m2,
#'           baseline_bmi_kg_m2_secondary
#'         ),
#'         baseline_parity_count = safe_coalesce_numeric(
#'           baseline_parity_count,
#'           baseline_parity_count_secondary
#'         ),
#'         baseline_smoking_status = safe_coalesce_character(
#'           baseline_smoking_status,
#'           baseline_smoking_status_secondary
#'         ),
#'         baseline_race_ethnicity = safe_coalesce_character(
#'           baseline_race_ethnicity,
#'           baseline_race_ethnicity_secondary
#'         ),
#'         baseline_menopausal_stage = safe_coalesce_character(
#'           baseline_menopausal_stage,
#'           baseline_menopausal_stage_secondary
#'         )
#'       ) %>%
#'       dplyr::select(-dplyr::ends_with("_secondary"))
#'   }
#'
#'   return(primary_dataset)
#' }
#'
#'
#' #' @noRd
#' harmonize_data_types <- function(secondary_dataset, primary_dataset, verbose) {
#'
#'   # Get common baseline columns
#'   baseline_columns <- names(secondary_dataset)[stringr::str_starts(names(secondary_dataset), "baseline_")]
#'
#'   for (col_name in baseline_columns) {
#'     if (col_name %in% names(primary_dataset)) {
#'
#'       primary_type <- class(primary_dataset[[col_name]])[1]
#'       secondary_type <- class(secondary_dataset[[col_name]])[1]
#'
#'       if (primary_type != secondary_type) {
#'         if (verbose) {
#'           logger::log_info("Harmonizing {col_name}: {secondary_type} -> {primary_type}")
#'         }
#'
#'         # Convert secondary to match primary type
#'         if (primary_type == "numeric" || primary_type == "double") {
#'           secondary_dataset[[col_name]] <- as.numeric(as.character(secondary_dataset[[col_name]]))
#'         } else if (primary_type == "character") {
#'           secondary_dataset[[col_name]] <- as.character(secondary_dataset[[col_name]])
#'         } else if (primary_type == "logical") {
#'           secondary_dataset[[col_name]] <- as.logical(secondary_dataset[[col_name]])
#'         }
#'       }
#'     }
#'   }
#'
#'   return(secondary_dataset)
#' }
#'
#'
#' #' @noRd
#' safe_coalesce_numeric <- function(x, y) {
#'   # Convert both to numeric if needed
#'   x_numeric <- if (is.numeric(x)) x else as.numeric(as.character(x))
#'   y_numeric <- if (is.numeric(y)) y else as.numeric(as.character(y))
#'   dplyr::coalesce(x_numeric, y_numeric)
#' }
#'
#'
#' #' @noRd
#' safe_coalesce_character <- function(x, y) {
#'   # Convert both to character if needed
#'   x_char <- if (is.character(x)) x else as.character(x)
#'   y_char <- if (is.character(y)) y else as.character(y)
#'   dplyr::coalesce(x_char, y_char)
#' }
#'
#'
#' #' @noRd
#' merge_baseline_with_involea <- function(baseline_data, involea_data, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Merging baseline demographics with INVOLEA data")
#'     logger::log_info("Baseline participants: {length(unique(baseline_data$swan_participant_id))}")
#'     logger::log_info("INVOLEA observations: {nrow(involea_data)}")
#'   }
#'
#'   # Merge datasets
#'   merged_data <- involea_data %>%
#'     dplyr::left_join(
#'       baseline_data %>% dplyr::select(-source_file),
#'       by = "swan_participant_id"
#'     )
#'
#'   # Add time variables
#'   merged_data <- merged_data %>%
#'     dplyr::mutate(
#'       years_since_baseline = visit_number
#'     ) %>%
#'     dplyr::arrange(swan_participant_id, visit_number)
#'
#'   if (verbose) {
#'     logger::log_info("Merged dataset: {nrow(merged_data)} observations")
#'     logger::log_info("Participants with both baseline + INVOLEA: {length(unique(merged_data$swan_participant_id[!is.na(merged_data$baseline_age_years)]))}")
#'   }
#'
#'   return(merged_data)
#' }
#'
#'
#' #' @noRd
#' format_final_dataset <- function(merged_data, format, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Formatting final dataset as: {format}")
#'   }
#'
#'   if (format == "long") {
#'     # Clean up but preserve all participant-visit combinations
#'     final_data <- merged_data %>%
#'       dplyr::select(
#'         swan_participant_id,
#'         visit_number,
#'         years_since_baseline,
#'         baseline_age_years,
#'         baseline_bmi_kg_m2,
#'         baseline_race_ethnicity,
#'         baseline_smoking_status,
#'         baseline_parity_count,
#'         baseline_menopausal_stage,
#'         involea_incontinence,
#'         involea_raw_response,
#'         involea_source_variable
#'       ) %>%
#'       # Only remove true duplicates (same participant, visit, AND response)
#'       # but keep different visits for the same participant
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         involea_source_variable,
#'         .keep_all = TRUE
#'       ) %>%
#'       dplyr::arrange(swan_participant_id, visit_number)
#'
#'   } else {
#'     # Convert to wide format
#'     final_data <- merged_data %>%
#'       dplyr::select(
#'         swan_participant_id,
#'         visit_number,
#'         baseline_age_years,
#'         baseline_bmi_kg_m2,
#'         baseline_race_ethnicity,
#'         baseline_smoking_status,
#'         baseline_parity_count,
#'         baseline_menopausal_stage,
#'         involea_incontinence
#'       ) %>%
#'       # Only remove true duplicates before pivoting
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         .keep_all = TRUE
#'       ) %>%
#'       tidyr::pivot_wider(
#'         names_from = visit_number,
#'         values_from = involea_incontinence,
#'         names_prefix = "involea_visit_"
#'       )
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Final formatted dataset: {nrow(final_data)} rows")
#'     logger::log_info("Unique participants: {length(unique(final_data$swan_participant_id))}")
#'     if (format == "long") {
#'       logger::log_info("Visit range: {min(final_data$visit_number, na.rm = TRUE)} to {max(final_data$visit_number, na.rm = TRUE)}")
#'     }
#'   }
#'
#'   return(final_data)
#' }
#'
#'
#' # Helper functions from previous code
#' #' @noRd
#' identify_participant_id_column <- function(dataset) {
#'   participant_id_patterns <- c("^SWANID$", "^ARCHID$", "^swanid$", "^archid$")
#'   for (pattern in participant_id_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' @noRd
#' identify_visit_column <- function(dataset) {
#'   visit_patterns <- c("^VISIT$", "^visit$", "^Visit$", "^VISITNO$")
#'   for (pattern in visit_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' @noRd
#' clean_race_variable <- function(race_raw) {
#'   dplyr::case_when(
#'     stringr::str_detect(as.character(race_raw), "White|Caucasian") ~ "White",
#'     stringr::str_detect(as.character(race_raw), "Black|African") ~ "Black",
#'     stringr::str_detect(as.character(race_raw), "Hispanic") ~ "Hispanic",
#'     stringr::str_detect(as.character(race_raw), "Chinese") ~ "Chinese",
#'     stringr::str_detect(as.character(race_raw), "Japanese") ~ "Japanese",
#'     TRUE ~ "Other"
#'   )
#' }
#'
#' #' @noRd
#' clean_smoking_variable <- function(smoking_raw) {
#'   dplyr::case_when(
#'     smoking_raw %in% c(1, "(1) Never smoked") ~ "Never",
#'     smoking_raw %in% c(2, "(2) Past smoker") ~ "Past",
#'     smoking_raw %in% c(3, "(3) Current smoker") ~ "Current",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' @noRd
#' clean_menopausal_variable <- function(status_raw) {
#'   dplyr::case_when(
#'     status_raw %in% c(2, "(2) Postmenopausal") ~ "Post",
#'     status_raw %in% c(3, "(3) Late perimenopausal") ~ "Late Peri",
#'     status_raw %in% c(4, "(4) Early perimenopausal") ~ "Early Peri",
#'     status_raw %in% c(5, "(5) Premenopausal") ~ "Pre",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' focused_dataset <- create_focused_swan_dataset(swan_data_directory,
#'                                         output_format = "long",
#'                                         verbose = TRUE)
#'
#' View(focused_dataset)
#'
#' write_csv(focused_dataset, "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/swan_focused_baseline_involea.csv")
#'
#' # Check incontinence prevalence by visit
#' focused_dataset %>%
#'   group_by(visit_number) %>%
#'   summarise(
#'     n = n(),
#'     prev = mean(involea_incontinence, na.rm = TRUE)
#'   )
#'
#' # Investigate the NaN visits
#' focused_dataset %>%
#'   filter(visit_number %in% c(6, 8, 9)) %>%
#'   group_by(visit_number) %>%
#'   summarise(
#'     n_total = n(),
#'     n_missing_incontinence = sum(is.na(involea_incontinence)),
#'     n_missing_raw = sum(is.na(involea_raw_response)),
#'     unique_raw_responses = list(unique(involea_raw_response))
#'   )
#'
#' # Check what the raw responses look like for these visits
#' focused_dataset %>%
#'   filter(visit_number %in% c(6, 8, 9)) %>%
#'   count(visit_number, involea_raw_response, involea_incontinence) %>%
#'   arrange(visit_number)
#'
#' # Overall data quality summary
#' data_quality_summary <- focused_dataset %>%
#'   group_by(visit_number) %>%
#'   summarise(
#'     participants = n(),
#'     incontinence_available = sum(!is.na(involea_incontinence)),
#'     incontinence_yes = sum(involea_incontinence == 1, na.rm = TRUE),
#'     incontinence_no = sum(involea_incontinence == 0, na.rm = TRUE),
#'     prevalence = round(mean(involea_incontinence, na.rm = TRUE), 3),
#'     source_variables = paste(unique(involea_source_variable), collapse = ", ")
#'   )
#'
#' print(data_quality_summary)
#'
#' # Function at 1701 ----
#' #' Create Focused SWAN Dataset: Baseline Demographics + INVOLEA Variables
#' #'
#' #' This function creates a clean, focused SWAN dataset containing baseline
#' #' demographic information (one row per participant) merged with urinary
#' #' incontinence (INVOLEA) variables across all visits in long format. This
#' #' structure is optimal for longitudinal analysis of urinary incontinence
#' #' while maintaining essential baseline covariates.
#' #'
#' #' @param swan_data_directory Character. Path to directory containing SWAN .rda
#' #'   files. The function will identify baseline demographic files and files
#' #'   containing INVOLEA variables across visits.
#' #' @param output_format Character. Format for the final dataset: "long" creates
#' #'   one row per participant-visit with INVOLEA data, "wide" creates one row
#' #'   per participant with INVOLEA variables spread across visits. Default "long".
#' #' @param verbose Logical. If TRUE (default), prints detailed logging about
#' #'   file processing, baseline demographics extraction, and INVOLEA variable
#' #'   identification across visits.
#' #'
#' #' @return A tibble containing focused SWAN data:
#' #'   For "long" format:
#' #'   \describe{
#' #'     \item{swan_participant_id}{Participant identifier}
#' #'     \item{visit_number}{Visit number (0=baseline, 1-15=follow-up)}
#' #'     \item{baseline_age_years}{Age at baseline (repeated for all visits)}
#' #'     \item{baseline_bmi_kg_m2}{BMI at baseline (repeated for all visits)}
#' #'     \item{baseline_race_ethnicity}{Race/ethnicity (repeated for all visits)}
#' #'     \item{baseline_smoking_status}{Smoking status at baseline}
#' #'     \item{baseline_parity_count}{Number of pregnancies at baseline}
#' #'     \item{baseline_menopausal_stage}{Menopausal stage at baseline}
#' #'     \item{involea_incontinence}{Binary incontinence variable (0/1)}
#' #'     \item{involea_raw_response}{Original INVOLEA response}
#' #'     \item{involea_source_variable}{Original variable name}
#' #'     \item{years_since_baseline}{Time since baseline visit}
#' #'   }
#' #'
#' #' @examples
#' #' # Example 1: Long format for longitudinal modeling
#' #' swan_long_dataset <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/Users/tylermuffly/Dropbox (Personal)/workforce/Dall_model/data/SWAN",
#' #'   output_format = "long",
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' # Example 2: Wide format for cross-sectional analysis
#' #' swan_wide_dataset <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "wide",
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' # Example 3: Silent processing for production pipelines
#' #' swan_analysis_ready <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "long",
#' #'   verbose = FALSE
#' #' )
#' #' # Expected output for long format:
#' #' # # A tibble: 21,629 × 12
#' #' #   swan_participant_id visit_number baseline_age_years baseline_bmi_kg_m2
#' #' #   <chr>                      <dbl>              <dbl>              <dbl>
#' #' # 1 10005                          0                 48               24.1
#' #' # 2 10005                          1                 48               24.1
#' #' # 3 10005                          3                 48               24.1
#' #' # # ... with 21,626 more rows and 8 more variables
#' #'
#' #' @importFrom dplyr filter select mutate left_join case_when
#' #' @importFrom dplyr group_by summarise ungroup arrange distinct
#' #' @importFrom tidyr pivot_longer pivot_wider
#' #' @importFrom stringr str_detect str_extract str_replace_all
#' #' @importFrom purrr map_dfr map_chr map_lgl keep
#' #' @importFrom assertthat assert_that
#' #' @importFrom logger log_info log_warn log_error
#' #' @importFrom tibble tibble as_tibble
#' #' @importFrom readr parse_number
#' #' @importFrom glue glue
#' #'
#' #' @export
#' create_focused_swan_dataset <- function(swan_data_directory,
#'                                         output_format = "long",
#'                                         verbose = TRUE) {
#'
#'   # Input validation
#'   if (verbose) {
#'     logger::log_info("Creating focused SWAN dataset: Baseline demographics + INVOLEA variables")
#'     logger::log_info("Input validation and setup")
#'   }
#'
#'   assertthat::assert_that(
#'     is.character(swan_data_directory),
#'     msg = "swan_data_directory must be a character string path"
#'   )
#'   assertthat::assert_that(
#'     dir.exists(swan_data_directory),
#'     msg = glue::glue("Directory does not exist: {swan_data_directory}")
#'   )
#'   assertthat::assert_that(
#'     output_format %in% c("long", "wide"),
#'     msg = "output_format must be 'long' or 'wide'"
#'   )
#'
#'   # Step 1: Extract baseline demographics
#'   baseline_demographics_data <- extract_baseline_demographics(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   # Step 2: Extract INVOLEA variables across all visits
#'   involea_longitudinal_data <- extract_involea_variables(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   # Step 3: Merge baseline demographics with INVOLEA data
#'   merged_focused_dataset <- merge_baseline_with_involea(
#'     baseline_data = baseline_demographics_data,
#'     involea_data = involea_longitudinal_data,
#'     verbose = verbose
#'   )
#'
#'   # Step 4: Format output according to user preference
#'   final_focused_dataset <- format_final_dataset(
#'     merged_data = merged_focused_dataset,
#'     format = output_format,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Focused SWAN dataset creation completed")
#'     logger::log_info("Final dataset: {nrow(final_focused_dataset)} rows × {ncol(final_focused_dataset)} columns")
#'   }
#'
#'   return(final_focused_dataset)
#' }
#'
#'
#' #' @noRd
#' extract_baseline_demographics <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting baseline demographics from SWAN files")
#'   }
#'
#'   # Find files likely to contain baseline demographics
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Searching {length(all_rda_files)} RDA files for baseline demographics")
#'   }
#'
#'   # Try to identify baseline/demographic files by name patterns
#'   baseline_file_patterns <- c("28762", "baseline", "demo", "04368")
#'
#'   baseline_candidates <- purrr::keep(all_rda_files, function(file_path) {
#'     file_name <- tolower(basename(file_path))
#'     any(stringr::str_detect(file_name, baseline_file_patterns))
#'   })
#'
#'   if (verbose) {
#'     logger::log_info("Found {length(baseline_candidates)} potential baseline files:")
#'     purrr::walk(baseline_candidates, ~logger::log_info("  - {basename(.x)}"))
#'   }
#'
#'   # Load and process baseline files
#'   baseline_datasets <- purrr::map(baseline_candidates, function(file_path) {
#'
#'     if (verbose) {
#'       logger::log_info("Processing baseline file: {basename(file_path)}")
#'     }
#'
#'     # Load file
#'     file_environment <- new.env()
#'     load(file_path, envir = file_environment)
#'     raw_baseline_data <- file_environment[[ls(file_environment)[1]]]
#'
#'     # Look for demographic variables
#'     demo_variables <- identify_demographic_variables(raw_baseline_data)
#'
#'     if (length(demo_variables$participant_id) == 0) {
#'       if (verbose) {
#'         logger::log_warn("No participant ID found in {basename(file_path)}")
#'       }
#'       return(NULL)
#'     }
#'
#'     # Extract and clean demographics
#'     cleaned_baseline <- extract_and_clean_demographics(
#'       dataset = raw_baseline_data,
#'       demo_vars = demo_variables,
#'       source_file = basename(file_path),
#'       verbose = verbose
#'     )
#'
#'     return(cleaned_baseline)
#'   })
#'
#'   # Combine baseline datasets
#'   baseline_datasets <- purrr::compact(baseline_datasets)
#'
#'   if (length(baseline_datasets) == 0) {
#'     logger::log_error("No valid baseline demographic data found")
#'     stop("No baseline demographic data could be extracted")
#'   }
#'
#'   # Merge multiple baseline sources if needed
#'   combined_baseline <- combine_baseline_sources(baseline_datasets, verbose)
#'
#'   if (verbose) {
#'     logger::log_info("Baseline demographics extracted: {nrow(combined_baseline)} participants")
#'   }
#'
#'   return(combined_baseline)
#' }
#'
#'
#' #' @noRd
#' extract_involea_variables <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting INVOLEA variables across all visits")
#'   }
#'
#'   # Find all RDA files
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   # Process each file to find INVOLEA variables
#'   involea_datasets <- purrr::map(all_rda_files, function(file_path) {
#'
#'     if (verbose) {
#'       logger::log_info("Scanning {basename(file_path)} for INVOLEA variables")
#'     }
#'
#'     # Load file
#'     file_environment <- new.env()
#'     tryCatch({
#'       load(file_path, envir = file_environment)
#'     }, error = function(e) {
#'       if (verbose) {
#'         logger::log_warn("Failed to load {basename(file_path)}: {e$message}")
#'       }
#'       return(NULL)
#'     })
#'
#'     if (length(ls(file_environment)) == 0) return(NULL)
#'
#'     raw_dataset <- file_environment[[ls(file_environment)[1]]]
#'
#'     # Look for INVOLEA variables
#'     involea_columns <- identify_involea_variables(raw_dataset)
#'
#'     if (length(involea_columns) == 0) {
#'       return(NULL)  # No INVOLEA variables in this file
#'     }
#'
#'     if (verbose) {
#'       logger::log_info("  Found {length(involea_columns)} INVOLEA variables: {paste(involea_columns, collapse = ', ')}")
#'     }
#'
#'     # Extract INVOLEA data
#'     involea_data <- extract_involea_data(
#'       dataset = raw_dataset,
#'       involea_vars = involea_columns,
#'       source_file = basename(file_path),
#'       verbose = verbose
#'     )
#'
#'     return(involea_data)
#'   })
#'
#'   # Combine all INVOLEA datasets
#'   involea_datasets <- purrr::compact(involea_datasets)
#'
#'   if (length(involea_datasets) == 0) {
#'     logger::log_error("No INVOLEA variables found in any files")
#'     stop("No INVOLEA variables could be found")
#'   }
#'
#'   combined_involea <- purrr::map_dfr(involea_datasets, ~.x)
#'
#'   # Remove duplicates (same participant, visit, and variable)
#'   combined_involea <- combined_involea %>%
#'     dplyr::distinct(
#'       swan_participant_id,
#'       visit_number,
#'       involea_source_variable,
#'       .keep_all = TRUE
#'     )
#'
#'   if (verbose) {
#'     logger::log_info("INVOLEA variables extracted: {nrow(combined_involea)} observations")
#'     logger::log_info("Visit range: {min(combined_involea$visit_number, na.rm = TRUE)} to {max(combined_involea$visit_number, na.rm = TRUE)}")
#'     logger::log_info("Unique participants with INVOLEA: {length(unique(combined_involea$swan_participant_id))}")
#'   }
#'
#'   return(combined_involea)
#' }
#'
#'
#' #' @noRd
#' identify_demographic_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Identify participant ID
#'   participant_id_patterns <- c("SWANID", "ARCHID", "swanid", "archid")
#'   participant_id_col <- NULL
#'   for (pattern in participant_id_patterns) {
#'     if (pattern %in% variable_names) {
#'       participant_id_col <- pattern
#'       break
#'     }
#'   }
#'
#'   # Identify demographic variables
#'   age_vars <- variable_names[stringr::str_detect(variable_names, "AGE|age")]
#'   bmi_vars <- variable_names[stringr::str_detect(variable_names, "BMI|bmi")]
#'   race_vars <- variable_names[stringr::str_detect(variable_names, "RACE|race")]
#'   smoking_vars <- variable_names[stringr::str_detect(variable_names, "SMOKE|smoke")]
#'   parity_vars <- variable_names[stringr::str_detect(variable_names, "PREG|CHILD|preg|child|parity")]
#'   menopausal_vars <- variable_names[stringr::str_detect(variable_names, "STATUS|MENO|status|meno")]
#'
#'   return(list(
#'     participant_id = participant_id_col,
#'     age = age_vars,
#'     bmi = bmi_vars,
#'     race = race_vars,
#'     smoking = smoking_vars,
#'     parity = parity_vars,
#'     menopausal = menopausal_vars
#'   ))
#' }
#'
#'
#' #' @noRd
#' identify_involea_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Look for INVOLEA variables (including LEKINVO variants)
#'   involea_patterns <- c("INVOLEA", "LEKINVO", "involea", "lekinvo")
#'
#'   involea_variables <- c()
#'   for (pattern in involea_patterns) {
#'     matching_vars <- variable_names[stringr::str_detect(variable_names, pattern)]
#'     involea_variables <- c(involea_variables, matching_vars)
#'   }
#'
#'   return(unique(involea_variables))
#' }
#'
#'
#' #' @noRd
#' extract_and_clean_demographics <- function(dataset, demo_vars, source_file, verbose) {
#'
#'   # Start with participant ID
#'   cleaned_data <- tibble::as_tibble(dataset) %>%
#'     dplyr::select(dplyr::all_of(demo_vars$participant_id))
#'
#'   names(cleaned_data)[1] <- "swan_participant_id"
#'
#'   # Add demographic variables if available - ensuring consistent data types
#'   if (length(demo_vars$age) > 0) {
#'     cleaned_data$baseline_age_years <- as.numeric(as.character(dataset[[demo_vars$age[1]]]))
#'   }
#'
#'   if (length(demo_vars$bmi) > 0) {
#'     cleaned_data$baseline_bmi_kg_m2 <- as.numeric(as.character(dataset[[demo_vars$bmi[1]]]))
#'   }
#'
#'   if (length(demo_vars$race) > 0) {
#'     cleaned_data$baseline_race_ethnicity <- clean_race_variable(dataset[[demo_vars$race[1]]])
#'   }
#'
#'   if (length(demo_vars$smoking) > 0) {
#'     cleaned_data$baseline_smoking_status <- clean_smoking_variable(dataset[[demo_vars$smoking[1]]])
#'   }
#'
#'   if (length(demo_vars$parity) > 0) {
#'     cleaned_data$baseline_parity_count <- as.numeric(as.character(dataset[[demo_vars$parity[1]]]))
#'   }
#'
#'   if (length(demo_vars$menopausal) > 0) {
#'     cleaned_data$baseline_menopausal_stage <- clean_menopausal_variable(dataset[[demo_vars$menopausal[1]]])
#'   }
#'
#'   # Standardize participant ID
#'   cleaned_data$swan_participant_id <- as.character(cleaned_data$swan_participant_id)
#'   cleaned_data$source_file <- source_file
#'
#'   if (verbose) {
#'     logger::log_info("  Extracted {nrow(cleaned_data)} participants with {ncol(cleaned_data)-2} demographic variables")
#'   }
#'
#'   return(cleaned_data)
#' }
#'
#'
#' #' @noRd
#' extract_involea_data <- function(dataset, involea_vars, source_file, verbose) {
#'
#'   # Identify participant ID and visit variables
#'   participant_id_col <- identify_participant_id_column(dataset)
#'   visit_col <- identify_visit_column(dataset)
#'
#'   if (is.null(participant_id_col)) {
#'     if (verbose) {
#'       logger::log_warn("No participant ID found in {source_file}")
#'     }
#'     return(NULL)
#'   }
#'
#'   # Select relevant columns
#'   core_columns <- c(participant_id_col)
#'   if (!is.null(visit_col)) {
#'     core_columns <- c(core_columns, visit_col)
#'   }
#'
#'   involea_dataset <- tibble::as_tibble(dataset) %>%
#'     dplyr::select(dplyr::all_of(c(core_columns, involea_vars)))
#'
#'   # Standardize column names
#'   names(involea_dataset)[1] <- "swan_participant_id"
#'   if (!is.null(visit_col)) {
#'     names(involea_dataset)[2] <- "visit_number_from_file"
#'   } else {
#'     involea_dataset$visit_number_from_file <- NA_real_
#'   }
#'
#'   # Convert to long format
#'   involea_long <- involea_dataset %>%
#'     tidyr::pivot_longer(
#'       cols = dplyr::all_of(involea_vars),
#'       names_to = "involea_source_variable",
#'       values_to = "involea_raw_response"
#'     ) %>%
#'     dplyr::mutate(
#'       swan_participant_id = as.character(swan_participant_id),
#'       # ALWAYS extract visit number from variable name since SWAN files
#'       # have one variable per visit per file
#'       visit_number = extract_visit_number_from_variable_name(involea_source_variable),
#'       involea_incontinence = clean_involea_response(involea_raw_response),
#'       source_file = source_file
#'     ) %>%
#'     dplyr::select(-visit_number_from_file) %>%
#'     dplyr::filter(!is.na(involea_raw_response))  # Remove missing responses
#'
#'   # Debug logging to see what visit numbers we actually extracted
#'   if (verbose && nrow(involea_long) > 0) {
#'     unique_visits <- sort(unique(involea_long$visit_number))
#'     logger::log_info("  Extracted visits for {source_file}: {paste(unique_visits, collapse = ', ')}")
#'     logger::log_info("  Observations in {source_file}: {nrow(involea_long)}")
#'   }
#'
#'   return(involea_long)
#' }
#'
#'
#' #' @noRd
#' extract_visit_number_from_variable_name <- function(variable_names) {
#'
#'   # Simple and robust extraction of numbers at end of variable names
#'   visit_numbers <- stringr::str_extract(variable_names, "\\d+$")
#'   visit_numbers <- as.numeric(visit_numbers)
#'
#'   # Replace any NAs with 0 (baseline)
#'   visit_numbers[is.na(visit_numbers)] <- 0
#'
#'   return(visit_numbers)
#' }
#'
#'
#' #' @noRd
#' clean_involea_response <- function(response_raw) {
#'
#'   dplyr::case_when(
#'     # Standard formats
#'     response_raw %in% c("(2) Yes", "2", 2, "Yes", "YES", "yes") ~ 1,
#'     response_raw %in% c("(1) No", "1", 1, "No", "NO", "no") ~ 0,
#'     # Extended formats found in visits 6, 8, 9
#'     response_raw %in% c("(2) 2: Yes", "(2)2: Yes", "2: Yes") ~ 1,
#'     response_raw %in% c("(1) 1: No", "(1)1: No", "1: No") ~ 0,
#'     TRUE ~ NA_real_
#'   )
#' }
#'
#'
#' #' @noRd
#' combine_baseline_sources <- function(baseline_datasets, verbose) {
#'
#'   if (length(baseline_datasets) == 1) {
#'     return(baseline_datasets[[1]])
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Combining {length(baseline_datasets)} baseline sources")
#'   }
#'
#'   # Start with the most complete dataset
#'   dataset_completeness <- purrr::map_dbl(baseline_datasets, function(ds) {
#'     sum(!is.na(ds$swan_participant_id))
#'   })
#'
#'   primary_dataset <- baseline_datasets[[which.max(dataset_completeness)]]
#'
#'   # Add data from other sources
#'   for (i in seq_along(baseline_datasets)) {
#'     if (i == which.max(dataset_completeness)) next
#'
#'     secondary_dataset <- baseline_datasets[[i]]
#'
#'     # Harmonize data types before merging
#'     secondary_dataset <- harmonize_data_types(secondary_dataset, primary_dataset, verbose)
#'
#'     primary_dataset <- primary_dataset %>%
#'       dplyr::full_join(
#'         secondary_dataset,
#'         by = "swan_participant_id",
#'         suffix = c("", "_secondary")
#'       ) %>%
#'       dplyr::mutate(
#'         # Use primary data preferentially with type-safe coalesce
#'         baseline_age_years = safe_coalesce_numeric(
#'           baseline_age_years,
#'           baseline_age_years_secondary
#'         ),
#'         baseline_bmi_kg_m2 = safe_coalesce_numeric(
#'           baseline_bmi_kg_m2,
#'           baseline_bmi_kg_m2_secondary
#'         ),
#'         baseline_parity_count = safe_coalesce_numeric(
#'           baseline_parity_count,
#'           baseline_parity_count_secondary
#'         ),
#'         baseline_smoking_status = safe_coalesce_character(
#'           baseline_smoking_status,
#'           baseline_smoking_status_secondary
#'         ),
#'         baseline_race_ethnicity = safe_coalesce_character(
#'           baseline_race_ethnicity,
#'           baseline_race_ethnicity_secondary
#'         ),
#'         baseline_menopausal_stage = safe_coalesce_character(
#'           baseline_menopausal_stage,
#'           baseline_menopausal_stage_secondary
#'         )
#'       ) %>%
#'       dplyr::select(-dplyr::ends_with("_secondary"))
#'   }
#'
#'   return(primary_dataset)
#' }
#'
#'
#' #' @noRd
#' harmonize_data_types <- function(secondary_dataset, primary_dataset, verbose) {
#'
#'   # Get common baseline columns
#'   baseline_columns <- names(secondary_dataset)[stringr::str_starts(names(secondary_dataset), "baseline_")]
#'
#'   for (col_name in baseline_columns) {
#'     if (col_name %in% names(primary_dataset)) {
#'
#'       primary_type <- class(primary_dataset[[col_name]])[1]
#'       secondary_type <- class(secondary_dataset[[col_name]])[1]
#'
#'       if (primary_type != secondary_type) {
#'         if (verbose) {
#'           logger::log_info("Harmonizing {col_name}: {secondary_type} -> {primary_type}")
#'         }
#'
#'         # Convert secondary to match primary type
#'         if (primary_type == "numeric" || primary_type == "double") {
#'           secondary_dataset[[col_name]] <- as.numeric(as.character(secondary_dataset[[col_name]]))
#'         } else if (primary_type == "character") {
#'           secondary_dataset[[col_name]] <- as.character(secondary_dataset[[col_name]])
#'         } else if (primary_type == "logical") {
#'           secondary_dataset[[col_name]] <- as.logical(secondary_dataset[[col_name]])
#'         }
#'       }
#'     }
#'   }
#'
#'   return(secondary_dataset)
#' }
#'
#'
#' #' @noRd
#' safe_coalesce_numeric <- function(x, y) {
#'   # Convert both to numeric if needed
#'   x_numeric <- if (is.numeric(x)) x else as.numeric(as.character(x))
#'   y_numeric <- if (is.numeric(y)) y else as.numeric(as.character(y))
#'   dplyr::coalesce(x_numeric, y_numeric)
#' }
#'
#'
#' #' @noRd
#' safe_coalesce_character <- function(x, y) {
#'   # Convert both to character if needed
#'   x_char <- if (is.character(x)) x else as.character(x)
#'   y_char <- if (is.character(y)) y else as.character(y)
#'   dplyr::coalesce(x_char, y_char)
#' }
#'
#'
#' #' @noRd
#' merge_baseline_with_involea <- function(baseline_data, involea_data, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Merging baseline demographics with INVOLEA data")
#'     logger::log_info("Baseline participants: {length(unique(baseline_data$swan_participant_id))}")
#'     logger::log_info("INVOLEA observations: {nrow(involea_data)}")
#'   }
#'
#'   # Merge datasets
#'   merged_data <- involea_data %>%
#'     dplyr::left_join(
#'       baseline_data %>% dplyr::select(-source_file),
#'       by = "swan_participant_id"
#'     )
#'
#'   # Add time variables
#'   merged_data <- merged_data %>%
#'     dplyr::mutate(
#'       years_since_baseline = visit_number
#'     ) %>%
#'     dplyr::arrange(swan_participant_id, visit_number)
#'
#'   if (verbose) {
#'     logger::log_info("Merged dataset: {nrow(merged_data)} observations")
#'     logger::log_info("Participants with both baseline + INVOLEA: {length(unique(merged_data$swan_participant_id[!is.na(merged_data$baseline_age_years)]))}")
#'   }
#'
#'   return(merged_data)
#' }
#'
#'
#' #' @noRd
#' format_final_dataset <- function(merged_data, format, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Formatting final dataset as: {format}")
#'   }
#'
#'   if (format == "long") {
#'     # Clean up but preserve all participant-visit combinations
#'     final_data <- merged_data %>%
#'       dplyr::select(
#'         swan_participant_id,
#'         visit_number,
#'         years_since_baseline,
#'         baseline_age_years,
#'         baseline_bmi_kg_m2,
#'         baseline_race_ethnicity,
#'         baseline_smoking_status,
#'         baseline_parity_count,
#'         baseline_menopausal_stage,
#'         involea_incontinence,
#'         involea_raw_response,
#'         involea_source_variable
#'       ) %>%
#'       # Only remove true duplicates (same participant, visit, AND response)
#'       # but keep different visits for the same participant
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         involea_source_variable,
#'         .keep_all = TRUE
#'       ) %>%
#'       dplyr::arrange(swan_participant_id, visit_number)
#'
#'   } else {
#'     # Convert to wide format
#'     final_data <- merged_data %>%
#'       dplyr::select(
#'         swan_participant_id,
#'         visit_number,
#'         baseline_age_years,
#'         baseline_bmi_kg_m2,
#'         baseline_race_ethnicity,
#'         baseline_smoking_status,
#'         baseline_parity_count,
#'         baseline_menopausal_stage,
#'         involea_incontinence
#'       ) %>%
#'       # Only remove true duplicates before pivoting
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         .keep_all = TRUE
#'       ) %>%
#'       tidyr::pivot_wider(
#'         names_from = visit_number,
#'         values_from = involea_incontinence,
#'         names_prefix = "involea_visit_"
#'       )
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Final formatted dataset: {nrow(final_data)} rows")
#'     logger::log_info("Unique participants: {length(unique(final_data$swan_participant_id))}")
#'     if (format == "long") {
#'       logger::log_info("Visit range: {min(final_data$visit_number, na.rm = TRUE)} to {max(final_data$visit_number, na.rm = TRUE)}")
#'     }
#'   }
#'
#'   return(final_data)
#' }
#'
#'
#' # Helper functions from previous code
#' #' @noRd
#' identify_participant_id_column <- function(dataset) {
#'   participant_id_patterns <- c("^SWANID$", "^ARCHID$", "^swanid$", "^archid$")
#'   for (pattern in participant_id_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' @noRd
#' identify_visit_column <- function(dataset) {
#'   visit_patterns <- c("^VISIT$", "^visit$", "^Visit$", "^VISITNO$")
#'   for (pattern in visit_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' @noRd
#' clean_race_variable <- function(race_raw) {
#'   dplyr::case_when(
#'     stringr::str_detect(as.character(race_raw), "White|Caucasian") ~ "White",
#'     stringr::str_detect(as.character(race_raw), "Black|African") ~ "Black",
#'     stringr::str_detect(as.character(race_raw), "Hispanic") ~ "Hispanic",
#'     stringr::str_detect(as.character(race_raw), "Chinese") ~ "Chinese",
#'     stringr::str_detect(as.character(race_raw), "Japanese") ~ "Japanese",
#'     TRUE ~ "Other"
#'   )
#' }
#'
#' #' @noRd
#' clean_smoking_variable <- function(smoking_raw) {
#'   dplyr::case_when(
#'     smoking_raw %in% c(1, "(1) Never smoked") ~ "Never",
#'     smoking_raw %in% c(2, "(2) Past smoker") ~ "Past",
#'     smoking_raw %in% c(3, "(3) Current smoker") ~ "Current",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' @noRd
#' clean_menopausal_variable <- function(status_raw) {
#'   dplyr::case_when(
#'     status_raw %in% c(2, "(2) Postmenopausal") ~ "Post",
#'     status_raw %in% c(3, "(3) Late perimenopausal") ~ "Late Peri",
#'     status_raw %in% c(4, "(4) Early perimenopausal") ~ "Early Peri",
#'     status_raw %in% c(5, "(5) Premenopausal") ~ "Pre",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' # Regenerate the focused dataset with the fixed cleaning function
#' focused_dataset_fixed <- create_focused_swan_dataset(
#'   swan_data_directory = swan_directory,
#'   output_format = "long",
#'   verbose = TRUE
#' )
#'
#' # Check the prevalence again
#' focused_dataset_fixed %>%
#'   group_by(visit_number) %>%
#'   summarise(
#'     n = n(),
#'     prev = round(mean(involea_incontinence, na.rm = TRUE), 3),
#'     missing = sum(is.na(involea_incontinence))
#'   )
#'
#' # Save the final, clean dataset
#' write_csv(focused_dataset_fixed, "swan_final_baseline_involea.csv")
#' save(focused_dataset_fixed, file = "swan_final_baseline_involea.rda")
#'
#' # Optional: Create a summary for documentation
#' summary_stats <- focused_dataset_fixed %>%
#'   group_by(visit_number) %>%
#'   summarise(
#'     participants = n(),
#'     prevalence = round(mean(involea_incontinence), 3),
#'     age_mean = round(mean(baseline_age_years, na.rm = TRUE), 1),
#'     years_followup = unique(years_since_baseline)
#'   )
#'
#' write_csv(summary_stats, "swan_prevalence_summary.csv")
#'
#' # Looking for age -----
#' # Investigate age variables and participant patterns across SWAN files
#'
#' library(tidyverse)
#' library(stringr)
#'
#' swan_directory <- "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/"
#'
#' # Get all SWAN files with INVOLEA data
#' involea_files <- list.files(
#'   path = swan_directory,
#'   pattern = "\\.rda$",
#'   full.names = TRUE
#' )[c(2:12)]  # Files that had INVOLEA variables
#'
#' cat("=== INVESTIGATING AGE VARIABLES AND PARTICIPANT PATTERNS ===\n\n")
#'
#' visit_info <- tibble()
#'
#' for (i in seq_along(involea_files)) {
#'   file_path <- involea_files[i]
#'   file_name <- basename(file_path)
#'
#'   cat("File", i, ":", file_name, "\n")
#'
#'   # Load file
#'   env <- new.env()
#'   load(file_path, envir = env)
#'   data <- env[[ls(env)[1]]]
#'
#'   # Look for age variables
#'   age_vars <- grep("AGE|age", names(data), value = TRUE, ignore.case = TRUE)
#'
#'   # Look for ID variables
#'   id_vars <- grep("SWANID|ARCHID|ID", names(data), value = TRUE, ignore.case = TRUE)
#'
#'   # Look for INVOLEA variables
#'   involea_vars <- grep("INVOLEA|LEKINVO", names(data), value = TRUE)
#'
#'   # Get basic info
#'   n_rows <- nrow(data)
#'   n_cols <- ncol(data)
#'
#'   cat("  Rows:", n_rows, "| Columns:", n_cols, "\n")
#'   cat("  ID variables:", paste(id_vars, collapse = ", "), "\n")
#'   cat("  Age variables:", paste(age_vars, collapse = ", "), "\n")
#'   cat("  INVOLEA variables:", paste(involea_vars, collapse = ", "), "\n")
#'
#'   # Extract visit number from filename or INVOLEA variable
#'   if (length(involea_vars) > 0) {
#'     visit_num <- str_extract(involea_vars[1], "\\d+$")
#'     visit_num <- as.numeric(visit_num)
#'   } else {
#'     visit_num <- NA
#'   }
#'
#'   # Check age data if available
#'   if (length(age_vars) > 0 && length(id_vars) > 0) {
#'     age_summary <- data %>%
#'       select(all_of(c(id_vars[1], age_vars[1]))) %>%
#'       summarise(
#'         age_mean = round(mean(.data[[age_vars[1]]], na.rm = TRUE), 1),
#'         age_min = min(.data[[age_vars[1]]], na.rm = TRUE),
#'         age_max = max(.data[[age_vars[1]]], na.rm = TRUE),
#'         age_missing = sum(is.na(.data[[age_vars[1]]]))
#'       )
#'
#'     cat("  Age summary - Mean:", age_summary$age_mean,
#'         "| Range:", age_summary$age_min, "-", age_summary$age_max,
#'         "| Missing:", age_summary$age_missing, "\n")
#'   }
#'
#'   # Count participants with INVOLEA data
#'   if (length(involea_vars) > 0 && length(id_vars) > 0) {
#'     involea_count <- data %>%
#'       select(all_of(c(id_vars[1], involea_vars[1]))) %>%
#'       filter(!is.na(.data[[involea_vars[1]]])) %>%
#'       nrow()
#'
#'     cat("  Participants with INVOLEA data:", involea_count, "\n")
#'   }
#'
#'   # Store info for summary
#'   visit_info <- bind_rows(visit_info, tibble(
#'     file_name = file_name,
#'     visit_number = visit_num,
#'     total_rows = n_rows,
#'     age_variables = paste(age_vars, collapse = ", "),
#'     involea_participants = if(exists("involea_count")) involea_count else NA
#'   ))
#'
#'   cat("\n")
#' }
#'
#' cat("=== SUMMARY TABLE ===\n")
#' print(visit_info)
#'
#' # Check for participant overlap between visits
#' cat("\n=== PARTICIPANT OVERLAP ANALYSIS ===\n")
#'
#' # Load a couple of files to check participant overlap
#' if (length(involea_files) >= 3) {
#'
#'   # Load visit 8 file (low N)
#'   env8 <- new.env()
#'   load(involea_files[9], envir = env8)  # Assuming 9th file is visit 8
#'   data8 <- env8[[ls(env8)[1]]]
#'
#'   # Load visit 10 file (high N)
#'   env10 <- new.env()
#'   load(involea_files[11], envir = env10)  # Assuming 11th file is visit 10
#'   data10 <- env10[[ls(env10)[1]]]
#'
#'   # Get participant IDs
#'   id_col8 <- grep("SWANID|ARCHID", names(data8), value = TRUE)[1]
#'   id_col10 <- grep("SWANID|ARCHID", names(data10), value = TRUE)[1]
#'
#'   if (!is.na(id_col8) && !is.na(id_col10)) {
#'     ids_visit8 <- data8[[id_col8]]
#'     ids_visit10 <- data10[[id_col10]]
#'
#'     cat("Visit 8 participants:", length(unique(ids_visit8)), "\n")
#'     cat("Visit 10 participants:", length(unique(ids_visit10)), "\n")
#'     cat("Overlap (participants in both):", length(intersect(ids_visit8, ids_visit10)), "\n")
#'     cat("Only in visit 8:", length(setdiff(ids_visit8, ids_visit10)), "\n")
#'     cat("Only in visit 10:", length(setdiff(ids_visit10, ids_visit8)), "\n")
#'   }
#' }
#'
#' # Check the participant count progression in your current dataset
#' participant_progression <- focused_dataset_with_aging %>%
#'   group_by(visit_number) %>%
#'   summarise(
#'     participants = n(),
#'     unique_participants = n_distinct(swan_participant_id)
#'   ) %>%
#'   arrange(visit_number)
#'
#' print(participant_progression)
#'
#' # Check which participants appear in which visits
#' participant_visits <- focused_dataset_with_aging %>%
#'   group_by(swan_participant_id) %>%
#'   summarise(
#'     visits_attended = list(sort(visit_number)),
#'     n_visits = n(),
#'     first_visit = min(visit_number),
#'     last_visit = max(visit_number)
#'   )
#'
#' # Look at visit patterns
#' cat("Visit attendance patterns:\n")
#' participant_visits %>%
#'   count(n_visits) %>%
#'   arrange(desc(n)) %>%
#'   print()
#'
#' # Check specifically for the visit 8 vs 10 anomaly
#' visit_8_participants <- focused_dataset_with_aging %>%
#'   filter(visit_number == 8) %>%
#'   pull(swan_participant_id)
#'
#' visit_10_participants <- focused_dataset_with_aging %>%
#'   filter(visit_number == 10) %>%
#'   pull(swan_participant_id)
#'
#' cat("\nVisit 8 vs 10 comparison:\n")
#' cat("Visit 8 participants:", length(visit_8_participants), "\n")
#' cat("Visit 10 participants:", length(visit_10_participants), "\n")
#' cat("Overlap:", length(intersect(visit_8_participants, visit_10_participants)), "\n")
#' cat("Only in visit 10:", length(setdiff(visit_10_participants, visit_8_participants)), "\n")
#'
#'
#' # Function at 1711 -----
#' #' Create Focused SWAN Dataset: Baseline Demographics + INVOLEA Variables
#' #'
#' #' This function creates a clean, focused SWAN dataset containing baseline
#' #' demographic information (one row per participant) merged with urinary
#' #' incontinence (INVOLEA) variables across all visits in long format. This
#' #' structure is optimal for longitudinal analysis of urinary incontinence
#' #' while maintaining essential baseline covariates.
#' #'
#' #' @param swan_data_directory Character. Path to directory containing SWAN .rda
#' #'   files. The function will identify baseline demographic files and files
#' #'   containing INVOLEA variables across visits.
#' #' @param output_format Character. Format for the final dataset: "long" creates
#' #'   one row per participant-visit with INVOLEA data, "wide" creates one row
#' #'   per participant with INVOLEA variables spread across visits. Default "long".
#' #' @param verbose Logical. If TRUE (default), prints detailed logging about
#' #'   file processing, baseline demographics extraction, and INVOLEA variable
#' #'   identification across visits.
#' #'
#' #' @return A tibble containing focused SWAN data:
#' #'   For "long" format:
#' #'   \describe{
#' #'     \item{swan_participant_id}{Participant identifier}
#' #'     \item{visit_number}{Visit number (0=baseline, 1-15=follow-up)}
#' #'     \item{baseline_age_years}{Age at baseline (repeated for all visits)}
#' #'     \item{baseline_bmi_kg_m2}{BMI at baseline (repeated for all visits)}
#' #'     \item{baseline_race_ethnicity}{Race/ethnicity (repeated for all visits)}
#' #'     \item{baseline_smoking_status}{Smoking status at baseline}
#' #'     \item{baseline_parity_count}{Number of pregnancies at baseline}
#' #'     \item{baseline_menopausal_stage}{Menopausal stage at baseline}
#' #'     \item{involea_incontinence}{Binary incontinence variable (0/1)}
#' #'     \item{involea_raw_response}{Original INVOLEA response}
#' #'     \item{involea_source_variable}{Original variable name}
#' #'     \item{years_since_baseline}{Time since baseline visit}
#' #'   }
#' #'
#' #' @examples
#' #' # Example 1: Long format for longitudinal modeling
#' #' swan_long_dataset <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/Users/tylermuffly/Dropbox (Personal)/workforce/Dall_model/data/SWAN",
#' #'   output_format = "long",
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' # Example 2: Wide format for cross-sectional analysis
#' #' swan_wide_dataset <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "wide",
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' # Example 3: Silent processing for production pipelines
#' #' swan_analysis_ready <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "long",
#' #'   verbose = FALSE
#' #' )
#' #' # Expected output for long format:
#' #' # # A tibble: 21,629 × 12
#' #' #   swan_participant_id visit_number baseline_age_years baseline_bmi_kg_m2
#' #' #   <chr>                      <dbl>              <dbl>              <dbl>
#' #' # 1 10005                          0                 48               24.1
#' #' # 2 10005                          1                 48               24.1
#' #' # 3 10005                          3                 48               24.1
#' #' # # ... with 21,626 more rows and 8 more variables
#' #'
#' #' @importFrom dplyr filter select mutate left_join case_when
#' #' @importFrom dplyr group_by summarise ungroup arrange distinct
#' #' @importFrom tidyr pivot_longer pivot_wider
#' #' @importFrom stringr str_detect str_extract str_replace_all
#' #' @importFrom purrr map_dfr map_chr map_lgl keep
#' #' @importFrom assertthat assert_that
#' #' @importFrom logger log_info log_warn log_error
#' #' @importFrom tibble tibble as_tibble
#' #' @importFrom readr parse_number
#' #' @importFrom glue glue
#' #'
#' #' @export
#' create_focused_swan_dataset <- function(swan_data_directory,
#'                                         output_format = "long",
#'                                         verbose = TRUE) {
#'
#'   # Input validation
#'   if (verbose) {
#'     logger::log_info("Creating focused SWAN dataset: Baseline demographics + INVOLEA variables")
#'     logger::log_info("Input validation and setup")
#'   }
#'
#'   assertthat::assert_that(
#'     is.character(swan_data_directory),
#'     msg = "swan_data_directory must be a character string path"
#'   )
#'   assertthat::assert_that(
#'     dir.exists(swan_data_directory),
#'     msg = glue::glue("Directory does not exist: {swan_data_directory}")
#'   )
#'   assertthat::assert_that(
#'     output_format %in% c("long", "wide"),
#'     msg = "output_format must be 'long' or 'wide'"
#'   )
#'
#'   # Step 1: Extract baseline demographics
#'   baseline_demographics_data <- extract_baseline_demographics(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   # Step 2: Extract INVOLEA variables across all visits
#'   involea_longitudinal_data <- extract_involea_variables(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   # Step 3: Merge baseline demographics with INVOLEA data
#'   merged_focused_dataset <- merge_baseline_with_involea(
#'     baseline_data = baseline_demographics_data,
#'     involea_data = involea_longitudinal_data,
#'     verbose = verbose
#'   )
#'
#'   # Step 4: Format output according to user preference
#'   final_focused_dataset <- format_final_dataset(
#'     merged_data = merged_focused_dataset,
#'     format = output_format,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Focused SWAN dataset creation completed")
#'     logger::log_info("Final dataset: {nrow(final_focused_dataset)} rows × {ncol(final_focused_dataset)} columns")
#'   }
#'
#'   return(final_focused_dataset)
#' }
#'
#'
#' #' @noRd
#' extract_baseline_demographics <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting baseline demographics from SWAN files")
#'   }
#'
#'   # Find files likely to contain baseline demographics
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Searching {length(all_rda_files)} RDA files for baseline demographics")
#'   }
#'
#'   # Try to identify baseline/demographic files by name patterns
#'   baseline_file_patterns <- c("28762", "baseline", "demo", "04368")
#'
#'   baseline_candidates <- purrr::keep(all_rda_files, function(file_path) {
#'     file_name <- tolower(basename(file_path))
#'     any(stringr::str_detect(file_name, baseline_file_patterns))
#'   })
#'
#'   if (verbose) {
#'     logger::log_info("Found {length(baseline_candidates)} potential baseline files:")
#'     purrr::walk(baseline_candidates, ~logger::log_info("  - {basename(.x)}"))
#'   }
#'
#'   # Load and process baseline files
#'   baseline_datasets <- purrr::map(baseline_candidates, function(file_path) {
#'
#'     if (verbose) {
#'       logger::log_info("Processing baseline file: {basename(file_path)}")
#'     }
#'
#'     # Load file
#'     file_environment <- new.env()
#'     load(file_path, envir = file_environment)
#'     raw_baseline_data <- file_environment[[ls(file_environment)[1]]]
#'
#'     # Look for demographic variables
#'     demo_variables <- identify_demographic_variables(raw_baseline_data)
#'
#'     if (length(demo_variables$participant_id) == 0) {
#'       if (verbose) {
#'         logger::log_warn("No participant ID found in {basename(file_path)}")
#'       }
#'       return(NULL)
#'     }
#'
#'     # Extract and clean demographics
#'     cleaned_baseline <- extract_and_clean_demographics(
#'       dataset = raw_baseline_data,
#'       demo_vars = demo_variables,
#'       source_file = basename(file_path),
#'       verbose = verbose
#'     )
#'
#'     return(cleaned_baseline)
#'   })
#'
#'   # Combine baseline datasets
#'   baseline_datasets <- purrr::compact(baseline_datasets)
#'
#'   if (length(baseline_datasets) == 0) {
#'     logger::log_error("No valid baseline demographic data found")
#'     stop("No baseline demographic data could be extracted")
#'   }
#'
#'   # Merge multiple baseline sources if needed
#'   combined_baseline <- combine_baseline_sources(baseline_datasets, verbose)
#'
#'   if (verbose) {
#'     logger::log_info("Baseline demographics extracted: {nrow(combined_baseline)} participants")
#'   }
#'
#'   return(combined_baseline)
#' }
#'
#'
#' #' @noRd
#' extract_involea_variables <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting INVOLEA variables across all visits")
#'   }
#'
#'   # Find all RDA files
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   # Process each file to find INVOLEA variables
#'   involea_datasets <- purrr::map(all_rda_files, function(file_path) {
#'
#'     if (verbose) {
#'       logger::log_info("Scanning {basename(file_path)} for INVOLEA variables")
#'     }
#'
#'     # Load file
#'     file_environment <- new.env()
#'     tryCatch({
#'       load(file_path, envir = file_environment)
#'     }, error = function(e) {
#'       if (verbose) {
#'         logger::log_warn("Failed to load {basename(file_path)}: {e$message}")
#'       }
#'       return(NULL)
#'     })
#'
#'     if (length(ls(file_environment)) == 0) return(NULL)
#'
#'     raw_dataset <- file_environment[[ls(file_environment)[1]]]
#'
#'     # Look for INVOLEA variables
#'     involea_columns <- identify_involea_variables(raw_dataset)
#'
#'     if (length(involea_columns) == 0) {
#'       return(NULL)  # No INVOLEA variables in this file
#'     }
#'
#'     if (verbose) {
#'       logger::log_info("  Found {length(involea_columns)} INVOLEA variables: {paste(involea_columns, collapse = ', ')}")
#'     }
#'
#'     # Extract INVOLEA data
#'     involea_data <- extract_involea_data(
#'       dataset = raw_dataset,
#'       involea_vars = involea_columns,
#'       source_file = basename(file_path),
#'       verbose = verbose
#'     )
#'
#'     return(involea_data)
#'   })
#'
#'   # Combine all INVOLEA datasets
#'   involea_datasets <- purrr::compact(involea_datasets)
#'
#'   if (length(involea_datasets) == 0) {
#'     logger::log_error("No INVOLEA variables found in any files")
#'     stop("No INVOLEA variables could be found")
#'   }
#'
#'   combined_involea <- purrr::map_dfr(involea_datasets, ~.x)
#'
#'   # Remove duplicates (same participant, visit, and variable)
#'   combined_involea <- combined_involea %>%
#'     dplyr::distinct(
#'       swan_participant_id,
#'       visit_number,
#'       involea_source_variable,
#'       .keep_all = TRUE
#'     )
#'
#'   if (verbose) {
#'     logger::log_info("INVOLEA variables extracted: {nrow(combined_involea)} observations")
#'     logger::log_info("Visit range: {min(combined_involea$visit_number, na.rm = TRUE)} to {max(combined_involea$visit_number, na.rm = TRUE)}")
#'     logger::log_info("Unique participants with INVOLEA: {length(unique(combined_involea$swan_participant_id))}")
#'   }
#'
#'   return(combined_involea)
#' }
#'
#'
#' #' @noRd
#' identify_demographic_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Identify participant ID
#'   participant_id_patterns <- c("SWANID", "ARCHID", "swanid", "archid")
#'   participant_id_col <- NULL
#'   for (pattern in participant_id_patterns) {
#'     if (pattern %in% variable_names) {
#'       participant_id_col <- pattern
#'       break
#'     }
#'   }
#'
#'   # Identify demographic variables
#'   age_vars <- variable_names[stringr::str_detect(variable_names, "AGE|age")]
#'   bmi_vars <- variable_names[stringr::str_detect(variable_names, "BMI|bmi")]
#'   race_vars <- variable_names[stringr::str_detect(variable_names, "RACE|race")]
#'   smoking_vars <- variable_names[stringr::str_detect(variable_names, "SMOKE|smoke")]
#'   parity_vars <- variable_names[stringr::str_detect(variable_names, "PREG|CHILD|preg|child|parity")]
#'   menopausal_vars <- variable_names[stringr::str_detect(variable_names, "STATUS|MENO|status|meno")]
#'
#'   return(list(
#'     participant_id = participant_id_col,
#'     age = age_vars,
#'     bmi = bmi_vars,
#'     race = race_vars,
#'     smoking = smoking_vars,
#'     parity = parity_vars,
#'     menopausal = menopausal_vars
#'   ))
#' }
#'
#'
#' #' @noRd
#' identify_involea_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Look for INVOLEA variables (including LEKINVO variants)
#'   involea_patterns <- c("INVOLEA", "LEKINVO", "involea", "lekinvo")
#'
#'   involea_variables <- c()
#'   for (pattern in involea_patterns) {
#'     matching_vars <- variable_names[stringr::str_detect(variable_names, pattern)]
#'     involea_variables <- c(involea_variables, matching_vars)
#'   }
#'
#'   return(unique(involea_variables))
#' }
#'
#'
#' #' @noRd
#' extract_and_clean_demographics <- function(dataset, demo_vars, source_file, verbose) {
#'
#'   # Start with participant ID
#'   cleaned_data <- tibble::as_tibble(dataset) %>%
#'     dplyr::select(dplyr::all_of(demo_vars$participant_id))
#'
#'   names(cleaned_data)[1] <- "swan_participant_id"
#'
#'   # Add demographic variables if available - ensuring consistent data types
#'   if (length(demo_vars$age) > 0) {
#'     cleaned_data$baseline_age_years <- as.numeric(as.character(dataset[[demo_vars$age[1]]]))
#'   }
#'
#'   if (length(demo_vars$bmi) > 0) {
#'     cleaned_data$baseline_bmi_kg_m2 <- as.numeric(as.character(dataset[[demo_vars$bmi[1]]]))
#'   }
#'
#'   if (length(demo_vars$race) > 0) {
#'     cleaned_data$baseline_race_ethnicity <- clean_race_variable(dataset[[demo_vars$race[1]]])
#'   }
#'
#'   if (length(demo_vars$smoking) > 0) {
#'     cleaned_data$baseline_smoking_status <- clean_smoking_variable(dataset[[demo_vars$smoking[1]]])
#'   }
#'
#'   if (length(demo_vars$parity) > 0) {
#'     cleaned_data$baseline_parity_count <- as.numeric(as.character(dataset[[demo_vars$parity[1]]]))
#'   }
#'
#'   if (length(demo_vars$menopausal) > 0) {
#'     cleaned_data$baseline_menopausal_stage <- clean_menopausal_variable(dataset[[demo_vars$menopausal[1]]])
#'   }
#'
#'   # Standardize participant ID
#'   cleaned_data$swan_participant_id <- as.character(cleaned_data$swan_participant_id)
#'   cleaned_data$source_file <- source_file
#'
#'   if (verbose) {
#'     logger::log_info("  Extracted {nrow(cleaned_data)} participants with {ncol(cleaned_data)-2} demographic variables")
#'   }
#'
#'   return(cleaned_data)
#' }
#'
#'
#' #' @noRd
#' extract_involea_data <- function(dataset, involea_vars, source_file, verbose) {
#'
#'   # Identify participant ID and visit variables
#'   participant_id_col <- identify_participant_id_column(dataset)
#'   visit_col <- identify_visit_column(dataset)
#'
#'   if (is.null(participant_id_col)) {
#'     if (verbose) {
#'       logger::log_warn("No participant ID found in {source_file}")
#'     }
#'     return(NULL)
#'   }
#'
#'   # Select relevant columns
#'   core_columns <- c(participant_id_col)
#'   if (!is.null(visit_col)) {
#'     core_columns <- c(core_columns, visit_col)
#'   }
#'
#'   # Look for visit-specific age variable (AGE0, AGE1, AGE2, etc.)
#'   age_vars <- grep("^AGE\\d+$", names(dataset), value = TRUE)
#'   if (length(age_vars) > 0) {
#'     core_columns <- c(core_columns, age_vars[1])  # Take the first age variable
#'   }
#'
#'   involea_dataset <- tibble::as_tibble(dataset) %>%
#'     dplyr::select(dplyr::all_of(c(core_columns, involea_vars)))
#'
#'   # Standardize column names
#'   names(involea_dataset)[1] <- "swan_participant_id"
#'   if (!is.null(visit_col)) {
#'     names(involea_dataset)[2] <- "visit_number_from_file"
#'   } else {
#'     involea_dataset$visit_number_from_file <- NA_real_
#'   }
#'
#'   # Add age variable if found
#'   age_col_index <- which(names(involea_dataset) %in% age_vars)
#'   if (length(age_col_index) > 0) {
#'     names(involea_dataset)[age_col_index[1]] <- "visit_specific_age"
#'   } else {
#'     involea_dataset$visit_specific_age <- NA_real_
#'   }
#'
#'   # Convert to long format
#'   involea_long <- involea_dataset %>%
#'     tidyr::pivot_longer(
#'       cols = dplyr::all_of(involea_vars),
#'       names_to = "involea_source_variable",
#'       values_to = "involea_raw_response"
#'     ) %>%
#'     dplyr::mutate(
#'       swan_participant_id = as.character(swan_participant_id),
#'       # ALWAYS extract visit number from variable name since SWAN files
#'       # have one variable per visit per file
#'       visit_number = extract_visit_number_from_variable_name(involea_source_variable),
#'       involea_incontinence = clean_involea_response(involea_raw_response),
#'       source_file = source_file
#'     ) %>%
#'     dplyr::select(-visit_number_from_file) %>%
#'     dplyr::filter(!is.na(involea_raw_response))  # Remove missing responses
#'
#'   # Debug logging to see what visit numbers we actually extracted
#'   if (verbose && nrow(involea_long) > 0) {
#'     unique_visits <- sort(unique(involea_long$visit_number))
#'     logger::log_info("  Extracted visits for {source_file}: {paste(unique_visits, collapse = ', ')}")
#'     logger::log_info("  Observations in {source_file}: {nrow(involea_long)}")
#'     if (!all(is.na(involea_long$visit_specific_age))) {
#'       age_summary <- round(mean(involea_long$visit_specific_age, na.rm = TRUE), 1)
#'       logger::log_info("  Mean age at this visit: {age_summary}")
#'     }
#'   }
#'
#'   return(involea_long)
#' }
#'
#'
#' #' @noRd
#' extract_visit_number_from_variable_name <- function(variable_names) {
#'
#'   # Simple and robust extraction of numbers at end of variable names
#'   visit_numbers <- stringr::str_extract(variable_names, "\\d+$")
#'   visit_numbers <- as.numeric(visit_numbers)
#'
#'   # Replace any NAs with 0 (baseline)
#'   visit_numbers[is.na(visit_numbers)] <- 0
#'
#'   return(visit_numbers)
#' }
#'
#'
#' #' @noRd
#' clean_involea_response <- function(response_raw) {
#'
#'   dplyr::case_when(
#'     # Standard formats
#'     response_raw %in% c("(2) Yes", "2", 2, "Yes", "YES", "yes") ~ 1,
#'     response_raw %in% c("(1) No", "1", 1, "No", "NO", "no") ~ 0,
#'     # Extended formats found in visits 6, 8, 9
#'     response_raw %in% c("(2) 2: Yes", "(2)2: Yes", "2: Yes") ~ 1,
#'     response_raw %in% c("(1) 1: No", "(1)1: No", "1: No") ~ 0,
#'     TRUE ~ NA_real_
#'   )
#' }
#'
#'
#' #' @noRd
#' combine_baseline_sources <- function(baseline_datasets, verbose) {
#'
#'   if (length(baseline_datasets) == 1) {
#'     return(baseline_datasets[[1]])
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Combining {length(baseline_datasets)} baseline sources")
#'   }
#'
#'   # Start with the most complete dataset
#'   dataset_completeness <- purrr::map_dbl(baseline_datasets, function(ds) {
#'     sum(!is.na(ds$swan_participant_id))
#'   })
#'
#'   primary_dataset <- baseline_datasets[[which.max(dataset_completeness)]]
#'
#'   # Add data from other sources
#'   for (i in seq_along(baseline_datasets)) {
#'     if (i == which.max(dataset_completeness)) next
#'
#'     secondary_dataset <- baseline_datasets[[i]]
#'
#'     # Harmonize data types before merging
#'     secondary_dataset <- harmonize_data_types(secondary_dataset, primary_dataset, verbose)
#'
#'     primary_dataset <- primary_dataset %>%
#'       dplyr::full_join(
#'         secondary_dataset,
#'         by = "swan_participant_id",
#'         suffix = c("", "_secondary")
#'       ) %>%
#'       dplyr::mutate(
#'         # Use primary data preferentially with type-safe coalesce
#'         baseline_age_years = safe_coalesce_numeric(
#'           baseline_age_years,
#'           baseline_age_years_secondary
#'         ),
#'         baseline_bmi_kg_m2 = safe_coalesce_numeric(
#'           baseline_bmi_kg_m2,
#'           baseline_bmi_kg_m2_secondary
#'         ),
#'         baseline_parity_count = safe_coalesce_numeric(
#'           baseline_parity_count,
#'           baseline_parity_count_secondary
#'         ),
#'         baseline_smoking_status = safe_coalesce_character(
#'           baseline_smoking_status,
#'           baseline_smoking_status_secondary
#'         ),
#'         baseline_race_ethnicity = safe_coalesce_character(
#'           baseline_race_ethnicity,
#'           baseline_race_ethnicity_secondary
#'         ),
#'         baseline_menopausal_stage = safe_coalesce_character(
#'           baseline_menopausal_stage,
#'           baseline_menopausal_stage_secondary
#'         )
#'       ) %>%
#'       dplyr::select(-dplyr::ends_with("_secondary"))
#'   }
#'
#'   return(primary_dataset)
#' }
#'
#'
#' #' @noRd
#' harmonize_data_types <- function(secondary_dataset, primary_dataset, verbose) {
#'
#'   # Get common baseline columns
#'   baseline_columns <- names(secondary_dataset)[stringr::str_starts(names(secondary_dataset), "baseline_")]
#'
#'   for (col_name in baseline_columns) {
#'     if (col_name %in% names(primary_dataset)) {
#'
#'       primary_type <- class(primary_dataset[[col_name]])[1]
#'       secondary_type <- class(secondary_dataset[[col_name]])[1]
#'
#'       if (primary_type != secondary_type) {
#'         if (verbose) {
#'           logger::log_info("Harmonizing {col_name}: {secondary_type} -> {primary_type}")
#'         }
#'
#'         # Convert secondary to match primary type
#'         if (primary_type == "numeric" || primary_type == "double") {
#'           secondary_dataset[[col_name]] <- as.numeric(as.character(secondary_dataset[[col_name]]))
#'         } else if (primary_type == "character") {
#'           secondary_dataset[[col_name]] <- as.character(secondary_dataset[[col_name]])
#'         } else if (primary_type == "logical") {
#'           secondary_dataset[[col_name]] <- as.logical(secondary_dataset[[col_name]])
#'         }
#'       }
#'     }
#'   }
#'
#'   return(secondary_dataset)
#' }
#'
#'
#' #' @noRd
#' safe_coalesce_numeric <- function(x, y) {
#'   # Convert both to numeric if needed
#'   x_numeric <- if (is.numeric(x)) x else as.numeric(as.character(x))
#'   y_numeric <- if (is.numeric(y)) y else as.numeric(as.character(y))
#'   dplyr::coalesce(x_numeric, y_numeric)
#' }
#'
#'
#' #' @noRd
#' safe_coalesce_character <- function(x, y) {
#'   # Convert both to character if needed
#'   x_char <- if (is.character(x)) x else as.character(x)
#'   y_char <- if (is.character(y)) y else as.character(y)
#'   dplyr::coalesce(x_char, y_char)
#' }
#'
#'
#' #' @noRd
#' merge_baseline_with_involea <- function(baseline_data, involea_data, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Merging baseline demographics with INVOLEA data")
#'     logger::log_info("Baseline participants: {length(unique(baseline_data$swan_participant_id))}")
#'     logger::log_info("INVOLEA observations: {nrow(involea_data)}")
#'   }
#'
#'   # Merge datasets
#'   merged_data <- involea_data %>%
#'     dplyr::left_join(
#'       baseline_data %>% dplyr::select(-source_file),
#'       by = "swan_participant_id"
#'     )
#'
#'   # Add time variables and current age
#'   merged_data <- merged_data %>%
#'     dplyr::mutate(
#'       years_since_baseline = visit_number,
#'       current_age_years = baseline_age_years + years_since_baseline
#'     ) %>%
#'     dplyr::arrange(swan_participant_id, visit_number)
#'
#'   if (verbose) {
#'     logger::log_info("Merged dataset: {nrow(merged_data)} observations")
#'     logger::log_info("Participants with both baseline + INVOLEA: {length(unique(merged_data$swan_participant_id[!is.na(merged_data$baseline_age_years)]))}")
#'   }
#'
#'   return(merged_data)
#' }
#'
#'
#' #' @noRd
#' format_final_dataset <- function(merged_data, format, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Formatting final dataset as: {format}")
#'   }
#'
#'   if (format == "long") {
#'     # Clean up but preserve all participant-visit combinations
#'     final_data <- merged_data %>%
#'       dplyr::select(
#'         swan_participant_id,
#'         visit_number,
#'         years_since_baseline,
#'         baseline_age_years,
#'         visit_specific_age,
#'         baseline_bmi_kg_m2,
#'         baseline_race_ethnicity,
#'         baseline_smoking_status,
#'         baseline_parity_count,
#'         baseline_menopausal_stage,
#'         involea_incontinence,
#'         involea_raw_response,
#'         involea_source_variable
#'       ) %>%
#'       # Only remove true duplicates (same participant, visit, AND response)
#'       # but keep different visits for the same participant
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         involea_source_variable,
#'         .keep_all = TRUE
#'       ) %>%
#'       dplyr::arrange(swan_participant_id, visit_number)
#'
#'   } else {
#'     # Convert to wide format
#'     final_data <- merged_data %>%
#'       dplyr::select(
#'         swan_participant_id,
#'         visit_number,
#'         baseline_age_years,
#'         visit_specific_age,
#'         baseline_bmi_kg_m2,
#'         baseline_race_ethnicity,
#'         baseline_smoking_status,
#'         baseline_parity_count,
#'         baseline_menopausal_stage,
#'         involea_incontinence
#'       ) %>%
#'       # Only remove true duplicates before pivoting
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         .keep_all = TRUE
#'       ) %>%
#'       tidyr::pivot_wider(
#'         names_from = visit_number,
#'         values_from = c(visit_specific_age, involea_incontinence),
#'         names_sep = "_visit_"
#'       )
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Final formatted dataset: {nrow(final_data)} rows")
#'     logger::log_info("Unique participants: {length(unique(final_data$swan_participant_id))}")
#'     if (format == "long") {
#'       logger::log_info("Visit range: {min(final_data$visit_number, na.rm = TRUE)} to {max(final_data$visit_number, na.rm = TRUE)}")
#'     }
#'   }
#'
#'   return(final_data)
#' }
#'
#'
#' # Helper functions from previous code
#' #' @noRd
#' identify_participant_id_column <- function(dataset) {
#'   participant_id_patterns <- c("^SWANID$", "^ARCHID$", "^swanid$", "^archid$")
#'   for (pattern in participant_id_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' @noRd
#' identify_visit_column <- function(dataset) {
#'   visit_patterns <- c("^VISIT$", "^visit$", "^Visit$", "^VISITNO$")
#'   for (pattern in visit_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' @noRd
#' clean_race_variable <- function(race_raw) {
#'   dplyr::case_when(
#'     stringr::str_detect(as.character(race_raw), "White|Caucasian") ~ "White",
#'     stringr::str_detect(as.character(race_raw), "Black|African") ~ "Black",
#'     stringr::str_detect(as.character(race_raw), "Hispanic") ~ "Hispanic",
#'     stringr::str_detect(as.character(race_raw), "Chinese") ~ "Chinese",
#'     stringr::str_detect(as.character(race_raw), "Japanese") ~ "Japanese",
#'     TRUE ~ "Other"
#'   )
#' }
#'
#' #' @noRd
#' clean_smoking_variable <- function(smoking_raw) {
#'   dplyr::case_when(
#'     smoking_raw %in% c(1, "(1) Never smoked") ~ "Never",
#'     smoking_raw %in% c(2, "(2) Past smoker") ~ "Past",
#'     smoking_raw %in% c(3, "(3) Current smoker") ~ "Current",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' @noRd
#' clean_menopausal_variable <- function(status_raw) {
#'   dplyr::case_when(
#'     status_raw %in% c(2, "(2) Postmenopausal") ~ "Post",
#'     status_raw %in% c(3, "(3) Late perimenopausal") ~ "Late Peri",
#'     status_raw %in% c(4, "(4) Early perimenopausal") ~ "Early Peri",
#'     status_raw %in% c(5, "(5) Premenopausal") ~ "Pre",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' # Create the final dataset with visit-specific ages
#' final_swan_dataset <- create_focused_swan_dataset(
#'   swan_data_directory = swan_directory,
#'   output_format = "long",
#'   verbose = TRUE
#' )
#'
#' # Check the corrected age progression
#' age_progression_final <- final_swan_dataset %>%
#'   group_by(visit_number) %>%
#'   summarise(
#'     participants = n(),
#'     prevalence = round(mean(involea_incontinence), 3),
#'     baseline_age_mean = round(mean(baseline_age_years, na.rm = TRUE), 1),
#'     visit_age_mean = round(mean(visit_specific_age, na.rm = TRUE), 1),
#'     years_followup = unique(years_since_baseline)
#'   )
#'
#' print(age_progression_final)
#'
#'
#' # Function to see what visit-specific varaibales are present across all ----
#' # Investigate visit-specific demographic variables across SWAN files
#'
#' library(tidyverse)
#' library(stringr)
#'
#' swan_directory <- "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/"
#'
#' # Get all SWAN files with INVOLEA data
#' involea_files <- list.files(
#'   path = swan_directory,
#'   pattern = "\\.rda$",
#'   full.names = TRUE
#' )[c(2:12)]  # Files that had INVOLEA variables
#'
#' cat("=== INVESTIGATING VISIT-SPECIFIC DEMOGRAPHIC VARIABLES ===\n\n")
#'
#' # Common demographic patterns to look for
#' demographic_patterns <- list(
#'   age = "^AGE\\d+$",
#'   bmi = "BMI\\d+$|^WTKGG\\d+$|^WT_KGS\\d+$",
#'   weight = "^WT_LBS\\d+$|^WT_KGS\\d+$|^WEIGHT\\d+$",
#'   height = "^HEIGHT\\d+$|^TALL\\d+$",
#'   smoking = "^SMOKE\\d+$|^SMOKERE\\d+$|^SMOKING\\d+$",
#'   menopause = "^STATUS\\d+$|^MENO\\d+$",
#'   pregnancy = "^PREG\\d+$|^NUMPREG\\d+$",
#'   hormone_use = "^HORMONE\\d+$|^HT\\d+$|^PILLSYR\\d+$",
#'   physical_activity = "^EXERCISE\\d+$|^ACTIVITY\\d+$|^PHY_ACT\\d+$"
#' )
#'
#' visit_specific_vars <- tibble()
#'
#' for (i in seq_along(involea_files)) {
#'   file_path <- involea_files[i]
#'   file_name <- basename(file_path)
#'
#'   cat("=== File", i, ":", file_name, "===\n")
#'
#'   # Load file
#'   env <- new.env()
#'   load(file_path, envir = env)
#'   data <- env[[ls(env)[1]]]
#'
#'   # Extract visit number
#'   involea_vars <- grep("INVOLEA|LEKINVO", names(data), value = TRUE)
#'   if (length(involea_vars) > 0) {
#'     visit_num <- str_extract(involea_vars[1], "\\d+$")
#'     visit_num <- as.numeric(visit_num)
#'   } else {
#'     visit_num <- NA
#'   }
#'
#'   cat("Visit:", visit_num, "\n")
#'   cat("Total variables:", ncol(data), "\n")
#'
#'   # Find visit-specific demographic variables
#'   found_vars <- list()
#'
#'   for (demo_type in names(demographic_patterns)) {
#'     pattern <- demographic_patterns[[demo_type]]
#'     matching_vars <- grep(pattern, names(data), value = TRUE)
#'
#'     if (length(matching_vars) > 0) {
#'       found_vars[[demo_type]] <- matching_vars
#'       cat(sprintf("  %s variables: %s\n",
#'                   str_to_title(demo_type),
#'                   paste(matching_vars, collapse = ", ")))
#'     }
#'   }
#'
#'   # Store results
#'   for (demo_type in names(found_vars)) {
#'     for (var_name in found_vars[[demo_type]]) {
#'       # Get basic statistics for numeric variables
#'       if (is.numeric(data[[var_name]])) {
#'         var_summary <- data %>%
#'           summarise(
#'             mean_val = round(mean(.data[[var_name]], na.rm = TRUE), 2),
#'             min_val = min(.data[[var_name]], na.rm = TRUE),
#'             max_val = max(.data[[var_name]], na.rm = TRUE),
#'             missing = sum(is.na(.data[[var_name]])),
#'             n_unique = n_distinct(.data[[var_name]], na.rm = TRUE)
#'           )
#'
#'         visit_specific_vars <- bind_rows(visit_specific_vars, tibble(
#'           file_name = file_name,
#'           visit_number = visit_num,
#'           demographic_type = demo_type,
#'           variable_name = var_name,
#'           data_type = "numeric",
#'           mean_value = var_summary$mean_val,
#'           range = paste(var_summary$min_val, "-", var_summary$max_val),
#'           missing_count = var_summary$missing,
#'           unique_values = var_summary$n_unique
#'         ))
#'       } else {
#'         # For categorical variables
#'         unique_vals <- unique(data[[var_name]])
#'         unique_vals_clean <- unique_vals[!is.na(unique_vals)]
#'
#'         visit_specific_vars <- bind_rows(visit_specific_vars, tibble(
#'           file_name = file_name,
#'           visit_number = visit_num,
#'           demographic_type = demo_type,
#'           variable_name = var_name,
#'           data_type = "categorical",
#'           mean_value = NA_real_,
#'           range = paste(length(unique_vals_clean), "categories"),
#'           missing_count = sum(is.na(data[[var_name]])),
#'           unique_values = length(unique_vals_clean)
#'         ))
#'       }
#'     }
#'   }
#'
#'   cat("\n")
#' }
#'
#' # Summary table
#' cat("=== SUMMARY OF VISIT-SPECIFIC VARIABLES ===\n")
#' print(visit_specific_vars)
#'
#' # Focus on the most promising variables
#' cat("\n=== VARIABLES AVAILABLE ACROSS MULTIPLE VISITS ===\n")
#'
#' vars_by_visit <- visit_specific_vars %>%
#'   group_by(demographic_type, variable_name) %>%
#'   summarise(
#'     visits_available = list(sort(visit_number)),
#'     n_visits = n(),
#'     data_type = first(data_type),
#'     .groups = "drop"
#'   ) %>%
#'   filter(n_visits >= 3) %>%  # Available in at least 3 visits
#'   arrange(desc(n_visits), demographic_type)
#'
#' if (nrow(vars_by_visit) > 0) {
#'   for (i in 1:nrow(vars_by_visit)) {
#'     row <- vars_by_visit[i,]
#'     cat(sprintf("%s (%s): %s - Available in visits %s (%d visits)\n",
#'                 str_to_title(row$demographic_type),
#'                 row$variable_name,
#'                 row$data_type,
#'                 paste(unlist(row$visits_available), collapse = ", "),
#'                 row$n_visits))
#'   }
#' } else {
#'   cat("No variables found across multiple visits with current patterns.\n")
#' }
#'
#' # Check for BMI-related variables specifically
#' cat("\n=== BMI-RELATED VARIABLES INVESTIGATION ===\n")
#' bmi_search_patterns <- c("BMI", "WEIGHT", "WT", "HEIGHT", "TALL")
#'
#' for (i in seq_along(involea_files)) {
#'   file_path <- involea_files[i]
#'   file_name <- basename(file_path)
#'
#'   env <- new.env()
#'   load(file_path, envir = env)
#'   data <- env[[ls(env)[1]]]
#'
#'   visit_num <- i - 1  # Approximate visit number
#'
#'   bmi_related <- c()
#'   for (pattern in bmi_search_patterns) {
#'     matches <- grep(pattern, names(data), value = TRUE, ignore.case = TRUE)
#'     bmi_related <- c(bmi_related, matches)
#'   }
#'
#'   if (length(bmi_related) > 0) {
#'     cat(sprintf("Visit %d (%s): %s\n", visit_num, file_name, paste(bmi_related, collapse = ", ")))
#'   }
#' }
#'
#' # Function at 1721 ----
#' #' Create Focused SWAN Dataset: Baseline Demographics + INVOLEA Variables
#' #'
#' #' This function creates a clean, focused SWAN dataset containing baseline
#' #' demographic information (one row per participant) merged with urinary
#' #' incontinence (INVOLEA) variables across all visits in long format. This
#' #' structure is optimal for longitudinal analysis of urinary incontinence
#' #' while maintaining essential baseline covariates.
#' #'
#' #' @param swan_data_directory Character. Path to directory containing SWAN .rda
#' #'   files. The function will identify baseline demographic files and files
#' #'   containing INVOLEA variables across visits.
#' #' @param output_format Character. Format for the final dataset: "long" creates
#' #'   one row per participant-visit with INVOLEA data, "wide" creates one row
#' #'   per participant with INVOLEA variables spread across visits. Default "long".
#' #' @param verbose Logical. If TRUE (default), prints detailed logging about
#' #'   file processing, baseline demographics extraction, and INVOLEA variable
#' #'   identification across visits.
#' #'
#' #' @return A tibble containing focused SWAN data:
#' #'   For "long" format:
#' #'   \describe{
#' #'     \item{swan_participant_id}{Participant identifier}
#' #'     \item{visit_number}{Visit number (0=baseline, 1-15=follow-up)}
#' #'     \item{baseline_age_years}{Age at baseline (repeated for all visits)}
#' #'     \item{baseline_bmi_kg_m2}{BMI at baseline (repeated for all visits)}
#' #'     \item{baseline_race_ethnicity}{Race/ethnicity (repeated for all visits)}
#' #'     \item{baseline_smoking_status}{Smoking status at baseline}
#' #'     \item{baseline_parity_count}{Number of pregnancies at baseline}
#' #'     \item{baseline_menopausal_stage}{Menopausal stage at baseline}
#' #'     \item{involea_incontinence}{Binary incontinence variable (0/1)}
#' #'     \item{involea_raw_response}{Original INVOLEA response}
#' #'     \item{involea_source_variable}{Original variable name}
#' #'     \item{years_since_baseline}{Time since baseline visit}
#' #'   }
#' #'
#' #' @examples
#' #' # Example 1: Long format for longitudinal modeling
#' #' swan_long_dataset <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/Users/tylermuffly/Dropbox (Personal)/workforce/Dall_model/data/SWAN",
#' #'   output_format = "long",
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' # Example 2: Wide format for cross-sectional analysis
#' #' swan_wide_dataset <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "wide",
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' # Example 3: Silent processing for production pipelines
#' #' swan_analysis_ready <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "long",
#' #'   verbose = FALSE
#' #' )
#' #' # Expected output for long format:
#' #' # # A tibble: 21,629 × 12
#' #' #   swan_participant_id visit_number baseline_age_years baseline_bmi_kg_m2
#' #' #   <chr>                      <dbl>              <dbl>              <dbl>
#' #' # 1 10005                          0                 48               24.1
#' #' # 2 10005                          1                 48               24.1
#' #' # 3 10005                          3                 48               24.1
#' #' # # ... with 21,626 more rows and 8 more variables
#' #'
#' #' @importFrom dplyr filter select mutate left_join case_when
#' #' @importFrom dplyr group_by summarise ungroup arrange distinct
#' #' @importFrom tidyr pivot_longer pivot_wider
#' #' @importFrom stringr str_detect str_extract str_replace_all
#' #' @importFrom purrr map_dfr map_chr map_lgl keep
#' #' @importFrom assertthat assert_that
#' #' @importFrom logger log_info log_warn log_error
#' #' @importFrom tibble tibble as_tibble
#' #' @importFrom readr parse_number
#' #' @importFrom glue glue
#' #'
#' #' @export
#' create_focused_swan_dataset <- function(swan_data_directory,
#'                                         output_format = "long",
#'                                         verbose = TRUE) {
#'
#'   # Input validation
#'   if (verbose) {
#'     logger::log_info("Creating focused SWAN dataset: Baseline demographics + INVOLEA variables")
#'     logger::log_info("Input validation and setup")
#'   }
#'
#'   assertthat::assert_that(
#'     is.character(swan_data_directory),
#'     msg = "swan_data_directory must be a character string path"
#'   )
#'   assertthat::assert_that(
#'     dir.exists(swan_data_directory),
#'     msg = glue::glue("Directory does not exist: {swan_data_directory}")
#'   )
#'   assertthat::assert_that(
#'     output_format %in% c("long", "wide"),
#'     msg = "output_format must be 'long' or 'wide'"
#'   )
#'
#'   # Step 1: Extract baseline demographics
#'   baseline_demographics_data <- extract_baseline_demographics(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   # Step 2: Extract INVOLEA variables across all visits
#'   involea_longitudinal_data <- extract_involea_variables(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   # Step 3: Merge baseline demographics with INVOLEA data
#'   merged_focused_dataset <- merge_baseline_with_involea(
#'     baseline_data = baseline_demographics_data,
#'     involea_data = involea_longitudinal_data,
#'     verbose = verbose
#'   )
#'
#'   # Step 4: Format output according to user preference
#'   final_focused_dataset <- format_final_dataset(
#'     merged_data = merged_focused_dataset,
#'     format = output_format,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Focused SWAN dataset creation completed")
#'     logger::log_info("Final dataset: {nrow(final_focused_dataset)} rows × {ncol(final_focused_dataset)} columns")
#'   }
#'
#'   return(final_focused_dataset)
#' }
#'
#'
#' #' @noRd
#' extract_baseline_demographics <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting baseline demographics from SWAN files")
#'   }
#'
#'   # Find files likely to contain baseline demographics
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Searching {length(all_rda_files)} RDA files for baseline demographics")
#'   }
#'
#'   # Try to identify baseline/demographic files by name patterns
#'   baseline_file_patterns <- c("28762", "baseline", "demo", "04368")
#'
#'   baseline_candidates <- purrr::keep(all_rda_files, function(file_path) {
#'     file_name <- tolower(basename(file_path))
#'     any(stringr::str_detect(file_name, baseline_file_patterns))
#'   })
#'
#'   if (verbose) {
#'     logger::log_info("Found {length(baseline_candidates)} potential baseline files:")
#'     purrr::walk(baseline_candidates, ~logger::log_info("  - {basename(.x)}"))
#'   }
#'
#'   # Load and process baseline files
#'   baseline_datasets <- purrr::map(baseline_candidates, function(file_path) {
#'
#'     if (verbose) {
#'       logger::log_info("Processing baseline file: {basename(file_path)}")
#'     }
#'
#'     # Load file
#'     file_environment <- new.env()
#'     load(file_path, envir = file_environment)
#'     raw_baseline_data <- file_environment[[ls(file_environment)[1]]]
#'
#'     # Look for demographic variables
#'     demo_variables <- identify_demographic_variables(raw_baseline_data)
#'
#'     if (length(demo_variables$participant_id) == 0) {
#'       if (verbose) {
#'         logger::log_warn("No participant ID found in {basename(file_path)}")
#'       }
#'       return(NULL)
#'     }
#'
#'     # Extract and clean demographics
#'     cleaned_baseline <- extract_and_clean_demographics(
#'       dataset = raw_baseline_data,
#'       demo_vars = demo_variables,
#'       source_file = basename(file_path),
#'       verbose = verbose
#'     )
#'
#'     return(cleaned_baseline)
#'   })
#'
#'   # Combine baseline datasets
#'   baseline_datasets <- purrr::compact(baseline_datasets)
#'
#'   if (length(baseline_datasets) == 0) {
#'     logger::log_error("No valid baseline demographic data found")
#'     stop("No baseline demographic data could be extracted")
#'   }
#'
#'   # Merge multiple baseline sources if needed
#'   combined_baseline <- combine_baseline_sources(baseline_datasets, verbose)
#'
#'   if (verbose) {
#'     logger::log_info("Baseline demographics extracted: {nrow(combined_baseline)} participants")
#'   }
#'
#'   return(combined_baseline)
#' }
#'
#'
#' #' @noRd
#' extract_involea_variables <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting INVOLEA variables across all visits")
#'   }
#'
#'   # Find all RDA files
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   # Process each file to find INVOLEA variables
#'   involea_datasets <- purrr::map(all_rda_files, function(file_path) {
#'
#'     if (verbose) {
#'       logger::log_info("Scanning {basename(file_path)} for INVOLEA variables")
#'     }
#'
#'     # Load file
#'     file_environment <- new.env()
#'     tryCatch({
#'       load(file_path, envir = file_environment)
#'     }, error = function(e) {
#'       if (verbose) {
#'         logger::log_warn("Failed to load {basename(file_path)}: {e$message}")
#'       }
#'       return(NULL)
#'     })
#'
#'     if (length(ls(file_environment)) == 0) return(NULL)
#'
#'     raw_dataset <- file_environment[[ls(file_environment)[1]]]
#'
#'     # Look for INVOLEA variables
#'     involea_columns <- identify_involea_variables(raw_dataset)
#'
#'     if (length(involea_columns) == 0) {
#'       return(NULL)  # No INVOLEA variables in this file
#'     }
#'
#'     if (verbose) {
#'       logger::log_info("  Found {length(involea_columns)} INVOLEA variables: {paste(involea_columns, collapse = ', ')}")
#'     }
#'
#'     # Extract INVOLEA data
#'     involea_data <- extract_involea_data(
#'       dataset = raw_dataset,
#'       involea_vars = involea_columns,
#'       source_file = basename(file_path),
#'       verbose = verbose
#'     )
#'
#'     return(involea_data)
#'   })
#'
#'   # Combine all INVOLEA datasets
#'   involea_datasets <- purrr::compact(involea_datasets)
#'
#'   if (length(involea_datasets) == 0) {
#'     logger::log_error("No INVOLEA variables found in any files")
#'     stop("No INVOLEA variables could be found")
#'   }
#'
#'   combined_involea <- purrr::map_dfr(involea_datasets, ~.x)
#'
#'   # Remove duplicates (same participant, visit, and variable)
#'   combined_involea <- combined_involea %>%
#'     dplyr::distinct(
#'       swan_participant_id,
#'       visit_number,
#'       involea_source_variable,
#'       .keep_all = TRUE
#'     )
#'
#'   if (verbose) {
#'     logger::log_info("INVOLEA variables extracted: {nrow(combined_involea)} observations")
#'     logger::log_info("Visit range: {min(combined_involea$visit_number, na.rm = TRUE)} to {max(combined_involea$visit_number, na.rm = TRUE)}")
#'     logger::log_info("Unique participants with INVOLEA: {length(unique(combined_involea$swan_participant_id))}")
#'   }
#'
#'   return(combined_involea)
#' }
#'
#'
#' #' @noRd
#' identify_demographic_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Identify participant ID
#'   participant_id_patterns <- c("SWANID", "ARCHID", "swanid", "archid")
#'   participant_id_col <- NULL
#'   for (pattern in participant_id_patterns) {
#'     if (pattern %in% variable_names) {
#'       participant_id_col <- pattern
#'       break
#'     }
#'   }
#'
#'   # Identify demographic variables
#'   age_vars <- variable_names[stringr::str_detect(variable_names, "AGE|age")]
#'   bmi_vars <- variable_names[stringr::str_detect(variable_names, "BMI|bmi")]
#'   race_vars <- variable_names[stringr::str_detect(variable_names, "RACE|race")]
#'   smoking_vars <- variable_names[stringr::str_detect(variable_names, "SMOKE|smoke")]
#'   parity_vars <- variable_names[stringr::str_detect(variable_names, "PREG|CHILD|preg|child|parity")]
#'   menopausal_vars <- variable_names[stringr::str_detect(variable_names, "STATUS|MENO|status|meno")]
#'
#'   return(list(
#'     participant_id = participant_id_col,
#'     age = age_vars,
#'     bmi = bmi_vars,
#'     race = race_vars,
#'     smoking = smoking_vars,
#'     parity = parity_vars,
#'     menopausal = menopausal_vars
#'   ))
#' }
#'
#'
#' #' @noRd
#' identify_involea_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Look for INVOLEA variables (including LEKINVO variants)
#'   involea_patterns <- c("INVOLEA", "LEKINVO", "involea", "lekinvo")
#'
#'   involea_variables <- c()
#'   for (pattern in involea_patterns) {
#'     matching_vars <- variable_names[stringr::str_detect(variable_names, pattern)]
#'     involea_variables <- c(involea_variables, matching_vars)
#'   }
#'
#'   return(unique(involea_variables))
#' }
#'
#'
#' #' @noRd
#' extract_and_clean_demographics <- function(dataset, demo_vars, source_file, verbose) {
#'
#'   # Start with participant ID
#'   cleaned_data <- tibble::as_tibble(dataset) %>%
#'     dplyr::select(dplyr::all_of(demo_vars$participant_id))
#'
#'   names(cleaned_data)[1] <- "swan_participant_id"
#'
#'   # Add demographic variables if available - ensuring consistent data types
#'   if (length(demo_vars$age) > 0) {
#'     cleaned_data$baseline_age_years <- as.numeric(as.character(dataset[[demo_vars$age[1]]]))
#'   }
#'
#'   if (length(demo_vars$bmi) > 0) {
#'     cleaned_data$baseline_bmi_kg_m2 <- as.numeric(as.character(dataset[[demo_vars$bmi[1]]]))
#'   }
#'
#'   if (length(demo_vars$race) > 0) {
#'     cleaned_data$baseline_race_ethnicity <- clean_race_variable(dataset[[demo_vars$race[1]]])
#'   }
#'
#'   if (length(demo_vars$smoking) > 0) {
#'     cleaned_data$baseline_smoking_status <- clean_smoking_variable(dataset[[demo_vars$smoking[1]]])
#'   }
#'
#'   if (length(demo_vars$parity) > 0) {
#'     cleaned_data$baseline_parity_count <- as.numeric(as.character(dataset[[demo_vars$parity[1]]]))
#'   }
#'
#'   if (length(demo_vars$menopausal) > 0) {
#'     cleaned_data$baseline_menopausal_stage <- clean_menopausal_variable(dataset[[demo_vars$menopausal[1]]])
#'   }
#'
#'   # Standardize participant ID
#'   cleaned_data$swan_participant_id <- as.character(cleaned_data$swan_participant_id)
#'   cleaned_data$source_file <- source_file
#'
#'   if (verbose) {
#'     logger::log_info("  Extracted {nrow(cleaned_data)} participants with {ncol(cleaned_data)-2} demographic variables")
#'   }
#'
#'   return(cleaned_data)
#' }
#'
#'
#' #' @noRd
#' extract_involea_data <- function(dataset, involea_vars, source_file, verbose) {
#'
#'   # Identify participant ID and visit variables
#'   participant_id_col <- identify_participant_id_column(dataset)
#'   visit_col <- identify_visit_column(dataset)
#'
#'   if (is.null(participant_id_col)) {
#'     if (verbose) {
#'       logger::log_warn("No participant ID found in {source_file}")
#'     }
#'     return(NULL)
#'   }
#'
#'   # Select relevant columns
#'   core_columns <- c(participant_id_col)
#'   if (!is.null(visit_col)) {
#'     core_columns <- c(core_columns, visit_col)
#'   }
#'
#'   # Look for visit-specific demographic variables
#'   visit_specific_demographics <- list()
#'
#'   # Age variables (AGE0, AGE1, etc.)
#'   age_vars <- grep("^AGE\\d+$", names(dataset), value = TRUE)
#'   if (length(age_vars) > 0) {
#'     visit_specific_demographics[["age"]] <- age_vars[1]
#'   }
#'
#'   # BMI variables (BMI0, BMI1, etc.)
#'   bmi_vars <- grep("^BMI\\d+$", names(dataset), value = TRUE)
#'   if (length(bmi_vars) > 0) {
#'     visit_specific_demographics[["bmi"]] <- bmi_vars[1]
#'   }
#'
#'   # Weight variables (WEIGHT0, WEIGHT1, etc.)
#'   weight_vars <- grep("^WEIGHT\\d+$", names(dataset), value = TRUE)
#'   if (length(weight_vars) > 0) {
#'     visit_specific_demographics[["weight"]] <- weight_vars[1]
#'   }
#'
#'   # Height variables (HEIGHT0, HEIGHT1, etc.)
#'   height_vars <- grep("^HEIGHT\\d+$", names(dataset), value = TRUE)
#'   if (length(height_vars) > 0) {
#'     visit_specific_demographics[["height"]] <- height_vars[1]
#'   }
#'
#'   # Smoking variables (SMOKERE0, SMOKERE1, etc.)
#'   smoking_vars <- grep("^SMOKERE\\d+$", names(dataset), value = TRUE)
#'   if (length(smoking_vars) > 0) {
#'     visit_specific_demographics[["smoking"]] <- smoking_vars[1]
#'   }
#'
#'   # Menopausal status variables (STATUS0, STATUS1, etc.)
#'   menopause_vars <- grep("^STATUS\\d+$", names(dataset), value = TRUE)
#'   if (length(menopause_vars) > 0) {
#'     visit_specific_demographics[["menopause"]] <- menopause_vars[1]
#'   }
#'
#'   # Add all found demographic variables to core columns
#'   demo_vars_to_extract <- unlist(visit_specific_demographics)
#'   core_columns <- c(core_columns, demo_vars_to_extract)
#'
#'   involea_dataset <- tibble::as_tibble(dataset) %>%
#'     dplyr::select(dplyr::all_of(c(core_columns, involea_vars)))
#'
#'   # Standardize column names
#'   names(involea_dataset)[1] <- "swan_participant_id"
#'   if (!is.null(visit_col)) {
#'     names(involea_dataset)[2] <- "visit_number_from_file"
#'   } else {
#'     involea_dataset$visit_number_from_file <- NA_real_
#'   }
#'
#'   # Rename visit-specific demographic variables with standardized names
#'   for (demo_type in names(visit_specific_demographics)) {
#'     var_name <- visit_specific_demographics[[demo_type]]
#'     if (var_name %in% names(involea_dataset)) {
#'       new_name <- paste0("visit_specific_", demo_type)
#'       names(involea_dataset)[names(involea_dataset) == var_name] <- new_name
#'     }
#'   }
#'
#'   # Convert to long format
#'   involea_long <- involea_dataset %>%
#'     tidyr::pivot_longer(
#'       cols = dplyr::all_of(involea_vars),
#'       names_to = "involea_source_variable",
#'       values_to = "involea_raw_response"
#'     ) %>%
#'     dplyr::mutate(
#'       swan_participant_id = as.character(swan_participant_id),
#'       # ALWAYS extract visit number from variable name since SWAN files
#'       # have one variable per visit per file
#'       visit_number = extract_visit_number_from_variable_name(involea_source_variable),
#'       involea_incontinence = clean_involea_response(involea_raw_response),
#'       source_file = source_file
#'     ) %>%
#'     dplyr::select(-visit_number_from_file) %>%
#'     dplyr::filter(!is.na(involea_raw_response))  # Remove missing responses
#'
#'   # Debug logging to see what visit numbers we actually extracted
#'   if (verbose && nrow(involea_long) > 0) {
#'     unique_visits <- sort(unique(involea_long$visit_number))
#'     logger::log_info("  Extracted visits for {source_file}: {paste(unique_visits, collapse = ', ')}")
#'     logger::log_info("  Observations in {source_file}: {nrow(involea_long)}")
#'
#'     # Log which demographic variables were found
#'     demo_vars_found <- names(involea_long)[stringr::str_starts(names(involea_long), "visit_specific_")]
#'     if (length(demo_vars_found) > 0) {
#'       logger::log_info("  Visit-specific demographics: {paste(str_remove(demo_vars_found, 'visit_specific_'), collapse = ', ')}")
#'     }
#'
#'     # Log age if available
#'     if ("visit_specific_age" %in% names(involea_long) && !all(is.na(involea_long$visit_specific_age))) {
#'       age_summary <- round(mean(involea_long$visit_specific_age, na.rm = TRUE), 1)
#'       logger::log_info("  Mean age at this visit: {age_summary}")
#'     }
#'   }
#'
#'   return(involea_long)
#' }
#'
#'
#' #' @noRd
#' extract_visit_number_from_variable_name <- function(variable_names) {
#'
#'   # Simple and robust extraction of numbers at end of variable names
#'   visit_numbers <- stringr::str_extract(variable_names, "\\d+$")
#'   visit_numbers <- as.numeric(visit_numbers)
#'
#'   # Replace any NAs with 0 (baseline)
#'   visit_numbers[is.na(visit_numbers)] <- 0
#'
#'   return(visit_numbers)
#' }
#'
#'
#' #' @noRd
#' clean_involea_response <- function(response_raw) {
#'
#'   dplyr::case_when(
#'     # Standard formats
#'     response_raw %in% c("(2) Yes", "2", 2, "Yes", "YES", "yes") ~ 1,
#'     response_raw %in% c("(1) No", "1", 1, "No", "NO", "no") ~ 0,
#'     # Extended formats found in visits 6, 8, 9
#'     response_raw %in% c("(2) 2: Yes", "(2)2: Yes", "2: Yes") ~ 1,
#'     response_raw %in% c("(1) 1: No", "(1)1: No", "1: No") ~ 0,
#'     TRUE ~ NA_real_
#'   )
#' }
#'
#'
#' #' @noRd
#' combine_baseline_sources <- function(baseline_datasets, verbose) {
#'
#'   if (length(baseline_datasets) == 1) {
#'     return(baseline_datasets[[1]])
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Combining {length(baseline_datasets)} baseline sources")
#'   }
#'
#'   # Start with the most complete dataset
#'   dataset_completeness <- purrr::map_dbl(baseline_datasets, function(ds) {
#'     sum(!is.na(ds$swan_participant_id))
#'   })
#'
#'   primary_dataset <- baseline_datasets[[which.max(dataset_completeness)]]
#'
#'   # Add data from other sources
#'   for (i in seq_along(baseline_datasets)) {
#'     if (i == which.max(dataset_completeness)) next
#'
#'     secondary_dataset <- baseline_datasets[[i]]
#'
#'     # Harmonize data types before merging
#'     secondary_dataset <- harmonize_data_types(secondary_dataset, primary_dataset, verbose)
#'
#'     primary_dataset <- primary_dataset %>%
#'       dplyr::full_join(
#'         secondary_dataset,
#'         by = "swan_participant_id",
#'         suffix = c("", "_secondary")
#'       ) %>%
#'       dplyr::mutate(
#'         # Use primary data preferentially with type-safe coalesce
#'         baseline_age_years = safe_coalesce_numeric(
#'           baseline_age_years,
#'           baseline_age_years_secondary
#'         ),
#'         baseline_bmi_kg_m2 = safe_coalesce_numeric(
#'           baseline_bmi_kg_m2,
#'           baseline_bmi_kg_m2_secondary
#'         ),
#'         baseline_parity_count = safe_coalesce_numeric(
#'           baseline_parity_count,
#'           baseline_parity_count_secondary
#'         ),
#'         baseline_smoking_status = safe_coalesce_character(
#'           baseline_smoking_status,
#'           baseline_smoking_status_secondary
#'         ),
#'         baseline_race_ethnicity = safe_coalesce_character(
#'           baseline_race_ethnicity,
#'           baseline_race_ethnicity_secondary
#'         ),
#'         baseline_menopausal_stage = safe_coalesce_character(
#'           baseline_menopausal_stage,
#'           baseline_menopausal_stage_secondary
#'         )
#'       ) %>%
#'       dplyr::select(-dplyr::ends_with("_secondary"))
#'   }
#'
#'   return(primary_dataset)
#' }
#'
#'
#' #' @noRd
#' harmonize_data_types <- function(secondary_dataset, primary_dataset, verbose) {
#'
#'   # Get common baseline columns
#'   baseline_columns <- names(secondary_dataset)[stringr::str_starts(names(secondary_dataset), "baseline_")]
#'
#'   for (col_name in baseline_columns) {
#'     if (col_name %in% names(primary_dataset)) {
#'
#'       primary_type <- class(primary_dataset[[col_name]])[1]
#'       secondary_type <- class(secondary_dataset[[col_name]])[1]
#'
#'       if (primary_type != secondary_type) {
#'         if (verbose) {
#'           logger::log_info("Harmonizing {col_name}: {secondary_type} -> {primary_type}")
#'         }
#'
#'         # Convert secondary to match primary type
#'         if (primary_type == "numeric" || primary_type == "double") {
#'           secondary_dataset[[col_name]] <- as.numeric(as.character(secondary_dataset[[col_name]]))
#'         } else if (primary_type == "character") {
#'           secondary_dataset[[col_name]] <- as.character(secondary_dataset[[col_name]])
#'         } else if (primary_type == "logical") {
#'           secondary_dataset[[col_name]] <- as.logical(secondary_dataset[[col_name]])
#'         }
#'       }
#'     }
#'   }
#'
#'   return(secondary_dataset)
#' }
#'
#'
#' #' @noRd
#' safe_coalesce_numeric <- function(x, y) {
#'   # Convert both to numeric if needed
#'   x_numeric <- if (is.numeric(x)) x else as.numeric(as.character(x))
#'   y_numeric <- if (is.numeric(y)) y else as.numeric(as.character(y))
#'   dplyr::coalesce(x_numeric, y_numeric)
#' }
#'
#'
#' #' @noRd
#' safe_coalesce_character <- function(x, y) {
#'   # Convert both to character if needed
#'   x_char <- if (is.character(x)) x else as.character(x)
#'   y_char <- if (is.character(y)) y else as.character(y)
#'   dplyr::coalesce(x_char, y_char)
#' }
#'
#'
#' #' @noRd
#' merge_baseline_with_involea <- function(baseline_data, involea_data, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Merging baseline demographics with INVOLEA data")
#'     logger::log_info("Baseline participants: {length(unique(baseline_data$swan_participant_id))}")
#'     logger::log_info("INVOLEA observations: {nrow(involea_data)}")
#'   }
#'
#'   # Merge datasets
#'   merged_data <- involea_data %>%
#'     dplyr::left_join(
#'       baseline_data %>% dplyr::select(-source_file),
#'       by = "swan_participant_id"
#'     )
#'
#'   # Add time variables and current age
#'   merged_data <- merged_data %>%
#'     dplyr::mutate(
#'       years_since_baseline = visit_number,
#'       current_age_years = baseline_age_years + years_since_baseline
#'     ) %>%
#'     dplyr::arrange(swan_participant_id, visit_number)
#'
#'   if (verbose) {
#'     logger::log_info("Merged dataset: {nrow(merged_data)} observations")
#'     logger::log_info("Participants with both baseline + INVOLEA: {length(unique(merged_data$swan_participant_id[!is.na(merged_data$baseline_age_years)]))}")
#'   }
#'
#'   return(merged_data)
#' }
#'
#'
#' #' @noRd
#' format_final_dataset <- function(merged_data, format, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Formatting final dataset as: {format}")
#'   }
#'
#'   if (format == "long") {
#'     # Get all visit-specific demographic columns
#'     visit_specific_cols <- names(merged_data)[stringr::str_starts(names(merged_data), "visit_specific_")]
#'
#'     # Clean up but preserve all participant-visit combinations
#'     final_data <- merged_data %>%
#'       dplyr::select(
#'         swan_participant_id,
#'         visit_number,
#'         years_since_baseline,
#'         baseline_age_years,
#'         dplyr::all_of(visit_specific_cols),
#'         baseline_bmi_kg_m2,
#'         baseline_race_ethnicity,
#'         baseline_smoking_status,
#'         baseline_parity_count,
#'         baseline_menopausal_stage,
#'         involea_incontinence,
#'         involea_raw_response,
#'         involea_source_variable
#'       ) %>%
#'       # Only remove true duplicates (same participant, visit, AND response)
#'       # but keep different visits for the same participant
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         involea_source_variable,
#'         .keep_all = TRUE
#'       ) %>%
#'       dplyr::arrange(swan_participant_id, visit_number)
#'
#'   } else {
#'     # Convert to wide format
#'     visit_specific_cols <- names(merged_data)[stringr::str_starts(names(merged_data), "visit_specific_")]
#'
#'     final_data <- merged_data %>%
#'       dplyr::select(
#'         swan_participant_id,
#'         visit_number,
#'         baseline_age_years,
#'         dplyr::all_of(visit_specific_cols),
#'         baseline_bmi_kg_m2,
#'         baseline_race_ethnicity,
#'         baseline_smoking_status,
#'         baseline_parity_count,
#'         baseline_menopausal_stage,
#'         involea_incontinence
#'       ) %>%
#'       # Only remove true duplicates before pivoting
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         .keep_all = TRUE
#'       ) %>%
#'       tidyr::pivot_wider(
#'         names_from = visit_number,
#'         values_from = c(dplyr::all_of(visit_specific_cols), involea_incontinence),
#'         names_sep = "_visit_"
#'       )
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Final formatted dataset: {nrow(final_data)} rows")
#'     logger::log_info("Unique participants: {length(unique(final_data$swan_participant_id))}")
#'     if (format == "long") {
#'       visit_specific_count <- sum(stringr::str_starts(names(final_data), "visit_specific_"))
#'       logger::log_info("Visit-specific variables: {visit_specific_count}")
#'       logger::log_info("Visit range: {min(final_data$visit_number, na.rm = TRUE)} to {max(final_data$visit_number, na.rm = TRUE)}")
#'     }
#'   }
#'
#'   return(final_data)
#' }
#'
#'
#' # Helper functions from previous code
#' #' @noRd
#' identify_participant_id_column <- function(dataset) {
#'   participant_id_patterns <- c("^SWANID$", "^ARCHID$", "^swanid$", "^archid$")
#'   for (pattern in participant_id_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' @noRd
#' identify_visit_column <- function(dataset) {
#'   visit_patterns <- c("^VISIT$", "^visit$", "^Visit$", "^VISITNO$")
#'   for (pattern in visit_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' @noRd
#' clean_race_variable <- function(race_raw) {
#'   dplyr::case_when(
#'     stringr::str_detect(as.character(race_raw), "White|Caucasian") ~ "White",
#'     stringr::str_detect(as.character(race_raw), "Black|African") ~ "Black",
#'     stringr::str_detect(as.character(race_raw), "Hispanic") ~ "Hispanic",
#'     stringr::str_detect(as.character(race_raw), "Chinese") ~ "Chinese",
#'     stringr::str_detect(as.character(race_raw), "Japanese") ~ "Japanese",
#'     TRUE ~ "Other"
#'   )
#' }
#'
#' #' @noRd
#' clean_smoking_variable <- function(smoking_raw) {
#'   dplyr::case_when(
#'     smoking_raw %in% c(1, "(1) Never smoked") ~ "Never",
#'     smoking_raw %in% c(2, "(2) Past smoker") ~ "Past",
#'     smoking_raw %in% c(3, "(3) Current smoker") ~ "Current",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' @noRd
#' clean_menopausal_variable <- function(status_raw) {
#'   dplyr::case_when(
#'     status_raw %in% c(2, "(2) Postmenopausal") ~ "Post",
#'     status_raw %in% c(3, "(3) Late perimenopausal") ~ "Late Peri",
#'     status_raw %in% c(4, "(4) Early perimenopausal") ~ "Early Peri",
#'     status_raw %in% c(5, "(5) Premenopausal") ~ "Pre",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' # Create the comprehensive dataset with all visit-specific demographics
#' comprehensive_swan_dataset <- create_focused_swan_dataset(
#'   swan_data_directory = swan_directory,
#'   output_format = "long",
#'   verbose = TRUE
#' )
#'
#' # Explore the new comprehensive dataset
#' cat("=== COMPREHENSIVE SWAN DATASET ===\n")
#' print(comprehensive_swan_dataset)
#'
#' # Check what visit-specific variables we now have
#' visit_specific_vars <- names(comprehensive_swan_dataset)[str_starts(names(comprehensive_swan_dataset), "visit_specific_")]
#' cat("\nVisit-specific variables captured:\n")
#' for(var in visit_specific_vars) {
#'   cat(" -", str_remove(var, "visit_specific_"), "\n")
#' }
#'
#' # Summary of time-varying demographics
#' time_varying_summary <- comprehensive_swan_dataset %>%
#'   group_by(visit_number) %>%
#'   summarise(
#'     participants = n(),
#'     incontinence_prev = round(mean(involea_incontinence), 3),
#'     mean_age = round(mean(visit_specific_age, na.rm = TRUE), 1),
#'     mean_bmi = round(mean(visit_specific_bmi, na.rm = TRUE), 1),
#'     mean_weight = round(mean(visit_specific_weight, na.rm = TRUE), 1),
#'     mean_height = round(mean(visit_specific_height, na.rm = TRUE), 1),
#'     .groups = "drop"
#'   )
#'
#' print(time_varying_summary)
#'
#' # Function at 1726 ----
#' #' Create Focused SWAN Dataset: Baseline Demographics + INVOLEA Variables
#' #'
#' #' This function creates a clean, focused SWAN dataset containing baseline
#' #' demographic information (one row per participant) merged with urinary
#' #' incontinence (INVOLEA) variables across all visits in long format. This
#' #' structure is optimal for longitudinal analysis of urinary incontinence
#' #' while maintaining essential baseline covariates.
#' #'
#' #' @param swan_data_directory Character. Path to directory containing SWAN .rda
#' #'   files. The function will identify baseline demographic files and files
#' #'   containing INVOLEA variables across visits.
#' #' @param output_format Character. Format for the final dataset: "long" creates
#' #'   one row per participant-visit with INVOLEA data, "wide" creates one row
#' #'   per participant with INVOLEA variables spread across visits. Default "long".
#' #' @param verbose Logical. If TRUE (default), prints detailed logging about
#' #'   file processing, baseline demographics extraction, and INVOLEA variable
#' #'   identification across visits.
#' #'
#' #' @return A tibble containing focused SWAN data:
#' #'   For "long" format:
#' #'   \describe{
#' #'     \item{swan_participant_id}{Participant identifier}
#' #'     \item{visit_number}{Visit number (0=baseline, 1-15=follow-up)}
#' #'     \item{baseline_age_years}{Age at baseline (repeated for all visits)}
#' #'     \item{baseline_bmi_kg_m2}{BMI at baseline (repeated for all visits)}
#' #'     \item{baseline_race_ethnicity}{Race/ethnicity (repeated for all visits)}
#' #'     \item{baseline_smoking_status}{Smoking status at baseline}
#' #'     \item{baseline_parity_count}{Number of pregnancies at baseline}
#' #'     \item{baseline_menopausal_stage}{Menopausal stage at baseline}
#' #'     \item{involea_incontinence}{Binary incontinence variable (0/1)}
#' #'     \item{involea_raw_response}{Original INVOLEA response}
#' #'     \item{involea_source_variable}{Original variable name}
#' #'     \item{years_since_baseline}{Time since baseline visit}
#' #'   }
#' #'
#' #' @examples
#' #' # Example 1: Long format for longitudinal modeling
#' #' swan_long_dataset <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/Users/tylermuffly/Dropbox (Personal)/workforce/Dall_model/data/SWAN",
#' #'   output_format = "long",
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' # Example 2: Wide format for cross-sectional analysis
#' #' swan_wide_dataset <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "wide",
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' # Example 3: Silent processing for production pipelines
#' #' swan_analysis_ready <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "long",
#' #'   verbose = FALSE
#' #' )
#' #' # Expected output for long format:
#' #' # # A tibble: 21,629 × 12
#' #' #   swan_participant_id visit_number baseline_age_years baseline_bmi_kg_m2
#' #' #   <chr>                      <dbl>              <dbl>              <dbl>
#' #' # 1 10005                          0                 48               24.1
#' #' # 2 10005                          1                 48               24.1
#' #' # 3 10005                          3                 48               24.1
#' #' # # ... with 21,626 more rows and 8 more variables
#' #'
#' #' @importFrom dplyr filter select mutate left_join case_when
#' #' @importFrom dplyr group_by summarise ungroup arrange distinct
#' #' @importFrom tidyr pivot_longer pivot_wider
#' #' @importFrom stringr str_detect str_extract str_replace_all
#' #' @importFrom purrr map_dfr map_chr map_lgl keep
#' #' @importFrom assertthat assert_that
#' #' @importFrom logger log_info log_warn log_error
#' #' @importFrom tibble tibble as_tibble
#' #' @importFrom readr parse_number
#' #' @importFrom glue glue
#' #'
#' #' @export
#' create_focused_swan_dataset <- function(swan_data_directory,
#'                                         output_format = "long",
#'                                         verbose = TRUE) {
#'
#'   # Input validation
#'   if (verbose) {
#'     logger::log_info("Creating focused SWAN dataset: Baseline demographics + INVOLEA variables")
#'     logger::log_info("Input validation and setup")
#'   }
#'
#'   assertthat::assert_that(
#'     is.character(swan_data_directory),
#'     msg = "swan_data_directory must be a character string path"
#'   )
#'   assertthat::assert_that(
#'     dir.exists(swan_data_directory),
#'     msg = glue::glue("Directory does not exist: {swan_data_directory}")
#'   )
#'   assertthat::assert_that(
#'     output_format %in% c("long", "wide"),
#'     msg = "output_format must be 'long' or 'wide'"
#'   )
#'
#'   # Step 1: Extract baseline demographics
#'   baseline_demographics_data <- extract_baseline_demographics(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   # Step 2: Extract INVOLEA variables across all visits
#'   involea_longitudinal_data <- extract_involea_variables(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   # Step 3: Merge baseline demographics with INVOLEA data
#'   merged_focused_dataset <- merge_baseline_with_involea(
#'     baseline_data = baseline_demographics_data,
#'     involea_data = involea_longitudinal_data,
#'     verbose = verbose
#'   )
#'
#'   # Step 4: Format output according to user preference
#'   final_focused_dataset <- format_final_dataset(
#'     merged_data = merged_focused_dataset,
#'     format = output_format,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Focused SWAN dataset creation completed")
#'     logger::log_info("Final dataset: {nrow(final_focused_dataset)} rows × {ncol(final_focused_dataset)} columns")
#'   }
#'
#'   return(final_focused_dataset)
#' }
#'
#'
#' #' @noRd
#' extract_baseline_demographics <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting baseline demographics from SWAN files")
#'   }
#'
#'   # Find files likely to contain baseline demographics
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Searching {length(all_rda_files)} RDA files for baseline demographics")
#'   }
#'
#'   # Try to identify baseline/demographic files by name patterns
#'   baseline_file_patterns <- c("28762", "baseline", "demo", "04368")
#'
#'   baseline_candidates <- purrr::keep(all_rda_files, function(file_path) {
#'     file_name <- tolower(basename(file_path))
#'     any(stringr::str_detect(file_name, baseline_file_patterns))
#'   })
#'
#'   if (verbose) {
#'     logger::log_info("Found {length(baseline_candidates)} potential baseline files:")
#'     purrr::walk(baseline_candidates, ~logger::log_info("  - {basename(.x)}"))
#'   }
#'
#'   # Load and process baseline files
#'   baseline_datasets <- purrr::map(baseline_candidates, function(file_path) {
#'
#'     if (verbose) {
#'       logger::log_info("Processing baseline file: {basename(file_path)}")
#'     }
#'
#'     # Load file
#'     file_environment <- new.env()
#'     load(file_path, envir = file_environment)
#'     raw_baseline_data <- file_environment[[ls(file_environment)[1]]]
#'
#'     # Look for demographic variables
#'     demo_variables <- identify_demographic_variables(raw_baseline_data)
#'
#'     if (length(demo_variables$participant_id) == 0) {
#'       if (verbose) {
#'         logger::log_warn("No participant ID found in {basename(file_path)}")
#'       }
#'       return(NULL)
#'     }
#'
#'     # Extract and clean demographics
#'     cleaned_baseline <- extract_and_clean_demographics(
#'       dataset = raw_baseline_data,
#'       demo_vars = demo_variables,
#'       source_file = basename(file_path),
#'       verbose = verbose
#'     )
#'
#'     return(cleaned_baseline)
#'   })
#'
#'   # Combine baseline datasets
#'   baseline_datasets <- purrr::compact(baseline_datasets)
#'
#'   if (length(baseline_datasets) == 0) {
#'     logger::log_error("No valid baseline demographic data found")
#'     stop("No baseline demographic data could be extracted")
#'   }
#'
#'   # Merge multiple baseline sources if needed
#'   combined_baseline <- combine_baseline_sources(baseline_datasets, verbose)
#'
#'   if (verbose) {
#'     logger::log_info("Baseline demographics extracted: {nrow(combined_baseline)} participants")
#'   }
#'
#'   return(combined_baseline)
#' }
#'
#'
#' #' @noRd
#' extract_involea_variables <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting INVOLEA variables across all visits")
#'   }
#'
#'   # Find all RDA files
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   # Process each file to find INVOLEA variables
#'   involea_datasets <- purrr::map(all_rda_files, function(file_path) {
#'
#'     if (verbose) {
#'       logger::log_info("Scanning {basename(file_path)} for INVOLEA variables")
#'     }
#'
#'     # Load file
#'     file_environment <- new.env()
#'     tryCatch({
#'       load(file_path, envir = file_environment)
#'     }, error = function(e) {
#'       if (verbose) {
#'         logger::log_warn("Failed to load {basename(file_path)}: {e$message}")
#'       }
#'       return(NULL)
#'     })
#'
#'     if (length(ls(file_environment)) == 0) return(NULL)
#'
#'     raw_dataset <- file_environment[[ls(file_environment)[1]]]
#'
#'     # Look for INVOLEA variables
#'     involea_columns <- identify_involea_variables(raw_dataset)
#'
#'     if (length(involea_columns) == 0) {
#'       return(NULL)  # No INVOLEA variables in this file
#'     }
#'
#'     if (verbose) {
#'       logger::log_info("  Found {length(involea_columns)} INVOLEA variables: {paste(involea_columns, collapse = ', ')}")
#'     }
#'
#'     # Extract INVOLEA data
#'     involea_data <- extract_involea_data(
#'       dataset = raw_dataset,
#'       involea_vars = involea_columns,
#'       source_file = basename(file_path),
#'       verbose = verbose
#'     )
#'
#'     return(involea_data)
#'   })
#'
#'   # Combine all INVOLEA datasets
#'   involea_datasets <- purrr::compact(involea_datasets)
#'
#'   if (length(involea_datasets) == 0) {
#'     logger::log_error("No INVOLEA variables found in any files")
#'     stop("No INVOLEA variables could be found")
#'   }
#'
#'   combined_involea <- purrr::map_dfr(involea_datasets, ~.x)
#'
#'   # Remove duplicates (same participant, visit, and variable)
#'   combined_involea <- combined_involea %>%
#'     dplyr::distinct(
#'       swan_participant_id,
#'       visit_number,
#'       involea_source_variable,
#'       .keep_all = TRUE
#'     )
#'
#'   if (verbose) {
#'     logger::log_info("INVOLEA variables extracted: {nrow(combined_involea)} observations")
#'     logger::log_info("Visit range: {min(combined_involea$visit_number, na.rm = TRUE)} to {max(combined_involea$visit_number, na.rm = TRUE)}")
#'     logger::log_info("Unique participants with INVOLEA: {length(unique(combined_involea$swan_participant_id))}")
#'   }
#'
#'   return(combined_involea)
#' }
#'
#'
#' #' @noRd
#' identify_demographic_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Identify participant ID
#'   participant_id_patterns <- c("SWANID", "ARCHID", "swanid", "archid")
#'   participant_id_col <- NULL
#'   for (pattern in participant_id_patterns) {
#'     if (pattern %in% variable_names) {
#'       participant_id_col <- pattern
#'       break
#'     }
#'   }
#'
#'   # Identify demographic variables
#'   age_vars <- variable_names[stringr::str_detect(variable_names, "AGE|age")]
#'   bmi_vars <- variable_names[stringr::str_detect(variable_names, "BMI|bmi")]
#'   race_vars <- variable_names[stringr::str_detect(variable_names, "RACE|race")]
#'   smoking_vars <- variable_names[stringr::str_detect(variable_names, "SMOKE|smoke")]
#'   parity_vars <- variable_names[stringr::str_detect(variable_names, "PREG|CHILD|preg|child|parity")]
#'   menopausal_vars <- variable_names[stringr::str_detect(variable_names, "STATUS|MENO|status|meno")]
#'
#'   return(list(
#'     participant_id = participant_id_col,
#'     age = age_vars,
#'     bmi = bmi_vars,
#'     race = race_vars,
#'     smoking = smoking_vars,
#'     parity = parity_vars,
#'     menopausal = menopausal_vars
#'   ))
#' }
#'
#'
#' #' @noRd
#' identify_involea_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Look for INVOLEA variables (including LEKINVO variants)
#'   involea_patterns <- c("INVOLEA", "LEKINVO", "involea", "lekinvo")
#'
#'   involea_variables <- c()
#'   for (pattern in involea_patterns) {
#'     matching_vars <- variable_names[stringr::str_detect(variable_names, pattern)]
#'     involea_variables <- c(involea_variables, matching_vars)
#'   }
#'
#'   return(unique(involea_variables))
#' }
#'
#'
#' #' @noRd
#' extract_and_clean_demographics <- function(dataset, demo_vars, source_file, verbose) {
#'
#'   # Start with participant ID
#'   cleaned_data <- tibble::as_tibble(dataset) %>%
#'     dplyr::select(dplyr::all_of(demo_vars$participant_id))
#'
#'   names(cleaned_data)[1] <- "swan_participant_id"
#'
#'   # Add demographic variables if available - ensuring consistent data types
#'   if (length(demo_vars$age) > 0) {
#'     cleaned_data$baseline_age_years <- as.numeric(as.character(dataset[[demo_vars$age[1]]]))
#'   }
#'
#'   if (length(demo_vars$bmi) > 0) {
#'     cleaned_data$baseline_bmi_kg_m2 <- as.numeric(as.character(dataset[[demo_vars$bmi[1]]]))
#'   }
#'
#'   if (length(demo_vars$race) > 0) {
#'     cleaned_data$baseline_race_ethnicity <- clean_race_variable(dataset[[demo_vars$race[1]]])
#'   }
#'
#'   if (length(demo_vars$smoking) > 0) {
#'     cleaned_data$baseline_smoking_status <- clean_smoking_variable(dataset[[demo_vars$smoking[1]]])
#'   }
#'
#'   if (length(demo_vars$parity) > 0) {
#'     cleaned_data$baseline_parity_count <- as.numeric(as.character(dataset[[demo_vars$parity[1]]]))
#'   }
#'
#'   if (length(demo_vars$menopausal) > 0) {
#'     cleaned_data$baseline_menopausal_stage <- clean_menopausal_variable(dataset[[demo_vars$menopausal[1]]])
#'   }
#'
#'   # Standardize participant ID
#'   cleaned_data$swan_participant_id <- as.character(cleaned_data$swan_participant_id)
#'   cleaned_data$source_file <- source_file
#'
#'   if (verbose) {
#'     logger::log_info("  Extracted {nrow(cleaned_data)} participants with {ncol(cleaned_data)-2} demographic variables")
#'   }
#'
#'   return(cleaned_data)
#' }
#'
#'
#' #' @noRd
#' extract_involea_data <- function(dataset, involea_vars, source_file, verbose) {
#'
#'   # Identify participant ID and visit variables
#'   participant_id_col <- identify_participant_id_column(dataset)
#'   visit_col <- identify_visit_column(dataset)
#'
#'   if (is.null(participant_id_col)) {
#'     if (verbose) {
#'       logger::log_warn("No participant ID found in {source_file}")
#'     }
#'     return(NULL)
#'   }
#'
#'   # Select relevant columns
#'   core_columns <- c(participant_id_col)
#'   if (!is.null(visit_col)) {
#'     core_columns <- c(core_columns, visit_col)
#'   }
#'
#'   # Look for visit-specific demographic variables
#'   visit_specific_demographics <- list()
#'
#'   # Age variables (AGE0, AGE1, etc.)
#'   age_vars <- grep("^AGE\\d+$", names(dataset), value = TRUE)
#'   if (length(age_vars) > 0) {
#'     visit_specific_demographics[["age"]] <- age_vars[1]
#'   }
#'
#'   # BMI variables (BMI0, BMI1, etc.)
#'   bmi_vars <- grep("^BMI\\d+$", names(dataset), value = TRUE)
#'   if (length(bmi_vars) > 0) {
#'     visit_specific_demographics[["bmi"]] <- bmi_vars[1]
#'   }
#'
#'   # Weight variables (WEIGHT0, WEIGHT1, etc.)
#'   weight_vars <- grep("^WEIGHT\\d+$", names(dataset), value = TRUE)
#'   if (length(weight_vars) > 0) {
#'     visit_specific_demographics[["weight"]] <- weight_vars[1]
#'   }
#'
#'   # Height variables (HEIGHT0, HEIGHT1, etc.)
#'   height_vars <- grep("^HEIGHT\\d+$", names(dataset), value = TRUE)
#'   if (length(height_vars) > 0) {
#'     visit_specific_demographics[["height"]] <- height_vars[1]
#'   }
#'
#'   # Smoking variables (SMOKERE0, SMOKERE1, etc.)
#'   smoking_vars <- grep("^SMOKERE\\d+$", names(dataset), value = TRUE)
#'   if (length(smoking_vars) > 0) {
#'     visit_specific_demographics[["smoking"]] <- smoking_vars[1]
#'   }
#'
#'   # Menopausal status variables (STATUS0, STATUS1, etc.)
#'   menopause_vars <- grep("^STATUS\\d+$", names(dataset), value = TRUE)
#'   if (length(menopause_vars) > 0) {
#'     visit_specific_demographics[["menopause"]] <- menopause_vars[1]
#'   }
#'
#'   # Add all found demographic variables to core columns
#'   demo_vars_to_extract <- unlist(visit_specific_demographics)
#'   core_columns <- c(core_columns, demo_vars_to_extract)
#'
#'   involea_dataset <- tibble::as_tibble(dataset) %>%
#'     dplyr::select(dplyr::all_of(c(core_columns, involea_vars)))
#'
#'   # Standardize column names
#'   names(involea_dataset)[1] <- "swan_participant_id"
#'   if (!is.null(visit_col)) {
#'     names(involea_dataset)[2] <- "visit_number_from_file"
#'   } else {
#'     involea_dataset$visit_number_from_file <- NA_real_
#'   }
#'
#'   # Rename visit-specific demographic variables with standardized names
#'   for (demo_type in names(visit_specific_demographics)) {
#'     var_name <- visit_specific_demographics[[demo_type]]
#'     if (var_name %in% names(involea_dataset)) {
#'       new_name <- paste0("visit_specific_", demo_type)
#'       names(involea_dataset)[names(involea_dataset) == var_name] <- new_name
#'     }
#'   }
#'
#'   # Convert to long format
#'   involea_long <- involea_dataset %>%
#'     tidyr::pivot_longer(
#'       cols = dplyr::all_of(involea_vars),
#'       names_to = "involea_source_variable",
#'       values_to = "involea_raw_response"
#'     ) %>%
#'     dplyr::mutate(
#'       swan_participant_id = as.character(swan_participant_id),
#'       # ALWAYS extract visit number from variable name since SWAN files
#'       # have one variable per visit per file
#'       visit_number = extract_visit_number_from_variable_name(involea_source_variable),
#'       involea_incontinence = clean_involea_response(involea_raw_response),
#'       source_file = source_file
#'     ) %>%
#'     dplyr::select(-visit_number_from_file) %>%
#'     dplyr::filter(!is.na(involea_raw_response))  # Remove missing responses
#'
#'   # Debug logging to see what visit numbers we actually extracted
#'   if (verbose && nrow(involea_long) > 0) {
#'     unique_visits <- sort(unique(involea_long$visit_number))
#'     logger::log_info("  Extracted visits for {source_file}: {paste(unique_visits, collapse = ', ')}")
#'     logger::log_info("  Observations in {source_file}: {nrow(involea_long)}")
#'
#'     # Log which demographic variables were found
#'     demo_vars_found <- names(involea_long)[stringr::str_starts(names(involea_long), "visit_specific_")]
#'     if (length(demo_vars_found) > 0) {
#'       logger::log_info("  Visit-specific demographics: {paste(str_remove(demo_vars_found, 'visit_specific_'), collapse = ', ')}")
#'     }
#'
#'     # Log age if available
#'     if ("visit_specific_age" %in% names(involea_long) && !all(is.na(involea_long$visit_specific_age))) {
#'       age_summary <- round(mean(involea_long$visit_specific_age, na.rm = TRUE), 1)
#'       logger::log_info("  Mean age at this visit: {age_summary}")
#'     }
#'   }
#'
#'   return(involea_long)
#' }
#'
#'
#' #' @noRd
#' extract_visit_number_from_variable_name <- function(variable_names) {
#'
#'   # Simple and robust extraction of numbers at end of variable names
#'   visit_numbers <- stringr::str_extract(variable_names, "\\d+$")
#'   visit_numbers <- as.numeric(visit_numbers)
#'
#'   # Replace any NAs with 0 (baseline)
#'   visit_numbers[is.na(visit_numbers)] <- 0
#'
#'   return(visit_numbers)
#' }
#'
#'
#' #' @noRd
#' clean_involea_response <- function(response_raw) {
#'
#'   dplyr::case_when(
#'     # Standard formats
#'     response_raw %in% c("(2) Yes", "2", 2, "Yes", "YES", "yes") ~ 1,
#'     response_raw %in% c("(1) No", "1", 1, "No", "NO", "no") ~ 0,
#'     # Extended formats found in visits 6, 8, 9
#'     response_raw %in% c("(2) 2: Yes", "(2)2: Yes", "2: Yes") ~ 1,
#'     response_raw %in% c("(1) 1: No", "(1)1: No", "1: No") ~ 0,
#'     TRUE ~ NA_real_
#'   )
#' }
#'
#'
#' #' @noRd
#' combine_baseline_sources <- function(baseline_datasets, verbose) {
#'
#'   if (length(baseline_datasets) == 1) {
#'     return(baseline_datasets[[1]])
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Combining {length(baseline_datasets)} baseline sources")
#'   }
#'
#'   # Start with the most complete dataset
#'   dataset_completeness <- purrr::map_dbl(baseline_datasets, function(ds) {
#'     sum(!is.na(ds$swan_participant_id))
#'   })
#'
#'   primary_dataset <- baseline_datasets[[which.max(dataset_completeness)]]
#'
#'   # Add data from other sources
#'   for (i in seq_along(baseline_datasets)) {
#'     if (i == which.max(dataset_completeness)) next
#'
#'     secondary_dataset <- baseline_datasets[[i]]
#'
#'     # Harmonize data types before merging
#'     secondary_dataset <- harmonize_data_types(secondary_dataset, primary_dataset, verbose)
#'
#'     primary_dataset <- primary_dataset %>%
#'       dplyr::full_join(
#'         secondary_dataset,
#'         by = "swan_participant_id",
#'         suffix = c("", "_secondary")
#'       ) %>%
#'       dplyr::mutate(
#'         # Use primary data preferentially with type-safe coalesce
#'         baseline_age_years = safe_coalesce_numeric(
#'           baseline_age_years,
#'           baseline_age_years_secondary
#'         ),
#'         baseline_bmi_kg_m2 = safe_coalesce_numeric(
#'           baseline_bmi_kg_m2,
#'           baseline_bmi_kg_m2_secondary
#'         ),
#'         baseline_parity_count = safe_coalesce_numeric(
#'           baseline_parity_count,
#'           baseline_parity_count_secondary
#'         ),
#'         baseline_smoking_status = safe_coalesce_character(
#'           baseline_smoking_status,
#'           baseline_smoking_status_secondary
#'         ),
#'         baseline_race_ethnicity = safe_coalesce_character(
#'           baseline_race_ethnicity,
#'           baseline_race_ethnicity_secondary
#'         ),
#'         baseline_menopausal_stage = safe_coalesce_character(
#'           baseline_menopausal_stage,
#'           baseline_menopausal_stage_secondary
#'         )
#'       ) %>%
#'       dplyr::select(-dplyr::ends_with("_secondary"))
#'   }
#'
#'   return(primary_dataset)
#' }
#'
#'
#' #' @noRd
#' harmonize_data_types <- function(secondary_dataset, primary_dataset, verbose) {
#'
#'   # Get common baseline columns
#'   baseline_columns <- names(secondary_dataset)[stringr::str_starts(names(secondary_dataset), "baseline_")]
#'
#'   for (col_name in baseline_columns) {
#'     if (col_name %in% names(primary_dataset)) {
#'
#'       primary_type <- class(primary_dataset[[col_name]])[1]
#'       secondary_type <- class(secondary_dataset[[col_name]])[1]
#'
#'       if (primary_type != secondary_type) {
#'         if (verbose) {
#'           logger::log_info("Harmonizing {col_name}: {secondary_type} -> {primary_type}")
#'         }
#'
#'         # Convert secondary to match primary type
#'         if (primary_type == "numeric" || primary_type == "double") {
#'           secondary_dataset[[col_name]] <- as.numeric(as.character(secondary_dataset[[col_name]]))
#'         } else if (primary_type == "character") {
#'           secondary_dataset[[col_name]] <- as.character(secondary_dataset[[col_name]])
#'         } else if (primary_type == "logical") {
#'           secondary_dataset[[col_name]] <- as.logical(secondary_dataset[[col_name]])
#'         }
#'       }
#'     }
#'   }
#'
#'   return(secondary_dataset)
#' }
#'
#'
#' #' @noRd
#' safe_coalesce_numeric <- function(x, y) {
#'   # Convert both to numeric if needed
#'   x_numeric <- if (is.numeric(x)) x else as.numeric(as.character(x))
#'   y_numeric <- if (is.numeric(y)) y else as.numeric(as.character(y))
#'   dplyr::coalesce(x_numeric, y_numeric)
#' }
#'
#'
#' #' @noRd
#' safe_coalesce_character <- function(x, y) {
#'   # Convert both to character if needed
#'   x_char <- if (is.character(x)) x else as.character(x)
#'   y_char <- if (is.character(y)) y else as.character(y)
#'   dplyr::coalesce(x_char, y_char)
#' }
#'
#'
#' #' @noRd
#' merge_baseline_with_involea <- function(baseline_data, involea_data, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Merging baseline demographics with INVOLEA data")
#'     logger::log_info("Baseline participants: {length(unique(baseline_data$swan_participant_id))}")
#'     logger::log_info("INVOLEA observations: {nrow(involea_data)}")
#'   }
#'
#'   # Merge datasets
#'   merged_data <- involea_data %>%
#'     dplyr::left_join(
#'       baseline_data %>% dplyr::select(-source_file),
#'       by = "swan_participant_id"
#'     )
#'
#'   # Add time variables and current age
#'   merged_data <- merged_data %>%
#'     dplyr::mutate(
#'       years_since_baseline = visit_number,
#'       current_age_years = baseline_age_years + years_since_baseline
#'     ) %>%
#'     dplyr::arrange(swan_participant_id, visit_number)
#'
#'   if (verbose) {
#'     logger::log_info("Merged dataset: {nrow(merged_data)} observations")
#'     logger::log_info("Participants with both baseline + INVOLEA: {length(unique(merged_data$swan_participant_id[!is.na(merged_data$baseline_age_years)]))}")
#'   }
#'
#'   return(merged_data)
#' }
#'
#'
#' #' @noRd
#' format_final_dataset <- function(merged_data, format, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Formatting final dataset as: {format}")
#'     logger::log_info("Available columns: {paste(names(merged_data), collapse = ', ')}")
#'   }
#'
#'   if (format == "long") {
#'     # Get all visit-specific demographic columns
#'     visit_specific_cols <- names(merged_data)[stringr::str_starts(names(merged_data), "visit_specific_")]
#'
#'     if (verbose && length(visit_specific_cols) > 0) {
#'       logger::log_info("Visit-specific columns found: {paste(visit_specific_cols, collapse = ', ')}")
#'     } else if (verbose) {
#'       logger::log_warn("No visit-specific columns found - they may have been lost during processing")
#'     }
#'
#'     # Build the selection dynamically to avoid missing column errors
#'     base_columns <- c(
#'       "swan_participant_id",
#'       "visit_number",
#'       "years_since_baseline",
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_parity_count",
#'       "baseline_menopausal_stage",
#'       "involea_incontinence",
#'       "involea_raw_response",
#'       "involea_source_variable"
#'     )
#'
#'     # Add visit-specific columns if they exist
#'     all_columns_to_select <- c(base_columns, visit_specific_cols)
#'
#'     # Only select columns that actually exist in the data
#'     columns_that_exist <- intersect(all_columns_to_select, names(merged_data))
#'
#'     if (verbose) {
#'       logger::log_info("Selecting {length(columns_that_exist)} columns for final dataset")
#'     }
#'
#'     # Clean up but preserve all participant-visit combinations
#'     final_data <- merged_data %>%
#'       dplyr::select(dplyr::all_of(columns_that_exist)) %>%
#'       # Only remove true duplicates (same participant, visit, AND response)
#'       # but keep different visits for the same participant
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         involea_source_variable,
#'         .keep_all = TRUE
#'       ) %>%
#'       dplyr::arrange(swan_participant_id, visit_number)
#'
#'   } else {
#'     # Convert to wide format
#'     visit_specific_cols <- names(merged_data)[stringr::str_starts(names(merged_data), "visit_specific_")]
#'
#'     base_columns <- c(
#'       "swan_participant_id",
#'       "visit_number",
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_parity_count",
#'       "baseline_menopausal_stage",
#'       "involea_incontinence"
#'     )
#'
#'     all_columns_to_select <- c(base_columns, visit_specific_cols)
#'     columns_that_exist <- intersect(all_columns_to_select, names(merged_data))
#'
#'     final_data <- merged_data %>%
#'       dplyr::select(dplyr::all_of(columns_that_exist)) %>%
#'       # Only remove true duplicates before pivoting
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         .keep_all = TRUE
#'       ) %>%
#'       tidyr::pivot_wider(
#'         names_from = visit_number,
#'         values_from = c(dplyr::any_of(visit_specific_cols), involea_incontinence),
#'         names_sep = "_visit_"
#'       )
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Final formatted dataset: {nrow(final_data)} rows")
#'     logger::log_info("Final columns: {paste(names(final_data), collapse = ', ')}")
#'     logger::log_info("Unique participants: {length(unique(final_data$swan_participant_id))}")
#'     if (format == "long") {
#'       final_visit_specific <- sum(stringr::str_starts(names(final_data), "visit_specific_"))
#'       logger::log_info("Visit-specific variables in final dataset: {final_visit_specific}")
#'       logger::log_info("Visit range: {min(final_data$visit_number, na.rm = TRUE)} to {max(final_data$visit_number, na.rm = TRUE)}")
#'     }
#'   }
#'
#'   return(final_data)
#' }
#'
#'
#' # Helper functions from previous code
#' #' @noRd
#' identify_participant_id_column <- function(dataset) {
#'   participant_id_patterns <- c("^SWANID$", "^ARCHID$", "^swanid$", "^archid$")
#'   for (pattern in participant_id_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' @noRd
#' identify_visit_column <- function(dataset) {
#'   visit_patterns <- c("^VISIT$", "^visit$", "^Visit$", "^VISITNO$")
#'   for (pattern in visit_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' @noRd
#' clean_race_variable <- function(race_raw) {
#'   dplyr::case_when(
#'     stringr::str_detect(as.character(race_raw), "White|Caucasian") ~ "White",
#'     stringr::str_detect(as.character(race_raw), "Black|African") ~ "Black",
#'     stringr::str_detect(as.character(race_raw), "Hispanic") ~ "Hispanic",
#'     stringr::str_detect(as.character(race_raw), "Chinese") ~ "Chinese",
#'     stringr::str_detect(as.character(race_raw), "Japanese") ~ "Japanese",
#'     TRUE ~ "Other"
#'   )
#' }
#'
#' #' @noRd
#' clean_smoking_variable <- function(smoking_raw) {
#'   dplyr::case_when(
#'     smoking_raw %in% c(1, "(1) Never smoked") ~ "Never",
#'     smoking_raw %in% c(2, "(2) Past smoker") ~ "Past",
#'     smoking_raw %in% c(3, "(3) Current smoker") ~ "Current",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' @noRd
#' clean_menopausal_variable <- function(status_raw) {
#'   dplyr::case_when(
#'     status_raw %in% c(2, "(2) Postmenopausal") ~ "Post",
#'     status_raw %in% c(3, "(3) Late perimenopausal") ~ "Late Peri",
#'     status_raw %in% c(4, "(4) Early perimenopausal") ~ "Early Peri",
#'     status_raw %in% c(5, "(5) Premenopausal") ~ "Pre",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' # Create the comprehensive dataset with enhanced debugging
#' comprehensive_swan_dataset_debug <- create_focused_swan_dataset(
#'   swan_data_directory = swan_directory,
#'   output_format = "long",
#'   verbose = TRUE
#' )
#'
#' # Check what we actually got
#' cat("=== DEBUGGING RESULTS ===\n")
#' cat("Final dataset columns:\n")
#' print(names(comprehensive_swan_dataset_debug))
#'
#' # Check for visit-specific variables
#' visit_specific_vars_final <- names(comprehensive_swan_dataset_debug)[str_starts(names(comprehensive_swan_dataset_debug), "visit_specific_")]
#' cat("\nVisit-specific variables in final dataset:\n")
#' print(visit_specific_vars_final)
#'
#' # If we have visit-specific variables, test the summary
#' if (length(visit_specific_vars_final) > 0) {
#'   time_varying_summary_fixed <- comprehensive_swan_dataset_debug %>%
#'     group_by(visit_number) %>%
#'     summarise(
#'       participants = n(),
#'       incontinence_prev = round(mean(involea_incontinence), 3),
#'       .groups = "drop"
#'     )
#'
#'   # Add visit-specific summaries dynamically
#'   if ("visit_specific_age" %in% names(comprehensive_swan_dataset_debug)) {
#'     time_varying_summary_fixed$mean_age <- comprehensive_swan_dataset_debug %>%
#'       group_by(visit_number) %>%
#'       summarise(mean_age = round(mean(visit_specific_age, na.rm = TRUE), 1), .groups = "drop") %>%
#'       pull(mean_age)
#'   }
#'
#'   if ("visit_specific_bmi" %in% names(comprehensive_swan_dataset_debug)) {
#'     time_varying_summary_fixed$mean_bmi <- comprehensive_swan_dataset_debug %>%
#'       group_by(visit_number) %>%
#'       summarise(mean_bmi = round(mean(visit_specific_bmi, na.rm = TRUE), 1), .groups = "drop") %>%
#'       pull(mean_bmi)
#'   }
#'
#'   print(time_varying_summary_fixed)
#' } else {
#'   cat("No visit-specific variables found - the extraction still needs debugging\n")
#' }
#'
#' # Debug at 1738 ----
#' # Debug the extraction step by step to find where variables are lost
#'
#' library(tidyverse)
#' library(stringr)
#'
#' # Test on one file to see exactly what happens
#' test_file <- "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/28762-0001-Data.rda"
#' env <- new.env()
#' load(test_file, envir = env)
#' dataset <- env[[ls(env)[1]]]
#'
#' cat("=== STEP-BY-STEP DEBUGGING ===\n")
#'
#' # Step 1: Identify variables
#' participant_id_col <- "SWANID"
#' involea_vars <- "INVOLEA0"
#'
#' cat("Step 1 - Identified variables:\n")
#' cat("  Participant ID:", participant_id_col, "\n")
#' cat("  INVOLEA vars:", involea_vars, "\n")
#'
#' # Step 2: Find visit-specific demographics
#' visit_specific_demographics <- list()
#'
#' age_vars <- grep("^AGE\\d+$", names(dataset), value = TRUE)
#' if (length(age_vars) > 0) {
#'   visit_specific_demographics[["age"]] <- age_vars[1]
#' }
#'
#' bmi_vars <- grep("^BMI\\d+$", names(dataset), value = TRUE)
#' if (length(bmi_vars) > 0) {
#'   visit_specific_demographics[["bmi"]] <- bmi_vars[1]
#' }
#'
#' weight_vars <- grep("^WEIGHT\\d+$", names(dataset), value = TRUE)
#' if (length(weight_vars) > 0) {
#'   visit_specific_demographics[["weight"]] <- weight_vars[1]
#' }
#'
#' height_vars <- grep("^HEIGHT\\d+$", names(dataset), value = TRUE)
#' if (length(height_vars) > 0) {
#'   visit_specific_demographics[["height"]] <- height_vars[1]
#' }
#'
#' smoking_vars <- grep("^SMOKERE\\d+$", names(dataset), value = TRUE)
#' if (length(smoking_vars) > 0) {
#'   visit_specific_demographics[["smoking"]] <- smoking_vars[1]
#' }
#'
#' menopause_vars <- grep("^STATUS\\d+$", names(dataset), value = TRUE)
#' if (length(menopause_vars) > 0) {
#'   visit_specific_demographics[["menopause"]] <- menopause_vars[1]
#' }
#'
#' cat("\nStep 2 - Found visit-specific demographics:\n")
#' for (demo_type in names(visit_specific_demographics)) {
#'   cat("  ", demo_type, ":", visit_specific_demographics[[demo_type]], "\n")
#' }
#'
#' # Step 3: Build column selection
#' demo_vars_to_extract <- unlist(visit_specific_demographics)
#' core_columns <- c(participant_id_col, demo_vars_to_extract)
#'
#' cat("\nStep 3 - Columns to extract:\n")
#' print(core_columns)
#'
#' # Step 4: Select columns
#' involea_dataset <- dataset %>%
#'   select(all_of(c(core_columns, involea_vars)))
#'
#' cat("\nStep 4 - After selection:\n")
#' cat("  Columns:", paste(names(involea_dataset), collapse = ", "), "\n")
#' cat("  Rows:", nrow(involea_dataset), "\n")
#'
#' # Step 5: Rename columns
#' names(involea_dataset)[1] <- "swan_participant_id"
#'
#' # Rename visit-specific demographic variables
#' for (demo_type in names(visit_specific_demographics)) {
#'   var_name <- visit_specific_demographics[[demo_type]]
#'   if (var_name %in% names(involea_dataset)) {
#'     new_name <- paste0("visit_specific_", demo_type)
#'     names(involea_dataset)[names(involea_dataset) == var_name] <- new_name
#'     cat("  Renamed", var_name, "to", new_name, "\n")
#'   }
#' }
#'
#' cat("\nStep 5 - After renaming:\n")
#' cat("  Columns:", paste(names(involea_dataset), collapse = ", "), "\n")
#'
#' # Step 6: Pivot longer
#' involea_long <- involea_dataset %>%
#'   pivot_longer(
#'     cols = all_of(involea_vars),
#'     names_to = "involea_source_variable",
#'     values_to = "involea_raw_response"
#'   ) %>%
#'   mutate(
#'     swan_participant_id = as.character(swan_participant_id),
#'     visit_number = 0,  # We know this is visit 0
#'     source_file = "test"
#'   ) %>%
#'   filter(!is.na(involea_raw_response))
#'
#' cat("\nStep 6 - After pivot_longer:\n")
#' cat("  Columns:", paste(names(involea_long), collapse = ", "), "\n")
#' cat("  Rows:", nrow(involea_long), "\n")
#'
#' # Check if visit-specific variables are still there
#' visit_specific_cols <- names(involea_long)[str_starts(names(involea_long), "visit_specific_")]
#' cat("  Visit-specific columns:", paste(visit_specific_cols, collapse = ", "), "\n")
#'
#' # Step 7: Show sample data
#' cat("\nStep 7 - Sample of final data:\n")
#' print(head(involea_long, 3))
#'
#' # Test the cleaning function
#' clean_involea_response <- function(response_raw) {
#'   case_when(
#'     response_raw %in% c("(2) Yes", "2", 2, "Yes", "YES", "yes") ~ 1,
#'     response_raw %in% c("(1) No", "1", 1, "No", "NO", "no") ~ 0,
#'     response_raw %in% c("(2) 2: Yes", "(2)2: Yes", "2: Yes") ~ 1,
#'     response_raw %in% c("(1) 1: No", "(1)1: No", "1: No") ~ 0,
#'     TRUE ~ NA_real_
#'   )
#' }
#'
#' # Add the cleaned incontinence variable
#' involea_final <- involea_long %>%
#'   mutate(involea_incontinence = clean_involea_response(involea_raw_response))
#'
#' cat("\nFinal test - all columns:\n")
#' print(names(involea_final))
#'
#' cat("\nFinal test - visit-specific variables:\n")
#' final_visit_specific <- names(involea_final)[str_starts(names(involea_final), "visit_specific_")]
#' print(final_visit_specific)
#'
#' # Function at 1743 -----
#' #' Create Focused SWAN Dataset: Baseline Demographics + INVOLEA Variables
#' #'
#' #' This function creates a clean, focused SWAN dataset containing baseline
#' #' demographic information (one row per participant) merged with urinary
#' #' incontinence (INVOLEA) variables across all visits in long format. This
#' #' structure is optimal for longitudinal analysis of urinary incontinence
#' #' while maintaining essential baseline covariates.
#' #'
#' #' @param swan_data_directory Character. Path to directory containing SWAN .rda
#' #'   files. The function will identify baseline demographic files and files
#' #'   containing INVOLEA variables across visits.
#' #' @param output_format Character. Format for the final dataset: "long" creates
#' #'   one row per participant-visit with INVOLEA data, "wide" creates one row
#' #'   per participant with INVOLEA variables spread across visits. Default "long".
#' #' @param verbose Logical. If TRUE (default), prints detailed logging about
#' #'   file processing, baseline demographics extraction, and INVOLEA variable
#' #'   identification across visits.
#' #'
#' #' @return A tibble containing focused SWAN data:
#' #'   For "long" format:
#' #'   \describe{
#' #'     \item{swan_participant_id}{Participant identifier}
#' #'     \item{visit_number}{Visit number (0=baseline, 1-15=follow-up)}
#' #'     \item{baseline_age_years}{Age at baseline (repeated for all visits)}
#' #'     \item{baseline_bmi_kg_m2}{BMI at baseline (repeated for all visits)}
#' #'     \item{baseline_race_ethnicity}{Race/ethnicity (repeated for all visits)}
#' #'     \item{baseline_smoking_status}{Smoking status at baseline}
#' #'     \item{baseline_parity_count}{Number of pregnancies at baseline}
#' #'     \item{baseline_menopausal_stage}{Menopausal stage at baseline}
#' #'     \item{involea_incontinence}{Binary incontinence variable (0/1)}
#' #'     \item{involea_raw_response}{Original INVOLEA response}
#' #'     \item{involea_source_variable}{Original variable name}
#' #'     \item{years_since_baseline}{Time since baseline visit}
#' #'   }
#' #'
#' #' @examples
#' #' # Example 1: Long format for longitudinal modeling
#' #' swan_long_dataset <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/Users/tylermuffly/Dropbox (Personal)/workforce/Dall_model/data/SWAN",
#' #'   output_format = "long",
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' # Example 2: Wide format for cross-sectional analysis
#' #' swan_wide_dataset <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "wide",
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' # Example 3: Silent processing for production pipelines
#' #' swan_analysis_ready <- create_focused_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "long",
#' #'   verbose = FALSE
#' #' )
#' #' # Expected output for long format:
#' #' # # A tibble: 21,629 × 12
#' #' #   swan_participant_id visit_number baseline_age_years baseline_bmi_kg_m2
#' #' #   <chr>                      <dbl>              <dbl>              <dbl>
#' #' # 1 10005                          0                 48               24.1
#' #' # 2 10005                          1                 48               24.1
#' #' # 3 10005                          3                 48               24.1
#' #' # # ... with 21,626 more rows and 8 more variables
#' #'
#' #' @importFrom dplyr filter select mutate left_join case_when
#' #' @importFrom dplyr group_by summarise ungroup arrange distinct
#' #' @importFrom tidyr pivot_longer pivot_wider
#' #' @importFrom stringr str_detect str_extract str_replace_all
#' #' @importFrom purrr map_dfr map_chr map_lgl keep
#' #' @importFrom assertthat assert_that
#' #' @importFrom logger log_info log_warn log_error
#' #' @importFrom tibble tibble as_tibble
#' #' @importFrom readr parse_number
#' #' @importFrom glue glue
#' #'
#' #' @export
#' create_focused_swan_dataset <- function(swan_data_directory,
#'                                         output_format = "long",
#'                                         verbose = TRUE) {
#'
#'   # Input validation
#'   if (verbose) {
#'     logger::log_info("Creating focused SWAN dataset: Baseline demographics + INVOLEA variables")
#'     logger::log_info("Input validation and setup")
#'   }
#'
#'   assertthat::assert_that(
#'     is.character(swan_data_directory),
#'     msg = "swan_data_directory must be a character string path"
#'   )
#'   assertthat::assert_that(
#'     dir.exists(swan_data_directory),
#'     msg = glue::glue("Directory does not exist: {swan_data_directory}")
#'   )
#'   assertthat::assert_that(
#'     output_format %in% c("long", "wide"),
#'     msg = "output_format must be 'long' or 'wide'"
#'   )
#'
#'   # Step 1: Extract baseline demographics
#'   baseline_demographics_data <- extract_baseline_demographics(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   # Step 2: Extract INVOLEA variables across all visits
#'   involea_longitudinal_data <- extract_involea_variables(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   # Step 3: Merge baseline demographics with INVOLEA data
#'   merged_focused_dataset <- merge_baseline_with_involea(
#'     baseline_data = baseline_demographics_data,
#'     involea_data = involea_longitudinal_data,
#'     verbose = verbose
#'   )
#'
#'   # Step 4: Format output according to user preference
#'   final_focused_dataset <- format_final_dataset(
#'     merged_data = merged_focused_dataset,
#'     format = output_format,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Focused SWAN dataset creation completed")
#'     logger::log_info("Final dataset: {nrow(final_focused_dataset)} rows × {ncol(final_focused_dataset)} columns")
#'   }
#'
#'   return(final_focused_dataset)
#' }
#'
#'
#' #' @noRd
#' extract_baseline_demographics <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting baseline demographics from SWAN files")
#'   }
#'
#'   # Find files likely to contain baseline demographics
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Searching {length(all_rda_files)} RDA files for baseline demographics")
#'   }
#'
#'   # Try to identify baseline/demographic files by name patterns
#'   baseline_file_patterns <- c("28762", "baseline", "demo", "04368")
#'
#'   baseline_candidates <- purrr::keep(all_rda_files, function(file_path) {
#'     file_name <- tolower(basename(file_path))
#'     any(stringr::str_detect(file_name, baseline_file_patterns))
#'   })
#'
#'   if (verbose) {
#'     logger::log_info("Found {length(baseline_candidates)} potential baseline files:")
#'     purrr::walk(baseline_candidates, ~logger::log_info("  - {basename(.x)}"))
#'   }
#'
#'   # Load and process baseline files
#'   baseline_datasets <- purrr::map(baseline_candidates, function(file_path) {
#'
#'     if (verbose) {
#'       logger::log_info("Processing baseline file: {basename(file_path)}")
#'     }
#'
#'     # Load file
#'     file_environment <- new.env()
#'     load(file_path, envir = file_environment)
#'     raw_baseline_data <- file_environment[[ls(file_environment)[1]]]
#'
#'     # Look for demographic variables
#'     demo_variables <- identify_demographic_variables(raw_baseline_data)
#'
#'     if (length(demo_variables$participant_id) == 0) {
#'       if (verbose) {
#'         logger::log_warn("No participant ID found in {basename(file_path)}")
#'       }
#'       return(NULL)
#'     }
#'
#'     # Extract and clean demographics
#'     cleaned_baseline <- extract_and_clean_demographics(
#'       dataset = raw_baseline_data,
#'       demo_vars = demo_variables,
#'       source_file = basename(file_path),
#'       verbose = verbose
#'     )
#'
#'     return(cleaned_baseline)
#'   })
#'
#'   # Combine baseline datasets
#'   baseline_datasets <- purrr::compact(baseline_datasets)
#'
#'   if (length(baseline_datasets) == 0) {
#'     logger::log_error("No valid baseline demographic data found")
#'     stop("No baseline demographic data could be extracted")
#'   }
#'
#'   # Merge multiple baseline sources if needed
#'   combined_baseline <- combine_baseline_sources(baseline_datasets, verbose)
#'
#'   if (verbose) {
#'     logger::log_info("Baseline demographics extracted: {nrow(combined_baseline)} participants")
#'   }
#'
#'   return(combined_baseline)
#' }
#'
#'
#' #' @noRd
#' extract_involea_variables <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting INVOLEA variables across all visits")
#'   }
#'
#'   # Find all RDA files
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   # Process each file to find INVOLEA variables
#'   involea_datasets <- purrr::map(all_rda_files, function(file_path) {
#'
#'     if (verbose) {
#'       logger::log_info("Scanning {basename(file_path)} for INVOLEA variables")
#'     }
#'
#'     # Load file
#'     file_environment <- new.env()
#'     tryCatch({
#'       load(file_path, envir = file_environment)
#'     }, error = function(e) {
#'       if (verbose) {
#'         logger::log_warn("Failed to load {basename(file_path)}: {e$message}")
#'       }
#'       return(NULL)
#'     })
#'
#'     if (length(ls(file_environment)) == 0) return(NULL)
#'
#'     raw_dataset <- file_environment[[ls(file_environment)[1]]]
#'
#'     # Look for INVOLEA variables
#'     involea_columns <- identify_involea_variables(raw_dataset)
#'
#'     if (length(involea_columns) == 0) {
#'       return(NULL)  # No INVOLEA variables in this file
#'     }
#'
#'     if (verbose) {
#'       logger::log_info("  Found {length(involea_columns)} INVOLEA variables: {paste(involea_columns, collapse = ', ')}")
#'     }
#'
#'     # Extract INVOLEA data
#'     involea_data <- extract_involea_data(
#'       dataset = raw_dataset,
#'       involea_vars = involea_columns,
#'       source_file = basename(file_path),
#'       verbose = verbose
#'     )
#'
#'     return(involea_data)
#'   })
#'
#'   # Combine all INVOLEA datasets
#'   involea_datasets <- purrr::compact(involea_datasets)
#'
#'   if (length(involea_datasets) == 0) {
#'     logger::log_error("No INVOLEA variables found in any files")
#'     stop("No INVOLEA variables could be found")
#'   }
#'
#'   combined_involea <- purrr::map_dfr(involea_datasets, ~.x)
#'
#'   # Remove duplicates (same participant, visit, and variable)
#'   combined_involea <- combined_involea %>%
#'     dplyr::distinct(
#'       swan_participant_id,
#'       visit_number,
#'       involea_source_variable,
#'       .keep_all = TRUE
#'     )
#'
#'   if (verbose) {
#'     logger::log_info("INVOLEA variables extracted: {nrow(combined_involea)} observations")
#'     logger::log_info("Visit range: {min(combined_involea$visit_number, na.rm = TRUE)} to {max(combined_involea$visit_number, na.rm = TRUE)}")
#'     logger::log_info("Unique participants with INVOLEA: {length(unique(combined_involea$swan_participant_id))}")
#'   }
#'
#'   return(combined_involea)
#' }
#'
#'
#' #' @noRd
#' identify_demographic_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Identify participant ID
#'   participant_id_patterns <- c("SWANID", "ARCHID", "swanid", "archid")
#'   participant_id_col <- NULL
#'   for (pattern in participant_id_patterns) {
#'     if (pattern %in% variable_names) {
#'       participant_id_col <- pattern
#'       break
#'     }
#'   }
#'
#'   # Identify demographic variables
#'   age_vars <- variable_names[stringr::str_detect(variable_names, "AGE|age")]
#'   bmi_vars <- variable_names[stringr::str_detect(variable_names, "BMI|bmi")]
#'   race_vars <- variable_names[stringr::str_detect(variable_names, "RACE|race")]
#'   smoking_vars <- variable_names[stringr::str_detect(variable_names, "SMOKE|smoke")]
#'   parity_vars <- variable_names[stringr::str_detect(variable_names, "PREG|CHILD|preg|child|parity")]
#'   menopausal_vars <- variable_names[stringr::str_detect(variable_names, "STATUS|MENO|status|meno")]
#'
#'   return(list(
#'     participant_id = participant_id_col,
#'     age = age_vars,
#'     bmi = bmi_vars,
#'     race = race_vars,
#'     smoking = smoking_vars,
#'     parity = parity_vars,
#'     menopausal = menopausal_vars
#'   ))
#' }
#'
#'
#' #' @noRd
#' identify_involea_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Look for INVOLEA variables (including LEKINVO variants)
#'   involea_patterns <- c("INVOLEA", "LEKINVO", "involea", "lekinvo")
#'
#'   involea_variables <- c()
#'   for (pattern in involea_patterns) {
#'     matching_vars <- variable_names[stringr::str_detect(variable_names, pattern)]
#'     involea_variables <- c(involea_variables, matching_vars)
#'   }
#'
#'   return(unique(involea_variables))
#' }
#'
#'
#' #' @noRd
#' extract_and_clean_demographics <- function(dataset, demo_vars, source_file, verbose) {
#'
#'   # Start with participant ID
#'   cleaned_data <- tibble::as_tibble(dataset) %>%
#'     dplyr::select(dplyr::all_of(demo_vars$participant_id))
#'
#'   names(cleaned_data)[1] <- "swan_participant_id"
#'
#'   # Add demographic variables if available - ensuring consistent data types
#'   if (length(demo_vars$age) > 0) {
#'     cleaned_data$baseline_age_years <- as.numeric(as.character(dataset[[demo_vars$age[1]]]))
#'   }
#'
#'   if (length(demo_vars$bmi) > 0) {
#'     cleaned_data$baseline_bmi_kg_m2 <- as.numeric(as.character(dataset[[demo_vars$bmi[1]]]))
#'   }
#'
#'   if (length(demo_vars$race) > 0) {
#'     cleaned_data$baseline_race_ethnicity <- clean_race_variable(dataset[[demo_vars$race[1]]])
#'   }
#'
#'   if (length(demo_vars$smoking) > 0) {
#'     cleaned_data$baseline_smoking_status <- clean_smoking_variable(dataset[[demo_vars$smoking[1]]])
#'   }
#'
#'   if (length(demo_vars$parity) > 0) {
#'     cleaned_data$baseline_parity_count <- as.numeric(as.character(dataset[[demo_vars$parity[1]]]))
#'   }
#'
#'   if (length(demo_vars$menopausal) > 0) {
#'     cleaned_data$baseline_menopausal_stage <- clean_menopausal_variable(dataset[[demo_vars$menopausal[1]]])
#'   }
#'
#'   # Standardize participant ID
#'   cleaned_data$swan_participant_id <- as.character(cleaned_data$swan_participant_id)
#'   cleaned_data$source_file <- source_file
#'
#'   if (verbose) {
#'     logger::log_info("  Extracted {nrow(cleaned_data)} participants with {ncol(cleaned_data)-2} demographic variables")
#'   }
#'
#'   return(cleaned_data)
#' }
#'
#'
#' #' @noRd
#' extract_involea_data <- function(dataset, involea_vars, source_file, verbose) {
#'
#'   # Identify participant ID and visit variables
#'   participant_id_col <- identify_participant_id_column(dataset)
#'   visit_col <- identify_visit_column(dataset)
#'
#'   if (is.null(participant_id_col)) {
#'     if (verbose) {
#'       logger::log_warn("No participant ID found in {source_file}")
#'     }
#'     return(NULL)
#'   }
#'
#'   # Select relevant columns
#'   core_columns <- c(participant_id_col)
#'   if (!is.null(visit_col)) {
#'     core_columns <- c(core_columns, visit_col)
#'   }
#'
#'   # Look for visit-specific demographic variables
#'   visit_specific_demographics <- list()
#'
#'   # Age variables (AGE0, AGE1, etc.)
#'   age_vars <- grep("^AGE\\d+$", names(dataset), value = TRUE)
#'   if (length(age_vars) > 0) {
#'     visit_specific_demographics[["age"]] <- age_vars[1]
#'   }
#'
#'   # BMI variables (BMI0, BMI1, etc.)
#'   bmi_vars <- grep("^BMI\\d+$", names(dataset), value = TRUE)
#'   if (length(bmi_vars) > 0) {
#'     visit_specific_demographics[["bmi"]] <- bmi_vars[1]
#'   }
#'
#'   # Weight variables (WEIGHT0, WEIGHT1, etc.)
#'   weight_vars <- grep("^WEIGHT\\d+$", names(dataset), value = TRUE)
#'   if (length(weight_vars) > 0) {
#'     visit_specific_demographics[["weight"]] <- weight_vars[1]
#'   }
#'
#'   # Height variables (HEIGHT0, HEIGHT1, etc.)
#'   height_vars <- grep("^HEIGHT\\d+$", names(dataset), value = TRUE)
#'   if (length(height_vars) > 0) {
#'     visit_specific_demographics[["height"]] <- height_vars[1]
#'   }
#'
#'   # Smoking variables (SMOKERE0, SMOKERE1, etc.)
#'   smoking_vars <- grep("^SMOKERE\\d+$", names(dataset), value = TRUE)
#'   if (length(smoking_vars) > 0) {
#'     visit_specific_demographics[["smoking"]] <- smoking_vars[1]
#'   }
#'
#'   # Menopausal status variables (STATUS0, STATUS1, etc.)
#'   menopause_vars <- grep("^STATUS\\d+$", names(dataset), value = TRUE)
#'   if (length(menopause_vars) > 0) {
#'     visit_specific_demographics[["menopause"]] <- menopause_vars[1]
#'   }
#'
#'   # Add all found demographic variables to core columns
#'   demo_vars_to_extract <- unlist(visit_specific_demographics)
#'   core_columns <- c(core_columns, demo_vars_to_extract)
#'
#'   involea_dataset <- tibble::as_tibble(dataset) %>%
#'     dplyr::select(dplyr::all_of(c(core_columns, involea_vars)))
#'
#'   # Standardize column names
#'   names(involea_dataset)[1] <- "swan_participant_id"
#'   if (!is.null(visit_col)) {
#'     names(involea_dataset)[2] <- "visit_number_from_file"
#'   } else {
#'     involea_dataset$visit_number_from_file <- NA_real_
#'   }
#'
#'   # Rename visit-specific demographic variables with standardized names
#'   for (demo_type in names(visit_specific_demographics)) {
#'     var_name <- visit_specific_demographics[[demo_type]]
#'     if (var_name %in% names(involea_dataset)) {
#'       new_name <- paste0("visit_specific_", demo_type)
#'       names(involea_dataset)[names(involea_dataset) == var_name] <- new_name
#'     }
#'   }
#'
#'   # Convert to long format
#'   involea_long <- involea_dataset %>%
#'     tidyr::pivot_longer(
#'       cols = dplyr::all_of(involea_vars),
#'       names_to = "involea_source_variable",
#'       values_to = "involea_raw_response"
#'     ) %>%
#'     dplyr::mutate(
#'       swan_participant_id = as.character(swan_participant_id),
#'       # ALWAYS extract visit number from variable name since SWAN files
#'       # have one variable per visit per file
#'       visit_number = extract_visit_number_from_variable_name(involea_source_variable),
#'       involea_incontinence = clean_involea_response(involea_raw_response),
#'       source_file = source_file
#'     ) %>%
#'     dplyr::select(-visit_number_from_file) %>%
#'     dplyr::filter(!is.na(involea_raw_response))  # Remove missing responses
#'
#'   # Debug logging to see what visit numbers we actually extracted
#'   if (verbose && nrow(involea_long) > 0) {
#'     unique_visits <- sort(unique(involea_long$visit_number))
#'     logger::log_info("  Extracted visits for {source_file}: {paste(unique_visits, collapse = ', ')}")
#'     logger::log_info("  Observations in {source_file}: {nrow(involea_long)}")
#'
#'     # Log which demographic variables were found
#'     demo_vars_found <- names(involea_long)[stringr::str_starts(names(involea_long), "visit_specific_")]
#'     if (length(demo_vars_found) > 0) {
#'       logger::log_info("  Visit-specific demographics: {paste(str_remove(demo_vars_found, 'visit_specific_'), collapse = ', ')}")
#'     }
#'
#'     # Log age if available
#'     if ("visit_specific_age" %in% names(involea_long) && !all(is.na(involea_long$visit_specific_age))) {
#'       age_summary <- round(mean(involea_long$visit_specific_age, na.rm = TRUE), 1)
#'       logger::log_info("  Mean age at this visit: {age_summary}")
#'     }
#'   }
#'
#'   return(involea_long)
#' }
#'
#'
#' #' @noRd
#' extract_visit_number_from_variable_name <- function(variable_names) {
#'
#'   # Simple and robust extraction of numbers at end of variable names
#'   visit_numbers <- stringr::str_extract(variable_names, "\\d+$")
#'   visit_numbers <- as.numeric(visit_numbers)
#'
#'   # Replace any NAs with 0 (baseline)
#'   visit_numbers[is.na(visit_numbers)] <- 0
#'
#'   return(visit_numbers)
#' }
#'
#'
#' #' @noRd
#' clean_involea_response <- function(response_raw) {
#'
#'   dplyr::case_when(
#'     # Standard formats
#'     response_raw %in% c("(2) Yes", "2", 2, "Yes", "YES", "yes") ~ 1,
#'     response_raw %in% c("(1) No", "1", 1, "No", "NO", "no") ~ 0,
#'     # Extended formats found in visits 6, 8, 9
#'     response_raw %in% c("(2) 2: Yes", "(2)2: Yes", "2: Yes") ~ 1,
#'     response_raw %in% c("(1) 1: No", "(1)1: No", "1: No") ~ 0,
#'     TRUE ~ NA_real_
#'   )
#' }
#'
#'
#' #' @noRd
#' combine_baseline_sources <- function(baseline_datasets, verbose) {
#'
#'   if (length(baseline_datasets) == 1) {
#'     return(baseline_datasets[[1]])
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Combining {length(baseline_datasets)} baseline sources")
#'   }
#'
#'   # Start with the most complete dataset
#'   dataset_completeness <- purrr::map_dbl(baseline_datasets, function(ds) {
#'     sum(!is.na(ds$swan_participant_id))
#'   })
#'
#'   primary_dataset <- baseline_datasets[[which.max(dataset_completeness)]]
#'
#'   # Add data from other sources
#'   for (i in seq_along(baseline_datasets)) {
#'     if (i == which.max(dataset_completeness)) next
#'
#'     secondary_dataset <- baseline_datasets[[i]]
#'
#'     # Harmonize data types before merging
#'     secondary_dataset <- harmonize_data_types(secondary_dataset, primary_dataset, verbose)
#'
#'     primary_dataset <- primary_dataset %>%
#'       dplyr::full_join(
#'         secondary_dataset,
#'         by = "swan_participant_id",
#'         suffix = c("", "_secondary")
#'       ) %>%
#'       dplyr::mutate(
#'         # Use primary data preferentially with type-safe coalesce
#'         baseline_age_years = safe_coalesce_numeric(
#'           baseline_age_years,
#'           baseline_age_years_secondary
#'         ),
#'         baseline_bmi_kg_m2 = safe_coalesce_numeric(
#'           baseline_bmi_kg_m2,
#'           baseline_bmi_kg_m2_secondary
#'         ),
#'         baseline_parity_count = safe_coalesce_numeric(
#'           baseline_parity_count,
#'           baseline_parity_count_secondary
#'         ),
#'         baseline_smoking_status = safe_coalesce_character(
#'           baseline_smoking_status,
#'           baseline_smoking_status_secondary
#'         ),
#'         baseline_race_ethnicity = safe_coalesce_character(
#'           baseline_race_ethnicity,
#'           baseline_race_ethnicity_secondary
#'         ),
#'         baseline_menopausal_stage = safe_coalesce_character(
#'           baseline_menopausal_stage,
#'           baseline_menopausal_stage_secondary
#'         )
#'       ) %>%
#'       dplyr::select(-dplyr::ends_with("_secondary"))
#'   }
#'
#'   return(primary_dataset)
#' }
#'
#'
#' #' @noRd
#' harmonize_data_types <- function(secondary_dataset, primary_dataset, verbose) {
#'
#'   # Get common baseline columns
#'   baseline_columns <- names(secondary_dataset)[stringr::str_starts(names(secondary_dataset), "baseline_")]
#'
#'   for (col_name in baseline_columns) {
#'     if (col_name %in% names(primary_dataset)) {
#'
#'       primary_type <- class(primary_dataset[[col_name]])[1]
#'       secondary_type <- class(secondary_dataset[[col_name]])[1]
#'
#'       if (primary_type != secondary_type) {
#'         if (verbose) {
#'           logger::log_info("Harmonizing {col_name}: {secondary_type} -> {primary_type}")
#'         }
#'
#'         # Convert secondary to match primary type
#'         if (primary_type == "numeric" || primary_type == "double") {
#'           secondary_dataset[[col_name]] <- as.numeric(as.character(secondary_dataset[[col_name]]))
#'         } else if (primary_type == "character") {
#'           secondary_dataset[[col_name]] <- as.character(secondary_dataset[[col_name]])
#'         } else if (primary_type == "logical") {
#'           secondary_dataset[[col_name]] <- as.logical(secondary_dataset[[col_name]])
#'         }
#'       }
#'     }
#'   }
#'
#'   return(secondary_dataset)
#' }
#'
#'
#' #' @noRd
#' safe_coalesce_numeric <- function(x, y) {
#'   # Convert both to numeric if needed
#'   x_numeric <- if (is.numeric(x)) x else as.numeric(as.character(x))
#'   y_numeric <- if (is.numeric(y)) y else as.numeric(as.character(y))
#'   dplyr::coalesce(x_numeric, y_numeric)
#' }
#'
#'
#' #' @noRd
#' safe_coalesce_character <- function(x, y) {
#'   # Convert both to character if needed
#'   x_char <- if (is.character(x)) x else as.character(x)
#'   y_char <- if (is.character(y)) y else as.character(y)
#'   dplyr::coalesce(x_char, y_char)
#' }
#'
#'
#' #' @noRd
#' merge_baseline_with_involea <- function(baseline_data, involea_data, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Merging baseline demographics with INVOLEA data")
#'     logger::log_info("Baseline participants: {length(unique(baseline_data$swan_participant_id))}")
#'     logger::log_info("INVOLEA observations: {nrow(involea_data)}")
#'   }
#'
#'   # Merge datasets
#'   merged_data <- involea_data %>%
#'     dplyr::left_join(
#'       baseline_data %>% dplyr::select(-source_file),
#'       by = "swan_participant_id"
#'     )
#'
#'   # Add time variables and current age
#'   merged_data <- merged_data %>%
#'     dplyr::mutate(
#'       years_since_baseline = visit_number,
#'       current_age_years = baseline_age_years + years_since_baseline
#'     ) %>%
#'     dplyr::arrange(swan_participant_id, visit_number)
#'
#'   if (verbose) {
#'     logger::log_info("Merged dataset: {nrow(merged_data)} observations")
#'     logger::log_info("Participants with both baseline + INVOLEA: {length(unique(merged_data$swan_participant_id[!is.na(merged_data$baseline_age_years)]))}")
#'   }
#'
#'   return(merged_data)
#' }
#'
#'
#' #' @noRd
#' format_final_dataset <- function(merged_data, format, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Formatting final dataset as: {format}")
#'     logger::log_info("Available columns: {paste(names(merged_data), collapse = ', ')}")
#'   }
#'
#'   if (format == "long") {
#'     # Get all visit-specific demographic columns
#'     visit_specific_cols <- names(merged_data)[stringr::str_starts(names(merged_data), "visit_specific_")]
#'
#'     if (verbose && length(visit_specific_cols) > 0) {
#'       logger::log_info("Visit-specific columns found: {paste(visit_specific_cols, collapse = ', ')}")
#'     } else if (verbose) {
#'       logger::log_warn("No visit-specific columns found - they may have been lost during processing")
#'     }
#'
#'     # Build the selection dynamically to avoid missing column errors
#'     base_columns <- c(
#'       "swan_participant_id",
#'       "visit_number",
#'       "years_since_baseline",
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_parity_count",
#'       "baseline_menopausal_stage",
#'       "involea_incontinence",
#'       "involea_raw_response",
#'       "involea_source_variable"
#'     )
#'
#'     # Add visit-specific columns if they exist
#'     all_columns_to_select <- c(base_columns, visit_specific_cols)
#'
#'     # Only select columns that actually exist in the data
#'     columns_that_exist <- intersect(all_columns_to_select, names(merged_data))
#'
#'     if (verbose) {
#'       logger::log_info("Selecting {length(columns_that_exist)} columns for final dataset")
#'     }
#'
#'     # Clean up but preserve all participant-visit combinations
#'     final_data <- merged_data %>%
#'       dplyr::select(dplyr::all_of(columns_that_exist)) %>%
#'       # Only remove true duplicates (same participant, visit, AND response)
#'       # but keep different visits for the same participant
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         involea_source_variable,
#'         .keep_all = TRUE
#'       ) %>%
#'       dplyr::arrange(swan_participant_id, visit_number)
#'
#'   } else {
#'     # Convert to wide format
#'     visit_specific_cols <- names(merged_data)[stringr::str_starts(names(merged_data), "visit_specific_")]
#'
#'     base_columns <- c(
#'       "swan_participant_id",
#'       "visit_number",
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_parity_count",
#'       "baseline_menopausal_stage",
#'       "involea_incontinence"
#'     )
#'
#'     all_columns_to_select <- c(base_columns, visit_specific_cols)
#'     columns_that_exist <- intersect(all_columns_to_select, names(merged_data))
#'
#'     final_data <- merged_data %>%
#'       dplyr::select(dplyr::all_of(columns_that_exist)) %>%
#'       # Only remove true duplicates before pivoting
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         .keep_all = TRUE
#'       ) %>%
#'       tidyr::pivot_wider(
#'         names_from = visit_number,
#'         values_from = c(dplyr::any_of(visit_specific_cols), involea_incontinence),
#'         names_sep = "_visit_"
#'       )
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Final formatted dataset: {nrow(final_data)} rows")
#'     logger::log_info("Final columns: {paste(names(final_data), collapse = ', ')}")
#'     logger::log_info("Unique participants: {length(unique(final_data$swan_participant_id))}")
#'     if (format == "long") {
#'       final_visit_specific <- sum(stringr::str_starts(names(final_data), "visit_specific_"))
#'       logger::log_info("Visit-specific variables in final dataset: {final_visit_specific}")
#'       logger::log_info("Visit range: {min(final_data$visit_number, na.rm = TRUE)} to {max(final_data$visit_number, na.rm = TRUE)}")
#'     }
#'   }
#'
#'   return(final_data)
#' }
#'
#'
#' # Helper functions from previous code
#' #' @noRd
#' identify_participant_id_column <- function(dataset) {
#'   participant_id_patterns <- c("^SWANID$", "^ARCHID$", "^swanid$", "^archid$")
#'   for (pattern in participant_id_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' @noRd
#' identify_visit_column <- function(dataset) {
#'   visit_patterns <- c("^VISIT$", "^visit$", "^Visit$", "^VISITNO$")
#'   for (pattern in visit_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' @noRd
#' clean_race_variable <- function(race_raw) {
#'   dplyr::case_when(
#'     stringr::str_detect(as.character(race_raw), "White|Caucasian") ~ "White",
#'     stringr::str_detect(as.character(race_raw), "Black|African") ~ "Black",
#'     stringr::str_detect(as.character(race_raw), "Hispanic") ~ "Hispanic",
#'     stringr::str_detect(as.character(race_raw), "Chinese") ~ "Chinese",
#'     stringr::str_detect(as.character(race_raw), "Japanese") ~ "Japanese",
#'     TRUE ~ "Other"
#'   )
#' }
#'
#' #' @noRd
#' clean_smoking_variable <- function(smoking_raw) {
#'   dplyr::case_when(
#'     smoking_raw %in% c(1, "(1) Never smoked") ~ "Never",
#'     smoking_raw %in% c(2, "(2) Past smoker") ~ "Past",
#'     smoking_raw %in% c(3, "(3) Current smoker") ~ "Current",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' @noRd
#' clean_menopausal_variable <- function(status_raw) {
#'   dplyr::case_when(
#'     status_raw %in% c(2, "(2) Postmenopausal") ~ "Post",
#'     status_raw %in% c(3, "(3) Late perimenopausal") ~ "Late Peri",
#'     status_raw %in% c(4, "(4) Early perimenopausal") ~ "Early Peri",
#'     status_raw %in% c(5, "(5) Premenopausal") ~ "Pre",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' # Create the comprehensive dataset with the bug fix
#' comprehensive_swan_fixed <- create_focused_swan_dataset(
#'   swan_data_directory = swan_directory,
#'   output_format = "long",
#'   verbose = TRUE
#' )
#'
#' # Check what we got this time
#' cat("=== BUG FIX RESULTS ===\n")
#' cat("Final dataset columns:\n")
#' print(names(comprehensive_swan_fixed))
#'
#' # Check for visit-specific variables
#' visit_specific_vars_fixed <- names(comprehensive_swan_fixed)[str_starts(names(comprehensive_swan_fixed), "visit_specific_")]
#' cat("\nVisit-specific variables found:\n")
#' print(visit_specific_vars_fixed)
#'
#' # If we have them, create the comprehensive summary
#' if (length(visit_specific_vars_fixed) > 0) {
#'   time_varying_summary_comprehensive <- comprehensive_swan_fixed %>%
#'     group_by(visit_number) %>%
#'     summarise(
#'       participants = n(),
#'       incontinence_prev = round(mean(involea_incontinence), 3),
#'       mean_age = round(mean(visit_specific_age, na.rm = TRUE), 1),
#'       mean_bmi = round(mean(visit_specific_bmi, na.rm = TRUE), 1),
#'       mean_weight = round(mean(visit_specific_weight, na.rm = TRUE), 1),
#'       mean_height = round(mean(visit_specific_height, na.rm = TRUE), 1),
#'       .groups = "drop"
#'     )
#'
#'   print(time_varying_summary_comprehensive)
#' } else {
#'   cat("Still no visit-specific variables - need further debugging\n")
#' }
#'
#'
#' # Robust SWAN Dataset Creation Function function at 2023 ----
#' #' Create Comprehensive SWAN Dataset with Visit-Specific Variables
#' #'
#' #' This function creates a comprehensive dataset from SWAN study files, extracting
#' #' baseline demographics and visit-specific variables including urinary incontinence
#' #' measures (INVOLEA/LEKINVO) and other longitudinal health indicators.
#' #'
#' #' @param swan_data_directory Character. Path to directory containing SWAN .rda files
#' #' @param output_format Character. Either "long" or "wide" format for final dataset
#' #' @param verbose Logical. Enable detailed logging output to console
#' #' @param include_visit_demographics Logical. Include visit-specific demographic
#' #'   variables (AGE, BMI, STATUS, etc.) in addition to baseline
#' #' @param filter_valid_responses Logical. Remove rows with missing INVOLEA responses
#' #'
#' #' @return tibble. Comprehensive SWAN dataset with baseline and visit-specific variables
#' #'
#' #' @examples
#' #' # Example 1: Basic long format with visit demographics
#' #' swan_long_comprehensive <- create_comprehensive_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "long",
#' #'   verbose = TRUE,
#' #'   include_visit_demographics = TRUE,
#' #'   filter_valid_responses = TRUE
#' #' )
#' #'
#' #' # Example 2: Wide format for analysis, no filtering
#' #' swan_wide_complete <- create_comprehensive_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "wide",
#' #'   verbose = FALSE,
#' #'   include_visit_demographics = TRUE,
#' #'   filter_valid_responses = FALSE
#' #' )
#' #'
#' #' # Example 3: Minimal dataset with only baseline + INVOLEA
#' #' swan_minimal <- create_comprehensive_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "long",
#' #'   verbose = TRUE,
#' #'   include_visit_demographics = FALSE,
#' #'   filter_valid_responses = TRUE
#' #' )
#' #'
#' #' @importFrom dplyr filter select mutate arrange group_by summarise
#' #'   left_join full_join distinct coalesce case_when all_of any_of ends_with
#' #' @importFrom tidyr pivot_longer pivot_wider
#' #' @importFrom stringr str_detect str_extract str_replace_all str_starts
#' #'   str_remove str_match
#' #' @importFrom purrr map_dfr map_chr map_lgl keep walk compact map
#' #' @importFrom assertthat assert_that
#' #' @importFrom logger log_info log_warn log_error
#' #' @importFrom tibble tibble as_tibble
#' #' @importFrom readr parse_number
#' #' @importFrom glue glue
#' #'
#' #' @export
#' create_comprehensive_swan_dataset <- function(swan_data_directory,
#'                                               output_format = "long",
#'                                               verbose = TRUE,
#'                                               include_visit_demographics = TRUE,
#'                                               filter_valid_responses = TRUE) {
#'
#'   # Input validation with assertthat
#'   if (verbose) {
#'     logger::log_info("Creating comprehensive SWAN dataset with visit-specific variables")
#'     logger::log_info("Function inputs - directory: {swan_data_directory}")
#'     logger::log_info("Function inputs - format: {output_format}, verbose: {verbose}")
#'     logger::log_info("Function inputs - include_visit_demographics: {include_visit_demographics}")
#'     logger::log_info("Function inputs - filter_valid_responses: {filter_valid_responses}")
#'   }
#'
#'   assertthat::assert_that(
#'     is.character(swan_data_directory),
#'     msg = "swan_data_directory must be a character string path"
#'   )
#'   assertthat::assert_that(
#'     dir.exists(swan_data_directory),
#'     msg = glue::glue("Directory does not exist: {swan_data_directory}")
#'   )
#'   assertthat::assert_that(
#'     output_format %in% c("long", "wide"),
#'     msg = "output_format must be 'long' or 'wide'"
#'   )
#'   assertthat::assert_that(
#'     is.logical(verbose),
#'     msg = "verbose must be TRUE or FALSE"
#'   )
#'   assertthat::assert_that(
#'     is.logical(include_visit_demographics),
#'     msg = "include_visit_demographics must be TRUE or FALSE"
#'   )
#'   assertthat::assert_that(
#'     is.logical(filter_valid_responses),
#'     msg = "filter_valid_responses must be TRUE or FALSE"
#'   )
#'
#'   # Step 1: Extract baseline demographics
#'   baseline_participant_demographics <- extract_baseline_participant_demographics(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Baseline demographics extracted: {nrow(baseline_participant_demographics)} participants")
#'   }
#'
#'   # Step 2: Extract comprehensive longitudinal data
#'   longitudinal_health_measurements <- extract_comprehensive_longitudinal_data(
#'     directory_path = swan_data_directory,
#'     include_visit_demographics = include_visit_demographics,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Longitudinal data extracted: {nrow(longitudinal_health_measurements)} observations")
#'   }
#'
#'   # Step 3: Merge datasets
#'   merged_comprehensive_dataset <- merge_baseline_with_longitudinal_data(
#'     baseline_data = baseline_participant_demographics,
#'     longitudinal_data = longitudinal_health_measurements,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Data merged: {nrow(merged_comprehensive_dataset)} total observations")
#'   }
#'
#'   # Step 4: Apply filtering if requested
#'   if (filter_valid_responses) {
#'     filtered_comprehensive_dataset <- apply_response_filtering(
#'       merged_data = merged_comprehensive_dataset,
#'       verbose = verbose
#'     )
#'   } else {
#'     filtered_comprehensive_dataset <- merged_comprehensive_dataset
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("After filtering: {nrow(filtered_comprehensive_dataset)} observations")
#'   }
#'
#'   # Step 5: Format final output
#'   final_comprehensive_dataset <- format_comprehensive_output(
#'     merged_data = filtered_comprehensive_dataset,
#'     format = output_format,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Comprehensive SWAN dataset creation completed")
#'     logger::log_info("Final dataset dimensions: {nrow(final_comprehensive_dataset)} rows × {ncol(final_comprehensive_dataset)} columns")
#'     logger::log_info("Output file path: Not saved to file (returned as object)")
#'   }
#'
#'   return(final_comprehensive_dataset)
#' }
#'
#' #' Extract baseline demographics from SWAN screener/baseline files
#' #' @noRd
#' extract_baseline_participant_demographics <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting baseline participant demographics from SWAN files")
#'   }
#'
#'   # Find baseline/screener files using known ICPSR numbers
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   # Known baseline file patterns from SWAN documentation
#'   baseline_file_identifiers <- c("28762", "04368", "baseline", "screener")
#'
#'   baseline_candidate_files <- purrr::keep(all_rda_files, function(file_path) {
#'     file_name <- tolower(basename(file_path))
#'     any(stringr::str_detect(file_name, baseline_file_identifiers))
#'   })
#'
#'   if (verbose) {
#'     logger::log_info("Found {length(baseline_candidate_files)} potential baseline files")
#'     purrr::walk(baseline_candidate_files, ~logger::log_info("  Candidate file: {basename(.x)}"))
#'   }
#'
#'   # Process each baseline file
#'   baseline_dataset_list <- purrr::map(baseline_candidate_files, function(file_path) {
#'     process_single_baseline_file(file_path, verbose)
#'   })
#'
#'   # Remove NULL results and combine
#'   baseline_dataset_list <- purrr::compact(baseline_dataset_list)
#'
#'   if (length(baseline_dataset_list) == 0) {
#'     logger::log_error("No valid baseline demographic data found in any files")
#'     stop("No baseline demographic data could be extracted")
#'   }
#'
#'   # Combine multiple baseline sources
#'   combined_baseline_demographics <- combine_multiple_baseline_sources(
#'     baseline_dataset_list,
#'     verbose
#'   )
#'
#'   return(combined_baseline_demographics)
#' }
#'
#' #' Process a single baseline file to extract demographics
#' #' @noRd
#' process_single_baseline_file <- function(file_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Processing baseline file: {basename(file_path)}")
#'   }
#'
#'   # Load the file safely
#'   file_environment <- new.env()
#'
#'   tryCatch({
#'     load(file_path, envir = file_environment)
#'   }, error = function(e) {
#'     if (verbose) {
#'       logger::log_error("Failed to load {basename(file_path)}: {e$message}")
#'     }
#'     return(NULL)
#'   })
#'
#'   if (length(ls(file_environment)) == 0) {
#'     if (verbose) {
#'       logger::log_warn("No datasets found in {basename(file_path)}")
#'     }
#'     return(NULL)
#'   }
#'
#'   raw_baseline_dataset <- file_environment[[ls(file_environment)[1]]]
#'
#'   # Identify demographic variables
#'   demographic_variable_mapping <- identify_baseline_demographic_variables(
#'     raw_baseline_dataset
#'   )
#'
#'   if (is.null(demographic_variable_mapping$participant_id)) {
#'     if (verbose) {
#'       logger::log_warn("No participant ID found in {basename(file_path)}")
#'     }
#'     return(NULL)
#'   }
#'
#'   # Extract and standardize demographics
#'   standardized_baseline_demographics <- extract_standardized_demographics(
#'     dataset = raw_baseline_dataset,
#'     variable_mapping = demographic_variable_mapping,
#'     source_file = basename(file_path),
#'     verbose = verbose
#'   )
#'
#'   return(standardized_baseline_demographics)
#' }
#'
#' #' Identify demographic variables in baseline dataset
#' #' @noRd
#' identify_baseline_demographic_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Participant ID patterns
#'   participant_id_patterns <- c("^SWANID$", "^ARCHID$", "^swanid$", "^archid$")
#'   participant_id_variable <- find_matching_variable(variable_names, participant_id_patterns)
#'
#'   # Demographic variable patterns (baseline variables often end with 0)
#'   age_patterns <- c("^AGE0?$", "^age0?$", "AGE.*0$")
#'   bmi_patterns <- c("^BMI0?$", "^bmi0?$", "BMI.*0$")
#'   race_patterns <- c("^RACE$", "^race$", "^ETHNIC")
#'   smoking_patterns <- c("^SMOKER0?$", "^SMOKING0?$", "^SMOKE")
#'   parity_patterns <- c("^NUMCHILD$", "^CHILDREN$", "^PARITY")
#'   menopausal_patterns <- c("^STATUS0?$", "^MENO")
#'   education_patterns <- c("^DEGREE$", "^EDUCATION", "^EDUC")
#'   marital_patterns <- c("^MARITAL", "^MARITALGP")
#'
#'   return(list(
#'     participant_id = participant_id_variable,
#'     age = find_matching_variable(variable_names, age_patterns),
#'     bmi = find_matching_variable(variable_names, bmi_patterns),
#'     race = find_matching_variable(variable_names, race_patterns),
#'     smoking = find_matching_variable(variable_names, smoking_patterns),
#'     parity = find_matching_variable(variable_names, parity_patterns),
#'     menopausal = find_matching_variable(variable_names, menopausal_patterns),
#'     education = find_matching_variable(variable_names, education_patterns),
#'     marital = find_matching_variable(variable_names, marital_patterns)
#'   ))
#' }
#'
#' #' Find first matching variable from patterns
#' #' @noRd
#' find_matching_variable <- function(variable_names, patterns) {
#'   for (pattern in patterns) {
#'     matches <- variable_names[stringr::str_detect(variable_names, pattern)]
#'     if (length(matches) > 0) {
#'       return(matches[1])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' Extract and standardize demographic variables
#' #' @noRd
#' extract_standardized_demographics <- function(dataset, variable_mapping,
#'                                               source_file, verbose) {
#'
#'   # Start with participant ID
#'   standardized_demographics <- tibble::tibble(
#'     swan_participant_id = as.character(dataset[[variable_mapping$participant_id]])
#'   )
#'
#'   # Add demographic variables with type-safe conversion
#'   if (!is.null(variable_mapping$age)) {
#'     standardized_demographics$baseline_age_years <-
#'       convert_to_numeric_safely(dataset[[variable_mapping$age]])
#'   }
#'
#'   if (!is.null(variable_mapping$bmi)) {
#'     standardized_demographics$baseline_bmi_kg_m2 <-
#'       convert_to_numeric_safely(dataset[[variable_mapping$bmi]])
#'   }
#'
#'   if (!is.null(variable_mapping$race)) {
#'     standardized_demographics$baseline_race_ethnicity <-
#'       standardize_race_ethnicity_variable(dataset[[variable_mapping$race]])
#'   }
#'
#'   if (!is.null(variable_mapping$smoking)) {
#'     standardized_demographics$baseline_smoking_status <-
#'       standardize_smoking_status_variable(dataset[[variable_mapping$smoking]])
#'   }
#'
#'   if (!is.null(variable_mapping$parity)) {
#'     standardized_demographics$baseline_parity_count <-
#'       convert_to_numeric_safely(dataset[[variable_mapping$parity]])
#'   }
#'
#'   if (!is.null(variable_mapping$menopausal)) {
#'     standardized_demographics$baseline_menopausal_stage <-
#'       standardize_menopausal_status_variable(dataset[[variable_mapping$menopausal]])
#'   }
#'
#'   if (!is.null(variable_mapping$education)) {
#'     standardized_demographics$baseline_education_level <-
#'       standardize_education_variable(dataset[[variable_mapping$education]])
#'   }
#'
#'   if (!is.null(variable_mapping$marital)) {
#'     standardized_demographics$baseline_marital_status <-
#'       standardize_marital_status_variable(dataset[[variable_mapping$marital]])
#'   }
#'
#'   standardized_demographics$source_file <- source_file
#'
#'   if (verbose) {
#'     valid_participant_count <- sum(!is.na(standardized_demographics$swan_participant_id))
#'     demographic_variable_count <- ncol(standardized_demographics) - 2  # exclude ID and source
#'     logger::log_info("  Extracted {valid_participant_count} participants with {demographic_variable_count} demographic variables")
#'   }
#'
#'   return(standardized_demographics)
#' }
#'
#' #' Extract comprehensive longitudinal data including visit-specific variables
#' #' @noRd
#' extract_comprehensive_longitudinal_data <- function(directory_path,
#'                                                     include_visit_demographics,
#'                                                     verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting comprehensive longitudinal data from all SWAN files")
#'   }
#'
#'   # Find all RDA files
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   # Process each file for longitudinal variables
#'   longitudinal_dataset_list <- purrr::map(all_rda_files, function(file_path) {
#'     process_single_longitudinal_file(
#'       file_path = file_path,
#'       include_visit_demographics = include_visit_demographics,
#'       verbose = verbose
#'     )
#'   })
#'
#'   # Remove NULL results
#'   longitudinal_dataset_list <- purrr::compact(longitudinal_dataset_list)
#'
#'   if (length(longitudinal_dataset_list) == 0) {
#'     logger::log_error("No longitudinal data found in any files")
#'     stop("No longitudinal data could be extracted")
#'   }
#'
#'   # Combine all longitudinal datasets
#'   combined_longitudinal_data <- purrr::map_dfr(longitudinal_dataset_list, ~.x)
#'
#'   # Remove duplicate observations (same participant, visit, variable)
#'   deduped_longitudinal_data <- combined_longitudinal_data %>%
#'     dplyr::distinct(
#'       swan_participant_id,
#'       visit_number,
#'       incontinence_source_variable,
#'       .keep_all = TRUE
#'     )
#'
#'   if (verbose) {
#'     unique_participant_count <- length(unique(deduped_longitudinal_data$swan_participant_id))
#'     visit_range <- paste(
#'       min(deduped_longitudinal_data$visit_number, na.rm = TRUE), "to",
#'       max(deduped_longitudinal_data$visit_number, na.rm = TRUE)
#'     )
#'     logger::log_info("Comprehensive longitudinal data extracted: {nrow(deduped_longitudinal_data)} observations")
#'     logger::log_info("Visit range: {visit_range}")
#'     logger::log_info("Unique participants with longitudinal data: {unique_participant_count}")
#'   }
#'
#'   return(deduped_longitudinal_data)
#' }
#'
#' #' Process single file for longitudinal variables
#' #' @noRd
#' process_single_longitudinal_file <- function(file_path, include_visit_demographics, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Scanning {basename(file_path)} for longitudinal variables")
#'   }
#'
#'   # Load file safely
#'   file_environment <- new.env()
#'
#'   tryCatch({
#'     load(file_path, envir = file_environment)
#'   }, error = function(e) {
#'     if (verbose) {
#'       logger::log_warn("Failed to load {basename(file_path)}: {e$message}")
#'     }
#'     return(NULL)
#'   })
#'
#'   if (length(ls(file_environment)) == 0) return(NULL)
#'
#'   raw_longitudinal_dataset <- file_environment[[ls(file_environment)[1]]]
#'
#'   # Look for incontinence variables (INVOLEA/LEKINVO)
#'   incontinence_variable_names <- identify_incontinence_variables(raw_longitudinal_dataset)
#'
#'   if (length(incontinence_variable_names) == 0) {
#'     return(NULL)  # No incontinence variables in this file
#'   }
#'
#'   if (verbose) {
#'     incontinence_vars_string <- paste(incontinence_variable_names, collapse = ", ")
#'     logger::log_info("  Found {length(incontinence_variable_names)} incontinence variables: {incontinence_vars_string}")
#'   }
#'
#'   # Extract longitudinal data
#'   extracted_longitudinal_data <- extract_longitudinal_measurements(
#'     dataset = raw_longitudinal_dataset,
#'     incontinence_variables = incontinence_variable_names,
#'     include_visit_demographics = include_visit_demographics,
#'     source_file = basename(file_path),
#'     verbose = verbose
#'   )
#'
#'   return(extracted_longitudinal_data)
#' }
#'
#' #' Identify incontinence variables (INVOLEA/LEKINVO) in dataset
#' #' @noRd
#' identify_incontinence_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Look for INVOLEA and LEKINVO patterns
#'   incontinence_patterns <- c("INVOLEA\\d*", "LEKINVO\\d*", "involea\\d*", "lekinvo\\d*")
#'
#'   incontinence_variables <- c()
#'   for (pattern in incontinence_patterns) {
#'     matching_variables <- variable_names[stringr::str_detect(variable_names, pattern)]
#'     incontinence_variables <- c(incontinence_variables, matching_variables)
#'   }
#'
#'   return(unique(incontinence_variables))
#' }
#'
#' #' Extract longitudinal measurements including visit-specific demographics
#' #' @noRd
#' extract_longitudinal_measurements <- function(dataset, incontinence_variables,
#'                                               include_visit_demographics,
#'                                               source_file, verbose) {
#'
#'   # Identify core variables
#'   participant_id_variable <- identify_participant_id_variable(dataset)
#'   visit_number_variable <- identify_visit_number_variable(dataset)
#'
#'   if (is.null(participant_id_variable)) {
#'     if (verbose) {
#'       logger::log_warn("No participant ID found in {source_file}")
#'     }
#'     return(NULL)
#'   }
#'
#'   # Start with core columns
#'   core_column_names <- c(participant_id_variable)
#'   if (!is.null(visit_number_variable)) {
#'     core_column_names <- c(core_column_names, visit_number_variable)
#'   }
#'
#'   # Identify visit-specific demographic variables if requested
#'   visit_specific_variables <- list()
#'
#'   if (include_visit_demographics) {
#'     visit_specific_variables <- identify_visit_specific_demographic_variables(dataset)
#'
#'     if (verbose && length(visit_specific_variables) > 0) {
#'       visit_vars_string <- paste(names(visit_specific_variables), collapse = ", ")
#'       logger::log_info("  Visit-specific demographics found: {visit_vars_string}")
#'     }
#'   }
#'
#'   # Combine all variables to extract
#'   all_variables_to_extract <- c(
#'     core_column_names,
#'     unlist(visit_specific_variables),
#'     incontinence_variables
#'   )
#'
#'   # Extract data
#'   longitudinal_measurements <- tibble::as_tibble(dataset) %>%
#'     dplyr::select(dplyr::all_of(all_variables_to_extract))
#'
#'   # Standardize core column names
#'   names(longitudinal_measurements)[1] <- "swan_participant_id"
#'   if (!is.null(visit_number_variable)) {
#'     names(longitudinal_measurements)[2] <- "visit_number_from_file"
#'   } else {
#'     longitudinal_measurements$visit_number_from_file <- NA_real_
#'   }
#'
#'   # Rename visit-specific variables with standardized prefixes
#'   for (variable_type in names(visit_specific_variables)) {
#'     original_variable_name <- visit_specific_variables[[variable_type]]
#'     if (original_variable_name %in% names(longitudinal_measurements)) {
#'       standardized_variable_name <- paste0("visit_", variable_type)
#'       names(longitudinal_measurements)[names(longitudinal_measurements) == original_variable_name] <- standardized_variable_name
#'     }
#'   }
#'
#'   # Convert to long format for incontinence variables
#'   incontinence_long_format <- longitudinal_measurements %>%
#'     tidyr::pivot_longer(
#'       cols = dplyr::all_of(incontinence_variables),
#'       names_to = "incontinence_source_variable",
#'       values_to = "incontinence_raw_response"
#'     ) %>%
#'     dplyr::mutate(
#'       swan_participant_id = as.character(swan_participant_id),
#'       # Extract visit number from variable name (primary method for SWAN)
#'       visit_number = extract_visit_number_from_variable(incontinence_source_variable),
#'       incontinence_status = standardize_incontinence_response(incontinence_raw_response),
#'       years_since_baseline = visit_number,
#'       source_file = source_file
#'     ) %>%
#'     dplyr::select(-visit_number_from_file) %>%
#'     dplyr::filter(!is.na(incontinence_raw_response))  # Remove missing responses
#'
#'   # Log results
#'   if (verbose && nrow(incontinence_long_format) > 0) {
#'     unique_visits <- sort(unique(incontinence_long_format$visit_number))
#'     visits_string <- paste(unique_visits, collapse = ", ")
#'     logger::log_info("  Extracted visits for {source_file}: {visits_string}")
#'     logger::log_info("  Total observations in {source_file}: {nrow(incontinence_long_format)}")
#'
#'     # Log visit-specific variables preserved
#'     visit_specific_columns <- names(incontinence_long_format)[stringr::str_starts(names(incontinence_long_format), "visit_")]
#'     if (length(visit_specific_columns) > 0) {
#'       visit_vars_preserved <- paste(stringr::str_remove(visit_specific_columns, "^visit_"), collapse = ", ")
#'       logger::log_info("  Visit-specific variables preserved: {visit_vars_preserved}")
#'     }
#'   }
#'
#'   return(incontinence_long_format)
#' }
#'
#' #' Identify visit-specific demographic variables in dataset
#' #' @noRd
#' identify_visit_specific_demographic_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'   visit_specific_vars <- list()
#'
#'   # AGE variables (AGE0, AGE1, AGE2, etc.)
#'   age_variables <- variable_names[stringr::str_detect(variable_names, "^AGE\\d+$")]
#'   if (length(age_variables) > 0) {
#'     visit_specific_vars[["age_years"]] <- age_variables[1]
#'   }
#'
#'   # BMI variables (BMI0, BMI1, BMI2, etc.)
#'   bmi_variables <- variable_names[stringr::str_detect(variable_names, "^BMI\\d+$")]
#'   if (length(bmi_variables) > 0) {
#'     visit_specific_vars[["bmi_kg_m2"]] <- bmi_variables[1]
#'   }
#'
#'   # Weight variables (WEIGHT0, WEIGHT1, etc.)
#'   weight_variables <- variable_names[stringr::str_detect(variable_names, "^WEIGHT\\d+$")]
#'   if (length(weight_variables) > 0) {
#'     visit_specific_vars[["weight_kg"]] <- weight_variables[1]
#'   }
#'
#'   # Height variables (HEIGHT0, HEIGHT1, etc.)
#'   height_variables <- variable_names[stringr::str_detect(variable_names, "^HEIGHT\\d+$")]
#'   if (length(height_variables) > 0) {
#'     visit_specific_vars[["height_cm"]] <- height_variables[1]
#'   }
#'
#'   # Smoking variables (SMOKER0, SMOKER1, etc.)
#'   smoking_variables <- variable_names[stringr::str_detect(variable_names, "^SMOKER?E?\\d+$")]
#'   if (length(smoking_variables) > 0) {
#'     visit_specific_vars[["smoking_status"]] <- smoking_variables[1]
#'   }
#'
#'   # Menopausal status variables (STATUS0, STATUS1, etc.)
#'   menopause_variables <- variable_names[stringr::str_detect(variable_names, "^STATUS\\d+$")]
#'   if (length(menopause_variables) > 0) {
#'     visit_specific_vars[["menopausal_stage"]] <- menopause_variables[1]
#'   }
#'
#'   # Marital status variables (MARITALGP0, MARITALGP1, etc.)
#'   marital_variables <- variable_names[stringr::str_detect(variable_names, "^MARITALGP\\d+$")]
#'   if (length(marital_variables) > 0) {
#'     visit_specific_vars[["marital_status"]] <- marital_variables[1]
#'   }
#'
#'   # Parity variables (NUMCHILD0, NUMCHILD1, etc.)
#'   parity_variables <- variable_names[stringr::str_detect(variable_names, "^NUMCHILD\\d+$")]
#'   if (length(parity_variables) > 0) {
#'     visit_specific_vars[["parity_count"]] <- parity_variables[1]
#'   }
#'
#'   return(visit_specific_vars)
#' }
#'
#' #' Extract visit number from variable name (e.g., INVOLEA9 -> 9)
#' #' @noRd
#' extract_visit_number_from_variable <- function(variable_names) {
#'
#'   # Extract trailing digits from variable names
#'   visit_numbers <- stringr::str_extract(variable_names, "\\d+$")
#'   visit_numbers <- as.numeric(visit_numbers)
#'
#'   # Replace NAs with 0 (baseline/screener)
#'   visit_numbers[is.na(visit_numbers)] <- 0
#'
#'   return(visit_numbers)
#' }
#'
#' #' Standardize incontinence response values
#' #' @noRd
#' standardize_incontinence_response <- function(response_values) {
#'
#'   dplyr::case_when(
#'     # Standard Yes responses
#'     response_values %in% c("(2) Yes", "2", 2, "Yes", "YES", "yes") ~ 1,
#'     # Standard No responses
#'     response_values %in% c("(1) No", "1", 1, "No", "NO", "no") ~ 0,
#'     # Extended formats from later visits
#'     response_values %in% c("(2) 2: Yes", "(2)2: Yes", "2: Yes") ~ 1,
#'     response_values %in% c("(1) 1: No", "(1)1: No", "1: No") ~ 0,
#'     TRUE ~ NA_real_
#'   )
#' }
#'
#' #' Helper function to identify participant ID column
#' #' @noRd
#' identify_participant_id_variable <- function(dataset) {
#'   participant_id_patterns <- c("^SWANID$", "^ARCHID$", "^swanid$", "^archid$")
#'   for (pattern in participant_id_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' Helper function to identify visit number column
#' #' @noRd
#' identify_visit_number_variable <- function(dataset) {
#'   visit_patterns <- c("^VISIT$", "^visit$", "^Visit$", "^VISITNO$")
#'   for (pattern in visit_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' Safe numeric conversion
#' #' @noRd
#' convert_to_numeric_safely <- function(variable_values) {
#'   suppressWarnings(as.numeric(as.character(variable_values)))
#' }
#'
#' #' Standardize race/ethnicity variable
#' #' @noRd
#' standardize_race_ethnicity_variable <- function(race_values) {
#'   dplyr::case_when(
#'     stringr::str_detect(as.character(race_values), "White|Caucasian|4") ~ "White",
#'     stringr::str_detect(as.character(race_values), "Black|African|1") ~ "Black",
#'     stringr::str_detect(as.character(race_values), "Hispanic|5") ~ "Hispanic",
#'     stringr::str_detect(as.character(race_values), "Chinese|2") ~ "Chinese",
#'     stringr::str_detect(as.character(race_values), "Japanese|3") ~ "Japanese",
#'     TRUE ~ "Other"
#'   )
#' }
#'
#' #' Standardize smoking status variable
#' #' @noRd
#' standardize_smoking_status_variable <- function(smoking_values) {
#'   dplyr::case_when(
#'     smoking_values %in% c(1, "(1) Never smoked", "Never") ~ "Never",
#'     smoking_values %in% c(2, "(2) Past smoker", "Past") ~ "Past",
#'     smoking_values %in% c(3, "(3) Current smoker", "Current") ~ "Current",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Standardize menopausal status variable
#' #' @noRd
#' standardize_menopausal_status_variable <- function(status_values) {
#'   dplyr::case_when(
#'     status_values %in% c(1, "(1) Post by BSO") ~ "Post BSO",
#'     status_values %in% c(2, "(2) Natural Post", "(2) Postmenopausal") ~ "Natural Post",
#'     status_values %in% c(3, "(3) Late Peri", "(3) Late perimenopausal") ~ "Late Peri",
#'     status_values %in% c(4, "(4) Early Peri", "(4) Early perimenopausal") ~ "Early Peri",
#'     status_values %in% c(5, "(5) Pre", "(5) Premenopausal") ~ "Pre",
#'     status_values %in% c(6, "(6) Pregnant/breastfeeding") ~ "Pregnant/Breastfeeding",
#'     status_values %in% c(7, "(7) Unknown due to HT use") ~ "Unknown HT",
#'     status_values %in% c(8, "(8) Unknown due to hysterectomy") ~ "Unknown Hysterectomy",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Standardize education variable
#' #' @noRd
#' standardize_education_variable <- function(education_values) {
#'   dplyr::case_when(
#'     education_values %in% c(1, "(1) Less than high school") ~ "Less than HS",
#'     education_values %in% c(2, "(2) High school") ~ "High School",
#'     education_values %in% c(3, "(3) Some college") ~ "Some College",
#'     education_values %in% c(4, "(4) College graduate") ~ "College Graduate",
#'     education_values %in% c(5, "(5) Graduate school") ~ "Graduate School",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Standardize marital status variable
#' #' @noRd
#' standardize_marital_status_variable <- function(marital_values) {
#'   dplyr::case_when(
#'     marital_values %in% c(1, "(1) Married", "Married") ~ "Married",
#'     marital_values %in% c(2, "(2) Single", "Single") ~ "Single",
#'     marital_values %in% c(3, "(3) Divorced/Separated", "Divorced") ~ "Divorced/Separated",
#'     marital_values %in% c(4, "(4) Widowed", "Widowed") ~ "Widowed",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Combine multiple baseline sources with type-safe merging
#' #' @noRd
#' combine_multiple_baseline_sources <- function(baseline_dataset_list, verbose) {
#'
#'   if (length(baseline_dataset_list) == 1) {
#'     return(baseline_dataset_list[[1]])
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Combining {length(baseline_dataset_list)} baseline sources")
#'   }
#'
#'   # Find the most complete dataset (most non-missing participant IDs)
#'   dataset_completeness_scores <- purrr::map_dbl(baseline_dataset_list, function(baseline_dataset) {
#'     sum(!is.na(baseline_dataset$swan_participant_id))
#'   })
#'
#'   primary_baseline_dataset <- baseline_dataset_list[[which.max(dataset_completeness_scores)]]
#'
#'   if (verbose) {
#'     logger::log_info("Primary baseline dataset: {primary_baseline_dataset$source_file[1]} with {max(dataset_completeness_scores)} participants")
#'   }
#'
#'   # Merge additional datasets
#'   for (dataset_index in seq_along(baseline_dataset_list)) {
#'     if (dataset_index == which.max(dataset_completeness_scores)) next
#'
#'     secondary_baseline_dataset <- baseline_dataset_list[[dataset_index]]
#'
#'     if (verbose) {
#'       logger::log_info("Merging secondary baseline: {secondary_baseline_dataset$source_file[1]}")
#'     }
#'
#'     # Harmonize data types before merging
#'     secondary_baseline_dataset <- harmonize_baseline_data_types(
#'       secondary_baseline_dataset,
#'       primary_baseline_dataset,
#'       verbose
#'     )
#'
#'     primary_baseline_dataset <- primary_baseline_dataset %>%
#'       dplyr::full_join(
#'         secondary_baseline_dataset,
#'         by = "swan_participant_id",
#'         suffix = c("", "_secondary")
#'       ) %>%
#'       dplyr::mutate(
#'         # Use primary data preferentially with type-safe coalesce
#'         baseline_age_years = apply_safe_numeric_coalesce(
#'           baseline_age_years,
#'           baseline_age_years_secondary
#'         ),
#'         baseline_bmi_kg_m2 = apply_safe_numeric_coalesce(
#'           baseline_bmi_kg_m2,
#'           baseline_bmi_kg_m2_secondary
#'         ),
#'         baseline_parity_count = apply_safe_numeric_coalesce(
#'           baseline_parity_count,
#'           baseline_parity_count_secondary
#'         ),
#'         baseline_race_ethnicity = apply_safe_character_coalesce(
#'           baseline_race_ethnicity,
#'           baseline_race_ethnicity_secondary
#'         ),
#'         baseline_smoking_status = apply_safe_character_coalesce(
#'           baseline_smoking_status,
#'           baseline_smoking_status_secondary
#'         ),
#'         baseline_menopausal_stage = apply_safe_character_coalesce(
#'           baseline_menopausal_stage,
#'           baseline_menopausal_stage_secondary
#'         ),
#'         baseline_education_level = apply_safe_character_coalesce(
#'           baseline_education_level,
#'           baseline_education_level_secondary
#'         ),
#'         baseline_marital_status = apply_safe_character_coalesce(
#'           baseline_marital_status,
#'           baseline_marital_status_secondary
#'         )
#'       ) %>%
#'       dplyr::select(-dplyr::ends_with("_secondary"))
#'   }
#'
#'   if (verbose) {
#'     final_participant_count <- sum(!is.na(primary_baseline_dataset$swan_participant_id))
#'     logger::log_info("Combined baseline dataset: {final_participant_count} total participants")
#'   }
#'
#'   return(primary_baseline_dataset)
#' }
#'
#' #' Harmonize data types between baseline datasets
#' #' @noRd
#' harmonize_baseline_data_types <- function(secondary_dataset, primary_dataset, verbose) {
#'
#'   baseline_column_names <- names(secondary_dataset)[stringr::str_starts(names(secondary_dataset), "baseline_")]
#'
#'   for (column_name in baseline_column_names) {
#'     if (column_name %in% names(primary_dataset)) {
#'
#'       primary_data_type <- class(primary_dataset[[column_name]])[1]
#'       secondary_data_type <- class(secondary_dataset[[column_name]])[1]
#'
#'       if (primary_data_type != secondary_data_type) {
#'         if (verbose) {
#'           logger::log_info("Harmonizing {column_name}: {secondary_data_type} -> {primary_data_type}")
#'         }
#'
#'         # Convert secondary to match primary type
#'         if (primary_data_type %in% c("numeric", "double")) {
#'           secondary_dataset[[column_name]] <- convert_to_numeric_safely(secondary_dataset[[column_name]])
#'         } else if (primary_data_type == "character") {
#'           secondary_dataset[[column_name]] <- as.character(secondary_dataset[[column_name]])
#'         } else if (primary_data_type == "logical") {
#'           secondary_dataset[[column_name]] <- as.logical(secondary_dataset[[column_name]])
#'         }
#'       }
#'     }
#'   }
#'
#'   return(secondary_dataset)
#' }
#'
#' #' Type-safe numeric coalesce
#' #' @noRd
#' apply_safe_numeric_coalesce <- function(primary_values, secondary_values) {
#'   # Convert both to numeric if needed
#'   primary_numeric <- if (is.numeric(primary_values)) primary_values else convert_to_numeric_safely(primary_values)
#'   secondary_numeric <- if (is.numeric(secondary_values)) secondary_values else convert_to_numeric_safely(secondary_values)
#'   dplyr::coalesce(primary_numeric, secondary_numeric)
#' }
#'
#' #' Type-safe character coalesce
#' #' @noRd
#' apply_safe_character_coalesce <- function(primary_values, secondary_values) {
#'   # Convert both to character if needed
#'   primary_character <- if (is.character(primary_values)) primary_values else as.character(primary_values)
#'   secondary_character <- if (is.character(secondary_values)) secondary_values else as.character(secondary_values)
#'   dplyr::coalesce(primary_character, secondary_character)
#' }
#'
#' #' Merge baseline demographics with longitudinal data
#' #' @noRd
#' merge_baseline_with_longitudinal_data <- function(baseline_data, longitudinal_data, verbose) {
#'
#'   if (verbose) {
#'     baseline_participant_count <- length(unique(baseline_data$swan_participant_id))
#'     longitudinal_observation_count <- nrow(longitudinal_data)
#'     logger::log_info("Merging baseline demographics with longitudinal measurements")
#'     logger::log_info("Baseline participants: {baseline_participant_count}")
#'     logger::log_info("Longitudinal observations: {longitudinal_observation_count}")
#'   }
#'
#'   # Merge datasets
#'   merged_comprehensive_data <- longitudinal_data %>%
#'     dplyr::left_join(
#'       baseline_data %>% dplyr::select(-source_file),
#'       by = "swan_participant_id"
#'     )
#'
#'   # Add derived time variables
#'   merged_comprehensive_data <- merged_comprehensive_data %>%
#'     dplyr::mutate(
#'       # Calculate current age at each visit
#'       current_age_years = baseline_age_years + years_since_baseline,
#'       # Add visit timing categories
#'       visit_timing_category = dplyr::case_when(
#'         visit_number == 0 ~ "Baseline",
#'         visit_number %in% 1:3 ~ "Early Follow-up",
#'         visit_number %in% 4:7 ~ "Mid Follow-up",
#'         visit_number >= 8 ~ "Late Follow-up",
#'         TRUE ~ "Unknown"
#'       )
#'     ) %>%
#'     dplyr::arrange(swan_participant_id, visit_number)
#'
#'   if (verbose) {
#'     merged_observation_count <- nrow(merged_comprehensive_data)
#'     participants_with_both_data <- length(unique(merged_comprehensive_data$swan_participant_id[!is.na(merged_comprehensive_data$baseline_age_years)]))
#'     logger::log_info("Merged comprehensive dataset: {merged_observation_count} observations")
#'     logger::log_info("Participants with both baseline + longitudinal data: {participants_with_both_data}")
#'   }
#'
#'   return(merged_comprehensive_data)
#' }
#'
#' #' Apply filtering for valid responses
#' #' @noRd
#' apply_response_filtering <- function(merged_data, verbose) {
#'
#'   if (verbose) {
#'     original_observation_count <- nrow(merged_data)
#'     logger::log_info("Applying response filtering to remove invalid/missing incontinence responses")
#'   }
#'
#'   filtered_data <- merged_data %>%
#'     dplyr::filter(
#'       !is.na(incontinence_status),
#'       !is.na(swan_participant_id),
#'       !is.na(visit_number)
#'     )
#'
#'   if (verbose) {
#'     filtered_observation_count <- nrow(filtered_data)
#'     observations_removed <- original_observation_count - filtered_observation_count
#'     logger::log_info("Filtering completed: {filtered_observation_count} observations retained")
#'     logger::log_info("Observations removed: {observations_removed}")
#'   }
#'
#'   return(filtered_data)
#' }
#'
#' #' Format comprehensive output dataset
#' #' @noRd
#' format_comprehensive_output <- function(merged_data, format, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Formatting comprehensive output as: {format}")
#'     all_column_names <- paste(names(merged_data), collapse = ", ")
#'     logger::log_info("Available columns: {all_column_names}")
#'   }
#'
#'   if (format == "long") {
#'     # Identify visit-specific columns
#'     visit_specific_column_names <- names(merged_data)[stringr::str_starts(names(merged_data), "visit_")]
#'
#'     if (verbose && length(visit_specific_column_names) > 0) {
#'       visit_vars_string <- paste(visit_specific_column_names, collapse = ", ")
#'       logger::log_info("Visit-specific columns preserved: {visit_vars_string}")
#'     } else if (verbose) {
#'       logger::log_warn("No visit-specific columns found in final dataset")
#'     }
#'
#'     # Build comprehensive column selection
#'     baseline_demographic_columns <- c(
#'       "swan_participant_id",
#'       "visit_number",
#'       "years_since_baseline",
#'       "current_age_years",
#'       "visit_timing_category",
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_parity_count",
#'       "baseline_menopausal_stage",
#'       "baseline_education_level",
#'       "baseline_marital_status"
#'     )
#'
#'     incontinence_measurement_columns <- c(
#'       "incontinence_status",
#'       "incontinence_raw_response",
#'       "incontinence_source_variable"
#'     )
#'
#'     # Combine all columns, keeping only those that exist
#'     all_desired_columns <- c(
#'       baseline_demographic_columns,
#'       incontinence_measurement_columns,
#'       visit_specific_column_names
#'     )
#'
#'     columns_that_exist_in_data <- intersect(all_desired_columns, names(merged_data))
#'
#'     if (verbose) {
#'       logger::log_info("Selecting {length(columns_that_exist_in_data)} columns for long format output")
#'     }
#'
#'     # Create final long format dataset
#'     final_comprehensive_dataset <- merged_data %>%
#'       dplyr::select(dplyr::all_of(columns_that_exist_in_data)) %>%
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         incontinence_source_variable,
#'         .keep_all = TRUE
#'       ) %>%
#'       dplyr::arrange(swan_participant_id, visit_number)
#'
#'   } else {
#'     # Wide format conversion
#'     visit_specific_column_names <- names(merged_data)[stringr::str_starts(names(merged_data), "visit_")]
#'
#'     baseline_columns_for_wide <- c(
#'       "swan_participant_id",
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_parity_count",
#'       "baseline_menopausal_stage",
#'       "baseline_education_level",
#'       "baseline_marital_status"
#'     )
#'
#'     measurement_columns_for_wide <- c(
#'       "visit_number",
#'       "incontinence_status",
#'       "current_age_years"
#'     )
#'
#'     all_columns_for_wide <- c(
#'       baseline_columns_for_wide,
#'       measurement_columns_for_wide,
#'       visit_specific_column_names
#'     )
#'
#'     columns_that_exist_for_wide <- intersect(all_columns_for_wide, names(merged_data))
#'
#'     if (verbose) {
#'       logger::log_info("Converting to wide format with {length(columns_that_exist_for_wide)} base columns")
#'     }
#'
#'     final_comprehensive_dataset <- merged_data %>%
#'       dplyr::select(dplyr::all_of(columns_that_exist_for_wide)) %>%
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         .keep_all = TRUE
#'       ) %>%
#'       tidyr::pivot_wider(
#'         names_from = visit_number,
#'         values_from = c(
#'           dplyr::any_of(visit_specific_column_names),
#'           incontinence_status,
#'           current_age_years
#'         ),
#'         names_sep = "_visit_"
#'       )
#'   }
#'
#'   if (verbose) {
#'     final_row_count <- nrow(final_comprehensive_dataset)
#'     final_column_count <- ncol(final_comprehensive_dataset)
#'     unique_participant_count <- length(unique(final_comprehensive_dataset$swan_participant_id))
#'
#'     logger::log_info("Final comprehensive dataset formatting completed")
#'     logger::log_info("Dimensions: {final_row_count} rows × {final_column_count} columns")
#'     logger::log_info("Unique participants: {unique_participant_count}")
#'
#'     if (format == "long") {
#'       final_visit_specific_count <- sum(stringr::str_starts(names(final_comprehensive_dataset), "visit_"))
#'       visit_range_min <- min(final_comprehensive_dataset$visit_number, na.rm = TRUE)
#'       visit_range_max <- max(final_comprehensive_dataset$visit_number, na.rm = TRUE)
#'       logger::log_info("Visit-specific variables in final dataset: {final_visit_specific_count}")
#'       logger::log_info("Visit range: {visit_range_min} to {visit_range_max}")
#'     }
#'   }
#'
#'   return(final_comprehensive_dataset)
#' }
#'
#' # Run -----
#' # Load required packages first
#' library(dplyr)
#' library(tidyr)
#' library(stringr)
#' library(purrr)
#' library(assertthat)
#' library(logger)
#' library(tibble)
#' library(readr)
#' library(glue)
#'
#' # Create comprehensive dataset
#' comprehensive_swan_dataset <- create_comprehensive_swan_dataset(
#'   swan_data_directory = swan_data_directory,
#'   output_format = "long",
#'   verbose = TRUE,
#'   include_visit_demographics = TRUE,
#'   filter_valid_responses = TRUE
#' )
#'
#'
#' # Function at 2029, Version 3 ----
#' #' Create Comprehensive SWAN Dataset with Visit-Specific Variables
#' #'
#' #' This function creates a comprehensive dataset from SWAN study files, extracting
#' #' baseline demographics and visit-specific variables including urinary incontinence
#' #' measures (INVOLEA/LEKINVO) and other longitudinal health indicators.
#' #'
#' #' @param swan_data_directory Character. Path to directory containing SWAN .rda files
#' #' @param output_format Character. Either "long" or "wide" format for final dataset
#' #' @param verbose Logical. Enable detailed logging output to console
#' #' @param include_visit_demographics Logical. Include visit-specific demographic
#' #'   variables (AGE, BMI, STATUS, etc.) in addition to baseline
#' #' @param filter_valid_responses Logical. Remove rows with missing INVOLEA responses
#' #'
#' #' @return tibble. Comprehensive SWAN dataset with baseline and visit-specific variables
#' #'
#' #' @examples
#' #' # Example 1: Basic long format with visit demographics
#' #' swan_long_comprehensive <- create_comprehensive_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "long",
#' #'   verbose = TRUE,
#' #'   include_visit_demographics = TRUE,
#' #'   filter_valid_responses = TRUE
#' #' )
#' #'
#' #' # Example 2: Wide format for analysis, no filtering
#' #' swan_wide_complete <- create_comprehensive_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "wide",
#' #'   verbose = FALSE,
#' #'   include_visit_demographics = TRUE,
#' #'   filter_valid_responses = FALSE
#' #' )
#' #'
#' #' # Example 3: Minimal dataset with only baseline + INVOLEA
#' #' swan_minimal <- create_comprehensive_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "long",
#' #'   verbose = TRUE,
#' #'   include_visit_demographics = FALSE,
#' #'   filter_valid_responses = TRUE
#' #' )
#' #'
#' #' @importFrom dplyr filter select mutate arrange group_by summarise
#' #'   left_join full_join distinct coalesce case_when all_of any_of ends_with
#' #' @importFrom tidyr pivot_longer pivot_wider
#' #' @importFrom stringr str_detect str_extract str_replace_all str_starts
#' #'   str_remove str_match
#' #' @importFrom purrr map_dfr map_chr map_lgl keep walk compact map
#' #' @importFrom assertthat assert_that
#' #' @importFrom logger log_info log_warn log_error
#' #' @importFrom tibble tibble as_tibble
#' #' @importFrom readr parse_number
#' #' @importFrom glue glue
#' #'
#' #' @export
#' create_comprehensive_swan_dataset <- function(swan_data_directory,
#'                                               output_format = "long",
#'                                               verbose = TRUE,
#'                                               include_visit_demographics = TRUE,
#'                                               filter_valid_responses = TRUE) {
#'
#'   # Input validation with assertthat
#'   if (verbose) {
#'     logger::log_info("Creating comprehensive SWAN dataset with visit-specific variables")
#'     logger::log_info("Function inputs - directory: {swan_data_directory}")
#'     logger::log_info("Function inputs - format: {output_format}, verbose: {verbose}")
#'     logger::log_info("Function inputs - include_visit_demographics: {include_visit_demographics}")
#'     logger::log_info("Function inputs - filter_valid_responses: {filter_valid_responses}")
#'   }
#'
#'   assertthat::assert_that(
#'     is.character(swan_data_directory),
#'     msg = "swan_data_directory must be a character string path"
#'   )
#'   assertthat::assert_that(
#'     dir.exists(swan_data_directory),
#'     msg = glue::glue("Directory does not exist: {swan_data_directory}")
#'   )
#'   assertthat::assert_that(
#'     output_format %in% c("long", "wide"),
#'     msg = "output_format must be 'long' or 'wide'"
#'   )
#'   assertthat::assert_that(
#'     is.logical(verbose),
#'     msg = "verbose must be TRUE or FALSE"
#'   )
#'   assertthat::assert_that(
#'     is.logical(include_visit_demographics),
#'     msg = "include_visit_demographics must be TRUE or FALSE"
#'   )
#'   assertthat::assert_that(
#'     is.logical(filter_valid_responses),
#'     msg = "filter_valid_responses must be TRUE or FALSE"
#'   )
#'
#'   # Step 1: Extract baseline demographics
#'   baseline_participant_demographics <- extract_baseline_participant_demographics(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Baseline demographics extracted: {nrow(baseline_participant_demographics)} participants")
#'   }
#'
#'   # Step 2: Extract comprehensive longitudinal data
#'   longitudinal_health_measurements <- extract_comprehensive_longitudinal_data(
#'     directory_path = swan_data_directory,
#'     include_visit_demographics = include_visit_demographics,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Longitudinal data extracted: {nrow(longitudinal_health_measurements)} observations")
#'   }
#'
#'   # Step 3: Merge datasets
#'   merged_comprehensive_dataset <- merge_baseline_with_longitudinal_data(
#'     baseline_data = baseline_participant_demographics,
#'     longitudinal_data = longitudinal_health_measurements,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Data merged: {nrow(merged_comprehensive_dataset)} total observations")
#'   }
#'
#'   # Step 4: Apply filtering if requested
#'   if (filter_valid_responses) {
#'     filtered_comprehensive_dataset <- apply_response_filtering(
#'       merged_data = merged_comprehensive_dataset,
#'       verbose = verbose
#'     )
#'   } else {
#'     filtered_comprehensive_dataset <- merged_comprehensive_dataset
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("After filtering: {nrow(filtered_comprehensive_dataset)} observations")
#'   }
#'
#'   # Step 5: Format final output
#'   final_comprehensive_dataset <- format_comprehensive_output(
#'     merged_data = filtered_comprehensive_dataset,
#'     format = output_format,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Comprehensive SWAN dataset creation completed")
#'     logger::log_info("Final dataset dimensions: {nrow(final_comprehensive_dataset)} rows × {ncol(final_comprehensive_dataset)} columns")
#'     logger::log_info("Output file path: Not saved to file (returned as object)")
#'   }
#'
#'   return(final_comprehensive_dataset)
#' }
#'
#' #' Extract baseline demographics from SWAN screener/baseline files
#' #' @noRd
#' extract_baseline_participant_demographics <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting baseline participant demographics from SWAN files")
#'   }
#'
#'   # Find baseline/screener files using known ICPSR numbers
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   # Known baseline file patterns from SWAN documentation
#'   baseline_file_identifiers <- c("28762", "04368", "baseline", "screener")
#'
#'   baseline_candidate_files <- purrr::keep(all_rda_files, function(file_path) {
#'     file_name <- tolower(basename(file_path))
#'     any(stringr::str_detect(file_name, baseline_file_identifiers))
#'   })
#'
#'   if (verbose) {
#'     logger::log_info("Found {length(baseline_candidate_files)} potential baseline files")
#'     purrr::walk(baseline_candidate_files, ~logger::log_info("  Candidate file: {basename(.x)}"))
#'   }
#'
#'   # Process each baseline file
#'   baseline_dataset_list <- purrr::map(baseline_candidate_files, function(file_path) {
#'     process_single_baseline_file(file_path, verbose)
#'   })
#'
#'   # Remove NULL results and combine
#'   baseline_dataset_list <- purrr::compact(baseline_dataset_list)
#'
#'   if (length(baseline_dataset_list) == 0) {
#'     logger::log_error("No valid baseline demographic data found in any files")
#'     stop("No baseline demographic data could be extracted")
#'   }
#'
#'   # Combine multiple baseline sources
#'   combined_baseline_demographics <- combine_multiple_baseline_sources(
#'     baseline_dataset_list,
#'     verbose
#'   )
#'
#'   return(combined_baseline_demographics)
#' }
#'
#' #' Process a single baseline file to extract demographics
#' #' @noRd
#' process_single_baseline_file <- function(file_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Processing baseline file: {basename(file_path)}")
#'   }
#'
#'   # Load the file safely
#'   file_environment <- new.env()
#'
#'   tryCatch({
#'     load(file_path, envir = file_environment)
#'   }, error = function(e) {
#'     if (verbose) {
#'       logger::log_error("Failed to load {basename(file_path)}: {e$message}")
#'     }
#'     return(NULL)
#'   })
#'
#'   if (length(ls(file_environment)) == 0) {
#'     if (verbose) {
#'       logger::log_warn("No datasets found in {basename(file_path)}")
#'     }
#'     return(NULL)
#'   }
#'
#'   raw_baseline_dataset <- file_environment[[ls(file_environment)[1]]]
#'
#'   # Identify demographic variables
#'   demographic_variable_mapping <- identify_baseline_demographic_variables(
#'     raw_baseline_dataset
#'   )
#'
#'   if (is.null(demographic_variable_mapping$participant_id)) {
#'     if (verbose) {
#'       logger::log_warn("No participant ID found in {basename(file_path)}")
#'     }
#'     return(NULL)
#'   }
#'
#'   # Extract and standardize demographics
#'   standardized_baseline_demographics <- extract_standardized_demographics(
#'     dataset = raw_baseline_dataset,
#'     variable_mapping = demographic_variable_mapping,
#'     source_file = basename(file_path),
#'     verbose = verbose
#'   )
#'
#'   return(standardized_baseline_demographics)
#' }
#'
#' #' Identify demographic variables in baseline dataset
#' #' @noRd
#' identify_baseline_demographic_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Participant ID patterns
#'   participant_id_patterns <- c("^SWANID$", "^ARCHID$", "^swanid$", "^archid$")
#'   participant_id_variable <- find_matching_variable(variable_names, participant_id_patterns)
#'
#'   # Demographic variable patterns (baseline variables often end with 0)
#'   age_patterns <- c("^AGE0?$", "^age0?$", "AGE.*0$")
#'   bmi_patterns <- c("^BMI0?$", "^bmi0?$", "BMI.*0$")
#'   race_patterns <- c("^RACE$", "^race$", "^ETHNIC")
#'   smoking_patterns <- c("^SMOKER0?$", "^SMOKING0?$", "^SMOKE")
#'   parity_patterns <- c("^NUMCHILD$", "^CHILDREN$", "^PARITY")
#'   menopausal_patterns <- c("^STATUS0?$", "^MENO")
#'   education_patterns <- c("^DEGREE$", "^EDUCATION", "^EDUC")
#'   marital_patterns <- c("^MARITAL", "^MARITALGP")
#'
#'   return(list(
#'     participant_id = participant_id_variable,
#'     age = find_matching_variable(variable_names, age_patterns),
#'     bmi = find_matching_variable(variable_names, bmi_patterns),
#'     race = find_matching_variable(variable_names, race_patterns),
#'     smoking = find_matching_variable(variable_names, smoking_patterns),
#'     parity = find_matching_variable(variable_names, parity_patterns),
#'     menopausal = find_matching_variable(variable_names, menopausal_patterns),
#'     education = find_matching_variable(variable_names, education_patterns),
#'     marital = find_matching_variable(variable_names, marital_patterns)
#'   ))
#' }
#'
#' #' Find first matching variable from patterns
#' #' @noRd
#' find_matching_variable <- function(variable_names, patterns) {
#'   for (pattern in patterns) {
#'     matches <- variable_names[stringr::str_detect(variable_names, pattern)]
#'     if (length(matches) > 0) {
#'       return(matches[1])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' Extract and standardize demographic variables
#' #' @noRd
#' extract_standardized_demographics <- function(dataset, variable_mapping,
#'                                               source_file, verbose) {
#'
#'   # Start with participant ID
#'   standardized_demographics <- tibble::tibble(
#'     swan_participant_id = as.character(dataset[[variable_mapping$participant_id]])
#'   )
#'
#'   # Add demographic variables with type-safe conversion
#'   if (!is.null(variable_mapping$age)) {
#'     standardized_demographics$baseline_age_years <-
#'       convert_to_numeric_safely(dataset[[variable_mapping$age]])
#'   }
#'
#'   if (!is.null(variable_mapping$bmi)) {
#'     standardized_demographics$baseline_bmi_kg_m2 <-
#'       convert_to_numeric_safely(dataset[[variable_mapping$bmi]])
#'   }
#'
#'   if (!is.null(variable_mapping$race)) {
#'     standardized_demographics$baseline_race_ethnicity <-
#'       standardize_race_ethnicity_variable(dataset[[variable_mapping$race]])
#'   }
#'
#'   if (!is.null(variable_mapping$smoking)) {
#'     standardized_demographics$baseline_smoking_status <-
#'       standardize_smoking_status_variable(dataset[[variable_mapping$smoking]])
#'   }
#'
#'   if (!is.null(variable_mapping$parity)) {
#'     standardized_demographics$baseline_parity_count <-
#'       convert_to_numeric_safely(dataset[[variable_mapping$parity]])
#'   }
#'
#'   if (!is.null(variable_mapping$menopausal)) {
#'     standardized_demographics$baseline_menopausal_stage <-
#'       standardize_menopausal_status_variable(dataset[[variable_mapping$menopausal]])
#'   }
#'
#'   if (!is.null(variable_mapping$education)) {
#'     standardized_demographics$baseline_education_level <-
#'       standardize_education_variable(dataset[[variable_mapping$education]])
#'   }
#'
#'   if (!is.null(variable_mapping$marital)) {
#'     standardized_demographics$baseline_marital_status <-
#'       standardize_marital_status_variable(dataset[[variable_mapping$marital]])
#'   }
#'
#'   standardized_demographics$source_file <- source_file
#'
#'   if (verbose) {
#'     valid_participant_count <- sum(!is.na(standardized_demographics$swan_participant_id))
#'     demographic_variable_count <- ncol(standardized_demographics) - 2  # exclude ID and source
#'     logger::log_info("  Extracted {valid_participant_count} participants with {demographic_variable_count} demographic variables")
#'   }
#'
#'   return(standardized_demographics)
#' }
#'
#' #' Extract comprehensive longitudinal data including visit-specific variables
#' #' @noRd
#' extract_comprehensive_longitudinal_data <- function(directory_path,
#'                                                     include_visit_demographics,
#'                                                     verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting comprehensive longitudinal data from all SWAN files")
#'   }
#'
#'   # Find all RDA files
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   # Process each file for longitudinal variables
#'   longitudinal_dataset_list <- purrr::map(all_rda_files, function(file_path) {
#'     process_single_longitudinal_file(
#'       file_path = file_path,
#'       include_visit_demographics = include_visit_demographics,
#'       verbose = verbose
#'     )
#'   })
#'
#'   # Remove NULL results
#'   longitudinal_dataset_list <- purrr::compact(longitudinal_dataset_list)
#'
#'   if (length(longitudinal_dataset_list) == 0) {
#'     logger::log_error("No longitudinal data found in any files")
#'     stop("No longitudinal data could be extracted")
#'   }
#'
#'   # Combine all longitudinal datasets
#'   combined_longitudinal_data <- purrr::map_dfr(longitudinal_dataset_list, ~.x)
#'
#'   # Remove duplicate observations (same participant, visit, variable)
#'   deduped_longitudinal_data <- combined_longitudinal_data %>%
#'     dplyr::distinct(
#'       swan_participant_id,
#'       visit_number,
#'       incontinence_source_variable,
#'       .keep_all = TRUE
#'     )
#'
#'   if (verbose) {
#'     unique_participant_count <- length(unique(deduped_longitudinal_data$swan_participant_id))
#'     visit_range <- paste(
#'       min(deduped_longitudinal_data$visit_number, na.rm = TRUE), "to",
#'       max(deduped_longitudinal_data$visit_number, na.rm = TRUE)
#'     )
#'     logger::log_info("Comprehensive longitudinal data extracted: {nrow(deduped_longitudinal_data)} observations")
#'     logger::log_info("Visit range: {visit_range}")
#'     logger::log_info("Unique participants with longitudinal data: {unique_participant_count}")
#'   }
#'
#'   return(deduped_longitudinal_data)
#' }
#'
#' #' Process single file for longitudinal variables
#' #' @noRd
#' process_single_longitudinal_file <- function(file_path, include_visit_demographics, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Scanning {basename(file_path)} for longitudinal variables")
#'   }
#'
#'   # Load file safely
#'   file_environment <- new.env()
#'
#'   tryCatch({
#'     load(file_path, envir = file_environment)
#'   }, error = function(e) {
#'     if (verbose) {
#'       logger::log_warn("Failed to load {basename(file_path)}: {e$message}")
#'     }
#'     return(NULL)
#'   })
#'
#'   if (length(ls(file_environment)) == 0) return(NULL)
#'
#'   raw_longitudinal_dataset <- file_environment[[ls(file_environment)[1]]]
#'
#'   # Look for incontinence variables (INVOLEA/LEKINVO)
#'   incontinence_variable_names <- identify_incontinence_variables(raw_longitudinal_dataset)
#'
#'   if (length(incontinence_variable_names) == 0) {
#'     return(NULL)  # No incontinence variables in this file
#'   }
#'
#'   if (verbose) {
#'     incontinence_vars_string <- paste(incontinence_variable_names, collapse = ", ")
#'     logger::log_info("  Found {length(incontinence_variable_names)} incontinence variables: {incontinence_vars_string}")
#'   }
#'
#'   # Extract longitudinal data
#'   extracted_longitudinal_data <- extract_longitudinal_measurements(
#'     dataset = raw_longitudinal_dataset,
#'     incontinence_variables = incontinence_variable_names,
#'     include_visit_demographics = include_visit_demographics,
#'     source_file = basename(file_path),
#'     verbose = verbose
#'   )
#'
#'   return(extracted_longitudinal_data)
#' }
#'
#' #' Identify incontinence variables (INVOLEA/LEKINVO) in dataset
#' #' @noRd
#' identify_incontinence_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Look for INVOLEA and LEKINVO patterns
#'   incontinence_patterns <- c("INVOLEA\\d*", "LEKINVO\\d*", "involea\\d*", "lekinvo\\d*")
#'
#'   incontinence_variables <- c()
#'   for (pattern in incontinence_patterns) {
#'     matching_variables <- variable_names[stringr::str_detect(variable_names, pattern)]
#'     incontinence_variables <- c(incontinence_variables, matching_variables)
#'   }
#'
#'   return(unique(incontinence_variables))
#' }
#'
#' #' Extract longitudinal measurements including visit-specific demographics
#' #' @noRd
#' extract_longitudinal_measurements <- function(dataset, incontinence_variables,
#'                                               include_visit_demographics,
#'                                               source_file, verbose) {
#'
#'   # Identify core variables
#'   participant_id_variable <- identify_participant_id_variable(dataset)
#'   visit_number_variable <- identify_visit_number_variable(dataset)
#'
#'   if (is.null(participant_id_variable)) {
#'     if (verbose) {
#'       logger::log_warn("No participant ID found in {source_file}")
#'     }
#'     return(NULL)
#'   }
#'
#'   # Start with core columns
#'   core_column_names <- c(participant_id_variable)
#'   if (!is.null(visit_number_variable)) {
#'     core_column_names <- c(core_column_names, visit_number_variable)
#'   }
#'
#'   # Identify visit-specific demographic variables if requested
#'   visit_specific_variables <- list()
#'
#'   if (include_visit_demographics) {
#'     visit_specific_variables <- identify_visit_specific_demographic_variables(dataset)
#'
#'     if (verbose && length(visit_specific_variables) > 0) {
#'       visit_vars_string <- paste(names(visit_specific_variables), collapse = ", ")
#'       logger::log_info("  Visit-specific demographics found: {visit_vars_string}")
#'     }
#'   }
#'
#'   # Combine all variables to extract
#'   all_variables_to_extract <- c(
#'     core_column_names,
#'     unlist(visit_specific_variables),
#'     incontinence_variables
#'   )
#'
#'   # Extract data
#'   longitudinal_measurements <- tibble::as_tibble(dataset) %>%
#'     dplyr::select(dplyr::all_of(all_variables_to_extract))
#'
#'   # Standardize core column names
#'   names(longitudinal_measurements)[1] <- "swan_participant_id"
#'   if (!is.null(visit_number_variable)) {
#'     names(longitudinal_measurements)[2] <- "visit_number_from_file"
#'   } else {
#'     longitudinal_measurements$visit_number_from_file <- NA_real_
#'   }
#'
#'   # Rename visit-specific variables with standardized prefixes
#'   for (variable_type in names(visit_specific_variables)) {
#'     original_variable_name <- visit_specific_variables[[variable_type]]
#'     if (original_variable_name %in% names(longitudinal_measurements)) {
#'       standardized_variable_name <- paste0("visit_", variable_type)
#'       names(longitudinal_measurements)[names(longitudinal_measurements) == original_variable_name] <- standardized_variable_name
#'     }
#'   }
#'
#'   # Convert to long format for incontinence variables
#'   incontinence_long_format <- longitudinal_measurements %>%
#'     tidyr::pivot_longer(
#'       cols = dplyr::all_of(incontinence_variables),
#'       names_to = "incontinence_source_variable",
#'       values_to = "incontinence_raw_response"
#'     ) %>%
#'     dplyr::mutate(
#'       swan_participant_id = as.character(swan_participant_id),
#'       # Extract visit number from variable name (primary method for SWAN)
#'       visit_number = extract_visit_number_from_variable(incontinence_source_variable),
#'       incontinence_status = standardize_incontinence_response(incontinence_raw_response),
#'       years_since_baseline = visit_number,
#'       source_file = source_file
#'     ) %>%
#'     dplyr::select(-visit_number_from_file) %>%
#'     dplyr::filter(!is.na(incontinence_raw_response))  # Remove missing responses
#'
#'   # Log results
#'   if (verbose && nrow(incontinence_long_format) > 0) {
#'     unique_visits <- sort(unique(incontinence_long_format$visit_number))
#'     visits_string <- paste(unique_visits, collapse = ", ")
#'     logger::log_info("  Extracted visits for {source_file}: {visits_string}")
#'     logger::log_info("  Total observations in {source_file}: {nrow(incontinence_long_format)}")
#'
#'     # Log visit-specific variables preserved
#'     visit_specific_columns <- names(incontinence_long_format)[stringr::str_starts(names(incontinence_long_format), "visit_")]
#'     if (length(visit_specific_columns) > 0) {
#'       visit_vars_preserved <- paste(stringr::str_remove(visit_specific_columns, "^visit_"), collapse = ", ")
#'       logger::log_info("  Visit-specific variables preserved: {visit_vars_preserved}")
#'     }
#'   }
#'
#'   return(incontinence_long_format)
#' }
#'
#' #' Identify visit-specific demographic variables in dataset
#' #' @noRd
#' identify_visit_specific_demographic_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'   visit_specific_vars <- list()
#'
#'   # AGE variables (AGE0, AGE1, AGE2, etc.)
#'   age_variables <- variable_names[stringr::str_detect(variable_names, "^AGE\\d+$")]
#'   if (length(age_variables) > 0) {
#'     visit_specific_vars[["age_years"]] <- age_variables[1]
#'   }
#'
#'   # BMI variables (BMI0, BMI1, BMI2, etc.)
#'   bmi_variables <- variable_names[stringr::str_detect(variable_names, "^BMI\\d+$")]
#'   if (length(bmi_variables) > 0) {
#'     visit_specific_vars[["bmi_kg_m2"]] <- bmi_variables[1]
#'   }
#'
#'   # Weight variables (WEIGHT0, WEIGHT1, etc.)
#'   weight_variables <- variable_names[stringr::str_detect(variable_names, "^WEIGHT\\d+$")]
#'   if (length(weight_variables) > 0) {
#'     visit_specific_vars[["weight_kg"]] <- weight_variables[1]
#'   }
#'
#'   # Height variables (HEIGHT0, HEIGHT1, etc.)
#'   height_variables <- variable_names[stringr::str_detect(variable_names, "^HEIGHT\\d+$")]
#'   if (length(height_variables) > 0) {
#'     visit_specific_vars[["height_cm"]] <- height_variables[1]
#'   }
#'
#'   # Smoking variables (SMOKER0, SMOKER1, etc.)
#'   smoking_variables <- variable_names[stringr::str_detect(variable_names, "^SMOKER?E?\\d+$")]
#'   if (length(smoking_variables) > 0) {
#'     visit_specific_vars[["smoking_status"]] <- smoking_variables[1]
#'   }
#'
#'   # Menopausal status variables (STATUS0, STATUS1, etc.)
#'   menopause_variables <- variable_names[stringr::str_detect(variable_names, "^STATUS\\d+$")]
#'   if (length(menopause_variables) > 0) {
#'     visit_specific_vars[["menopausal_stage"]] <- menopause_variables[1]
#'   }
#'
#'   # Marital status variables (MARITALGP0, MARITALGP1, etc.)
#'   marital_variables <- variable_names[stringr::str_detect(variable_names, "^MARITALGP\\d+$")]
#'   if (length(marital_variables) > 0) {
#'     visit_specific_vars[["marital_status"]] <- marital_variables[1]
#'   }
#'
#'   # Parity variables (NUMCHILD0, NUMCHILD1, etc.)
#'   parity_variables <- variable_names[stringr::str_detect(variable_names, "^NUMCHILD\\d+$")]
#'   if (length(parity_variables) > 0) {
#'     visit_specific_vars[["parity_count"]] <- parity_variables[1]
#'   }
#'
#'   return(visit_specific_vars)
#' }
#'
#' #' Extract visit number from variable name (e.g., INVOLEA9 -> 9)
#' #' @noRd
#' extract_visit_number_from_variable <- function(variable_names) {
#'
#'   # Extract trailing digits from variable names
#'   visit_numbers <- stringr::str_extract(variable_names, "\\d+$")
#'   visit_numbers <- as.numeric(visit_numbers)
#'
#'   # Replace NAs with 0 (baseline/screener)
#'   visit_numbers[is.na(visit_numbers)] <- 0
#'
#'   return(visit_numbers)
#' }
#'
#' #' Standardize incontinence response values
#' #' @noRd
#' standardize_incontinence_response <- function(response_values) {
#'
#'   dplyr::case_when(
#'     # Standard Yes responses
#'     response_values %in% c("(2) Yes", "2", 2, "Yes", "YES", "yes") ~ 1,
#'     # Standard No responses
#'     response_values %in% c("(1) No", "1", 1, "No", "NO", "no") ~ 0,
#'     # Extended formats from later visits
#'     response_values %in% c("(2) 2: Yes", "(2)2: Yes", "2: Yes") ~ 1,
#'     response_values %in% c("(1) 1: No", "(1)1: No", "1: No") ~ 0,
#'     TRUE ~ NA_real_
#'   )
#' }
#'
#' #' Helper function to identify participant ID column
#' #' @noRd
#' identify_participant_id_variable <- function(dataset) {
#'   participant_id_patterns <- c("^SWANID$", "^ARCHID$", "^swanid$", "^archid$")
#'   for (pattern in participant_id_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' Helper function to identify visit number column
#' #' @noRd
#' identify_visit_number_variable <- function(dataset) {
#'   visit_patterns <- c("^VISIT$", "^visit$", "^Visit$", "^VISITNO$")
#'   for (pattern in visit_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' Safe numeric conversion
#' #' @noRd
#' convert_to_numeric_safely <- function(variable_values) {
#'   suppressWarnings(as.numeric(as.character(variable_values)))
#' }
#'
#' #' Standardize race/ethnicity variable
#' #' @noRd
#' standardize_race_ethnicity_variable <- function(race_values) {
#'   dplyr::case_when(
#'     stringr::str_detect(as.character(race_values), "White|Caucasian|4") ~ "White",
#'     stringr::str_detect(as.character(race_values), "Black|African|1") ~ "Black",
#'     stringr::str_detect(as.character(race_values), "Hispanic|5") ~ "Hispanic",
#'     stringr::str_detect(as.character(race_values), "Chinese|2") ~ "Chinese",
#'     stringr::str_detect(as.character(race_values), "Japanese|3") ~ "Japanese",
#'     TRUE ~ "Other"
#'   )
#' }
#'
#' #' Standardize smoking status variable
#' #' @noRd
#' standardize_smoking_status_variable <- function(smoking_values) {
#'   dplyr::case_when(
#'     smoking_values %in% c(1, "(1) Never smoked", "Never") ~ "Never",
#'     smoking_values %in% c(2, "(2) Past smoker", "Past") ~ "Past",
#'     smoking_values %in% c(3, "(3) Current smoker", "Current") ~ "Current",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Standardize menopausal status variable
#' #' @noRd
#' standardize_menopausal_status_variable <- function(status_values) {
#'   dplyr::case_when(
#'     status_values %in% c(1, "(1) Post by BSO") ~ "Post BSO",
#'     status_values %in% c(2, "(2) Natural Post", "(2) Postmenopausal") ~ "Natural Post",
#'     status_values %in% c(3, "(3) Late Peri", "(3) Late perimenopausal") ~ "Late Peri",
#'     status_values %in% c(4, "(4) Early Peri", "(4) Early perimenopausal") ~ "Early Peri",
#'     status_values %in% c(5, "(5) Pre", "(5) Premenopausal") ~ "Pre",
#'     status_values %in% c(6, "(6) Pregnant/breastfeeding") ~ "Pregnant/Breastfeeding",
#'     status_values %in% c(7, "(7) Unknown due to HT use") ~ "Unknown HT",
#'     status_values %in% c(8, "(8) Unknown due to hysterectomy") ~ "Unknown Hysterectomy",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Standardize education variable
#' #' @noRd
#' standardize_education_variable <- function(education_values) {
#'   dplyr::case_when(
#'     education_values %in% c(1, "(1) Less than high school") ~ "Less than HS",
#'     education_values %in% c(2, "(2) High school") ~ "High School",
#'     education_values %in% c(3, "(3) Some college") ~ "Some College",
#'     education_values %in% c(4, "(4) College graduate") ~ "College Graduate",
#'     education_values %in% c(5, "(5) Graduate school") ~ "Graduate School",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Standardize marital status variable
#' #' @noRd
#' standardize_marital_status_variable <- function(marital_values) {
#'   dplyr::case_when(
#'     marital_values %in% c(1, "(1) Married", "Married") ~ "Married",
#'     marital_values %in% c(2, "(2) Single", "Single") ~ "Single",
#'     marital_values %in% c(3, "(3) Divorced/Separated", "Divorced") ~ "Divorced/Separated",
#'     marital_values %in% c(4, "(4) Widowed", "Widowed") ~ "Widowed",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Combine multiple baseline sources with type-safe merging
#' #' @noRd
#' combine_multiple_baseline_sources <- function(baseline_dataset_list, verbose) {
#'
#'   if (length(baseline_dataset_list) == 1) {
#'     return(baseline_dataset_list[[1]])
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Combining {length(baseline_dataset_list)} baseline sources")
#'   }
#'
#'   # Find the most complete dataset (most non-missing participant IDs)
#'   dataset_completeness_scores <- purrr::map_dbl(baseline_dataset_list, function(baseline_dataset) {
#'     sum(!is.na(baseline_dataset$swan_participant_id))
#'   })
#'
#'   primary_baseline_dataset <- baseline_dataset_list[[which.max(dataset_completeness_scores)]]
#'
#'   if (verbose) {
#'     logger::log_info("Primary baseline dataset: {primary_baseline_dataset$source_file[1]} with {max(dataset_completeness_scores)} participants")
#'   }
#'
#'   # Merge additional datasets
#'   for (dataset_index in seq_along(baseline_dataset_list)) {
#'     if (dataset_index == which.max(dataset_completeness_scores)) next
#'
#'     secondary_baseline_dataset <- baseline_dataset_list[[dataset_index]]
#'
#'     if (verbose) {
#'       logger::log_info("Merging secondary baseline: {secondary_baseline_dataset$source_file[1]}")
#'     }
#'
#'     # Harmonize data types before merging
#'     secondary_baseline_dataset <- harmonize_baseline_data_types(
#'       secondary_baseline_dataset,
#'       primary_baseline_dataset,
#'       verbose
#'     )
#'
#'     primary_baseline_dataset <- primary_baseline_dataset %>%
#'       dplyr::full_join(
#'         secondary_baseline_dataset,
#'         by = "swan_participant_id",
#'         suffix = c("", "_secondary")
#'       )
#'
#'     # Smart coalescing - only coalesce variables that exist in both datasets
#'     baseline_variables_to_merge <- c(
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_parity_count",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_menopausal_stage",
#'       "baseline_education_level",
#'       "baseline_marital_status"
#'     )
#'
#'     # Check which variables actually exist with _secondary suffix
#'     available_columns <- names(primary_baseline_dataset)
#'
#'     for (baseline_variable in baseline_variables_to_merge) {
#'       secondary_variable_name <- paste0(baseline_variable, "_secondary")
#'
#'       if (baseline_variable %in% available_columns && secondary_variable_name %in% available_columns) {
#'         if (verbose) {
#'           logger::log_info("  Coalescing {baseline_variable}")
#'         }
#'
#'         # Apply appropriate coalescing based on data type
#'         if (stringr::str_detect(baseline_variable, "age|bmi|parity|count")) {
#'           # Numeric variables
#'           primary_baseline_dataset[[baseline_variable]] <- apply_safe_numeric_coalesce(
#'             primary_baseline_dataset[[baseline_variable]],
#'             primary_baseline_dataset[[secondary_variable_name]]
#'           )
#'         } else {
#'           # Character variables
#'           primary_baseline_dataset[[baseline_variable]] <- apply_safe_character_coalesce(
#'             primary_baseline_dataset[[baseline_variable]],
#'             primary_baseline_dataset[[secondary_variable_name]]
#'           )
#'         }
#'       } else if (verbose) {
#'         logger::log_info("  Skipping {baseline_variable} - not available in secondary dataset")
#'       }
#'     }
#'
#'     # Remove all secondary columns
#'     primary_baseline_dataset <- primary_baseline_dataset %>%
#'       dplyr::select(-dplyr::ends_with("_secondary"))
#'   }
#'
#'   if (verbose) {
#'     final_participant_count <- sum(!is.na(primary_baseline_dataset$swan_participant_id))
#'     logger::log_info("Combined baseline dataset: {final_participant_count} total participants")
#'   }
#'
#'   return(primary_baseline_dataset)
#' }
#'
#' #' Harmonize data types between baseline datasets
#' #' @noRd
#' harmonize_baseline_data_types <- function(secondary_dataset, primary_dataset, verbose) {
#'
#'   baseline_column_names <- names(secondary_dataset)[stringr::str_starts(names(secondary_dataset), "baseline_")]
#'
#'   for (column_name in baseline_column_names) {
#'     if (column_name %in% names(primary_dataset)) {
#'
#'       primary_data_type <- class(primary_dataset[[column_name]])[1]
#'       secondary_data_type <- class(secondary_dataset[[column_name]])[1]
#'
#'       if (primary_data_type != secondary_data_type) {
#'         if (verbose) {
#'           logger::log_info("Harmonizing {column_name}: {secondary_data_type} -> {primary_data_type}")
#'         }
#'
#'         # Convert secondary to match primary type
#'         if (primary_data_type %in% c("numeric", "double")) {
#'           secondary_dataset[[column_name]] <- convert_to_numeric_safely(secondary_dataset[[column_name]])
#'         } else if (primary_data_type == "character") {
#'           secondary_dataset[[column_name]] <- as.character(secondary_dataset[[column_name]])
#'         } else if (primary_data_type == "logical") {
#'           secondary_dataset[[column_name]] <- as.logical(secondary_dataset[[column_name]])
#'         }
#'       }
#'     }
#'   }
#'
#'   return(secondary_dataset)
#' }
#'
#' #' Type-safe numeric coalesce
#' #' @noRd
#' apply_safe_numeric_coalesce <- function(primary_values, secondary_values) {
#'   # Convert both to numeric if needed
#'   primary_numeric <- if (is.numeric(primary_values)) primary_values else convert_to_numeric_safely(primary_values)
#'   secondary_numeric <- if (is.numeric(secondary_values)) secondary_values else convert_to_numeric_safely(secondary_values)
#'   dplyr::coalesce(primary_numeric, secondary_numeric)
#' }
#'
#' #' Type-safe character coalesce
#' #' @noRd
#' apply_safe_character_coalesce <- function(primary_values, secondary_values) {
#'   # Convert both to character if needed
#'   primary_character <- if (is.character(primary_values)) primary_values else as.character(primary_values)
#'   secondary_character <- if (is.character(secondary_values)) secondary_values else as.character(secondary_values)
#'   dplyr::coalesce(primary_character, secondary_character)
#' }
#'
#' #' Merge baseline demographics with longitudinal data
#' #' @noRd
#' merge_baseline_with_longitudinal_data <- function(baseline_data, longitudinal_data, verbose) {
#'
#'   if (verbose) {
#'     baseline_participant_count <- length(unique(baseline_data$swan_participant_id))
#'     longitudinal_observation_count <- nrow(longitudinal_data)
#'     logger::log_info("Merging baseline demographics with longitudinal measurements")
#'     logger::log_info("Baseline participants: {baseline_participant_count}")
#'     logger::log_info("Longitudinal observations: {longitudinal_observation_count}")
#'   }
#'
#'   # Merge datasets
#'   merged_comprehensive_data <- longitudinal_data %>%
#'     dplyr::left_join(
#'       baseline_data %>% dplyr::select(-source_file),
#'       by = "swan_participant_id"
#'     )
#'
#'   # Add derived time variables
#'   merged_comprehensive_data <- merged_comprehensive_data %>%
#'     dplyr::mutate(
#'       # Calculate current age at each visit
#'       current_age_years = baseline_age_years + years_since_baseline,
#'       # Add visit timing categories
#'       visit_timing_category = dplyr::case_when(
#'         visit_number == 0 ~ "Baseline",
#'         visit_number %in% 1:3 ~ "Early Follow-up",
#'         visit_number %in% 4:7 ~ "Mid Follow-up",
#'         visit_number >= 8 ~ "Late Follow-up",
#'         TRUE ~ "Unknown"
#'       )
#'     ) %>%
#'     dplyr::arrange(swan_participant_id, visit_number)
#'
#'   if (verbose) {
#'     merged_observation_count <- nrow(merged_comprehensive_data)
#'     participants_with_both_data <- length(unique(merged_comprehensive_data$swan_participant_id[!is.na(merged_comprehensive_data$baseline_age_years)]))
#'     logger::log_info("Merged comprehensive dataset: {merged_observation_count} observations")
#'     logger::log_info("Participants with both baseline + longitudinal data: {participants_with_both_data}")
#'   }
#'
#'   return(merged_comprehensive_data)
#' }
#'
#' #' Apply filtering for valid responses
#' #' @noRd
#' apply_response_filtering <- function(merged_data, verbose) {
#'
#'   if (verbose) {
#'     original_observation_count <- nrow(merged_data)
#'     logger::log_info("Applying response filtering to remove invalid/missing incontinence responses")
#'   }
#'
#'   filtered_data <- merged_data %>%
#'     dplyr::filter(
#'       !is.na(incontinence_status),
#'       !is.na(swan_participant_id),
#'       !is.na(visit_number)
#'     )
#'
#'   if (verbose) {
#'     filtered_observation_count <- nrow(filtered_data)
#'     observations_removed <- original_observation_count - filtered_observation_count
#'     logger::log_info("Filtering completed: {filtered_observation_count} observations retained")
#'     logger::log_info("Observations removed: {observations_removed}")
#'   }
#'
#'   return(filtered_data)
#' }
#'
#' #' Format comprehensive output dataset
#' #' @noRd
#' format_comprehensive_output <- function(merged_data, format, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Formatting comprehensive output as: {format}")
#'     all_column_names <- paste(names(merged_data), collapse = ", ")
#'     logger::log_info("Available columns: {all_column_names}")
#'   }
#'
#'   if (format == "long") {
#'     # Identify visit-specific columns
#'     visit_specific_column_names <- names(merged_data)[stringr::str_starts(names(merged_data), "visit_")]
#'
#'     if (verbose && length(visit_specific_column_names) > 0) {
#'       visit_vars_string <- paste(visit_specific_column_names, collapse = ", ")
#'       logger::log_info("Visit-specific columns preserved: {visit_vars_string}")
#'     } else if (verbose) {
#'       logger::log_warn("No visit-specific columns found in final dataset")
#'     }
#'
#'     # Build comprehensive column selection
#'     baseline_demographic_columns <- c(
#'       "swan_participant_id",
#'       "visit_number",
#'       "years_since_baseline",
#'       "current_age_years",
#'       "visit_timing_category",
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_parity_count",
#'       "baseline_menopausal_stage",
#'       "baseline_education_level",
#'       "baseline_marital_status"
#'     )
#'
#'     incontinence_measurement_columns <- c(
#'       "incontinence_status",
#'       "incontinence_raw_response",
#'       "incontinence_source_variable"
#'     )
#'
#'     # Combine all columns, keeping only those that exist
#'     all_desired_columns <- c(
#'       baseline_demographic_columns,
#'       incontinence_measurement_columns,
#'       visit_specific_column_names
#'     )
#'
#'     columns_that_exist_in_data <- intersect(all_desired_columns, names(merged_data))
#'
#'     if (verbose) {
#'       logger::log_info("Selecting {length(columns_that_exist_in_data)} columns for long format output")
#'     }
#'
#'     # Create final long format dataset
#'     final_comprehensive_dataset <- merged_data %>%
#'       dplyr::select(dplyr::all_of(columns_that_exist_in_data)) %>%
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         incontinence_source_variable,
#'         .keep_all = TRUE
#'       ) %>%
#'       dplyr::arrange(swan_participant_id, visit_number)
#'
#'   } else {
#'     # Wide format conversion
#'     visit_specific_column_names <- names(merged_data)[stringr::str_starts(names(merged_data), "visit_")]
#'
#'     baseline_columns_for_wide <- c(
#'       "swan_participant_id",
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_parity_count",
#'       "baseline_menopausal_stage",
#'       "baseline_education_level",
#'       "baseline_marital_status"
#'     )
#'
#'     measurement_columns_for_wide <- c(
#'       "visit_number",
#'       "incontinence_status",
#'       "current_age_years"
#'     )
#'
#'     all_columns_for_wide <- c(
#'       baseline_columns_for_wide,
#'       measurement_columns_for_wide,
#'       visit_specific_column_names
#'     )
#'
#'     columns_that_exist_for_wide <- intersect(all_columns_for_wide, names(merged_data))
#'
#'     if (verbose) {
#'       logger::log_info("Converting to wide format with {length(columns_that_exist_for_wide)} base columns")
#'     }
#'
#'     final_comprehensive_dataset <- merged_data %>%
#'       dplyr::select(dplyr::all_of(columns_that_exist_for_wide)) %>%
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         .keep_all = TRUE
#'       ) %>%
#'       tidyr::pivot_wider(
#'         names_from = visit_number,
#'         values_from = c(
#'           dplyr::any_of(visit_specific_column_names),
#'           incontinence_status,
#'           current_age_years
#'         ),
#'         names_sep = "_visit_"
#'       )
#'   }
#'
#'   if (verbose) {
#'     final_row_count <- nrow(final_comprehensive_dataset)
#'     final_column_count <- ncol(final_comprehensive_dataset)
#'     unique_participant_count <- length(unique(final_comprehensive_dataset$swan_participant_id))
#'
#'     logger::log_info("Final comprehensive dataset formatting completed")
#'     logger::log_info("Dimensions: {final_row_count} rows × {final_column_count} columns")
#'     logger::log_info("Unique participants: {unique_participant_count}")
#'
#'     if (format == "long") {
#'       final_visit_specific_count <- sum(stringr::str_starts(names(final_comprehensive_dataset), "visit_"))
#'       visit_range_min <- min(final_comprehensive_dataset$visit_number, na.rm = TRUE)
#'       visit_range_max <- max(final_comprehensive_dataset$visit_number, na.rm = TRUE)
#'       logger::log_info("Visit-specific variables in final dataset: {final_visit_specific_count}")
#'       logger::log_info("Visit range: {visit_range_min} to {visit_range_max}")
#'     }
#'   }
#'
#'   return(final_comprehensive_dataset)
#' }
#'
#' comprehensive_swan_dataset <- create_comprehensive_swan_dataset(
#'   swan_data_directory = swan_data_directory,
#'   output_format = "long",
#'   verbose = TRUE,
#'   include_visit_demographics = TRUE,
#'   filter_valid_responses = TRUE
#' )
#'
#'
#' # V5 Create Comprehensive SWAN Dataset with Visit-Specific Variables -----
#' #' Create Comprehensive SWAN Dataset with Visit-Specific Variables
#' #'
#' #' This function creates a comprehensive dataset from SWAN study files, extracting
#' #' baseline demographics and visit-specific variables including urinary incontinence
#' #' measures (INVOLEA/LEKINVO) and other longitudinal health indicators.
#' #'
#' #' @param swan_data_directory Character. Path to directory containing SWAN .rda files
#' #' @param output_format Character. Either "long" or "wide" format for final dataset
#' #' @param verbose Logical. Enable detailed logging output to console
#' #' @param include_visit_demographics Logical. Include visit-specific demographic
#' #'   variables (AGE, BMI, STATUS, etc.) in addition to baseline
#' #' @param filter_valid_responses Logical. Remove rows with missing INVOLEA responses
#' #'
#' #' @return tibble. Comprehensive SWAN dataset with baseline and visit-specific variables
#' #'
#' #' @examples
#' #' # Example 1: Basic long format with visit demographics
#' #' swan_long_comprehensive <- create_comprehensive_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "long",
#' #'   verbose = TRUE,
#' #'   include_visit_demographics = TRUE,
#' #'   filter_valid_responses = TRUE
#' #' )
#' #'
#' #' # Example 2: Wide format for analysis, no filtering
#' #' swan_wide_complete <- create_comprehensive_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "wide",
#' #'   verbose = FALSE,
#' #'   include_visit_demographics = TRUE,
#' #'   filter_valid_responses = FALSE
#' #' )
#' #'
#' #' # Example 3: Minimal dataset with only baseline + INVOLEA
#' #' swan_minimal <- create_comprehensive_swan_dataset(
#' #'   swan_data_directory = "/path/to/swan/data",
#' #'   output_format = "long",
#' #'   verbose = TRUE,
#' #'   include_visit_demographics = FALSE,
#' #'   filter_valid_responses = TRUE
#' #' )
#' #'
#' #' @importFrom dplyr filter select mutate arrange group_by summarise
#' #'   left_join full_join distinct coalesce case_when all_of any_of ends_with
#' #' @importFrom tidyr pivot_longer pivot_wider
#' #' @importFrom stringr str_detect str_extract str_replace_all str_starts
#' #'   str_remove str_match
#' #' @importFrom purrr map_dfr map_chr map_lgl keep walk compact map
#' #' @importFrom assertthat assert_that
#' #' @importFrom logger log_info log_warn log_error
#' #' @importFrom tibble tibble as_tibble
#' #' @importFrom readr parse_number
#' #' @importFrom glue glue
#' #'
#' #' @export
#' create_comprehensive_swan_dataset <- function(swan_data_directory,
#'                                               output_format = "long",
#'                                               verbose = TRUE,
#'                                               include_visit_demographics = TRUE,
#'                                               filter_valid_responses = TRUE) {
#'
#'   # Input validation with assertthat
#'   if (verbose) {
#'     logger::log_info("Creating comprehensive SWAN dataset with visit-specific variables")
#'     logger::log_info("Function inputs - directory: {swan_data_directory}")
#'     logger::log_info("Function inputs - format: {output_format}, verbose: {verbose}")
#'     logger::log_info("Function inputs - include_visit_demographics: {include_visit_demographics}")
#'     logger::log_info("Function inputs - filter_valid_responses: {filter_valid_responses}")
#'   }
#'
#'   assertthat::assert_that(
#'     is.character(swan_data_directory),
#'     msg = "swan_data_directory must be a character string path"
#'   )
#'   assertthat::assert_that(
#'     dir.exists(swan_data_directory),
#'     msg = glue::glue("Directory does not exist: {swan_data_directory}")
#'   )
#'   assertthat::assert_that(
#'     output_format %in% c("long", "wide"),
#'     msg = "output_format must be 'long' or 'wide'"
#'   )
#'   assertthat::assert_that(
#'     is.logical(verbose),
#'     msg = "verbose must be TRUE or FALSE"
#'   )
#'   assertthat::assert_that(
#'     is.logical(include_visit_demographics),
#'     msg = "include_visit_demographics must be TRUE or FALSE"
#'   )
#'   assertthat::assert_that(
#'     is.logical(filter_valid_responses),
#'     msg = "filter_valid_responses must be TRUE or FALSE"
#'   )
#'
#'   # Step 1: Extract baseline demographics
#'   baseline_participant_demographics <- extract_baseline_participant_demographics(
#'     directory_path = swan_data_directory,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Baseline demographics extracted: {nrow(baseline_participant_demographics)} participants")
#'   }
#'
#'   # Step 2: Extract comprehensive longitudinal data
#'   longitudinal_health_measurements <- extract_comprehensive_longitudinal_data(
#'     directory_path = swan_data_directory,
#'     include_visit_demographics = include_visit_demographics,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Longitudinal data extracted: {nrow(longitudinal_health_measurements)} observations")
#'   }
#'
#'   # Step 3: Merge datasets
#'   merged_comprehensive_dataset <- merge_baseline_with_longitudinal_data(
#'     baseline_data = baseline_participant_demographics,
#'     longitudinal_data = longitudinal_health_measurements,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Data merged: {nrow(merged_comprehensive_dataset)} total observations")
#'   }
#'
#'   # Step 4: Apply filtering if requested
#'   if (filter_valid_responses) {
#'     filtered_comprehensive_dataset <- apply_response_filtering(
#'       merged_data = merged_comprehensive_dataset,
#'       verbose = verbose
#'     )
#'   } else {
#'     filtered_comprehensive_dataset <- merged_comprehensive_dataset
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("After filtering: {nrow(filtered_comprehensive_dataset)} observations")
#'   }
#'
#'   # Step 5: Format final output
#'   final_comprehensive_dataset <- format_comprehensive_output(
#'     merged_data = filtered_comprehensive_dataset,
#'     format = output_format,
#'     verbose = verbose
#'   )
#'
#'   if (verbose) {
#'     logger::log_info("Comprehensive SWAN dataset creation completed")
#'     logger::log_info("Final dataset dimensions: {nrow(final_comprehensive_dataset)} rows × {ncol(final_comprehensive_dataset)} columns")
#'     logger::log_info("Output file path: Not saved to file (returned as object)")
#'   }
#'
#'   return(final_comprehensive_dataset)
#' }
#'
#' #' Extract baseline demographics from SWAN screener/baseline files
#' #' @noRd
#' extract_baseline_participant_demographics <- function(directory_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting baseline participant demographics from SWAN files")
#'   }
#'
#'   # Find baseline/screener files using known ICPSR numbers
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   # Known baseline file patterns from SWAN documentation
#'   baseline_file_identifiers <- c("28762", "04368", "baseline", "screener")
#'
#'   baseline_candidate_files <- purrr::keep(all_rda_files, function(file_path) {
#'     file_name <- tolower(basename(file_path))
#'     any(stringr::str_detect(file_name, baseline_file_identifiers))
#'   })
#'
#'   if (verbose) {
#'     logger::log_info("Found {length(baseline_candidate_files)} potential baseline files")
#'     purrr::walk(baseline_candidate_files, ~logger::log_info("  Candidate file: {basename(.x)}"))
#'   }
#'
#'   # Process each baseline file
#'   baseline_dataset_list <- purrr::map(baseline_candidate_files, function(file_path) {
#'     process_single_baseline_file(file_path, verbose)
#'   })
#'
#'   # Remove NULL results and combine
#'   baseline_dataset_list <- purrr::compact(baseline_dataset_list)
#'
#'   if (length(baseline_dataset_list) == 0) {
#'     logger::log_error("No valid baseline demographic data found in any files")
#'     stop("No baseline demographic data could be extracted")
#'   }
#'
#'   # Combine multiple baseline sources
#'   combined_baseline_demographics <- combine_multiple_baseline_sources(
#'     baseline_dataset_list,
#'     verbose
#'   )
#'
#'   return(combined_baseline_demographics)
#' }
#'
#' #' Process a single baseline file to extract demographics
#' #' @noRd
#' process_single_baseline_file <- function(file_path, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Processing baseline file: {basename(file_path)}")
#'   }
#'
#'   # Load the file safely
#'   file_environment <- new.env()
#'
#'   tryCatch({
#'     load(file_path, envir = file_environment)
#'   }, error = function(e) {
#'     if (verbose) {
#'       logger::log_error("Failed to load {basename(file_path)}: {e$message}")
#'     }
#'     return(NULL)
#'   })
#'
#'   if (length(ls(file_environment)) == 0) {
#'     if (verbose) {
#'       logger::log_warn("No datasets found in {basename(file_path)}")
#'     }
#'     return(NULL)
#'   }
#'
#'   raw_baseline_dataset <- file_environment[[ls(file_environment)[1]]]
#'
#'   # Identify demographic variables
#'   demographic_variable_mapping <- identify_baseline_demographic_variables(
#'     raw_baseline_dataset
#'   )
#'
#'   if (is.null(demographic_variable_mapping$participant_id)) {
#'     if (verbose) {
#'       logger::log_warn("No participant ID found in {basename(file_path)}")
#'     }
#'     return(NULL)
#'   }
#'
#'   # Extract and standardize demographics
#'   standardized_baseline_demographics <- extract_standardized_demographics(
#'     dataset = raw_baseline_dataset,
#'     variable_mapping = demographic_variable_mapping,
#'     source_file = basename(file_path),
#'     verbose = verbose
#'   )
#'
#'   return(standardized_baseline_demographics)
#' }
#'
#' #' Identify demographic variables in baseline dataset
#' #' @noRd
#' identify_baseline_demographic_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Participant ID patterns
#'   participant_id_patterns <- c("^SWANID$", "^ARCHID$", "^swanid$", "^archid$")
#'   participant_id_variable <- find_matching_variable(variable_names, participant_id_patterns)
#'
#'   # Demographic variable patterns (baseline variables often end with 0)
#'   age_patterns <- c("^AGE0?$", "^age0?$", "AGE.*0$")
#'   bmi_patterns <- c("^BMI0?$", "^bmi0?$", "BMI.*0$")
#'   race_patterns <- c("^RACE$", "^race$", "^ETHNIC")
#'   smoking_patterns <- c("^SMOKER0?$", "^SMOKING0?$", "^SMOKE")
#'   parity_patterns <- c("^NUMCHILD$", "^CHILDREN$", "^PARITY")
#'   menopausal_patterns <- c("^STATUS0?$", "^MENO")
#'   education_patterns <- c("^DEGREE$", "^EDUCATION", "^EDUC")
#'   marital_patterns <- c("^MARITAL", "^MARITALGP")
#'
#'   return(list(
#'     participant_id = participant_id_variable,
#'     age = find_matching_variable(variable_names, age_patterns),
#'     bmi = find_matching_variable(variable_names, bmi_patterns),
#'     race = find_matching_variable(variable_names, race_patterns),
#'     smoking = find_matching_variable(variable_names, smoking_patterns),
#'     parity = find_matching_variable(variable_names, parity_patterns),
#'     menopausal = find_matching_variable(variable_names, menopausal_patterns),
#'     education = find_matching_variable(variable_names, education_patterns),
#'     marital = find_matching_variable(variable_names, marital_patterns)
#'   ))
#' }
#'
#' #' Find first matching variable from patterns
#' #' @noRd
#' find_matching_variable <- function(variable_names, patterns) {
#'   for (pattern in patterns) {
#'     matches <- variable_names[stringr::str_detect(variable_names, pattern)]
#'     if (length(matches) > 0) {
#'       return(matches[1])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' Extract and standardize demographic variables
#' #' @noRd
#' extract_standardized_demographics <- function(dataset, variable_mapping,
#'                                               source_file, verbose) {
#'
#'   # Start with participant ID
#'   standardized_demographics <- tibble::tibble(
#'     swan_participant_id = as.character(dataset[[variable_mapping$participant_id]])
#'   )
#'
#'   # Add demographic variables with type-safe conversion
#'   if (!is.null(variable_mapping$age)) {
#'     standardized_demographics$baseline_age_years <-
#'       convert_to_numeric_safely(dataset[[variable_mapping$age]])
#'   }
#'
#'   if (!is.null(variable_mapping$bmi)) {
#'     standardized_demographics$baseline_bmi_kg_m2 <-
#'       convert_to_numeric_safely(dataset[[variable_mapping$bmi]])
#'   }
#'
#'   if (!is.null(variable_mapping$race)) {
#'     standardized_demographics$baseline_race_ethnicity <-
#'       standardize_race_ethnicity_variable(dataset[[variable_mapping$race]])
#'   }
#'
#'   if (!is.null(variable_mapping$smoking)) {
#'     standardized_demographics$baseline_smoking_status <-
#'       standardize_smoking_status_variable(dataset[[variable_mapping$smoking]])
#'   }
#'
#'   if (!is.null(variable_mapping$parity)) {
#'     standardized_demographics$baseline_parity_count <-
#'       convert_to_numeric_safely(dataset[[variable_mapping$parity]])
#'   }
#'
#'   if (!is.null(variable_mapping$menopausal)) {
#'     standardized_demographics$baseline_menopausal_stage <-
#'       standardize_menopausal_status_variable(dataset[[variable_mapping$menopausal]])
#'   }
#'
#'   if (!is.null(variable_mapping$education)) {
#'     standardized_demographics$baseline_education_level <-
#'       standardize_education_variable(dataset[[variable_mapping$education]])
#'   }
#'
#'   if (!is.null(variable_mapping$marital)) {
#'     standardized_demographics$baseline_marital_status <-
#'       standardize_marital_status_variable(dataset[[variable_mapping$marital]])
#'   }
#'
#'   standardized_demographics$source_file <- source_file
#'
#'   if (verbose) {
#'     valid_participant_count <- sum(!is.na(standardized_demographics$swan_participant_id))
#'     demographic_variable_count <- ncol(standardized_demographics) - 2  # exclude ID and source
#'     logger::log_info("  Extracted {valid_participant_count} participants with {demographic_variable_count} demographic variables")
#'   }
#'
#'   return(standardized_demographics)
#' }
#'
#' #' Extract comprehensive longitudinal data including visit-specific variables
#' #' @noRd
#' extract_comprehensive_longitudinal_data <- function(directory_path,
#'                                                     include_visit_demographics,
#'                                                     verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Extracting comprehensive longitudinal data from all SWAN files")
#'   }
#'
#'   # Find all RDA files
#'   all_rda_files <- list.files(
#'     path = directory_path,
#'     pattern = "\\.rda$",
#'     full.names = TRUE,
#'     ignore.case = TRUE
#'   )
#'
#'   # Process each file for longitudinal variables
#'   longitudinal_dataset_list <- purrr::map(all_rda_files, function(file_path) {
#'     process_single_longitudinal_file(
#'       file_path = file_path,
#'       include_visit_demographics = include_visit_demographics,
#'       verbose = verbose
#'     )
#'   })
#'
#'   # Remove NULL results
#'   longitudinal_dataset_list <- purrr::compact(longitudinal_dataset_list)
#'
#'   if (length(longitudinal_dataset_list) == 0) {
#'     logger::log_error("No longitudinal data found in any files")
#'     stop("No longitudinal data could be extracted")
#'   }
#'
#'   # Combine all longitudinal datasets
#'   combined_longitudinal_data <- purrr::map_dfr(longitudinal_dataset_list, ~.x)
#'
#'   # Remove duplicate observations (same participant, visit, variable)
#'   deduped_longitudinal_data <- combined_longitudinal_data %>%
#'     dplyr::distinct(
#'       swan_participant_id,
#'       visit_number,
#'       incontinence_source_variable,
#'       .keep_all = TRUE
#'     )
#'
#'   if (verbose) {
#'     unique_participant_count <- length(unique(deduped_longitudinal_data$swan_participant_id))
#'     visit_range <- paste(
#'       min(deduped_longitudinal_data$visit_number, na.rm = TRUE), "to",
#'       max(deduped_longitudinal_data$visit_number, na.rm = TRUE)
#'     )
#'     logger::log_info("Comprehensive longitudinal data extracted: {nrow(deduped_longitudinal_data)} observations")
#'     logger::log_info("Visit range: {visit_range}")
#'     logger::log_info("Unique participants with longitudinal data: {unique_participant_count}")
#'   }
#'
#'   return(deduped_longitudinal_data)
#' }
#'
#' #' Process single file for longitudinal variables
#' #' @noRd
#' process_single_longitudinal_file <- function(file_path, include_visit_demographics, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Scanning {basename(file_path)} for longitudinal variables")
#'   }
#'
#'   # Load file safely
#'   file_environment <- new.env()
#'
#'   tryCatch({
#'     load(file_path, envir = file_environment)
#'   }, error = function(e) {
#'     if (verbose) {
#'       logger::log_warn("Failed to load {basename(file_path)}: {e$message}")
#'     }
#'     return(NULL)
#'   })
#'
#'   if (length(ls(file_environment)) == 0) return(NULL)
#'
#'   raw_longitudinal_dataset <- file_environment[[ls(file_environment)[1]]]
#'
#'   # Look for incontinence variables (INVOLEA/LEKINVO)
#'   incontinence_variable_names <- identify_incontinence_variables(raw_longitudinal_dataset)
#'
#'   if (length(incontinence_variable_names) == 0) {
#'     return(NULL)  # No incontinence variables in this file
#'   }
#'
#'   if (verbose) {
#'     incontinence_vars_string <- paste(incontinence_variable_names, collapse = ", ")
#'     logger::log_info("  Found {length(incontinence_variable_names)} incontinence variables: {incontinence_vars_string}")
#'   }
#'
#'   # Extract longitudinal data
#'   extracted_longitudinal_data <- extract_longitudinal_measurements(
#'     dataset = raw_longitudinal_dataset,
#'     incontinence_variables = incontinence_variable_names,
#'     include_visit_demographics = include_visit_demographics,
#'     source_file = basename(file_path),
#'     verbose = verbose
#'   )
#'
#'   return(extracted_longitudinal_data)
#' }
#'
#' #' Identify incontinence variables (INVOLEA/LEKINVO) in dataset
#' #' @noRd
#' identify_incontinence_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'
#'   # Look for INVOLEA and LEKINVO patterns
#'   incontinence_patterns <- c("INVOLEA\\d*", "LEKINVO\\d*", "involea\\d*", "lekinvo\\d*")
#'
#'   incontinence_variables <- c()
#'   for (pattern in incontinence_patterns) {
#'     matching_variables <- variable_names[stringr::str_detect(variable_names, pattern)]
#'     incontinence_variables <- c(incontinence_variables, matching_variables)
#'   }
#'
#'   return(unique(incontinence_variables))
#' }
#'
#' #' Extract longitudinal measurements including visit-specific demographics
#' #' @noRd
#' extract_longitudinal_measurements <- function(dataset, incontinence_variables,
#'                                               include_visit_demographics,
#'                                               source_file, verbose) {
#'
#'   # Identify core variables
#'   participant_id_variable <- identify_participant_id_variable(dataset)
#'   visit_number_variable <- identify_visit_number_variable(dataset)
#'
#'   if (is.null(participant_id_variable)) {
#'     if (verbose) {
#'       logger::log_warn("No participant ID found in {source_file}")
#'     }
#'     return(NULL)
#'   }
#'
#'   # Start with core columns
#'   core_column_names <- c(participant_id_variable)
#'   if (!is.null(visit_number_variable)) {
#'     core_column_names <- c(core_column_names, visit_number_variable)
#'   }
#'
#'   # Identify ALL visit-specific demographic variables if requested
#'   visit_specific_variables <- list()
#'
#'   if (include_visit_demographics) {
#'     visit_specific_variables <- identify_visit_specific_demographic_variables(dataset)
#'
#'     if (verbose && length(visit_specific_variables) > 0) {
#'       total_visit_vars <- sum(lengths(visit_specific_variables))
#'       visit_vars_string <- paste(names(visit_specific_variables), collapse = ", ")
#'       logger::log_info("  Visit-specific demographics found: {visit_vars_string} (total: {total_visit_vars} variables)")
#'     }
#'   }
#'
#'   # Combine all variables to extract - flatten the visit-specific variables list
#'   all_visit_specific_vars <- unlist(visit_specific_variables)
#'   all_variables_to_extract <- c(
#'     core_column_names,
#'     all_visit_specific_vars,  # Now includes ALL visit-specific variables
#'     incontinence_variables
#'   )
#'
#'   # Extract data
#'   longitudinal_measurements <- tibble::as_tibble(dataset) %>%
#'     dplyr::select(dplyr::all_of(all_variables_to_extract))
#'
#'   # Standardize core column names
#'   names(longitudinal_measurements)[1] <- "swan_participant_id"
#'   if (!is.null(visit_number_variable)) {
#'     names(longitudinal_measurements)[2] <- "visit_number_from_file"
#'   } else {
#'     longitudinal_measurements$visit_number_from_file <- NA_real_
#'   }
#'
#'   # Convert incontinence variables to long format first
#'   incontinence_long_format <- longitudinal_measurements %>%
#'     tidyr::pivot_longer(
#'       cols = dplyr::all_of(incontinence_variables),
#'       names_to = "incontinence_source_variable",
#'       values_to = "incontinence_raw_response"
#'     ) %>%
#'     dplyr::mutate(
#'       swan_participant_id = as.character(swan_participant_id),
#'       # Extract visit number from incontinence variable name
#'       visit_number = extract_visit_number_from_variable(incontinence_source_variable),
#'       incontinence_status = standardize_incontinence_response(incontinence_raw_response),
#'       years_since_baseline = visit_number,
#'       source_file = source_file
#'     ) %>%
#'     dplyr::select(-visit_number_from_file) %>%
#'     dplyr::filter(!is.na(incontinence_raw_response))  # Remove missing responses
#'
#'   # Now add visit-specific demographic variables that match the visit number
#'   if (include_visit_demographics && length(visit_specific_variables) > 0) {
#'
#'     for (demographic_type in names(visit_specific_variables)) {
#'       variable_names_for_this_type <- visit_specific_variables[[demographic_type]]
#'
#'       # Create a mapping of visit numbers to values for this demographic type
#'       visit_demographic_mapping <- tibble::tibble()
#'
#'       for (var_name in variable_names_for_this_type) {
#'         # Extract visit number from variable name (e.g., AGE3 -> 3, BMI0 -> 0)
#'         var_visit_number <- extract_visit_number_from_variable(var_name)
#'
#'         # Get the values for this variable
#'         var_values <- dataset[[var_name]]
#'
#'         # Create temporary mapping
#'         temp_mapping <- tibble::tibble(
#'           swan_participant_id = as.character(dataset[[participant_id_variable]]),
#'           visit_number = var_visit_number,
#'           !!paste0("visit_", demographic_type) := var_values
#'         ) %>%
#'           dplyr::filter(!is.na(.data[[paste0("visit_", demographic_type)]]))
#'
#'         # Combine with existing mapping
#'         if (nrow(visit_demographic_mapping) == 0) {
#'           visit_demographic_mapping <- temp_mapping
#'         } else {
#'           visit_demographic_mapping <- dplyr::bind_rows(visit_demographic_mapping, temp_mapping)
#'         }
#'       }
#'
#'       # Merge this demographic variable with the incontinence data
#'       if (nrow(visit_demographic_mapping) > 0) {
#'         incontinence_long_format <- incontinence_long_format %>%
#'           dplyr::left_join(
#'             visit_demographic_mapping,
#'             by = c("swan_participant_id", "visit_number")
#'           )
#'       }
#'     }
#'   }
#'
#'   # Log results
#'   if (verbose && nrow(incontinence_long_format) > 0) {
#'     unique_visits <- sort(unique(incontinence_long_format$visit_number))
#'     visits_string <- paste(unique_visits, collapse = ", ")
#'     logger::log_info("  Extracted visits for {source_file}: {visits_string}")
#'     logger::log_info("  Total observations in {source_file}: {nrow(incontinence_long_format)}")
#'
#'     # Log visit-specific variables preserved
#'     visit_specific_columns <- names(incontinence_long_format)[stringr::str_starts(names(incontinence_long_format), "visit_")]
#'     if (length(visit_specific_columns) > 0) {
#'       visit_vars_preserved <- paste(stringr::str_remove(visit_specific_columns, "^visit_"), collapse = ", ")
#'       logger::log_info("  Visit-specific variables preserved: {visit_vars_preserved}")
#'
#'       # Sample a few values to verify they're changing across visits
#'       if ("visit_bmi_kg_m2" %in% names(incontinence_long_format)) {
#'         sample_participant <- incontinence_long_format$swan_participant_id[1]
#'         sample_bmi_values <- incontinence_long_format %>%
#'           dplyr::filter(swan_participant_id == sample_participant) %>%
#'           dplyr::select(visit_number, visit_bmi_kg_m2) %>%
#'           dplyr::arrange(visit_number)
#'
#'         if (nrow(sample_bmi_values) > 1) {
#'           bmi_range <- paste(round(range(sample_bmi_values$visit_bmi_kg_m2, na.rm = TRUE), 1), collapse = " to ")
#'           logger::log_info("  Sample BMI range for participant {sample_participant}: {bmi_range}")
#'         }
#'       }
#'     }
#'   }
#'
#'   return(incontinence_long_format)
#' }
#'
#' #' Identify visit-specific demographic variables in dataset
#' #' @noRd
#' identify_visit_specific_demographic_variables <- function(dataset) {
#'
#'   variable_names <- names(dataset)
#'   visit_specific_vars <- list()
#'
#'   # AGE variables (AGE0, AGE1, AGE2, etc.) - get ALL visit-specific age variables
#'   age_variables <- variable_names[stringr::str_detect(variable_names, "^AGE\\d+$")]
#'   if (length(age_variables) > 0) {
#'     visit_specific_vars[["age_years"]] <- age_variables  # ALL age variables, not just [1]
#'   }
#'
#'   # BMI variables (BMI0, BMI1, BMI2, etc.) - get ALL visit-specific BMI variables
#'   bmi_variables <- variable_names[stringr::str_detect(variable_names, "^BMI\\d+$")]
#'   if (length(bmi_variables) > 0) {
#'     visit_specific_vars[["bmi_kg_m2"]] <- bmi_variables  # ALL BMI variables, not just [1]
#'   }
#'
#'   # Weight variables (WEIGHT0, WEIGHT1, etc.) - get ALL visit-specific weight variables
#'   weight_variables <- variable_names[stringr::str_detect(variable_names, "^WEIGHT\\d+$")]
#'   if (length(weight_variables) > 0) {
#'     visit_specific_vars[["weight_kg"]] <- weight_variables  # ALL weight variables, not just [1]
#'   }
#'
#'   # Height variables (HEIGHT0, HEIGHT1, etc.) - get ALL visit-specific height variables
#'   height_variables <- variable_names[stringr::str_detect(variable_names, "^HEIGHT\\d+$")]
#'   if (length(height_variables) > 0) {
#'     visit_specific_vars[["height_cm"]] <- height_variables  # ALL height variables, not just [1]
#'   }
#'
#'   # Smoking variables (SMOKER0, SMOKER1, etc.) - get ALL visit-specific smoking variables
#'   smoking_variables <- variable_names[stringr::str_detect(variable_names, "^SMOKER?E?\\d+$")]
#'   if (length(smoking_variables) > 0) {
#'     visit_specific_vars[["smoking_status"]] <- smoking_variables  # ALL smoking variables, not just [1]
#'   }
#'
#'   # Menopausal status variables (STATUS0, STATUS1, etc.) - get ALL visit-specific status variables
#'   menopause_variables <- variable_names[stringr::str_detect(variable_names, "^STATUS\\d+$")]
#'   if (length(menopause_variables) > 0) {
#'     visit_specific_vars[["menopausal_stage"]] <- menopause_variables  # ALL status variables, not just [1]
#'   }
#'
#'   # Marital status variables (MARITALGP0, MARITALGP1, etc.) - get ALL visit-specific marital variables
#'   marital_variables <- variable_names[stringr::str_detect(variable_names, "^MARITALGP\\d+$")]
#'   if (length(marital_variables) > 0) {
#'     visit_specific_vars[["marital_status"]] <- marital_variables  # ALL marital variables, not just [1]
#'   }
#'
#'   # Parity variables (NUMCHILD0, NUMCHILD1, etc.) - get ALL visit-specific parity variables
#'   parity_variables <- variable_names[stringr::str_detect(variable_names, "^NUMCHILD\\d+$")]
#'   if (length(parity_variables) > 0) {
#'     visit_specific_vars[["parity_count"]] <- parity_variables  # ALL parity variables, not just [1]
#'   }
#'
#'   return(visit_specific_vars)
#' }
#'
#' #' Extract visit number from variable name (e.g., INVOLEA9 -> 9)
#' #' @noRd
#' extract_visit_number_from_variable <- function(variable_names) {
#'
#'   # Extract trailing digits from variable names
#'   visit_numbers <- stringr::str_extract(variable_names, "\\d+$")
#'   visit_numbers <- as.numeric(visit_numbers)
#'
#'   # Replace NAs with 0 (baseline/screener)
#'   visit_numbers[is.na(visit_numbers)] <- 0
#'
#'   return(visit_numbers)
#' }
#'
#' #' Standardize incontinence response values
#' #' @noRd
#' standardize_incontinence_response <- function(response_values) {
#'
#'   dplyr::case_when(
#'     # Standard Yes responses
#'     response_values %in% c("(2) Yes", "2", 2, "Yes", "YES", "yes") ~ 1,
#'     # Standard No responses
#'     response_values %in% c("(1) No", "1", 1, "No", "NO", "no") ~ 0,
#'     # Extended formats from later visits
#'     response_values %in% c("(2) 2: Yes", "(2)2: Yes", "2: Yes") ~ 1,
#'     response_values %in% c("(1) 1: No", "(1)1: No", "1: No") ~ 0,
#'     TRUE ~ NA_real_
#'   )
#' }
#'
#' #' Helper function to identify participant ID column
#' #' @noRd
#' identify_participant_id_variable <- function(dataset) {
#'   participant_id_patterns <- c("^SWANID$", "^ARCHID$", "^swanid$", "^archid$")
#'   for (pattern in participant_id_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' Helper function to identify visit number column
#' #' @noRd
#' identify_visit_number_variable <- function(dataset) {
#'   visit_patterns <- c("^VISIT$", "^visit$", "^Visit$", "^VISITNO$")
#'   for (pattern in visit_patterns) {
#'     matching_columns <- stringr::str_detect(names(dataset), pattern)
#'     if (any(matching_columns)) {
#'       return(names(dataset)[which(matching_columns)[1]])
#'     }
#'   }
#'   return(NULL)
#' }
#'
#' #' Safe numeric conversion
#' #' @noRd
#' convert_to_numeric_safely <- function(variable_values) {
#'   suppressWarnings(as.numeric(as.character(variable_values)))
#' }
#'
#' #' Standardize race/ethnicity variable
#' #' @noRd
#' standardize_race_ethnicity_variable <- function(race_values) {
#'   dplyr::case_when(
#'     stringr::str_detect(as.character(race_values), "White|Caucasian|4") ~ "White",
#'     stringr::str_detect(as.character(race_values), "Black|African|1") ~ "Black",
#'     stringr::str_detect(as.character(race_values), "Hispanic|5") ~ "Hispanic",
#'     stringr::str_detect(as.character(race_values), "Chinese|2") ~ "Chinese",
#'     stringr::str_detect(as.character(race_values), "Japanese|3") ~ "Japanese",
#'     TRUE ~ "Other"
#'   )
#' }
#'
#' #' Standardize smoking status variable
#' #' @noRd
#' standardize_smoking_status_variable <- function(smoking_values) {
#'   dplyr::case_when(
#'     smoking_values %in% c(1, "(1) Never smoked", "Never") ~ "Never",
#'     smoking_values %in% c(2, "(2) Past smoker", "Past") ~ "Past",
#'     smoking_values %in% c(3, "(3) Current smoker", "Current") ~ "Current",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Standardize menopausal status variable
#' #' @noRd
#' standardize_menopausal_status_variable <- function(status_values) {
#'   dplyr::case_when(
#'     status_values %in% c(1, "(1) Post by BSO") ~ "Post BSO",
#'     status_values %in% c(2, "(2) Natural Post", "(2) Postmenopausal") ~ "Natural Post",
#'     status_values %in% c(3, "(3) Late Peri", "(3) Late perimenopausal") ~ "Late Peri",
#'     status_values %in% c(4, "(4) Early Peri", "(4) Early perimenopausal") ~ "Early Peri",
#'     status_values %in% c(5, "(5) Pre", "(5) Premenopausal") ~ "Pre",
#'     status_values %in% c(6, "(6) Pregnant/breastfeeding") ~ "Pregnant/Breastfeeding",
#'     status_values %in% c(7, "(7) Unknown due to HT use") ~ "Unknown HT",
#'     status_values %in% c(8, "(8) Unknown due to hysterectomy") ~ "Unknown Hysterectomy",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Standardize education variable
#' #' @noRd
#' standardize_education_variable <- function(education_values) {
#'   dplyr::case_when(
#'     education_values %in% c(1, "(1) Less than high school") ~ "Less than HS",
#'     education_values %in% c(2, "(2) High school") ~ "High School",
#'     education_values %in% c(3, "(3) Some college") ~ "Some College",
#'     education_values %in% c(4, "(4) College graduate") ~ "College Graduate",
#'     education_values %in% c(5, "(5) Graduate school") ~ "Graduate School",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Standardize marital status variable
#' #' @noRd
#' standardize_marital_status_variable <- function(marital_values) {
#'   dplyr::case_when(
#'     marital_values %in% c(1, "(1) Married", "Married") ~ "Married",
#'     marital_values %in% c(2, "(2) Single", "Single") ~ "Single",
#'     marital_values %in% c(3, "(3) Divorced/Separated", "Divorced") ~ "Divorced/Separated",
#'     marital_values %in% c(4, "(4) Widowed", "Widowed") ~ "Widowed",
#'     TRUE ~ NA_character_
#'   )
#' }
#'
#' #' Combine multiple baseline sources with type-safe merging
#' #' @noRd
#' combine_multiple_baseline_sources <- function(baseline_dataset_list, verbose) {
#'
#'   if (length(baseline_dataset_list) == 1) {
#'     return(baseline_dataset_list[[1]])
#'   }
#'
#'   if (verbose) {
#'     logger::log_info("Combining {length(baseline_dataset_list)} baseline sources")
#'   }
#'
#'   # Find the most complete dataset (most non-missing participant IDs)
#'   dataset_completeness_scores <- purrr::map_dbl(baseline_dataset_list, function(baseline_dataset) {
#'     sum(!is.na(baseline_dataset$swan_participant_id))
#'   })
#'
#'   primary_baseline_dataset <- baseline_dataset_list[[which.max(dataset_completeness_scores)]]
#'
#'   if (verbose) {
#'     logger::log_info("Primary baseline dataset: {primary_baseline_dataset$source_file[1]} with {max(dataset_completeness_scores)} participants")
#'   }
#'
#'   # Merge additional datasets
#'   for (dataset_index in seq_along(baseline_dataset_list)) {
#'     if (dataset_index == which.max(dataset_completeness_scores)) next
#'
#'     secondary_baseline_dataset <- baseline_dataset_list[[dataset_index]]
#'
#'     if (verbose) {
#'       logger::log_info("Merging secondary baseline: {secondary_baseline_dataset$source_file[1]}")
#'     }
#'
#'     # Harmonize data types before merging
#'     secondary_baseline_dataset <- harmonize_baseline_data_types(
#'       secondary_baseline_dataset,
#'       primary_baseline_dataset,
#'       verbose
#'     )
#'
#'     primary_baseline_dataset <- primary_baseline_dataset %>%
#'       dplyr::full_join(
#'         secondary_baseline_dataset,
#'         by = "swan_participant_id",
#'         suffix = c("", "_secondary")
#'       )
#'
#'     # Smart coalescing - only coalesce variables that exist in both datasets
#'     baseline_variables_to_merge <- c(
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_parity_count",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_menopausal_stage",
#'       "baseline_education_level",
#'       "baseline_marital_status"
#'     )
#'
#'     # Check which variables actually exist with _secondary suffix
#'     available_columns <- names(primary_baseline_dataset)
#'
#'     for (baseline_variable in baseline_variables_to_merge) {
#'       secondary_variable_name <- paste0(baseline_variable, "_secondary")
#'
#'       if (baseline_variable %in% available_columns && secondary_variable_name %in% available_columns) {
#'         if (verbose) {
#'           logger::log_info("  Coalescing {baseline_variable}")
#'         }
#'
#'         # Apply appropriate coalescing based on data type
#'         if (stringr::str_detect(baseline_variable, "age|bmi|parity|count")) {
#'           # Numeric variables
#'           primary_baseline_dataset[[baseline_variable]] <- apply_safe_numeric_coalesce(
#'             primary_baseline_dataset[[baseline_variable]],
#'             primary_baseline_dataset[[secondary_variable_name]]
#'           )
#'         } else {
#'           # Character variables
#'           primary_baseline_dataset[[baseline_variable]] <- apply_safe_character_coalesce(
#'             primary_baseline_dataset[[baseline_variable]],
#'             primary_baseline_dataset[[secondary_variable_name]]
#'           )
#'         }
#'       } else if (verbose) {
#'         logger::log_info("  Skipping {baseline_variable} - not available in secondary dataset")
#'       }
#'     }
#'
#'     # Remove all secondary columns
#'     primary_baseline_dataset <- primary_baseline_dataset %>%
#'       dplyr::select(-dplyr::ends_with("_secondary"))
#'   }
#'
#'   if (verbose) {
#'     final_participant_count <- sum(!is.na(primary_baseline_dataset$swan_participant_id))
#'     logger::log_info("Combined baseline dataset: {final_participant_count} total participants")
#'   }
#'
#'   return(primary_baseline_dataset)
#' }
#'
#' #' Harmonize data types between baseline datasets
#' #' @noRd
#' harmonize_baseline_data_types <- function(secondary_dataset, primary_dataset, verbose) {
#'
#'   baseline_column_names <- names(secondary_dataset)[stringr::str_starts(names(secondary_dataset), "baseline_")]
#'
#'   for (column_name in baseline_column_names) {
#'     if (column_name %in% names(primary_dataset)) {
#'
#'       primary_data_type <- class(primary_dataset[[column_name]])[1]
#'       secondary_data_type <- class(secondary_dataset[[column_name]])[1]
#'
#'       if (primary_data_type != secondary_data_type) {
#'         if (verbose) {
#'           logger::log_info("Harmonizing {column_name}: {secondary_data_type} -> {primary_data_type}")
#'         }
#'
#'         # Convert secondary to match primary type
#'         if (primary_data_type %in% c("numeric", "double")) {
#'           secondary_dataset[[column_name]] <- convert_to_numeric_safely(secondary_dataset[[column_name]])
#'         } else if (primary_data_type == "character") {
#'           secondary_dataset[[column_name]] <- as.character(secondary_dataset[[column_name]])
#'         } else if (primary_data_type == "logical") {
#'           secondary_dataset[[column_name]] <- as.logical(secondary_dataset[[column_name]])
#'         }
#'       }
#'     }
#'   }
#'
#'   return(secondary_dataset)
#' }
#'
#' #' Type-safe numeric coalesce
#' #' @noRd
#' apply_safe_numeric_coalesce <- function(primary_values, secondary_values) {
#'   # Convert both to numeric if needed
#'   primary_numeric <- if (is.numeric(primary_values)) primary_values else convert_to_numeric_safely(primary_values)
#'   secondary_numeric <- if (is.numeric(secondary_values)) secondary_values else convert_to_numeric_safely(secondary_values)
#'   dplyr::coalesce(primary_numeric, secondary_numeric)
#' }
#'
#' #' Type-safe character coalesce
#' #' @noRd
#' apply_safe_character_coalesce <- function(primary_values, secondary_values) {
#'   # Convert both to character if needed
#'   primary_character <- if (is.character(primary_values)) primary_values else as.character(primary_values)
#'   secondary_character <- if (is.character(secondary_values)) secondary_values else as.character(secondary_values)
#'   dplyr::coalesce(primary_character, secondary_character)
#' }
#'
#' #' Merge baseline demographics with longitudinal data
#' #' @noRd
#' merge_baseline_with_longitudinal_data <- function(baseline_data, longitudinal_data, verbose) {
#'
#'   if (verbose) {
#'     baseline_participant_count <- length(unique(baseline_data$swan_participant_id))
#'     longitudinal_observation_count <- nrow(longitudinal_data)
#'     logger::log_info("Merging baseline demographics with longitudinal measurements")
#'     logger::log_info("Baseline participants: {baseline_participant_count}")
#'     logger::log_info("Longitudinal observations: {longitudinal_observation_count}")
#'   }
#'
#'   # Merge datasets
#'   merged_comprehensive_data <- longitudinal_data %>%
#'     dplyr::left_join(
#'       baseline_data %>% dplyr::select(-source_file),
#'       by = "swan_participant_id"
#'     )
#'
#'   # Add derived time variables
#'   merged_comprehensive_data <- merged_comprehensive_data %>%
#'     dplyr::mutate(
#'       # Calculate current age at each visit
#'       current_age_years = baseline_age_years + years_since_baseline,
#'       # Add visit timing categories
#'       visit_timing_category = dplyr::case_when(
#'         visit_number == 0 ~ "Baseline",
#'         visit_number %in% 1:3 ~ "Early Follow-up",
#'         visit_number %in% 4:7 ~ "Mid Follow-up",
#'         visit_number >= 8 ~ "Late Follow-up",
#'         TRUE ~ "Unknown"
#'       )
#'     ) %>%
#'     dplyr::arrange(swan_participant_id, visit_number)
#'
#'   if (verbose) {
#'     merged_observation_count <- nrow(merged_comprehensive_data)
#'     participants_with_both_data <- length(unique(merged_comprehensive_data$swan_participant_id[!is.na(merged_comprehensive_data$baseline_age_years)]))
#'     logger::log_info("Merged comprehensive dataset: {merged_observation_count} observations")
#'     logger::log_info("Participants with both baseline + longitudinal data: {participants_with_both_data}")
#'   }
#'
#'   return(merged_comprehensive_data)
#' }
#'
#' #' Apply filtering for valid responses
#' #' @noRd
#' apply_response_filtering <- function(merged_data, verbose) {
#'
#'   if (verbose) {
#'     original_observation_count <- nrow(merged_data)
#'     logger::log_info("Applying response filtering to remove invalid/missing incontinence responses")
#'   }
#'
#'   filtered_data <- merged_data %>%
#'     dplyr::filter(
#'       !is.na(incontinence_status),
#'       !is.na(swan_participant_id),
#'       !is.na(visit_number)
#'     )
#'
#'   if (verbose) {
#'     filtered_observation_count <- nrow(filtered_data)
#'     observations_removed <- original_observation_count - filtered_observation_count
#'     logger::log_info("Filtering completed: {filtered_observation_count} observations retained")
#'     logger::log_info("Observations removed: {observations_removed}")
#'   }
#'
#'   return(filtered_data)
#' }
#'
#' #' Format comprehensive output dataset
#' #' @noRd
#' format_comprehensive_output <- function(merged_data, format, verbose) {
#'
#'   if (verbose) {
#'     logger::log_info("Formatting comprehensive output as: {format}")
#'     all_column_names <- paste(names(merged_data), collapse = ", ")
#'     logger::log_info("Available columns: {all_column_names}")
#'   }
#'
#'   if (format == "long") {
#'     # Identify visit-specific columns
#'     visit_specific_column_names <- names(merged_data)[stringr::str_starts(names(merged_data), "visit_")]
#'
#'     if (verbose && length(visit_specific_column_names) > 0) {
#'       visit_vars_string <- paste(visit_specific_column_names, collapse = ", ")
#'       logger::log_info("Visit-specific columns preserved: {visit_vars_string}")
#'     } else if (verbose) {
#'       logger::log_warn("No visit-specific columns found in final dataset")
#'     }
#'
#'     # Build comprehensive column selection
#'     baseline_demographic_columns <- c(
#'       "swan_participant_id",
#'       "visit_number",
#'       "years_since_baseline",
#'       "current_age_years",
#'       "visit_timing_category",
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_parity_count",
#'       "baseline_menopausal_stage",
#'       "baseline_education_level",
#'       "baseline_marital_status"
#'     )
#'
#'     incontinence_measurement_columns <- c(
#'       "incontinence_status",
#'       "incontinence_raw_response",
#'       "incontinence_source_variable"
#'     )
#'
#'     # Combine all columns, keeping only those that exist
#'     all_desired_columns <- c(
#'       baseline_demographic_columns,
#'       incontinence_measurement_columns,
#'       visit_specific_column_names
#'     )
#'
#'     columns_that_exist_in_data <- intersect(all_desired_columns, names(merged_data))
#'
#'     if (verbose) {
#'       logger::log_info("Selecting {length(columns_that_exist_in_data)} columns for long format output")
#'     }
#'
#'     # Create final long format dataset
#'     final_comprehensive_dataset <- merged_data %>%
#'       dplyr::select(dplyr::all_of(columns_that_exist_in_data)) %>%
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         incontinence_source_variable,
#'         .keep_all = TRUE
#'       ) %>%
#'       dplyr::arrange(swan_participant_id, visit_number)
#'
#'   } else {
#'     # Wide format conversion
#'     visit_specific_column_names <- names(merged_data)[stringr::str_starts(names(merged_data), "visit_")]
#'
#'     baseline_columns_for_wide <- c(
#'       "swan_participant_id",
#'       "baseline_age_years",
#'       "baseline_bmi_kg_m2",
#'       "baseline_race_ethnicity",
#'       "baseline_smoking_status",
#'       "baseline_parity_count",
#'       "baseline_menopausal_stage",
#'       "baseline_education_level",
#'       "baseline_marital_status"
#'     )
#'
#'     measurement_columns_for_wide <- c(
#'       "visit_number",
#'       "incontinence_status",
#'       "current_age_years"
#'     )
#'
#'     all_columns_for_wide <- c(
#'       baseline_columns_for_wide,
#'       measurement_columns_for_wide,
#'       visit_specific_column_names
#'     )
#'
#'     columns_that_exist_for_wide <- intersect(all_columns_for_wide, names(merged_data))
#'
#'     if (verbose) {
#'       logger::log_info("Converting to wide format with {length(columns_that_exist_for_wide)} base columns")
#'     }
#'
#'     final_comprehensive_dataset <- merged_data %>%
#'       dplyr::select(dplyr::all_of(columns_that_exist_for_wide)) %>%
#'       dplyr::distinct(
#'         swan_participant_id,
#'         visit_number,
#'         .keep_all = TRUE
#'       ) %>%
#'       tidyr::pivot_wider(
#'         names_from = visit_number,
#'         values_from = c(
#'           dplyr::any_of(visit_specific_column_names),
#'           incontinence_status,
#'           current_age_years
#'         ),
#'         names_sep = "_visit_"
#'       )
#'   }
#'
#'   if (verbose) {
#'     final_row_count <- nrow(final_comprehensive_dataset)
#'     final_column_count <- ncol(final_comprehensive_dataset)
#'     unique_participant_count <- length(unique(final_comprehensive_dataset$swan_participant_id))
#'
#'     logger::log_info("Final comprehensive dataset formatting completed")
#'     logger::log_info("Dimensions: {final_row_count} rows × {final_column_count} columns")
#'     logger::log_info("Unique participants: {unique_participant_count}")
#'
#'     if (format == "long") {
#'       final_visit_specific_count <- sum(stringr::str_starts(names(final_comprehensive_dataset), "visit_"))
#'       visit_range_min <- min(final_comprehensive_dataset$visit_number, na.rm = TRUE)
#'       visit_range_max <- max(final_comprehensive_dataset$visit_number, na.rm = TRUE)
#'       logger::log_info("Visit-specific variables in final dataset: {final_visit_specific_count}")
#'       logger::log_info("Visit range: {visit_range_min} to {visit_range_max}")
#'     }
#'   }
#'
#'   return(final_comprehensive_dataset)
#' }
#'
#' comprehensive_swan_dataset_fixed <- create_comprehensive_swan_dataset(
#'   swan_data_directory = swan_data_directory,
#'   output_format = "long",
#'   verbose = TRUE,
#'   include_visit_demographics = TRUE,
#'   filter_valid_responses = TRUE
#' )
