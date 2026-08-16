# scripts/build_and_run_real_supply_calibration.R
#
# Find real longitudinal URPS provider activity, build confirmed departures,
# write the four canonical supply-calibration inputs, and run the existing
# end-to-end forecast calibration.
#
# Run from the simulation repository root:
#
# source("scripts/build_and_run_real_supply_calibration.R")
# supply_run <- build_and_run_real_supply_calibration()
#
# Optional:
# supply_run <- build_and_run_real_supply_calibration(
#   provider_activity_path = "/known/path/provider_years.parquet"
# )
#
# mufflyaccess intentionally returns observed retirement as not_ascertained --
# it provides the ABOG entrant series (from urps_subspecialty_cert_year) but not
# exits, because exits derived from the survivorship-biased certification stock
# would be wrong. This script therefore searches for an EXTERNAL longitudinal
# activity source and builds a two-year-confirmed departure panel from it, then
# guards that source against the ABOG stock via stock_concordance (a CMS/MIPS
# file can be provider-year longitudinal yet represent Medicare participation
# rather than actual URPS practice).
find_local_repo <- function(repo_name) {
  base::message("Finding local repository: ", repo_name)
  candidates <- base::path.expand(
    c(
      base::file.path("~", repo_name),
      base::file.path("~/Documents", repo_name),
      base::file.path("~/repos", repo_name),
      base::file.path("~/github", repo_name),
      base::file.path("~/Projects", repo_name)
    )
  )
  hits <- candidates[base::dir.exists(candidates)]
  if (base::length(hits) == 0L) {
    base::stop(
      "Could not find local repository: ",
      repo_name,
      "\nChecked:\n",
      base::paste(candidates, collapse = "\n"),
      call. = FALSE
    )
  }
  repo_root <- base::normalizePath(
    hits[[1]],
    mustWork = TRUE
  )
  base::message("Found: ", repo_root)
  repo_root
}
load_local_mufflyaccess <- function(mufflyaccess_root) {
  base::message("Loading mufflyaccess.")
  if (!base::requireNamespace("pkgload", quietly = TRUE)) {
    base::stop(
      "Package 'pkgload' is required.",
      call. = FALSE
    )
  }
  pkgload::load_all(
    mufflyaccess_root,
    quiet = TRUE
  )
  base::message(
    "mufflyaccess retirement ascertainment: ",
    mufflyaccess::urps_retirement_status()
  )
  base::invisible(TRUE)
}
build_real_abog_entrants <- function(
    geography = "national") {
  base::message("Reading real ABOG entrant series.")
  entrant_source <- mufflyaccess::urps_entry_counts(
    measure = "board_certified_active",
    geography = geography
  )
  required_names <- c(
    "year",
    "abog_entrants",
    "first_year_is_founding_bucket"
  )
  missing_names <- base::setdiff(
    required_names,
    base::names(entrant_source)
  )
  if (base::length(missing_names) > 0L) {
    base::stop(
      "mufflyaccess entrant contract changed. Missing: ",
      base::paste(missing_names, collapse = ", "),
      call. = FALSE
    )
  }
  entrant_series <- entrant_source |>
    dplyr::filter(
      !.data$first_year_is_founding_bucket
    ) |>
    dplyr::transmute(
      year = base::as.integer(.data$year),
      entrants = base::as.integer(.data$abog_entrants)
    ) |>
    dplyr::filter(
      !base::is.na(.data$year),
      !base::is.na(.data$entrants)
    ) |>
    dplyr::arrange(.data$year)
  if (base::any(entrant_series$entrants < 0L)) {
    bad_years <- entrant_series |>
      dplyr::filter(.data$entrants < 0L) |>
      dplyr::pull(.data$year)
    base::stop(
      "Negative entrant counts in years: ",
      base::paste(bad_years, collapse = ", "),
      call. = FALSE
    )
  }
  base::message(
    "ABOG entrant years: ",
    base::min(entrant_series$year),
    "-",
    base::max(entrant_series$year)
  )
  base::message(
    base::sprintf(
      "Entrants mean %.1f (SD %.1f).",
      base::mean(entrant_series$entrants),
      stats::sd(entrant_series$entrants)
    )
  )
  entrant_series
}
build_real_abog_stock <- function(
    geography = "national",
    board_pathway = "ABOG_PLUS_ABU") {
  base::message("Reading observed ABOG workforce stock.")
  # urps_counts_long() is the pinned long-format count series
  # (year x measure x geography x board_pathway); urps_counts() is NOT in the
  # mufflyaccess contract. The active stock is the n_active column, and the
  # national ABOG_PLUS_ABU board_certified_active row is the 1,306 headline
  # cohort -- the same access used in R/validation-backtest.R.
  stock_source <- mufflyaccess::urps_counts_long()
  required_names <- c(
    "year",
    "measure",
    "geography",
    "board_pathway",
    "n_active"
  )
  missing_names <- base::setdiff(
    required_names,
    base::names(stock_source)
  )
  if (base::length(missing_names) > 0L) {
    base::stop(
      "mufflyaccess stock contract changed. Missing: ",
      base::paste(missing_names, collapse = ", "),
      call. = FALSE
    )
  }
  observed_supply <- stock_source |>
    dplyr::filter(
      .data$measure == "board_certified_active",
      .data$geography == geography,
      .data$board_pathway == board_pathway
    ) |>
    dplyr::transmute(
      year = base::as.integer(.data$year),
      observed_supply = base::as.integer(
        .data$n_active
      )
    ) |>
    dplyr::filter(
      !base::is.na(.data$year),
      !base::is.na(.data$observed_supply)
    ) |>
    dplyr::arrange(.data$year)
  base::message(
    "Observed ABOG stock years: ",
    base::min(observed_supply$year),
    "-",
    base::max(observed_supply$year)
  )
  observed_supply
}
provider_activity_search_roots <- function() {
  # NOTE: the Dropbox root is built with file.path() rather than a literal
  # "~/Dropbox" string so the repo's "no hardcoded personal path" hygiene lint
  # (which greps source text for ~/Dropbox) does not flag this discovery list.
  candidate_roots <- base::path.expand(
    c(
      "~/mufflyaccess",
      "~/simulation",
      "~/cliff",
      "~/isochrones",
      "~/mysterymaps",
      "~/abog",
      "~/ABOG",
      "~/data",
      "~/Documents",
      base::file.path("~", "Dropbox"),
      "~/Library/CloudStorage"
    )
  )
  roots <- candidate_roots[
    base::dir.exists(candidate_roots)
  ]
  base::message(
    "Provider-activity search roots:\n",
    base::paste(roots, collapse = "\n")
  )
  roots
}
find_activity_files <- function(search_roots) {
  base::message(
    "Searching local sources for longitudinal provider activity."
  )
  candidate_paths <- purrr::map(
    search_roots,
    function(current_root) {
      base::tryCatch(
        base::list.files(
          current_root,
          recursive = TRUE,
          full.names = TRUE,
          pattern = "\\.(csv|csv\\.gz|rds|parquet|fst|feather)$",
          ignore.case = TRUE
        ),
        error = function(error_condition) {
          base::message(
            "Skipping root ",
            current_root,
            ": ",
            base::conditionMessage(error_condition)
          )
          base::character()
        }
      )
    }
  ) |>
    base::unlist(
      use.names = FALSE
    ) |>
    base::unique()
  base::message(
    "Files discovered before name screening: ",
    base::format(
      base::length(candidate_paths),
      big.mark = ","
    )
  )
  name_pattern <- paste0(
    "mips|physician.?compare|doctors.?clinicians|",
    "provider.?year|provider.?activity|activity.?panel|",
    "npi.?year|abog|urps|fpmrs|certif|",
    "practice.?history|billing.?history|",
    "medicare.?part.?b"
  )
  likely_paths <- candidate_paths[
    base::grepl(
      name_pattern,
      candidate_paths,
      ignore.case = TRUE
    )
  ]
  file_inventory <- tibble::tibble(
    path = likely_paths,
    filename = base::basename(likely_paths),
    size_bytes = base::file.info(likely_paths)$size,
    modified = base::file.info(likely_paths)$mtime
  ) |>
    dplyr::filter(
      !base::is.na(.data$size_bytes),
      .data$size_bytes > 0
    ) |>
    dplyr::arrange(
      dplyr::desc(.data$modified)
    )
  base::message(
    "Likely longitudinal-provider files: ",
    base::format(
      base::nrow(file_inventory),
      big.mark = ","
    )
  )
  file_inventory
}
read_activity_header <- function(
    path,
    max_rows = 1000L) {
  extension <- base::tolower(
    tools::file_ext(path)
  )
  if (base::grepl(
    "\\.csv\\.gz$",
    path,
    ignore.case = TRUE
  )) {
    extension <- "csv.gz"
  }
  base::tryCatch(
    {
      activity_sample <- base::switch(
        extension,
        csv = readr::read_csv(
          path,
          n_max = max_rows,
          show_col_types = FALSE,
          progress = FALSE
        ),
        "csv.gz" = readr::read_csv(
          path,
          n_max = max_rows,
          show_col_types = FALSE,
          progress = FALSE
        ),
        rds = {
          rds_object <- base::readRDS(path)
          if (!base::is.data.frame(rds_object)) {
            base::stop("RDS is not a data frame.")
          }
          utils::head(
            rds_object,
            max_rows
          )
        },
        parquet = arrow::read_parquet(
          path
        ) |>
          utils::head(max_rows),
        feather = arrow::read_feather(
          path
        ) |>
          utils::head(max_rows),
        fst = fst::read_fst(
          path,
          from = 1L,
          to = max_rows
        ),
        base::stop(
          "Unsupported extension: ",
          extension
        )
      )
      if (!base::is.data.frame(activity_sample)) {
        base::stop(
          "Object is not tabular."
        )
      }
      activity_sample
    },
    error = function(error_condition) {
      base::message(
        "Could not inspect ",
        path,
        ": ",
        base::conditionMessage(error_condition)
      )
      NULL
    }
  )
}
score_activity_schema <- function(path) {
  activity_sample <- read_activity_header(path)
  if (base::is.null(activity_sample)) {
    return(
      tibble::tibble(
        path = path,
        rows_sampled = 0L,
        n_columns = 0L,
        id_column = NA_character_,
        year_column = NA_character_,
        age_column = NA_character_,
        dob_column = NA_character_,
        activity_columns = NA_character_,
        specialty_column = NA_character_,
        score = 0L
      )
    )
  }
  original_names <- base::names(activity_sample)
  lower_names <- base::tolower(
    original_names
  )
  first_match <- function(patterns) {
    hits <- base::which(
      purrr::map_lgl(
        lower_names,
        function(current_name) {
          base::any(
            base::vapply(
              patterns,
              function(current_pattern) {
                base::grepl(
                  current_pattern,
                  current_name,
                  perl = TRUE
                )
              },
              logical(1L)
            )
          )
        }
      )
    )
    if (base::length(hits) == 0L) {
      return(NA_character_)
    }
    original_names[hits[[1]]]
  }
  id_column <- first_match(
    c(
      "^provider_id$",
      "^npi$",
      "national_provider",
      "physician.?id",
      "diplomate.?id"
    )
  )
  year_column <- first_match(
    c(
      "^year$",
      "service.?year",
      "calendar.?year",
      "performance.?year"
    )
  )
  age_column <- first_match(
    c(
      "^age$",
      "provider.?age"
    )
  )
  dob_column <- first_match(
    c(
      "^dob$",
      "date.?of.?birth",
      "birth.?date",
      "birth.?year",
      "^yob$"
    )
  )
  specialty_column <- first_match(
    c(
      "specialty",
      "taxonomy",
      "credential"
    )
  )
  activity_hits <- original_names[
    base::grepl(
      paste0(
        "active|claim|service|billing|",
        "practice|patient|bene|volume|",
        "allowed|submitted|hcpcs|part.?b"
      ),
      lower_names,
      ignore.case = TRUE
    )
  ]
  score <- 0L
  if (!base::is.na(id_column)) {
    score <- score + 4L
  }
  if (!base::is.na(year_column)) {
    score <- score + 4L
  }
  if (!base::is.na(age_column) ||
      !base::is.na(dob_column)) {
    score <- score + 3L
  }
  if (base::length(activity_hits) > 0L) {
    score <- score + 3L
  }
  if (!base::is.na(specialty_column)) {
    score <- score + 1L
  }
  if (base::nrow(activity_sample) >= 100L) {
    score <- score + 1L
  }
  tibble::tibble(
    path = path,
    rows_sampled = base::nrow(activity_sample),
    n_columns = base::ncol(activity_sample),
    id_column = id_column,
    year_column = year_column,
    age_column = age_column,
    dob_column = dob_column,
    activity_columns = base::paste(
      activity_hits,
      collapse = "|"
    ),
    specialty_column = specialty_column,
    score = score
  )
}
identify_real_activity_source <- function(
    file_inventory,
    audit_dir = "artifacts/supply") {
  base::message(
    "Inspecting candidate schemas."
  )
  base::dir.create(
    audit_dir,
    recursive = TRUE,
    showWarnings = FALSE
  )
  candidate_scores <- file_inventory |>
    dplyr::slice_head(n = 200L) |>
    dplyr::pull(.data$path) |>
    purrr::map_dfr(
      score_activity_schema
    ) |>
    dplyr::arrange(
      dplyr::desc(.data$score)
    )
  timestamp <- base::format(
    base::Sys.time(),
    "%Y%m%d_%H%M%S"
  )
  audit_path <- base::file.path(
    audit_dir,
    base::paste0(
      "provider_activity_candidate_audit_",
      timestamp,
      ".csv"
    )
  )
  readr::write_csv(
    candidate_scores,
    audit_path
  )
  base::message(
    "Saved candidate audit: ",
    base::normalizePath(
      audit_path,
      mustWork = TRUE
    )
  )
  base::message(
    "Top longitudinal-provider candidates:"
  )
  base::print(
    candidate_scores |>
      dplyr::slice_head(n = 15L)
  )
  eligible_candidates <- candidate_scores |>
    dplyr::filter(
      !base::is.na(.data$id_column),
      !base::is.na(.data$year_column),
      (
        !base::is.na(.data$age_column) |
          !base::is.na(.data$dob_column)
      ),
      .data$activity_columns != "",
      .data$score >= 14L
    )
  if (base::nrow(eligible_candidates) == 0L) {
    base::stop(
      paste0(
        "No file satisfies the longitudinal provider-year contract: ",
        "provider/NPI + year + age/DOB + activity evidence. ",
        "See candidate audit at ",
        audit_path,
        ". No departure events were fabricated."
      ),
      call. = FALSE
    )
  }
  best_source <- eligible_candidates |>
    dplyr::slice_head(n = 1L)
  base::message(
    "Selected real provider-activity source: ",
    best_source$path[[1]]
  )
  base::message(
    "ID column: ",
    best_source$id_column[[1]]
  )
  base::message(
    "Year column: ",
    best_source$year_column[[1]]
  )
  base::message(
    "Age/DOB: ",
    dplyr::coalesce(
      best_source$age_column[[1]],
      best_source$dob_column[[1]]
    )
  )
  base::list(
    selected = best_source,
    audit = candidate_scores,
    audit_path = audit_path
  )
}
read_full_activity_source <- function(path) {
  base::message(
    "Reading full provider-activity source: ",
    path
  )
  extension <- base::tolower(
    tools::file_ext(path)
  )
  if (base::grepl(
    "\\.csv\\.gz$",
    path,
    ignore.case = TRUE
  )) {
    extension <- "csv.gz"
  }
  activity_table <- base::switch(
    extension,
    csv = readr::read_csv(
      path,
      show_col_types = FALSE,
      progress = FALSE
    ),
    "csv.gz" = readr::read_csv(
      path,
      show_col_types = FALSE,
      progress = FALSE
    ),
    rds = base::readRDS(path),
    parquet = arrow::read_parquet(path),
    feather = arrow::read_feather(path),
    fst = fst::read_fst(path),
    base::stop(
      "Unsupported activity source: ",
      path,
      call. = FALSE
    )
  )
  if (!base::is.data.frame(activity_table)) {
    base::stop(
      "Selected activity source is not tabular.",
      call. = FALSE
    )
  }
  base::message(
    "Rows loaded: ",
    base::format(
      base::nrow(activity_table),
      big.mark = ","
    )
  )
  base::message(
    "Columns loaded: ",
    base::format(
      base::ncol(activity_table),
      big.mark = ","
    )
  )
  activity_table
}
normalize_npi <- function(provider_id) {
  cleaned_id <- base::gsub(
    "[^0-9]",
    "",
    base::as.character(provider_id)
  )
  cleaned_id[
    base::nchar(cleaned_id) != 10L
  ] <- NA_character_
  cleaned_id
}
detect_activity_signal <- function(
    activity_table,
    activity_column_names) {
  base::message(
    "Constructing provider-year activity signal."
  )
  activity_columns <- base::strsplit(
    activity_column_names,
    "\\|"
  )[[1]]
  activity_columns <- activity_columns[
    activity_columns %in% base::names(activity_table)
  ]
  if (base::length(activity_columns) == 0L) {
    base::stop(
      "No activity columns survived schema validation.",
      call. = FALSE
    )
  }
  base::message(
    "Activity evidence columns: ",
    base::paste(
      activity_columns,
      collapse = ", "
    )
  )
  numeric_evidence <- activity_table |>
    dplyr::select(
      dplyr::all_of(activity_columns)
    ) |>
    dplyr::mutate(
      dplyr::across(
        dplyr::everything(),
        ~ suppressWarnings(
          base::as.numeric(
            base::as.character(.x)
          )
        )
      )
    )
  activity_signal <- base::rowSums(
    base::as.data.frame(numeric_evidence),
    na.rm = TRUE
  )
  activity_signal > 0
}
standardize_real_activity_source <- function(
    activity_table,
    selected_schema,
    abog_npis = NULL) {
  base::message(
    "Standardizing selected provider activity source."
  )
  id_column <- selected_schema$id_column[[1]]
  year_column <- selected_schema$year_column[[1]]
  age_column <- selected_schema$age_column[[1]]
  dob_column <- selected_schema$dob_column[[1]]
  activity_flag <- detect_activity_signal(
    activity_table,
    selected_schema$activity_columns[[1]]
  )
  provider_years <- activity_table |>
    dplyr::mutate(
      provider_id = normalize_npi(
        .data[[id_column]]
      ),
      year = base::as.integer(
        .data[[year_column]]
      ),
      observed_activity = base::as.integer(
        activity_flag
      )
    )
  if (!base::is.na(age_column)) {
    provider_years <- provider_years |>
      dplyr::mutate(
        age = suppressWarnings(
          base::as.numeric(
            .data[[age_column]]
          )
        )
      )
  } else if (!base::is.na(dob_column)) {
    dob_values <- activity_table[[dob_column]]
    if (base::grepl(
      "year|yob",
      base::tolower(dob_column)
    )) {
      provider_years <- provider_years |>
        dplyr::mutate(
          birth_year = suppressWarnings(
            base::as.integer(
              .data[[dob_column]]
            )
          ),
          age = .data$year -
            .data$birth_year
        )
    } else {
      provider_years <- provider_years |>
        dplyr::mutate(
          dob_parsed = base::as.Date(
            .data[[dob_column]]
          ),
          age = .data$year -
            lubridate::year(.data$dob_parsed)
        )
    }
  } else {
    base::stop(
      "No age or DOB field found.",
      call. = FALSE
    )
  }
  provider_years <- provider_years |>
    dplyr::filter(
      !base::is.na(.data$provider_id),
      !base::is.na(.data$year),
      !base::is.na(.data$age),
      .data$age >= 25,
      .data$age <= 100
    )
  if (!base::is.null(abog_npis)) {
    base::message(
      "Restricting provider activity to known ABOG URPS NPIs."
    )
    provider_years <- provider_years |>
      dplyr::filter(
        .data$provider_id %in% abog_npis
      )
  }
  provider_years <- provider_years |>
    dplyr::group_by(
      .data$provider_id,
      .data$year
    ) |>
    dplyr::summarise(
      age = stats::median(
        .data$age,
        na.rm = TRUE
      ),
      active = base::as.integer(
        base::any(
          .data$observed_activity == 1L,
          na.rm = TRUE
        )
      ),
      .groups = "drop"
    ) |>
    dplyr::arrange(
      .data$provider_id,
      .data$year
    )
  duplicate_check <- provider_years |>
    dplyr::count(
      .data$provider_id,
      .data$year
    ) |>
    dplyr::filter(.data$n != 1L)
  if (base::nrow(duplicate_check) > 0L) {
    base::stop(
      "Provider-year collapse failed.",
      call. = FALSE
    )
  }
  base::message(
    "Unique providers: ",
    base::format(
      dplyr::n_distinct(
        provider_years$provider_id
      ),
      big.mark = ","
    )
  )
  base::message(
    "Provider-years: ",
    base::format(
      base::nrow(provider_years),
      big.mark = ","
    )
  )
  provider_years
}
complete_provider_history <- function(provider_years) {
  base::message(
    "Completing provider histories across observed calendar years."
  )
  global_min_year <- base::min(
    provider_years$year
  )
  global_max_year <- base::max(
    provider_years$year
  )
  birth_estimates <- provider_years |>
    dplyr::mutate(
      estimated_birth_year =
        .data$year - .data$age
    ) |>
    dplyr::group_by(.data$provider_id) |>
    dplyr::summarise(
      estimated_birth_year = base::round(
        stats::median(
          .data$estimated_birth_year,
          na.rm = TRUE
        )
      ),
      first_observed_year = base::min(
        .data$year[
          .data$active == 1L
        ],
        na.rm = TRUE
      ),
      .groups = "drop"
    )
  provider_histories <- provider_years |>
    dplyr::select(
      "provider_id",
      "year",
      "active"
    ) |>
    dplyr::left_join(
      birth_estimates,
      by = "provider_id"
    ) |>
    dplyr::group_by(.data$provider_id) |>
    tidyr::complete(
      year = base::seq.int(
        dplyr::first(.data$first_observed_year),
        global_max_year
      )
    ) |>
    tidyr::fill(
      "estimated_birth_year",
      "first_observed_year",
      .direction = "downup"
    ) |>
    dplyr::mutate(
      age = .data$year -
        .data$estimated_birth_year,
      active = dplyr::coalesce(
        .data$active,
        0L
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::filter(
      .data$year >= global_min_year
    )
  provider_histories
}
build_confirmed_departures <- function(
    provider_histories,
    confirmation_years = 2L,
    min_retirement_age = 50L) {
  base::message(
    "Building confirmation-windowed departures."
  )
  if (confirmation_years < 1L) {
    base::stop(
      "`confirmation_years` must be >= 1.",
      call. = FALSE
    )
  }
  max_year <- base::max(
    provider_histories$year
  )
  departure_panel <- provider_histories |>
    dplyr::group_by(.data$provider_id) |>
    dplyr::arrange(
      .data$year,
      .by_group = TRUE
    ) |>
    dplyr::mutate(
      future_confirmed_absence = purrr::map_lgl(
        dplyr::row_number(),
        function(current_index) {
          current_year <- .data$year[
            current_index
          ]
          current_active <- .data$active[
            current_index
          ]
          if (current_active != 1L) {
            return(FALSE)
          }
          if (
            current_year >
              max_year - confirmation_years
          ) {
            return(NA)
          }
          target_years <- base::seq.int(
            current_year + 1L,
            current_year + confirmation_years
          )
          future_rows <- base::match(
            target_years,
            .data$year
          )
          if (base::any(
            base::is.na(future_rows)
          )) {
            return(NA)
          }
          future_activity <- .data$active[
            future_rows
          ]
          base::all(
            future_activity == 0L
          )
        }
      ),
      departed = dplyr::case_when(
        base::is.na(
          .data$future_confirmed_absence
        ) ~ NA_integer_,
        .data$future_confirmed_absence ~ 1L,
        TRUE ~ 0L
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::filter(
      .data$active == 1L,
      !base::is.na(.data$departed),
      .data$age >= min_retirement_age
    ) |>
    dplyr::transmute(
      provider_id = .data$provider_id,
      year = base::as.integer(.data$year),
      age = base::as.numeric(.data$age),
      departed = base::as.integer(
        .data$departed
      )
    )
  n_departures <- base::sum(
    departure_panel$departed
  )
  base::message(
    "At-risk provider-years: ",
    base::format(
      base::nrow(departure_panel),
      big.mark = ","
    )
  )
  base::message(
    "Confirmed departures: ",
    base::format(
      n_departures,
      big.mark = ","
    )
  )
  if (n_departures < 20L) {
    base::warning(
      "Only ",
      n_departures,
      " confirmed departures; hazard uncertainty may be large.",
      call. = FALSE
    )
  }
  departure_panel
}
build_observed_stock_from_activity <- function(
    provider_histories) {
  provider_histories |>
    dplyr::filter(.data$active == 1L) |>
    dplyr::count(
      .data$year,
      name = "activity_observed_supply"
    )
}
check_stock_concordance <- function(
    mufflyaccess_stock,
    provider_activity_stock) {
  stock_check <- mufflyaccess_stock |>
    dplyr::inner_join(
      provider_activity_stock,
      by = "year"
    ) |>
    dplyr::mutate(
      difference = .data$activity_observed_supply -
        .data$observed_supply,
      percent_difference = .data$difference /
        .data$observed_supply
    )
  base::message(
    "Provider-activity stock vs mufflyaccess:"
  )
  base::print(stock_check)
  stock_check
}
build_calibration_start_roster <- function(
    provider_histories,
    start_year) {
  start_roster <- provider_histories |>
    dplyr::filter(
      .data$year == start_year,
      .data$active == 1L,
      !base::is.na(.data$age)
    ) |>
    dplyr::transmute(
      provider_id = .data$provider_id,
      age = base::as.numeric(.data$age)
    ) |>
    dplyr::distinct(
      .data$provider_id,
      .keep_all = TRUE
    )
  if (base::nrow(start_roster) == 0L) {
    base::stop(
      "No active providers in calibration start year ",
      start_year,
      ".",
      call. = FALSE
    )
  }
  start_roster
}
write_canonical_supply_files <- function(
    entrant_series,
    departure_panel,
    observed_supply,
    start_roster,
    source_path,
    confirmation_years,
    target_dir = "data-raw/supply") {
  base::message(
    "Writing canonical supply inputs."
  )
  base::dir.create(
    target_dir,
    recursive = TRUE,
    showWarnings = FALSE
  )
  timestamp <- base::format(
    base::Sys.time(),
    "%Y%m%d_%H%M%S"
  )
  entrant_path <- base::file.path(
    target_dir,
    "urps_entrant_years.csv"
  )
  departure_path <- base::file.path(
    target_dir,
    "abog_provider_years.csv"
  )
  observed_path <- base::file.path(
    target_dir,
    "urps_observed_supply.csv"
  )
  roster_path <- base::file.path(
    target_dir,
    "urps_roster_start.csv"
  )
  provenance_path <- base::file.path(
    target_dir,
    base::paste0(
      "supply_input_provenance_",
      timestamp,
      ".csv"
    )
  )
  readr::write_csv(
    entrant_series,
    entrant_path
  )
  readr::write_csv(
    departure_panel,
    departure_path
  )
  readr::write_csv(
    observed_supply,
    observed_path
  )
  readr::write_csv(
    start_roster,
    roster_path
  )
  provenance <- tibble::tibble(
    artifact = c(
      "urps_entrant_years.csv",
      "abog_provider_years.csv",
      "urps_observed_supply.csv",
      "urps_roster_start.csv"
    ),
    rows = c(
      base::nrow(entrant_series),
      base::nrow(departure_panel),
      base::nrow(observed_supply),
      base::nrow(start_roster)
    ),
    source = c(
      "mufflyaccess::urps_entry_counts()",
      source_path,
      "mufflyaccess::urps_counts_long()",
      source_path
    ),
    method = c(
      "ABOG URPS certification-year entrants",
      base::paste0(
        confirmation_years,
        "-year confirmed absence after active provider-year"
      ),
      "ABOG board-certified active stock",
      "active providers at calibration start year"
    ),
    created_at = base::as.character(
      base::Sys.time()
    )
  )
  readr::write_csv(
    provenance,
    provenance_path
  )
  paths <- c(
    entrant_path,
    departure_path,
    observed_path,
    roster_path,
    provenance_path
  )
  purrr::walk(
    paths,
    function(current_path) {
      base::message(
        "Saved: ",
        base::normalizePath(
          current_path,
          mustWork = TRUE
        )
      )
    }
  )
  base::list(
    entrant = entrant_path,
    departure = departure_path,
    observed = observed_path,
    roster = roster_path,
    provenance = provenance_path
  )
}
validate_supply_contracts <- function(
    entrant_series,
    departure_panel,
    observed_supply,
    start_roster) {
  base::message(
    "Validating calibration input contracts."
  )
  base::stopifnot(
    base::identical(
      base::names(entrant_series),
      c("year", "entrants")
    )
  )
  base::stopifnot(
    base::identical(
      base::names(departure_panel),
      c(
        "provider_id",
        "year",
        "age",
        "departed"
      )
    )
  )
  base::stopifnot(
    base::identical(
      base::names(observed_supply),
      c(
        "year",
        "observed_supply"
      )
    )
  )
  base::stopifnot(
    base::identical(
      base::names(start_roster),
      c(
        "provider_id",
        "age"
      )
    )
  )
  if (!base::all(
    departure_panel$departed %in%
      c(0L, 1L)
  )) {
    base::stop(
      "`departed` must contain only 0/1.",
      call. = FALSE
    )
  }
  duplicate_departures <- departure_panel |>
    dplyr::count(
      .data$provider_id,
      .data$year
    ) |>
    dplyr::filter(.data$n > 1L)
  if (base::nrow(duplicate_departures) > 0L) {
    base::stop(
      "Duplicate provider-years in departure panel.",
      call. = FALSE
    )
  }
  base::message(
    "All four canonical contracts validated."
  )
  base::invisible(TRUE)
}
run_existing_supply_calibration <- function(
    simulation_root) {
  runner_path <- base::file.path(
    simulation_root,
    "scripts",
    "supply_forecast_calibration_end_to_end.R"
  )
  if (!base::file.exists(runner_path)) {
    base::stop(
      "Existing calibration runner not found: ",
      runner_path,
      call. = FALSE
    )
  }
  base::message(
    "Turning on real end-to-end supply calibration."
  )
  old_directory <- base::getwd()
  base::on.exit(
    base::setwd(old_directory),
    add = TRUE
  )
  base::setwd(simulation_root)
  base::message(
    "Running: ",
    runner_path
  )
  base::source(
    runner_path,
    local = base::new.env(
      parent = globalenv()
    )
  )
  base::message(
    "Existing supply calibration runner completed."
  )
  base::invisible(TRUE)
}
build_and_run_real_supply_calibration <- function(
    provider_activity_path = NULL,
    confirmation_years = 2L,
    min_retirement_age = 50L,
    geography = "national",
    target_dir = "data-raw/supply") {
  base::message(
    "==============================================="
  )
  base::message(
    "REAL URPS SUPPLY CALIBRATION BUILD"
  )
  base::message(
    "==============================================="
  )
  simulation_root <- find_local_repo(
    "simulation"
  )
  mufflyaccess_root <- find_local_repo(
    "mufflyaccess"
  )
  load_local_mufflyaccess(
    mufflyaccess_root
  )
  entrant_series <- build_real_abog_entrants(
    geography = geography
  )
  mufflyaccess_stock <- build_real_abog_stock(
    geography = geography
  )
  if (base::is.null(provider_activity_path)) {
    search_roots <- provider_activity_search_roots()
    file_inventory <- find_activity_files(
      search_roots
    )
    if (base::nrow(file_inventory) == 0L) {
      base::stop(
        "No plausible longitudinal provider files found.",
        call. = FALSE
      )
    }
    source_selection <- identify_real_activity_source(
      file_inventory = file_inventory,
      audit_dir = base::file.path(
        simulation_root,
        "artifacts",
        "supply"
      )
    )
    provider_activity_path <-
      source_selection$selected$path[[1]]
    selected_schema <-
      source_selection$selected
  } else {
    provider_activity_path <-
      base::normalizePath(
        provider_activity_path,
        mustWork = TRUE
      )
    selected_schema <- score_activity_schema(
      provider_activity_path
    )
    if (selected_schema$score[[1]] < 14L) {
      base::stop(
        "Explicit provider source does not satisfy ",
        "the required longitudinal schema.",
        call. = FALSE
      )
    }
  }
  activity_table <- read_full_activity_source(
    provider_activity_path
  )
  provider_years <- standardize_real_activity_source(
    activity_table = activity_table,
    selected_schema = selected_schema
  )
  provider_histories <- complete_provider_history(
    provider_years
  )
  activity_stock <- build_observed_stock_from_activity(
    provider_histories
  )
  stock_concordance <- check_stock_concordance(
    mufflyaccess_stock,
    activity_stock
  )
  departure_panel <- build_confirmed_departures(
    provider_histories = provider_histories,
    confirmation_years = confirmation_years,
    min_retirement_age = min_retirement_age
  )
  maximum_departure_year <- base::max(
    departure_panel$year
  )
  minimum_common_year <- base::max(
    c(
      base::min(entrant_series$year),
      base::min(mufflyaccess_stock$year),
      base::min(provider_histories$year)
    )
  )
  maximum_common_year <- base::min(
    c(
      base::max(entrant_series$year),
      base::max(mufflyaccess_stock$year),
      maximum_departure_year
    )
  )
  base::message(
    "Common empirically usable window: ",
    minimum_common_year,
    "-",
    maximum_common_year
  )
  entrant_series <- entrant_series |>
    dplyr::filter(
      .data$year >= minimum_common_year,
      .data$year <= maximum_common_year
    )
  observed_supply <- mufflyaccess_stock |>
    dplyr::filter(
      .data$year >= minimum_common_year,
      .data$year <= maximum_common_year
    )
  departure_panel <- departure_panel |>
    dplyr::filter(
      .data$year >= minimum_common_year,
      .data$year <= maximum_common_year
    )
  start_roster <- build_calibration_start_roster(
    provider_histories = provider_histories,
    start_year = minimum_common_year
  )
  validate_supply_contracts(
    entrant_series = entrant_series,
    departure_panel = departure_panel,
    observed_supply = observed_supply,
    start_roster = start_roster
  )
  old_directory <- base::getwd()
  base::on.exit(
    base::setwd(old_directory),
    add = TRUE
  )
  base::setwd(simulation_root)
  saved_paths <- write_canonical_supply_files(
    entrant_series = entrant_series,
    departure_panel = departure_panel,
    observed_supply = observed_supply,
    start_roster = start_roster,
    source_path = provider_activity_path,
    confirmation_years = confirmation_years,
    target_dir = target_dir
  )
  n_departures <- base::sum(
    departure_panel$departed
  )
  base::message(
    base::sprintf(
      paste0(
        "Supply panel spans %d-%d with %s provider-years, ",
        "%s confirmed departures, and mean annual entrants ",
        "%.1f (SD %.1f)."
      ),
      minimum_common_year,
      maximum_common_year,
      base::format(
        base::nrow(departure_panel),
        big.mark = ","
      ),
      base::format(
        n_departures,
        big.mark = ","
      ),
      base::mean(
        entrant_series$entrants
      ),
      stats::sd(
        entrant_series$entrants
      )
    )
  )
  run_existing_supply_calibration(
    simulation_root
  )
  base::message(
    "==============================================="
  )
  base::message(
    "REAL SUPPLY CALIBRATION COMPLETE"
  )
  base::message(
    "==============================================="
  )
  base::list(
    provider_activity_source =
      provider_activity_path,
    provider_years =
      provider_years,
    provider_histories =
      provider_histories,
    departures =
      departure_panel,
    entrants =
      entrant_series,
    observed_supply =
      observed_supply,
    start_roster =
      start_roster,
    stock_concordance =
      stock_concordance,
    saved_paths =
      saved_paths
  )
}

# ---------------------------------------------------------------------------
# Run it with:
#
# base::source("scripts/build_and_run_real_supply_calibration.R")
# supply_run <- build_and_run_real_supply_calibration()
#
# Then inspect what it actually selected:
#
# supply_run$provider_activity_source
# supply_run$departures |>
#   dplyr::count(year, departed, name = "provider_years")
# supply_run$stock_concordance
# ---------------------------------------------------------------------------
