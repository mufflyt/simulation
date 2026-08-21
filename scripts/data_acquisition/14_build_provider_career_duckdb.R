#!/usr/bin/env Rscript
# Build the public provider-career evidence DuckDB.
#
# Usage from an R session:
# source("scripts/data_acquisition/14_build_provider_career_duckdb.R")
# build_public_provider_career_duckdb(
#   database_path = "data-raw/provider_career/provider_career.duckdb",
#   identity_path = "data-raw/provider_career/provider_identity.csv"
# )

`%||%` <- function(left_value, right_value) {
  if (base::is.null(left_value)) right_value else left_value
}

normalize_column_names <- function(column_names) {
  normalized_names <- base::tolower(column_names)
  normalized_names <- base::gsub(
    "[^a-z0-9]+",
    "_",
    normalized_names
  )
  base::gsub("^_|_$", "", normalized_names)
}

download_public_source <- function(url, destination_path) {
  base::message(
    "download_public_source(): url=", url,
    ", destination=", destination_path
  )
  base::dir.create(
    base::dirname(destination_path),
    recursive = TRUE,
    showWarnings = FALSE
  )
  temporary_path <- base::paste0(destination_path, ".partial")
  request <- httr2::request(url) |>
    httr2::req_user_agent("urpssim/0.5.0 provider-career research") |>
    httr2::req_retry(max_tries = 4L) |>
    httr2::req_timeout(600)
  httr2::req_perform(
    request,
    path = temporary_path
  )
  base::file.rename(temporary_path, destination_path)
  base::message(
    "download_public_source(): saved bytes=",
    scales::comma(base::file.info(destination_path)$size)
  )
  destination_path
}

discover_cms_distribution <- function(title_pattern, return_all = FALSE) {
  base::message(
    "discover_cms_distribution(): title_pattern=", title_pattern
  )
  catalog_request <- httr2::request("https://data.cms.gov/data.json") |>
    httr2::req_user_agent("urpssim/0.5.0 provider-career research") |>
    httr2::req_retry(max_tries = 4L) |>
    httr2::req_timeout(120)
  catalog_payload <- httr2::req_perform(catalog_request) |>
    httr2::resp_body_json(simplifyVector = FALSE)
  catalog_datasets <- catalog_payload$dataset
  matching_datasets <- base::Filter(
    function(dataset_item) {
      base::grepl(
        title_pattern,
        dataset_item$title %||% "",
        ignore.case = TRUE
      )
    },
    catalog_datasets
  )
  if (base::length(matching_datasets) == 0L) {
    base::stop(
      "No CMS catalog dataset matched: ", title_pattern,
      call. = FALSE
    )
  }
  candidate_rows <- base::do.call(
    base::rbind,
    base::lapply(matching_datasets, function(dataset_item) {
      distributions <- dataset_item$distribution %||% base::list()
      base::do.call(
        base::rbind,
        base::lapply(distributions, function(distribution_item) {
          base::data.frame(
            title = dataset_item$title %||% NA_character_,
            modified = dataset_item$modified %||% NA_character_,
            format = distribution_item$format %||% NA_character_,
            download_url = distribution_item$downloadURL %||%
              distribution_item$accessURL %||% NA_character_,
            stringsAsFactors = FALSE
          )
        })
      )
    })
  )
  candidate_table <- tibble::as_tibble(candidate_rows) |>
    dplyr::filter(!base::is.na(.data$download_url)) |>
    dplyr::mutate(
      csv_like = base::grepl(
        "csv|zip",
        .data$format,
        ignore.case = TRUE
      ),
      modified_time = base::as.POSIXct(
        .data$modified,
        format = "%Y-%m-%dT%H:%M:%S",
        tz = "UTC"
      )
    ) |>
    dplyr::arrange(
      dplyr::desc(.data$csv_like),
      dplyr::desc(.data$modified_time)
    ) |>
    dplyr::filter(.data$csv_like) |>
    dplyr::distinct(.data$download_url, .keep_all = TRUE)
  if (base::nrow(candidate_table) == 0L) {
    base::stop(
      "CMS dataset had no downloadable distribution: ", title_pattern,
      call. = FALSE
    )
  }
  selected_distribution <- if (base::isTRUE(return_all)) {
    candidate_table
  } else {
    candidate_table |>
      dplyr::slice(1L)
  }
  base::message(
    "discover_cms_distribution(): selected distributions=",
    scales::comma(base::nrow(selected_distribution))
  )
  selected_distribution
}

read_delimited_public_file <- function(file_path) {
  base::message("read_delimited_public_file(): path=", file_path)
  extension <- base::tolower(tools::file_ext(file_path))
  if (extension == "zip") {
    archive_members <- utils::unzip(file_path, list = TRUE)
    delimited_members <- archive_members$Name[
      base::grepl("\\.(csv|txt)$", archive_members$Name, ignore.case = TRUE)
    ]
    if (base::length(delimited_members) == 0L) {
      base::stop("ZIP contains no CSV or TXT file.", call. = FALSE)
    }
    extraction_directory <- base::tempfile("career_source_")
    base::dir.create(extraction_directory)
    utils::unzip(
      file_path,
      files = delimited_members[[1L]],
      exdir = extraction_directory
    )
    file_path <- base::file.path(
      extraction_directory,
      delimited_members[[1L]]
    )
  }
  source_rows <- readr::read_delim(
    file_path,
    delim = if (base::tolower(tools::file_ext(file_path)) == "txt") {
      "\t"
    } else {
      ","
    },
    show_col_types = FALSE,
    progress = FALSE,
    guess_max = 100000L
  )
  base::names(source_rows) <- normalize_column_names(base::names(source_rows))
  base::message(
    "read_delimited_public_file(): rows=",
    scales::comma(base::nrow(source_rows)),
    ", columns=", base::ncol(source_rows)
  )
  source_rows
}

first_existing_column <- function(source_rows, candidates, required = TRUE) {
  matched_columns <- base::intersect(candidates, base::names(source_rows))
  if (base::length(matched_columns) > 0L) {
    return(source_rows[[matched_columns[[1L]]]])
  }
  if (base::isTRUE(required)) {
    base::stop(
      "None of the expected columns exists: ",
      base::paste(candidates, collapse = ", "),
      call. = FALSE
    )
  }
  base::rep(NA, base::nrow(source_rows))
}

normalize_cms_opt_out <- function(source_rows) {
  normalized_rows <- tibble::tibble(
    npi = base::as.character(first_existing_column(
      source_rows,
      base::c("npi", "national_provider_identifier")
    )),
    effective_date = base::as.Date(base::as.character(first_existing_column(
      source_rows,
      base::c("effective_date", "opt_out_effective_date")
    ))),
    end_date = base::as.Date(base::as.character(first_existing_column(
      source_rows,
      base::c("end_date", "opt_out_end_date"),
      required = FALSE
    ))),
    specialty = base::as.character(first_existing_column(
      source_rows,
      base::c("specialty", "provider_type"),
      required = FALSE
    ))
  ) |>
    dplyr::mutate(
      source_year = base::as.integer(base::format(
        .data$effective_date,
        "%Y"
      ))
    )
  normalized_rows
}

normalization_year <- function(date_values) {
  base::as.integer(base::format(date_values, "%Y"))
}

normalize_cms_pecos <- function(source_rows) {
  normalized_rows <- tibble::tibble(
    npi = base::as.character(first_existing_column(
      source_rows,
      base::c("npi", "national_provider_identifier")
    )),
    enrollment_id = base::as.character(first_existing_column(
      source_rows,
      base::c("enrollment_id", "enrlmt_id"),
      required = FALSE
    )),
    enrollment_type = base::as.character(first_existing_column(
      source_rows,
      base::c(
        "enrollment_type", "provider_enrollment_type",
        "provider_type_cd", "provider_type_desc"
      ),
      required = FALSE
    )),
    specialty = base::as.character(first_existing_column(
      source_rows,
      base::c(
        "specialty", "provider_type", "provider_type_desc"
      ),
      required = FALSE
    )),
    organization_name = base::as.character(first_existing_column(
      source_rows,
      base::c(
        "organization_name", "org_name", "legal_business_name"
      ),
      required = FALSE
    )),
    state = base::as.character(first_existing_column(
      source_rows,
      base::c("state", "adr_state", "state_cd"),
      required = FALSE
    )),
    enrollment_date = base::as.Date(base::as.character(first_existing_column(
      source_rows,
      base::c("enrollment_date", "enrlmt_dt"),
      required = FALSE
    )))
  ) |>
    dplyr::mutate(
      source_year = base::as.integer(base::format(base::Sys.Date(), "%Y"))
    )
  normalized_rows
}

normalize_cms_part_d <- function(source_rows) {
  source_year_values <- first_existing_column(
    source_rows,
    base::c("year", "source_year"),
    required = FALSE
  )
  source_year_values[base::is.na(source_year_values)] <-
    base::as.integer(base::format(base::Sys.Date(), "%Y"))
  tibble::tibble(
    npi = base::as.character(first_existing_column(
      source_rows,
      base::c("prscrbr_npi", "npi")
    )),
    source_year = base::as.integer(source_year_values),
    total_claim_count = base::as.numeric(first_existing_column(
      source_rows,
      base::c("tot_clms", "total_claim_count")
    )),
    total_30_day_fills = base::as.numeric(first_existing_column(
      source_rows,
      base::c("tot_30day_fills", "total_30_day_fills"),
      required = FALSE
    )),
    total_drug_cost = base::as.numeric(first_existing_column(
      source_rows,
      base::c("tot_drug_cst", "total_drug_cost"),
      required = FALSE
    ))
  )
}

normalize_cms_revoked <- function(source_rows) {
  tibble::tibble(
    npi = base::as.character(first_existing_column(
      source_rows,
      base::c("npi", "national_provider_identifier")
    )),
    revocation_date = base::as.Date(base::as.character(first_existing_column(
      source_rows,
      base::c("revocation_date", "revctn_dt")
    ))),
    reinstatement_date = base::as.Date(base::as.character(
      first_existing_column(
      source_rows,
      base::c(
        "reinstatement_date", "reinstatement_dt", "rinstmt_dt"
      ),
      required = FALSE
      )
    )),
    revocation_reason = base::as.character(first_existing_column(
      source_rows,
      base::c("revocation_reason", "revctn_reason"),
      required = FALSE
    )),
    state = base::as.character(first_existing_column(
      source_rows,
      base::c("state", "adr_state"),
      required = FALSE
    ))
  )
}

normalize_irs_form_990 <- function(source_rows) {
  tibble::tibble(
    normalized_name = stringr::str_squish(base::tolower(
      base::as.character(first_existing_column(
        source_rows,
        base::c("person_name", "officer_name", "name_person")
      ))
    )),
    organization_name = stringr::str_squish(base::tolower(
      base::as.character(first_existing_column(
        source_rows,
        base::c("organization_name", "business_name", "org_name")
      ))
    )),
    organization_ein = base::as.character(first_existing_column(
      source_rows,
      base::c("ein", "organization_ein")
    )),
    tax_year = base::as.integer(first_existing_column(
      source_rows,
      base::c("tax_year", "tax_period", "year")
    )),
    role_title = base::as.character(first_existing_column(
      source_rows,
      base::c("title", "role_title", "officer_title")
    )),
    compensation = base::as.numeric(first_existing_column(
      source_rows,
      base::c("compensation", "reportable_comp_from_org"),
      required = FALSE
    ))
  )
}

orcid_year_value <- function(date_structure) {
  year_value <- date_structure$year$value %||% NA_character_
  base::as.integer(year_value)
}

fetch_orcid_affiliations <- function(identities, access_token) {
  if (!base::nzchar(access_token)) {
    base::message(
      "fetch_orcid_affiliations(): ORCID token absent; source unavailable"
    )
    return(tibble::tibble(
      orcid = character(),
      organization_name = character(),
      start_year = integer(),
      end_year = integer(),
      role_title = character(),
      affiliation_type = character()
    ))
  }
  verified_orcids <- identities |>
    dplyr::filter(
      .data$identity_verified,
      !base::is.na(.data$orcid),
      base::nzchar(.data$orcid)
    ) |>
    dplyr::distinct(.data$orcid)
  base::message(
    "fetch_orcid_affiliations(): verified ORCIDs=",
    scales::comma(base::nrow(verified_orcids))
  )
  affiliation_rows <- base::list()
  for (orcid_value in verified_orcids$orcid) {
    for (endpoint_name in base::c("employments", "educations")) {
      request <- httr2::request(base::paste0(
        "https://pub.orcid.org/v3.0/", orcid_value, "/", endpoint_name
      )) |>
        httr2::req_headers(
          Accept = "application/vnd.orcid+json",
          Authorization = base::paste("Bearer", access_token)
        ) |>
        httr2::req_retry(max_tries = 3L) |>
        httr2::req_timeout(60)
      response <- httr2::req_perform(request)
      payload <- httr2::resp_body_json(
        response,
        simplifyVector = FALSE
      )
      group_name <- base::paste0(
        base::sub("s$", "", endpoint_name),
        "-summary"
      )
      groups <- payload$`affiliation-group` %||% base::list()
      for (affiliation_group in groups) {
        summaries <- affiliation_group$summaries %||% base::list()
        for (summary_wrapper in summaries) {
          summary_item <- summary_wrapper[[group_name]]
          if (base::is.null(summary_item)) {
            next
          }
          affiliation_rows[[base::length(affiliation_rows) + 1L]] <-
            tibble::tibble(
              orcid = orcid_value,
              organization_name = summary_item$organization$name %||%
                NA_character_,
              start_year = orcid_year_value(summary_item$`start-date`),
              end_year = orcid_year_value(summary_item$`end-date`),
              role_title = summary_item$`role-title` %||% NA_character_,
              affiliation_type = endpoint_name
            )
        }
      }
    }
  }
  affiliation_table <- dplyr::bind_rows(affiliation_rows)
  base::message(
    "fetch_orcid_affiliations(): affiliation rows=",
    scales::comma(base::nrow(affiliation_table))
  )
  affiliation_table
}

fetch_clinical_trials <- function(
    query_text,
    maximum_studies = 5000L) {
  base::message(
    "fetch_clinical_trials(): query=", query_text,
    ", maximum_studies=", scales::comma(maximum_studies)
  )
  page_token <- NULL
  study_rows <- base::list()
  total_studies <- 0L
  repeat {
    request <- httr2::request(
      "https://clinicaltrials.gov/api/v2/studies"
    ) |>
      httr2::req_url_query(
        `query.term` = query_text,
        pageSize = 100L,
        pageToken = page_token,
        format = "json"
      ) |>
      httr2::req_retry(max_tries = 4L) |>
      httr2::req_timeout(120)
    payload <- httr2::req_perform(request) |>
      httr2::resp_body_json(simplifyVector = FALSE)
    for (study_item in payload$studies %||% base::list()) {
      protocol <- study_item$protocolSection
      identification <- protocol$identificationModule
      status <- protocol$statusModule
      contacts <- protocol$contactsLocationsModule
      officials <- contacts$overallOfficials %||% base::list()
      if (base::length(officials) == 0L) {
        next
      }
      for (official in officials) {
        official_name <- official$name %||% NA_character_
        organization_name <- stringr::str_squish(base::tolower(
          official$affiliation %||% NA_character_
        ))
        start_year <- base::as.integer(base::substr(
          status$startDateStruct$date %||% NA_character_,
          1L,
          4L
        ))
        end_year <- base::as.integer(base::substr(
          status$completionDateStruct$date %||% NA_character_,
          1L,
          4L
        ))
        if (base::is.na(end_year)) {
          end_year <- base::as.integer(base::format(base::Sys.Date(), "%Y"))
        }
        if (base::is.na(start_year)) {
          next
        }
        end_year <- base::max(start_year, end_year)
        study_rows[[base::length(study_rows) + 1L]] <- tibble::tibble(
          normalized_name = stringr::str_squish(
            base::tolower(official_name)
          ),
          organization_name = organization_name,
          nct_id = identification$nctId %||% NA_character_,
          source_year = base::seq.int(start_year, end_year),
          investigator_role = official$role %||% NA_character_,
          overall_status = status$overallStatus %||% NA_character_
        )
      }
    }
    total_studies <- total_studies + base::length(payload$studies)
    page_token <- payload$nextPageToken %||% NULL
    if (base::is.null(page_token) || total_studies >= maximum_studies) {
      break
    }
  }
  trial_rows <- dplyr::bind_rows(study_rows)
  base::message(
    "fetch_clinical_trials(): investigator rows=",
    scales::comma(base::nrow(trial_rows))
  )
  trial_rows
}

load_provider_identity_seed <- function(identity_path) {
  if (base::file.exists(identity_path)) {
    base::message(
      "load_provider_identity_seed(): reading verified crosswalk=",
      identity_path
    )
    return(readr::read_csv(identity_path, show_col_types = FALSE))
  }
  activity_path <- base::file.path(
    "inst", "extdata", "provider_year", "provider_year_activity_long.csv"
  )
  if (!base::file.exists(activity_path)) {
    base::stop(
      "Neither identity crosswalk nor provider-year activity exists.",
      call. = FALSE
    )
  }
  base::message(
    "load_provider_identity_seed(): crosswalk absent; seeding from ",
    activity_path
  )
  identity_rows <- readr::read_csv(
    activity_path,
    show_col_types = FALSE,
    col_select = "npi"
  ) |>
    dplyr::distinct(.data$npi) |>
    dplyr::transmute(
      provider_id = base::as.character(.data$npi),
      npi = base::as.character(.data$npi),
      orcid = NA_character_,
      normalized_name = NA_character_,
      organization_name = NA_character_,
      identity_tier = 1L,
      identity_verified = TRUE
    )
  base::message(
    "load_provider_identity_seed(): seeded NPIs=",
    scales::comma(base::nrow(identity_rows)),
    "; name-based sources remain unavailable"
  )
  identity_rows
}

build_public_provider_career_duckdb <- function(
    database_path,
    identity_path = "data-raw/provider_career/provider_identity.csv",
    download_directory = "data-raw/provider_career/downloads",
    download_cms = TRUE,
    download_trials = TRUE,
    irs_990_url = base::Sys.getenv("IRS_FORM_990_OFFICERS_URL", ""),
    irs_990_path = base::Sys.getenv("IRS_FORM_990_OFFICERS_PATH", ""),
    orcid_access_token = base::Sys.getenv("ORCID_ACCESS_TOKEN", ""),
    clinical_trial_query = base::paste(
      "pelvic organ prolapse OR urinary incontinence OR",
      "overactive bladder OR fecal incontinence"
    )) {
  base::message(
    "build_public_provider_career_duckdb(): database_path=", database_path
  )
  base::dir.create(
    base::dirname(database_path),
    recursive = TRUE,
    showWarnings = FALSE
  )
  connection <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = database_path,
    read_only = FALSE
  )
  base::on.exit(
    DBI::dbDisconnect(connection, shutdown = TRUE),
    add = TRUE
  )
  initialize_provider_career_duckdb(connection)
  identities <- load_provider_identity_seed(identity_path)
  register_provider_career_identities(
    connection = connection,
    identities = identities,
    overwrite = TRUE
  )

  cms_specs <- tibble::tribble(
    ~source_id, ~title_pattern, ~normalizer, ~all_distributions,
    "cms_opt_out", "Opt.Out Affidavits", normalize_cms_opt_out, FALSE,
    "cms_pecos", "Fee.for.Service Public Provider Enrollment",
    normalize_cms_pecos, FALSE,
    "cms_part_d", "Part D Prescribers.by Provider$",
    normalize_cms_part_d, TRUE,
    "cms_revoked", "Revoked Medicare Providers", normalize_cms_revoked,
    FALSE
  )
  if (base::isTRUE(download_cms)) {
    for (specification_index in base::seq_len(base::nrow(cms_specs))) {
      specification <- cms_specs[specification_index, ]
      distributions <- discover_cms_distribution(
        specification$title_pattern[[1L]],
        return_all = specification$all_distributions[[1L]]
      )
      for (distribution_index in base::seq_len(
        base::nrow(distributions)
      )) {
        distribution <- distributions[distribution_index, ]
        source_url <- distribution$download_url[[1L]]
        extension <- if (base::grepl("\\.zip($|\\?)", source_url)) {
          "zip"
        } else {
          "csv"
        }
        local_path <- base::file.path(
          download_directory,
          base::paste0(
            specification$source_id[[1L]], "_",
            base::format(base::Sys.time(), "%Y%m%d_%H%M%S"),
            "_", distribution_index, ".", extension
          )
        )
        download_public_source(source_url, local_path)
        source_rows <- read_delimited_public_file(local_path)
        if (specification$source_id[[1L]] == "cms_part_d" &&
            !base::any(base::c("year", "source_year") %in%
              base::names(source_rows))) {
          year_text <- base::paste(
            distribution$title[[1L]],
            source_url
          )
          source_year <- stringr::str_extract(
            year_text,
            "(19|20)[0-9]{2}"
          )
          if (base::is.na(source_year)) {
            base::stop(
              "Part D distribution has no identifiable data year: ",
              source_url,
              call. = FALSE
            )
          }
          source_rows$source_year <- base::as.integer(source_year)
        }
        normalized_rows <- specification$normalizer[[1L]](source_rows)
        ingest_provider_career_source(
          connection = connection,
          source_id = specification$source_id[[1L]],
          source_rows = normalized_rows,
          local_path = local_path,
          source_url = source_url,
          replace_source = distribution_index == 1L
        )
      }
    }
  }

  if (base::isTRUE(download_trials)) {
    trial_rows <- fetch_clinical_trials(clinical_trial_query)
    ingest_provider_career_source(
      connection = connection,
      source_id = "clinical_trials",
      source_rows = trial_rows,
      source_url = "https://clinicaltrials.gov/api/v2/studies"
    )
  }

  if (base::nzchar(irs_990_url) && !base::nzchar(irs_990_path)) {
    irs_990_path <- base::file.path(
      download_directory,
      base::paste0(
        "irs_form_990_officers_",
        base::format(base::Sys.time(), "%Y%m%d_%H%M%S"),
        ".csv"
      )
    )
    download_public_source(irs_990_url, irs_990_path)
  }
  if (base::nzchar(irs_990_path)) {
    irs_source_rows <- read_delimited_public_file(irs_990_path)
    irs_rows <- normalize_irs_form_990(irs_source_rows)
    ingest_provider_career_source(
      connection = connection,
      source_id = "irs_form_990",
      source_rows = irs_rows,
      local_path = irs_990_path,
      source_url = irs_990_url
    )
  } else {
    base::message(
      "build_public_provider_career_duckdb(): IRS officer extract absent; ",
      "source remains unavailable, not zero"
    )
  }

  orcid_rows <- fetch_orcid_affiliations(
    identities = identities,
    access_token = orcid_access_token
  )
  if (base::nrow(orcid_rows) > 0L) {
    ingest_provider_career_source(
      connection = connection,
      source_id = "orcid",
      source_rows = orcid_rows,
      source_url = "https://pub.orcid.org/v3.0/"
    )
  }

  source_audit <- audit_provider_career_sources(connection)
  audit_path <- base::file.path(
    base::dirname(database_path),
    base::paste0(
      "provider_career_source_audit_",
      base::format(base::Sys.time(), "%Y%m%d_%H%M%S"),
      ".csv"
    )
  )
  readr::write_csv(source_audit, audit_path)
  base::message(
    "build_public_provider_career_duckdb(): saved database=",
    base::normalizePath(database_path, mustWork = FALSE)
  )
  base::message(
    "build_public_provider_career_duckdb(): saved audit=",
    base::normalizePath(audit_path, mustWork = FALSE)
  )
  base::invisible(source_audit)
}
