# DuckDB evidence pipeline for endogenous APP skill mix ----------------------

.app_sql_name <- function(connection, name) {
  base::as.character(DBI::dbQuoteIdentifier(connection, name))
}

.app_sql_string <- function(connection, value) {
  base::as.character(DBI::dbQuoteString(connection, value))
}

.app_resolve_column <- function(columns, candidates, required = TRUE) {
  matched <- columns[base::tolower(columns) %in% base::tolower(candidates)]
  if (base::length(matched) > 0L) return(matched[[1]])
  if (required) {
    base::stop("Required column not found. Tried: ", base::paste(candidates, collapse = ", "), ".")
  }
  NA_character_
}

.app_table_columns <- function(connection, schema, table) {
  query <- "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position"
  DBI::dbGetQuery(connection, query, params = list(schema, table)) |> dplyr::pull(.data$column_name)
}

.app_assert_identifier <- function(value, argument) {
  if (!base::is.character(value) || base::length(value) != 1L || !base::grepl("^[A-Za-z][A-Za-z0-9_]*$", value)) {
    base::stop("`", argument, "` is not a safe SQL identifier.")
  }
  base::invisible(value)
}

.app_provider_case_sql <- function(provider_expression) {
  base::paste0(
    "CASE WHEN lower(", provider_expression, ") LIKE '%nurse practitioner%' THEN 'nurse_practitioner' ",
    "WHEN lower(", provider_expression, ") LIKE '%physician assistant%' THEN 'physician_assistant' ",
    "WHEN lower(", provider_expression, ") LIKE '%clinical nurse%' THEN 'clinical_nurse_specialist' ",
    "WHEN lower(", provider_expression, ") LIKE '%certified nurse midwife%' THEN 'certified_nurse_midwife' ",
    "WHEN lower(", provider_expression, ") LIKE '%urology%' THEN 'urology_physician' ",
    "WHEN lower(", provider_expression, ") LIKE '%obstetrics%' OR lower(", provider_expression, ") LIKE '%gynecology%' THEN 'obgyn_physician' ",
    "ELSE 'other_provider' END"
  )
}

.app_taxonomy_case_sql <- function(taxonomy_expression) {
  base::paste0(
    "CASE WHEN ", taxonomy_expression, " LIKE '%207VF0040X%' THEN 'fpmrs_obgyn' ",
    "WHEN ", taxonomy_expression, " LIKE '%2088F0040X%' THEN 'fpmrs_urology' ",
    "WHEN ", taxonomy_expression, " LIKE '%363L%' THEN 'nurse_practitioner' ",
    "WHEN ", taxonomy_expression, " LIKE '%363A00000X%' THEN 'physician_assistant' ",
    "WHEN ", taxonomy_expression, " LIKE '%364S%' THEN 'clinical_nurse_specialist' ",
    "WHEN ", taxonomy_expression, " LIKE '%367A00000X%' THEN 'certified_nurse_midwife' ",
    "WHEN ", taxonomy_expression, " LIKE '%207V%' THEN 'obgyn_physician' ",
    "WHEN ", taxonomy_expression, " LIKE '%208800000X%' THEN 'urology_physician' ",
    "ELSE 'other_provider' END"
  )
}

#' Inventory APP evidence already present in a DuckDB
#'
#' @param duckdb_path Existing DuckDB path.
#' @param part_b_table Expected CMS Part B union table.
#' @param nppes_schema Schema containing longitudinal NPPES tables.
#'
#' @return A tibble describing matched Part B, NPPES, and DAC tables.
#' @family data
#' @concept data
#' @export
inventory_app_evidence_duckdb <- function(
    duckdb_path = default_part_b_duckdb(),
    part_b_table = "medicare_part_b_by_service_all_years",
    nppes_schema = "credentials") {
  base::message("inventory_app_evidence_duckdb(): starting")
  if (!base::file.exists(duckdb_path)) {
    base::stop("DuckDB not found: ", duckdb_path, ".")
  }
  connection <- DBI::dbConnect(duckdb::duckdb(), duckdb_path, read_only = TRUE)
  base::on.exit(DBI::dbDisconnect(connection, shutdown = TRUE), add = TRUE)

  tables <- DBI::dbGetQuery(connection, "SELECT table_schema, table_name FROM information_schema.tables ORDER BY table_schema, table_name") |>
    tibble::as_tibble() |>
    dplyr::mutate(
      source_family = dplyr::case_when(
        .data$table_schema == "main" & .data$table_name == part_b_table ~ "cms_part_b",
        .data$table_schema == nppes_schema & base::grepl("^temporal_nppes_[0-9]{4}(_fixed)?$", .data$table_name) ~ "nppes_longitudinal",
        base::grepl("dac|doctor.*clinician", .data$table_name, ignore.case = TRUE) ~ "doctors_clinicians",
        TRUE ~ "other"
      )
    ) |>
    dplyr::filter(.data$source_family != "other")

  base::message("Matched evidence tables: ", scales::comma(base::nrow(tables)))
  base::message("inventory_app_evidence_duckdb(): complete")
  tables
}

#' Build URPS APP evidence tables inside an existing DuckDB
#'
#' @param duckdb_path Existing source DuckDB.
#' @param doctors_clinicians_path Optional DAC National Downloadable File CSV.
#' @param output_schema Schema for derived evidence.
#' @param part_b_table Existing all-years CMS table.
#' @param nppes_schema Schema holding temporal NPPES tables.
#' @param replace Whether existing derived tables may be replaced.
#'
#' @return A tibble containing row counts for every derived table.
#' @family data
#' @concept data
#' @export
build_app_skill_mix_evidence_duckdb <- function(
    duckdb_path = default_part_b_duckdb(),
    doctors_clinicians_path = base::Sys.getenv("CMS_DAC_CSV", ""),
    output_schema = "app_evidence",
    part_b_table = "medicare_part_b_by_service_all_years",
    nppes_schema = "credentials",
    replace = FALSE) {
  base::message("build_app_skill_mix_evidence_duckdb(): starting")
  .app_assert_identifier(output_schema, "output_schema")
  .app_assert_identifier(part_b_table, "part_b_table")
  .app_assert_identifier(nppes_schema, "nppes_schema")

  if (!base::file.exists(duckdb_path)) {
    base::stop("DuckDB not found: ", duckdb_path, ".")
  }
  connection <- DBI::dbConnect(duckdb::duckdb(), duckdb_path)
  base::on.exit(DBI::dbDisconnect(connection, shutdown = TRUE), add = TRUE)

  schema_sql <- .app_sql_name(connection, output_schema)
  DBI::dbExecute(connection, base::paste("CREATE SCHEMA IF NOT EXISTS", schema_sql))

  existing <- DBI::dbGetQuery(connection, "SELECT table_name FROM information_schema.tables WHERE table_schema = ?", params = list(output_schema)) |>
    dplyr::pull(.data$table_name)
  derived_names <- c("urps_hcpcs_crosswalk", "medicare_app_service_share", "nppes_provider_year",
                     "doctors_clinicians_affiliations", "practice_supervision_pools", "evidence_provenance")
  collisions <- base::intersect(existing, derived_names)
  if (base::length(collisions) > 0L && !base::isTRUE(replace)) {
    base::stop("Derived tables already exist: ", base::paste(collisions, collapse = ", "), ". Set `replace = TRUE` to rebuild them.")
  }

  base::message("Writing the canonical URPS HCPCS crosswalk")
  crosswalk <- urps_medicare_service_crosswalk() |>
    dplyr::mutate(hcpcs = base::as.character(.data$hcpcs), service = base::as.character(.data$service))
  DBI::dbWriteTable(connection, DBI::Id(schema = output_schema, table = "urps_hcpcs_crosswalk"), crosswalk, overwrite = base::isTRUE(replace))

  base::message("Aggregating CMS Part B provider-service evidence")
  part_columns <- .app_table_columns(connection, "main", part_b_table)
  year_column <- .app_resolve_column(part_columns, c("data_year", "year"))
  hcpcs_column <- .app_resolve_column(part_columns, c("HCPCS_Cd", "hcpcs_cd", "hcpcs"))
  service_column <- .app_resolve_column(part_columns, c("Tot_Srvcs", "tot_srvcs", "services"))
  provider_column <- .app_resolve_column(part_columns, c("Rndrng_Prvdr_Type", "rndrng_prvdr_type", "provider_type"))
  npi_column <- .app_resolve_column(part_columns, c("Rndrng_NPI", "rndrng_npi", "npi"))
  state_column <- .app_resolve_column(part_columns, c("Rndrng_Prvdr_State_Abrvtn", "state", "prvdr_state"))

  part_sql <- .app_sql_name(connection, part_b_table)
  crosswalk_sql <- base::paste0(schema_sql, ".", .app_sql_name(connection, "urps_hcpcs_crosswalk"))

  share_sql <- base::paste0(
    "CREATE OR REPLACE TABLE ", schema_sql, ".medicare_app_service_share AS WITH classified AS (",
    "SELECT CAST(p.", .app_sql_name(connection, year_column), " AS INTEGER) AS year, ",
    "upper(trim(CAST(p.", .app_sql_name(connection, state_column), " AS VARCHAR))) AS state, x.service, ",
    .app_provider_case_sql(base::paste0("CAST(p.", .app_sql_name(connection, provider_column), " AS VARCHAR)")), " AS provider_type, ",
    "CAST(p.", .app_sql_name(connection, npi_column), " AS VARCHAR) AS npi, ",
    "CAST(p.", .app_sql_name(connection, service_column), " AS DOUBLE) AS billed_services ",
    "FROM main.", part_sql, " p INNER JOIN ", crosswalk_sql, " x ON CAST(p.", .app_sql_name(connection, hcpcs_column), " AS VARCHAR) = x.hcpcs ",
    "WHERE p.", .app_sql_name(connection, service_column), " IS NOT NULL), ",
    "grouped AS (SELECT year, state, service, provider_type, SUM(billed_services) AS billed_services, COUNT(DISTINCT npi) AS billing_npis FROM classified GROUP BY ALL) ",
    "SELECT *, SUM(billed_services) OVER (PARTITION BY year, state, service) AS all_provider_services, ",
    "billed_services / NULLIF(SUM(billed_services) OVER (PARTITION BY year, state, service), 0) AS billed_service_share, ",
    "provider_type IN ('nurse_practitioner', 'physician_assistant', 'clinical_nurse_specialist', 'certified_nurse_midwife') AS is_app, ",
    "TRUE AS is_lower_bound, 'CMS Original Medicare FFS; incident-to APP work may be hidden' AS estimand_caveat FROM grouped"
  )
  DBI::dbExecute(connection, share_sql)

  base::message("Normalizing longitudinal NPPES provider-year taxonomy")
  nppes_tables <- DBI::dbGetQuery(connection, "SELECT table_name FROM information_schema.tables WHERE table_schema = ? ORDER BY table_name", params = list(nppes_schema)) |>
    dplyr::filter(base::grepl("^temporal_nppes_[0-9]{4}(_fixed)?$", .data$table_name)) |>
    dplyr::mutate(year = base::as.integer(base::sub(".*_([0-9]{4})(_fixed)?$", "\\1", .data$table_name)), fixed_priority = base::grepl("_fixed$", .data$table_name)) |>
    dplyr::arrange(.data$year, dplyr::desc(.data$fixed_priority)) |>
    dplyr::distinct(.data$year, .keep_all = TRUE) |>
    dplyr::pull(.data$table_name)

  if (base::length(nppes_tables) == 0L) base::stop("No longitudinal NPPES tables were found.")

  nppes_queries <- purrr::map_chr(nppes_tables, function(table_name) {
    columns <- .app_table_columns(connection, nppes_schema, table_name)
    npi_name <- .app_resolve_column(columns, c("npi", "NPI"))
    state_name <- .app_resolve_column(columns, c("state", "provider_business_practice_location_address_state_name", "Provider Business Practice Location Address State Name"), required = FALSE)
    taxonomy_names <- columns[base::grepl("taxonomy.*code", columns, ignore.case = TRUE)]
    if (base::length(taxonomy_names) == 0L) base::stop("No taxonomy columns in ", table_name, ".")

    taxonomy_sql <- base::paste0("upper(concat_ws('|', ", base::paste(base::paste0("coalesce(CAST(", purrr::map_chr(taxonomy_names, ~ .app_sql_name(connection, .x)), " AS VARCHAR), '')"), collapse = ", "), "))")
    state_sql <- if (base::is.na(state_name)) "CAST(NULL AS VARCHAR)" else base::paste0("upper(trim(CAST(", .app_sql_name(connection, state_name), " AS VARCHAR)))")
    year_value <- base::as.integer(base::sub(".*_([0-9]{4})(_fixed)?$", "\\1", table_name))

    base::paste0(
      "SELECT CAST(", .app_sql_name(connection, npi_name), " AS VARCHAR) AS npi, ", year_value, " AS year, ",
      state_sql, " AS state, ", taxonomy_sql, " AS taxonomy_codes, ", .app_taxonomy_case_sql(taxonomy_sql), " AS provider_type, ",
      .app_sql_string(connection, table_name), " AS source_table FROM ", .app_sql_name(connection, nppes_schema), ".", .app_sql_name(connection, table_name)
    )
  })

  nppes_sql <- base::paste0("CREATE OR REPLACE TABLE ", schema_sql, ".nppes_provider_year AS ", base::paste(nppes_queries, collapse = " UNION ALL "))
  DBI::dbExecute(connection, nppes_sql)

  base::message("Loading Doctors & Clinicians practice affiliations")
  dac_table <- DBI::dbGetQuery(connection, "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema <> ? AND (lower(table_name) LIKE '%dac%' OR lower(table_name) LIKE '%doctor%clinician%') ORDER BY table_schema, table_name LIMIT 1", params = list(output_schema))

  dac_relation <- NULL
  dac_columns <- NULL
  if (base::nrow(dac_table) > 0L) {
    dac_relation <- base::paste0(.app_sql_name(connection, dac_table$table_schema[[1]]), ".", .app_sql_name(connection, dac_table$table_name[[1]]))
    dac_columns <- .app_table_columns(connection, dac_table$table_schema[[1]], dac_table$table_name[[1]])
  } else {
    if (!base::nzchar(doctors_clinicians_path) || !base::file.exists(doctors_clinicians_path)) {
      base::stop("Doctors & Clinicians data were not found in DuckDB. Set `doctors_clinicians_path` or `CMS_DAC_CSV`.")
    }
    file_sql <- .app_sql_string(connection, doctors_clinicians_path)
    dac_relation <- base::paste0("read_csv_auto(", file_sql, ", header = true, all_varchar = true, sample_size = -1)")
    dac_columns <- DBI::dbGetQuery(connection, base::paste0("DESCRIBE SELECT * FROM ", dac_relation)) |> dplyr::pull(.data$column_name)
  }

  dac_npi <- .app_resolve_column(dac_columns, c("NPI", "npi"))
  dac_group <- .app_resolve_column(dac_columns, c("org_pac_id", "Org_PAC_ID", "organization_pac_id"))
  dac_org <- .app_resolve_column(dac_columns, c("org_nm", "Org_nm", "organization_name"), required = FALSE)
  dac_state <- .app_resolve_column(dac_columns, c("State", "state"))

  org_sql <- if (base::is.na(dac_org)) "CAST(NULL AS VARCHAR)" else base::paste0("CAST(d.", .app_sql_name(connection, dac_org), " AS VARCHAR)")

  affiliation_sql <- base::paste0(
    "CREATE OR REPLACE TABLE ", schema_sql, ".doctors_clinicians_affiliations AS WITH latest AS (",
    "SELECT npi, provider_type, taxonomy_codes, year, row_number() OVER (PARTITION BY npi ORDER BY year DESC) AS rn ",
    "FROM ", schema_sql, ".nppes_provider_year) SELECT DISTINCT CAST(d.", .app_sql_name(connection, dac_npi), " AS VARCHAR) AS npi, ",
    "CAST(d.", .app_sql_name(connection, dac_group), " AS VARCHAR) AS practice_id, ", org_sql, " AS practice_name, ",
    "upper(trim(CAST(d.", .app_sql_name(connection, dac_state), " AS VARCHAR))) AS state, l.provider_type, l.taxonomy_codes, ",
    "l.year AS nppes_year, CURRENT_DATE AS build_date FROM ", dac_relation, " d LEFT JOIN latest l ON CAST(d.",
    .app_sql_name(connection, dac_npi), " AS VARCHAR) = l.npi AND l.rn = 1 WHERE d.", .app_sql_name(connection, dac_group), " IS NOT NULL"
  )
  DBI::dbExecute(connection, affiliation_sql)

  base::message("Building practice-level APP-to-physician pools")
  pool_sql <- base::paste0(
    "CREATE OR REPLACE TABLE ", schema_sql, ".practice_supervision_pools AS SELECT state, practice_id, max(practice_name) AS practice_name, ",
    "count(DISTINCT CASE WHEN provider_type IN ('nurse_practitioner', 'physician_assistant', 'clinical_nurse_specialist', 'certified_nurse_midwife') THEN npi END) AS app_headcount, ",
    "count(DISTINCT CASE WHEN provider_type IN ('fpmrs_obgyn', 'fpmrs_urology', 'obgyn_physician', 'urology_physician') THEN npi END) AS physician_headcount, ",
    "app_headcount / NULLIF(physician_headcount, 0) AS observed_app_physician_ratio, max(nppes_year) AS nppes_year, CURRENT_DATE AS build_date ",
    "FROM ", schema_sql, ".doctors_clinicians_affiliations GROUP BY state, practice_id"
  )
  DBI::dbExecute(connection, pool_sql)

  provenance <- tibble::tibble(
    derived_table = derived_names[derived_names != "evidence_provenance"],
    source = c("URPS_CPT_BASKET", "CMS Medicare Physician & Other Practitioners Provider-Service",
               "NPPES longitudinal snapshots", "CMS Doctors & Clinicians National Downloadable File", "NPPES linked to Doctors & Clinicians"),
    source_path = c("urps_medicare_service_crosswalk()", base::paste0("main.", part_b_table), base::paste0(nppes_schema, ".temporal_nppes_YEAR_fixed"),
                    dplyr::if_else(base::nrow(dac_table) > 0L, dac_relation, doctors_clinicians_path), "derived join"),
    build_date = base::as.character(base::Sys.Date()),
    estimand = c("procedure-specific URPS code definition", "observed Medicare FFS billed services; APP lower bound",
                "self-reported taxonomy and location by snapshot year", "current Medicare group affiliation; undated cross-section",
                "observed affiliated headcount ratio; not an FTE ratio")
  )
  DBI::dbWriteTable(connection, DBI::Id(schema = output_schema, table = "evidence_provenance"), provenance, overwrite = TRUE)

  row_counts <- purrr::map_dfr(derived_names, function(table_name) {
    count_query <- base::paste0("SELECT COUNT(*) AS row_count FROM ", schema_sql, ".", .app_sql_name(connection, table_name))
    tibble::tibble(table = table_name, rows = DBI::dbGetQuery(connection, count_query)$row_count[[1]])
  })

  base::message("Derived rows: ", scales::comma(base::sum(row_counts$rows)))
  base::message("build_app_skill_mix_evidence_duckdb(): complete")
  row_counts
}

#' Read empirical APP evidence for the delegation optimizer
#'
#' @param duckdb_path DuckDB built by `build_app_skill_mix_evidence_duckdb()`.
#' @param output_schema Derived evidence schema.
#' @param years Optional calendar-year restriction.
#'
#' @return A list with service evidence, practice pools, and provenance.
#' @family data
#' @concept data
#' @export
read_app_skill_mix_evidence <- function(
    duckdb_path = default_part_b_duckdb(),
    output_schema = "app_evidence",
    years = NULL) {
  base::message("read_app_skill_mix_evidence(): starting")
  .app_assert_identifier(output_schema, "output_schema")
  connection <- DBI::dbConnect(duckdb::duckdb(), duckdb_path, read_only = TRUE)
  base::on.exit(DBI::dbDisconnect(connection, shutdown = TRUE), add = TRUE)

  schema_sql <- .app_sql_name(connection, output_schema)
  year_clause <- ""
  if (!base::is.null(years)) {
    if (!base::is.numeric(years) || base::any(!base::is.finite(years))) {
      base::stop("`years` must contain finite calendar years.")
    }
    year_clause <- base::paste0(" WHERE year IN (", base::paste(base::as.integer(years), collapse = ", "), ")")
  }

  service_evidence <- DBI::dbGetQuery(connection, base::paste0("SELECT * FROM ", schema_sql, ".medicare_app_service_share", year_clause)) |> tibble::as_tibble()
  practice_pools <- DBI::dbGetQuery(connection, base::paste0("SELECT * FROM ", schema_sql, ".practice_supervision_pools")) |> tibble::as_tibble()
  provenance <- DBI::dbGetQuery(connection, base::paste0("SELECT * FROM ", schema_sql, ".evidence_provenance")) |> tibble::as_tibble()

  base::message("Service evidence rows: ", scales::comma(base::nrow(service_evidence)))
  base::message("read_app_skill_mix_evidence(): complete")
  list(service_evidence = service_evidence, practice_pools = practice_pools, provenance = provenance)
}

#' Add claims evidence to APP productivity eligibility inputs
#'
#' @param productivity Optimizer productivity table.
#' @param service_evidence Output service evidence.
#' @param minimum_services Minimum billed services for a positive evidence flag.
#'
#' @return Productivity rows with empirical evidence columns added.
#' @family data
#' @concept data
#' @export
augment_app_productivity_evidence <- function(
    productivity,
    service_evidence,
    minimum_services = 11) {
  base::message("augment_app_productivity_evidence(): starting")
  required_productivity <- c("service", "provider_type", "clinically_eligible")
  required_evidence <- c("service", "provider_type", "billed_services", "billed_service_share", "billing_npis", "is_lower_bound")

  if (base::length(base::setdiff(required_productivity, base::names(productivity))) > 0L ||
      base::length(base::setdiff(required_evidence, base::names(service_evidence))) > 0L) {
    base::stop("Productivity or evidence inputs are missing required columns.")
  }

  evidence_summary <- service_evidence |>
    dplyr::group_by(.data$service, .data$provider_type) |>
    dplyr::summarise(
      observed_billed_services = base::sum(.data$billed_services, na.rm = TRUE),
      observed_billing_npis = base::sum(.data$billing_npis, na.rm = TRUE),
      median_billed_share = stats::median(.data$billed_service_share, na.rm = TRUE),
      p25_billed_share = stats::quantile(.data$billed_service_share, probs = 0.25, na.rm = TRUE, names = FALSE),
      p75_billed_share = stats::quantile(.data$billed_service_share, probs = 0.75, na.rm = TRUE, names = FALSE),
      evidence_is_lower_bound = base::all(.data$is_lower_bound),
      .groups = "drop"
    )

  enhanced <- productivity |>
    dplyr::left_join(evidence_summary, by = c("service", "provider_type"), relationship = "many-to-one") |>
    dplyr::mutate(
      positive_claims_evidence = dplyr::coalesce(.data$observed_billed_services >= minimum_services & .data$observed_billing_npis > 0, FALSE),
      evidence_interpretation = dplyr::case_when(
        .data$positive_claims_evidence ~ "positive Medicare billing evidence; lower bound",
        TRUE ~ "no positive billing evidence; eligibility remains unclassified"
      ),
      clinical_eligibility_preserved = .data$clinically_eligible
    )

  base::message("Rows with positive APP billing evidence: ", scales::comma(base::sum(enhanced$positive_claims_evidence)))
  base::message("augment_app_productivity_evidence(): complete")
  enhanced
}

#' Estimate an evidence-informed APP ratio scenario
#'
#' @param practice_pools Practice pools from `read_app_skill_mix_evidence()`.
#' @param probability Quantile of observed practice ratios to use.
#' @param policy_ceiling Separate legal or prespecified scenario ceiling.
#' @param minimum_physicians Minimum physician headcount in included practices.
#'
#' @return A one-row tibble with mean (SD), median (p25, p75), and the bounded scenario ratio.
#' @family data
#' @concept data
#' @export
estimate_app_ratio_scenario <- function(
    practice_pools,
    probability = 0.75,
    policy_ceiling = 3,
    minimum_physicians = 1) {
  base::message("estimate_app_ratio_scenario(): starting")
  required <- c("app_headcount", "physician_headcount", "observed_app_physician_ratio")
  missing <- base::setdiff(required, base::names(practice_pools))
  if (base::length(missing) > 0L) {
    base::stop("`practice_pools` is missing: ", base::paste(missing, collapse = ", "), ".")
  }
  if (!base::is.numeric(probability) || base::length(probability) != 1L || !base::is.finite(probability) || probability <= 0 || probability >= 1) {
    base::stop("`probability` must be one number strictly between zero and one.")
  }
  if (!base::is.numeric(policy_ceiling) || base::length(policy_ceiling) != 1L || !base::is.finite(policy_ceiling) || policy_ceiling <= 0) {
    base::stop("`policy_ceiling` must be one positive finite number.")
  }

  eligible_pools <- practice_pools |>
    dplyr::filter(.data$physician_headcount >= minimum_physicians, base::is.finite(.data$observed_app_physician_ratio), .data$observed_app_physician_ratio >= 0)
  if (base::nrow(eligible_pools) == 0L) base::stop("No eligible practice ratios remain after filtering.")

  ratio_values <- eligible_pools$observed_app_physician_ratio
  observed_quantile <- stats::quantile(ratio_values, probs = probability, names = FALSE, type = 8)
  scenario_ratio <- base::min(observed_quantile, policy_ceiling)

  summary_table <- tibble::tibble(
    practices = base::nrow(eligible_pools),
    mean_ratio = base::mean(ratio_values),
    sd_ratio = stats::sd(ratio_values),
    median_ratio = stats::median(ratio_values),
    p25_ratio = stats::quantile(ratio_values, probs = 0.25, names = FALSE, type = 8),
    p75_ratio = stats::quantile(ratio_values, probs = 0.75, names = FALSE, type = 8),
    requested_probability = probability,
    observed_quantile = observed_quantile,
    policy_ceiling = policy_ceiling,
    scenario_ratio = scenario_ratio,
    estimand = "Doctors & Clinicians affiliated APP-to-physician headcount ratio; cross-sectional benchmark, not FTE and not a legal limit"
  )

  base::message("Scenario ratio: ", scales::number(scenario_ratio, accuracy = 0.01), " APPs per physician")
  base::message("estimate_app_ratio_scenario(): complete")
  summary_table
}
