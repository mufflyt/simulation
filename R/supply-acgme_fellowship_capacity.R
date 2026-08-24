# ACGME URPS Fellowship Capacity & Case Log Bottleneck Model ------------------

#' Official ACGME URPS Procedural Minimums Table (Effective 2025 Graduates)
#'
#' @description
#' Returns the exact ACGME Case Log procedural minimums required across the
#' 3-year fellowship. Meeting a minimum indicates completion of required
#' educational experience, not a determination of clinical competence.
#'
#' @return A tibble with `domain`, `category`, `acgme_minimum`, and `role_credit_rules`.
#' @family supply
#' @concept fellowship
#' @export
acgme_procedural_minimums <- function() {
  base::message("[acgme-capacity] Loading official 2025 ACGME Case Log minimums.")
  tibble::tribble(
    ~domain, ~category, ~acgme_minimum, ~role_credit_rules,
    "Diagnostic studies", "Diagnostic studies", 100L, "Surgeon, TA, Assistant",
    "Diagnostic studies", "Complex urodynamics", 25L, "Surgeon, TA, Assistant",
    "Urinary-incontinence", "Urinary-incontinence procedures", 95L, "Surgeon, TA",
    "Urinary-incontinence", "Stress-incontinence procedures", 65L, "Surgeon, TA",
    "Urinary-incontinence", "Periurethral injections", 5L, "Surgeon, TA, Assistant",
    "Urinary-incontinence", "Sling procedures", 50L, "Surgeon, TA",
    "Urinary-incontinence", "Urgency-incontinence procedures", 25L, "Surgeon, TA",
    "Urinary-incontinence", "Sacral neuromodulation", 10L, "Surgeon, TA",
    "Urinary-incontinence", "Botox injections", 10L, "Surgeon, TA",
    "Prolapse operations", "Prolapse operations", 130L, "Surgeon, TA",
    "Prolapse operations", "Sacrocolpopexy", 20L, "Surgeon, TA",
    "Prolapse operations", "Colpocleisis", 10L, "Surgeon, TA",
    "Prolapse operations", "Vaginal colpopexy", 40L, "Surgeon, TA",
    "Prolapse operations", "Extraperitoneal vaginal colpopexy", 10L, "Surgeon, TA",
    "Prolapse operations", "Intraperitoneal vaginal colpopexy", 10L, "Surgeon, TA",
    "Prolapse operations", "Posterior repair", 20L, "Surgeon, TA",
    "Urinary-system operations", "Urinary-system operations", 25L, "Surgeon, TA",
    "Urinary-system operations", "Urinary fistula repair", 2L, "Surgeon, TA, Assistant",
    "Urinary-system operations", "Urethral diverticulectomy", 2L, "Surgeon, TA, Assistant",
    "Urinary-system operations", "Ureteral stent placement", 3L, "Surgeon, TA, Assistant",
    "Urinary-system operations", "Retrograde pyelogram", 1L, "Surgeon, TA, Assistant",
    "Urinary-system operations", "Sling removal or revision", 5L, "Surgeon, TA",
    "Urinary-system operations", "Urethrolysis", 0L, "Tracked only",
    "Urinary-system operations", "Cystotomy closure", 1L, "Surgeon, TA, Assistant",
    "Genital-system operations", "Genital-system operations", 30L, "Surgeon, TA",
    "Genital-system operations", "Vaginal hysterectomy", 15L, "Surgeon, TA",
    "Genital-system operations", "Laparoscopic hysterectomy", 10L, "Surgeon, TA",
    "Genital-system operations", "Vaginal graft revision/removal", 2L, "Surgeon, TA",
    "Gastrointestinal-system operations", "Gastrointestinal-system operations", 4L, "Surgeon, TA",
    "Gastrointestinal-system operations", "Anal sphincter repair", 1L, "Surgeon, TA, Assistant",
    "Gastrointestinal-system operations", "Rectovaginal fistula repair", 1L, "Surgeon, TA, Assistant"
  )
}

#' ACGME Program Infrastructure Requirements (Effective July 1, 2026)
#'
#' @return A list of regulatory program parameters.
#' @family supply
#' @concept fellowship
#' @export
acgme_program_requirements <- function() {
  base::message("[acgme-capacity] Loading 2026 ACGME program infrastructure standards.")
  base::list(
    requires_urologist_faculty = TRUE,
    requires_obgyn_faculty = TRUE,
    minimum_core_faculty_count = 2L, # PD + at least 1 core
    pd_min_admin_fte = 0.20,
    coordinator_fte_1_2_fellows = 0.20,
    coordinator_fte_3_plus_fellows = 0.30,
    typical_min_fellows_enrolled = 2L,
    has_faculty_fte_per_fellow_rule = FALSE # Explicitly FALSE: ACGME does not require 2.0 clinical FTE per fellow
  )
}

#' CPT-to-ACGME Minimum Incidence Crosswalk Matrix
#'
#' Maps URPS CPT codes to ACGME case log categories, supporting multi-category
#' overlapping Case Log credit (e.g. a sling counts for sling, stress-incontinence,
#' and total urinary-incontinence).
#'
#' @return A tibble with `cpt`, `service`, `category`, and `incidence_weight`.
#' @family supply
#' @concept fellowship
#' @export
build_cpt_acgme_incidence_matrix <- function() {
  base::message("[acgme-capacity] Building CPT-to-ACGME incidence crosswalk.")
  tibble::tribble(
    ~cpt, ~service, ~category, ~incidence_weight,
    "57288", "sling_procedure", "Sling procedures", 1.0,
    "57288", "sling_procedure", "Stress-incontinence procedures", 1.0,
    "57288", "sling_procedure", "Urinary-incontinence procedures", 1.0,
    "57425", "sacrocolpopexy", "Sacrocolpopexy", 1.0,
    "57425", "sacrocolpopexy", "Prolapse operations", 1.0,
    "57120", "colpocleisis", "Colpocleisis", 1.0,
    "57120", "colpocleisis", "Prolapse operations", 1.0,
    "57282", "extraperitoneal_colpopexy", "Extraperitoneal vaginal colpopexy", 1.0,
    "57282", "extraperitoneal_colpopexy", "Vaginal colpopexy", 1.0,
    "57282", "extraperitoneal_colpopexy", "Prolapse operations", 1.0,
    "57283", "intraperitoneal_colpopexy", "Intraperitoneal vaginal colpopexy", 1.0,
    "57283", "intraperitoneal_colpopexy", "Vaginal colpopexy", 1.0,
    "57283", "intraperitoneal_colpopexy", "Prolapse operations", 1.0,
    "57250", "posterior_repair", "Posterior repair", 1.0,
    "57250", "posterior_repair", "Prolapse operations", 1.0,
    "57320", "urinary_fistula", "Urinary fistula repair", 1.0,
    "57320", "urinary_fistula", "Urinary-system operations", 1.0,
    "57330", "rectovaginal_fistula", "Rectovaginal fistula repair", 1.0,
    "57330", "rectovaginal_fistula", "Gastrointestinal-system operations", 1.0,
    "51729", "complex_urodynamics", "Complex urodynamics", 1.0,
    "51729", "complex_urodynamics", "Diagnostic studies", 1.0,
    "64590", "sacral_neuromodulation", "Sacral neuromodulation", 1.0,
    "64590", "sacral_neuromodulation", "Urgency-incontinence procedures", 1.0,
    "64590", "sacral_neuromodulation", "Urinary-incontinence procedures", 1.0,
    "52287", "botox_injection", "Botox injections", 1.0,
    "52287", "botox_injection", "Urgency-incontinence procedures", 1.0,
    "52287", "botox_injection", "Urinary-incontinence procedures", 1.0,
    "58260", "vaginal_hysterectomy", "Vaginal hysterectomy", 1.0,
    "58260", "vaginal_hysterectomy", "Genital-system operations", 1.0,
    "58570", "laparoscopic_hysterectomy", "Laparoscopic hysterectomy", 1.0,
    "58570", "laparoscopic_hysterectomy", "Genital-system operations", 1.0,
    "46750", "anal_sphincter_repair", "Anal sphincter repair", 1.0,
    "46750", "anal_sphincter_repair", "Gastrointestinal-system operations", 1.0
  )
}

# The formula below is \deqn{}{} with braced subscripts; see the note above
# calculate_patient_destination_probabilities() in
# R/geography-patient_destination_choice.R for why.

#' Evaluate Program Fellowship Capacity Under ACGME Bottlenecks
#'
#' @description
#' Calculates fellowship training capacity per program-year as the minimum across
#' 5 structural bounds: approved complement, case-log volume bottlenecks,
#' faculty composition, participating-site capacity, and funding slots:
#' \deqn{\mathrm{Capacity}_{p,t} = \min(C^{\mathrm{approved}}, C^{\mathrm{cases}}, C^{\mathrm{faculty}}, C^{\mathrm{sites}}, C^{\mathrm{funding}})}{Capacity(p,t) = min(C.approved, C.cases, C.faculty, C.sites, C.funding)}
#'
#' @param program_tbl Tibble containing program parameters (`program_id`, `year`, `approved_complement`, `faculty_urologists`, `faculty_obgyn`, `funding_slots`, `site_capacity`).
#' @param case_volume_tbl Institutional surgical volume per program (`program_id`, `category`, `annual_case_volume`, `fellow_accessible_share`, `qualifying_role_prob`).
#' @param minimums_tbl Output of [acgme_procedural_minimums()].
#'
#' @return A list with program capacity, limiting bottlenecks, and detailed case-volume metrics.
#' @family supply
#' @concept fellowship
#' @export
simulate_acgme_fellowship_capacity <- function(
    program_tbl,
    case_volume_tbl,
    minimums_tbl = acgme_procedural_minimums()) {
  base::message("[acgme-capacity] Evaluating ACGME fellowship training capacity bottlenecks.")

  req_cols <- base::c("program_id", "year", "approved_complement", "faculty_urologists", "faculty_obgyn")
  missing_cols <- base::setdiff(req_cols, base::names(program_tbl))
  if (base::length(missing_cols) > 0L) {
    base::stop("Missing required columns in program_tbl: ", base::paste(missing_cols, collapse = ", "), call. = FALSE)
  }

  prepared_prog <- program_tbl |>
    dplyr::mutate(
      funding_slots = dplyr::coalesce(.data$funding_slots, .data$approved_complement),
      site_capacity = dplyr::coalesce(.data$site_capacity, .data$approved_complement),
      faculty_valid = .data$faculty_urologists >= 1 & .data$faculty_obgyn >= 1,
      capacity_faculty = dplyr::if_else(.data$faculty_valid, .data$approved_complement, 0L)
    )

  # Calculate procedural case-volume capacity per category
  case_capacity <- case_volume_tbl |>
    dplyr::inner_join(minimums_tbl, by = "category") |>
    dplyr::filter(.data$acgme_minimum > 0) |>
    dplyr::mutate(
      effective_credited_cases = .data$annual_case_volume *
        dplyr::coalesce(.data$fellow_accessible_share, 1.0) *
        dplyr::coalesce(.data$qualifying_role_prob, 1.0),
      # 3-year fellowship case log capacity
      category_fellow_capacity = base::floor((.data$effective_credited_cases * 3.0) / .data$acgme_minimum)
    )

  program_case_bottleneck <- case_capacity |>
    dplyr::group_by(.data$program_id) |>
    dplyr::summarise(
      capacity_cases = base::min(.data$category_fellow_capacity),
      binding_category = .data$category[[base::which.min(.data$category_fellow_capacity)]],
      .groups = "drop"
    )

  # Combine all 5 capacity bounds
  result_tbl <- prepared_prog |>
    dplyr::left_join(program_case_bottleneck, by = "program_id") |>
    dplyr::mutate(
      capacity_cases = dplyr::coalesce(.data$capacity_cases, .data$approved_complement),
      binding_category = dplyr::coalesce(.data$binding_category, "None (Approved Complement)"),
      max_simulated_capacity = base::pmin(
        .data$approved_complement,
        .data$capacity_cases,
        .data$capacity_faculty,
        .data$site_capacity,
        .data$funding_slots
      ),
      primary_bottleneck = dplyr::case_when(
        .data$max_simulated_capacity == .data$capacity_faculty & !.data$faculty_valid ~ "Faculty Composition (Missing Urology/OBGYN)",
        .data$max_simulated_capacity == .data$capacity_cases & .data$capacity_cases < .data$approved_complement ~ base::paste0("Case Volume: ", .data$binding_category),
        .data$max_simulated_capacity == .data$funding_slots & .data$funding_slots < .data$approved_complement ~ "GME Funding Slots",
        .data$max_simulated_capacity == .data$site_capacity & .data$site_capacity < .data$approved_complement ~ "Participating Site Infrastructure",
        TRUE ~ "Approved Complement Cap"
      )
    )

  base::message("[acgme-capacity] Evaluated ", base::nrow(result_tbl), " fellowship programs.")
  base::list(
    program_capacity = result_tbl,
    case_level_breakdown = case_capacity,
    minimums_reference = minimums_tbl
  )
}
