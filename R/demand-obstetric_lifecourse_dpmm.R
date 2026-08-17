# Obstetric Lifecourse Disease Prevention & Demand Microsimulation (DPMM) ----
#
# Implements the reproductive life-course microsimulation architecture, linking
# cumulative vaginal deliveries, operative delivery history, age, BMI, and hysterectomy
# to pelvic organ prolapse (POP), stress urinary incontinence (SUI), and fecal incontinence (FI).

#' Generate Representative Obstetric Lifecourse Synthetic Cohort
#'
#' Generates a synthetic cohort of women with rich obstetric and clinical histories.
#'
#' @param n_women Number of synthetic women to simulate.
#' @param base_year Target simulation year.
#' @param seed Optional random seed.
#' @return Tibble of individual person-year records.
#' @family demand
#' @concept lifecourse
#' @export
generate_obstetric_lifecourse_cohort <- function(n_women = 1000L, base_year = 2025L, seed = 42L) {
  if (!is.null(seed)) set.seed(seed)

  ages <- pmin(pmax(round(stats::rnorm(n_women, mean = 48, sd = 16)), 18), 90)

  # Impute parity and delivery mode based on age
  parity <- ifelse(ages < 22, stats::rpois(n_women, 0.3),
            ifelse(ages < 35, stats::rpois(n_women, 1.4),
                              stats::rpois(n_women, 2.1)))

  vaginal_births <- pmin(stats::rbinom(n_women, size = parity, prob = 0.72), parity)
  cesarean_births <- parity - vaginal_births
  years_since_last_vaginal <- ifelse(vaginal_births > 0, pmax(ages - 28, 0), NA_real_)

  bmi <- pmax(stats::rnorm(n_women, mean = 28.5, sd = 5.5), 17.5)
  hysterectomy <- stats::rbinom(n_women, size = 1, prob = pmin(pmax((ages - 40) * 0.008, 0), 0.35)) == 1L
  menopause <- ages >= 51

  tibble::tibble(
    person_id                      = seq_len(n_women),
    year                           = base_year,
    age                            = ages,
    parity                         = parity,
    vaginal_births                 = vaginal_births,
    cesarean_births                = cesarean_births,
    years_since_last_vaginal_birth = years_since_last_vaginal,
    bmi                            = bmi,
    menopause_status               = menopause,
    hysterectomy                   = hysterectomy,
    ui_state                       = "none",
    pop_state                      = "none",
    ai_state                       = "none",
    care_seeking_state             = "untreated",
    annual_service_units           = 0.0
  )
}

#' Predict Pelvic Floor Disease Onset from Obstetric Lifecourse Trajectory
#'
#' @param cohort Output of [generate_obstetric_lifecourse_cohort()].
#' @return Cohort with updated `pop_state`, `ui_state`, and `annual_service_units`.
#' @family demand
#' @concept lifecourse
#' @export
predict_pelvic_floor_disease_trajectory <- function(cohort) {
  # Logistic hazard for POP: strongly driven by vaginal deliveries, modified by age, BMI, hysterectomy
  logit_pop <- -5.2 +
               0.68 * cohort$vaginal_births +
               0.04 * (cohort$age - 45) +
               0.03 * (cohort$bmi - 25) +
               0.45 * as.numeric(cohort$hysterectomy)

  prob_pop <- 1 / (1 + exp(-logit_pop))
  has_pop <- stats::runif(nrow(cohort)) < prob_pop
  cohort$pop_state <- ifelse(has_pop, "symptomatic_stage2plus", "none")

  # Logistic hazard for SUI: driven by parity and age
  logit_sui <- -4.1 +
               0.42 * cohort$vaginal_births +
               0.02 * (cohort$age - 45) +
               0.04 * (cohort$bmi - 25)

  prob_sui <- 1 / (1 + exp(-logit_sui))
  has_sui <- stats::runif(nrow(cohort)) < prob_sui
  cohort$ui_state <- ifelse(has_sui, "sui_moderate", "none")

  # Assign annual service units for care-seeking patients
  care_seeking_prob <- 0.35 # ~35% of symptomatic women seek specialist care annually
  cohort$care_seeking_state <- ifelse((has_pop | has_sui) & stats::runif(nrow(cohort)) < care_seeking_prob,
                                      "care_engaged", "untreated")

  cohort$annual_service_units <- ifelse(cohort$care_seeking_state == "care_engaged", 1.85, 0.0)

  cohort
}
