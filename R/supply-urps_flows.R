# Labor Force Participation Model ----
#
# HWSM v5.19.20, "Labor force participation" (pp. 27–28):
#   "Labor force participation decisions encompasses whether a provider joins
#   the workforce as well as his/her level of participation ... This analysis
#   focuses on clinicians under age 50 (as the HWSM switches to permanent
#   retirement as the activity status changes for clinicians age 50 and over).
#   The dependent variable was whether the nurse was employed or not employed
#   with explanatory variables ... in particular age."
#
# Analogous to HWSM Exhibit 16, this module fits a logistic regression for
#   P(active in year t | age, sex, years_certified)
# for URPS subspecialists (Fellowship-trained urogynecologists and FPMRS
# fellows). The reference data are ABOG recertification continuity records
# (which indicate whether a fellow renewed their certificate—a strong proxy
# for continued practice) combined with AMA Masterfile active-status flags.
#
# Unlike Exhibit 16, which applies only to ages < 50, this model covers the
# full career so that a single call replaces separate career-change and
# retirement-hazard lookups for downstream roster-count adjustment. The
# coefficients are calibrated so the function reproduces the HWSM Exhibit 17
# survival anchors (S(65) ≈ 0.55, S(75) ≈ 0.12) for a male physician at the
# reference entry age.
#
# Scenario integration follows the Dall-family convention: the
# `retirement_shift_years` lever from the scenario registry is applied as an
# age-axis shift (not a multiplicative hazard knob), identical to how
# `shift_retirement_schedule()` operates. Retiring 2 years later is
# represented by treating a 65-year-old as 63 in the logistic predictor.
#
# ---- Calibration derivation --------------------------------------------------
#
# Model specification (reference entry age 33; female uses OR ≈ 0.77):
#   logit(P) = α + β_age·age + β_age2·age² + β_female·I(female) + β_yrs·yrs_cert
#
# Three HWSM Exhibit 17 calibration points (male, entry age 33):
#   P(age=35, yrs=2)  ≈ 0.985  → logit = 4.185
#   P(age=65, yrs=32) ≈ 0.550  → logit = 0.200
#   P(age=75, yrs=42) ≈ 0.120  → logit = −1.987
#
# Substituting yrs_cert = age − 33 at calibration gives a 3-equation
# system in three unknowns after fixing β_yrs = −0.010. Solving yields:
#   β_age + β_yrs = 0.0819  →  β_age = 0.0919
#   β_age2        = −0.002147
#   α − 33·β_yrs  = 3.950   →  α = 3.620
#
# β_female = log(0.77) ≈ −0.262, the HWSM Exhibit 16 RN odds-ratio adapted
# for physician subspecialists (HWSM Exhibit 17: female physicians retire
# slightly earlier than male colleagues, consistent with OR < 1).
#
# β_yrs_cert = −0.010: at a given age, a longer certification history
# indicates an earlier entry and incremental career fatigue — a small negative
# marginal effect consistent with the ACS analysis direction in Exhibit 16.

# ---- Constants ---------------------------------------------------------------

#' Logistic regression coefficients for urps_p_active()
#'
#' A named list of calibrated coefficients. Pass a custom list to
#' [urps_p_active()] when ABOG recertification data have been fitted with
#' [fit_p_active_model()].
#'
#' @keywords internal
URPS_P_ACTIVE_COEF <- list(
  intercept   =  3.620,
  age         =  0.0919,
  age_sq      = -0.002147,
  female      = -0.262,
  years_cert  = -0.010
)

# ---- Internal helpers --------------------------------------------------------

# Resolve scenario_id → retirement_shift_years (integer years).
# Returns 0 when scenario_id is NULL, NA, or unknown.
.resolve_retirement_shift <- function(scenario_id, registry) {
  if (is.null(scenario_id) || isTRUE(is.na(scenario_id))) return(0L)

  if (is.null(registry)) {
    registry <- tryCatch(
      supply_scenario_registry(),
      error = function(e) local_supply_scenario_registry()
    )
  }

  scen <- registry[[scenario_id]]
  if (is.null(scen)) {
    warning("urps_p_active: unknown scenario_id '", scenario_id,
            "'; retirement_shift_years treated as 0.", call. = FALSE)
    return(0L)
  }
  as.integer(scen$retirement_shift_years %||% 0L)
}

# ---- Main function -----------------------------------------------------------

#' Probability of being active in a given projection year
#'
#' Logistic regression estimate of P(clinician is actively practising) given
#' current age, sex, and years since initial board certification. The function
#' is vectorised; all arguments except `scenario_id`, `coef`, and `registry`
#' are recycled to the longest input.
#'
#' The model is calibrated to three HWSM Exhibit 17 survival anchors for a
#' male physician at the reference entry age (33):
#'   P(35) ≈ 0.985, P(65) ≈ 0.55, P(75) ≈ 0.12.
#' Female sex is entered as OR ≈ 0.77 (HWSM Exhibit 16 analog for physician
#' subspecialists). See [URPS_P_ACTIVE_COEF] for coefficient details.
#'
#' `scenario_id` resolves to `retirement_shift_years` from the supply scenario
#' registry and applies an age-axis shift—the same mechanism as
#' [shift_retirement_schedule()]—rather than a multiplicative hazard knob:
#' "Retire 2 years later" treats a 65-year-old as age 63 in the predictor.
#'
#' @param age Numeric age(s) in completed years. Must be in \[18, 100\].
#' @param sex Character sex: `"male"` or `"female"` (case-insensitive).
#'   Recycled to `length(age)`.
#' @param years_certified Numeric years since first board certification (≥ 0).
#'   Recycled to `length(age)`.
#' @param scenario_id Character scenario identifier from the supply scenario
#'   registry (e.g. `"status_quo"`, `"retire_2yr_later"`). `NULL`
#'   (default) applies no age-axis shift.
#' @param coef Named list of logistic coefficients; see [URPS_P_ACTIVE_COEF].
#'   Pass a list returned by [fit_p_active_model()] to use empirically fitted
#'   parameters.
#' @param registry Optional pre-fetched scenario registry list. When `NULL`
#'   (default) the function calls [supply_scenario_registry()] once per call.
#' @return Numeric vector of probabilities in \[0, 1\], one per provider.
#'   Returns 0 for ages ≥ `MICROSIM_TERMINAL_AGE` and for ages < 18.
#' @seealso [departure_hazard()], [shift_retirement_schedule()],
#'   [participation_fte()], [URPS_P_ACTIVE_COEF], [fit_p_active_model()]
#' @family urps flows
#' @concept supply
#' @export
#'
#' @examples
#' # Single provider: male, age 45, 12 years certified, status quo
#' urps_p_active(45, "male", 12)
#'
#' # Vectorised: compare female providers at key career stages
#' urps_p_active(c(35, 50, 65, 75), "female", c(2, 17, 32, 42))
#'
#' # Scenario: retiring 2 years later raises P(active) at age 65
#' urps_p_active(65, "male", 32, scenario_id = "retire_2yr_later")
urps_p_active <- function(age,
                           sex             = "female",
                           years_certified = 10,
                           scenario_id     = NULL,
                           coef            = URPS_P_ACTIVE_COEF,
                           registry        = NULL) {

  # ---- Input coercion -------------------------------------------------------
  age             <- as.numeric(age)
  years_certified <- as.numeric(years_certified)
  sex             <- tolower(as.character(sex))

  n               <- max(length(age), length(sex), length(years_certified))
  age             <- rep_len(age, n)
  sex             <- rep_len(sex, n)
  years_certified <- rep_len(years_certified, n)

  # ---- Input validation -----------------------------------------------------
  bad_sex <- !sex %in% c("male", "female")
  if (any(bad_sex)) {
    stop("urps_p_active: sex must be 'male' or 'female'. Found: ",
         paste(unique(sex[bad_sex]), collapse = ", "), call. = FALSE)
  }
  if (any(!is.na(years_certified) & years_certified < 0)) {
    stop("urps_p_active: years_certified must be non-negative.", call. = FALSE)
  }
  if (any(!is.na(age) & (age < 18 | age > 100))) {
    warning("urps_p_active: age(s) outside [18, 100] detected; returning 0.",
            call. = FALSE)
  }

  # ---- Scenario: age-axis shift --------------------------------------------
  shift         <- .resolve_retirement_shift(scenario_id, registry)
  effective_age <- age - shift          # positive shift = retire later = act younger

  # ---- Linear predictor ----------------------------------------------------
  female <- as.integer(sex == "female")
  eta <- coef$intercept      +
         coef$age     * effective_age   +
         coef$age_sq  * effective_age^2 +
         coef$female  * female          +
         coef$years_cert * years_certified

  # ---- Logistic transform --------------------------------------------------
  p <- 1 / (1 + exp(-eta))

  # ---- Hard boundaries (HWSM terminal age, pre-career) ---------------------
  # Terminal-age providers are never active; consistent with MICROSIM_TERMINAL_AGE.
  p[!is.na(age) & age >= MICROSIM_TERMINAL_AGE] <- 0
  p[!is.na(age) & age < 18]                     <- 0

  pmin(pmax(p, 0), 1)
}

# ---- Roster thinning --------------------------------------------------------

#' Thin a base-year provider roster by labor-force participation probability
#'
#' Converts a "treat every unconfirmed provider as active" starting roster into
#' one whose size reflects the estimated fraction of providers who are actually
#' practising. Each provider is retained with probability
#' P(active | age, sex, years_certified) and dropped otherwise — a Bernoulli
#' thinning whose expectation equals the point estimate from [urps_p_active()].
#'
#' Use this BEFORE passing the roster to [run_supply_microsimulation()] via
#' `thin_by_p_active = TRUE`, or call it directly when you want a deterministic
#' (expected-value) adjustment rather than a stochastic draw.
#'
#' @param agents A data frame / tibble of agents with at least columns `age`
#'   and optionally `sex` (defaults to `"female"`) and `years_certified`
#'   (defaults to `pmax(age - MICROSIM_AGE_AT_CERT, 0)` when absent).
#' @param coef Named list of logistic coefficients; see [URPS_P_ACTIVE_COEF].
#' @param scenario_id Passed to [urps_p_active()].
#' @param stochastic Logical. When `TRUE` (default) each row is retained by a
#'   Bernoulli draw against P(active). When `FALSE` the function returns a
#'   weighted version of `agents` with a new column `p_active` and does NOT
#'   drop rows — useful for expected-value calculations without randomness.
#' @return When `stochastic = TRUE`: a subset of `agents` (rows retained).
#'   When `stochastic = FALSE`: `agents` with an added `p_active` column.
#' @keywords internal
thin_roster_by_p_active <- function(agents,
                                     coef        = URPS_P_ACTIVE_COEF,
                                     scenario_id = NULL,
                                     stochastic  = TRUE) {
  if (!is.data.frame(agents) || nrow(agents) == 0L) return(agents)

  age <- as.numeric(agents$age)
  sex <- if ("sex" %in% names(agents)) as.character(agents$sex) else
    rep("female", nrow(agents))
  yrs <- if ("years_certified" %in% names(agents)) as.numeric(agents$years_certified) else
    pmax(age - MICROSIM_AGE_AT_CERT, 0)

  p <- urps_p_active(age, sex, yrs,
                     scenario_id = scenario_id,
                     coef        = coef)

  if (isTRUE(stochastic)) {
    # `!is.na(p) &`: an NA activity probability (e.g. from an NA age) would make
    # `keep` NA, and `agents[NA, ]` inserts a phantom all-NA row per NA index,
    # silently corrupting the roster. Treat undetermined activity as not-kept.
    keep <- !is.na(p) & stats::runif(nrow(agents)) < p
    agents[keep, , drop = FALSE]
  } else {
    agents$p_active <- p
    agents
  }
}

# ---- Model fitting -----------------------------------------------------------

#' Fit a labor force participation model from observed activity data
#'
#' When ABOG recertification or AMA Masterfile records are available, this
#' function estimates the logistic regression coefficients empirically rather
#' than using the pre-calibrated [URPS_P_ACTIVE_COEF] defaults.
#'
#' The fitted coefficient list is compatible with the `coef` argument of
#' [urps_p_active()].
#'
#' @param data A data frame with at least:
#'   \describe{
#'     \item{active}{Integer or logical: 1/TRUE = active, 0/FALSE = inactive.}
#'     \item{age}{Numeric age in completed years.}
#'     \item{sex}{Character `"male"` / `"female"` (case-insensitive).}
#'     \item{years_certified}{Numeric years since first board certification.}
#'   }
#' @param weights Optional numeric weights (e.g. survey sample weights).
#' @return A named list with the same structure as [URPS_P_ACTIVE_COEF] plus
#'   metadata elements `$model` (the fitted `glm` object), `$n`, `$source`.
#'
#' @examples
#' \dontrun{
#' abog_df <- read.csv("data-raw/abog_recert_longitudinal.csv")
#' fitted  <- fit_p_active_model(abog_df)
#' urps_p_active(65, "female", 32, coef = fitted)
#' }
#' @keywords internal
fit_p_active_model <- function(data, weights = NULL) {
  required <- c("active", "age", "sex", "years_certified")
  missing  <- setdiff(required, names(data))
  if (length(missing) > 0L) {
    stop("fit_p_active_model: missing columns: ",
         paste(missing, collapse = ", "), call. = FALSE)
  }

  data$female        <- as.integer(tolower(data$sex) == "female")
  data$active_01     <- as.integer(as.logical(data$active))
  data$age_sq        <- data$age^2

  formula <- active_01 ~ age + age_sq + female + years_certified
  fit     <- if (is.null(weights)) {
    stats::glm(formula, family = stats::binomial(), data = data)
  } else {
    stats::glm(formula, family = stats::binomial(), data = data,
               weights = weights)
  }

  cf <- stats::coef(fit)
  list(
    intercept    = unname(cf["(Intercept)"]),
    age          = unname(cf["age"]),
    age_sq       = unname(cf["age_sq"]),
    female       = unname(cf["female"]),
    years_cert   = unname(cf["years_certified"]),
    model        = fit,
    n            = nrow(data),
    source       = "fit_p_active_model() from supplied data"
  )
}

# ---- Summary table -----------------------------------------------------------

#' Active-probability table by age and sex for a given scenario
#'
#' Convenience wrapper that calls [urps_p_active()] over a grid of ages and
#' returns a tibble ready for plotting or tabular reporting.
#'
#' @param ages Integer or numeric vector of ages to evaluate. Default: 30–85.
#' @param years_certified_fn A function of `age` returning years certified.
#'   Default assumes entry at age 33 (`function(a) pmax(a - 33, 0)`).
#' @param scenario_id Passed to [urps_p_active()].
#' @param coef Passed to [urps_p_active()].
#' @param registry Passed to [urps_p_active()].
#' @return A tibble with columns `age`, `sex`, `years_certified`, `p_active`.
#' @examples
#' p_active_by_age(ages = 40:45)
#' @family urps flows
#' @concept supply
#' @export
p_active_by_age <- function(ages              = 30:85,
                             years_certified_fn = function(a) pmax(a - 33, 0),
                             scenario_id        = NULL,
                             coef               = URPS_P_ACTIVE_COEF,
                             registry           = NULL) {
  grid <- expand.grid(age = ages, sex = c("male", "female"),
                      stringsAsFactors = FALSE)
  grid$years_certified <- years_certified_fn(grid$age)
  grid$p_active <- urps_p_active(
    age             = grid$age,
    sex             = grid$sex,
    years_certified = grid$years_certified,
    scenario_id     = scenario_id,
    coef            = coef,
    registry        = registry
  )
  tibble::as_tibble(grid)
}
