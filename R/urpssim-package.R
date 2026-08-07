#' @keywords internal
#'
#' @details
#' `urpssim` projects the supply of and demand for urogynecology and
#' reconstructive pelvic surgery (URPS) providers in the United States, and
#' reports the gap between them **in the same units on both sides**.
#'
#' # Where to start
#'
#' One call runs the whole pipeline:
#'
#' ```r
#' result <- run_workforce_microsimulation(
#'   years = 2025:2050,
#'   baseline_gap_estimate = baseline_gap(
#'     base_supply_fte = 1306, adequacy = 0.95, method = "capacity_survey",
#'     calibration_status = "uncalibrated_illustrative", evidence = "example"),
#'   allow_analogy = TRUE
#' )
#' ```
#'
#' See `vignette("getting-started", package = "urpssim")` for a worked run and
#' `?run_workforce_microsimulation` for the arguments.
#'
#' # The four things that will surprise you
#'
#' This package is unusually opinionated about what it refuses to do, because
#' each refusal exists for a defect that reached production.
#'
#' \enumerate{
#'   \item **An interval is not automatically a forecast interval.** Without a
#'     [supply_parameter_spec()], every Monte Carlo iteration shares one entrant
#'     rate, so the band describes individual stochasticity only. In the
#'     2020->2023 back-test such bands covered the observation in 0 of 8 arms.
#'     [interval_label()] will not call them prediction intervals.
#'   \item **Every input declares a calibration tier.** [baseline_gap()] requires
#'     `calibration_status` rather than inferring it: identical arithmetic is
#'     *calibrated* from a fielded survey and *derived by analogy* from another
#'     specialty, and the function refuses to guess which it was handed.
#'   \item **Supply and demand are always compared as FTE.** Provider FTE over a
#'     count of cases or procedures is dimensionally meaningless;
#'     `compute_demand_coverage()` errors rather than computing it.
#'   \item **The base-year capacity anchor is not resolved.** [capacity_status()]
#'     says so in the returned object. It is a published physical-therapy
#'     distribution standing in for a URPS survey nobody has fielded, and it
#'     passes to the headline gap with a coefficient of one.
#' }
#'
#' # Finding your way around 400+ functions
#'
#' Every exported object carries a `@concept` naming its layer, so
#' `help.search("supply", package = "urpssim")` narrows to that layer, and a
#' `@family` naming its module, so each help page lists its siblings under
#' "See also". The eight layers match the `R/` filename prefixes:
#'
#' \describe{
#'   \item{`supply`}{provider cohort, hours, retirement, entrants}
#'   \item{`demand`}{population -> service volumes -> required FTE}
#'   \item{`geography`}{coordinates, placement, drive-time access}
#'   \item{`calibration`}{parameter draws, PSA, anchors to observed data}
#'   \item{`validation`}{back-test, leakage guards, coverage}
#'   \item{`reporting`}{the gap, scenarios, export contract}
#'   \item{`core`}{orchestration, paths, provenance, the SSOT contract}
#'   \item{`data`}{shipped inputs and their provenance}
#' }
#'
#' # Reading the source
#'
#' `docs/ARCHITECTURE.md` maps the code and `docs/GUARDS.md` explains each guard
#' and the defect that motivated it. Both are in the repository rather than the
#' installed package.
#'
#' @seealso [run_workforce_microsimulation()] to run the model,
#'   [baseline_gap()] to state the base-year shortfall,
#'   [capacity_status()] and [backtest_status()] for what is not yet resolved.
"_PACKAGE"

## usethis namespace: start
#' @importFrom dplyr %>%
#' @importFrom rlang .data
#' @importFrom rlang %||%
#' @importFrom splines ns
#' @importFrom stats median quantile runif rnorm setNames lm predict cor uniroot na.omit as.formula
#' @importFrom utils combn modifyList packageVersion write.csv head
## usethis namespace: end
NULL

#' Pipe operator
#'
#' @name %>%
#' @rdname pipe
#' @keywords internal
#' @export
#' @importFrom dplyr %>%
#' @usage lhs \%>\% rhs
#' @param lhs A value or the magrittr placeholder.
#' @param rhs A function call using the magrittr semantics.
#' @return The result of calling `rhs(lhs)`.
NULL

# PR #8's survey-weighted fits construct temporary offset and weight columns
# inside the model frame; R CMD check reads the names as undeclared globals.
# Column names used in dplyr/tidy NSE. Declared so R CMD check does not read
# them as undefined globals. Base objects are NOT listed here -- na.pass and
# state.abb were in this note too, and declaring them would have masked a
# missing stats:: / datasets:: qualifier rather than fixing it.
utils::globalVariables(c(
  ".hdmm_off", ".hdmm_w",
  "age",
  "age_group",
  "agent_id",
  "census_division",
  "clinical_fte",
  "exit_draw",
  "hours_per_week",
  "npi",
  "pathway",
  "peak_hrs",
  "prob_exit",
  "relative_fte",
  "sex",
  "sex_clean",
  "simulation_year",
  "sought",
  "status",
  "subspecialty_name",
  "synthetic_id"
))
