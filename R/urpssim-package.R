#' @keywords internal
"_PACKAGE"

## usethis namespace: start
#' @importFrom dplyr %>%
#' @importFrom rlang .data
#' @importFrom rlang %||%
#' @importFrom splines ns
#' @importFrom stats median quantile runif rnorm setNames cut lm predict cor uniroot na.omit as.formula
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
utils::globalVariables(c(".hdmm_off", ".hdmm_w"))

# The Fraher agent engine (R/38) and HRSA FTE calibration (R/40) reference these
# columns inside dplyr data-masking verbs; R CMD check reads them as undeclared
# globals.
utils::globalVariables(c(
  "age", "age_group", "agent_id", "census_division", "clinical_fte", "exit_draw",
  "hours_per_week", "npi", "pathway", "peak_hrs", "prob_exit", "relative_fte",
  "sex", "sex_clean", "simulation_year", "state.abb", "status",
  "subspecialty_name", "synthetic_id"
))
