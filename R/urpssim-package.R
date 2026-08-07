#' @keywords internal
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
