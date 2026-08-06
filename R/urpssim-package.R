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
# The remaining names are dplyr/tidyr columns referenced by bare name (NSE) in
# the agent-lifecycle, HRSA-FTE and care-seeking transforms -- declared here so
# `checking R code for possible problems` stays NOTE-free.
utils::globalVariables(c(
  ".hdmm_off", ".hdmm_w",
  "age", "sex", "status", "prob_exit", "exit_draw", "npi", "sought"
))
