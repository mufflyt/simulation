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
