# Interactive URPS Simulation Workbench Helper -------------------------

#' Launch Interactive URPS Simulation Workbench Dashboard
#'
#' @description
#' Launches the interactive Shiny application for running URPS workforce supply and demand
#' simulation scenarios, exploring audit ledgers, and evaluating policy interventions.
#'
#' @param port Optional port number for the Shiny web server.
#' @param launch_browser Logical; if TRUE (default), automatically opens the browser.
#'
#' @return Runs the Shiny application.
#' @family ui
#' @concept dashboard
#' @export
run_workbench <- function(port = NULL, launch_browser = TRUE) {
  if (!requireNamespace("shiny", quietly = TRUE)) {
    base::stop("Install the `shiny` package to run the simulation workbench.", call. = FALSE)
  }

  app_dir <- base::system.file("shiny", package = "urpssim")
  if (app_dir == "" || !base::dir.exists(app_dir)) {
    # Fallback to local inst/shiny if uninstalled
    app_dir <- base::file.path(base::getwd(), "inst", "shiny")
  }

  if (!base::dir.exists(app_dir)) {
    base::stop("Could not locate the Shiny workbench application directory.", call. = FALSE)
  }

  base::message("Launching URPS Workforce Simulation Workbench from: ", app_dir)
  shiny::runApp(appDir = app_dir, port = port, launch.browser = launch_browser)
}
