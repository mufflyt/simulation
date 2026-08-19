# Interactive Policy Intervention Simulation Dashboard ----
#
# Interactive policy scenario simulation engine and Shiny dashboard interface for
# real-time workforce policy planning (fellowship slot expansion, Medicaid
# reimbursement multipliers, APP task delegation, and retirement age shifts).

#' Simulate Interactive Policy Intervention Scenario
#'
#' @description
#' Simulates 2025-2050 URPS workforce supply, demand, and FTE gap trajectories
#' under user-selected policy intervention levers.
#'
#' @param fellowship_delta Annual change in FPMRS fellowship graduate entrants
#'   (e.g., +10 to +50 per year).
#' @param medicaid_multiplier Medicaid reimbursement multiplier (0.50 to 1.50).
#'   Higher reimbursement increases effective patient care-seeking demand.
#' @param app_delegation_rate Share of conservative evaluation visits delegated to
#'   Advanced Practice Providers (APPs, 0.0 to 0.30).
#' @param retirement_shift Shift in median retirement age in years (-3 to +3).
#' @param start_year Projection start year (default 2025).
#' @param end_year Projection end year (default 2050).
#' @return A tibble with `year`, `supply_fte`, `demand_fte`, `gap_fte`, `deficit_status`.
#' @family policy dashboard
#' @concept reporting
#' @export
simulate_policy_scenario <- function(fellowship_delta = 0L,
                                      medicaid_multiplier = 1.0,
                                      app_delegation_rate = 0.0,
                                      retirement_shift = 0.0,
                                      survival_engine = NULL,
                                      start_year = 2025L,
                                      end_year = 2050L) {

  years <- start_year:end_year

  # Baseline 2025 supply = 1,306 providers (~1,120 FTEs)
  base_supply_fte <- 1120.0
  base_entrant_fte <- (55.0 + fellowship_delta) * 0.85

  # Fit default survival engine if not passed (Tool 3 integration)
  if (is.null(survival_engine)) {
    set.seed(42)
    n_sample <- 100
    mock_history <- tibble::tibble(
      provider_id = sprintf("P%04d", seq_len(n_sample)),
      years_experience = stats::runif(n_sample, 1, 35),
      event_exit = stats::rbinom(n_sample, 1, 0.35),
      pathway = sample(c("ABOG_PLUS_ABU", "ABOG_ONLY", "ABU_ONLY"), n_sample, replace = TRUE),
      practice_setting = sample(c("office", "academic_medical_center", "community_hospital"), n_sample, replace = TRUE),
      malpractice_tier = sample(c("low", "moderate", "high"), n_sample, replace = TRUE)
    )
    survival_engine <- fit_provider_survival_hazards(mock_history, model_type = "cox_ph")
  }

  # Predict exit hazard using Tool 3 survival probability function
  mock_agents <- tibble::tibble(
    years_experience = 15.0 - retirement_shift,
    pathway = "ABOG_PLUS_ABU",
    practice_setting = "office",
    malpractice_tier = "moderate"
  )
  surv_pred <- predict_provider_survival_probability(survival_engine, mock_agents, t_years = 1.0)
  base_exit_rate <- surv_pred$exit_probability[1]

  # Compute supply trajectory over time
  supply_vec <- numeric(length(years))
  curr_supply <- base_supply_fte
  for (i in seq_along(years)) {
    supply_vec[i] <- curr_supply
    exits <- curr_supply * base_exit_rate
    entrants <- base_entrant_fte
    curr_supply <- curr_supply - exits + entrants
  }

  # Demand trajectory: demographic growth + Medicaid care-seeking multiplier - APP delegation
  base_demand_fte <- 1450.0
  demographic_growth_rate <- 0.012 # 1.2% annual growth in female 65+ population

  demand_vec <- numeric(length(years))
  for (i in seq_along(years)) {
    y_idx <- i - 1
    raw_demand <- base_demand_fte * (1 + demographic_growth_rate)^y_idx
    adjusted_demand <- raw_demand * (0.85 + 0.15 * medicaid_multiplier) * (1 - app_delegation_rate * 0.35)
    demand_vec[i] <- adjusted_demand
  }

  gap_vec <- supply_vec - demand_vec

  tibble::tibble(
    year = as.integer(years),
    supply_fte = round(supply_vec, 1),
    demand_fte = round(demand_vec, 1),
    gap_fte = round(gap_vec, 1),
    deficit_status = ifelse(gap_vec < 0, "Deficit (Shortage)", "Surplus")
  )
}

#' Launch Interactive Policy Intervention Dashboard
#'
#' @description
#' Launches an interactive Shiny web application for simulating workforce policy
#' scenarios (fellowship expansion, Medicaid multipliers, APP delegation,
#' retirement shifts) and rendering 2025-2050 supply-demand gap trajectories.
#'
#' @param launch_browser If `TRUE`, opens the dashboard in the default browser.
#' @param port Network port to listen on (default 3838).
#' @return Runs the Shiny application (blocking).
#' @family policy dashboard
#' @concept reporting
#' @export
run_policy_dashboard <- function(launch_browser = TRUE, port = 3838) {
  if (!requireNamespace("shiny", quietly = TRUE)) {
    stop("run_policy_dashboard() requires the 'shiny' package.", call. = FALSE)
  }

  ui <- shiny::fluidPage(
    shiny::titlePanel("URPS Workforce Policy Intervention Simulator (2025-2050)"),
    shiny::sidebarLayout(
      shiny::sidebarPanel(
        shiny::h4("Policy Intervention Levers"),
        shiny::sliderInput("fellowship_delta", "Fellowship Graduates Expansion (+/- per yr):",
                           min = -15, max = 50, value = 0, step = 5),
        shiny::sliderInput("medicaid_mult", "Medicaid Reimbursement Multiplier:",
                           min = 0.50, max = 1.50, value = 1.0, step = 0.05),
        shiny::sliderInput("app_delegation", "APP Task Delegation Rate (% conservative):",
                           min = 0.0, max = 0.30, value = 0.0, step = 0.05),
        shiny::sliderInput("retirement_shift", "Median Retirement Age Shift (years):",
                           min = -3.0, max = 3.0, value = 0.0, step = 0.5),
        shiny::hr(),
        shiny::helpText("Integrates Tool 3 (Provider Survival Engine) and Tool 4 (Policy Simulator).")
      ),
      shiny::mainPanel(
        shiny::tabsetPanel(
          shiny::tabPanel("Workforce Trajectory",
            shiny::h4("2025-2050 Projected National Supply vs Demand Trajectory"),
            shiny::plotOutput("trajectory_plot", height = "380px"),
            shiny::h4("Key Scenario Metrics (2050 Horizon)"),
            shiny::tableOutput("metrics_table")
          ),
          shiny::tabPanel("Provider Survival Hazards (Tool 3)",
            shiny::h4("Longitudinal Provider Cox PH Survival Probabilities"),
            shiny::plotOutput("survival_plot", height = "380px"),
            shiny::helpText("Provider career exit survival probability over 30 years of clinical experience.")
          )
        )
      )
    )
  )

  server <- function(input, output, session) {
    # Fit Tool 3 survival engine once
    survival_engine_obj <- shiny::reactive({
      set.seed(42)
      n_sample <- 150
      mock_history <- tibble::tibble(
        provider_id = sprintf("P%04d", seq_len(n_sample)),
        years_experience = stats::runif(n_sample, 1, 35),
        event_exit = stats::rbinom(n_sample, 1, 0.35),
        pathway = sample(c("ABOG_PLUS_ABU", "ABOG_ONLY", "ABU_ONLY"), n_sample, replace = TRUE),
        practice_setting = sample(c("office", "academic_medical_center", "community_hospital"), n_sample, replace = TRUE),
        malpractice_tier = sample(c("low", "moderate", "high"), n_sample, replace = TRUE)
      )
      fit_provider_survival_hazards(mock_history, model_type = "cox_ph")
    })

    scenario_data <- shiny::reactive({
      simulate_policy_scenario(
        fellowship_delta = input$fellowship_delta,
        medicaid_multiplier = input$medicaid_mult,
        app_delegation_rate = input$app_delegation,
        retirement_shift = input$retirement_shift,
        survival_engine = survival_engine_obj()
      )
    })

    output$trajectory_plot <- shiny::renderPlot({
      df <- scenario_data()
      if (requireNamespace("ggplot2", quietly = TRUE)) {
        ggplot2::ggplot(df, ggplot2::aes(x = year)) +
          ggplot2::geom_line(ggplot2::aes(y = supply_fte, colour = "Supply FTE"), linewidth = 1.2) +
          ggplot2::geom_line(ggplot2::aes(y = demand_fte, colour = "Demand FTE"), linewidth = 1.2, linetype = "dashed") +
          ggplot2::scale_colour_manual(values = c("Supply FTE" = "#2a78d6", "Demand FTE" = "#eb6834")) +
          ggplot2::labs(title = "Projected URPS Clinical FTE Supply vs Demand",
                        x = "Year", y = "Clinical FTEs", colour = "Series") +
          ggplot2::theme_minimal(base_size = 14) +
          ggplot2::theme(legend.position = "top")
      }
    })

    output$survival_plot <- shiny::renderPlot({
      eng <- survival_engine_obj()
      years <- 1:30
      mock_cohort <- tibble::tibble(
        years_experience = years,
        pathway = "ABOG_PLUS_ABU",
        practice_setting = "office",
        malpractice_tier = "moderate"
      )
      preds <- predict_provider_survival_probability(eng, mock_cohort, t_years = 1.0)
      surv_curve <- cumprod(preds$survival_probability)
      df_surv <- tibble::tibble(years = years, survival_prob = surv_curve)

      if (requireNamespace("ggplot2", quietly = TRUE)) {
        ggplot2::ggplot(df_surv, ggplot2::aes(x = years, y = survival_prob)) +
          ggplot2::geom_line(colour = "#2a78d6", linewidth = 1.2) +
          ggplot2::geom_point(colour = "#2a78d6", size = 2) +
          ggplot2::labs(title = "Provider Career Survival Curve (Tool 3 Engine)",
                        x = "Years of Clinical Experience", y = "Survival Probability S(t)") +
          ggplot2::scale_y_continuous(limits = c(0, 1)) +
          ggplot2::theme_minimal(base_size = 14)
      }
    })

    output$metrics_table <- shiny::renderTable({
      df <- scenario_data()
      y2050 <- df[df$year == 2050L, ]
      tibble::tibble(
        Metric = c("2050 Supply FTE", "2050 Demand FTE", "2050 FTE Gap (Deficit/Surplus)", "2050 Status"),
        Value = c(as.character(y2050$supply_fte), as.character(y2050$demand_fte),
                  as.character(y2050$gap_fte), y2050$deficit_status)
      )
    })
  }

  shiny::shinyApp(ui = ui, server = server, options = list(launch.browser = launch_browser, port = port))
}
