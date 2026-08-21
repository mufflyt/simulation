# Interactive URPS Workforce Simulation & Policy Workbench ----------------

library(shiny)
library(dplyr)
library(ggplot2)
library(scales)
library(urpssim)

ui <- fluidPage(
  theme = bslib::bs_theme(
    version = 5,
    bootswatch = "flatly",
    primary = "#1B365D",
    secondary = "#4B6B94"
  ),
  titlePanel(
    windowTitle = "URPS Workforce Simulation & Policy Workbench",
    title = div(
      h2("URPS Health Workforce Simulation Workbench", class = "fw-bold text-primary mb-1"),
      p("National & Subnational Urogynecology Workforce Supply, Demand, & Policy Scenarios", class = "text-muted lead fs-6")
    )
  ),
  sidebarLayout(
    sidebarPanel(
      width = 3,
      h4("Scenario Parameters", class = "fw-bold border-bottom pb-2"),
      sliderInput("year_range", "Simulation Horizon:", min = 2025, max = 2040, value = c(2025, 2035), step = 1, sep = ""),
      numericInput("initial_providers", "Initial URPS Providers:", value = 1200, min = 500, max = 3000, step = 50),
      numericInput("fellowship_entrants", "Annual Fellowship Entrants:", value = 55, min = 20, max = 150, step = 5),
      sliderInput("app_delegation", "APP Delegation Share:", min = 0.0, max = 0.40, value = 0.15, step = 0.05),
      sliderInput("medicaid_fee_ratio", "Medicaid-to-Medicare Fee Ratio:", min = 0.40, max = 1.30, value = 0.75, step = 0.05),
      hr(),
      actionButton("run_sim", "Run Simulation Scenario", class = "btn-primary w-100 fw-bold py-2"),
      br(), br(),
      downloadButton("download_audit", "Export Audit Ledger (CSV)", class = "btn-outline-secondary btn-sm w-100")
    ),
    mainPanel(
      width = 9,
      tabsetPanel(
        type = "pills",
        tabPanel(
          "Executive Summary",
          br(),
          fluidRow(
            column(3, div(class = "card text-white bg-primary mb-3 p-3 text-center rounded-3 shadow-sm",
                          h6("Baseline Adequacy", class = "card-title text-uppercase opacity-75 small"),
                          h3(textOutput("base_adequacy_kpi"), class = "fw-bold mb-0"))),
            column(3, div(class = "card text-white bg-info mb-3 p-3 text-center rounded-3 shadow-sm",
                          h6("End Horizon Adequacy", class = "card-title text-uppercase opacity-75 small"),
                          h3(textOutput("end_adequacy_kpi"), class = "fw-bold mb-0"))),
            column(3, div(class = "card text-white bg-success mb-3 p-3 text-center rounded-3 shadow-sm",
                          h6("Cumulative Served Patients", class = "card-title text-uppercase opacity-75 small"),
                          h3(textOutput("total_served_kpi"), class = "fw-bold mb-0"))),
            column(3, div(class = "card text-white bg-warning mb-3 p-3 text-center rounded-3 shadow-sm",
                          h6("Cumulative Unserved/Delayed", class = "card-title text-uppercase opacity-75 small"),
                          h3(textOutput("total_unserved_kpi"), class = "fw-bold mb-0")))
          ),
          br(),
          h5("National Workforce Supply vs. Demand Trajectory", class = "fw-bold"),
          plotOutput("supply_demand_plot", height = "380px")
        ),
        tabPanel(
          "Patient-Flow Conservation Ledger",
          br(),
          h5("Annual Patient Conservation Audit Ledger", class = "fw-bold"),
          p("Verifies the conservation identity: Served Patients + Unserved/Delayed = Appointment Requests"),
          tableOutput("audit_ledger_table")
        ),
        tabPanel(
          "Provider Capacity & Workload",
          br(),
          h5("Annual Effective Clinical FTE vs. Required Workload", class = "fw-bold"),
          plotOutput("capacity_workload_plot", height = "380px")
        ),
        tabPanel(
          "Model Documentation & Methodology",
          br(),
          h5("URPS Microsimulation Architecture", class = "fw-bold"),
          p("This simulation couples 8 annual event sequence steps using empirical benchmarks from MEPS, NHANES, ACGME, CMS, and KFF:"),
          tags$ul(
            tags$li(b("Step A:"), " Individual micro-population aging and sample weight updates."),
            tags$li(b("Step B:"), " Pelvic floor disease stage transitions (UI, POP, AI)."),
            tags$li(b("Step C:"), " Care-seeking and subspecialty referral cascades."),
            tags$li(b("Step D:"), " Geographic & Medicaid insurance access clearing."),
            tags$li(b("Step E:"), " Queue & appointment capacity allocation."),
            tags$li(b("Step F:"), " Service delivery & wRVU workload conversion."),
            tags$li(b("Step G:"), " 306-HRR spatial balance accounting."),
            tags$li(b("Step H:"), " Provider lifecycle (attrition, ACGME entrants, spatial relocation).")
          )
        )
      )
    )
  )
)

server <- function(input, output, session) {
  sim_results <- eventReactive(input$run_sim, {
    run_end_to_end_simulation(
      start_year = as.integer(input$year_range[1]),
      end_year = as.integer(input$year_range[2]),
      n_agents = 1000L,
      initial_provider_count = as.numeric(input$initial_providers),
      fellowship_entrants = as.numeric(input$fellowship_entrants),
      app_delegation_rate = as.numeric(input$app_delegation),
      medicaid_fee_ratio = as.numeric(input$medicaid_fee_ratio)
    )
  }, ignoreNULL = FALSE)

  output$base_adequacy_kpi <- renderText({
    res <- sim_results()
    base_val <- res$audit_ledger_tbl$overall_adequacy_rate[[1]]
    sprintf("%.1f%%", 100 * base_val)
  })

  output$end_adequacy_kpi <- renderText({
    res <- sim_results()
    end_val <- tail(res$audit_ledger_tbl$overall_adequacy_rate, 1)
    sprintf("%.1f%%", 100 * end_val)
  })

  output$total_served_kpi <- renderText({
    res <- sim_results()
    total_val <- sum(res$audit_ledger_tbl$served_patients_n)
    comma(total_val)
  })

  output$total_unserved_kpi <- renderText({
    res <- sim_results()
    total_val <- sum(res$audit_ledger_tbl$unserved_delayed_n)
    comma(total_val)
  })

  output$supply_demand_plot <- renderPlot({
    res <- sim_results()
    df <- res$audit_ledger_tbl

    ggplot(df, aes(x = year)) +
      geom_line(aes(y = total_supplied_fte, color = "Supplied Clinical FTE"), size = 1.3) +
      geom_line(aes(y = total_required_fte, color = "Required Clinical FTE"), size = 1.3, linetype = "dashed") +
      scale_color_manual(values = c("Supplied Clinical FTE" = "#1B365D", "Required Clinical FTE" = "#D9534F")) +
      labs(x = "Simulation Year", y = "Full-Time Equivalent Providers (FTE)", color = "Legend") +
      theme_minimal(base_size = 14) +
      theme(legend.position = "top", panel.grid.minor = element_blank())
  })

  output$audit_ledger_table <- renderTable({
    res <- sim_results()
    res$audit_ledger_tbl |>
      mutate(
        overall_adequacy_rate = sprintf("%.1f%%", 100 * overall_adequacy_rate),
        hrr_deficit_share = sprintf("%.1f%%", 100 * hrr_deficit_share)
      )
  }, striped = TRUE, hover = TRUE, bordered = TRUE)

  output$capacity_workload_plot <- renderPlot({
    res <- sim_results()
    df <- res$audit_ledger_tbl

    ggplot(df, aes(x = year, y = overall_adequacy_rate)) +
      geom_area(fill = "#1B365D", alpha = 0.2) +
      geom_line(color = "#1B365D", size = 1.3) +
      scale_y_continuous(labels = scales::percent_format(accuracy = 1), limits = c(0, 1)) +
      labs(x = "Simulation Year", y = "National Workforce Adequacy Score", title = "National Adequacy Trajectory") +
      theme_minimal(base_size = 14) +
      theme(panel.grid.minor = element_blank())
  })

  output$download_audit <- downloadHandler(
    filename = function() {
      paste0("urps_simulation_audit_ledger_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv")
    },
    content = function(file) {
      res <- sim_results()
      readr::write_csv(res$audit_ledger_tbl, file)
    }
  )
}

shinyApp(ui = ui, server = server)
