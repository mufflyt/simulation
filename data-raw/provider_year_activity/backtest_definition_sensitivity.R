# Back-test sensitivity to the ACTIVITY DEFINITION used as the scoring target.
#
# The forecast is run ONCE. Every arm, seed, parameter and Monte Carlo draw is
# identical across definitions; only the observed series being scored against
# changes. No forecast parameter is tuned to any target, and none can be:
# run_backtest() never sees the definitions.
#
# Targets are built by rescaling the contract certification series by the
# roster's observed activity RATE, r_k(y) = D_k(y) / D0(y). D0 reproduces the
# production target exactly (r == 1), which is the consistency check.
#
# ASSUMPTION, stated because it is not testable here: the 1,092-NPI roster is
# representative of the 1,306 certified urogynecologists w.r.t. activity. The
# roster covers 83.6% of the 2023 certified population.

suppressMessages({library(dplyr); devtools::load_all("~/simulation", quiet = TRUE)})
SCR <- "/private/tmp/claude-501/-Users-tylermuffly-simulation/1f034148-4385-41e2-9828-bd0c2abe4fc4/scratchpad"
OUT <- path.expand("~/simulation/inst/extdata/provider_year")
panel <- readRDS(file.path(SCR, "panel.rds"))
DEFS <- c("d0_certified","d1_any_partb","d2_meaningful_partb","d3_multisource","d4_high_confidence")

bt <- run_backtest()
cy <- bt$settings$cutoff_year; ty <- bt$settings$target_year
yrs <- cy:ty
contract <- bt$observed[as.character(yrs)]

rate <- panel |> group_by(year) |> summarise(across(all_of(DEFS), ~ sum(.x, na.rm = TRUE)),
                                             d0n = sum(d0_certified), .groups = "drop")
targets <- lapply(DEFS, function(d) {
  r <- setNames(rate[[d]] / rate$d0n, rate$year)[as.character(yrs)]
  if (any(is.na(r)) || any(r == 0)) return(NULL)
  setNames(as.numeric(contract) * as.numeric(r), as.character(yrs))
})
names(targets) <- DEFS
targets <- Filter(Negate(is.null), targets)

it <- bt$iterations
per_year <- it |> group_by(arm, apply_attrition, year) |>
  summarise(med = median(headcount),
            lo95 = quantile(headcount, .025, names = FALSE),
            hi95 = quantile(headcount, .975, names = FALSE), .groups = "drop")

res <- lapply(names(targets), function(d) {
  obs <- targets[[d]]
  per_year |> filter(year %in% yrs, year > cy) |>
    mutate(o = as.numeric(obs[as.character(year)])) |>
    group_by(arm, apply_attrition) |>
    summarise(definition = d,
              MAE = mean(abs(med - o)), RMSE = sqrt(mean((med - o)^2)),
              bias = mean(med - o), pct_bias = 100 * mean((med - o) / o),
              coverage_95 = mean(o >= lo95 & o <= hi95),
              n_years = n(),
              target_year_obs = o[year == ty], target_year_pred = med[year == ty],
              .groups = "drop")
}) |> bind_rows() |>
  group_by(definition) |> mutate(rank_by_rmse = rank(RMSE, ties.method = "min")) |> ungroup() |>
  arrange(definition, rank_by_rmse)

write.csv(res, file.path(OUT, "backtest_definition_sensitivity.csv"), row.names = FALSE)
saveRDS(list(res = res, targets = targets, contract = contract), file.path(SCR, "bt_sens.rds"))

cat("\n=== targets by definition ===\n")
print(as.data.frame(do.call(rbind, lapply(targets, round))))
cat("\n=== arm ranking by RMSE, per definition (attrition arms only) ===\n")
print(res |> filter(apply_attrition) |>
      transmute(definition, arm = substr(arm, 1, 46), rank_by_rmse,
                MAE = round(MAE,1), RMSE = round(RMSE,1), bias = round(bias,1),
                cov95 = coverage_95) |> as.data.frame(), row.names = FALSE)
