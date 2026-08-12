# Link NRMP fellowship cohorts (appointment year) to certification and
# first-billing outcomes; report the conversion fraction and lag distribution.
suppressMessages(devtools::load_all("~/simulation", quiet = TRUE))
OUT <- path.expand("~/simulation/inst/extdata/provider_year")

filled <- {n <- nrmp_entrant_series()
           stats::setNames(as.numeric(n$positions_filled), n$appointment_year)}

billing <- fellowship_first_billing_series(
  path = file.path(OUT, "provider_year_activity_long.csv"), pathway = "ABOG")
certs <- fellowship_certification_series(pathway = "ABOG")

cat("=== NRMP positions filled by appointment year ===\n"); print(filled)
cat("\n=== ABOG roster entries by first Part B billing year ===\n"); print(billing)
cat("\n=== new ABOG certifications by year ===\n"); print(certs)

cat("\n\n################ OUTCOME 1: first Medicare billing ################\n")
fb <- fit_fellowship_conversion(filled, billing, lags = 2:6,
                                coverage = FELLOWSHIP_ROSTER_ABOG_COVERAGE)
print(fb)
cat("\n-- steady-state conversion by fixed lag --\n"); print(fb$steady_state, row.names = FALSE)

cat("\n\n################ OUTCOME 2: certification ################\n")
# 2014 is the founding backlog (URPS certification began 2013) and 2020 is the
# cancelled examination. Both are administrative, not cohort flow.
fc <- fit_fellowship_conversion(filled, certs, lags = 2:6, coverage = 1,
                                exclude_years = c(2014, 2020))
print(fc)
cat("\n-- steady-state conversion by fixed lag --\n"); print(fc$steady_state, row.names = FALSE)

res <- rbind(
  data.frame(outcome = "first_medicare_billing", conversion = fb$conversion,
             mean_lag = fb$mean_lag, modal_lag = fb$modal_lag, r2 = fb$r_squared,
             n_obs = fb$n_obs, n_params = fb$n_params, coverage = fb$coverage),
  data.frame(outcome = "certification", conversion = fc$conversion,
             mean_lag = fc$mean_lag, modal_lag = fc$modal_lag, r2 = fc$r_squared,
             n_obs = fc$n_obs, n_params = fc$n_params, coverage = fc$coverage))
write.csv(res, file.path(OUT, "fellowship_conversion_fit.csv"), row.names = FALSE)
w <- rbind(
  data.frame(outcome = "first_medicare_billing", lag = fb$lags, weight = as.numeric(fb$lag_weights)),
  data.frame(outcome = "certification", lag = fc$lags, weight = as.numeric(fc$lag_weights)))
write.csv(w, file.path(OUT, "fellowship_conversion_lag_distribution.csv"), row.names = FALSE)
cat("\nwrote fellowship_conversion_fit.csv and fellowship_conversion_lag_distribution.csv\n")
