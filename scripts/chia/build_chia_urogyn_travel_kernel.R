#!/usr/bin/env Rscript
# Urogynaecology-specific surgical travel kernel.
#
# The all-surgery kernel (build_chia_surgical_travel_kernel.R) measures travel
# against ALL acute hospitals. That overstates availability for urogynaecology:
# only 18-30 of ~76 Massachusetts hospitals host any URPS operation in a given
# year, and only 4-16 reach 10 cases. The correct availability denominator is
# the set of hospitals that actually perform this surgery.
#
# "Urogynaecologic surgery" is defined here by the OPERATOR -- an operation on
# the female-adult cohort performed by a board-certified URPS surgeon (NPI
# matched to urps.provider_snapshot). A procedure-code definition awaits
# config/chia_urps_inpatient_codes.yml; hand-rolled code families break across
# the ICD-9/ICD-10 seam and are not used.

suppressPackageStartupMessages({
  library(DBI); library(duckdb); library(sf); library(dplyr)
})

DB   <- "/Volumes/MufflySamsung/DuckDB/chia_cadr.duckdb"
ZCTA <- "/Users/tylermuffly/twostep/data/tigris/cb_2020_us_zcta520_500k.shp"
OUT  <- "/Users/tylermuffly/simulation/data-raw/chia"
MIN_CASES <- as.integer(Sys.getenv("URPS_SITE_MIN_CASES", "10"))

con <- dbConnect(duckdb::duckdb(), DB, read_only = TRUE)
message("querying URPS operations ...")
od <- dbGetQuery(con, "
  WITH xw AS (SELECT DISTINCT TRY_CAST(license AS BIGINT) AS b, trim(NPI) AS npi
              FROM chia_provider.borim_stdrel_npi_straight_from_cd
              WHERE license IS NOT NULL AND trim(coalesce(NPI,'')) <> ''),
       u AS (SELECT DISTINCT trim(npi) AS npi FROM urps.provider_snapshot WHERE npi IS NOT NULL)
  SELECT c._data_year AS fy, g.zip_code AS d_zip, g.org_name, c.org_site,
         nullif(nullif(trim(coalesce(a.PermanentPatientZIP5CodeLDS,
                                     a.PermanentPatientZIPCode)), '-'), '') AS o_zip,
         CASE WHEN c.age_capped < 50 THEN '18-49' WHEN c.age_capped < 65 THEN '50-64'
              WHEN c.age_capped < 80 THEN '65-79' ELSE '80+' END AS age_band,
         c.PrimaryPayerType AS payer, c.race_primary,
         count(*) AS cases
  FROM chia_casemix.v_cohort_female_adult c
  JOIN chia_casemix.v_hdd_discharge_all_years a USING (RecordType20ID, _data_year)
  JOIN chia_casemix.v_hdd_discharge_procedure p USING (RecordType20ID, _data_year)
  JOIN chia_casemix.v_hdd_discharge_physician d USING (RecordType20ID, _data_year)
  JOIN xw ON xw.b = d.borim_license
  JOIN u  ON u.npi = xw.npi
  JOIN ref.chia_facility_guide g ON g.IdOrgSite = c.org_site
  WHERE p.procedure_class = 'operative' AND c.AdmissionType <> '4'
    AND c._data_year >= 2007
  GROUP BY ALL")
dbDisconnect(con, shutdown = TRUE)
message(sprintf("  %s cases at %d distinct sites",
                format(sum(od$cases), big.mark = ","), n_distinct(od$org_site)))

message("loading ZCTA centroids ...")
z <- st_read(ZCTA, quiet = TRUE)
zn <- grep("ZCTA5|GEOID", names(z), value = TRUE)[1]
cent <- z |> st_transform(4326) |> st_centroid(of_largest_polygon = TRUE) |>
  mutate(zip = as.character(.data[[zn]])) |> select(zip)
cc <- st_coordinates(cent)
cent <- tibble(zip = cent$zip, lon = cc[,1], lat = cc[,2])
cent3 <- cent |> mutate(zip3 = substr(zip,1,3)) |> group_by(zip3) |>
  summarise(lon3 = mean(lon), lat3 = mean(lat), .groups = "drop")

pad <- function(x) sprintf("%05s", gsub("\\s", "", substr(x, 1, 5)))
geo <- function(d, zipcol, pre) {
  d[[paste0(pre,"_zip")]] <- pad(d[[zipcol]])
  d$.z3 <- substr(d[[paste0(pre,"_zip")]], 1, 3)
  d <- d |> left_join(cent, by = setNames("zip", paste0(pre,"_zip"))) |>
    left_join(cent3, by = c(".z3" = "zip3")) |>
    mutate(!!paste0(pre,"_lon") := coalesce(lon, lon3),
           !!paste0(pre,"_lat") := coalesce(lat, lat3)) |>
    select(-lon, -lat, -lon3, -lat3, -.z3)
  d
}
od <- geo(od, "o_zip", "o") |> geo("d_zip", "d")

hav <- function(lon1, lat1, lon2, lat2) {
  R <- 3958.8; p <- pi/180
  a <- sin((lat2-lat1)*p/2)^2 + cos(lat1*p)*cos(lat2*p)*sin((lon2-lon1)*p/2)^2
  2*R*asin(pmin(1, sqrt(a)))
}
od <- od |> filter(!is.na(o_lat), !is.na(d_lat)) |>
  mutate(miles = hav(o_lon, o_lat, d_lon, d_lat))
message(sprintf("  geocoded %.1f%% of cases", 100*sum(od$cases)/sum(od$cases)))

# ---- availability: nearest URPS-CAPABLE hospital, per year -------------------
# A site counts as urogyn-capable in a year if it hosted >= MIN_CASES URPS
# operations that year. Sensitivity over the threshold is written out too.
site_year <- od |> count(fy, org_site, d_zip, d_lon, d_lat, wt = cases, name = "site_cases")

nearest_for <- function(threshold) {
  cap <- site_year |> filter(site_cases >= threshold)
  purrr_map <- lapply(split(od, od$fy), function(dd) {
    cc <- cap |> filter(fy == dd$fy[1])
    if (nrow(cc) == 0) { dd$near_urps <- NA_real_; return(dd) }
    dd$near_urps <- sapply(seq_len(nrow(dd)), function(i)
      min(hav(dd$o_lon[i], dd$o_lat[i], cc$d_lon, cc$d_lat)))
    dd
  })
  bind_rows(purrr_map)
}

od2 <- nearest_for(MIN_CASES)

# All-hospital nearest, for the contrast that motivated this script.
con <- dbConnect(duckdb::duckdb(), DB, read_only = TRUE)
allf <- dbGetQuery(con, "SELECT DISTINCT zip_code FROM ref.chia_facility_guide WHERE zip_code IS NOT NULL")
dbDisconnect(con, shutdown = TRUE)
allf$zip <- pad(allf$zip_code)
allf <- allf |> left_join(cent, by = "zip") |>
  mutate(z3 = substr(zip,1,3)) |> left_join(cent3, by = c("z3"="zip3")) |>
  mutate(lon = coalesce(lon, lon3), lat = coalesce(lat, lat3)) |> filter(!is.na(lat))
org <- od2 |> distinct(o_zip, o_lon, o_lat)
org$near_any <- sapply(seq_len(nrow(org)), function(i)
  min(hav(org$o_lon[i], org$o_lat[i], allf$lon, allf$lat)))
od2 <- od2 |> left_join(org[,c("o_zip","near_any")], by = "o_zip")

band <- function(m) cut(m, c(-Inf,5,10,25,50,100,Inf),
                        labels = c("0-5","5-10","10-25","25-50","50-100",">100"))
res <- od2 |>
  mutate(actual = band(miles), near_u = band(near_urps), near_a = band(near_any))

tot <- sum(res$cases)
cmp <- full_join(
  res |> count(band = actual, wt = cases, name = "c") |> mutate(urogyn_actual = c/sum(c)) |> select(band, urogyn_actual),
  res |> count(band = near_u, wt = cases, name = "c") |> mutate(nearest_urps_capable = c/sum(c)) |> select(band, nearest_urps_capable),
  by = "band") |>
  full_join(res |> count(band = near_a, wt = cases, name = "c") |>
              mutate(nearest_any_hospital = c/sum(c)) |> select(band, nearest_any_hospital), by = "band") |>
  mutate(across(-band, ~round(.x, 4)))
print(as.data.frame(cmp))
write.csv(cmp, file.path(OUT, "urogyn_travel_vs_availability.csv"), row.names = FALSE)

qs <- quantile(rep(res$miles, times = pmin(res$cases, 40)), c(.25,.5,.75,.9,.95,.99))
qn <- quantile(rep(res$near_urps, times = pmin(res$cases, 40)), c(.25,.5,.75,.9,.95,.99), na.rm=TRUE)
qa <- quantile(rep(res$near_any, times = pmin(res$cases, 40)), c(.25,.5,.75,.9,.95,.99), na.rm=TRUE)
qt <- data.frame(quantile = names(qs), urogyn_actual_miles = round(as.numeric(qs),1),
                 nearest_urps_capable_miles = round(as.numeric(qn),1),
                 nearest_any_hospital_miles = round(as.numeric(qa),1))
print(qt); write.csv(qt, file.path(OUT,"urogyn_travel_quantiles.csv"), row.names=FALSE)

cat(sprintf("\ncases: %s\n", format(tot, big.mark=",")))
cat(sprintf("travelled >10 miles past nearest URPS-capable site: %.1f%%\n",
            100*sum(res$cases[res$miles > res$near_urps + 10], na.rm=TRUE)/tot))

# ---- Stratification by patient demographics ---------------------------------
# Payer, age band and race. Reported as distance quantiles and as the share
# travelling past their nearest urogyn-capable site, because the second is the
# access-relevant quantity: it isolates travel that scarcity imposed.

payer_lab <- c("1"="Self-pay","2"="Workers comp","3"="Medicare","F"="Medicare MC",
               "4"="Medicaid","B"="Medicaid MC","5"="Other govt","6"="Blue Cross",
               "C"="Blue Cross MC","7"="Commercial","D"="Commercial MC","8"="HMO")

strat <- function(d, key) {
  d |> filter(!is.na(.data[[key]]), !is.na(near_urps)) |>
    mutate(beyond = as.integer(miles > near_urps + 10)) |>
    group_by(grp = .data[[key]]) |>
    summarise(
      n_cases        = sum(cases),
      p50_miles      = round(quantile(rep(miles, times = pmin(cases, 40)), .50), 1),
      p75_miles      = round(quantile(rep(miles, times = pmin(cases, 40)), .75), 1),
      p90_miles      = round(quantile(rep(miles, times = pmin(cases, 40)), .90), 1),
      p50_nearest    = round(quantile(rep(near_urps, times = pmin(cases, 40)), .50), 1),
      pct_beyond_10mi = round(100 * sum(cases * beyond) / sum(cases), 1),  # uses the vector, not n_cases
      .groups = "drop") |>
    filter(n_cases >= 100) |>
    mutate(stratum = key) |> arrange(desc(n_cases))
}

res_p <- res |> mutate(payer_label = coalesce(payer_lab[as.character(payer)],
                                              paste0("Other (", payer, ")")))
by_payer <- strat(res_p, "payer_label")
by_age   <- strat(res,   "age_band")
by_race  <- strat(res |> mutate(race_grp = as.character(race_primary)), "race_grp")

demog <- bind_rows(by_payer, by_age, by_race) |>
  select(stratum, group = grp, cases = n_cases, p50_miles, p75_miles, p90_miles,
         p50_nearest_capable = p50_nearest, pct_beyond_10mi)
print(as.data.frame(demog))
write.csv(demog, file.path(OUT, "urogyn_travel_by_demographics.csv"), row.names = FALSE)

sens <- bind_rows(lapply(c(1,10,25), function(th) {
  d <- nearest_for(th)
  data.frame(min_cases = th,
             median_nearest_miles = round(median(rep(d$near_urps, times=pmin(d$cases,40)), na.rm=TRUE),1),
             sites = nrow(site_year |> filter(site_cases >= th) |> distinct(fy, org_site)))
}))
print(sens); write.csv(sens, file.path(OUT,"urogyn_site_threshold_sensitivity.csv"), row.names=FALSE)
