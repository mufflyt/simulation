#!/usr/bin/env Rscript
# Empirical surgical travel kernel from CHIA inpatient operations.
#
# The E2SFCA layer in mufflyt/twostep uses generic Luo/Qi distance decay
# (E2SFCA_DEFAULT_WEIGHTS: 30=1.00, 60=0.68, 120=0.22, 180=0.09). Those are
# reasonable for general accessibility but are not urogynaecology patients
# travelling for a major operation. This measures what they actually did.
#
# Origin  = patient residential ZIP  (99%+ populated FY2007-2018)
# Dest    = facility ZIP from ref.chia_facility_guide (all 77 sites)
# Cohort  = female, 18+, operative principal procedure, newborn stays excluded

suppressPackageStartupMessages({
  library(DBI); library(duckdb); library(sf); library(dplyr); library(tidyr)
})

DB   <- "/Volumes/MufflySamsung/DuckDB/chia_cadr.duckdb"
ZCTA <- "/Users/tylermuffly/twostep/data/tigris/cb_2020_us_zcta520_500k.shp"
OUT  <- "/Volumes/MufflySamsung/chia_cadr_build/travel"

con <- dbConnect(duckdb::duckdb(), DB, read_only = TRUE)

# Aggregate to ZIP-pair level first: ~250k cases collapse to a few thousand pairs.
message("querying origin-destination pairs ...")
od <- dbGetQuery(con, "
  WITH src AS (
    SELECT c._data_year AS fy, c.age, c.age_capped, c.PrimaryPayerType AS payer,
           c.org_site,
           nullif(nullif(trim(coalesce(a.PermanentPatientZIP5CodeLDS,
                                       a.PermanentPatientZIPCode)), '-'), '') AS o_zip
    FROM chia_casemix.v_cohort_female_adult c
    JOIN chia_casemix.v_hdd_discharge_all_years a USING (RecordType20ID, _data_year)
    JOIN chia_casemix.v_hdd_discharge_procedure p USING (RecordType20ID, _data_year)
    WHERE p.procedure_class = 'operative' AND c.AdmissionType <> '4'
      AND c._data_year >= 2007)
  SELECT s.fy, s.o_zip, g.zip_code AS d_zip, g.org_name,
         CASE WHEN s.age_capped < 50 THEN '18-49'
              WHEN s.age_capped < 65 THEN '50-64'
              WHEN s.age_capped < 80 THEN '65-79'
              ELSE '80+' END AS age_band,
         s.payer,
         count(*) AS cases
  FROM src s JOIN ref.chia_facility_guide g ON g.IdOrgSite = s.org_site
  WHERE s.o_zip IS NOT NULL AND g.zip_code IS NOT NULL
  GROUP BY ALL")
dbDisconnect(con, shutdown = TRUE)
message(sprintf("  %s O-D rows, %s cases", format(nrow(od), big.mark=","),
                format(sum(od$cases), big.mark=",")))

message("loading ZCTA centroids ...")
z <- st_read(ZCTA, quiet = TRUE)
zn <- grep("ZCTA5|GEOID", names(z), value = TRUE)[1]
cent <- z |>
  st_transform(4326) |>
  st_centroid(of_largest_polygon = TRUE) |>
  mutate(zip = as.character(.data[[zn]])) |>
  select(zip)
cc <- st_coordinates(cent)
cent <- tibble(zip = cent$zip, lon = cc[,1], lat = cc[,2])

pad <- function(x) sprintf("%05s", gsub("\\s", "", substr(x, 1, 5)))
od <- od |> mutate(o_zip = pad(o_zip), d_zip = pad(d_zip))

# Seven hospitals hold UNIQUE institutional ZIPs with no ZCTA (Baystate 01199,
# Lahey Burlington 01805, UMass University 01655, Mercy Springfield 01102,
# Lawrence General 01842, Cooley Dickinson 01061, Noble 01086). Dropping them
# loses 15.9% of cases -- and they are western/central MA, exactly where travel
# is longest, so the loss biases the kernel toward short trips. Fall back to the
# centroid of the surrounding ZIP3 area, accurate well within a 30-minute band.
cent3 <- cent |>
  mutate(zip3 = substr(zip, 1, 3)) |>
  group_by(zip3) |>
  summarise(lon3 = mean(lon), lat3 = mean(lat), .groups = "drop")

od <- od |>
  left_join(cent, by = c("o_zip" = "zip")) |> rename(o_lon = lon, o_lat = lat) |>
  left_join(cent, by = c("d_zip" = "zip")) |> rename(d_lon = lon, d_lat = lat) |>
  mutate(o_zip3 = substr(o_zip, 1, 3), d_zip3 = substr(d_zip, 1, 3)) |>
  left_join(cent3, by = c("o_zip3" = "zip3")) |>
  mutate(o_lon = coalesce(o_lon, lon3), o_lat = coalesce(o_lat, lat3),
         o_approx = is.na(o_lon) | !is.na(lon3) & is.na(o_lat)) |>
  select(-lon3, -lat3) |>
  left_join(cent3, by = c("d_zip3" = "zip3")) |>
  mutate(d_approx = is.na(d_lat),
         d_lon = coalesce(d_lon, lon3), d_lat = coalesce(d_lat, lat3)) |>
  select(-lon3, -lat3)

matched <- sum(od$cases[!is.na(od$o_lat) & !is.na(od$d_lat)])
message(sprintf("  geocoded %.1f%% of cases", 100 * matched / sum(od$cases)))

# Haversine great-circle miles.
hav <- function(lon1, lat1, lon2, lat2) {
  R <- 3958.8; p <- pi/180
  a <- sin((lat2-lat1)*p/2)^2 + cos(lat1*p)*cos(lat2*p)*sin((lon2-lon1)*p/2)^2
  2*R*asin(pmin(1, sqrt(a)))
}
od <- od |>
  filter(!is.na(o_lat), !is.na(d_lat)) |>
  mutate(miles = hav(o_lon, o_lat, d_lon, d_lat),
         # Straight-line -> road distance ~1.3x, then ~40 mph effective door-to-door.
         # Approximate: the isochrones repo has true drive times when we want them.
         drive_min = miles * 1.3 / 40 * 60,
         band = cut(drive_min, c(-Inf, 30, 60, 120, 180, Inf),
                    labels = c("<=30", "31-60", "61-120", "121-180", ">180")))

saveRDS(od, file.path(OUT, "chia_od_pairs.rds"))

overall <- od |> count(band, wt = cases, name = "cases") |>
  mutate(share = cases / sum(cases),
         cum   = cumsum(share))
print(as.data.frame(overall))
write.csv(overall, file.path(OUT, "travel_bands_overall.csv"), row.names = FALSE)

by_age <- od |> count(age_band, band, wt = cases, name = "cases") |>
  group_by(age_band) |> mutate(share = cases/sum(cases)) |> ungroup()
write.csv(by_age, file.path(OUT, "travel_bands_by_age.csv"), row.names = FALSE)

by_year <- od |> count(fy, band, wt = cases, name = "cases") |>
  group_by(fy) |> mutate(share = cases/sum(cases)) |> ungroup()
write.csv(by_year, file.path(OUT, "travel_bands_by_year.csv"), row.names = FALSE)
message("wrote travel_bands_{overall,by_age,by_year}.csv")
