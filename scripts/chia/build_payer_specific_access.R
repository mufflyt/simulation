#!/usr/bin/env Rscript
# How far, and how long, must a Massachusetts woman travel to reach a hospital
# that BOTH performs urogynaecologic surgery AND accepts her insurance?
#
# The supply set is payer-specific. A hospital that performs urogynaecology but
# does not serve Medicaid patients is not available to a Medicaid patient, and
# treating it as available is the central error in an access surface built on a
# single facility list. Pooled FY2007-2018, a site counts as capable-for-payer
# when it performed >= MIN_CASES URPS operations for that payer group.
#
# DISTANCE is measured (ZCTA centroids, great-circle miles).
# DRIVE TIME is approximated: miles * 1.3 circuity / mph. Reported across
# 30-50 mph because the constant moves the answer materially. True drive times
# require the HERE isochrone pipeline in mufflyt/isochrones.
suppressPackageStartupMessages({
  library(DBI); library(duckdb); library(sf); library(dplyr); library(readr); library(tibble)
})
DB   <- "/Volumes/MufflySamsung/DuckDB/chia_cadr.duckdb"
ZCTA <- "/Users/tylermuffly/twostep/data/tigris/cb_2020_us_zcta520_500k.shp"
OUT  <- "/Users/tylermuffly/simulation/data-raw/chia"
MIN_CASES <- as.integer(Sys.getenv("URPS_PAYER_MIN_CASES", "10"))

con <- dbConnect(duckdb::duckdb(), DB, read_only = TRUE)
od <- dbGetQuery(con, "
  WITH xw AS (SELECT DISTINCT TRY_CAST(license AS BIGINT) AS b, trim(NPI) AS npi
              FROM chia_provider.borim_stdrel_npi_straight_from_cd
              WHERE license IS NOT NULL AND trim(coalesce(NPI,'')) <> ''),
       u AS (SELECT DISTINCT trim(npi) AS npi FROM urps.provider_snapshot WHERE npi IS NOT NULL)
  SELECT c._data_year AS fy, c.org_site, g.zip_code AS d_zip, g.org_name,
         nullif(nullif(trim(coalesce(a.PermanentPatientZIP5CodeLDS,
                                     a.PermanentPatientZIPCode)),'-'),'') AS o_zip,
         CASE WHEN c.PrimaryPayerType IN ('4','B')             THEN 'medicaid'
              WHEN c.PrimaryPayerType IN ('6','7','8','C','D') THEN 'private'
              WHEN c.PrimaryPayerType IN ('3','F')             THEN 'medicare' END AS payer_grp,
         CASE WHEN c.age_capped < 50 THEN '18-49' WHEN c.age_capped < 65 THEN '50-64'
              WHEN c.age_capped < 80 THEN '65-79' ELSE '80+' END AS age_band,
         count(*) AS cases
  FROM chia_casemix.v_cohort_female_adult c
  JOIN chia_casemix.v_hdd_discharge_all_years a USING (RecordType20ID,_data_year)
  JOIN chia_casemix.v_hdd_discharge_procedure p USING (RecordType20ID,_data_year)
  JOIN chia_casemix.v_hdd_discharge_physician d USING (RecordType20ID,_data_year)
  JOIN xw ON xw.b = d.borim_license JOIN u ON u.npi = xw.npi
  JOIN ref.chia_facility_guide g ON g.IdOrgSite = c.org_site
  WHERE p.procedure_class='operative' AND c.AdmissionType<>'4' AND c._data_year>=2007
  GROUP BY ALL")
dbDisconnect(con, shutdown = TRUE)
od <- od |> filter(!is.na(payer_grp), !is.na(o_zip))

z <- st_read(ZCTA, quiet = TRUE)
zn <- grep("ZCTA5|GEOID", names(z), value = TRUE)[1]
ce <- z |> st_transform(4326) |> st_centroid(of_largest_polygon = TRUE) |>
  mutate(zip = as.character(.data[[zn]])) |> select(zip)
cc <- st_coordinates(ce); ce <- tibble(zip = ce$zip, lon = cc[,1], lat = cc[,2])
c3 <- ce |> mutate(z3 = substr(zip,1,3)) |> group_by(z3) |>
  summarise(l3 = mean(lon), a3 = mean(lat), .groups = "drop")
pad <- function(x) sprintf("%05s", gsub("\\s","",substr(x,1,5)))
g2 <- function(d,col,pre){ d[[paste0(pre,"z")]] <- pad(d[[col]]); d$.k <- substr(d[[paste0(pre,"z")]],1,3)
  d |> left_join(ce, by = setNames("zip", paste0(pre,"z"))) |> left_join(c3, by = c(".k"="z3")) |>
    mutate(!!paste0(pre,"lon") := coalesce(lon,l3), !!paste0(pre,"lat") := coalesce(lat,a3)) |>
    select(-lon,-lat,-l3,-a3,-.k) }
od <- g2(od,"o_zip","o") |> g2("d_zip","d")
hav <- function(x1,y1,x2,y2){R<-3958.8;p<-pi/180
  a<-sin((y2-y1)*p/2)^2+cos(y1*p)*cos(y2*p)*sin((x2-x1)*p/2)^2; 2*R*asin(pmin(1,sqrt(a)))}
od <- od |> filter(!is.na(olat), !is.na(dlat)) |> mutate(miles = hav(olon,olat,dlon,dlat))

# ---- payer-specific capable supply sets -------------------------------------
capable <- od |> count(payer_grp, org_site, dlon, dlat, org_name, wt = cases, name = "n") |>
  filter(n >= MIN_CASES)
cat(sprintf("capable sites at >= %d pooled cases:\n", MIN_CASES))
print(capable |> count(payer_grp, name = "sites") |> as.data.frame())

# nearest capable site THAT TAKES THE PATIENT'S INSURANCE
od <- bind_rows(lapply(split(od, od$payer_grp), function(dd) {
  cap <- capable |> filter(payer_grp == dd$payer_grp[1])
  dd$near_payer <- sapply(seq_len(nrow(dd)), function(i)
    min(hav(dd$olon[i], dd$olat[i], cap$dlon, cap$dlat)))
  dd
}))
# nearest capable site ignoring insurance, for the contrast
allcap <- capable |> distinct(org_site, dlon, dlat)
od$near_any_capable <- sapply(seq_len(nrow(od)), function(i)
  min(hav(od$olon[i], od$olat[i], allcap$dlon, allcap$dlat)))

wq <- function(x, w, p) as.numeric(quantile(rep(x, times = pmin(round(w), 60)), p, na.rm = TRUE))
mins <- function(mi, mph) mi * 1.3 / mph * 60

summ <- od |> filter(payer_grp %in% c("private","medicaid")) |>
  group_by(payer_grp) |>
  summarise(
    cases                   = sum(cases),
    actual_p50_mi           = round(wq(miles, cases, .50), 1),
    actual_p75_mi           = round(wq(miles, cases, .75), 1),
    actual_p90_mi           = round(wq(miles, cases, .90), 1),
    nearest_inNetwork_p50   = round(wq(near_payer, cases, .50), 1),
    nearest_inNetwork_p90   = round(wq(near_payer, cases, .90), 1),
    nearest_anyCapable_p50  = round(wq(near_any_capable, cases, .50), 1),
    nearest_anyCapable_p90  = round(wq(near_any_capable, cases, .90), 1),
    insurance_penalty_p50   = round(wq(near_payer, cases, .50) - wq(near_any_capable, cases, .50), 1),
    insurance_penalty_p90   = round(wq(near_payer, cases, .90) - wq(near_any_capable, cases, .90), 1),
    .groups = "drop")

cat("\n=== DISTANCE (measured, miles) ===\n"); print(as.data.frame(summ))
cat("\n=== DRIVE TIME to nearest IN-NETWORK urogyn hospital (approximated) ===\n")
tt <- summ |> select(payer_grp, nearest_inNetwork_p50, nearest_inNetwork_p90) |>
  rowwise() |>
  mutate(p50_30mph = round(mins(nearest_inNetwork_p50,30)), p50_40mph = round(mins(nearest_inNetwork_p50,40)),
         p50_50mph = round(mins(nearest_inNetwork_p50,50)), p90_30mph = round(mins(nearest_inNetwork_p90,30)),
         p90_40mph = round(mins(nearest_inNetwork_p90,40)), p90_50mph = round(mins(nearest_inNetwork_p90,50))) |>
  ungroup()
print(as.data.frame(tt))
write_csv(summ, file.path(OUT, "payer_specific_access_distance.csv"))
write_csv(tt,   file.path(OUT, "payer_specific_access_drivetime.csv"))
cat("\nwrote payer_specific_access_{distance,drivetime}.csv\n")
