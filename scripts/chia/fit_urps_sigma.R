#!/usr/bin/env Rscript
# Fit the Gaussian decay bandwidth sigma for urogynaecologic inpatient surgery,
# stratified by how far the nearest capable hospital actually is.
#
# WHY STRATIFY. A kernel fitted only on women whose nearest urogyn-capable site
# is within 5 miles isolates choice from availability, but selects urban women
# who HAVE close options. Their revealed decay is necessarily tighter than that
# of a woman whose nearest option is 25 miles away, who must travel or go
# without. Fitting only the urban stratum biases sigma downward and would
# understate rural access. This fits sigma within each nearest-distance stratum
# and reports a case-weighted global value.
#
# Within a stratum, weights are normalised to the band CONTAINING that stratum's
# nearest facility -- not to band 1, which is unreachable for rural women.
suppressPackageStartupMessages({
  library(DBI); library(duckdb); library(sf); library(dplyr); library(readr)
})
DB   <- "/Volumes/MufflySamsung/DuckDB/chia_cadr.duckdb"
ZCTA <- "/Users/tylermuffly/twostep/data/tigris/cb_2020_us_zcta520_500k.shp"
OUT  <- "/Users/tylermuffly/simulation/data-raw/chia"

con <- dbConnect(duckdb::duckdb(), DB, read_only = TRUE)
od <- dbGetQuery(con, "
  WITH xw AS (SELECT DISTINCT TRY_CAST(license AS BIGINT) AS b, trim(NPI) AS npi
              FROM chia_provider.borim_stdrel_npi_straight_from_cd
              WHERE license IS NOT NULL AND trim(coalesce(NPI,'')) <> ''),
       u AS (SELECT DISTINCT trim(npi) AS npi FROM urps.provider_snapshot WHERE npi IS NOT NULL)
  SELECT c._data_year AS fy, g.zip_code AS d_zip, c.org_site,
         nullif(nullif(trim(coalesce(a.PermanentPatientZIP5CodeLDS,
                                     a.PermanentPatientZIPCode)),'-'),'') AS o_zip,
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
sy <- od |> count(fy, org_site, dlon, dlat, wt = cases, name = "sc") |> filter(sc >= 10)
od <- bind_rows(lapply(split(od, od$fy), function(dd){
  cc2 <- sy |> filter(fy == dd$fy[1]); if (!nrow(cc2)) { dd$near <- NA_real_; return(dd) }
  dd$near <- sapply(seq_len(nrow(dd)), function(i)
    min(hav(dd$olon[i], dd$olat[i], cc2$dlon, cc2$dlat))); dd })) |>
  filter(!is.na(near))

edges  <- c(5, 10, 25, 50, 100)
gnorm  <- function(d, s, ref) { g <- exp(-d^2/(2*s^2)); g / exp(-ref^2/(2*s^2)) }

fit_stratum <- function(d, ref_edge) {
  keep <- edges[edges >= ref_edge]
  b <- cut(d$miles, c(-Inf, keep, Inf), labels = c(as.character(keep), "beyond"))
  k <- tapply(d$cases, b, sum); k[is.na(k)] <- 0
  w <- as.numeric(k[seq_along(keep)])
  if (w[1] <= 0) return(NULL)
  w <- w / w[1]
  s <- optimize(function(s) sum((gnorm(keep, s, keep[1]) - w)^2), c(0.5, 400))$minimum
  list(sigma = s, w = w, bands = keep, n = sum(d$cases),
       monotone = all(diff(w) <= 1e-9))
}

strata <- list(
  list(lab = "nearest <=5 mi (urban)",  f = function(d) d$near <= 5,               ref = 5),
  list(lab = "nearest 5-10 mi",         f = function(d) d$near > 5  & d$near <= 10, ref = 10),
  list(lab = "nearest 10-25 mi",        f = function(d) d$near > 10 & d$near <= 25, ref = 25),
  list(lab = "nearest >25 mi (rural)",  f = function(d) d$near > 25,               ref = 50)
)

rows <- lapply(strata, function(s) {
  d <- od[s$f(od), ]
  if (sum(d$cases) < 100) return(NULL)
  r <- fit_stratum(d, s$ref)
  if (is.null(r)) return(NULL)
  tibble(stratum = s$lab, n_cases = r$n, ref_band_mi = s$ref,
         sigma_mi = round(r$sigma, 1), monotone = r$monotone)
})
res <- bind_rows(rows)

global <- sum(res$sigma_mi * res$n_cases) / sum(res$n_cases)
cat("=== sigma by nearest-capable-hospital distance ===\n")
print(as.data.frame(res))
cat(sprintf("\ncase-weighted GLOBAL sigma = %.1f miles  (n = %s)\n",
            global, format(sum(res$n_cases), big.mark = ",")))
cat("\nsigma in minutes across the speed assumption:\n")
for (mph in c(30, 40, 50))
  cat(sprintf("  %2d mph -> %.0f min\n", mph, global * 1.3 / mph * 60))
cat(sprintf("\ntwostep default: 60 min\n"))

res <- res |> add_row(stratum = "GLOBAL (case-weighted)", n_cases = sum(res$n_cases),
                      ref_band_mi = NA, sigma_mi = round(global, 1), monotone = NA)
write_csv(res, file.path(OUT, "urps_sigma_by_nearest_distance.csv"))
cat("\nwrote urps_sigma_by_nearest_distance.csv\n")
