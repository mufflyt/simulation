suppressPackageStartupMessages({library(DBI);library(duckdb);library(sf);library(dplyr)})
con <- dbConnect(duckdb::duckdb(), "/Volumes/MufflySamsung/DuckDB/chia_cadr.duckdb", read_only=TRUE)
# ZIP stability from the FULL cohort (large n), applied to URPS travel (small n).
stab <- dbGetQuery(con, "
WITH z AS (SELECT nullif(nullif(trim(coalesce(a.PermanentPatientZIP5CodeLDS,a.PermanentPatientZIPCode)),'-'),'') AS zip,
       CASE WHEN c._data_year BETWEEN 2007 AND 2009 THEN 'early'
            WHEN c._data_year BETWEEN 2014 AND 2018 THEN 'late' END AS era, c.PrimaryPayerType AS pt
  FROM chia_casemix.v_cohort_female_adult c
  JOIN chia_casemix.v_hdd_discharge_all_years a USING (RecordType20ID,_data_year)),
m AS (SELECT zip, era, count(*) n, avg(CASE WHEN pt IN ('4','B') THEN 1.0 ELSE 0 END) s
      FROM z WHERE zip IS NOT NULL AND era IS NOT NULL GROUP BY 1,2)
SELECT zip, max(n) FILTER (WHERE era='early') ne, max(n) FILTER (WHERE era='late') nl,
       max(s) FILTER (WHERE era='early') se, max(s) FILTER (WHERE era='late') sl
FROM m GROUP BY 1")
stab <- stab |> filter(ne>=100, nl>=100) |>
  mutate(grp = ifelse(abs(sl-se) < 0.03, "stable coverage mix", "rising Medicaid share"))

od <- dbGetQuery(con, "
  WITH xw AS (SELECT DISTINCT TRY_CAST(license AS BIGINT) b, trim(NPI) npi
              FROM chia_provider.borim_stdrel_npi_straight_from_cd
              WHERE license IS NOT NULL AND trim(coalesce(NPI,''))<>''),
       u AS (SELECT DISTINCT trim(npi) npi FROM urps.provider_snapshot WHERE npi IS NOT NULL)
  SELECT c._data_year fy, g.zip_code d_zip,
         nullif(nullif(trim(coalesce(a.PermanentPatientZIP5CodeLDS,a.PermanentPatientZIPCode)),'-'),'') o_zip,
         CASE WHEN c.PrimaryPayerType IN ('4','B') THEN 'medicaid'
              WHEN c.PrimaryPayerType IN ('6','7','8','C','D','E','J','K') THEN 'private' END pg,
         count(*) cases
  FROM chia_casemix.v_cohort_female_adult c
  JOIN chia_casemix.v_hdd_discharge_all_years a USING (RecordType20ID,_data_year)
  JOIN chia_casemix.v_hdd_discharge_procedure p USING (RecordType20ID,_data_year)
  JOIN chia_casemix.v_hdd_discharge_physician d USING (RecordType20ID,_data_year)
  JOIN xw ON xw.b=d.borim_license JOIN u ON u.npi=xw.npi
  JOIN ref.chia_facility_guide g ON g.IdOrgSite=c.org_site
  WHERE p.procedure_class='operative' AND c.AdmissionType<>'4' AND c._data_year>=2007
  GROUP BY ALL")
dbDisconnect(con, shutdown=TRUE)
od <- od |> filter(!is.na(pg), !is.na(o_zip))
z <- st_read("/Users/tylermuffly/twostep/data/tigris/cb_2020_us_zcta520_500k.shp", quiet=TRUE)
zn <- grep("ZCTA5|GEOID", names(z), value=TRUE)[1]
ce <- z |> st_transform(4326) |> st_centroid(of_largest_polygon=TRUE) |> mutate(zip=as.character(.data[[zn]])) |> select(zip)
cc <- st_coordinates(ce); ce <- tibble(zip=ce$zip, lon=cc[,1], lat=cc[,2])
c3 <- ce |> mutate(z3=substr(zip,1,3)) |> group_by(z3) |> summarise(l3=mean(lon), a3=mean(lat), .groups="drop")
pad <- function(x) sprintf("%05s", gsub("\\s","",substr(x,1,5)))
g2 <- function(d,col,pre){ d[[paste0(pre,"z")]] <- pad(d[[col]]); d$.k <- substr(d[[paste0(pre,"z")]],1,3)
  d |> left_join(ce, by=setNames("zip",paste0(pre,"z"))) |> left_join(c3, by=c(".k"="z3")) |>
    mutate(!!paste0(pre,"lon"):=coalesce(lon,l3), !!paste0(pre,"lat"):=coalesce(lat,a3)) |> select(-lon,-lat,-l3,-a3,-.k) }
od <- g2(od,"o_zip","o") |> g2("d_zip","d")
hav <- function(x1,y1,x2,y2){R<-3958.8;p<-pi/180
  a<-sin((y2-y1)*p/2)^2+cos(y1*p)*cos(y2*p)*sin((x2-x1)*p/2)^2; 2*R*asin(pmin(1,sqrt(a)))}
od <- od |> filter(!is.na(olat), !is.na(dlat)) |> mutate(miles=hav(olon,olat,dlon,dlat), zip5=pad(o_zip))
od <- od |> inner_join(stab |> mutate(zip5=pad(zip)) |> select(zip5, grp), by="zip5") |>
  mutate(era = ifelse(fy<=2010, "2007-10", ifelse(fy<=2014, "2011-14", "2015-18")))
wq <- function(x,w,p) as.numeric(quantile(rep(x, times=pmin(round(w),60)), p, na.rm=TRUE))
out <- od |> group_by(grp, era, pg) |>
  summarise(n=sum(cases), p50=round(wq(miles,cases,.50),1), .groups="drop")
cat("=== median travel, by ZIP coverage stability ===\n"); print(as.data.frame(out))
gap <- out |> select(grp,era,pg,p50) |> tidyr::pivot_wider(names_from=pg, values_from=p50) |>
  left_join(out |> group_by(grp,era) |> summarise(n=sum(n), .groups="drop"), by=c("grp","era")) |>
  mutate(gap = round(private - medicaid,1))
cat("\n=== the private-minus-Medicaid gap ===\n"); print(as.data.frame(gap))
