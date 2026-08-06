# CMS Medicare Physician & Other Practitioners PUF — Download Instructions

## What this file is for

The Physician/Supplier Procedure Summary (PSPS) / Medicare Physician & Other
Practitioners Public Use File (MUP_PHY) contains utilisation counts broken out
by **place of service (POS)** for every HCPCS code. For the URPS CPT basket
(defined in `R/data-cms_rvu.R::URPS_CPT_BASKET`) this tells us what fraction of
each service is delivered in an office (POS 11), hospital outpatient department
(POS 22), ambulatory surgery centre (POS 24), or inpatient hospital (POS 21).

These fractions replace the physiatry-borrowed 82/15/3 time-share defaults
in `allocate_fte_by_setting()` and seed the `URPS_DEFAULT_SETTING_MIX`
constants in `R/supply-urps_settings.R`.

---

## Step-by-step download

### Option A — Browser (recommended, ~400 MB compressed)

1. Go to:
   <https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service>

2. Select year **2024** (most recent stable release as of August 2026; file is
   `MUP_PHY_R26_P05_V10_D24_Geo.csv` inside the zip).

3. Click **"Download"** → **"ZIP"** (the compressed Geography & Service file,
   ~13 MB compressed, ~40 MB uncompressed).

4. Unzip and place the CSV here:
   ```
   data-raw/cms_psps/MUP_PHY_R26_P05_V10_D24_Geo.csv
   ```

### Option B — Full Provider & Service file (~3.2 GB unzipped)

Download the **"By Provider and Service"** file if you need individual provider-level
place-of-service detail. The 2024 filename is `PHY_R26_P05_V10_D24_Prov_Svc.csv`.

URL:
```
https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service
```

Place the CSV here:
```
data-raw/cms_psps/PHY_R26_P05_V10_D24_Prov_Svc.csv
```

Then pass `file_type = "geo_svc"` to `load_psps_pos_shares()`.

---

## Columns used

| Column | Description |
|--------|-------------|
| `HCPCS_CD` | CPT/HCPCS procedure code |
| `PLACE_OF_SRVC` | F = facility, O = non-facility (office) |
| `TOT_SRVCS` | Total services rendered |

The file does **not** use numeric POS codes (11, 22, 24, 21). It uses a
binary `F` / `O` split. For URPS purposes:
- `O` (non-facility/office) → maps to `"office"` setting
- `F` (facility) → maps to either `"hospital_outpatient"`, `"asc"`, or
  `"operative"` depending on the CPT code (surgical codes → `"operative"`;
  E/M and diagnostics at facility → `"hospital_outpatient"`)

The `load_psps_pos_shares()` function in `R/supply-urps_settings.R` handles this
mapping automatically.

---

## After downloading

Run from the R console:
```r
pkgload::load_all()
source("scripts/calibrate_setting_mix_from_psps.R")
print(shares)
# Then copy the output to replace URPS_DEFAULT_SETTING_MIX in R/supply-urps_settings.R
```

Add the file to `config/canonical_sources.yml`:
```yaml
cms_psps_2024:
  path: data-raw/cms_psps/MUP_PHY_R24P04_0001_D22_Prov_Svc.csv
  sha256: ""   # fill in after download: tools::md5sum() or sha256sum
```

---

## Why the download cannot be automated

The CMS data portal requires a session cookie / JavaScript redirect for bulk
file downloads. The direct file URLs return HTTP 302 → not-found without a
valid browser session. `curl` and `wget` cannot complete the download without
the cookie. ResDAC (CMS Research Data Assistance Center) provides programmatic
access but requires a signed DUA.
