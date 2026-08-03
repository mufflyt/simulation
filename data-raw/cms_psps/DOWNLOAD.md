# CMS Medicare Physician & Other Practitioners PUF — Download Instructions

## What this file is for

The Physician/Supplier Procedure Summary (PSPS) / Medicare Physician & Other
Practitioners Public Use File (MUP_PHY) contains utilisation counts broken out
by **place of service (POS)** for every HCPCS code. For the URPS CPT basket
(defined in `R/23-cms_rvu.R::URPS_CPT_BASKET`) this tells us what fraction of
each service is delivered in an office (POS 11), hospital outpatient department
(POS 22), ambulatory surgery centre (POS 24), or inpatient hospital (POS 21).

These fractions replace the physiatry-borrowed 82/15/3 time-share defaults
in `allocate_fte_by_setting()` and seed the `URPS_DEFAULT_SETTING_MIX`
constants in `R/urps_settings.R`.

---

## Step-by-step download

### Option A — Browser (recommended, ~400 MB compressed)

1. Go to:
   <https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service>

2. Select year **2022** (most recent with stable release).

3. Click **"Download"** → **"CSV"** (the full Provider & Service file).
   Filename will be something like `MUP_PHY_R24P04_0001_D22_Prov_Svc.csv.zip`
   (~400 MB compressed, ~2.5 GB uncompressed).

4. Place the unzipped CSV here:
   ```
   data-raw/cms_psps/MUP_PHY_R24P04_0001_D22_Prov_Svc.csv
   ```

### Option B — Smaller aggregate file (Geography & Service level)

If the full Provider & Service file is too large, download the
**"By Geography and Service"** aggregate instead (~20 MB compressed).
It does not have individual provider rows but still carries `place_of_service`.

URL pattern:
```
https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-geography-and-service
```

Place the CSV here:
```
data-raw/cms_psps/MUP_PHY_R24P04_0001_D22_Geo_Svc.csv
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

The `load_psps_pos_shares()` function in `R/urps_settings.R` handles this
mapping automatically.

---

## After downloading

Run from the R console:
```r
pkgload::load_all()
shares <- load_psps_pos_shares("data-raw/cms_psps/MUP_PHY_R24P04_0001_D22_Prov_Svc.csv")
print(shares)
# Then copy the output to replace URPS_DEFAULT_SETTING_MIX in R/urps_settings.R
```

Add the file to `config/canonical_sources.yml`:
```yaml
cms_psps_2022:
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
