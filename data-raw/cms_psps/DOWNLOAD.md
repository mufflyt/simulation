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

Both files are plain HTTPS objects and `curl` fetches them unauthenticated. The
stable way to find the URL is the DCAT catalogue rather than the portal page,
because the download URLs embed a release-dated path that changes each refresh:

```sh
curl -s https://data.cms.gov/data.json |
  python3 -c 'import json,sys
for d in json.load(sys.stdin)["dataset"]:
    if d["title"].startswith("Medicare Physician & Other Practitioners"):
        print(d["title"])
        for r in d.get("distribution", []):
            if r.get("format") == "CSV":
                print("  ", r.get("title","")[-10:], r["downloadURL"])'
```

Then download. Both are uncompressed CSV — there is no ZIP resource.

```sh
# By Geography and Service -- national/state totals by HCPCS  (42 MB)
curl -L --fail --retry 5 -o data-raw/cms_psps/MUP_PHY_R26_P05_V10_D24_Geo.csv \
  https://data.cms.gov/sites/default/files/2026-05/e534c74b-79b8-4892-8a95-5a17e2dfec9f/MUP_PHY_R26_P05_V10_D24_Geo.csv

# By Provider and Service -- NPI x HCPCS x POS               (3.1 GB)
curl -L --fail --retry 5 -C - -o data-raw/cms_psps/PHY_R26_P05_V10_D24_Prov_Svc.csv \
  https://data.cms.gov/sites/default/files/2026-05/b5ebab5a-f490-418a-9bce-4b9f31419356/PHY_R26_P05_V10_D24_Prov_Svc.csv
```

Both are gitignored by the blanket `*.csv` rule; `data-raw/cms_psps/` is not
whitelisted, so neither can be committed by accident.

### Verified 2024 release (data year 2024, published 2026-05-11)

| file | bytes | sha256 |
|---|---:|---|
| `PHY_R26_P05_V10_D24_Prov_Svc.csv` | 3,250,282,192 | `509dc7ce4cd02d8dd160d50d33ce5d942cd120ea306ff1eb2b6ece4f59cb2c23` |
| `MUP_PHY_R26_P05_V10_D24_Geo.csv` | 42,078,094 | `c26956788333d03c0080017121c19e8e4d9990e9fa8ff385d7e1a2849c45074a` |

Provider & Service: 9,781,673 rows, 1,207,473 distinct NPIs, 28 columns.
Geography & Service: 268,350 rows.

---

## YOU ALMOST CERTAINLY NEED BOTH FILES

**Do not compute a national denominator by summing the provider file.** CMS
suppresses any NPI × HCPCS × POS cell serving fewer than 11 beneficiaries, and
that rule removes low-volume providers specifically. Against the Geography file's
national totals for the URPS basket, the provider file captures:

| service | prov-file / national | captured |
|---|---:|---:|
| prolapse_procedure | 29,593 / 73,523 | 40.2% |
| pessary_care | 36,250 / 73,363 | 49.4% |
| sling_procedure | 14,650 / 26,964 | 54.3% |
| ptns | 97,198 / 157,306 | 61.8% |
| botox_bladder | 57,471 / 83,974 | 68.4% |
| bladder_instillation | 123,618 / 173,341 | 71.3% |
| new_consultation | 21,656,866 / 24,372,607 | 88.9% |
| urodynamics | 523,884 / 584,330 | 89.7% |
| cystoscopy | 800,242 / 826,866 | 96.8% |
| return_visit | 176,125,248 / 179,791,316 | 98.0% |

Two basket codes vanish entirely — 51992 (104 services nationally) and 57268
(643) — because no single provider reached 11 beneficiaries. 57287 retains 2.0%.

The 96.8% pooled capture rate is an artifact of E/M volume and is **not** a
reassurance: on the operative codes the loss is 45-60%, and it is the
generalist tail that disappears while high-volume subspecialists are retained.
Any share-of-provider-type computed from the provider file alone is therefore
biased **toward the subspecialist**, most severely for exactly the surgical
services. Use the Geography file for denominators and the provider file for
numerators, and report the capture rate alongside any share.

---

## Columns used

CMS ships mixed-case names; `load_psps_pos_shares()` matches case-insensitively,
so both the historical upper-case names and the ones below resolve.

| Column | Description |
|--------|-------------|
| `HCPCS_Cd` | CPT/HCPCS procedure code |
| `Place_Of_Srvc` | F = facility, O = non-facility (office) |
| `Tot_Srvcs` | Total services rendered |
| `Tot_Benes` | Distinct beneficiaries (drives the <11 suppression) |
| `Rndrng_NPI` | Provider & Service file only — the join key to the URPS roster |
| `Rndrng_Prvdr_Type` | Provider & Service file only — see the caveat below |

**There is no FPMRS / urogynecology provider type in this file.** On the twelve
operative basket codes the types present are Obstetrics & Gynecology (28,365
services, 572 NPIs), Urology (6,439 / 172), Physician Assistant, Ambulatory
Surgical Center, Nurse Practitioner and Gynecological Oncology. CMS pools URPS
subspecialists with generalists exactly as the carrier specialty code does in
the CADR archive, so `Rndrng_Prvdr_Type` distinguishes *discipline*, never
*subspecialty*. Splitting URPS from generalist requires joining `Rndrng_NPI` to
`data-raw/urps_roster/`; 1,176 of 1,495 roster NPIs (78.7%) appear on at least
one basket code.

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

Add the files to `config/canonical_sources.yml` using the hashes tabulated above.

Note that `load_psps_pos_shares()` calls `utils::read.csv()`, which is viable for
the 42 MB Geography file and not for the 3.1 GB provider file. Read the latter
with `data.table::fread(select = ...)`; the seven columns needed for the URPS
basket subset extract in about 16 seconds and yield 1,586,026 rows over 602,880
NPIs.

---

## On automating the download

An earlier revision of this file asserted that the CMS portal requires a session
cookie and that `curl` and `wget` cannot complete the download. **That is
false.** The DCAT `downloadURL`s above return HTTP 200 to unauthenticated `curl`
and transferred the full 3.1 GB at ~30 MB/s. The claim appears to have come from
hitting the human-facing portal page rather than the catalogue-listed object.

The ResDAC caveat is a separate matter and remains true, but it does not apply
here: these are public use files, and nothing in them is DUA-restricted.
