# Spec: build `chia_casemix.ood_urogynecology_service_events` (the six outpatient-only URPS services)

## Context

`chia_casemix.urogynecology_service_events` (built per the 2026-08-22 plan, now live and populated: 40 rows, `sling_procedure`/`prolapse_procedure` only, FY2015-2018, `setting='inpatient'` always) covers exactly two of the eight `urogynecology_service_share_registry()` services, because it is built from CHIA Hospital Discharge Data (HDD) — inpatient discharges only. That plan's own scope decision explicitly ruled the other six services (`pessary_care`, `urodynamics`, `cystoscopy`, `botox_bladder`, `ptns`, `bladder_instillation`) out as "structurally impossible from inpatient discharge data."

That ruling was correct for HDD specifically, but incomplete for CHIA as a whole: `chia_cadr.duckdb` also has `chia_casemix.ood_observation_YYYY` tables (2004-2018, "Outpatient Observation Data," ~130k-160k rows/year) that were not examined by the original plan. This document is triggered by a direct check of those tables against the six target CPT codes, run 2026-08-28:

| Service | CPT code(s) checked | OOD matches, 2015-2018 |
|---|---|---|
| Pessary care | 57160 | included in totals below |
| Urodynamics | 51725-29, 51741, 51784-85 | included in totals below |
| Cystoscopy | 52000 | included in totals below |
| Botox bladder | 52287 | included in totals below |
| PTNS | 64566 | included in totals below |
| Bladder instillation | 51720 | included in totals below |

Combined yearly match counts (any of the above CPT codes in `CPTCode1`-`CPTCode5` or `PrincipalProcedureCode`): **177 (2015), 102 (2016), 104 (2017), 86 (2018)** — and the same query against the pre-2015 tables (correcting for the column-name era split below) returns **88-285 rows every year back to 2004**. This is real, non-trivial, consistently-present volume, not a false positive from a malformed query.

## Scope decision — the honest limit, again

**"Outpatient Observation Data" is hospital-based observation-status billing, not general ambulatory/office data.** It captures encounters CHIA-reporting hospitals billed under observation status — plausibly a hospital outpatient department or hospital-affiliated ambulatory surgery unit. It does **not** capture the large share of routine pessary changes, PTNS series treatments, bladder instillations, and office cystoscopies that happen entirely in a private urogynecology practice with no hospital involvement at all. CHIA has no comprehensive private-office/ASC claims data source — this repo's only source for that setting remains NAMCS (already used for national ambulatory payer-mix, see `namcs_urps_payer_mix()`).

**Consequence for how this data may be used**: OOD-derived service-shares for these six services are a **partial, hospital-selected sample**, not a representative national or even Massachusetts-wide estimate of ambulatory URPS utilization. Two decisions follow directly from that:

1. **Do not blend OOD-derived shares into `calibrate_service_share_model()`'s primary CMS/CHIA evidence combination for these six services.** Register them as `"cross-check only, not blended"` in `practice_economics_evidence()`-style provenance — the same pattern already used for `ahrq_3prd_medicare_medicaid_ratio()` and `chia_medicare_medicaid_ratio()`, which are real, computed, and deliberately never fed into the blended default.
2. **Explicitly validate against NAMCS before drawing any conclusion from the comparison.** If OOD-derived payer-mix or relative service volume diverges sharply from the NAMCS-derived ambulatory shares already in the model, the right read is "OOD selects a different population than NAMCS captures" (expected, given the mechanism above), not "one of them is wrong." Document the comparison; do not silently pick a side (this repo's own newly-adopted meta-rule: a finding capable of changing the study's frame needs independent confirmation before being trusted, and a raw disagreement between two correctly-computed sources is not itself a defect to resolve).

**Physician attribution**: verified directly (2026-08-28) that `chia_casemix.ood_observation_2016.PhysicianNumber` joins to `chia_provider.borim_stdrel_npi_straight_from_cd.license` on simple equality — **3,054 of 3,172 distinct physician numbers matched (96.3%)**, no transformation needed, reusing the exact crosswalk the HDD-side table already uses. This crosswalk's existing NPPES-taxonomy step (`temporal_nppes_<year>_fixed`) is itself only validated FY2015-2018 (matching the existing `urogynecology_service_events` table's own window), so physician-attributed OOD output inherits the same FY2015-2018 limit. **Decision: build two outputs, not one** —

- `chia_casemix.ood_urogynecology_service_events` — physician-attributed, FY2015-2018 only, same grain/schema as the existing HDD table (`encounter_id, year, rendering_npi, service, payer_group, setting, service_events`), `setting='outpatient_observation'` (a distinct label from `'inpatient'`, honest about what it actually is, not "outpatient" unqualified which would overclaim office-visit coverage).
- `chia_casemix.ood_urogynecology_service_volume_2004_2018` — physician-blind (no `rendering_npi`), full 2004-2018 range, grain `(year, service, payer_group, setting, service_events)`. The 11 extra years of pre-attribution volume are still useful for a trend/validation view even without a name attached to each encounter, and this avoids discarding real data purely because the physician-linkage step has a narrower validated window than the source data does.

## The four real gaps and how to close each

**(a) CPT → service crosswalk (doesn't exist).** New config `config/chia_urps_outpatient_cpt_codes.yml`, small closed mapping:
```
57160                              -> pessary_care
51725,51726,51727,51728,51729,
51741,51784,51785                  -> urodynamics
52000                              -> cystoscopy
52287                              -> botox_bladder
64566                              -> ptns
51720                              -> bladder_instillation
```
Match against `CPTCode1`-`CPTCode5` (2015+ era) / `CPT1`-`CPT5` (pre-2015 era, see gap (b)) OR `PrincipalProcedureCode` — a service counts if it appears in ANY of the CPT slots or as the principal procedure, matching the "associated procedure" pattern the existing HDD `procedure_family` classification already uses.

**(b) Column-name era split (CPT1-5 vs CPTCode1-5), NOT solved by the existing `v_ood_observation_all_years` view.** Verified 2026-08-28: that view is a plain `UNION ALL BY NAME`, which means it creates a column per distinct name ever seen and leaves it `NULL` where a given source year lacks it — querying `.CPTCode1` against that view silently returns `NULL` for every 2004-2014 row, and querying `.CPT1` silently returns `NULL` for every 2015-2018 row. This is exactly the "detector blind spot" failure shape this repo's own `tests/export-registry.csv` header warns about (a technically-successful query that quietly covers less than it appears to). Fix: a new view, `chia_casemix.v_ood_observation_cpt_normalized`, that does `COALESCE(CPTCode1, CPT1) AS cpt_1` (and 2-5) plus a `_cpt_column_era` flag (`'CPTCode1-5'` / `'CPT1-5'`) so downstream code and tests can assert which era supplied a given row rather than assuming.

**(c) `payer_group` — OOD does not have `PrimaryPayerType` at all.** Verified: OOD's only payer field is `PrimarySourceOfPayment`/`SecondarySourceOfPayment` — HDD has *both* `PrimaryPayerType` (what the existing `.chia_resolve_payer_group()` resolver was built against) *and* a `PrimarySourceOfPayment`/`SecondarySourceOfPayment` pair. Whether `PrimarySourceOfPayment`'s code vocabulary matches `PrimaryPayerType`'s is **not yet verified** — Step 0 below closes this before anything else is built. If the vocabularies match, reuse `.chia_resolve_payer_group()` directly, pointed at the new column; if they differ, extend it with a documented second mapping, not a silent guess.

**(d) `setting`.** Fixed `'outpatient_observation'` for every row in both new tables — see the Scope Decision above for why this is not simply `'outpatient'`.

## Build sequence

**Step 0 — verify the payer-vocabulary question (gap c), before writing any classification code.** `SELECT PrimarySourceOfPayment, COUNT(*) FROM chia_casemix.ood_observation_2016 GROUP BY 1` vs. the same for `PrimaryPayerType` on `hdd_discharge_2016`, compare the code sets. This determines whether Step 4 is "reuse" or "extend."

**Step 1 — `chia_casemix.v_ood_observation_cpt_normalized` view.** Closes gap (b). Place in a new file `R/data-chia_ood_observation_normalization.R`, function `build_chia_ood_observation_normalized_view(con)`, following the same thin-view pattern as `build_chia_hdd_diagnosis_long_view()` in `R/data-chia_physician_attribution.R`.

**Step 2 — CPT-to-service classification, new file `R/data-chia_outpatient_cpt_family.R`.** `build_chia_ood_cpt_service_view(con, config = yaml::read_yaml("config/chia_urps_outpatient_cpt_codes.yml"))`, porting the same `EXISTS`-based associated-procedure-slot matching pattern already proven in `run_chia_revenue_setting.R`'s `fam_sql()` for the inpatient side. Adds a `service` column to a view over `v_ood_observation_cpt_normalized`.

**Step 3 — payer_group resolution (gap c), depends on Step 0's answer.** Either reuse `.chia_resolve_payer_group()` unchanged (pointed at `PrimarySourceOfPayment`) or add a documented second small mapping function in the same file, following the same closed-vocabulary-with-`Other/Public`-catchall convention as the existing one.

**Step 4 — the two assemblers, new file `R/data-chia_ood_urogynecology_service_events.R`:**
- `build_chia_ood_urogynecology_service_events(con, years = 2015:2018)` — joins the Step 2 view (filtered to the six services) → `chia_provider.borim_stdrel_npi_straight_from_cd` (verified real match) → NPPES taxonomy crosswalk (reuse the existing `temporal_nppes_<year>_fixed` join pattern from the HDD assembler) → Step 3's payer_group → `setting='outpatient_observation'` fixed → small-cell floor (same `min_cell_size=11L` default and null-the-`rendering_npi`-not-the-row behavior as the existing `build_chia_urogynecology_service_events()`). Writes `chia_casemix.ood_urogynecology_service_events`.
- `build_chia_ood_urogynecology_service_volume(con, years = 2004:2018)` — same pipeline minus the physician-attribution join (so all years are usable, not just 2015-2018), no `rendering_npi` column, no small-cell floor needed (aggregate counts, no physician identity to protect once `rendering_npi` is never in the table). Writes `chia_casemix.ood_urogynecology_service_volume_2004_2018`.

**Step 5 — NAMCS cross-check, new file `R/calibration-ood_namcs_crosscheck.R`.** `compare_ood_to_namcs_service_shares()`: for each of the six services, compute OOD's payer-mix / relative-volume shares from the Step 4 volume table and compare (report, not gate) against `namcs_urps_payer_mix()`'s national ambulatory shares. Output a tibble with both sources' numbers side by side and a qualitative flag, no automatic reconciliation.

## Testing / verification

New file `tests/testthat/test-data-chia-ood-urogynecology-service-events.R`, following the same in-memory-DuckDB synthetic-fixture convention as `test-data-chia-urogynecology-service-shares.R`:
1. Unit-test the CPT crosswalk (gap a): each of the 13 codes maps to its documented service; a code not in the list maps to nothing (not silently swallowed into a default).
2. Unit-test `v_ood_observation_cpt_normalized` (gap b) against a synthetic 2-row fixture, one row shaped like the pre-2015 era (`CPT1`-`CPT5` populated, `CPTCode1`-`CPTCode5` absent) and one shaped like 2015+ (reverse) — assert both produce the same normalized `cpt_1`..`cpt_5` output and the era flag is correctly set per row.
3. Integration test: tiny synthetic `chia_casemix`/`chia_provider` schema → assert `build_chia_ood_urogynecology_service_events()` produces a table matching `urogynecology_service_events`'s existing schema contract, and `build_chia_ood_urogynecology_service_volume()` produces the wider-year, `rendering_npi`-free table.
4. **Real-data validation (manual, not a unit test, once built):** run `compare_ood_to_namcs_service_shares()` against the real database and read the output before trusting anything derived from it — per this session's own newly-adopted meta-rule #47 (an audit/comparison result capable of changing the study's frame needs independent confirmation, not a single pass).

## In scope for v1 / explicitly deferred

**In scope:** Step 0's payer-vocabulary verification; the normalized-CPT view; the CPT-to-service crosswalk; both assemblers (physician-attributed 2015-2018 table, physician-blind 2004-2018 volume table); the NAMCS cross-check report.

**Explicitly deferred:** extending physician attribution before FY2015 (the same BORIM/NPPES-taxonomy validation-window limit the HDD-side table already accepted — not re-litigated here); treating OOD-derived shares as calibration-grade evidence to blend into `calibrate_service_share_model()`'s defaults (deliberately cross-check-only per the Scope Decision, unless a future session finds a defensible reweighting method that corrects for the hospital-observation selection bias — that would be new methodological work, not a data-engineering task); any attempt to model true private-office/ASC volume for these six services (CHIA structurally cannot answer this; would need a different data source entirely, e.g. a state all-payer claims database with office-visit-level CPT detail, which is the same APCD-class acquisition already gated in `docs/APCD_DATA_REQUEST.md` for the incident-entry estimand).

## Implementation notes (added after building, 2026-08-28)

Two real corrections to this plan's assumptions, both caught by verifying against real output rather than trusting the first pass -- per this session's own meta-rule (a blocking scientific test/finding must be proven, not assumed):

1. **Step 0's payer-vocabulary question resolved differently than expected.** OOD's `PrimarySourceOfPayment` is not a small closed code set comparable to HDD's `PrimaryPayerType` -- it is ~150 specific-insurer numeric codes (verified against `insurance_table.csv`, shipped as `inst/extdata/chia_ood_source_of_payment_lookup.csv`). `.chia_resolve_payer_group()` was NOT extended; a new, separate classifier (`.chia_ood_classify_source_of_payment()`, keyword-rule-based against the real published definition text, not a hand-transcribed 150-row table) was built instead. **No `Self-pay` code exists in this lookup at all** -- OOD-derived `payer_group` structurally never produces a `Self-pay` row, a real asymmetry with the HDD side, documented in the function's roxygen.

2. **A real bug found and fixed while validating against the live database**: a first version of `.chia_ood_resolve_payer_group()` silently folded `NA`/blank `PrimarySourceOfPayment` into `"Other/Public"` via the same fallback used for a genuinely-known-but-unmapped code. Checked against the real six-service data: **77% of matched encounters have no `PrimarySourceOfPayment` at all** -- folding that into `"Other/Public"` inflated it to 84.6% of total volume and silently misrepresented "we don't know the payer" as a real classified category. Fixed: missing/blank input now resolves to a distinct `"Unknown"` value, never conflated with `"Other/Public"`. Corrected real totals (2,049 six-service encounters, 2004-2018): Unknown 77.1%, Other/Public 7.5%, Commercial 6.1%, Medicare 5.8%, Medicaid 3.5%.

**This 77% Unknown rate is itself the most important finding from building this**: for these six specific procedure codes in OOD data, payer information is usable for barely a fifth of encounters. Combined with the pre-existing hospital-observation-selection caveat, this reinforces (does not weaken) the Scope Decision above -- OOD-derived payer shares for these six services are cross-check-only, both because of what OOD structurally can't see (office-based volume) and now also because of how much of what it DOES see lacks usable payer data.

**NAMCS cross-check, run against real data**: among the ~23% of OOD volume with a usable, comparable payer_group (Commercial/Medicare/Medicaid), OOD's Commercial share (39.9%) closely matches NAMCS's Private share (39.4%), while OOD's Medicaid share (22.8% of the comparable subset) is well above NAMCS (4.7%) and OOD's Medicare share (37.3%) is well below NAMCS (55.9%). Per the Scope Decision: this is reported, not reconciled -- a hospital-observation-selected sample and a national ambulatory survey disagreeing is expected, not evidence either source is wrong.

**Service volume is heavily skewed toward cystoscopy** (1,913 of ~2,049 real matched encounters, ~93%) -- the other five services (pessary_care, urodynamics, botox_bladder, ptns, bladder_instillation) are barely represented (1-87 encounters each over 15 years), consistent with them being predominantly office-based procedures that this hospital-observation data source structurally underrepresents.

Both real tables (`chia_casemix.ood_urogynecology_service_events`, physician-attributed 2015-2018; `chia_casemix.ood_urogynecology_service_volume_2004_2018`, physician-blind full range) are built and populated in the live database, backed up first (`chia_cadr.duckdb.bak-2026-08-28-pre-ood-events`).

## Critical files
- `R/data-chia_urogynecology_service_events.R` — the HDD-side sibling this mirrors; read first for the exact assembler pattern (join order, small-cell floor implementation, `DBI::Id()` write pattern)
- `R/data-chia_urogynecology_service_shares.R` — the consumer contract (`read_chia_service_share_events()`) both HDD and OOD tables feed
- `R/supply-practice_payer_mix.R` — `namcs_urps_payer_mix()`, the Step 5 cross-check target
- `config/chia_urps_inpatient_codes.yml` — the HDD-side sibling config to `config/chia_urps_outpatient_cpt_codes.yml` (new)
- `R/data-chia_physician_attribution.R` — the thin-view pattern (`build_chia_hdd_diagnosis_long_view()`) Step 1 follows, and the NPPES-taxonomy join Step 4 reuses
- `chia_provider.borim_stdrel_npi_straight_from_cd` — verified-real crosswalk, 96.3% match rate against OOD `PhysicianNumber` (2026-08-28 spot check, table `ood_observation_2016`)
