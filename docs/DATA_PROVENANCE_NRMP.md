# NRMP entrant series: why the source PDFs are disposable

**Short answer.** `data-raw/calibration/nrmp_urps_entrants_series.csv` carries
its own provenance on every row, a test suite enforces that it does, and a
fetcher rebuilds it from the public web. The 170 MB of NRMP PDFs cached in a
session scratchpad during acquisition were deleted without loss, and a future
acquisition should expect to delete its cache too.

This document exists because that conclusion is not obvious. The instinct on
finding 170 MB of primary-source PDFs behind a manuscript-cited series is to
preserve them. That instinct is right in general and wrong here, and the reason
it is wrong is worth writing down — it is a property the extract was *built* to
have, not luck.

---

## What the series is

`data-raw/calibration/nrmp_urps_entrants_series.csv` — 16 rows, appointment
years **2010–2025**, one row per NRMP *Results and Data: Specialties Matching
Service* report. Positions filled runs 30 (2010) to 70 (2025).

It matters because it is the only pre-cutoff evidence of URPS fellowship
expansion. The 2020→2023 back-test under-predicts in every arm, and the
diagnosis was that the pre-cutoff estimator had nothing good to work with: the
certification flow for 2018–2020 reads 40, 48, 10 — mean 32.7/yr — against a
realized 69/yr, because the 2020 examination was COVID-disrupted and its cohort
spilled into 2021. Arm 5 of the specification grid uses this series instead, and
it is the arm with the tightest intervals.

**It is a declared validation input.** `validation_inputs()` in
`scripts/validation/_provenance.R` names it, so its SHA-256 is hashed into the
manifest of every manuscript run *before* computation and re-checked at
completion. A run whose NRMP series changed underneath it fails rather than
reporting.

---

## Why the PDFs are not the record

### 1. Provenance is per row, not per file

Eleven columns, five of them provenance:

| column | example |
|---|---|
| `report_title` | `Results and Data: Specialties Matching Service, 2010 Appointment Year` |
| `table_name` | `Table 1, Fellowship Match Summary, 2010 Appointments` |
| `report_published` | `2010` |
| `available_by_year` | `2010` |
| `retrieved_on` | `2026-08-06` |
| `source_url` | `https://www.nrmp.org/wp-content/uploads/2021/07/resultsanddatasms2010.pdf` |

All 16 rows carry a **distinct** `source_url`. So for any number in the series a
reader can name the report, the table inside it, the year it became available,
the date it was retrieved, and the address to re-download it. That is strictly
more than a folder of PDFs provides, because a folder does not record *which
table on which page* a figure came from.

`available_by_year` deserves its own note: each NRMP report is published **in**
its appointment year, so this column is what a leakage audit tests against a
cutoff. It is recorded per row rather than inferred, which is why arm 5 can be
shown to use only information a modeller held in 2020.

### 2. A test enforces the property

`tests/testthat/test-nrmp-series-provenance.R` is not documentation of the
provenance — it is a gate on it. Seven blocks:

- **GATE 1, arithmetic self-verification.** `positions_filled / positions_offered`
  must reproduce the printed `pct_filled_all` within 0.15pp, for every row.
  *This proves the column mapping from the data itself, independently of any
  remembered value.* A layout change that moved a column fails here rather than
  silently returning positions offered where matches were meant.
- **GATE 2, the documented human read.** Twelve years pinned to values a person
  read off the PDF by eye.
- **Format-era fixtures.** 2010–2012, 2013–2016 and 2025 are asserted
  separately, because the row label wraps differently across eras — 2010 prints
  "Female Pelvic Medicine and" where later years print "…and Reconstructive", so
  a label-anchored parser would miss it entirely.
- **No report counted twice.** Appointment years unique, `source_url` unique,
  and `report_published == appointment_year == available_by_year`.
- **Full provenance travels with every row.** Every `report_title`,
  `table_name`, `source_url` and `retrieved_on` non-empty; every URL `https://`;
  every `table_name` containing `Table 1`.
- **Compiled series matches the CSV**, and `filled <= offered` everywhere.
- **The establishment ramp is excluded from growth estimation** — a
  first-to-last CAGR over 2010–2025 returns ~4.9%/yr by averaging a one-off
  ramp with a plateau.

The fifth of those is the one that makes this document true. The self-describing
property cannot silently decay, because a row stripped of its `source_url` fails
the suite.

### 3. A fetcher rebuilds it

`scripts/data_acquisition/07_fetch_nrmp_urps_series.R` regenerates the CSV from
the public web. It is careful in ways that matter for a re-run years from now:

- **The URL scheme is not stable** — it changes at least four times across
  2010–2025. Each URL was resolved from that year's NRMP landing page rather
  than guessed, and the resolved URLs are what the CSV stores.
- **The extractor does not rely on column position or label form.** It takes
  every candidate line and keeps only the one whose numbers satisfy the table's
  own arithmetic, i.e. GATE 1 applied at parse time.
- **The right column is named explicitly**: the entering cohort is *Matches,
  All* — positions actually filled, not offered.

Requires `pdftotext` (poppler) and network access.

### 4. An independent read corroborates it

`cliff`'s `data/nrmp_fellowship_entrants.csv` carries 70 filled positions for
2025, read by a different person from the same report. The 2025 anchor is
therefore verified across two repositories and two readers.

---

## The finding that settles it: the cache was never complete

The cached folder held **12 PDFs** — `sms_2010` … `sms_2020`, plus `sms_2025`.
The series has **16 rows**. Appointment years **2021, 2022, 2023 and 2024 were
never in the cache at all**, yet their rows exist, each with a distinct URL
under a different naming scheme:

```
2022  https://www.nrmp.org/wp-content/uploads/2022/03/2022-SMS-Results-Data-FINAL.pdf
2023  https://www.nrmp.org/wp-content/uploads/2023/04/2023-SMS-Results-and-Data-Book.pdf
2024  https://www.nrmp.org/wp-content/uploads/2024/02/2024-SMS-Results-Data-1.pdf
2025  https://www.nrmp.org/wp-content/uploads/2025/02/SMS_Results_and_Data_2025.pdf
```

So retaining the cache would have preserved **12 of 16 years while looking
complete** — worse than deleting it, because a partial archive presented as the
source of record invites someone to trust the four missing years to it.

**And the 12 are not an arbitrary subset.** They are exactly the twelve years
GATE 2 pins to a documented human read: 2010–2020 and 2025. The cache is the
by-product of the hand-verification pass, not of the series build.

That correspondence has a consequence worth stating plainly rather than leaving
implicit: **appointment years 2021–2024 rest on GATE 1 alone.** Their column
mapping is proven by the table's own arithmetic, and no human has been recorded
reading them off the page. That is a real, bounded gap in the verification — not
a reason to keep PDFs, but a reason to extend GATE 2 the next time anyone opens
those four reports.

---

## The general rule

An extract may be treated as the record, and its source cache deleted, when all
four hold:

1. **Provenance is per row**, not per file — enough to re-derive any single
   value, including which table it came from.
2. **A test enforces the provenance**, so the property cannot decay silently.
3. **A re-acquisition path exists in the repository**, and it does not assume
   the source's format or URL scheme is stable.
4. **The values are self-verifying** against something in the data, so a
   re-fetch that parses differently fails loudly instead of drifting.

The NRMP series satisfies all four. Where one fails, keep the source — and note
that (3) and (4) are the two that usually fail: a URL pasted into a comment is
not a re-acquisition path, and a number nothing cross-checks is a transcription
waiting to rot.

**What this rule does not license.** It applies to *public* sources that can be
re-fetched. It does not apply to licensed material — the AUGS/MGMA productivity
report is hashed precisely because it cannot be re-downloaded by a reader — nor
to anything whose URL is expected to disappear. For those, the artifact is the
record and the hash is the only defence.

---

## See also

- `scripts/data_acquisition/07_fetch_nrmp_urps_series.R` — the fetcher, with the
  column-identification argument in its header.
- `tests/testthat/test-nrmp-series-provenance.R` — the gates.
- `docs/DATA_DOWNLOAD_GUIDE.md` — acquisition for the other external sources.
- `docs/ENTRANT_REGIME_MODEL.md` — what the series is used for.
- `R/calibration-sources.R` — the frozen in-package transcription.

## Two neighbouring NRMP files, which are not this one

Named here because three CSVs in `data-raw/calibration/` begin with `nrmp_` and
only one is the time series:

| file | what it is |
|---|---|
| `nrmp_urps_entrants_series.csv` | **This document.** 16 rows, 2010–2025, URPS only. The declared validation input. |
| `nrmp_fellowship_entrants.csv` | A cross-sectional 2025 snapshot across subspecialties (URPS, GO, MIGS, …). Registered in `config/canonical_sources.yml` as `urps_nrmp_entrants`. |
| `nrmp_urps_track_split.csv` | The ABOG/ABU pathway split. |
