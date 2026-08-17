# Technical appendix: the URPS entrant rate

What the entrant rate is, which series measure it, and why the figure the
back-test appears to demand — 69 per year — is an artifact rather than a rate.

Prepared 2026-08-16 on `feat/chia-inpatient-surgical-layer`, R 4.4.2. All
figures are reproducible from `urps_certification_cohorts()` and
`data-raw/calibration/nrmp_urps_entrants_series.csv`. Machine-readable version:
`config/entrant_rate_source.yml`.

---

## 1. The estimand

The modelled supply stock is `board_certified_active`, keyed on
`urps_subspecialty_cert_year`. An **entrant** is therefore a physician who
becomes URPS-subspecialty-certified in a given year — **not** one who starts
fellowship.

That distinction is the whole of the problem. Two series are routinely quoted
as "entrants" and they measure different events, separated by the length of
fellowship:

| | measures | timing | role |
|---|---|---|---|
| URPS certification flow | entry into the modelled stock | the event itself | **primary** |
| NRMP filled fellowship positions | entry into training | 2–3 years earlier | corroborating leading indicator |

Fellowship is 3 years for OB-GYN and 2 for urology, so the lag is not a single
number. A 3-year lag is used throughout below and the residual sensitivity to
that choice is noted in §7.

## 2. URPS certification flow

Source: `urps_certification_cohorts()`.

| cert year | n certified | regime |
|---:|---:|---|
| 2013 | **655** | initial backlog — certification began in 2013 |
| 2014 | **175** | backlog tail |
| 2015 | **102** | backlog tail |
| 2016 | 36 | steady state |
| 2017 | 33 | steady state |
| 2018 | 40 | steady state |
| 2019 | 48 | steady state |
| 2020 | **10** | COVID examination disruption |
| 2021 | **81** | catch-up bolus |
| 2022 | 54 | |
| 2023 | 72 | |

**2013–2015 are excluded from every estimate below.** URPS subspecialty
certification began in 2013, so those years certify physicians already in
practice. Treating 655 as an annual entrant flow would overstate entry by more
than an order of magnitude.

## 3. NRMP filled fellowship positions

Source: `data-raw/calibration/nrmp_urps_entrants_series.csv`, NRMP Results and
Data (Specialties Matching Service), with `source_url` and `retrieved_on` per
row.

| appointment year | offered | filled | % filled | programs |
|---:|---:|---:|---:|---:|
| 2010 | 34 | 30 | 88.2 | 33 |
| 2011 | 40 | 40 | 100.0 | 37 |
| 2012 | 39 | 37 | 94.9 | 37 |
| 2013 | 51 | 48 | 94.1 | 45 |
| 2014 | 55 | 50 | 90.9 | 50 |
| 2015 | 58 | 57 | 98.3 | 53 |
| 2016 | 54 | 53 | 98.1 | 48 |
| 2017 | 64 | 59 | 92.2 | 59 |
| 2018 | 60 | 59 | 98.3 | 57 |
| 2019 | 64 | 58 | 90.6 | 59 |
| 2020 | 65 | 56 | 86.2 | 61 |
| 2021 | 63 | 62 | 98.4 | 61 |
| 2022 | 65 | 61 | 93.8 | 58 |
| 2023 | 65 | 61 | 93.8 | 61 |
| 2024 | 67 | 65 | 97.0 | 62 |
| 2025 | 70 | 70 | 100.0 | 66 |

Training capacity grew steadily — 33 programs and 34 positions in 2010 against
66 programs and 70 positions in 2025 — and fill rates stay between 86% and 100%.
The pipeline is not the constraint.

## 4. Certification against fellowship starts, three years earlier

| cert year | certified | ← appt year | filled | ratio |
|---:|---:|---:|---:|---:|
| 2013 | 655 | 2010 | 30 | 21.833 |
| 2014 | 175 | 2011 | 40 | 4.375 |
| 2015 | 102 | 2012 | 37 | 2.757 |
| 2016 | 36 | 2013 | 48 | 0.750 |
| 2017 | 33 | 2014 | 50 | 0.660 |
| 2018 | 40 | 2015 | 57 | 0.702 |
| 2019 | 48 | 2016 | 53 | 0.906 |
| 2020 | 10 | 2017 | 59 | **0.169** |
| 2021 | 81 | 2018 | 59 | **1.373** |
| 2022 | 54 | 2019 | 58 | 0.931 |
| 2023 | 72 | 2020 | 56 | 1.286 |

The backlog years are visible immediately: a ratio above 1 means more people
certified than entered fellowship three years earlier, which is only possible
when a stock is being drained. Ratios of 21.8, 4.4 and 2.8 are the 2013 backlog
clearing.

The 2020 and 2021 ratios — 0.169 then 1.373 — are the same disruption seen from
the other side.

**Conversion, 2016–2023:**

| basis | certified | filled | ratio |
|---|---:|---:|---:|
| pooled totals | 374 | 440 | **0.850** |
| means excluding the 2020/21 pair | 47.17 | 53.67 | 0.879 |

Roughly 85% of fellowship starts appear as certifications three years later.
That is the expected direction — attrition, delayed sitting, and physicians who
never certify.

## 5. The COVID artifact

| | value |
|---|---:|
| 2020 certifications | 10 |
| 2021 certifications | 81 |
| **2020 + 2021 pair mean** | **45.5** |
| 2019 (before) | 48 |
| 2022 (after) | 54 |

The pair mean sits between its neighbours. The 2021 spike is not a change in
entry; it is the 2020 cohort certifying a year late.

## 6. Window estimates — the number depends on where you start and stop

| window | mean/yr | years | note |
|---|---:|---:|---|
| 2016–2019 | 39.25 | 4 | pre-COVID, post-backlog |
| 2016–2023 | 46.75 | 8 | longest post-backlog window |
| 2018–2023 | 50.83 | 6 | the "50.8/yr" quoted in the supply engine warning |
| 2019–2023 | 53.00 | 5 | |
| **2021–2023** | **69.00** | 3 | **contaminated by the catch-up bolus** |
| 2021–2023, COVID pair smoothed | 57.17 | 3 | |

A factor of 1.76 separates the smallest and largest of these, all computed from
the same series. The window is not a detail.

## 7. Convergence

| instrument | window | estimate |
|---|---|---:|
| certification flow, COVID pair smoothed | cert 2021–2023 | **57.17/yr** |
| NRMP filled, lagged 3 years | appt 2018–2020 | **57.67/yr** |

Two different instruments, measuring different events three years apart, agree
to within 0.5 per year once the examination artifact is removed. This is the
strongest evidence in the appendix, precisely because neither series was
adjusted to match the other.

Residual sensitivity: the urology pathway is a 2-year fellowship, so a fraction
of each certification cohort should be lagged 2 rather than 3 years. Using a
uniform 3-year lag mixes those. The 2023 ratio of 1.286 is the most likely place
that shows up, and it is the reason no single point value is adopted below.

## 8. What this means for the back-test

The 2020→2023 back-test observes annual change of **69/yr** against **36/yr**
predicted at the shipped entrant assumption of 55. The apparent remedy is to
raise the entrant rate to ~69.

**That would be wrong.** 69/yr is §6's contaminated window: the 2020→2023
interval contains the entire catch-up bolus. Adopting it would write a one-time
examination backlog into every future year of the forecast — improving the
back-test while making the forecast worse, which is the failure mode the
calibration gates exist to prevent.

| | value |
|---|---:|
| long-run certification mean (2016–2023) | 46.75 |
| certification mean (2019–2023) | 53.00 |
| **shipped assumption** | **55** |
| NRMP-lagged (cert 2016–2023) | 55.00 |
| COVID-smoothed / NRMP-lagged convergence | 57.17 / 57.67 |
| observed 2020→2023 annual change | 69.00 |

**The shipped 55 sits inside the defensible range, not outside it.** The
back-test under-prediction is therefore not primarily an entrant-rate error, and
the remaining gap (matched arms −5.44%, coverage 0.40 against a required 0.80)
has to be explained by something else.

## 9. Status

**No single value is adopted.** The defensible range is roughly **47–58/yr**
depending on two choices that should be made deliberately rather than inferred
here:

1. how far back the window reaches, and
2. whether the forecast should represent the COVID-disrupted years as they
   happened or as smoothed.

What is established:

- the rate is **not** 69/yr;
- the shipped 55 is defensible;
- two independent instruments converge near 57 once the artifact is removed.

**What would settle it:** a certification series separating first-time
certification from recertification, and an explicit decision on the COVID
treatment. Both are recorded in `config/entrant_rate_source.yml`, and
`tests/testthat/test-entrant-rate-source.R` pins the finding so the back-test
cannot later be closed by adopting the artifact.
