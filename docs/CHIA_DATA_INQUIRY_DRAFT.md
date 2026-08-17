# CHIA data inquiry — DRAFT, not sent

Two questions in one message: the OOD physician-field discontinuity we hit in
the existing Case Mix extract, and whether the MA APCD can supply longitudinal
ambulatory follow-up that Case Mix structurally cannot.

**Status: unsent. Requires review before transmission.** Nothing in this draft
should be sent without confirming the recipient address and that the DUA
citation below is accurate for our current agreement.

---

**To:** CHIA Data Requests (confirm current address before sending)
**Subject:** Case Mix OOD physician field, FY2016 onward — and APCD availability
for longitudinal ambulatory analysis

Dear CHIA Data Team,

I am writing with two questions about data we hold under our current Case Mix
DUA, in connection with a workforce-supply study of urogynecology (Female Pelvic
Medicine and Reconstructive Surgery) in Massachusetts.

**1. Outpatient Observation physician identifiers, FY2016 onward**

Working with the Outpatient Observation Database, we observe that a growing set
of hospitals stops populating the physician identifier field beginning in FY2016
— 14 facilities initially, rising to 17 and then 20 in subsequent years. The
same facilities continue to populate the corresponding field in the Hospital
Inpatient Discharge Database over the identical period, and we confirmed the
pattern against the source files rather than our own processing, so it does not
appear to be an artifact of our extract.

The practical consequence is that the surviving facilities are a non-random
subset (roughly 15-22 of about 70 sites), so we cannot treat post-FY2015
observation-stay physician data as a usable series.

Could you tell us:

- whether a submission-specification or guidance change around FY2016 made this
  field optional, or altered how it is expected to be populated, for the
  Outpatient Observation Database specifically;
- whether the affected records can be re-derived or backfilled from another
  submitted element; and
- whether CHIA considers the post-FY2015 observation physician field usable for
  provider-level analysis, or whether it should be treated as discontinued.

We are not asking for any additional identified data — only for guidance on
whether this field can support the analysis, so that we describe its limits
correctly.

**2. All-Payer Claims Database availability**

The Case Mix databases bind acute care hospitals under 957 CMR 8.00, so
freestanding ambulatory surgery centers do not submit, and there is no
ambulatory-surgery database. Most urogynecologic surgery is now performed in the
ambulatory setting, and our office-visit parameters require longitudinal
follow-up across a full year, which a discharge-based file cannot express.

We would like to know:

- what the current application pathway, timeline, and fee schedule are for MA
  APCD data;
- whether a limited data set with encounter dates and a stable de-identified
  member linkage across years is available at the release level we would
  qualify for;
- whether professional/office claims are included at that release level, since
  our need is ambulatory follow-up rather than facility claims; and
- whether ambulatory surgery performed at freestanding centers is captured.

Our specific analytic need is the annual probability that a patient under care
for a pelvic floor disorder returns for further care in the following year, and
the number of visits associated with that care. We attempted this in MEPS using
Panels 27 and 24 and exhausted it: the baseline cohort was 20 patients in one
panel and 11 in the other, which cannot support an estimate.

Happy to provide our study protocol, IRB determination, or a more specific
variable list if that would help route the question.

With thanks for your time,

Tyler Muffly, MD
Denver Health / Department of Obstetrics and Gynecology
tyler.muffly@dhha.org

---

## Review checklist before sending

- [ ] Confirm the current CHIA data-request address and whether the OOD question
      and the APCD question route to different teams (they may; consider splitting).
- [ ] Confirm the DUA reference and that describing the FY2016 pattern discloses
      no cell sizes or identifiable content. As written it reports facility
      *counts*, not facility identities or patient data.
- [ ] Confirm institutional affiliation line and whether a co-investigator or
      the IRB protocol number should appear.
- [ ] Decide whether to state the intended publication venue.
- [ ] Confirm the MEPS sample sizes cited (20 and 11) match the committed
      analysis before quoting them externally.
