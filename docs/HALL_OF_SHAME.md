# Hall of shame

`GUARDS.md` catalogues the guards and the defect that motivated each. This file
catalogues the **mistakes themselves** — including the ones made while building
those guards, by the people and agents building them. It exists because the
defect classes below recur, and because a repository that records only its
successes teaches nothing about how the failures got in.

Rules for this file:

- Every entry is real, dated and attributable. Nothing hypothetical.
- Entries are kept even when the fix was fast. Speed of repair says nothing
  about how the defect entered.
- The most valuable column is **what caught it** — and the most valuable
  entries are the ones where the honest answer is *nothing did*.

Sorted by the lesson, not by date, because the lessons repeat.

---

## I. Guards that did not guard

The worst class. A guard that is silently inert is worse than no guard, because
the absence of an alarm gets read as evidence of health.

### 1. A gate that refused everything looked safe and delivered nothing

**2026-08-08, mine.** The first manuscript-eligibility gate free-text scanned
`MANIFEST.txt` for the tokens `exploratory`, `fallback`, `failed`. Every
manifest `begin_validation_run()` writes carries the line:

```
exploratory          FALSE
```

The scan matched its own **field name** and refused all five authoritative run
directories — a declaration of eligibility read as evidence of ineligibility.

Dangerous because the failure mode is invisible in the direction that matters.
A gate that refuses everything produces no bad output, looks maximally strict,
and would have been "fixed" by whoever next needed a table — by relaxing it.

**Caught by:** running it. It failed loudly on all five sources at once, which
is the only reason the design flaw surfaced instead of a token being quietly
narrowed.

**Rule:** a structured field must be read as structure. `parse_manifest()` now
parses key/value and asserts `exploratory` is present and `FALSE`; only the
*values* are scanned. Pinned in
`tests/testthat/test-manuscript-eligibility.R` in **both** directions, because
"admits what is eligible" is the direction that actually broke.

### 2. The export registry a stray row had silently disabled

**2026-08-08.** `demand_dimension_status,api` had been appended **above** the
`"export","category"` header in `tests/export-registry.csv`. `read.csv` took
the data row as the header, `reg$export` came back `NULL`, and **all four
export-wiring gates collapsed at once** — reporting 449/449 exports as
unregistered while asserting nothing.

**Caught by:** reading the gate's output because a number looked implausible.
Not by the gate. Not by the suite.

**Rule:** the file now carries an ordering comment naming this incident. A
registry whose schema can be destroyed by an append is a registry that needs a
schema assertion, not a comment — that remains open.

### 3. A guard wired to nothing, and a fix that hid itself from the detector

**2026-08-08, mine.** `assert_no_coverage_rate_claim()` was implemented, tested
and called by nothing. Wiring it into `interval_label()`, I wrote:

```r
say <- function(x) { assert_no_coverage_rate_claim(x, status); x }
```

The orphan detector drops any line matching `name <- function`, so the fix
**hid the call from the very gate meant to notice it**. Folding it onto one
line made the wiring invisible to the detector that motivated the wiring.

**Caught by:** the gate, on the first attempt. The one entry in this file where
the machinery worked exactly as designed against its own author.

**Sequel, 2026-08-09 — the workaround was the wrong fix.** Moving the call to
its own line satisfied the detector without addressing why the detector was
blind, and the blind spot bit a different session within the day. A concurrent
commit exported `isochrone_source_dir()` and called it twice — as the default
argument of `verify_canonical_isochrones()` and `assert_canonical_isochrones()`.
Both call sites sit **on those functions' definition lines**, so the whole-line
drop deleted them, and the gate reported an orphan that two exported functions
were calling. `main` went red on a false positive.

The detector now **trims the `name <- function` head instead of dropping the
line**, which keeps both properties: the defined name is gone so it cannot mark
itself used, and the argument list survives so a default-argument call counts.
Orphans fell 57 → 56 with no unregistered entries and no registry row becoming
stale, so `isochrone_source_dir` was the only function the bug had been hiding.

The lesson is not about R parsing. **A guard that produces a false positive
teaches people to edit around it**, and each accommodation makes the underlying
defect harder to see — the first workaround is what let the second failure look
like ordinary bookkeeping. The obvious repair here was to register the function
as an orphan and bump the ratchet; that would have been two lines, would have
gone green, and would have written a false statement into the registry while
leaving the blind spot for the next caller.

### 4. A gitignore rule that swallowed source, silently — three times now

**2026-08-08, mine.** `*/manuscript/`, written for simulation *output* folders,
matched `scripts/manuscript/`. `git status` simply did not mention the two new
source files. Nothing errored; the work just would not have been committed.

The aggravating fact: `.gitignore` **already documents an identical incident**
twenty lines above, where `*_projections*` swallowed
`man/gap_projections_all_scenarios.Rd` and R CMD check reported the function as
undocumented no matter how often the docs were regenerated.

**Caught by:** listing what `git add` would stage, rather than trusting
`git status` to be complete.

**Rule:** whitelist narrowly (`!scripts/manuscript/`), never by broadening the
original pattern — whatever `*/manuscript/` was written to exclude still
exists. And a broad ignore pattern is a silent-failure generator: prefer
anchored paths.

**Third instance, 2026-08-09, and the sharpest.** `98ad3f8` rewrote the README
to lead with three flowcharts, three figures and three maps. The blanket `*.png`
meant `git add -A` **skipped all six new figures and reported success**, so the
README went live with six broken images.

What makes it the best example in this file: *that same commit was fixing this
exact defect for a different image* — a README embedding a file the repository
does not contain — and reintroduced it sixfold in the act of fixing it. The
follow-up `c30b03e` states the lesson better than a rule could:

> `git add -A` succeeding says nothing about whether a file was added when an
> ignore rule excludes it; only `git ls-files` does.

Which is precisely how the second instance was caught — by listing what
`git add` *would* stage rather than trusting `git status` to mention it. The
technique existed, in this file, and did not travel to the next person.

### 4b. One broken documentation link blocked every push, for days

**2026-08-09, the concurrent session's find.** Two roxygen blocks referenced
`[BACKTEST_CAREER_CHANGE_HAZARD]`, a constant defined at
`R/validation-backtest_run.R:57` and **not exported**. A link to an unexported
object cannot resolve, so `R CMD check` reported *"Missing link or links"* —
and because the pre-push gate runs `error_on = "warning"`, that single square
bracket blocked **every push in the repository**, across sessions, since
`8597b8f`.

The pairing with entry 1 is the point. That was a gate that refused everything
and therefore delivered nothing; this is a gate that refused everything and
therefore *delivered nothing*, from the opposite cause — one correct, strict
rule meeting one trivial typo. Strictness is not free, and the cost is paid at
the moment of least patience.

**The tempting wrong fix was export.** Adding the constant to the NAMESPACE
resolves the link in one line. It would also have enlarged the public API to
accommodate a typo, and created a new unregistered orphan for the gate in entry
3 to catch. The repository's own convention decided it instead:
`E2SFCA_DEFAULT_WEIGHTS`, `MICROSIM_ENTRY_AGE`,
`WORKFORCE_OUTLOOK_ADEQUATE_MIN` and `URPS_FELLOWSHIP_YEARS` are all unexported
signature-default constants, and none is referenced with `[...]`. These two were
the anomaly, so they were de-linked to backticks.

---

## II. Primitives that are silently wrong

No error, no warning, plausible output, wrong number.

### 5. `formatC(x, format = "d")` truncates

**2026-08-08, mine.** A 95% lower bound of `1070.975` rendered as **1,070**.
Every fractional bound in the specification table was low by up to a full unit,
in the direction of the null, invisibly.

**Caught by:** eyeballing one rendered bound against its source CSV — an
accident, not a check.

**Rule:** `round()` before `formatC()`. There is still no test asserting that a
rendered value round-trips to its artifact; that is the real fix and it is open.

### 6. `nzchar(NA)` is `TRUE`

**2026-08-08, mine.** Six NA-NPI roster rows survived a blank filter and
entered the numerator's key set **while being reported as zero blanks**. It
would not have stopped the run; it would have shifted a bound.

**Rule:** never filter missingness with a string predicate. `is.na()` first,
explicitly.

### 7. `data.table::fread()` has no `comment.char`

**2026-08-08, mine.** The provider-type mapping's rationale header parsed as
data; all 25 provider types came back unmapped.

**Caught by:** the mapping's own design — unknown types **stop the run**. The
loud-failure requirement working on its author within minutes of being written.
Had the mapping defaulted unknown types to `physician`, the run would have
completed with every provider silently misclassified.

### 8. An alignment heuristic that tested row 1

**2026-08-08, mine.** Right-align inference looked at the first value only, so
the Specification column went right because it begins `1. Derived cohort`.
Cosmetic — listed because it is the same bug shape as #6: a predicate applied
to a sample and generalised to a column.

---

## III. Claims made faster than they were checked

Assertions stated with confidence, then withdrawn. Each was stated to a human
who could have acted on it.

### 9. "The URPS split is definitively not in the archive we have"

**2026-08-07, mine.** False. The Massachusetts Dropbox tree contains BORIM with
NPI and the hash→physician crosswalk. Withdrawn and corrected. The word
*definitively* was doing work that the search behind it had not earned.

### 10. "Five duplicate NPIs"

**2026-08-08, mine.** They were five duplicate `NA`s. There are **zero** true
duplicate NPIs. Corrected in the frozen prespecification by explicit erratum
rather than silent edit.

### 11. "1,498 rows with cert_year ≤ 2024"

**2026-08-08, mine.** Computed across all 1,500 rows including the six with no
NPI. Correct figure: **1,492**. Same erratum.

### 12. "CMS requires a session cookie; curl and wget cannot complete the download"

**Pre-existing, in `data-raw/cms_psps/DOWNLOAD.md`.** False, and it had been
standing instruction to download 3.1 GB by hand. The DCAT `downloadURL`s return
HTTP 200 to unauthenticated `curl` and transferred the full file at ~30 MB/s.
The likely origin: someone hit the human-facing portal page rather than the
catalogue-listed object, and wrote the conclusion into the runbook.

**Rule:** a claim that something is impossible is a claim, and belongs to the
same evidentiary standard as a result. The correction is now in the file, named
as false rather than quietly replaced.

---

## IV. Numbers that drifted from their source

### 13. The Tier A upper bound was wrong in five places

**2026-08-08.** The artifact says `89.5459`, which rounds to **89.5**.
`VALIDATION_RESULTS.md` said **89.6** in five locations, and the error had
propagated into conversation and back into two paragraphs I wrote by copying
the existing value.

**Caught by:** the first run of `scripts/manuscript/build_tables.R`. The
generator found the error on the first number it rendered — which is the
argument for the generator, made by the generator.

### 14. The manuscript's own tables have no run identity

**Standing, unresolved.** Both tables currently in `MANUSCRIPT.docx` come from
bare CSVs in `artifacts/diagnostics/`. No diagnostics script calls
`begin_validation_run()`, so those numbers carry no model SHA, no input hashes
and no `COMPLETED` marker. They are not wrong — they are **unattributable**,
which is a different problem and one a renderer cannot fix.

Recorded in `docs/submission/tables/TABLE_INDEX.md` under *Excluded, on
purpose*, so the omission reads as a decision. Converting
`entrant_regime_bias_decomposition.R` and `interval_honesty_scorecard.R` to
manifest-first is the open work.

### 15. A capture rate that is true and misleading

**2026-08-08.** Pooled provider-file capture across the URPS basket is 96.8%.
Quoted alone it suggests near-complete data. It is an E/M artifact: capture on
the *procedural* codes runs 40.2% (prolapse) to 54.3% (sling).

**Rule:** capture is now column 2 of Table 5, immediately after the service, so
a bound cannot be read off the table without the fraction of volume behind it.
Structural, not conventional.

---

### A constant presented as a measurement

**2026-08-14, mine (CHIA travel kernel).** The kernel reports how far women
travel for major pelvic surgery, in 30/60/120/180-minute bands, to be compared
against the Luo/Qi weights the E2SFCA layer uses. Distance was measured. Drive
time was not: it was `miles * 1.3 circuity / 40 mph`, two constants I chose.

The module and its outputs said "drive_min" throughout, and the band table read
like an observation. It was an assumption wearing an observation's units.

How much it mattered was not obvious until asked. The `<=30` band share ranges
**0.646 at 30 mph to 0.790 at 50 mph** — a 14-point swing from the speed
constant alone, wider than most effects the kernel would be used to detect.

**Caught by:** the user asking, flatly, "how did you do drive time from zip code
to zip code." Nothing in the pipeline would have surfaced it; the number had
already been written into a module, a CSV and a summary table. The repair was to
demote drive time to an explicitly-labelled approximation, promote the
assumption-free **distance** distribution to the primary result, and ship the
speed-sensitivity table beside it so the fragility is impossible to miss.

**The general form:** a derived quantity in the units of a measured one. Miles
were measured; minutes were manufactured, and only the minutes were comparable
to the thing being challenged — which is exactly why the manufacturing happened.

---

### Seven hospitals fell out of a geocode and took the long trips with them

**2026-08-14, mine (CHIA travel kernel).** Facility ZIPs were joined to ZCTA
centroids. Seven hospitals hold **unique institutional ZIPs that have no ZCTA** —
Baystate (01199), Lahey Burlington (01805), UMass University (01655), Mercy
Springfield (01102), Lawrence General (01842), Cooley Dickinson (01061), Noble
(01086). They dropped silently: **263,745 cases, 15.9% of the cohort.**

The bias was not random. Those are western and central Massachusetts — Springfield,
Worcester, Northampton, Westfield — **exactly the regions where patients travel
furthest**. Dropping them biases a travel kernel toward short trips, in a
deliverable whose entire purpose is to characterise how far people travel.

**Caught by:** distrusting a summary line. The script printed `geocoded 82.7% of
cases` and moved on to a plausible-looking distribution. Nothing failed. The only
reason it surfaced was refusing to accept 82.7% without knowing what the missing
17% were. Origins turned out to be 98% fine; the loss was entirely on the
destination side.

A ZIP3-area centroid fallback took geocoding to 99.0%. The final distribution
moved only slightly (73.5% -> 73.1% in the near band), which is the
uncomfortable part: **the answer was nearly right for the wrong reason**, and a
check that stopped at "does this look plausible" would have passed it.

---

### Circumcision was the top urogynaecology procedure

**2026-08-14, mine (CHIA workforce layer).** Building surgeon-year operative
volume, the FY2018 case-mix listing for board-certified urogynaecologists came
back with `0VTTXZZ` *Resection of Prepuce* at the top, 146 cases.

Newborn stays (`AdmissionType = '4'`) carry the **mother's obstetrician** as
operating physician. Newborns are 6-7% of all operative discharges but a far
larger share for obstetric-adjacent specialties: excluding them cut URPS
operative volume by **41%** in FY2018.

**Caught by:** the result being clinically absurd on sight. Every structural
check passed — the procedure code was valid, the physician resolved to a real
NPI, the discharge was a genuine operation, the specialty was correct. Nothing
in the data was malformed. The join was simply answering a different question
than the one asked.

**The general form:** attribution defects survive every validity check, because
each field is individually true. They are caught by knowing what the answer
should look like, which is not a property of the pipeline.

---

## V. Reinventing what already existed

### 16. A hand-rolled GFM table renderer

**2026-08-09, mine.** `build_tables.R` hand-wrote the pipe-table emitter —
separator row, column widths, padding — when `knitr::kable(format = "pipe")`
does it, knitr is already this package's `VignetteBuilder`, and the repo
already depends on it. Same turn: a local `sha256()` wrapper when
`hash_inputs()` in `scripts/validation/_provenance.R` already takes a named
vector of paths, returns named digests, and records a missing file as `NA`.

Not merely redundant. A second hasher is a second place for "what counts as the
identity of an input" to drift from the manifests it has to agree with.

**Caught by:** the maintainer, reviewing. Not by any check.

**Rule:** search the repo and the existing dependency set before writing a
utility. What survived the swap is the part that was actually domain logic —
deciding *which columns are magnitudes* — not the rendering.

---

---

## VI. A guardrail described as a proof

### 17. A "classification invariant" that was a grep

**2026-08-09, mine.** `1c9bb0c` shipped
`test-data-artifact-classification.R` and its commit message said it *enforces*
that no validation script reads a non-canonical artifact. It did no such thing.
It scanned comment-stripped source for seven hard-coded file names. This passes:

```r
readRDS(file.path(cache_dir, stem))
```

so does anything routed through a helper, and so does any cache invented after
the list was written.

**Caught by:** the maintainer, reading the description and asking whether the
check worked from a declared registry or from text patterns. Not by me, and not
by any test — a name scan cannot fail in the direction that reveals it is a name
scan.

**Why it belongs here rather than in a changelog.** The defect was not the grep;
a grep is a reasonable backstop. The defect was **describing a guardrail as an
invariant**, which is worse than having no check, because a documented invariant
stops people looking. Everything else in this repository is careful to say what
a green build does *not* establish; this commit forgot to.

**Rule, now implemented in `ef561ec`:** the primary check parses, resolves each
reader's path argument through the script's own constants, and requires every
resolved path to be declared — inverting the burden from "is this forbidden file
mentioned?" to "is everything you open declared?". Where a path cannot be
resolved statically it is **counted**, and a third test fails if that count
grows. The name scan survives as an explicitly labelled backstop, and the
inventory states plainly what the pair does and does not prove.

---

## VII. Two sessions, one repository

### 18. The same red build fixed twice, in opposite directions

**2026-08-09.** Entry 3's sequel produced a race. `main` was red on the
`isochrone_source_dir` false positive, and two sessions fixed it independently:

| | fix | cost | what it asserts |
|---|---|---|---|
| concurrent session | register the export as `api`, ratchet 56 → 57 | 2 lines | the function is unwired — **false** |
| this session | repair the detector to trim, not drop, definition lines | ~10 lines | the function is wired — true |

Both go green. They are mutually exclusive: with the detector repaired, the
registry row becomes provably stale and the gate fails on it, so the second fix
had to *remove* the first. That is an override of another session's committed
decision, and it is recorded here rather than left in a diff.

**The general shape.** When a gate misfires, the cheapest green is almost always
to feed the gate a declaration that satisfies it. That declaration is a
statement about the code, and if the gate was wrong the statement is false — so
the repository ends up holding a lie in the exact file that exists to prevent
one. The register in entry 2 is only useful while every row in it is true.

**Rule:** before satisfying a gate, establish whether the gate is right. If the
finding is a false positive, fixing the detector is not gold-plating; it is the
only fix that does not add a falsehood.

### 19. Regenerating documentation in a tree somebody else is editing

**2026-08-09, mine.** Running `roxygen2::roxygenise()` regenerates the *whole*
`man/` tree and `NAMESPACE` from whatever sources are on disk. With a concurrent
session mid-edit, that meant one command modified 19 of their `man/` pages,
deleted `man/business_days_to_calendar.Rd`, and produced a `NAMESPACE`
containing four of their pending export changes alongside my two.

Nothing was lost, because I noticed before staging and rebuilt `NAMESPACE` from
`HEAD` with only my two lines. But the near-miss is the entry: had I run
`git commit -am`, I would have committed another session's half-finished API
under my message, and the commit would have looked entirely ordinary.

**Rule:** a generated file in a shared tree is not yours just because you ran
the generator. Diff every generated file before staging, stage by path and never
`-a`, and if a generated file is entangled, reconstruct your slice from `HEAD`
rather than committing the union.

---

## What the pattern says

Counting the entries by what caught them:

| caught by | n |
|---|---:|
| running the thing and reading the output | 6 |
| a human reviewing | 5 |
| a guard doing its job | 4 |
| accident — noticed a number that looked off | 3 |
| still open | 2 |

Two of those four guard catches deserve an asterisk. The orphan detector caught
a false positive of its own making (#3's sequel) — the machinery working only in
the sense that it failed loudly enough to be investigated rather than
accommodated. And #4b was a correct gate blocking every push in the repository
over one square bracket, which is a catch and a cost at the same time.

Four of nineteen were caught by automation, and two of those four were the
automation reacting to itself. The single most productive habit in this
repository is still not a gate: it is **rendering the number next to its source
and looking at both**. Every gate here was written after a human did that and
found something.

**The same defect shape recurs across unrelated layers**, which is the argument
for reading this file rather than each entry's fix:

| shape | instances |
|---|---|
| a predicate applied to a sample and generalised to a column | #6, #8 |
| a broad pattern reaching a path invented later | #2, #4 (×3) |
| a claim of impossibility asserted without the attempt | #9, #12 |
| a check whose own output is indistinguishable from its subject | #1, #3 |
| a guardrail mistaken for a proof | #14, #17 |

**And the newest pattern, which the first sixteen entries did not have: a fix
that satisfies a gate by telling it something false.** #3's sequel and #18 are
the same move — register the orphan, bump the ratchet, go green — reached
independently by two sessions within a day. It is always the cheapest option and
it always costs the register its meaning. When a gate misfires, decide whether
the gate is right *before* deciding what to tell it.

**A note on what recurrence means here.** Entry 4 is now three instances of one
pattern, and the third arrived inside a commit fixing the second. The technique
that caught instance two — list what `git add` *would* stage — was written down
in this file and still did not reach the person who hit instance three. Writing
the lesson down is necessary and demonstrably not sufficient; the ones that stop
recurring are the ones that became a test.
