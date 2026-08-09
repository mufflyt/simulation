# Data artifact inventory

**Organizing principle.** Some files on this machine are canonical scientific
inputs. Most are not. The difference is invisible from a directory listing, and
that is the whole problem: a 9.7 MB file called `urps_basket_prov_svc.rds`
sitting beside real data looks exactly like a frozen manuscript input, and is
not one.

> **Local convenience artifacts are not canonical scientific inputs. Their
> provenance, regeneration path, and relationship to manuscript analyses must be
> explicit.**

Four classes. The class determines what a reader may assume and what a script is
permitted to read.

| class | may a manuscript/validation script read it? | may it be deleted? |
|---|---|---|
| **Canonical input** | Yes — this is what they are for | No. Re-acquire it |
| **Derived intermediate** | **No** | Yes — rebuild from a canonical input |
| **Download cache** | **No** | Yes — re-fetch from the public source |
| **Obsolete exploratory** | **No** | Yes — and preferably do |

**The invariant, enforced not just stated:**
`tests/testthat/test-data-artifact-classification.R` fails if any script under
`scripts/validation/` reads a file classified below as intermediate, cache or
obsolete. Analysis `05` already satisfies it; the test keeps it true.

---

## 1. Canonical inputs

Hash-controlled scientific inputs. Registered in `config/canonical_sources.yml`
with a SHA-256, resolved through `resolve_canonical()`, and — for the ones a
manuscript analysis consumes — hashed into every run manifest *before*
computation by `begin_validation_run()` and re-checked at completion.

| artifact | registry id / declared as | consumed by |
|---|---|---|
| `data-raw/cms_psps/PHY_R26_P05_V10_D24_Prov_Svc.csv` (3.25 GB) | `cms_mup_phy_2024_prov_svc` | analysis `05` |
| `data-raw/cms_psps/MUP_PHY_R26_P05_V10_D24_Geo.csv` (42 MB) | `cms_mup_phy_2024_geo` | analysis `05` |
| `data-raw/calibration/nrmp_urps_entrants_series.csv` | `validation_inputs()` | back-test arm 5 |
| `data-raw/urps_roster/urps_roster_2026-07-22.csv` | `validation_inputs()` | analyses `05`, `06` |
| `data-raw/urps_roster/urps_linkage_roster_2024.csv` | declared input of `05` | analysis `05` |
| `artifacts/backtest_2020_to_2023_summary.csv` | sidecar manifest | manuscript Table 1 |

Deleting one of these is a re-acquisition, not a cleanup. `data-raw/cms_psps/DOWNLOAD.md`
and `docs/DATA_DOWNLOAD_GUIDE.md` are the routes back.

---

## 2. Derived intermediates

Rebuildable products of canonical inputs. **Never authoritative.**

### `urps_basket_prov_svc.rds` — 9.7 MB

> **`urps_basket_prov_svc.rds` is not part of the manuscript evidence chain.
> Analysis 05 reads the hash-registered CMS Provider & Service PUF directly;
> this RDS is a regenerable convenience subset only.**

That sentence is the point of this entry. The name is misleading by accident,
and the misreading it invites — that a manuscript-citable bound was computed
from a prebuilt extract — is the single most likely future provenance mistake
in this repository.

```text
CMS Provider & Service 2024 CSV  (3.25 GB, SHA-256 registered)
    │
    │ canonical input
    ▼
05_urps_share_partial_identification.R
    │
    ├── reads the raw CMS PUF directly
    │
    └── produces run-identified evidence in artifacts/validation/
```

```text
CMS Provider & Service 2024 CSV
    │
    └── optional extraction
        ▼
    urps_basket_prov_svc.rds
        │
        └── convenience / cache only; nothing downstream
```

| | |
|---|---|
| **Status** | Derived intermediate |
| **Canonical source** | `cms_mup_phy_2024_prov_svc` |
| **Regeneration** | `Rscript scripts/data/build_urps_basket_prov_svc.R` |
| **Safe to delete?** | **Yes** |
| **Read by any analysis?** | **No** — asserted by test |

The builder resolves its source through `resolve_canonical()`, so it verifies
the PUF's SHA-256 rather than extracting from whichever similarly named CSV is
present. It also stamps `urpssim_artifact_class = "derived_intermediate"` and
the source hash as attributes **inside** the RDS, so a stray copy that has lost
its directory can still say what it is.

*Was:* an ad-hoc extract in a session scratchpad, deleted 2026-08-09. Its exact
column selection was never recorded, so a rebuild is content-equivalent, not
byte-identical. If byte-identity mattered it would have been an artifact with a
manifest.

---

## 3. Download caches

Public-source conveniences that can be re-acquired. **Not scientific inputs.**

### `meps_FYC_2023.rds`, `meps_COND_2023.rds`, `meps_CLNK_2023.rds`, `meps_ob_2023.rds` — ~13 MB

Raw MEPS 2023 public-use files, imported and `saveRDS()`'d with **no
transformation** — variable selection, recoding and joins all happened
downstream, so these four are honest caches rather than derived intermediates.
Recording that distinction is the point: "downloaded MEPS" and "MEPS with our
filters already applied" are different objects and only one of them is a cache.

| local filename | MEPS component | year | AHRQ file | acquired by | transformed before save? |
|---|---|---|---|---|---|
| `meps_FYC_2023.rds` | Full-year consolidated | 2023 | HC-consolidated | MEPS R package via `06_download_meps_2023.R` | No |
| `meps_COND_2023.rds` | Medical conditions | 2023 | HC-conditions | same | No |
| `meps_CLNK_2023.rds` | Condition–event link | 2023 | HC-CLNK | same | No |
| `meps_ob_2023.rds` | Office-based visits | 2023 | HC-office-based | same | No |

| | |
|---|---|
| **Status** | Download cache (unmodified import) |
| **Canonical source** | AHRQ MEPS 2023 public-use files |
| **Regeneration** | `Rscript scripts/data_acquisition/06_download_meps_2023.R` |
| **Safe to delete?** | **Yes** |

**Note the acquisition script does more than download.** It goes on to filter to
women 18+ with an ICD-10 N39 diagnosis, join conditions to events, and emit
survey-weighted calibration targets to `data-raw/meps/`. Those outputs are
derived intermediates in their own right; the four RDS files above are the
untransformed inputs to that step. The shipped care-seeking model is
`R/data-meps_care_seeking.R`.

### `cms_datajson.json` — 2.8 MB

The `data.cms.gov` DCAT catalogue, used for **resource discovery only** — never
scientific computation. It is how the 2024 PUFs' stable `downloadURL`s were
resolved.

| | |
|---|---|
| **Status** | Download-discovery cache |
| **Canonical source** | `https://data.cms.gov/data.json` |
| **Regeneration** | `curl -s https://data.cms.gov/data.json` |
| **Safe to delete?** | **Yes** |

`data-raw/cms_psps/DOWNLOAD.md` records the exact query, the resolved URLs, the
verified byte counts and the SHA-256s — and corrects an earlier claim in that
same file that the CMS portal required a session cookie and could not be fetched
with `curl`. It can: the DCAT `downloadURL`s return HTTP 200 unauthenticated and
transferred 3.1 GB at ~30 MB/s. Go there, not to a cached copy of the JSON.

*A future rename to `cms_dcat_catalog_cache.json` would make the class obvious
from the filename. Not done now — renaming a path other sessions may hold is not
worth the aesthetics mid-flight.*

---

## 4. Obsolete exploratory artifacts

### `fitted_model.rds` — 4.5 MB

> **OBSOLETE EXPLORATORY ARTIFACT — DO NOT USE.**

| | |
|---|---|
| **Status** | Superseded prototype |
| **Created** | 2026-08-03, by an ad-hoc scratchpad script reading the MEPS caches above |
| **Contents** | A list of `panel_n` and `model` — a survey-weighted care-seeking fit |
| **Canonical source** | None. It was exploratory from the start |
| **Replaced by** | `R/data-meps_care_seeking.R`, with figures committed to `figures/meps_care_seeking_multipliers.png` and `figures/meps_care_seeking_comorbidity.png` |
| **Regeneration** | **Do not.** Rebuilding it recovers an earlier, worse version of shipped code |
| **Safe to delete?** | **Yes — and preferable.** Deleted 2026-08-09 |

"Regenerable" is not the relevant property here; **superseded** is. A prototype
fit sitting near live data is more dangerous than useful, because the thing that
makes it dangerous — that it loads and returns plausible numbers — is exactly
what makes it look usable.

The guard is that no production or manuscript script may `readRDS()` it; the
classification test enforces that by name, so the prohibition survives the file
itself being gone.

---

## The rule for adding a new artifact

Before saving a derived file anywhere:

1. **Decide its class** and say so in the producing script's header.
2. **If it is canonical**, register it in `config/canonical_sources.yml` with a
   SHA-256 and resolve it through `resolve_canonical()`.
3. **If it is not canonical**, make sure no `scripts/validation/` script reads
   it, and add it here. The test will catch you if the first part is wrong.
4. **Record the regeneration command**, not just the source. A URL in a comment
   is not a regeneration path.
5. **Prefer stamping the class inside the object** (see the builder's
   `urpssim_artifact_class` attribute) so a stray copy can still identify itself.

---

## See also

- `docs/DATA_PROVENANCE_NRMP.md` — the case that established this vocabulary:
  when an extract carries per-row provenance and a test enforces it, the source
  cache becomes disposable.
- `docs/DATA_DOWNLOAD_GUIDE.md` — acquisition for each external source.
- `data-raw/cms_psps/DOWNLOAD.md` — the CMS DCAT query and direct URLs.
- `scripts/dev/regenerate_intermediates.R` — checks that every regeneration path
  declared here still resolves.
- `config/canonical_sources.yml` — the hash registry.
