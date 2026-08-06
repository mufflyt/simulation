# Legacy scripts — FROZEN, reference only

These are the original DPMM / SWAN / early-workforce scripts: 25,588 lines across
nine files, the largest being `dppm_validate_SWAN.R` at 12,186. They are kept so
a published number can be traced back to the analysis it came from. They are
**not part of the package**, they are **not maintained**, and **nothing in the
package depends on them**.

## Frozen means two specific things, both checked rather than claimed

**No call sites.** The whole loader API — `legacy_dir()`, `legacy_definitions()`,
`legacy_collisions()`, `load_legacy()`, `check_legacy_canonical()`,
`LEGACY_LOAD_ORDER`, `LEGACY_CANONICAL` — is referenced from exactly one place in
the repository: `tests/testthat/test-repo-hygiene.R`. No module, script, or
vignette calls any of it.

**No shared surface.** These scripts define 131 function names. The package
defines 468. The intersection is **empty**. `R/data-swan_incontinence_panel.R` and
`R/data-swan_dmdm_panel.R` are reimplementations of the SWAN work, not extractions
of it, so there is no path by which a change here reaches a model output.

The rule that follows: **do not add a call to `load_legacy()` from package
code.** Doing so un-freezes the directory and puts 25,588 unmaintained lines back
on the dependency graph. If you need one of these implementations, extract it
into a small tested module in `R/` — which is what was done for SWAN.

## Not shipped

`.Rbuildignore` excludes this whole directory from the built package, so
`load_legacy()` works in a **source checkout only**.

This used to be backwards. The ignore rule named `inst/legacy/README.md` and
nothing else, so every installation carried all 980 KB of frozen script while
stripping out the one document explaining what it was. `legacy_dir()` now says
so explicitly when it cannot find the directory, rather than failing with a bare
not-found.

## How to load them (source checkout)

```r
pkgload::load_all(".")
load_legacy()            # definitions only — safe, touches no files
load_legacy(functions_only = FALSE)   # runs them as scripts; needs the external data
```

`load_legacy()` sources in the order declared by `LEGACY_LOAD_ORDER` and reports
every function name that gets redefined along the way, so a collision is visible
instead of silent.

## Duplicate function names

Fifteen names are defined in more than one of these files. Which implementation
you get therefore depends on source order. `LEGACY_CANONICAL` declares the
intended owner of each, and `check_legacy_canonical()` verifies the load order
actually delivers it — a test enforces this.

The validation family resolves as:

```
99-unsorted-fragments.R   (oldest, superseded)
  -> dppm_validate_SWAN.R          (superseded for most names)
    -> 03-dppm_validate_SWAN_better.R   (canonical; see the top-level README)
```

Within-file duplicates have been removed. Where the two bodies were identical
the earlier one was deleted; where they diverged the earlier one was renamed to
`<name>_variant1` (or `_variant2`) and flagged with a comment, so nothing was
lost and nothing silently shadows anything else. Those `_variant*` functions are
unreviewed — check which implementation is wanted before relying on either.

## External data

No path is hardcoded. Everything routes through `swan_path()`,
`data_raw_path()` and `external_path()`, which resolve against
`SIMULATION_DATA_ROOT`, then `config/paths.local.yml`, then `config/paths.yml`.
Run `check_external_data()` to see what is reachable on this machine before
starting a long job.
