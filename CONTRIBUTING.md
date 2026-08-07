# Contributing to urpssim

`urpssim` is an R package. The one rule that matters: **`main` should always
pass `R CMD check`.** The CI gate (`.github/workflows/R-CMD-check.yaml`) runs
`R CMD check --as-cran` with `error_on = "warning"`, so a *warning* fails the
build, not just an error. This document explains how to keep changes green
locally and how to keep `main` green structurally.

## Local development loop

A `Makefile` mirrors CI so failures surface before you push:

```sh
make deps          # once: install package deps + rcmdcheck + roxygen2
make document      # regenerate man/*.Rd + NAMESPACE from roxygen comments
make test          # run the testthat suite
make check         # R CMD check --as-cran, error on WARNING  <-- the CI gate
make check-fast    # doc/namespace/Rd checks only (skips tests + vignettes)
make validate-docs # doc-drift tripwire, NO package deps needed (base R only)
```

`make check` runs the **exact** command CI runs, so a green `make check` means
a green PR. On Claude-on-web the SessionStart hook
(`.claude/hooks/session-start.sh`) installs the dependencies automatically;
elsewhere run `make deps` once.

**When you can't install the dependencies** (`duckdb` and the rest of `Imports`
are required to build the package, so `make document` and `make check` both need
`make deps` first), run `make validate-docs` — or `Rscript
scripts/dev/validate_docs.R` directly. It only *parses* the committed `R/` and
`man/`, so it runs on a bare `Rscript` with nothing installed, and it catches
the doc-drift subset that keeps reaching `main`: a stale NAMESPACE (an `@export`
with no `export()`), an `export()` with no man page, a codoc `\usage`/formals
mismatch, and structurally invalid Rd. It is a pre-push tripwire, not a
substitute for `make check`.

### Optional pre-push gate

To have the check run automatically before every push:

```sh
git config core.hooksPath .githooks
```

Then `.githooks/pre-push` runs `make check` on each push. Skip it for a single
push with `git push --no-verify`.

## The defect classes this catches

Every one of these is a `--as-cran` **warning** (so a merge blocker) and every
one has reached `main` before. `make check` / `make check-fast` catches them
locally:

- **Broken Rd cross-references.** roxygen markdown turns `[text]` into a
  cross-reference link. `[psa_input]`, `[0, 1]`, `[some_internal_fn()]` become
  links to topics that don't exist. Use a code span — `` `psa_input` ``,
  `` `[0, 1]` `` — for anything that isn't a real exported/aliased topic.
- **Namespace typos.** `urpssim::foo` where `foo` lives in another package
  (e.g. `mufflyaccess::urps_scenarios`) is a "Missing or unexported object"
  warning. Only self-reference `urpssim::` for actual exports.
- **Undeclared globals.** dplyr data-masking columns read as "no visible
  binding for global variable" — register them in the `utils::globalVariables()`
  block in `R/urpssim-package.R`.
- **Stale docs.** Edit roxygen comments, then `make document` and commit the
  regenerated `man/` + `NAMESPACE`. Uncommitted regeneration is a codoc warning.

## Keeping `main` green structurally

CI catches these only if it *blocks the merge*. To make the check required:

1. Repo **Settings → Branches → Add branch ruleset** (or classic *Add rule*),
   targeting `main`.
2. Enable **Require status checks to pass before merging**, and add the check
   named **`ubuntu-latest (release)`**.
3. Also enable **Require branches to be up to date before merging** — this runs
   the check against the *merge result*, catching the case where `main` moved
   after the PR's last green run (a defect merged onto `main` after the PR
   opened, only visible once the branch is tested against current `main`).

Without a required check, a red branch can still be merged and `main` goes red
reactively; with it, the merge button is disabled until the check passes.

## Private dependency

`mufflyaccess` is a private repo listed in `Suggests` (via `Remotes:`). Tests
that need it skip themselves when it is absent, and the package must still check
without it. CI resolves it from the `MUFFLYACCESS_PAT` secret; locally, set
`GITHUB_PAT` to a token with read access if you need those tests to run.
