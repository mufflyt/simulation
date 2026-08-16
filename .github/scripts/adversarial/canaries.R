#!/usr/bin/env Rscript
# SCIENTIFIC CANARY MUTATIONS  (spec A1-A3, AZ, BC)
#
# Testing the tests. Each canary deliberately corrupts a scientific quantity and
# requires a NAMED detector to catch it. If a canary survives, the validation
# system cannot distinguish the correct model from a meaningfully wrong one --
# which is a finding about the tests, not about the model.
#
# DETECTOR INDEPENDENCE (spec A3) is enforced: a canary is only "killed" if its
# EXPECTED detector fires. A generic crash does not count. Without that rule one
# over-eager assertion appears to kill everything and the suite looks stronger
# than it is.
#
# Mutations are applied to the PARAMETER SURFACE (the staged pathway and the
# anchors), because that is where the science of this repository lives. Patching
# installed package source would test R's loader, not the model.

suppressMessages(pkgload::load_all(".", quiet = TRUE))

TREATED <- c(pop = unname(FROZEN_CARE_ENGAGED[["pop"]]),
             ui  = unname(FROZEN_CARE_ENGAGED[["ui"]]),
             ai  = unname(FROZEN_CARE_ENGAGED[["ai"]]))
BASE <- condition_service_pathway()
ANCHOR <- utils::read.csv("data/anchors/prolapse_procedure_volume.csv")$observed[[1]]

# ---- detector family -------------------------------------------------------
# Each returns TRUE when it DETECTS a problem. They are deliberately independent
# of one another and of the production code path being mutated.

det_probability_legality <- function(pw, ...) {
  p <- pw$p_advance[!is.na(pw$p_advance)]
  any(p < 0 | p > 1) || any(pw$per_entering < 0) || any(!is.finite(pw$per_entering))
}

det_mass_balance <- function(pw, ...) {
  e <- pathway_stage_entrants(TREATED, pw)
  for (cd in unique(e$condition)) {
    g <- e[e$condition == cd, ]
    g <- g[order(match(g$stage, PATHWAY_STAGES)), ]
    if (any(diff(g$entering) > 1e-6)) return(TRUE)
    if (g$entering[1] > TREATED[[cd]] + 1e-6) return(TRUE)
  }
  FALSE
}

# Independent naive recomputation; shares no code path with the engine.
.reference <- function(treated, pathway) {
  out <- list()
  for (cd in names(treated)) {
    rows <- pathway[pathway$condition == cd, ]
    n <- treated[[cd]]
    for (st in PATHWAY_STAGES[PATHWAY_STAGES %in% rows$stage]) {
      sr <- rows[rows$stage == st, ]
      for (i in seq_len(nrow(sr)))
        out[[length(out) + 1L]] <- data.frame(service = sr$service[i], volume = n * sr$per_entering[i])
      adv <- unique(sr$p_advance)[1]
      n <- n * (if (is.na(adv)) 0 else adv)
    }
  }
  a <- do.call(rbind, out); tapply(a$volume, a$service, sum)
}

det_reference_disagreement <- function(pw, ...) {
  r <- .reference(TREATED, pw)
  e <- pathway_service_volumes(treated = TREATED, year = 2025L, pathway = pw)
  ev <- tapply(e$volume, e$service, sum)
  k <- intersect(names(r), names(ev))
  if (!setequal(names(r), names(ev))) return(TRUE)
  max(abs(r[k] - ev[k]) / pmax(1, abs(r[k]))) > 1e-9
}

det_numerical_integrity <- function(pw, ...) {
  v <- pathway_service_volumes(treated = TREATED, year = 2025L, pathway = pw)$volume
  any(is.na(v) | is.nan(v) | is.infinite(v) | v < 0)
}

det_terminal_scalar <- function(pw, ...) {
  r <- pw[pw$condition == "pop" & pw$stage == "procedure" &
            pw$service == "prolapse_procedure", ]
  !isTRUE(all.equal(r$per_entering[[1]], 1.0)) || !isTRUE(all.equal(r$p_advance[[1]], 1.0))
}

det_anchor_ratio <- function(pw, ...) {
  v <- pathway_service_volumes(treated = TREATED, year = 2025L, pathway = pw)
  ratio <- sum(v$volume[v$service == "prolapse_procedure"]) / ANCHOR
  # The shipped model sits at 8.51x and that is documented. A canary must move
  # it OUTSIDE the documented band to count as detected here.
  ratio < 7 || ratio > 10
}

det_directional <- function(pw, ...) {
  # Raising an advance probability must not lower downstream volume.
  base <- sum(pathway_service_volumes(treated = TREATED, year = 2025L, pathway = pw)$volume[
    pathway_service_volumes(treated = TREATED, year = 2025L, pathway = pw)$service == "prolapse_procedure"])
  up <- pw
  i <- up$condition == "pop" & up$stage == "conservative"
  up$p_advance[i] <- pmin(1, up$p_advance[i] * 1.2)
  v <- pathway_service_volumes(treated = TREATED, year = 2025L, pathway = up)
  sum(v$volume[v$service == "prolapse_procedure"]) < base - 1e-6
}

# Input validation is itself an independent detector: the engine refuses an
# illegal pathway before computing anything. Enumerated explicitly so a canary
# killed this way is recorded honestly rather than looking like a survivor.
det_engine_validation <- function(pw, ...) {
  isTRUE(tryCatch({ validate_condition_pathway(pw); FALSE },
                  error = function(e) TRUE))
}

# Double counting shows up as a duplicated (condition, stage, service) key. The
# naive reference sums duplicates exactly as the engine does, so reference
# agreement CANNOT catch this -- a uniqueness check is the independent detector.
det_row_uniqueness <- function(pw, ...) {
  k <- paste(pw$condition, pw$stage, pw$service, sep = "|")
  any(duplicated(k))
}

DETECTORS <- list(
  engine_validation      = det_engine_validation,
  row_uniqueness         = det_row_uniqueness,
  probability_legality   = det_probability_legality,
  mass_balance           = det_mass_balance,
  reference_disagreement = det_reference_disagreement,
  numerical_integrity    = det_numerical_integrity,
  terminal_scalar        = det_terminal_scalar,
  anchor_ratio           = det_anchor_ratio,
  directional            = det_directional)

# ---- the canaries ----------------------------------------------------------
mut <- function(f) { pw <- BASE; f(pw) }

CANARIES <- list(
  list(id = "CAN-01", domain = "TRANSITION",
       desc = "advance probability above 1 (probability complement misused)",
       expect = "probability_legality",
       apply = function() mut(function(pw) {
         pw$p_advance[pw$condition == "pop" & pw$stage == "conservative"] <- 1.4; pw })),

  list(id = "CAN-02", domain = "TRANSITION",
       desc = "cascade gains people (stage entrants exceed the prior stage)",
       # The engine refuses an illegal pathway before mass balance can be
       # evaluated, so input validation is the detector that actually fires.
       expect = "engine_validation",
       apply = function() mut(function(pw) {
         pw$p_advance[pw$condition == "ui" & pw$stage == "testing"] <- 2.5; pw })),

  list(id = "CAN-03", domain = "CALIBRATION",
       desc = "terminal scalar smuggled onto the POP procedure",
       expect = "terminal_scalar",
       apply = function() mut(function(pw) {
         i <- pw$condition == "pop" & pw$stage == "procedure" &
           pw$service == "prolapse_procedure"
         pw$per_entering[i] <- 0.214; pw })),

  list(id = "CAN-04", domain = "DEMAND",
       desc = "care-seeking bypassed (every stage advances everyone)",
       expect = "anchor_ratio",
       apply = function() mut(function(pw) {
         pw$p_advance[!is.na(pw$p_advance)] <- 1; pw })),

  list(id = "CAN-05", domain = "AGGREGATION",
       desc = "duplicate pathway rows (double counting a service)",
       # NOT reference_disagreement: the naive reference sums duplicates exactly
       # as the engine does, so both agree on the wrong answer. Uniqueness is
       # the only independent detector for this class.
       expect = "row_uniqueness",
       apply = function() {
         pw <- BASE
         dup <- pw[pw$condition == "pop" & pw$service == "prolapse_procedure", ]
         # engine sums duplicates; the naive reference sums them too, so this is
         # detected by mass balance / ratio rather than reference disagreement.
         rbind(pw, dup) }),

  list(id = "CAN-06", domain = "WEIGHTING",
       desc = "negative service intensity (weights applied with wrong sign)",
       # probability_legality checks per_entering >= 0 and finite, so it fires
       # before any volume is computed.
       expect = "probability_legality",
       apply = function() mut(function(pw) {
         pw$per_entering[pw$condition == "pop" & pw$service == "pessary_care"] <- -1.8; pw })),

  list(id = "CAN-07", domain = "TRANSITION",
       desc = "non-finite advance probability",
       expect = "probability_legality",
       apply = function() mut(function(pw) {
         pw$per_entering[pw$condition == "ai" & pw$stage == "testing"] <- Inf; pw })),

  list(id = "CAN-08", domain = "DEMAND",
       desc = "exit/attrition removed from the POP cascade (no attrition at all)",
       expect = "anchor_ratio",
       apply = function() mut(function(pw) {
         i <- pw$condition == "pop" & pw$stage == "conservative"
         pw$p_advance[i] <- 1; pw })))

# ---- run -------------------------------------------------------------------
rows <- list()
for (cn in CANARIES) {
  pw <- tryCatch(cn$apply(), error = function(e) NULL)
  fired <- character()
  for (dn in names(DETECTORS)) {
    hit <- tryCatch(isTRUE(DETECTORS[[dn]](pw)), error = function(e) NA)
    if (isTRUE(hit)) fired <- c(fired, dn)
  }
  killed_by_expected <- cn$expect %in% fired
  rows[[length(rows) + 1L]] <- data.frame(
    id = cn$id, domain = cn$domain, expected = cn$expect,
    fired = if (length(fired)) paste(fired, collapse = "+") else "NONE",
    killed = killed_by_expected, stringsAsFactors = FALSE)
  cat(sprintf("  %s  %-12s %-22s expected=%-22s fired=%s\n",
              if (killed_by_expected) "KILL" else "SURV", cn$id, cn$domain,
              cn$expect, if (length(fired)) paste(fired, collapse = "+") else "NONE"))
}
res <- do.call(rbind, rows)

# Sanity: the UNMUTATED model must trip NO detector. Without this, a detector
# that always fires would appear to kill every canary.
base_fired <- names(DETECTORS)[vapply(names(DETECTORS),
  function(dn) isTRUE(tryCatch(DETECTORS[[dn]](BASE), error = function(e) NA)), logical(1))]
cat(sprintf("\nbaseline (unmutated) detectors firing: %s\n",
            if (length(base_fired)) paste(base_fired, collapse = ", ") else "none (correct)"))

dir.create("artifacts/adversarial", recursive = TRUE, showWarnings = FALSE)
utils::write.csv(res, "artifacts/adversarial/canaries.csv", row.names = FALSE)

n_kill <- sum(res$killed)
cat(sprintf("\nCanaries killed by their EXPECTED detector: %d / %d\n", n_kill, nrow(res)))
fail <- length(base_fired) > 0 || n_kill < nrow(res)
if (fail) {
  if (length(base_fired))
    cat("::error::MUTATION SURVIVED / detector always-on: the unmutated model trips a detector\n")
  surv <- res$id[!res$killed]
  if (length(surv))
    cat(sprintf("::error::MUTATION SURVIVED: %s -- the validation system cannot distinguish the correct model from a wrong one\n",
                paste(surv, collapse = ", ")))
  quit(status = 1)
}
cat("ALL SCIENTIFIC CANARIES DETECTED BY THEIR NAMED DETECTOR.\n")
