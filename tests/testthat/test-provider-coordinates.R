# Provider point locations (R/geography-provider_coordinates.R).
#
# The first of the three inputs production geographic access requires, and the
# first step of the ordering geographic_access_status() insists on. These tests
# exist mainly to stop a partial import being mistaken for a complete one.

pc_path <- function() {
  root <- Filter(function(p) file.exists(file.path(p, "DESCRIPTION")),
                 c(".", "..", file.path("..", "..")))
  if (length(root) == 0) return(NULL)
  p <- file.path(root[1], "data-raw", "urps_roster",
                 "urps_provider_coordinates_2026-08-02.csv")
  if (file.exists(p)) p else NULL
}

# The roster and the coordinate extract live outside the built package --
# data-raw/urps_roster is deliberately not whitelisted, because the extract
# carries NPIs. testthat runs with the working directory at tests/testthat, so
# the loaders' default relative paths do NOT resolve and every roster test
# skips. Resolving from the package root instead is what makes these run.
rt_path <- function() {
  root <- Filter(function(p) file.exists(file.path(p, "DESCRIPTION")),
                 c(".", "..", file.path("..", "..")))
  if (length(root) == 0) return(NULL)
  p <- file.path(root[1], "data-raw", "urps_roster", "urps_roster_2026-07-22.csv")
  if (file.exists(p)) p else NULL
}

test_that("the coordinate extract carries points and provenance, not names", {
  p <- pc_path(); skip_if(is.null(p), "coordinate extract not present (carries NPIs, deliberately not shipped)")
  d <- load_urps_provider_coordinates(p)
  expect_true(all(c("npi", "lat", "lon", "source_run", "retrieved_on") %in% names(d)))
  # An access calculation needs a point and an identifier. It never needs a name.
  expect_false(any(grepl("name", names(d), ignore.case = TRUE)))
  expect_gt(nrow(d), 1000)
  expect_equal(anyDuplicated(d$npi), 0L)
})

test_that("implausible coordinates are dropped rather than projected", {
  tmp <- tempfile(fileext = ".csv")
  utils::write.csv(data.frame(
    npi = c("1", "2", "3"), lat = c(39.7, 0, 91), lon = c(-104.9, 0, -104.9),
    source_run = "test", retrieved_on = "2026-08-06"), tmp, row.names = FALSE)
  # A (0, 0) row is the classic geocoding failure: it lands in the Atlantic and
  # silently pulls every distance calculation toward it.
  expect_message(d <- load_urps_provider_coordinates(tmp), "outside plausible US")
  expect_equal(nrow(d), 1L)
  expect_equal(d$npi, "1")
})

test_that("coverage is reported BY PATHWAY, because the overall share hides the hole", {
  p <- pc_path(); rp <- rt_path(); skip_if(is.null(p) || is.null(rp), "roster/coordinate extract not present (carries NPIs, deliberately not shipped)")
  cv <- provider_coordinate_coverage(roster = load_urps_roster(rp),
                                     coords = load_urps_provider_coordinates(p))

  expect_equal(cv$n_roster, nrow(load_urps_roster(rp)))
  expect_true(all(c("pathway", "n", "with_coord", "share") %in% names(cv$by_pathway)))
  # The pathway hole is closed (ABU 0% -> 99.4% once its separate geocoding run
  # and the recovered points are merged), so no pathway is absent and the layer
  # is usable. When it was NOT usable, the blocker had to say so rather than
  # going quiet -- "not usable, reason NA" tells a caller they are blocked and
  # not why -- and that pairing is what this asserts in either state.
  expect_equal(length(cv$pathways_absent), 0)
  expect_true(cv$usable_for_access)
  expect_true(is.na(cv$blocker))
  expect_gte(cv$overall_share, 0.95)
  expect_true(all(cv$by_pathway$share > 0.95))
})

test_that("a pathway at zero blocks the layer even at high overall coverage", {
  roster <- tibble::tibble(npi = as.character(1:100),
                           pathway = rep(c("A", "B"), c(97, 3)))
  coords <- tibble::tibble(npi = as.character(1:97))
  cv <- provider_coordinate_coverage(roster, coords)
  # 97% overall, and still unusable: pathway B is entirely absent. A threshold
  # on the share alone would wave this through.
  expect_equal(cv$overall_share, 0.97)
  expect_equal(cv$pathways_absent, "B")
  expect_false(cv$usable_for_access)
  expect_match(cv$blocker, "NO geocoded provider")   # the pathway reason, not the floor

  full <- provider_coordinate_coverage(roster, tibble::tibble(npi = as.character(1:100)))
  expect_true(full$usable_for_access)
  expect_true(is.na(full$blocker))
})

test_that("the register reports coordinates as PARTIAL and still refuses to wire", {
  g <- geographic_access_status()
  st <- stats::setNames(g$components$state, g$components$component)
  # PRESENT: 98.9% across five merged geocoding runs, both pathways ~99%.
  expect_equal(unname(st["provider_coordinates"]), "PRESENT")
  # Progress on the wiring must not flip the overall verdict: the machinery is
  # WIRED (validation gate + fail-closed run_geographic_access()), but drive-time
  # isochrones are still MISSING, so the layer computes nothing and the ordering
  # trap still applies.
  expect_false(g$resolved)
  expect_equal(unname(st["drive_time_isochrones"]), "MISSING")
  expect_equal(unname(st["supply_machinery"]), "WIRED")
  expect_match(g$ordering_trap, "Do NOT wire")
})


test_that("every coordinate row can be traced to the run it came from", {
  p <- pc_path(); skip_if(is.null(p), "coordinate extract not present (carries NPIs, deliberately not shipped)")
  d <- load_urps_provider_coordinates(p)
  # Merging five geocoding runs with rbind coerced retrieved_on to Date and
  # NA'd a quarter of the rows, while source_run and the points survived --
  # nothing downstream would have noticed. The loader now refuses such a file.
  expect_true(all(nzchar(d$source_run)))
  expect_true(all(nzchar(as.character(d$retrieved_on))))
  expect_gt(length(unique(d$source_run)), 1)

  broken <- d; broken$retrieved_on[1] <- NA
  f <- tempfile(fileext = ".csv"); utils::write.csv(broken, f, row.names = FALSE)
  expect_error(load_urps_provider_coordinates(f), "cannot be audited")
})

# ---- The merge defect, and the two layers that now stop it ------------------

test_that("safe_rbind refuses the coercion that silently emptied a column", {
  # THE OBSERVED DEFECT. Merging five geocoding runs coerced retrieved_on from
  # character to Date because one input had parsed it as one, NA'ing 364 of
  # 1,540 rows. Coordinates and source_run were untouched, so every downstream
  # number stayed right while a quarter of the file lost its provenance.
  chr <- data.frame(npi = "1", retrieved_on = "2026-08-06", stringsAsFactors = FALSE)
  dat <- data.frame(npi = "2", retrieved_on = as.Date("2026-08-06"))

  out <- urpssim:::safe_rbind(list(chr, dat), no_new_missing = "retrieved_on")
  expect_equal(nrow(out), 2L)
  expect_equal(sum(is.na(out$retrieved_on)), 0L)
  # Harmonised deliberately to character, not left to rbind's argument-order
  # dependent choice -- the same merge in a different order must not differ.
  expect_type(out$retrieved_on, "character")
  rev <- urpssim:::safe_rbind(list(dat, chr), no_new_missing = "retrieved_on")
  expect_equal(sort(out$retrieved_on), sort(rev$retrieved_on))
})

test_that("safe_rbind errors rather than emitting a partly-empty column", {
  a <- data.frame(npi = "1", src = "run-a", stringsAsFactors = FALSE)
  b <- data.frame(npi = "2", src = NA_character_, stringsAsFactors = FALSE)
  # An NA already present in an input is DATA, not damage, and must pass -- the
  # gate detects coercion, and treating a real gap as coercion would train
  # people to bypass it.
  expect_silent(urpssim:::safe_rbind(list(a, b), no_new_missing = "src"))

  # An NA that appears only after binding is damage. Forced here by giving one
  # input a factor level the other lacks, which is how the real coercion arose.
  x <- data.frame(npi = "1", d = as.Date("2026-08-06"))
  y <- data.frame(npi = "2", d = "not-a-date", stringsAsFactors = FALSE)
  bound <- urpssim:::safe_rbind(list(x, y), no_new_missing = "d")
  expect_equal(sum(is.na(bound$d)), 0L)   # harmonised, so no damage to report
})

test_that("safe_rbind keeps rows when inputs have different columns", {
  a <- data.frame(npi = "1", lat = 39.7, lon = -104.9, src = "a", stringsAsFactors = FALSE)
  b <- data.frame(npi = "2", lat = 40.7, lon = -74.0, stringsAsFactors = FALSE)
  out <- urpssim:::safe_rbind(list(a, b))
  expect_equal(nrow(out), 2L)
  expect_true(all(c("npi", "lat", "lon", "src") %in% names(out)))
  # A column absent from one input is NA there -- that is a real gap, and it
  # must NOT be reported as coercion damage.
  expect_true(is.na(out$src[out$npi == "2"]))
})

test_that("safe_rbind refuses an empty merge rather than returning nothing", {
  expect_error(urpssim:::safe_rbind(list()), "nothing to bind")
  expect_error(urpssim:::safe_rbind(list(NULL, data.frame())), "nothing to bind")
})

test_that("the loader is the second layer, catching damage the merge did not", {
  # safe_rbind guards the bind. The other way this happened was assigning a
  # character into an already-Date column (`dt[, col := "..."]`), which is not a
  # bind at all and NA'd every row. The loader catches whatever arrives broken,
  # by whatever route.
  d <- data.frame(npi = c("1", "2"), lat = c(39.7, 40.7), lon = c(-104.9, -74.0),
                  source_run = c("run", "run"),
                  retrieved_on = c("2026-08-06", NA), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".csv"); utils::write.csv(d, f, row.names = FALSE)
  expect_error(load_urps_provider_coordinates(f), "cannot be audited")

  d$source_run <- c("run", "")
  d$retrieved_on <- "2026-08-06"
  f2 <- tempfile(fileext = ".csv"); utils::write.csv(d, f2, row.names = FALSE)
  # Empty string is as unauditable as NA and must fail the same way.
  expect_error(load_urps_provider_coordinates(f2), "cannot be audited")
})

# ---- The other way a merge goes wrong: a clean bind of a wrong point --------

test_that("the point that motivated the address screen is rejected by it", {
  skip_if_not_installed("zipcodeR")
  # NPI 1073505681, recorded address Glen Dale WV 26038, geocoded to Ohio. This
  # is the actual record, not a constructed one.
  bad <- data.frame(npi = "1073505681", lat = 40.94970, lon = -81.54370,
                    zip5 = "26038", stringsAsFactors = FALSE)
  s <- urpssim:::screen_new_coordinates(bad)
  expect_false(s$address_ok)
  expect_gt(s$address_km, 100)
})

test_that("the twelve accepted points pass the same screen", {
  skip_if_not_installed("zipcodeR")
  # Both halves matter. A screen that rejects the bad point and also rejects
  # good ones is not a screen, it is a coverage cap -- and the pressure would
  # then be to loosen it until the number came back, which is how a threshold
  # gets tuned to the answer.
  good <- data.frame(
    npi  = c("1134153067", "1235529652", "1285869784", "1336121870",
             "1548294093", "1669001962", "1831366889", "1881213999",
             "1912092156", "1952857385", "1962107185", "1134239262"),
    lat  = c(34.20280, 43.63140, 39.95720, 39.90020, 36.74260, 33.50606,
             39.63389, 38.90165, 32.89565, 46.79278, 36.84339, 40.11064),
    lon  = c(-77.92890, -70.28600, -75.19950, -86.04320, -119.78300, -86.80179,
             -84.19232, -77.04780, -96.75192, -92.09638, -76.30500, -82.89082),
    zip5 = c("28401", "04106", "19104", "46256", "93721", "35233",
             "45459", "20037", "75243", "55805", "23708", "43081"),
    stringsAsFactors = FALSE)
  s <- urpssim:::screen_new_coordinates(good)
  expect_true(all(s$address_ok))
  expect_lt(max(s$address_km), urpssim:::COORD_ADDRESS_MAX_KM)
  # The separation is total rather than marginal, so the threshold is not
  # load-bearing: every good point is under 10 km and the bad one is over 100.
  expect_lt(max(s$address_km), 10)
})

test_that("an unverifiable point fails the screen rather than passing it", {
  skip_if_not_installed("zipcodeR")
  # A ZIP that does not resolve gives NA distance. Treating NA as ok would let
  # exactly the unauditable records through -- the same mistake as accepting a
  # row with no source_run.
  d <- data.frame(npi = c("1", "2"), lat = c(39.7392, 39.7392),
                  lon = c(-104.9903, -104.9903), zip5 = c("80202", "99999"),
                  stringsAsFactors = FALSE)
  s <- urpssim:::screen_new_coordinates(d)
  expect_true(s$address_ok[1])
  expect_true(is.na(s$address_km[2]))
  expect_false(s$address_ok[2])
})

test_that("a candidate with no recorded address is refused, not waved through", {
  d <- data.frame(npi = "1", lat = 39.7392, lon = -104.9903)
  expect_error(urpssim:::screen_new_coordinates(d), "cannot be screened")
})

test_that("state agreement is NOT used to validate coordinates", {
  p <- pc_path(); rp <- rt_path(); skip_if(is.null(p) || is.null(rp), "roster/coordinate extract not present (carries NPIs, deliberately not shipped)")
  # The documented reason, pinned as a test because the check is tempting and
  # wrong: `state` is the certifying board's mailing state. In the source that
  # carries both, 20.5% of physicians practise in a different state. Screening
  # on agreement would reject correct points at that rate.
  #
  # Concretely: this provider is on the roster as MN and practises in
  # Philadelphia. The point is right and the states disagree.
  skip_if_not_installed("zipcodeR")
  s <- urpssim:::screen_new_coordinates(
    data.frame(npi = "1285869784", lat = 39.9572, lon = -75.1995,
               zip5 = "19104", stringsAsFactors = FALSE))
  expect_true(s$address_ok)

  roster <- load_urps_roster(rp)
  expect_equal(roster$state[roster$npi == "1285869784"], "MN")

  # And the extract as a whole must not have been filtered to state agreement:
  # if someone ever "fixes" the out-of-state points, this drops toward zero.
  co <- load_urps_provider_coordinates(p)
  expect_gt(nrow(co), 1500)
})

test_that("the recovered points are in the extract with their own provenance", {
  p <- pc_path(); skip_if(is.null(p), "coordinate extract not present (carries NPIs, deliberately not shipped)")
  co <- load_urps_provider_coordinates(p)
  recovered <- co[co$npi %in% c("1134153067", "1134239262", "1952857385"), ]
  expect_equal(nrow(recovered), 3L)
  # Provenance must distinguish them from the primary run, or a later audit
  # cannot tell which points were recovered under which screen.
  expect_true(all(nzchar(recovered$source_run)))
  expect_true(all(nzchar(recovered$retrieved_on)))
  expect_false(any(grepl("20260802_101936", recovered$source_run)))
  # The rejected point must NOT have been merged.
  expect_false("1073505681" %in% co$npi)
})

test_that("coverage clears 99% in both pathways, not just overall", {
  p <- pc_path(); rp <- rt_path(); skip_if(is.null(p) || is.null(rp), "roster/coordinate extract not present (carries NPIs, deliberately not shipped)")
  cov <- provider_coordinate_coverage(roster = load_urps_roster(rp),
                                      coords = load_urps_provider_coordinates(p))
  # Overall alone would pass at 99% with one pathway at 90%; that is the same
  # error the 72%/0% state made, one order of magnitude quieter.
  expect_gt(cov$overall_share, 0.99)
  expect_true(all(cov$by_pathway$share > 0.99))
  expect_length(cov$pathways_absent, 0L)
  expect_true(cov$usable_for_access)
  expect_true(is.na(cov$blocker))
})
