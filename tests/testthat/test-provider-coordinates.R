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

test_that("the coordinate extract carries points and provenance, not names", {
  p <- pc_path(); skip_if(is.null(p))
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
  p <- pc_path(); skip_if(is.null(p))
  skip_if(is.null(tryCatch(load_urps_roster(), error = function(e) NULL)))
  cv <- provider_coordinate_coverage()

  expect_equal(cv$n_roster, nrow(load_urps_roster()))
  expect_true(all(c("pathway", "n", "with_coord", "share") %in% names(cv$by_pathway)))
  # THE SUBSTANTIVE CHECK. 72% overall would pass any reasonable threshold. It
  # is 93.5% in one pathway and 0.0% in the other, which is not 72% coverage --
  # it is a missing pathway wearing an acceptable average.
  expect_gt(length(cv$pathways_absent), 0)
  expect_false(cv$usable_for_access)
  expect_true(nzchar(cv$blocker))
  expect_match(cv$blocker, "understates access")
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

  full <- provider_coordinate_coverage(roster, tibble::tibble(npi = as.character(1:100)))
  expect_true(full$usable_for_access)
  expect_true(is.na(full$blocker))
})

test_that("the register reports coordinates as PARTIAL and still refuses to wire", {
  g <- geographic_access_status()
  st <- stats::setNames(g$components$state, g$components$component)
  expect_equal(unname(st["provider_coordinates"]), "PARTIAL")
  # Progress on one input must not flip the overall verdict: isochrones and the
  # validation gate are still missing, and the ordering trap still applies.
  expect_false(g$resolved)
  expect_equal(unname(st["drive_time_isochrones"]), "MISSING")
  expect_equal(unname(st["supply_machinery"]), "DORMANT")
  expect_match(g$ordering_trap, "Do NOT wire")
})
