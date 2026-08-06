# The mufflyaccess contract pin (R/core-contract_pin).
#
# THE FAILURE THIS EXISTS FOR, observed rather than imagined. Two materially
# different mufflyaccess builds both reported version 0.10.0 during one working
# session: 56 exports without urps_retirement_status(), and 98 exports with it.
# They also disagreed about how n_retired is served -- integer zeros vs NA --
# which is the field the back-test attrition guard reads. Any check of the form
# packageVersion("mufflyaccess") >= "0.10.0" passes for both.

test_that("DESCRIPTION pins the contract to a commit, not a bare repo", {
  root <- Filter(function(p) file.exists(file.path(p, "DESCRIPTION")),
                 c(".", "..", file.path("..", "..")))
  skip_if(length(root) == 0)
  d <- readLines(file.path(root[1], "DESCRIPTION"), warn = FALSE)
  # The Remotes ENTRY, not every line naming the package -- Imports lists it too,
  # and matching that would pass on a bare dependency declaration.
  start <- grep("^Remotes:", d)
  expect_length(start, 1L)
  block <- d[seq(start + 1L, length(d))]
  block <- block[seq_len(which(!grepl("^\\s", block))[1] - 1L)]
  remote <- grep("mufflyaccess", block, value = TRUE)
  expect_length(remote, 1L)
  # An unpinned remote resolves HEAD, so two machines installing months apart
  # can disagree about what the contract says while both reporting 0.10.0.
  expect_match(remote, "mufflyaccess@[0-9a-f]{40}")
  expect_match(remote, MUFFLYACCESS_PINNED_SHA, fixed = TRUE)
  expect_equal(nchar(MUFFLYACCESS_PINNED_SHA), 40L)
})

test_that("the required-export list matches what the package actually calls", {
  root <- Filter(function(p) file.exists(file.path(p, "DESCRIPTION")),
                 c(".", "..", file.path("..", "..")))
  skip_if(length(root) == 0)
  files <- c(list.files(file.path(root[1], "R"), pattern = "[.]R$", full.names = TRUE),
             list.files(file.path(root[1], "scripts"), pattern = "[.]R$",
                        full.names = TRUE, recursive = TRUE))
  called <- unique(unlist(lapply(files, function(f) {
    code <- sub("#.*$", "", readLines(f, warn = FALSE))
    m <- regmatches(code, gregexpr("mufflyaccess::[A-Za-z_.][A-Za-z0-9_.]*", code))
    sub("mufflyaccess::", "", unlist(m))
  })))
  # A frozen list that drifts from the real call sites is worse than none: it
  # would certify a build as usable while the package calls something it lacks.
  expect_setequal(called, urpssim:::MUFFLYACCESS_REQUIRED_EXPORTS)
})

test_that("the installed build is identified by commit, not by version alone", {
  skip_if_not_installed("mufflyaccess")
  b <- mufflyaccess_build()
  expect_true(b$installed)
  expect_true(nzchar(b$version))
  # A build with no RemoteSha is UNIDENTIFIED, not mismatched -- a local install
  # is not evidence of the wrong commit.
  expect_true(is.na(b$sha) || nchar(b$sha) == 40L)
  expect_true(is.na(b$sha_matches_pin) || is.logical(b$sha_matches_pin))
})

test_that("capability is checked, and it is checked before identity", {
  skip_if_not_installed("mufflyaccess")
  b <- mufflyaccess_build()
  # The installed build must actually provide everything the package calls.
  expect_equal(b$missing_exports, character(0))
  expect_true(b$usable)
  expect_true(assert_mufflyaccess_contract(mode = "strict"))
})

test_that("a build missing an export is refused whatever version it claims", {
  # Simulated rather than installed: the point is that the VERSION is fine and
  # the build still cannot run this package.
  fake <- list(installed = TRUE, version = "0.10.0", sha = "0123456789abcdef0123456789abcdef01234567",
               sha_matches_pin = FALSE, n_exports = 56L,
               missing_exports = c("urps_retirement_status", "urps_entry_counts"),
               usable = FALSE)
  local_mocked_bindings(mufflyaccess_build = function() fake)
  expect_error(assert_mufflyaccess_contract(mode = "strict"), "does not export")
  expect_error(assert_mufflyaccess_contract(mode = "strict"), "urps_retirement_status")
  # The message must name the remedy, including the pinned commit.
  msg <- tryCatch(assert_mufflyaccess_contract(mode = "strict"), error = conditionMessage)
  expect_match(msg, "install_github", fixed = TRUE)
  expect_match(msg, MUFFLYACCESS_PINNED_SHA, fixed = TRUE)
  expect_message(assert_mufflyaccess_contract(mode = "relaxed"), "does not export")
})

test_that("a usable build on a different commit warns rather than failing", {
  fake <- list(installed = TRUE, version = "0.11.0",
               sha = "ffffffffffffffffffffffffffffffffffffffff",
               sha_matches_pin = FALSE, n_exports = 120L,
               missing_exports = character(0), usable = TRUE)
  local_mocked_bindings(mufflyaccess_build = function() fake)
  # Failing here would block a legitimate contract upgrade. The build satisfies
  # every capability; it is simply not the pinned commit.
  expect_message(assert_mufflyaccess_contract(mode = "strict"), "differs from the pinned commit")
  expect_true(assert_mufflyaccess_contract(mode = "strict"))
})

test_that("the tolerant retirement guard survives the build swap that motivated the pin", {
  skip_if_not_installed("mufflyaccess")
  # The 56-export build served n_retired as integer zeros; the 98-export build
  # serves NA and exposes the accessor. backtest_retirement_regime() reports the
  # same SEMANTIC state on either, via a different route -- which is what let
  # this repository absorb the swap mid-session without a code change.
  r <- backtest_retirement_regime()
  expect_true(as.character(r) %in% c("not_ascertained", "zero", "ascertained"))
  expect_true(nzchar(attr(r, "source")))
  v <- validate_backtest_target(acknowledge_no_attrition = TRUE)
  expect_false(v$observed_series_applies_attrition)
  expect_false(v$retirement_ascertained)
})
