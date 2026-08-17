# The numerical validation gate. UI prevalence is clinically approved but must
# also demonstrate that the implementation reproduces the reference estimand on
# the reference vintage. Fails closed.
#
# History: exact label matching returned 5.7% here against Nygaard's 15.7% --
# the 2005-2006 cycle attaches the conjunction to the penultimate option
# ("small splashes, or"), so the MIDDLE category of both ISI items silently
# became NA. Nothing errored. Normalised, the same code returns 15.7%.

skip_if_offline <- function() {
  skip_if_not(requireNamespace("nhanesA", quietly = TRUE), "nhanesA package not available")
  ok <- tryCatch(!is.null(nhanesA::nhanes("DEMO_D")), error = function(e) FALSE)
  skip_if_not(ok, "NHANES not reachable")
}

test_that("the pipeline reproduces Nygaard 2005-2006 within 1 percentage point", {
  skip_on_cran(); skip_if_offline()
  suppressPackageStartupMessages({library(dplyr); library(survey)})
  options(survey.lonely.psu = "adjust")
  norm <- function(x) {
    x <- tolower(as.character(x)); x <- gsub("^or\\s+", "", x)
    x <- gsub("[[:space:],]*\\bor\\b[[:space:]]*$", "", x)
    x <- gsub("[[:punct:]]+$", "", x); trimws(x)
  }
  demo <- nhanesA::nhanes("DEMO_D"); kiq <- nhanesA::nhanes("KIQ_U_D")
  d <- demo |>
    dplyr::select(SEQN, RIAGENDR, RIDAGEYR, RIDEXPRG, wt = WTMEC2YR,
                  SDMVPSU, SDMVSTRA) |>
    dplyr::inner_join(dplyr::select(kiq, SEQN, KIQ005, KIQ010), by = "SEQN") |>
    dplyr::filter(as.character(RIAGENDR) == "Female", RIDAGEYR >= 20,
                  is.na(RIDEXPRG) | !grepl("^Yes", as.character(RIDEXPRG))) |>
    dplyr::mutate(
      f = dplyr::case_when(norm(KIQ005)=="never"~0L, norm(KIQ005)=="less than once a month"~1L,
                           norm(KIQ005)=="a few times a month"~2L, norm(KIQ005)=="a few times a week"~3L,
                           norm(KIQ005)=="every day and/or night"~4L, TRUE~NA_integer_),
      a = dplyr::case_when(norm(KIQ010)=="drops"~1L, norm(KIQ010)=="small splashes"~2L,
                           norm(KIQ010)=="more"~3L, TRUE~NA_integer_),
      isi = dplyr::case_when(f==0L~0L, !is.na(f)&!is.na(a)~f*a, TRUE~NA_integer_),
      ui = dplyr::if_else(!is.na(isi), as.integer(isi>=3L), NA_integer_)) |>
    dplyr::filter(!is.na(ui), !is.na(wt), wt > 0)

  des <- survey::svydesign(id=~SDMVPSU, strata=~SDMVSTRA, weights=~wt, nest=TRUE, data=d)
  replicated <- as.numeric(coef(survey::svymean(~ui, des)))

  expect_lte(abs(replicated - 0.157), 0.01)
  # and the analytic sample should be close to Nygaard's 1,961
  expect_gt(nrow(d), 1800); expect_lt(nrow(d), 2100)
})

test_that("middle ISI categories are never silently empty", {
  # the specific failure mode: a label-matching miss shows up as a MISSING
  # score level, not as an error
  skip_on_cran(); skip_if_offline()
  norm <- function(x) {
    x <- tolower(as.character(x)); x <- gsub("^or\\s+", "", x)
    x <- gsub("[[:space:],]*\\bor\\b[[:space:]]*$", "", x)
    x <- gsub("[[:punct:]]+$", "", x); trimws(x)
  }
  k <- nhanesA::nhanes("KIQ_U_D")
  f <- norm(k$KIQ005); a <- norm(k$KIQ010)
  expect_true(any(f == "a few times a week", na.rm = TRUE))   # frequency score 3
  expect_true(any(a == "small splashes", na.rm = TRUE))       # amount score 2
})
