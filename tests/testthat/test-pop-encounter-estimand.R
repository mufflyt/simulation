# The production POP estimand is ENCOUNTERS with >= 1 operative treatment of
# prolapse, counted once. Summing procedure-family counts is explicitly NOT
# approved: a vaginal hysterectomy + uterosacral suspension + anterior/posterior
# repair is one POP operation, and family sums make it three or four.

.db <- "/Volumes/MufflySamsung/DuckDB/chia_cadr.duckdb"

test_that("the encounter view deduplicates, and family sums would not", {
  skip_if(!file.exists(.db), "CHIA case-mix database not attached")
  con <- DBI::dbConnect(duckdb::duckdb(), .db, read_only = TRUE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  
  has_view <- tryCatch({
    DBI::dbExistsTable(con, DBI::Id(schema = "chia_casemix", table = "v_pop_encounters"))
  }, error = function(e) FALSE)
  skip_if(!has_view, "chia_casemix.v_pop_encounters view not built in DuckDB")

  d <- DBI::dbGetQuery(con, "
    SELECT _data_year AS fy, count(*) AS encounters,
           sum(apical_abdominal_mesh::INT + transvaginal_mesh::INT
             + vaginal_native_tissue::INT + obliterative::INT
             + pop_hysterectomy::INT) AS family_sum
    FROM chia_casemix.v_pop_encounters GROUP BY 1")

  # one row per encounter
  n_rows <- DBI::dbGetQuery(con, "SELECT count(*) n FROM chia_casemix.v_pop_encounters")$n
  n_enc  <- DBI::dbGetQuery(con,
    "SELECT count(*) n FROM (SELECT DISTINCT RecordType20ID, _data_year
                             FROM chia_casemix.v_pop_encounters)")$n
  expect_equal(n_rows, n_enc)

  # and the naive sum materially exceeds it -- this is why the sum is not the anchor
  expect_true(all(d$family_sum >= d$encounters))
  expect_gt(max(d$family_sum / d$encounters), 1.2)
})

test_that("compartment is never asserted from the procedure code", {
  skip_if_not(file.exists("../../config/chia_urps_inpatient_codes.yml"))
  fam <- yaml::read_yaml("../../config/chia_urps_inpatient_codes.yml")$families
  # transvaginal mesh must be flagged compartment-unspecified, never anterior/posterior
  expect_equal(fam$transvaginal_mesh_pop$compartment, "not_identifiable_from_pcs")
  expect_false("anterior_posterior_repair" %in% names(fam))
  # and native-tissue repair must require a POP diagnosis, since 0UQG is just "Repair Vagina"
  expect_equal(fam$vaginal_native_tissue_pop_repair$requires_diagnosis,
               "pelvic_organ_prolapse")
  expect_true(all(c("genitourinary_fistula", "traumatic_vaginal_injury",
                    "obstetric_perineal_repair") %in%
                  fam$vaginal_native_tissue_pop_repair$exclusion_diagnoses))
})

test_that("the POP diagnosis family spans the seam identically", {
  skip_if_not(file.exists("../../config/chia_urps_inpatient_codes.yml"))
  q <- yaml::read_yaml("../../config/chia_urps_inpatient_codes.yml")$diagnosis_qualifiers
  # ICD-9 618.x contains vault prolapse (618.5); ICD-10 moved it OUT of N81 to N99.3
  expect_true("N993" %in% q$pelvic_organ_prolapse$icd10cm)
  expect_true("618" %in% q$pelvic_organ_prolapse$icd9cm)
})


test_that("the YAML declares the aggregation contract, not just the code", {
  skip_if_not(file.exists("../../config/calibration_targets.yml"))
  a <- yaml::read_yaml("../../config/calibration_targets.yml")$anchors$prolapse_procedure_volume
  expect_equal(a$estimand$unit, "encounter")
  expect_equal(a$estimand$aggregation_rule, "unique_encounter")
  # the contract must say the sum is NOT a valid anchor, in the config, so the
  # rule survives a rewrite of the implementation
  expect_false(a$estimand$procedure_family_sum_is_valid_anchor)
  expect_true(all(c("compartment_not_always_identifiable",
                    "procedure_families_may_overlap",
                    "N99.3_required_across_icd_seam") %in%
                  a$clinical_review$known_limitations))
})

test_that("the model's prolapse_procedure unit is an encounter, not a procedure", {
  skip_if_not(requireNamespace("pkgload", quietly = TRUE))
  p <- condition_service_pathway()
  pop <- p[p$condition == "pop" & p$service == "prolapse_procedure", ]
  # one surgical episode per patient entering the procedure stage; the
  # recurrence row is a SECOND episode, not a second procedure within one
  expect_equal(pop$per_entering[pop$stage == "procedure"], 1.0)
  expect_true(all(pop$per_entering <= 1.0))
  # so the model and the anchor share a unit, and any gap is a cascade
  # question rather than a definitional one
})
