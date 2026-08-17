# A STRUCTURALLY VALID PATHWAY FIXTURE FOR THE CI GATE SCRIPTS.
#
# WHY THIS EXISTS. The shipped condition_service_pathway() carries
# per_entering = 1.00 on new_consultation, which turns a prevalence STOCK into
# an annual FLOW -- every prevalent treated patient counted as newly presenting
# each year. assert_incident_not_prevalent() refuses it, correctly, and it stays
# refused until the parameter is sourced (docs/INCIDENT_ENTRY_ESTIMAND.md).
#
# The gate scripts that source this file -- canaries, metamorphic, scientific
# invariants, refusal gates -- are not testing that parameter. They test
# STRUCTURAL properties: does the cascade multiply correctly, does a mutation
# get detected, is the result invariant to row order. Those need a pathway that
# RUNS. Without one, every Layer-1 and Layer-2 gate goes red for the same single
# reason, and 31 informative refusals become ~50 uninformative ones.
#
# THIS IS NOT A CANDIDATE VALUE AND MUST NEVER BE READ AS ONE. 0.25 is chosen
# only because it is a plausible FLOW rather than a stock. The real parameter is
# unresolved, its estimator is pre-registered before data access, and it will be
# estimated from longitudinal all-payer outpatient claims -- not from this file,
# not from the 0.297 utilization ratio, and not from whatever makes a gate pass.
#
# THIS IS NOT A BYPASS. It does not disable the guard, and it is not reachable
# from library code: nothing in R/ sources it. The shipped configuration is
# still refused everywhere it is actually used, and that refusal is asserted by
# the scientific-readiness gate (.github/scripts/assert-canonical-science.R),
# which runs the REAL table and stays red. When per_entering is sourced, this
# file should be DELETED and the gates should run against the real table.

ci_pathway_fixture <- function() {
  pw <- condition_service_pathway()
  pw$per_entering[pw$service == "new_consultation"] <- 0.25
  pw
}
