#!/usr/bin/env python3
"""
Assertion helpers for analyses built on chia_cadr.duckdb.

These fail loudly at query time, before a wrong number reaches a figure. They
exist because this dataset's failure mode is silence: a query written for one
era returns NULL for the other, a rate whose numerator and denominator have
different age ceilings looks fine, and CADR tables with near-identical names sit
at different cohort stages.

    from chia_guards import cohort, rate_contract, era_span

    con = duckdb.connect(DB, read_only=True)
    tbl = cohort(con, "cadr_wuid_16937")        # declare the stage; get the table
    era_span(con, "chia_casemix.v_hdd_discharge_all_years", 2004, 2018)
    rate_contract(con, "chia_casemix.v_urogyn_female_inpatient_rates")

Each raises GuardError on violation. None of them is optional politeness -- a
silent wrong answer is the thing they are here to prevent.
"""
from __future__ import annotations


class GuardError(AssertionError):
    """A declared expectation about the data does not hold."""


# --------------------------------------------------------------------------
def cohort(con, stage_id: str, *, verify_counts: bool = True) -> str:
    """Declare which CADR cohort stage an analysis is on; return its table.

    CADR was filtered repeatedly. `cadr_2023_results_one_row_per_wu_id_jeeves`
    (18,544) and `cadr_2023_results_one_row_per_wu_id` (16,937) differ by 1,607
    patients and differ by one word. Intent cannot be inferred from a query, so
    it has to be stated.
    """
    row = con.execute(
        "SELECT qualified_table, patients, rows, subset_of, description "
        "FROM meta.cohort_stage WHERE stage_id = ?", [stage_id]).fetchone()
    if row is None:
        known = [r[0] for r in con.execute(
            "SELECT stage_id FROM meta.cohort_stage ORDER BY patients DESC").fetchall()]
        raise GuardError(f"unknown cohort stage {stage_id!r}; known stages: {known}")
    table, patients, rows, subset_of, _ = row
    if verify_counts:
        actual = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        if actual != rows:
            raise GuardError(
                f"cohort {stage_id}: {table} has {actual:,} rows, registry says {rows:,}. "
                "The table changed under the registry -- re-verify before trusting any "
                "number computed from it.")
    return table


# --------------------------------------------------------------------------
def rate_contract(con, rate_view: str) -> dict:
    """Assert a rate's numerator and denominator share an age ceiling.

    A rate whose numerator stops at 89 and whose denominator runs to 85+ (which
    includes 90+) is biased downward in the top band with nothing on screen to
    say so. This refuses to let that pass unnoticed.
    """
    row = con.execute(
        "SELECT numerator_source, denominator_source, numerator_max_age, "
        "       denominator_max_age, denominator_modelled_above, notes "
        "FROM meta.rate_contract WHERE rate_view = ?", [rate_view]).fetchone()
    if row is None:
        raise GuardError(
            f"{rate_view} has no entry in meta.rate_contract. Register its numerator "
            "and denominator ceilings before computing rates from it.")
    num_src, den_src, num_max, den_max, modelled_above, notes = row
    if num_max != den_max:
        raise GuardError(
            f"{rate_view}: numerator ceiling {num_max} != denominator ceiling {den_max}. "
            f"The top band is biased. numerator={num_src}; denominator={den_src}")
    return {"numerator_source": num_src, "denominator_source": den_src,
            "max_age": num_max, "denominator_modelled_above": modelled_above,
            "notes": notes}


# --------------------------------------------------------------------------
def era_span(con, object_name: str, first_year: int | None = None,
             last_year: int | None = None, *, year_col: str = "_data_year") -> tuple:
    """Assert an era-spanning object still covers the years it is supposed to.

    This is the COALESCE-forgotten class of bug: the query runs, returns rows,
    and silently omits an era. Catching it needs an expectation stated up front.
    """
    reg = con.execute(
        "SELECT first_year, last_year, n_years FROM meta.era_span WHERE object_name = ?",
        [object_name]).fetchone()
    if reg is None and (first_year is None or last_year is None):
        raise GuardError(
            f"{object_name} is not in meta.era_span and no explicit range was given. "
            "Declare the years this analysis intends to cover.")
    exp_first, exp_last, exp_n = reg if reg else (first_year, last_year, None)
    if first_year is not None:
        exp_first = first_year
    if last_year is not None:
        exp_last = last_year

    lo, hi, n = con.execute(
        f"SELECT min({year_col}), max({year_col}), count(DISTINCT {year_col}) "
        f"FROM {object_name}").fetchone()
    if lo != exp_first or hi != exp_last:
        raise GuardError(
            f"{object_name} spans {lo}-{hi}, expected {exp_first}-{exp_last}. "
            "An era is missing -- check for a renamed table or a column that only "
            "exists in one era.")
    if exp_n is not None and n != exp_n:
        raise GuardError(
            f"{object_name} covers {n} distinct years, expected {exp_n}. "
            "A year is missing from the middle of the range.")
    return lo, hi, n


# --------------------------------------------------------------------------
def describe(con) -> None:
    """Print every registered contract -- what an analysis is allowed to assume."""
    for title, sql in [
        ("COHORT STAGES",
         "SELECT stage_id, qualified_table, patients, subset_of FROM meta.cohort_stage ORDER BY patients DESC"),
        ("RATE CONTRACTS",
         "SELECT rate_view, numerator_max_age, denominator_max_age, denominator_modelled_above FROM meta.rate_contract"),
        ("ERA SPANS",
         "SELECT object_name, first_year, last_year, n_years FROM meta.era_span ORDER BY object_name"),
    ]:
        print(f"\n=== {title} ===")
        for r in con.execute(sql).fetchall():
            print("  " + " | ".join(str(x) for x in r))


if __name__ == "__main__":
    import argparse, duckdb
    ap = argparse.ArgumentParser(description="show the registered data contracts")
    ap.add_argument("--db", required=True)
    a = ap.parse_args()
    describe(duckdb.connect(a.db, read_only=True))
