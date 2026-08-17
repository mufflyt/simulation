#!/usr/bin/env python3
"""
Column-presence and rename audit for the CHIA per-year table families.

Five era renames were found one at a time by tripping over them -- physician
(twice), age/sex, and facility. This finds them all at once, and answers the
question directly: is every column present in every year, and where it is not,
is there another column that covers exactly the missing years?

Method: build column -> {years} for a family. A column present in every year is
stable. Columns with partial coverage are candidates. Two or more candidates
whose year-sets are pairwise DISJOINT and whose UNION is the full span are a
rename group -- that is the signature of a field renamed at an era boundary.
Name similarity is reported but never required, because CHIA's renames are not
always similar (Sex -> SexLDS is, SiteOrgID -> IdOrgSite is not).

Usage: column_audit.py --db PATH [--family hdd_discharge] [--show-stable]
"""
from __future__ import annotations
import argparse, difflib, itertools, re, sys
import duckdb


def coverage(con, schema: str, prefix: str) -> tuple[dict[str, set[int]], list[int]]:
    rows = con.execute("""
        SELECT table_name, column_name FROM duckdb_columns()
        WHERE schema_name = ? AND table_name LIKE ? || '%'
    """, [schema, prefix]).fetchall()
    cols: dict[str, set[int]] = {}
    years: set[int] = set()
    for tbl, col in rows:
        m = re.search(r"_((?:19|20)\d{2})$", tbl)   # skip _working / _copy variants
        if not m:
            continue
        yr = int(m.group(1))
        years.add(yr)
        if col.startswith("_"):                      # provenance columns we added
            continue
        cols.setdefault(col, set()).add(yr)
    return cols, sorted(years)


def signature_table(partial, years):
    """Distinct year-coverage patterns and the columns that share each."""
    sig = {}
    for c, y in partial.items():
        sig.setdefault(frozenset(y), []).append(c)
    return sorted(sig.items(), key=lambda kv: (-len(kv[0]), min(kv[0])))


def complementary_sets(sigs, years, max_n=4):
    """Signature combinations that partition the full span -- the era boundaries."""
    full = set(years)
    out = []
    keys = [k for k, _ in sigs]
    for n in range(2, max_n + 1):
        for combo in itertools.combinations(keys, n):
            if sum(len(k) for k in combo) == len(full) and set().union(*combo) == full:
                out.append(combo)
    return out


def norm(name: str) -> str:
    """Strip the decorations CHIA adds when it renames: LDS, Id/Number, case."""
    n = name.lower()
    for junk in ("lds", "code", "number", "id", "_"):
        n = n.replace(junk, "")
    return n


def rename_candidates(partial, combos, threshold=0.55):
    """Pair columns across complementary signatures, ranked by name similarity."""
    seen, out = set(), []
    for combo in combos:
        buckets = [[c for c, y in partial.items() if frozenset(y) == k] for k in combo]
        for pair in itertools.product(*buckets):
            if len(set(pair)) != len(pair):
                continue
            sims = [difflib.SequenceMatcher(None, norm(a), norm(b)).ratio()
                    for a, b in itertools.combinations(pair, 2)]
            sim = min(sims)                      # ALL members must look alike
            if sim < threshold:
                continue
            key = frozenset(pair)
            if key in seen:
                continue
            seen.add(key)
            out.append((sorted(pair, key=lambda c: min(partial[c])), sim))
    return sorted(out, key=lambda t: -t[1])


def case_collisions(cols):
    """Columns differing only by case -- these break case-sensitive joins."""
    by_lower = {}
    for c in cols:
        by_lower.setdefault(c.lower(), []).append(c)
    return {k: v for k, v in by_lower.items() if len(v) > 1}


def audit(con, schema: str, prefix: str, show_stable: bool) -> int:
    cols, years = coverage(con, schema, prefix)
    if not years:
        print(f"no per-year tables matching {schema}.{prefix}*")
        return 0
    full = set(years)
    stable = {c for c, y in cols.items() if y == full}
    partial = {c: y for c, y in cols.items() if y != full}

    print(f"\n{'='*78}\n{schema}.{prefix}*   years {min(years)}-{max(years)} "
          f"({len(years)} tables)\n{'='*78}")
    print(f"{len(cols)} distinct columns: {len(stable)} in EVERY year, {len(partial)} partial")

    sigs = signature_table(partial, years)
    print(f"\n--- year-coverage patterns ({len(sigs)} distinct) ---")
    for k, members in sigs:
        ys = sorted(k)
        rng = f"{ys[0]}-{ys[-1]}" if len(ys) > 1 else str(ys[0])
        gap = "" if ys == list(range(ys[0], ys[-1]+1)) else "  [non-contiguous]"
        sample = ", ".join(sorted(members)[:4])
        more = f" +{len(members)-4}" if len(members) > 4 else ""
        print(f"  {rng:<12} {len(ys):>2}yr  {len(members):>3} cols{gap}  e.g. {sample}{more}")

    combos = complementary_sets(sigs, years)
    print(f"\n--- era boundaries: patterns that partition the span ({len(combos)}) ---")
    for combo in combos[:8]:
        print("  " + "  |  ".join(
            f"{min(k)}-{max(k)}" if len(k) > 1 else str(min(k)) for k in combo))

    cands = rename_candidates(partial, combos)
    print(f"\n--- rename candidates (same field, renamed at a boundary) ---")
    if not cands:
        print("  none above the similarity threshold")
    for combo, sim in cands[:25]:
        parts = " -> ".join(f"{c} [{min(partial[c])}-{max(partial[c])}]" for c in combo)
        print(f"  {sim:.2f}  {parts}")

    cc = case_collisions(cols)
    print(f"\n--- columns differing only by CASE ({len(cc)}) ---")
    for k, v in sorted(cc.items()):
        yrs = {c: f"{min(partial.get(c, full))}-{max(partial.get(c, full))}" for c in v}
        print(f"  {' vs '.join(v)}   {yrs}")

    if show_stable:
        print(f"\n--- present in every year ({len(stable)}) ---")
        for c in sorted(stable):
            print(f"      {c}")
    return len(cands)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--family", action="append",
                    help="table prefix, e.g. hdd_discharge (repeatable)")
    ap.add_argument("--show-stable", action="store_true")
    a = ap.parse_args()
    con = duckdb.connect(a.db, read_only=True)
    families = a.family or ["hdd_discharge", "ood_observation"]
    total = 0
    for fam in families:
        total += audit(con, "chia_casemix", fam, a.show_stable)
    con.close()
    print(f"\n{total} rename group(s) detected across {len(families)} family/families")
    return 0


if __name__ == "__main__":
    sys.exit(main())
