#!/usr/bin/env python3
"""
Computational exploration of Erdős Problem 181: diagonal Ramsey numbers R(Q_n, Q_n).

Mathematical setup
------------------
Let Q_n be the n-dimensional hypercube: 2^n vertices (binary n-tuples), edges between
vertices differing in exactly one coordinate. It has n·2^{n-1} edges and is n-regular
and bipartite.

The diagonal Ramsey number R(Q_n) := R(Q_n, Q_n) is the least N such that every
2-coloring of the edges of K_N contains a monochromatic (embedded) copy of Q_n.
“Embedded” means: pick 2^n distinct vertices of K_N and map V(Q_n) onto them so that
every edge of Q_n becomes an edge of K_N of the same color (red or blue). Since K_N is
complete, only the vertex mapping matters.

Conjecture (problem context): R(Q_n) = O(2^n), i.e. R(Q_n)/2^n stays bounded.

Encoding “no monochromatic Q_n” for SAT (lower bounds)
-------------------------------------------------------
Variables: one Boolean x_{uv} per unordered pair {u,v} ⊂ V(K_N), say x=1 means RED,
x=0 means BLUE.

For each injective map f : V(Q_n) → V(K_N) (an “embedding template”), let
E_f = { {f(a),f(b)} : (a,b) ∈ E(Q_n) }. If all edges in E_f are RED, we have a red Q_n;
if all are BLUE, a blue Q_n. To forbid monochromatic Q_n for this template:

  NOT(all RED):  ⋁_{e ∈ E_f} ¬x_e
  NOT(all BLUE): ⋁_{e ∈ E_f} x_e

So two clauses per template. A satisfying assignment = a 2-coloring of K_N with no
monochromatic Q_n → N is a lower bound on R(Q_n).

The number of injective maps is N!/(N-2^n)!, which explodes; this script caps work by
default to modest (n, N). Use symmetry breaking (fix images of the first k cube
vertices) to shrink the search space.

Upper bounds
------------
Trivial: if every coloring of K_N has a monochromatic Q_n, then R(Q_n) ≤ N.
Exhaustive enumeration over 2^(N choose 2) colorings is feasible only for tiny N (e.g.
N ≤ 5). We include a brute-force routine for micro cases and document limits.

Layered / Turán-style construction
----------------------------------
Partition V(K_N) into two parts A and B. Color edges inside A and inside B RED; color
edges between A and B BLUE. Then the RED graph is K_{|A|} ⊔ K_{|B|} (disjoint union of
two cliques) and the BLUE graph is complete bipartite K_{|A|,|B|}.

We test whether either monochromatic graph contains Q_n as a subgraph (using the
backtracking checker below). If not, we get a concrete lower bound witness for that N.

References (context, not exhaustive)
------------------------------------
- Erdős problems database: Problem 181.
- Q_n standard: e.g. Bollobás, modern graph theory texts.
"""

from __future__ import annotations

import argparse
import itertools
import math
import sys
import time
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Optional: PySAT for lower-bound search
# ---------------------------------------------------------------------------
try:
    from pysat.formula import CNF
    from pysat.solvers import Glucose3

    HAS_PYSAT = True
except ImportError:
    HAS_PYSAT = False
    CNF = None  # type: ignore
    Glucose3 = None  # type: ignore

import matplotlib.pyplot as plt
import networkx as nx


# --- Hypercube Q_n ----------------------------------------------------------

def hypercube_graph(n: int) -> nx.Graph:
    """Return Q_n with integer vertices 0..2^n-1 (NetworkX uses bit-tuples by default)."""
    g = nx.hypercube_graph(n)
    mapping = {node: i for i, node in enumerate(sorted(g.nodes()))}
    return nx.relabel_nodes(g, mapping)


def hypercube_edges(n: int) -> List[Tuple[int, int]]:
    """Edges of Q_n as unordered pairs (u,v) with u < v."""
    g = hypercube_graph(n)
    out: List[Tuple[int, int]] = []
    for u, v in g.edges():
        if u > v:
            u, v = v, u
        out.append((u, v))
    return sorted(set(out))


def describe_hypercube(n: int) -> str:
    g = hypercube_graph(n)
    return (
        f"Q_{n}: |V|={g.number_of_nodes()}, |E|={g.number_of_edges()}, "
        f"regularity={g.degree(next(iter(g)))}"
    )


# --- K_N edge variable indexing ---------------------------------------------

def edge_index_map(N: int) -> Dict[Tuple[int, int], int]:
    """Map unordered pair (i,j) with i<j to 0..M-1."""
    idx = 0
    m: Dict[Tuple[int, int], int] = {}
    for i in range(N):
        for j in range(i + 1, N):
            m[(i, j)] = idx
            idx += 1
    return m


def num_edges_complete(N: int) -> int:
    return N * (N - 1) // 2


# --- Embeddings: injective maps V(Q_n) -> V(K_N) -----------------------------

def iter_embeddings(
    n: int,
    N: int,
    *,
    fix_initial: int = 0,
) -> Iterator[Tuple[int, ...]]:
    """
    Yield injective maps as tuples (f(0), f(1), ..., f(2^n-1)) with values in range(N).

    If fix_initial=k, fix f(i)=i for i<k (symmetry breaking in K_N: relabel vertices).
    Requires N >= 2^n and k <= 2^n.
    """
    k = max(0, min(fix_initial, 2**n))
    if N < 2**n:
        return
    used = set(range(k))
    rest_cube = list(range(k, 2**n))
    rest_verts = [v for v in range(N) if v not in used]

    if len(rest_verts) < len(rest_cube):
        return

    for perm in itertools.permutations(rest_verts, len(rest_cube)):
        f = list(range(k)) + list(perm)
        yield tuple(f)


def count_embeddings(n: int, N: int, *, fix_initial: int = 0) -> int:
    k = max(0, min(fix_initial, 2**n))
    if N < 2**n:
        return 0
    r = 2**n - k
    avail = N - k
    if r < 0 or avail < r:
        return 0
    m = 1
    for t in range(r):
        m *= avail - t
    return m


# --- SAT: satisfiable coloring with no monochromatic Q_n? -------------------

def build_sat_instance(
    n: int,
    N: int,
    *,
    fix_initial: int = 0,
    max_clauses: int = 5_000_000,
) -> Optional[CNF]:
    """
    Build CNF over edge variables x_{uv} (True = RED). Returns None if too many clauses.
    """
    if not HAS_PYSAT:
        raise RuntimeError("pysat not installed (pip install python-sat)")
    q_edges = hypercube_edges(n)
    eidx = edge_index_map(N)
    M = num_edges_complete(N)
    cnf = CNF()
    clauses = 0
    emb_n = 0
    for f in iter_embeddings(n, N, fix_initial=fix_initial):
        emb_n += 1
        lit_red: List[int] = []
        lit_blue: List[int] = []
        for u, v in q_edges:
            a, b = f[u], f[v]
            if a > b:
                a, b = b, a
            var = eidx[(a, b)] + 1  # DIMACS 1-based
            lit_red.append(var)
            lit_blue.append(-var)
        # NOT all red: at least one blue
        cnf.append([-lit for lit in lit_red])
        # NOT all blue: at least one red
        cnf.append(lit_red)
        clauses += 2
        if clauses > max_clauses:
            return None
    return cnf


def sat_coloring_exists(
    n: int,
    N: int,
    *,
    fix_initial: int = 0,
    max_clauses: int = 5_000_000,
    verbose: bool = False,
) -> Tuple[bool, float, int]:
    """
    Return (satisfiable, seconds, num_embeddings_used).

    If UNSAT, every coloring has a monochromatic Q_n (under full embedding list) →
    R(Q_n) ≤ N (for this *finite* template set; in practice we enumerate all embeddings
    so it's exact for “no monochromatic Q_n” at this N).
    """
    t0 = time.perf_counter()
    cnf = build_sat_instance(n, N, fix_initial=fix_initial, max_clauses=max_clauses)
    if cnf is None:
        raise RuntimeError("Clause budget exceeded; reduce n or N or increase max_clauses")
    emb_count = count_embeddings(n, N, fix_initial=fix_initial)
    if verbose:
        print(f"    SAT: n={n} N={N} embeddings≈{emb_count} vars={cnf.nv} clauses={len(cnf.clauses)}")
    with Glucose3(bootstrap_with=cnf.clauses) as slv:
        sat = slv.solve()
    dt = time.perf_counter() - t0
    return sat, dt, emb_count


def lower_bound_scan(
    n: int,
    N_max: int,
    *,
    N_min: Optional[int] = None,
    fix_initial: int = 0,
    max_clauses: int = 5_000_000,
    verbose: bool = False,
) -> List[Tuple[int, bool, float]]:
    """For N from N_min..N_max, record whether SAT (witness exists)."""
    lo = N_min if N_min is not None else 2**n
    results: List[Tuple[int, bool, float]] = []
    for N in range(lo, N_max + 1):
        if N < 2**n:
            continue
        ok, dt, _ = sat_coloring_exists(
            n, N, fix_initial=fix_initial, max_clauses=max_clauses, verbose=verbose
        )
        results.append((N, ok, dt))
        if verbose:
            print(f"  N={N}  SAT={ok}  ({dt:.2f}s)")
    return results


# --- Subgraph isomorphism: does H contain Q_n? (backtracking) ---------------

def qn_adjacency_masks(n: int) -> Tuple[int, List[int]]:
    """Return (num_vertices, list of neighbor bitmasks for vertices 0..2^n-1)."""
    g = hypercube_graph(n)
    nv = 2**n
    masks = [0] * nv
    for u, v in g.edges():
        masks[u] |= 1 << v
        masks[v] |= 1 << u
    return nv, masks


def contains_hypercube_subgraph(
    host: nx.Graph,
    n: int,
    *,
    host_vertex_cap: Optional[int] = None,
) -> bool:
    """
    True iff `host` contains a subgraph isomorphic to Q_n.

    Backtracking: assign cube vertices 0..2^n-1 to distinct host vertices; each cube edge
    must be a host edge. Pruning: remaining degree in host must be ≥ remaining degree
    need in cube (n-regular).
    """
    if host.number_of_nodes() < 2**n:
        return False

    nv, qmask = qn_adjacency_masks(n)
    # Restrict to vertices 0..cap-1 if host is on that subset
    nodes = sorted(host.nodes())
    if host_vertex_cap is not None:
        nodes = [v for v in nodes if v < host_vertex_cap]
    if len(nodes) < nv:
        return False

    # Host adjacency as sets for quick lookup
    h_adj: Dict[int, Set[int]] = {v: set(host.neighbors(v)) for v in nodes}

    order = sorted(range(nv), key=lambda u: -bin(qmask[u]).count("1"))  # all degree n

    def dfs(depth: int, used: int, assign: List[int]) -> bool:
        if depth == nv:
            return True
        u = order[depth]
        need = qmask[u]
        for v in nodes:
            bit = 1 << v
            if used & bit:
                continue
            # Every already-mapped neighbor of u in Q_n must map to neighbor of v
            ok = True
            w = need
            while w:
                lsb = w & -w
                i = (lsb.bit_length() - 1)
                w ^= lsb
                if assign[i] >= 0:
                    if assign[i] not in h_adj[v]:
                        ok = False
                        break
            if not ok:
                continue
            assign[u] = v
            if dfs(depth + 1, used | bit, assign):
                return True
            assign[u] = -1
        return False

    assign = [-1] * nv
    return dfs(0, 0, assign)


# --- Layered two-clique + complete bipartite construction -------------------

def layered_two_clique_coloring(N: int, a: int, b: int) -> nx.Graph:
    """
    Vertices 0..a-1 and a..a+b-1 (require a+b=N). RED: edges inside each part.
    BLUE would be cross edges; we return the RED graph as nx.Graph for checking.
    """
    assert a + b == N and a >= 0 and b >= 0
    R = nx.Graph()
    R.add_nodes_from(range(N))
    R.add_edges_from(itertools.combinations(range(a), 2))
    R.add_edges_from(itertools.combinations(range(a, a + b), 2))
    return R


def layered_blue_bipartite(N: int, a: int, b: int) -> nx.Graph:
    """BLUE graph: K_{a,b} between the two parts."""
    assert a + b == N
    B = nx.Graph()
    B.add_nodes_from(range(N))
    for u in range(a):
        for v in range(a, a + b):
            B.add_edge(u, v)
    return B


def best_layered_lower_bound(n: int, N_max: int) -> Tuple[int, Optional[Tuple[int, int]]]:
    """
    Search a,b with a+b=N ≤ N_max, each clique size ≤ 2^n - 1 (else RED contains K_{2^n}
    hence Q_n), and neither RED nor BLUE graph contains Q_n.

    Returns (best_N, (a,b)) or (0, None) if none found.
    """
    best = (0, None)
    for N in range(2**n, N_max + 1):
        for a in range(0, N + 1):
            b = N - a
            if a >= 2**n or b >= 2**n:
                continue  # RED clique would contain Q_n as subgraph
            Rg = layered_two_clique_coloring(N, a, b)
            Bg = layered_blue_bipartite(N, a, b)
            if contains_hypercube_subgraph(Rg, n):
                continue
            if contains_hypercube_subgraph(Bg, n):
                continue
            best = (N, (a, b))
    return best


# --- Tiny exhaustive upper-bound check --------------------------------------

def exhaustive_no_monochromatic_qn(n: int, N: int) -> bool:
    """
    True iff there exists a coloring of K_N with no monochromatic Q_n (brute force).
    Only feasible for very small N (e.g. N≤6 for n=2).
    """
    eidx = edge_index_map(N)
    inv = {v: k for k, v in eidx.items()}
    M = num_edges_complete(N)
    qe = hypercube_edges(n)

    def mono_q_for_coloring(mask: int) -> bool:
        # Build red and blue edge sets as adjacency for fast check
        red_adj: Set[Tuple[int, int]] = set()
        blue_adj: Set[Tuple[int, int]] = set()
        for i in range(M):
            u, v = inv[i]
            if (mask >> i) & 1:
                red_adj.add((u, v) if u < v else (v, u))
            else:
                blue_adj.add((u, v) if u < v else (v, u))

        def has_mono(adj_set: Set[Tuple[int, int]]) -> bool:
            G = nx.Graph()
            G.add_nodes_from(range(N))
            G.add_edges_from(adj_set)
            return contains_hypercube_subgraph(G, n)

        if has_mono(red_adj):
            return True
        if has_mono(blue_adj):
            return True
        return False

    for mask in range(1 << M):
        if not mono_q_for_coloring(mask):
            return True
    return False


# --- Reporting --------------------------------------------------------------

@dataclass
class RowResult:
    n: int
    lower_sat: Optional[int] = None  # largest N where SAT found in scan
    upper_exhaustive: Optional[int] = None  # smallest N where brute says no good coloring
    layered_N: Optional[int] = None
    layered_parts: Optional[Tuple[int, int]] = None
    conjecture_C: float = 1.0
    note: str = ""


def ratio_bounds(row: RowResult) -> Tuple[Optional[float], Optional[float]]:
    """R/2^n from available data (rough)."""
    base = 2**row.n
    lo = row.lower_sat
    hi = row.upper_exhaustive
    return (
        lo / base if lo else None,
        hi / base if hi else None,
    )


def print_table(rows: List[RowResult]) -> None:
    print("\n" + "=" * 80)
    print("Summary (Erdős #181 style — experimental / partial)")
    print("=" * 80)
    print(
        f"{'n':>3} {'|V(Q_n)|':>10} {'LB(SAT)':>10} {'LB(layer)':>12} {'UB(exh)':>10} "
        f"{'C·2^n':>10} {'LB/2^n':>10} {'UB/2^n':>10}"
    )
    print("-" * 80)
    for r in rows:
        vn = 2**r.n
        lb = r.lower_sat or "-"
        ll = r.layered_N or "-"
        ub = r.upper_exhaustive or "-"
        c2 = r.conjecture_C * vn
        rl, ru = ratio_bounds(r)
        print(
            f"{r.n:3d} {vn:10d} {str(lb):>10} {str(ll):>12} {str(ub):>10} "
            f"{c2:10.2f} {rl if rl is not None else '-':>10} {ru if ru is not None else '-':>10}"
        )
        if r.note:
            print(f"    note: {r.note}")
    print("=" * 80)


def plot_ratios(rows: List[RowResult], outfile: str = "erdos181_ratio.png") -> None:
    ns = [r.n for r in rows]
    lbs = []
    for r in rows:
        lo, _ = ratio_bounds(r)
        lbs.append(lo if lo is not None else float("nan"))

    plt.figure(figsize=(8, 5))
    plt.plot(ns, lbs, "bo-", label="LB/2^n (from SAT scan, partial)")
    plt.axhline(1.0, color="gray", linestyle="--", label="slope 1 if R ~ 2^n")
    plt.xlabel("n (hypercube dimension)")
    plt.ylabel("R(Q_n) / 2^n (proxy from available lower bounds)")
    plt.title("Erdős Problem 181 — empirical ratio (very incomplete data)")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(outfile, dpi=150)
    print(f"Wrote plot {outfile}")


# --- Main demo --------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("References")[0].strip())
    parser.add_argument("--n-max", type=int, default=3, help="Max hypercube dimension")
    parser.add_argument("--sat-N-max", type=int, default=9, help="Max N for SAT scan")
    parser.add_argument(
        "--fix-initial",
        type=int,
        default=0,
        help="Symmetry break: fix f(i)=i for i<k (speed only; UNSAT is unsound as UB unless k=0)",
    )
    parser.add_argument("--no-plot", action="store_true")
    parser.add_argument("--plot-file", default="erdos181_ratio.png")
    args = parser.parse_args()

    print("Erdős Problem 181 — hypercube diagonal Ramsey (computational demo)\n")

    for n in range(1, args.n_max + 1):
        print(describe_hypercube(n))
        print(f"  |E(Q_{n})| = {len(hypercube_edges(n))}")

    if not HAS_PYSAT:
        print("\n[warn] python-sat not installed; SAT lower-bound block skipped.")
        print("       pip install -r examples/requirements-erdos181.txt\n")

    rows: List[RowResult] = []

    for n in range(1, args.n_max + 1):
        row = RowResult(n=n)
        emb_total = count_embeddings(n, args.sat_N_max, fix_initial=args.fix_initial)
        row.note = (
            f"Embeddings at N={args.sat_N_max} (fix_initial={args.fix_initial}): ~{emb_total}. "
            "Full enumeration may be intractable for larger n."
        )

        # Layered construction (cheap)
        lay_N, lay_parts = best_layered_lower_bound(n, min(args.sat_N_max + 5, 40))
        row.layered_N = lay_N if lay_N > 0 else None
        row.layered_parts = lay_parts

        # SAT scan
        if HAS_PYSAT:
            best_sat = None
            for N in range(2**n, args.sat_N_max + 1):
                c = count_embeddings(n, N, fix_initial=args.fix_initial)
                # Skip if clearly too big (clause count = 2c)
                if c > 200_000:
                    row.note += f" | skipped N≥{N} (>{200_000} embeddings)."
                    break
                try:
                    sat, dt, _ = sat_coloring_exists(
                        n, N, fix_initial=args.fix_initial, verbose=False
                    )
                except RuntimeError as e:
                    row.note += f" | SAT error at N={N}: {e}"
                    break
                print(f"  n={n} N={N}  SAT={sat}  ({dt:.2f}s)")
                if sat:
                    best_sat = N
            row.lower_sat = best_sat

        # Exhaustive upper bound: only microscopic
        if n == 1:
            # Q_1 = K_2; R is small — brute tiny N
            row.upper_exhaustive = None
        elif n == 2 and args.sat_N_max >= 4:
            # Optional: brute N=4..6 if M small
            for N in range(4, 7):
                M = num_edges_complete(N)
                if M > 18:
                    continue
                t0 = time.perf_counter()
                ok = exhaustive_no_monochromatic_qn(n, N)
                dt = time.perf_counter() - t0
                print(f"  [brute] n={n} N={N} exists_good_coloring={ok} ({dt:.3f}s)")
                if not ok:
                    row.upper_exhaustive = N
                    break

        rows.append(row)

    print_table(rows)

    if not args.no_plot:
        try:
            plot_ratios(rows, args.plot_file)
        except Exception as e:
            print(f"Plot failed: {e}")

    print(
        "\nDisclaimer: Values are from restricted search / small N. "
        "They are NOT claimed as best known mathematics for R(Q_n)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
