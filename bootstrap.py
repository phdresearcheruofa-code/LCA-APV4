from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import sqlite3

from lca_app.db import q
from lca_app.safe_eval import safe_eval


@dataclass(frozen=True)
class RunOptions:
    scenario: str
    stages: tuple[str, ...]
    allocation_mode: str  # none|mass|economic|manual
    allocation_factor: float  # only used for manual
    substitution_map: dict[str, str]  # co_product_flow_id -> substitute_process_id
    parameters: dict[str, float]


@dataclass(frozen=True)
class LciResult:
    activity_by_process: dict[str, float]
    inventory_by_flow: dict[str, float]


@dataclass(frozen=True)
class LciaCategoryResult:
    category_id: str
    category_name: str
    unit: str
    value: float


@dataclass(frozen=True)
class LciaResult:
    method_id: str
    method_name: str
    categories: list[LciaCategoryResult]


def _get_processes(conn: sqlite3.Connection, scenario: str, stages: tuple[str, ...]) -> list[sqlite3.Row]:
    placeholders = ",".join(["?"] * len(stages))
    return q(
        conn,
        f"""
        SELECT * FROM processes
        WHERE scenario=? AND stage IN ({placeholders})
        """,
        (scenario, *stages),
    )


def _get_flows(conn: sqlite3.Connection) -> dict[str, sqlite3.Row]:
    rows = q(conn, "SELECT * FROM flows")
    return {r["id"]: r for r in rows}


def _get_exchanges(conn: sqlite3.Connection, process_ids: Iterable[str]) -> list[sqlite3.Row]:
    p = list(process_ids)
    if not p:
        return []
    placeholders = ",".join(["?"] * len(p))
    return q(conn, f"SELECT * FROM exchanges WHERE process_id IN ({placeholders})", tuple(p))


def _eval_amount(row: sqlite3.Row, params: dict[str, float]) -> float:
    if row["amount"] is not None:
        return float(row["amount"])
    expr = row["amount_expr"]
    if not expr:
        raise ValueError("Exchange has neither amount nor amount_expr.")
    return float(safe_eval(expr, params))


def _allocation_multiplier(mode: str, manual_factor: float) -> float:
    if mode == "none":
        return 1.0
    if mode == "manual":
        return float(manual_factor)
    if mode in ("mass", "economic"):
        return 1.0
    raise ValueError(f"Unknown allocation mode: {mode}")


def build_matrices(
    conn: sqlite3.Connection,
    options: RunOptions,
) -> tuple[np.ndarray, np.ndarray, list[str], list[str], list[str], dict[str, list[tuple[str, float]]]]:
    """
    Returns (A, B, product_flow_ids, process_ids, elementary_flow_ids, coproducts_by_process)
    """
    flows = _get_flows(conn)
    prows = _get_processes(conn, options.scenario, options.stages)
    if not prows:
        raise ValueError("No processes found for the selected scenario/stages.")

    process_ids = [p["id"] for p in prows]

    product_flow_ids = []
    producer_of: dict[str, str] = {}
    for p in prows:
        rf = p["reference_flow_id"]
        if rf not in flows:
            raise ValueError(f"Process '{p['name']}' references unknown flow id '{rf}'.")
        if flows[rf]["flow_type"] != "product":
            raise ValueError(f"Reference flow '{flows[rf]['name']}' must be flow_type='product'.")
        if rf in producer_of:
            raise ValueError(f"Multiple processes produce reference product '{flows[rf]['name']}'.")
        producer_of[rf] = p["id"]
        product_flow_ids.append(rf)

    exchanges = _get_exchanges(conn, process_ids)

    elementary_flow_ids = sorted(
        {
            e["flow_id"]
            for e in exchanges
            if flows.get(e["flow_id"]) is not None and flows[e["flow_id"]]["flow_type"] == "elementary"
        }
    )

    coproducts_by_process: dict[str, list[tuple[str, float]]] = {pid: [] for pid in process_ids}

    p_index = {pid: j for j, pid in enumerate(process_ids)}
    prod_index = {fid: i for i, fid in enumerate(product_flow_ids)}
    elem_index = {fid: i for i, fid in enumerate(elementary_flow_ids)}

    n = len(product_flow_ids)
    m = len(process_ids)
    A = np.zeros((n, m), dtype=float)
    B = np.zeros((len(elementary_flow_ids), m), dtype=float)

    alloc_mult = _allocation_multiplier(options.allocation_mode, options.allocation_factor)

    proc_by_id = {p["id"]: p for p in prows}

    for pid in process_ids:
        p = proc_by_id[pid]
        rf = p["reference_flow_id"]
        i = prod_index[rf]
        A[i, p_index[pid]] = 1.0

    for e in exchanges:
        pid = e["process_id"]
        p = proc_by_id[pid]
        denom = float(p["reference_amount"] or 1.0)

        amt = _eval_amount(e, options.parameters) / denom
        fid = e["flow_id"]
        f = flows.get(fid)
        if f is None:
            continue

        if f["flow_type"] == "product":
            if int(e["is_reference"]) == 1:
                continue
            if e["exchange_type"] == "input":
                if fid not in prod_index:
                    raise ValueError(
                        f"Missing producer process for required product '{f['name']}'. "
                        f"Add a process that produces it as reference, or include a background/supplier process."
                    )
                A[prod_index[fid], p_index[pid]] += -amt
            else:
                coproducts_by_process[pid].append((fid, amt))
        else:
            if fid not in elem_index:
                continue
            B[elem_index[fid], p_index[pid]] += float(amt) * float(alloc_mult)

    return A, B, product_flow_ids, process_ids, elementary_flow_ids, coproducts_by_process


def solve_lci(
    conn: sqlite3.Connection,
    *,
    functional_unit_flow_id: str,
    functional_unit_amount: float,
    options: RunOptions,
) -> LciResult:
    A, B, product_flows, process_ids, elem_flows, coproducts_by_process = build_matrices(conn, options)

    if functional_unit_flow_id not in product_flows:
        raise ValueError("Functional unit must be a reference product of a selected process (in this starter engine).")

    y = np.zeros((len(product_flows),), dtype=float)
    y[product_flows.index(functional_unit_flow_id)] = float(functional_unit_amount)

    try:
        x = np.linalg.solve(A, y)
    except np.linalg.LinAlgError as e:
        raise ValueError(
            "Technosphere matrix is not solvable. Ensure 1 reference product per process, "
            "and that all required inputs have producing processes in the selected system."
        ) from e

    inv = B @ x

    if options.substitution_map:
        memo: dict[tuple[str, float], np.ndarray] = {}

        def lci_for_product(flow_id: str, amount: float) -> np.ndarray:
            key = (flow_id, amount)
            if key in memo:
                return memo[key]
            y2 = np.zeros((len(product_flows),), dtype=float)
            y2[product_flows.index(flow_id)] = float(amount)
            x2 = np.linalg.solve(A, y2)
            inv2 = B @ x2
            memo[key] = inv2
            return inv2

        for pid, co_list in coproducts_by_process.items():
            j = process_ids.index(pid)
            act = float(x[j])
            for co_flow, co_amt_per_act in co_list:
                if co_flow not in options.substitution_map:
                    continue
                avoided = lci_for_product(co_flow, act * co_amt_per_act)
                inv = inv - avoided

    activity_by_process = {pid: float(x[i]) for i, pid in enumerate(process_ids)}
    inventory_by_flow = {fid: float(inv[i]) for i, fid in enumerate(elem_flows)}

    return LciResult(activity_by_process=activity_by_process, inventory_by_flow=inventory_by_flow)


def solve_lcia(conn: sqlite3.Connection, lci: LciResult, method_id: str) -> LciaResult:
    m = q(conn, "SELECT * FROM lcia_methods WHERE id=?", (method_id,))
    if not m:
        raise ValueError("Unknown LCIA method.")
    method = m[0]

    cats = q(conn, "SELECT * FROM lcia_categories WHERE method_id=? ORDER BY name", (method_id,))
    if not cats:
        raise ValueError("This method has no categories loaded.")

    inv = lci.inventory_by_flow

    out: list[LciaCategoryResult] = []
    for c in cats:
        factors = q(conn, "SELECT flow_id, cf FROM lcia_factors WHERE category_id=?", (c["id"],))
        total = 0.0
        for f in factors:
            total += float(inv.get(f["flow_id"], 0.0)) * float(f["cf"])
        out.append(
            LciaCategoryResult(
                category_id=c["id"],
                category_name=c["name"],
                unit=c["unit"],
                value=float(total),
            )
        )

    return LciaResult(method_id=method_id, method_name=method["name"], categories=out)
