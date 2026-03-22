from __future__ import annotations

import uuid
import sqlite3

import pandas as pd

from lca_app.db import q, upsert_flow
from lca_app.utils import try_download


IPCC_XLSX_URL = "https://pasteur.epa.gov/uploads/10.23719/1529821/IPCC_AR4-AR6_GWPs.xlsx"
IPCC_PARQUET_URL = "https://dmap-data-commons-ord.s3.amazonaws.com/lciafmt/ipcc/IPCC_v1.1.1_27ba917.parquet"


def _mkid(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def import_ipcc_gwp(conn: sqlite3.Connection, cache_dir: str) -> dict[str, int]:
    """
    Imports IPCC AR4/AR5/AR6 GWPs (20/100/500) if present in source file.
    Creates flows as needed and stores CFs for categories in catalog methods.
    """
    fp = try_download([IPCC_XLSX_URL, IPCC_PARQUET_URL], cache_dir)
    if fp is None:
        raise RuntimeError("Could not download IPCC dataset from known sources.")

    if fp.endswith(".parquet"):
        df = pd.read_parquet(fp)
    else:
        xls = pd.ExcelFile(fp)
        frames = []
        for sh in xls.sheet_names:
            t = pd.read_excel(xls, sheet_name=sh)
            if t.shape[1] < 3:
                continue
            frames.append(t)
        if not frames:
            raise RuntimeError("IPCC XLSX contained no readable tables.")
        df = pd.concat(frames, ignore_index=True)

    cols = {c: str(c).strip().lower() for c in df.columns}
    df = df.rename(columns=cols)

    name_col = next((c for c in df.columns if "name" in c or "ghg" in c or "gas" in c), None)
    if name_col is None:
        raise RuntimeError("Could not identify gas name column in IPCC dataset.")

    numeric_cols = [c for c in df.columns if c != name_col]
    df = df[[name_col] + numeric_cols].copy()
    df[name_col] = df[name_col].astype(str).str.strip()

    imported = 0

    method_map = {"ar4": "ipcc_ar4", "ar5": "ipcc_ar5", "ar6": "ipcc_ar6"}

    def ensure_category(method_id: str, cat_name: str, unit: str) -> str:
        existing = q(conn, "SELECT id FROM lcia_categories WHERE method_id=? AND name=?", (method_id, cat_name))
        if existing:
            return existing[0]["id"]
        cid = _mkid("cat")
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO lcia_categories(id,method_id,name,unit,direction,description) VALUES(?,?,?,?,?,?)",
            (cid, method_id, cat_name, unit, "higher_worse", ""),
        )
        conn.commit()
        return cid

    def mark_loaded(method_id: str) -> None:
        cur = conn.cursor()
        cur.execute("UPDATE lcia_methods SET status='loaded' WHERE id=?", (method_id,))
        conn.commit()

    for col in numeric_cols:
        c = str(col).lower()
        report = next((r for r in method_map if r in c), None)
        horizon = next((h for h in ("20", "100", "500") if f"gwp{h}" in c or f"gwp-{h}" in c), None)
        if report is None or horizon is None:
            continue

        method_id = f"{method_map[report]}_gwp{horizon}"
        if not q(conn, "SELECT 1 FROM lcia_methods WHERE id=?", (method_id,)):
            continue

        cat_id = ensure_category(method_id, f"Climate change (GWP{horizon})", "kg CO2-eq")

        sub = df[[name_col, col]].dropna()
        for _, row in sub.iterrows():
            gas = str(row[name_col]).strip()
            try:
                cf = float(row[col])
            except Exception:
                continue

            flow_id = "elem_" + gas.lower().replace(" ", "_").replace("/", "_").replace("-", "_")
            upsert_flow(conn, id=flow_id, name=gas, flow_type="elementary", unit="kg", compartment="air")

            cur = conn.cursor()
            cur.execute(
                "INSERT INTO lcia_factors(category_id,flow_id,cf,cf_unit,notes) VALUES(?,?,?,?,?)",
                (cat_id, flow_id, cf, "kg CO2-eq / kg", "Imported from EPA IPCC dataset"),
            )
            imported += 1

        mark_loaded(method_id)

    if imported == 0:
        raise RuntimeError("Parsed IPCC file but imported 0 factors (column patterns did not match).")

    return {"factors": imported}
