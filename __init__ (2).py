from __future__ import annotations

import uuid
import sqlite3

import pandas as pd

from lca_app.db import q, upsert_flow
from lca_app.utils import try_download


TRACI_XLSX_URL = "https://www.epa.gov/system/files/documents/2024-01/traci_2_2.xlsx"


def _mkid(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def import_traci_2_2(conn: sqlite3.Connection, cache_dir: str) -> dict[str, int]:
    fp = try_download([TRACI_XLSX_URL], cache_dir)
    if fp is None:
        raise RuntimeError("Could not download TRACI 2.2 xlsx.")

    xls = pd.ExcelFile(fp)
    imported = 0

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

    method_id = "traci_2_2"
    if not q(conn, "SELECT 1 FROM lcia_methods WHERE id=?", (method_id,)):
        raise RuntimeError("TRACI method not present in catalog (bootstrap missing).")

    for sh in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sh)
        if df.shape[0] < 3 or df.shape[1] < 2:
            continue

        cols = {c: str(c).strip().lower() for c in df.columns}
        df = df.rename(columns=cols)

        name_col = next((c for c in df.columns if "substance" in c or "chemical" in c or "name" in c), None)
        cf_col = next((c for c in df.columns if c in ("cf", "factor") or "character" in c or "value" in c), None)
        if name_col is None or cf_col is None:
            continue

        unit = "TRACI unit"
        sh_l = sh.lower()
        if "global warming" in sh_l or "gwp" in sh_l:
            unit = "kg CO2-eq"
        elif "acid" in sh_l:
            unit = "mol H+-eq"
        elif "eutroph" in sh_l:
            unit = "kg N-eq"
        elif "smog" in sh_l or "ozone" in sh_l:
            unit = "kg O3-eq"
        elif "cancer" in sh_l or "non-cancer" in sh_l:
            unit = "CTUh"
        elif "eco" in sh_l:
            unit = "CTUe"

        cat_id = ensure_category(method_id, sh.strip(), unit)

        sub = df[[name_col, cf_col]].dropna()
        for _, r in sub.iterrows():
            name = str(r[name_col]).strip()
            try:
                cf = float(r[cf_col])
            except Exception:
                continue

            flow_id = "elem_" + name.lower().replace(" ", "_").replace("/", "_").replace("-", "_")[:80]
            upsert_flow(conn, id=flow_id, name=name, flow_type="elementary", unit="kg", compartment="air")

            cur = conn.cursor()
            cur.execute(
                "INSERT INTO lcia_factors(category_id,flow_id,cf,cf_unit,notes) VALUES(?,?,?,?,?)",
                (cat_id, flow_id, cf, unit, "Imported from TRACI 2.2 xlsx (sheet heuristic)"),
            )
            imported += 1

    if imported == 0:
        raise RuntimeError("Parsed TRACI file but imported 0 factors (sheet/column patterns did not match).")

    cur = conn.cursor()
    cur.execute("UPDATE lcia_methods SET status='loaded' WHERE id=?", (method_id,))
    conn.commit()

    return {"factors": imported}
