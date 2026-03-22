from __future__ import annotations

import sqlite3

import pandas as pd

from lca_app.utils import try_download


NAICS_V1_2_CO2E_URL = "https://pasteur.epa.gov/uploads/10.23719/1528686/SupplyChainGHGEmissionFactors_v1.2_NAICS_CO2e_USD2021.csv"
NAICS_V1_3_CO2E_URL = "https://pasteur.epa.gov/uploads/10.23719/1531143/SupplyChainGHGEmissionFactors_v1.3.0_NAICS_CO2e_USD2022.csv"


def import_naics_factors(conn: sqlite3.Connection, cache_dir: str, *, url: str = NAICS_V1_3_CO2E_URL) -> dict[str, int]:
    fp = try_download([url, NAICS_V1_2_CO2E_URL], cache_dir)
    if fp is None:
        raise RuntimeError("Could not download NAICS factors CSV.")

    df = pd.read_csv(fp)
    cols = {c: str(c).strip().lower() for c in df.columns}
    df = df.rename(columns=cols)

    code_col = next((c for c in df.columns if "naics" in c and "code" in c), None)
    title_col = next((c for c in df.columns if "title" in c), None)
    factor_col = next((c for c in df.columns if "supply chain" in c and "factor" in c and "margin" not in c), None)
    unit_col = next((c for c in df.columns if c == "unit"), None)

    if not all([code_col, title_col, factor_col]):
        raise RuntimeError("NAICS CSV did not contain expected columns.")

    year = 2022 if "2022" in url else 2021 if "2021" in url else None

    cur = conn.cursor()
    inserted = 0
    for _, r in df.iterrows():
        naics = str(r[code_col]).strip().strip('"')
        title = str(r[title_col]).strip().strip('"')
        try:
            factor = float(r[factor_col])
        except Exception:
            continue
        unit = str(r[unit_col]).strip() if unit_col else "kg CO2e / USD"
        cur.execute(
            """
            INSERT INTO naics_factors(naics,title,unit,year,factor,source)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(naics) DO UPDATE SET
              title=excluded.title,
              unit=excluded.unit,
              year=excluded.year,
              factor=excluded.factor,
              source=excluded.source
            """,
            (naics, title, unit, year, factor, url),
        )
        inserted += 1
    conn.commit()
    return {"rows": inserted}
