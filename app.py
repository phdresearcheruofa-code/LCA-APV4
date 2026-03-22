from __future__ import annotations

import pandas as pd
import streamlit as st

from lca_app.config import AppConfig
from lca_app.db import get_conn, q

st.set_page_config(page_title="NAICS Screening", layout="wide")
cfg = AppConfig.from_env()
conn = get_conn(cfg.database_url)

st.title("🏭 Screening LCA (all industries) — NAICS spend-based GHG")

rows = q(conn, "SELECT * FROM naics_factors ORDER BY naics")
if not rows:
    st.warning("No NAICS factors loaded. Go to Import page and import NAICS factors.")
    st.stop()

df = pd.DataFrame([dict(r) for r in rows])

search = st.text_input("Search NAICS title/code", value="")
if search.strip():
    mask = df["title"].str.contains(search, case=False, na=False) | df["naics"].astype(str).str.contains(search, case=False, na=False)
    df_view = df[mask].copy()
else:
    df_view = df

st.dataframe(df_view[["naics", "title", "factor", "unit", "year"]], use_container_width=True, height=420)

st.write("### Estimate")
naics = st.selectbox("NAICS", df_view["naics"].tolist())
spend = st.number_input("Spend (USD, same basis as dataset unit)", value=10000.0)

row = df[df["naics"] == naics].iloc[0]
factor = float(row["factor"])
unit = row["unit"]
year = row["year"]

result = spend * factor
st.metric("Estimated supply-chain GHG", f"{result:,.2f} kg CO2e", help=f"Factor: {factor} ({unit}), year={year}")
