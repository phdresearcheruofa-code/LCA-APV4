from __future__ import annotations

import io
import zipfile
import pandas as pd
import streamlit as st

from lca_app.config import AppConfig
from lca_app.db import get_conn, q, execmany
from lca_app.importers.ipcc import import_ipcc_gwp
from lca_app.importers.traci import import_traci_2_2
from lca_app.importers.naics import import_naics_factors

st.set_page_config(page_title="Import", layout="wide")
cfg = AppConfig.from_env()
conn = get_conn(cfg.database_url)

st.title("⬇️ Import open datasets + bring your own")

colA, colB = st.columns(2)

with colA:
    st.subheader("Open datasets (one-click)")
    if st.button("Import IPCC AR4/AR5/AR6 GWPs"):
        with st.spinner("Downloading + importing IPCC dataset..."):
            stats = import_ipcc_gwp(conn, cfg.cache_dir)
        st.success(f"Imported IPCC factors: {stats}")

    if st.button("Import TRACI 2.2"):
        with st.spinner("Downloading + importing TRACI 2.2..."):
            stats = import_traci_2_2(conn, cfg.cache_dir)
        st.success(f"Imported TRACI factors: {stats}")

    if st.button("Import NAICS spend-based GHG factors"):
        with st.spinner("Downloading + importing NAICS factors..."):
            stats = import_naics_factors(conn, cfg.cache_dir)
        st.success(f"Imported NAICS factors: {stats}")

with colB:
    st.subheader("Bring your own DB (ZIP of CSV tables)")
    st.write("Export from this app, edit, then re-import to update everything.")

    up = st.file_uploader("Upload DB ZIP (csv tables)", type=["zip"])
    if up is not None:
        data = up.getvalue()
        with zipfile.ZipFile(io.BytesIO(data), "r") as z:
            names = z.namelist()
            st.write("Found files:", names)

            if st.button("Import ZIP into DB (overwrite)"):
                for name in names:
                    if not name.endswith(".csv"):
                        continue
                    table = name.replace(".csv", "")
                    df = pd.read_csv(z.open(name))
                    if table not in {"flows", "processes", "exchanges", "lcia_methods", "lcia_categories", "lcia_factors", "naics_factors"}:
                        continue

                    cur = conn.cursor()
                    cur.execute(f"DELETE FROM {table}")
                    conn.commit()

                    cols = list(df.columns)
                    placeholders = ",".join(["?"] * len(cols))
                    sql = f"INSERT INTO {table}({','.join(cols)}) VALUES({placeholders})"
                    rows = [tuple(None if pd.isna(v) else v for v in r) for r in df.to_numpy()]
                    execmany(conn, sql, rows)

                st.success("Imported ZIP.")
                st.rerun()

st.write("---")
st.subheader("Export current DB as ZIP")
if st.button("Export ZIP"):
    tables = ["flows", "processes", "exchanges", "lcia_methods", "lcia_categories", "lcia_factors", "naics_factors"]
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for t in tables:
            df = pd.DataFrame([dict(r) for r in q(conn, f"SELECT * FROM {t}")])
            z.writestr(f"{t}.csv", df.to_csv(index=False))
    st.download_button("Download DB ZIP", data=mem.getvalue(), file_name="lca_db_export.zip", mime="application/zip")
