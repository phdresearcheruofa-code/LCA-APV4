from __future__ import annotations

import streamlit as st
import sqlite3

from lca_app.db import q


def render_header() -> None:
    st.title("🌍 Open & Extensible LCA App")
    st.caption("Database-driven • Importable LCIA methods • Attributional/Consequential • CTG/CTG+Use/EoL")


def sidebar_status(conn: sqlite3.Connection, cfg) -> None:
    st.subheader("Status")
    flows = q(conn, "SELECT COUNT(*) AS n FROM flows")[0]["n"]
    procs = q(conn, "SELECT COUNT(*) AS n FROM processes")[0]["n"]
    exch = q(conn, "SELECT COUNT(*) AS n FROM exchanges")[0]["n"]
    methods = q(conn, "SELECT COUNT(*) AS n FROM lcia_methods")[0]["n"]
    loaded = q(conn, "SELECT COUNT(*) AS n FROM lcia_methods WHERE status='loaded'")[0]["n"]
    naics = q(conn, "SELECT COUNT(*) AS n FROM naics_factors")[0]["n"]

    st.write(f"**Flows:** {flows}")
    st.write(f"**Processes:** {procs}")
    st.write(f"**Exchanges:** {exch}")
    st.write(f"**LCIA methods (catalog):** {methods} | **loaded:** {loaded}")
    st.write(f"**NAICS screening rows:** {naics}")
    st.write("---")
    st.write("DB:", cfg.database_url)
