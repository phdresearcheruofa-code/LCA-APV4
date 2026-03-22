from __future__ import annotations

import pandas as pd
import streamlit as st

from lca_app.config import AppConfig
from lca_app.db import get_conn, q

st.set_page_config(page_title="Methods", layout="wide")
cfg = AppConfig.from_env()
conn = get_conn(cfg.database_url)

st.title("📚 LCIA methods catalog (≈20)")

m = pd.DataFrame([dict(r) for r in q(conn, "SELECT * FROM lcia_methods ORDER BY status DESC, family, name")])
st.dataframe(m, use_container_width=True, height=420)

st.write("---")
st.subheader("Loaded categories & factor counts")
rows = q(
    conn,
    """
    SELECT lm.name AS method, lm.id AS method_id, COUNT(DISTINCT lc.id) AS categories, COUNT(lf.id) AS factors
    FROM lcia_methods lm
    LEFT JOIN lcia_categories lc ON lc.method_id = lm.id
    LEFT JOIN lcia_factors lf ON lf.category_id = lc.id
    GROUP BY lm.id
    ORDER BY factors DESC
    """
)
st.dataframe(pd.DataFrame([dict(r) for r in rows]), use_container_width=True, height=420)
