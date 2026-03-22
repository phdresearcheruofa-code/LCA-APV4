from __future__ import annotations

import pandas as pd
import streamlit as st

from lca_app.config import AppConfig
from lca_app.db import get_conn, q, upsert_flow, upsert_process, add_exchange

st.set_page_config(page_title="Database", layout="wide")
cfg = AppConfig.from_env()
conn = get_conn(cfg.database_url)

st.title("🗄️ Database")
tab1, tab2, tab3 = st.tabs(["Flows", "Processes", "Exchanges"])

with tab1:
    st.subheader("Flows")
    df = pd.DataFrame([dict(r) for r in q(conn, "SELECT * FROM flows ORDER BY flow_type, name")])
    st.dataframe(df, use_container_width=True, height=420)

    with st.expander("Add / Update flow"):
        c1, c2, c3 = st.columns(3)
        with c1:
            fid = st.text_input("id", value="")
            name = st.text_input("name", value="")
        with c2:
            flow_type = st.selectbox("flow_type", ["product", "elementary"])
            unit = st.text_input("unit", value="kg")
        with c3:
            compartment = st.text_input("compartment", value="")
            synonyms = st.text_input("synonyms (comma-separated)", value="")

        if st.button("Upsert flow"):
            if not fid or not name:
                st.error("id and name are required.")
            else:
                upsert_flow(
                    conn,
                    id=fid,
                    name=name,
                    flow_type=flow_type,
                    unit=unit,
                    compartment=compartment or None,
                    synonyms=[s.strip() for s in synonyms.split(",") if s.strip()],
                )
                st.success("Saved.")
                st.rerun()

with tab2:
    st.subheader("Processes")
    df = pd.DataFrame([dict(r) for r in q(conn, "SELECT * FROM processes ORDER BY scenario, stage, name")])
    st.dataframe(df, use_container_width=True, height=420)

    with st.expander("Add / Update process"):
        c1, c2, c3 = st.columns(3)
        with c1:
            pid = st.text_input("process id", value="")
            name = st.text_input("process name", value="")
        with c2:
            scenario = st.selectbox("scenario", ["attributional", "consequential"])
            stage = st.selectbox("stage", ["production", "use", "eol"])
        with c3:
            reference_flow_id = st.text_input("reference_flow_id (must be product)", value="")
            reference_amount = st.number_input("reference_amount", value=1.0)

        description = st.text_area("description", value="")
        location = st.text_input("location", value="")
        data_source = st.text_input("data_source", value="user")

        if st.button("Upsert process"):
            if not pid or not name or not reference_flow_id:
                st.error("process id, name, reference_flow_id are required.")
            else:
                upsert_process(
                    conn,
                    id=pid,
                    name=name,
                    reference_flow_id=reference_flow_id,
                    reference_amount=float(reference_amount),
                    description=description,
                    location=location,
                    scenario=scenario,
                    stage=stage,
                    data_source=data_source,
                )
                st.success("Saved.")
                st.rerun()

with tab3:
    st.subheader("Exchanges")
    df = pd.DataFrame([dict(r) for r in q(conn, "SELECT * FROM exchanges ORDER BY process_id, id")])
    st.dataframe(df, use_container_width=True, height=420)

    with st.expander("Add exchange"):
        process_id = st.text_input("process_id", value="")
        flow_id = st.text_input("flow_id", value="")
        exchange_type = st.selectbox("exchange_type", ["input", "output"])
        c1, c2, c3 = st.columns(3)
        with c1:
            amount = st.text_input("amount (numeric) OR leave blank to use expression", value="")
        with c2:
            amount_expr = st.text_input("amount_expr (e.g. 0.2 * kwh)", value="")
        with c3:
            is_reference = st.checkbox("is_reference", value=False)
        notes = st.text_input("notes", value="")

        if st.button("Insert exchange"):
            if not process_id or not flow_id:
                st.error("process_id and flow_id are required.")
            else:
                add_exchange(
                    conn,
                    process_id=process_id,
                    flow_id=flow_id,
                    exchange_type=exchange_type,
                    amount=float(amount) if amount.strip() else None,
                    amount_expr=amount_expr.strip(),
                    is_reference=is_reference,
                    notes=notes,
                )
                st.success("Inserted.")
                st.rerun()
