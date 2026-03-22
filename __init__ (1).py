from __future__ import annotations

import sqlite3

from lca_app.db import q, upsert_flow, upsert_process, add_exchange, execmany


def bootstrap_if_empty(conn: sqlite3.Connection) -> None:
    if q(conn, "SELECT COUNT(*) AS n FROM flows")[0]["n"] > 0:
        return

    f_methanol = "prod_methanol_crude_kg"
    f_electricity = "prod_electricity_kwh"
    f_ng = "prod_natural_gas_mj"

    f_co2 = "elem_co2_fossil_air_kg"
    f_ch4 = "elem_ch4_air_kg"
    f_n2o = "elem_n2o_air_kg"

    upsert_flow(conn, id=f_methanol, name="Methanol, crude", flow_type="product", unit="kg", compartment="technosphere")
    upsert_flow(conn, id=f_electricity, name="Electricity, medium voltage", flow_type="product", unit="kWh", compartment="technosphere")
    upsert_flow(conn, id=f_ng, name="Natural gas, energy", flow_type="product", unit="MJ", compartment="technosphere")

    upsert_flow(conn, id=f_co2, name="Carbon dioxide, fossil to air", flow_type="elementary", unit="kg", compartment="air")
    upsert_flow(conn, id=f_ch4, name="Methane to air", flow_type="elementary", unit="kg", compartment="air")
    upsert_flow(conn, id=f_n2o, name="Nitrous oxide to air", flow_type="elementary", unit="kg", compartment="air")

    p_grid = "proc_electricity_grid"
    upsert_process(
        conn,
        id=p_grid,
        name="Electricity, grid average (starter)",
        reference_flow_id=f_electricity,
        reference_amount=1.0,
        scenario="attributional",
        stage="production",
        data_source="starter",
    )
    add_exchange(conn, process_id=p_grid, flow_id=f_electricity, exchange_type="output", amount=1.0, is_reference=True)
    add_exchange(conn, process_id=p_grid, flow_id=f_co2, exchange_type="output", amount=0.6, notes="kg CO2 per kWh (starter placeholder)")
    add_exchange(conn, process_id=p_grid, flow_id=f_ch4, exchange_type="output", amount=0.00002)
    add_exchange(conn, process_id=p_grid, flow_id=f_n2o, exchange_type="output", amount=0.00001)

    p_ng = "proc_natural_gas_supply"
    upsert_process(
        conn,
        id=p_ng,
        name="Natural gas supply (starter)",
        reference_flow_id=f_ng,
        reference_amount=1.0,
        scenario="attributional",
        stage="production",
        data_source="starter",
    )
    add_exchange(conn, process_id=p_ng, flow_id=f_ng, exchange_type="output", amount=1.0, is_reference=True)
    add_exchange(conn, process_id=p_ng, flow_id=f_ch4, exchange_type="output", amount=0.0005, notes="kg CH4 per MJ (starter placeholder)")
    add_exchange(conn, process_id=p_ng, flow_id=f_co2, exchange_type="output", amount=0.05, notes="kg CO2 per MJ (starter placeholder)")

    p_meoh = "proc_methanol_grey_starter"
    upsert_process(
        conn,
        id=p_meoh,
        name="Methanol synthesis (grey, starter)",
        reference_flow_id=f_methanol,
        reference_amount=1.0,
        scenario="attributional",
        stage="production",
        data_source="starter",
        metadata={"note": "Replace with real LCI; this is a teaching template."},
    )
    add_exchange(conn, process_id=p_meoh, flow_id=f_methanol, exchange_type="output", amount=1.0, is_reference=True)
    add_exchange(conn, process_id=p_meoh, flow_id=f_ng, exchange_type="input", amount=20.0, notes="MJ per kg methanol (starter placeholder)")
    add_exchange(conn, process_id=p_meoh, flow_id=f_electricity, exchange_type="input", amount=0.2, notes="kWh per kg methanol (starter placeholder)")
    add_exchange(conn, process_id=p_meoh, flow_id=f_co2, exchange_type="output", amount=0.56, notes="kg CO2 per kg methanol (starter placeholder)")

    methods = [
        ("ipcc_ar6_gwp100", "IPCC AR6 GWP 100y", "IPCC", "AR6", "n/a", 0, "EPA dataset / LCIAformatter", "open", "catalog"),
        ("ipcc_ar6_gwp20", "IPCC AR6 GWP 20y", "IPCC", "AR6", "n/a", 0, "EPA dataset / LCIAformatter", "open", "catalog"),
        ("ipcc_ar6_gwp500", "IPCC AR6 GWP 500y", "IPCC", "AR6", "n/a", 0, "EPA dataset / LCIAformatter", "open", "catalog"),
        ("ipcc_ar5_gwp100", "IPCC AR5 GWP 100y", "IPCC", "AR5", "n/a", 0, "EPA dataset / LCIAformatter", "open", "catalog"),
        ("ipcc_ar4_gwp100", "IPCC AR4 GWP 100y", "IPCC", "AR4", "n/a", 0, "EPA dataset / LCIAformatter", "open", "catalog"),
        ("traci_2_2", "TRACI 2.2 Midpoint", "TRACI", "2.2", "US", 0, "US EPA", "open", "catalog"),
        ("ilcd_2011_midpoint", "ILCD 2011 Midpoint+", "ILCD", "2011", "EU", 0, "JRC", "open-ish", "catalog"),
        ("ef_3_1", "EF 3.1 Midpoint", "EF", "3.1", "EU", 0, "JRC", "open-ish", "catalog"),
        ("cml_ia_baseline", "CML-IA Baseline", "CML", "baseline", "n/a", 0, "CML", "varies", "catalog"),
        ("cml_ia_nonbaseline", "CML-IA Non-baseline", "CML", "non-baseline", "n/a", 0, "CML", "varies", "catalog"),
        ("usetox_2_1", "USEtox 2.1", "USEtox", "2.1", "global", 0, "USEtox", "varies", "catalog"),
        ("aware_1_2", "AWARE 1.2 Water Scarcity", "AWARE", "1.2", "global", 0, "WULCA", "varies", "catalog"),
        ("ced_v1", "Cumulative Energy Demand", "CED", "v1", "n/a", 0, "method devs", "varies", "catalog"),
        ("impact_world_plus", "IMPACT World+ (midpoint)", "IMPACT World+", "latest", "global", 0, "method devs", "varies", "catalog"),
        ("recipe_2016_mid_h", "ReCiPe 2016 Midpoint (H)", "ReCiPe", "2016", "H", 0, "PRé/RIVM", "not open", "catalog"),
        ("recipe_2016_end_h", "ReCiPe 2016 Endpoint (H)", "ReCiPe", "2016", "H", 1, "PRé/RIVM", "not open", "catalog"),
        ("iso_14067_ipcc", "ISO 14067 / IPCC 2021", "IPCC", "2021", "n/a", 0, "openLCA adaptation", "varies", "catalog"),
        ("odps_2022", "Ozone Depletion (WMO 2022 ODPs)", "ODP", "2022", "n/a", 0, "WMO/Researchers", "open-ish", "catalog"),
        ("reed_endpoint", "Endpoint weights (custom)", "Endpoint", "custom", "n/a", 1, "user", "user", "catalog"),
        ("custom_method", "Custom LCIA Method (CSV import)", "Custom", "n/a", "n/a", 0, "user", "user", "catalog"),
    ]

    execmany(
        conn,
        """
        INSERT INTO lcia_methods(id,name,family,version,perspective,endpoint,source,license,status)
        VALUES(?,?,?,?,?,?,?,?,?)
        """,
        [(a, b, c, d, e, f, g, h, i) for (a, b, c, d, e, f, g, h, i) in methods],
    )
