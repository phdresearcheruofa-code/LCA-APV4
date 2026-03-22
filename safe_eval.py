from __future__ import annotations

import json
import sqlite3
from typing import Any, Iterable, Optional
from urllib.parse import urlparse


def get_conn(database_url: str) -> sqlite3.Connection:
    """
    Supports:
      - sqlite:///path/to/file.sqlite
      - sqlite:///:memory:
      - plain file paths (treated as sqlite file)
    """
    if "://" not in database_url:
        path = database_url
        conn = sqlite3.connect(path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    u = urlparse(database_url)
    if u.scheme != "sqlite":
        raise ValueError("Only sqlite is supported in this starter repo. Use sqlite:///file.sqlite")

    path = (u.path or "").lstrip("/")
    if path == ":memory:":
        conn = sqlite3.connect(":memory:", check_same_thread=False)
    else:
        conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS flows (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            flow_type TEXT NOT NULL CHECK(flow_type IN ('product','elementary')),
            unit TEXT NOT NULL,
            compartment TEXT,
            synonyms_json TEXT DEFAULT '[]'
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS processes (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            location TEXT,
            scenario TEXT DEFAULT 'attributional',
            stage TEXT DEFAULT 'production',
            reference_flow_id TEXT NOT NULL,
            reference_amount REAL NOT NULL DEFAULT 1.0,
            data_source TEXT,
            metadata_json TEXT DEFAULT '{}'
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS exchanges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            process_id TEXT NOT NULL,
            flow_id TEXT NOT NULL,
            exchange_type TEXT NOT NULL CHECK(exchange_type IN ('input','output')),
            amount REAL,
            amount_expr TEXT,
            is_reference INTEGER NOT NULL DEFAULT 0,
            notes TEXT,
            FOREIGN KEY(process_id) REFERENCES processes(id),
            FOREIGN KEY(flow_id) REFERENCES flows(id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS lcia_methods (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            family TEXT,
            version TEXT,
            perspective TEXT,
            endpoint INTEGER NOT NULL DEFAULT 0,
            source TEXT,
            license TEXT,
            status TEXT DEFAULT 'catalog'
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS lcia_categories (
            id TEXT PRIMARY KEY,
            method_id TEXT NOT NULL,
            name TEXT NOT NULL,
            unit TEXT NOT NULL,
            direction TEXT DEFAULT 'higher_worse',
            description TEXT,
            FOREIGN KEY(method_id) REFERENCES lcia_methods(id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS lcia_factors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id TEXT NOT NULL,
            flow_id TEXT NOT NULL,
            cf REAL NOT NULL,
            cf_unit TEXT,
            notes TEXT,
            FOREIGN KEY(category_id) REFERENCES lcia_categories(id),
            FOREIGN KEY(flow_id) REFERENCES flows(id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS naics_factors (
            naics TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            unit TEXT NOT NULL,
            year INTEGER,
            factor REAL NOT NULL,
            source TEXT
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )

    conn.commit()


def q(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchall()


def execmany(conn: sqlite3.Connection, sql: str, rows: Iterable[tuple[Any, ...]]) -> None:
    cur = conn.cursor()
    cur.executemany(sql, list(rows))
    conn.commit()


def set_setting(conn: sqlite3.Connection, key: str, value: Any) -> None:
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, json.dumps(value)),
    )
    conn.commit()


def get_setting(conn: sqlite3.Connection, key: str, default: Any = None) -> Any:
    rows = q(conn, "SELECT value FROM settings WHERE key=?", (key,))
    if not rows:
        return default
    try:
        return json.loads(rows[0]["value"])
    except Exception:
        return default


def upsert_flow(
    conn: sqlite3.Connection,
    *,
    id: str,
    name: str,
    flow_type: str,
    unit: str,
    compartment: Optional[str] = None,
    synonyms: Optional[list[str]] = None,
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO flows(id,name,flow_type,unit,compartment,synonyms_json)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(id) DO UPDATE SET
          name=excluded.name,
          flow_type=excluded.flow_type,
          unit=excluded.unit,
          compartment=excluded.compartment,
          synonyms_json=excluded.synonyms_json
        """
        ,
        (id, name, flow_type, unit, compartment, json.dumps(synonyms or [])),
    )
    conn.commit()


def upsert_process(
    conn: sqlite3.Connection,
    *,
    id: str,
    name: str,
    reference_flow_id: str,
    reference_amount: float = 1.0,
    description: str = "",
    location: str = "",
    scenario: str = "attributional",
    stage: str = "production",
    data_source: str = "",
    metadata: Optional[dict[str, Any]] = None,
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO processes(
            id,name,description,location,scenario,stage,
            reference_flow_id,reference_amount,data_source,metadata_json
        )
        VALUES(?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name,
            description=excluded.description,
            location=excluded.location,
            scenario=excluded.scenario,
            stage=excluded.stage,
            reference_flow_id=excluded.reference_flow_id,
            reference_amount=excluded.reference_amount,
            data_source=excluded.data_source,
            metadata_json=excluded.metadata_json
        """
        ,
        (
            id,
            name,
            description,
            location,
            scenario,
            stage,
            reference_flow_id,
            float(reference_amount),
            data_source,
            json.dumps(metadata or {}),
        ),
    )
    conn.commit()


def add_exchange(
    conn: sqlite3.Connection,
    *,
    process_id: str,
    flow_id: str,
    exchange_type: str,
    amount: Optional[float] = None,
    amount_expr: str = "",
    is_reference: bool = False,
    notes: str = "",
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO exchanges(process_id,flow_id,exchange_type,amount,amount_expr,is_reference,notes)
        VALUES(?,?,?,?,?,?,?)
        """
        ,
        (process_id, flow_id, exchange_type, amount, amount_expr or None, 1 if is_reference else 0, notes),
    )
    conn.commit()
