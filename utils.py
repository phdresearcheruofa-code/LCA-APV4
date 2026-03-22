from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AppConfig:
    database_url: str
    cache_dir: str

    @staticmethod
    def from_env() -> "AppConfig":
        return AppConfig(
            database_url=os.getenv("LCA_DATABASE_URL", "sqlite:///lca.sqlite"),
            cache_dir=os.getenv("LCA_CACHE_DIR", ".cache_lca"),
        )
