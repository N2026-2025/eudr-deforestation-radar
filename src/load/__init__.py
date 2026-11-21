"""Load layer: DuckDB Silver layer and PostGIS Gold layer for EUDR pipeline."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Optional

import duckdb
import geopandas as gpd
import pandas as pd
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class DuckDBLoader:
    """Load Silver layer data into DuckDB for analytical queries."""

    def __init__(self, db_path: Path | str = Path(".data/silver/eudr.duckdb")) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: duckdb.DuckDBPyConnection | None = None

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            self._conn = duckdb.connect(str(self.db_path))
            self._init_spatial()
        return self._conn

    def _init_spatial(self) -> None:
        """Initialize DuckDB spatial extension."""
        self.conn.execute("INSTALL spatial;")
        self.conn.execute("LOAD spatial;")

    def create_lots_table(self) -> None:
        """Create lots table with spatial index."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS lots (
                lot_id VARCHAR PRIMARY KEY,
                geometry GEOMETRY,
                country_of_production VARCHAR,
                commodity VARCHAR,
                production_date DATE,
                area_ha DOUBLE,
                has_post_cutoff_alert BOOLEAN,
                has_recent_fire BOOLEAN,
                eudr_compliant BOOLEAN,
                h3_cell VARCHAR,
                h3_cell_res8 VARCHAR,
                alert_count INTEGER,
                max_alert_intensity DOUBLE,
                fire_count INTEGER,
                max_frp DOUBLE,
                transformed_at TIMESTAMP,
                source_data JSON
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_lots_geometry ON lots USING RTREE (geometry)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_lots_h3 ON lots (h3_cell)
        """)

    def load_lots(self, gdf: gpd.GeoDataFrame, if_exists: str = "append") -> int:
        """Load lots GeoDataFrame into DuckDB."""
        if len(gdf) == 0:
            return 0

        # Ensure all required columns exist with defaults
        df = gdf.copy()
        if "geometry" in df.columns:
            df["geometry"] = df["geometry"].apply(
                lambda geom: geom.wkt if geom else None
            )

        if "production_date" in df.columns:
            df["production_date"] = pd.to_datetime(df["production_date"], errors="coerce").dt.date

        if "transformed_at" in df.columns:
            df["transformed_at"] = pd.to_datetime(df["transformed_at"], errors="coerce")

        if "source_data" not in df.columns:
            df["source_data"] = "{}"
        if "h3_cell_res8" not in df.columns:
            df["h3_cell_res8"] = None

        # Get actual table columns
        result = self.conn.execute("SELECT * FROM lots LIMIT 0")
        table_cols = [col[0] for col in result.description]

        # Build SELECT with only columns that exist in both
        common_cols = [c for c in table_cols if c in df.columns]
        select_clause = ", ".join(common_cols)
        placeholder_clause = ", ".join([f"?"] * len(common_cols))

        for _, row in df[common_cols].iterrows():
            # Build INSERT with ST_GeomFromWKT for geometry column
            placeholders = []
            values = []
            for col in common_cols:
                val = row[col]
                if col == "geometry":
                    placeholders.append("ST_GeomFromText(?)")
                else:
                    placeholders.append("?")
                values.append(val)

            insert_cols = ", ".join(common_cols)
            placeholder_clause = ", ".join(placeholders)
            self.conn.execute(
                f"INSERT INTO lots ({insert_cols}) VALUES ({placeholder_clause})",
                tuple(values),
            )

        return len(df)

    def query_deforestation_flags(
        self,
        country: Optional[str] = None,
        commodity: Optional[str] = None,
    ) -> pd.DataFrame:
        """Query lots with deforestation flags."""
        query = """
            SELECT
                lot_id,
                country_of_production,
                commodity,
                area_ha,
                has_post_cutoff_alert,
                has_recent_fire,
                eudr_compliant,
                alert_count,
                fire_count
            FROM lots
            WHERE 1=1
        """
        params = []
        if country:
            query += " AND country_of_production = ?"
            params.append(country)
        if commodity:
            query += " AND commodity = ?"
            params.append(commodity)

        return self.conn.execute(query, params).df()

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "DuckDBLoader":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


class PostGISLoader:
    """Load Gold layer data into PostGIS for serving API."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "eudr",
        user: str = "postgres",
        password: str = "",
    ) -> None:
        self.conn_params = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
        }
        self._conn: Any = None

    @property
    def conn(self) -> Any:
        if self._conn is None:
            import psycopg2
            self._conn = psycopg2.connect(**self.conn_params)
            self._init_schema()
        return self._conn

    def _init_schema(self) -> None:
        """Initialize PostGIS schema."""
        with self.conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lots_gold (
                    lot_id VARCHAR PRIMARY KEY,
                    geometry GEOMETRY(Geometry, 4326),
                    country_of_production CHAR(2),
                    commodity VARCHAR,
                    production_date DATE,
                    area_ha DOUBLE PRECISION,
                    has_post_cutoff_alert BOOLEAN,
                    has_recent_fire BOOLEAN,
                    eudr_compliant BOOLEAN,
                    h3_cell VARCHAR,
                    alert_count INTEGER,
                    max_alert_intensity DOUBLE PRECISION,
                    fire_count INTEGER,
                    max_frp DOUBLE PRECISION,
                    risk_score DOUBLE PRECISION DEFAULT 0,
                    risk_level VARCHAR,
                    dds_payload JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_lots_gold_geometry
                ON lots_gold USING GIST (geometry)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_lots_gold_h3 ON lots_gold (h3_cell)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_lots_gold_risk ON lots_gold (risk_level)
            """)
            self.conn.commit()

    def upsert_lots(self, gdf: gpd.GeoDataFrame) -> int:
        """Upsert lots into PostGIS Gold layer."""
        if len(gdf) == 0:
            return 0

        df = gdf.copy()
        if "geometry" in df.columns:
            df["geometry_wkt"] = df["geometry"].apply(
                lambda geom: geom.wkt if geom else None
            )

        if "production_date" in df.columns:
            df["production_date"] = pd.to_datetime(df["production_date"], errors="coerce").dt.date

        if "dds_payload" in df.columns:
            df["dds_payload_json"] = df["dds_payload"].apply(json.dumps)
        else:
            df["dds_payload_json"] = "{}"

        cols = [
            "lot_id", "geometry_wkt", "country_of_production", "commodity",
            "production_date", "area_ha", "has_post_cutoff_alert",
            "has_recent_fire", "eudr_compliant", "h3_cell",
            "alert_count", "max_alert_intensity", "fire_count", "max_frp",
            "risk_score", "risk_level", "dds_payload_json",
        ]

        placeholders = ", ".join(["%s"] * len(cols))
        update_cols = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c != "lot_id"])

        query = f"""
            INSERT INTO lots_gold ({", ".join(cols)})
            VALUES ({placeholders})
            ON CONFLICT (lot_id) DO UPDATE SET
                {update_cols},
                updated_at = NOW()
        """

        with self.conn.cursor() as cur:
            for _, row in df[cols].iterrows():
                cur.execute(query, tuple(row))
            self.conn.commit()
        return len(df)

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "PostGISLoader":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


def load_to_duckdb(
    gdf: gpd.GeoDataFrame,
    db_path: Path | str = Path(".data/silver/eudr.duckdb"),
) -> int:
    """Convenience function to load lots to DuckDB."""
    with DuckDBLoader(db_path) as loader:
        loader.create_lots_table()
        return loader.load_lots(gdf)


def load_to_postgis(
    gdf: gpd.GeoDataFrame,
    host: str = "localhost",
    port: int = 5432,
    database: str = "eudr",
    user: str = "postgres",
    password: str = "",
) -> int:
    """Convenience function to load lots to PostGIS."""
    with PostGISLoader(host, port, database, user, password) as loader:
        return loader.upsert_lots(gdf)