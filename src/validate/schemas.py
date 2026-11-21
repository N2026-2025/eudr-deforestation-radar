"""Data schemas for EUDR Deforestation Radar."""

from __future__ import annotations

from typing import Any


def validate_geojson_geometry(geom: Any) -> bool:
    """Validate that geometry is a valid GeoJSON geometry object."""
    import shapely.geometry
    if geom is None:
        return False
    if isinstance(geom, dict):
        try:
            shapely.geometry.shape(geom)
            return True
        except Exception:
            return False
    return False


def validate_crs(crs_value: Any) -> bool:
    """Validate CRS value."""
    import pyproj
    try:
        if isinstance(crs_value, str):
            pyproj.CRS(crs_value)
            return True
        elif hasattr(crs_value, "to_string"):
            pyproj.CRS(crs_value.to_string())
            return True
        return False
    except Exception:
        return False


# EUDR-specific schemas

EUDR_DEFORESTATION_LOT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": [
        "lot_id",
        "geometry",
        "country_of_production",
        "commodity",
        "production_date",
    ],
    "properties": {
        "lot_id": {"type": "string"},
        "geometry": {"type": "object", "patternProperties": {"^$": {"type": "object"}}},
        "country_of_production": {"type": "string"},
        "commodity": {"type": "string"},
        "production_date": {"type": "string", "format": "date"},
        "area_ha": {"type": "number"},
        "has_post_cutoff_alert": {"type": "boolean"},
        "has_recent_fire": {"type": "boolean"},
        "eudr_compliant": {"type": "boolean"},
        "h3_cell": {"type": ["string", "null"]},
    },
}


ECONOMIC_OPERATOR_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["operator_id", "operator_role", "name", "address"],
    "properties": {
        "operator_id": {"type": "string"},
        "operator_role": {
            "type": "string",
            "enum": ["OPERATOR", "REPRESENTATIVE_OPERATOR", "DOWNSTREAM_OPERATOR"],
        },
        "name": {"type": "string"},
        "address": {"type": "object"},
        "country": {"type": "string", "pattern": "^[A-Z]{2}$"},
    },
}


GEO_LOCATION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["type", "coordinates"],
    "properties": {
        "type": {
            "type": "string",
            "enum": ["Point", "MultiPoint", "Polygon", "MultiPolygon", "GeometryCollection"],
        },
        "coordinates": {"type": "array"},
    },
}


LOT_RISK_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["lot_id", "country_risk", "deforestation_risk", "overall_risk_level"],
    "properties": {
        "lot_id": {"type": "string"},
        "country_risk": {
            "type": "string",
            "enum": ["low", "standard", "high"],
        },
        "deforestation_risk": {
            "type": "string",
            "enum": ["clear", "suspect", "high"],
        },
        "overall_risk_level": {
            "type": "string",
            "enum": ["low", "medium", "high"],
        },
        "risk_assessment_date": {"type": "string", "format": "date"},
    },
}