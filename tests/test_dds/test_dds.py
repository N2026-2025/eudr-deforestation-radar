"""Tests for EUDR Deforestation Radar DDS generation."""

from __future__ import annotations

import json

import pytest

from src.dds import DDSGenerator


def test_dds_generator_init():
    """Test DDS generator initialization."""
    gen = DDSGenerator(operator_id="test_operator")
    assert gen.operator_id == "test_operator"
    assert gen.DDS_VERSION == "3"


def test_dds_generation_basic():
    """Test basic DDS generation for a compliant lot."""
    gen = DDSGenerator(operator_id="test_operator")
    lot_data = {
        "lot_id": "LOT-001",
        "geometry": None,
        "commodity": "soy",
        "production_date": "2019-08-15",
        "country_of_production": "AR",
        "hs_heading": "1201",
        "quantity": 500.0,
        "eudr_compliant": True,
    }

    dds = gen.generate_dds_for_lot(lot_data)

    assert dds["ddsVersion"] == "3"
    assert dds["ddsType"] == "due-diligence-statement-v3"
    assert dds["activityType"] == "IMPORT"
    assert len(dds["commodities"]) == 1
    assert dds["commodities"][0]["hsHeading"] == "1201"
    assert dds["riskAssessment"]["riskLevel"] == "low"


def test_dds_generation_high_risk():
    """Test DDS generation flags high risk for non-compliant lot."""
    gen = DDSGenerator(operator_id="test_operator")
    lot_data = {
        "lot_id": "LOT-002",
        "geometry": None,
        "commodity": "beef",
        "production_date": "2021-01-01",
        "country_of_production": "AR",
        "has_post_cutoff_alert": True,
        "has_recent_fire": True,
        "eudr_compliant": False,
    }

    dds = gen.generate_dds_for_lot(lot_data)

    assert dds["riskAssessment"]["riskLevel"] == "high"


def test_dds_medium_risk_alert_only():
    """Test DDS medium risk when only alert present."""
    gen = DDSGenerator(operator_id="test_operator")
    lot_data = {
        "lot_id": "LOT-003",
        "geometry": None,
        "commodity": "soy",
        "has_post_cutoff_alert": True,
        "has_recent_fire": False,
        "eudr_compliant": True,
    }

    dds = gen.generate_dds_for_lot(lot_data)

    assert dds["riskAssessment"]["riskLevel"] == "medium"


def test_dds_batch_generation():
    """Test batch DDS generation."""
    gen = DDSGenerator(operator_id="test_operator")
    lots = [
        {"lot_id": "LOT-001", "geometry": None, "commodity": "soy", "eudr_compliant": True},
        {"lot_id": "LOT-002", "geometry": None, "commodity": "beef", "eudr_compliant": True},
    ]

    dds_list = gen.generate_dds_batch(lots)

    assert len(dds_list) == 2
    assert all(d["ddsVersion"] == "3" for d in dds_list)


def test_dds_json_serialization():
    """Test DDS JSON string generation."""
    gen = DDSGenerator(operator_id="test_operator")
    dds = gen.generate_dds_for_lot({"lot_id": "LOT-001", "geometry": None, "commodity": "soy"})

    json_str = gen.generate_dds_json_string(dds)
    parsed = json.loads(json_str)

    assert parsed["ddsVersion"] == "3"
    assert "internalReferenceNumber" in parsed