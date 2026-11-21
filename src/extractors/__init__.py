"""Extractors package for EUDR Deforestation Radar."""

from .base import BaseExtractor, Provenance
from .comtrade import ComtradeExtractor, MAGyPExtractor
from .firms import FIRMSExtractor
from .gfw import GFWExtractor
from .planetary_computer import PlanetaryComputerExtractor

__all__ = [
    "BaseExtractor",
    "Provenance",
    "GFWExtractor",
    "FIRMSExtractor",
    "ComtradeExtractor",
    "MAGyPExtractor",
    "PlanetaryComputerExtractor",
]