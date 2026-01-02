"""
Level 1 - Leaf Level

This is the LEAF level of your TACO hierarchy. Each sample here is a FILE
(the actual data), not a FOLDER containing more samples.

Structure:
    level0 (root) → level1 (LEAF - FILEs)

How to use:
    1. Define your sample builders (one function per file type)
    2. Each builder receives a context dict and returns a Sample
    3. Add extensions to extract metadata (Header, GeotiffStats, STAC, etc.)
    4. Add your builders to the SAMPLES list

Run directly to test:
    python dataset/levels/level1.py

Note:
    build(ctx) receives ONE context and creates ONE Tortilla.
    The parent level (level0) is responsible for iterating over multiple contexts.
    
    IMPORTANT: Sample IDs at level1+ must be FIXED (same for all parents).
    Only level0 can have different IDs. This ensures PIT compliance.
"""

from pathlib import Path

from tacotoolbox.datamodel import Sample, Tortilla
from tacotoolbox.sample.extensions.tacotiff import Header
from tacotoolbox.sample.extensions.geotiff_stats import GeotiffStats

from dataset.metadata import load_contexts


# Tortilla parameters
PAD_TO = None
STRICT_SCHEMA = True


def build_sample_geo_patch(ctx: dict) -> Sample:
    """
    GOES-16/ABI geostationary imagery.
    
    20 bands total:
        - 16 spectral channels (visible + infrared)
        - 4 geometry bands (satellite/solar azimuth & zenith angles)
    
    Format: Cloud-Optimized GeoTIFF, 256x256 pixels
    """
    path: Path = ctx["path"]
    sample = Sample(id="geo_patch", path=path / "geo_patch.tif")
    sample.extend_with(Header())
    sample.extend_with(GeotiffStats())
    return sample


def build_sample_cloudsat(ctx: dict) -> Sample:
    """
    CloudSat radar profile (ground truth).
    
    Aligned and reprojected CloudSat CPR data for supervised learning.
    Contains vertical cloud structure information.
    
    Format: Cloud-Optimized GeoTIFF
    """
    path: Path = ctx["path"]
    sample = Sample(id="cloudsat_aligned", path=path / "cloudsat_aligned.tif")
    sample.extend_with(Header())
    sample.extend_with(GeotiffStats())
    return sample


SAMPLES = [
    build_sample_geo_patch,
    build_sample_cloudsat,
]


def build(ctx: dict) -> Tortilla:
    """Build level1 Tortilla from context."""
    return Tortilla(
        samples=[fn(ctx) for fn in SAMPLES],
        pad_to=PAD_TO,
        strict_schema=STRICT_SCHEMA,
    )


if __name__ == "__main__":
    contexts = load_contexts(limit=2)
    print(f"Testing level1 with {len(contexts)} contexts...")
    for ctx in contexts:
        tortilla = build(ctx)
        print(f"  {ctx['id']}: {len(tortilla.samples)} samples")
    print("Done!")