"""
Level 1 - Leaf Level (FILEs)

This is the LEAF level containing the actual data files:
  - geo_patch.tif: Geostationary imagery (21 bands: 16 spectral + 4 angles + 1 mask)
  - cloudsat_aligned.tif: CloudSat radar profiles (8 bands: 7 variables + Height)

Structure:
    level0 (root, FOLDERS) → level1 (FILEs)

Sample IDs are FIXED (same for all parents) to ensure PIT compliance.
"""

from pathlib import Path

import pyarrow as pa
from tacotoolbox.datamodel import Sample, Tortilla
from tacotoolbox.sample.extensions.tacotiff import Header
from tacotoolbox.sample.extensions.geotiff_stats import GeotiffStats


# Tortilla parameters
PAD_TO = None
STRICT_SCHEMA = True


def build_geo_patch(ctx: dict) -> Sample:
    """Build Sample for geostationary imagery (GOES or Himawari)."""
    folder = Path(ctx["path"].decode() if isinstance(ctx["path"], bytes) else ctx["path"])
    tif_path = folder / "geo_patch.tif"
    
    if not tif_path.exists():
        raise FileNotFoundError(f"geo_patch.tif not found in {folder}")
    
    sample = Sample(id="geo_patch", path=tif_path)
    sample.extend_with(Header())
    sample.extend_with(GeotiffStats())
    
    return sample


def build_cloudsat_aligned(ctx: dict) -> Sample:
    """Build Sample for CloudSat radar profiles aligned to geostationary grid."""
    folder = Path(ctx["path"].decode() if isinstance(ctx["path"], bytes) else ctx["path"])
    tif_path = folder / "cloudsat_aligned.tif"
    
    if not tif_path.exists():
        raise FileNotFoundError(f"cloudsat_aligned.tif not found in {folder}")
    
    sample = Sample(id="cloudsat_aligned", path=tif_path)
    sample.extend_with(Header())
    sample.extend_with(GeotiffStats())
    
    return sample


SAMPLES = [
    build_geo_patch,
    build_cloudsat_aligned,
]


def build(ctx: dict) -> Tortilla:
    """Build level1 Tortilla containing geo_patch and cloudsat_aligned files."""
    return Tortilla(
        samples=[fn(ctx) for fn in SAMPLES],
        pad_to=PAD_TO,
        strict_schema=STRICT_SCHEMA,
    )


if __name__ == "__main__":
    # Test with first sample from metadata
    from dataset.metadata import load_contexts
    
    contexts = load_contexts(limit=2)
    print(f"Testing level1 with {len(contexts)} contexts...")
    
    for ctx in contexts:
        tortilla = build(ctx)
        print(f"\n{ctx['id']}:")
        print(f"  Samples: {[s.id for s in tortilla.samples]}")
        print(f"  Satellite: {ctx.get('satellite_type', 'unknown')}")
    
    print("\nSchema:")
    print(tortilla.export_metadata())