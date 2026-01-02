"""
Root Tortilla Definition

Creates root Tortilla by:
1. Calling level0.build() to get all root samples
2. Applying Tortilla-level extensions (MajorTOM, GeoEnrich)

Extensions:
- MajorTOM: spherical grid codes for spatial grouping
- GeoEnrich: Earth Engine data (elevation, precipitation, etc.)
"""

import ee

from tacotoolbox.datamodel import Tortilla
from tacotoolbox.tortilla.extensions.majortom import MajorTOM
from tacotoolbox.tortilla.extensions.geoenrich import GeoEnrich

from dataset.levels.level0 import build as build_level0
from dataset.metadata import load_contexts
from dataset.config import LEVEL0_SAMPLE_LIMIT, LEVEL0_PARALLEL, WORKERS

ee.Initialize()


def create_tortilla(
    contexts: list[dict] | None = None,
    parallel: bool | None = None,
    workers: int | None = None,
) -> Tortilla:
    """
    Build root Tortilla from level0.
    
    Args:
        contexts: List of context dicts, if None uses load_contexts(LEVEL0_SAMPLE_LIMIT)
        parallel: Enable parallel processing, if None uses LEVEL0_PARALLEL from config
        workers: Number of workers, if None uses WORKERS from config
    
    Returns:
        Tortilla with all extensions applied
    """
    if contexts is None:
        contexts = load_contexts(limit=LEVEL0_SAMPLE_LIMIT)
    
    if parallel is None:
        parallel = LEVEL0_PARALLEL
    
    if workers is None:
        workers = WORKERS
    
    print(f"Building root Tortilla with {len(contexts)} contexts...")
    print(f"Parallel: {parallel}, Workers: {workers}")
    
    root_tortilla = build_level0(contexts, parallel=parallel, workers=workers)
    
    # MajorTOM extension - 1000km grid codes
    print("Applying MajorTOM extension (1000km)...")
    root_tortilla.extend_with(MajorTOM(dist_km=1000))
    
    # GeoEnrich extension - Earth Engine data
    print("Applying GeoEnrich extension...")
    root_tortilla.extend_with(GeoEnrich(
        variables=["elevation", "precipitation", "temperature", "admin_countries"],
        batch_size=250,
        scale_m=50_000,
    ))
    
    return root_tortilla


if __name__ == "__main__":
    import importlib
    from dataset.levels import level0
    importlib.reload(level0)
    
    contexts = load_contexts(limit=LEVEL0_SAMPLE_LIMIT or 2)
    print(f"Creating root Tortilla...")
    tortilla = create_tortilla(contexts)
    print(f"Created {len(tortilla.samples)} root samples")
    print(tortilla.export_metadata())