"""
Level 0 - Root Level

This is the ROOT and LEAF level of your TACO hierarchy. Each sample here is a FILE
(the actual data), not a FOLDER containing more samples.

Structure:
    level0 (root - FILEs)

How to use:
    1. Define your sample builders (one function per file type)
    2. Each builder receives a context dict and returns a Sample
    3. Add extensions to extract metadata (Header, GeotiffStats, STAC, etc.)
    4. Add your builders to the SAMPLES list

Run directly to test:
    python dataset/levels/level0.py

Note:
    level0.build() iterates over ALL contexts and creates the root Tortilla.
    This is the only level that iterates - all others receive a single context.
    Parallel processing is controlled by config.py (LEVEL0_PARALLEL, WORKERS).
"""

import numpy as np
import shapely
import pyproj
import rasterio as rio
from dateutil.parser import isoparse

from tacotoolbox.datamodel import Sample, Tortilla
from tacotoolbox.sample.extensions.stac import STAC
from tacotoolbox.sample.extensions.tacotiff import Header
from tacotoolbox.sample.extensions.geotiff_stats import GeotiffStats
from tacotoolbox.validator.sample import TacoTIFF

from dataset.metadata import load_contexts
from dataset.config import LEVEL0_SAMPLE_LIMIT, LEVEL0_PARALLEL, WORKERS
from dataset.extensions import Satellite


# Tortilla parameters
PAD_TO = None
STRICT_SCHEMA = True


# Sample builders - one function per file type
def build_sample_msg(ctx: dict) -> Sample:
    """
    MSG GeoTIFF sample with STAC metadata.
    
    Reads existing .tif file and extracts:
    - STAC metadata with EPSG:4326 centroid
    - TacoTIFF Header
    - GeotiffStats
    - cloud3d custom metadata (satellite only, no cyclones)
    """
    sample = Sample(id=ctx["id"], path=ctx["path"], type="FILE")
    
    # Validate as TacoTIFF
    sample.validate_with(TacoTIFF())
    
    # Add TacoTIFF Header
    sample.extend_with(Header())
    
    # Read GeoTIFF metadata
    with rio.open(ctx["path"]) as src:
        meta = src.meta
        tags = src.tags()
        bounds = src.bounds
        
        # Calculate centroid in EPSG:4326
        transformer = pyproj.Transformer.from_crs(
            src.crs,
            "EPSG:4326",
            always_xy=True
        )
        
        # Reproject corners
        lon_min, lat_min = transformer.transform(bounds.left, bounds.bottom)
        lon_max, lat_max = transformer.transform(bounds.right, bounds.top)
        
        centroid_lon = (lon_min + lon_max) / 2
        centroid_lat = (lat_min + lat_max) / 2
        centroid_wkb = shapely.to_wkb(shapely.Point(centroid_lon, centroid_lat))
    
    # Extract timestamp from tags (MSG uses acquisition_time)
    # Convert to microseconds (STAC expects microseconds)
    acquisition_time_us = int(isoparse(tags["acquisition_time"]).timestamp() * 1_000_000)
    
    # Add STAC metadata with corrected centroid
    stac = STAC(
        crs=meta["crs"].to_string(),
        tensor_shape=(meta["count"], meta["height"], meta["width"]),
        geotransform=meta["transform"].to_gdal(),
        time_start=acquisition_time_us,
        time_end=acquisition_time_us,
        centroid=centroid_wkb
    )
    sample.extend_with(stac)
    
    # Add GeotiffStats
    sample.extend_with(GeotiffStats())
    
    # Add cloud3d custom metadata
    sample.extend_with(Satellite(satellite="MSG"))
    
    return sample


SAMPLES = [
    build_sample_msg,
]


# Helper for parallel processing - must be at module level for pickling
def _build_samples_parallel(ctx: dict) -> tuple[list[Sample] | None, tuple[str, str] | None]:
    """
    Build samples for one context (used in parallel mode).
    
    Returns:
        (samples, None) on success
        (None, (id, error)) on failure
    """
    try:
        samples = [fn(ctx) for fn in SAMPLES]
        
        # Validate geometry (no infinites, valid shapes)
        for sample in samples:
            geom = shapely.from_wkb(sample.model_dump()["stac:centroid"])
            if not shapely.is_valid(geom):
                return None, (ctx["id"], "Invalid geometry")
            if np.isinf(geom.bounds).any():
                return None, (ctx["id"], "Infinite bounds")
        
        return samples, None
    except Exception as e:
        return None, (ctx["id"], str(e))


# Build function - ROOT level iterates over ALL contexts
def build(contexts: list[dict] | None = None, parallel: bool | None = None, workers: int | None = None) -> Tortilla:
    """
    Build root Tortilla from contexts.
    
    Args:
        contexts: List of context dicts, if None uses load_contexts(LEVEL0_SAMPLE_LIMIT)
        parallel: Enable parallel processing, if None uses LEVEL0_PARALLEL from config
        workers: Number of workers, if None uses WORKERS from config
    """
    if contexts is None:
        contexts = load_contexts(limit=LEVEL0_SAMPLE_LIMIT)
    
    if parallel is None:
        parallel = LEVEL0_PARALLEL
    
    if workers is None:
        workers = WORKERS
    
    failed_ids = []
    
    # Generate samples in parallel or serial
    if parallel:
        from concurrent.futures import ProcessPoolExecutor
        
        with ProcessPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(_build_samples_parallel, contexts))
        
        samples = []
        for result, error in results:
            if error:
                failed_ids.append(error[0])
                print(f"Failed to build sample {error[0]}: {error[1]}")
            else:
                samples.extend(result)
    else:
        samples = []
        for ctx in contexts:
            result, error = _build_samples_parallel(ctx)
            if error:
                failed_ids.append(error[0])
                print(f"Failed to build sample {error[0]}: {error[1]}")
            else:
                samples.extend(result)
    
    if failed_ids:
        print(f"\nTotal failed samples: {len(failed_ids)}")
        print(f"Failed IDs: {failed_ids}")
    
    return Tortilla(
        samples=samples,
        pad_to=PAD_TO,
        strict_schema=STRICT_SCHEMA,
    )


# Validation - run directly to test
if __name__ == "__main__":
    contexts = load_contexts(limit=LEVEL0_SAMPLE_LIMIT or 10)
    print(f"Building level0 with {len(contexts)} contexts...")
    print(f"Parallel: {LEVEL0_PARALLEL}, Workers: {WORKERS}")
    tortilla = build(contexts)
    print(f"Created {len(tortilla.samples)} root samples")
    print(tortilla.export_metadata())