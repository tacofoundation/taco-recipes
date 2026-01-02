"""
Level 0 - Root Level (FOLDER samples)

Each sample is a FOLDER containing MSG/SEVIRI-CloudSat colocated data.
This level applies spatial/temporal metadata and dataset-specific extensions.

Structure:
    level0 (FOLDER) → level1 (FILE: geo_patch, cloudsat_aligned)

Extensions applied at this level:
    - STAC: Spatial/temporal metadata (from geo_patch.tif + directory name)
    - Split: Train/val/test partition (based on day of month)
    - Cloud3DMetadata: Dataset-specific fields (from JSON + directory name)
"""

import re
from datetime import datetime
from pathlib import Path
from typing import Literal

import numpy as np
import rasterio as rio
import shapely

from tacotoolbox.datamodel import Sample, Tortilla
from tacotoolbox.sample.extensions.stac import STAC
from tacotoolbox.sample.extensions.split import Split

from dataset.levels.level1 import build as build_level1
from dataset.extensions import Cloud3DMetadata
from dataset.metadata import load_contexts


# Split configuration based on day of month
SPLIT_DAYS = {
    "train": list(range(1, 24)),       # Days 1-23
    "validation": list(range(24, 28)), # Days 24-27
    "test": list(range(28, 32)),       # Days 28-31
}


def parse_timestamp_from_dirname(dirname: str) -> int:
    """
    Parse timestamp from MSG directory name.
    
    Pattern: MSG1_20060613001240_CS_...
                  ↑↑↑↑↑↑↑↑↑↑↑↑↑↑
                  YYYYMMDDHHmmss
    
    Args:
        dirname: Directory name (e.g., "MSG1_20060613001240_CS_2006163223419_00664_merged_patch_00")
        
    Returns:
        Timestamp in microseconds since Unix epoch
    """
    # Extract timestamp string: MSG{N}_{TIMESTAMP}_CS_...
    match = re.match(r"MSG\d+_(\d{14})_", dirname)
    if not match:
        raise ValueError(f"Cannot parse timestamp from: {dirname}")
    
    timestamp_str = match.group(1)  # "20060613001240"
    dt = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
    
    return int(dt.timestamp() * 1_000_000)


def determine_split(timestamp_us: int) -> Literal["train", "validation", "test"]:
    """
    Determine train/val/test split based on acquisition date.
    
    Split logic:
        - train: days 1-23 of any month
        - validation: days 24-27 of any month
        - test: days 28-31 of any month
    
    Args:
        timestamp_us: Timestamp in microseconds since Unix epoch
    """
    dt = datetime.fromtimestamp(timestamp_us / 1_000_000)
    day = dt.day
    
    if day in SPLIT_DAYS["train"]:
        return "train"
    elif day in SPLIT_DAYS["validation"]:
        return "validation"
    else:
        return "test"


def extract_stac_metadata(ref_file: Path, timestamp_us: int) -> STAC:
    """
    Extract STAC metadata from reference GeoTIFF (geo_patch.tif).
    
    Args:
        ref_file: Path to geo_patch.tif
        timestamp_us: Acquisition timestamp in microseconds (parsed from dirname)
    
    Returns:
        STAC extension with spatial/temporal metadata
    """
    with rio.open(ref_file) as src:
        metadata = src.meta
    
    return STAC(
        crs=metadata["crs"].to_string(),
        tensor_shape=(metadata["count"], metadata["height"], metadata["width"]),
        geotransform=metadata["transform"].to_gdal(),
        time_start=timestamp_us,
        time_end=timestamp_us,
    )


def build_sample(ctx: dict) -> Sample | None:
    """
    Build a single FOLDER sample from context.
    
    Args:
        ctx: Context dict with 'id' and 'path' keys
        
    Returns:
        Sample object or None if invalid
    """
    try:
        sample_id: str = ctx["id"]
        sample_path: Path = ctx["path"]
        
        # Parse timestamp from directory name
        timestamp_us = parse_timestamp_from_dirname(sample_id)
        
        # Build level1 Tortilla (FILE samples)
        tortilla = build_level1(ctx)
        
        # Create FOLDER sample
        sample = Sample(
            id=sample_id,
            type="FOLDER",
            path=tortilla,
        )
        
        # Extract STAC metadata from geo_patch.tif
        geo_patch_file = sample_path / "geo_patch.tif"
        stac = extract_stac_metadata(geo_patch_file, timestamp_us)
        sample.extend_with(stac)
        
        # Validate geometry
        sample_data = sample.model_dump()
        shapely_geom = shapely.from_wkb(sample_data["stac:centroid"])
        if not shapely.is_valid(shapely_geom) or np.isinf(shapely_geom.bounds).any():
            print(f"Invalid geometry for {sample_id}")
            return None
        
        # Determine split based on acquisition time
        split = determine_split(timestamp_us)
        sample.extend_with(Split(split=split))
        
        # Add Cloud3D-specific metadata
        cloud3d_meta = Cloud3DMetadata.from_directory(
            directory=sample_path,
            satellite="MSG",
            is_cyclone=False,
        )
        sample.extend_with(cloud3d_meta)
        
        return sample
        
    except Exception as e:
        print(f"Error processing {ctx.get('id', 'unknown')}: {e}")
        return None


SAMPLES = [build_sample]


def build(
    contexts: list[dict],
    parallel: bool = False,
    workers: int = 4,
) -> Tortilla:
    """
    Build root Tortilla from all contexts.
    
    Args:
        contexts: List of context dicts
        parallel: Whether to use parallel processing
        workers: Number of parallel workers
        
    Returns:
        Root Tortilla with all valid samples
    """
    if parallel:
        from multiprocessing import Pool
        with Pool(workers) as pool:
            samples = pool.map(build_sample, contexts)
    else:
        samples = [build_sample(ctx) for ctx in contexts]
    
    # Filter out None (invalid) samples
    valid_samples = [s for s in samples if s is not None]
    print(f"Built {len(valid_samples)}/{len(contexts)} valid samples")
    
    return Tortilla(samples=valid_samples)


if __name__ == "__main__":
    contexts = load_contexts(limit=3)
    print(f"Testing level0 with {len(contexts)} contexts...")
    tortilla = build(contexts, parallel=False)
    print(f"Root Tortilla: {len(tortilla.samples)} samples")
    if tortilla.samples:
        print(f"First sample metadata: {tortilla.samples[0].model_dump().keys()}")