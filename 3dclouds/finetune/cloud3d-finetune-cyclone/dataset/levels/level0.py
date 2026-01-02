"""
Level 0 - Root Level (FOLDERs)

Root level containing FOLDER samples. Each sample wraps a level1 Tortilla
and includes metadata extensions.

Structure:
    level0 (root, FOLDERS) -> level1 (FILEs)

Extensions applied:
    - STAC: spatial/temporal metadata from geo_patch.tif
    - Cloud3DMetadata: satellite and CloudSat identifiers
    - Cloud3DCycloneMetadata: IBTrACS cyclone metadata
"""

from datetime import datetime
from pathlib import Path

from osgeo import gdal
from tacotoolbox.datamodel import Sample, Tortilla
from tacotoolbox.sample.extensions.stac import STAC

from dataset.levels import level1
from dataset.metadata import load_contexts
from dataset.extensions import Cloud3DMetadata, Cloud3DCycloneMetadata


# Tortilla parameters
PAD_TO = None
STRICT_SCHEMA = True


def parse_timestamp(date_str: str) -> datetime:
    """Parse timestamp from global.json format (with or without milliseconds)."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")


def get_geostationary_crs(satellite_type: str) -> str:
    """Return the correct PROJ string for each geostationary satellite."""
    if satellite_type == "GOES-16":
        # GOES: lon_0=-75, sweep=x
        return (
            "+proj=geos +h=35786023 +a=6378137 +b=6356752.31414 "
            "+f=0.00335281066474748 +lat_0=0 +lon_0=-75 +sweep=x +no_defs"
        )
    else:
        # Himawari: lon_0=140.7, sweep=y
        return (
            "+proj=geos +h=35786023 +a=6378137 +b=6356752.31414 "
            "+f=0.00335281066474748 +lat_0=0 +lon_0=140.7 +sweep=y +no_defs"
        )


def build_sample(ctx: dict) -> Sample:
    """Build a FOLDER sample containing level1 tortilla with all extensions."""
    import json
    from shapely.geometry import Point
    
    folder = Path(ctx["path"].decode() if isinstance(ctx["path"], bytes) else ctx["path"])
    
    # Build child tortilla
    child_tortilla = level1.build(ctx)
    
    # Create FOLDER sample
    sample = Sample(id=ctx["id"], path=child_tortilla)
    
    # Read global.json for cyclone center (use as patch centroid approximation)
    json_files = list(folder.glob("*_global.json"))
    attrs = {}
    if json_files:
        with open(json_files[0], "r") as f:
            attrs = json.load(f).get("attributes", {})
    
    # STAC extension (read from geo_patch.tif)
    geo_patch_path = folder / "geo_patch.tif"
    ds = gdal.Open(str(geo_patch_path))
    
    # Parse timestamp (STAC expects int64 microseconds since Unix epoch)
    ts_micro = None
    if ctx.get("start"):
        ts = parse_timestamp(ctx["start"])
        ts_micro = int(ts.timestamp() * 1_000_000)
    
    # Use cyclone center as centroid (WKB format)
    # Normalize longitude to [-180, 180] (IBTrACS uses [0, 360] for some basins)
    centroid_wkb = None
    if "LAT" in attrs and "LON" in attrs:
        lon = attrs["LON"]
        lat = attrs["LAT"]
        if lon > 180:
            lon = lon - 360
        centroid = Point(lon, lat)
        centroid_wkb = centroid.wkb
    
    stac = STAC(
        crs=get_geostationary_crs(ctx["satellite_type"]),
        tensor_shape=(ds.RasterCount, ds.RasterYSize, ds.RasterXSize),
        geotransform=ds.GetGeoTransform(),
        time_start=ts_micro,
        centroid=centroid_wkb,
    )
    ds = None  # Close dataset
    
    sample.extend_with(stac)
    
    # Cloud3DMetadata extension
    satellite = "GOES" if ctx["satellite_type"] == "GOES-16" else "Himawari"
    cloud3d_meta = Cloud3DMetadata.from_directory(folder, satellite=satellite)
    sample.extend_with(cloud3d_meta)
    
    # Cloud3DCycloneMetadata extension
    cyclone_meta = Cloud3DCycloneMetadata.from_directory(folder)
    sample.extend_with(cyclone_meta)
    
    return sample


def build(
    contexts: list[dict] | None = None,
    parallel: bool = True,
    workers: int = 32,
) -> Tortilla:
    """Build root tortilla with all samples."""
    from concurrent.futures import ProcessPoolExecutor, as_completed
    from dataset.config import LEVEL0_SAMPLE_LIMIT
    
    if contexts is None:
        contexts = load_contexts(limit=LEVEL0_SAMPLE_LIMIT)
    
    if parallel and len(contexts) > 1:
        samples = []
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(build_sample, ctx): ctx["id"] for ctx in contexts}
            for future in as_completed(futures):
                sample_id = futures[future]
                try:
                    sample = future.result()
                    samples.append(sample)
                except Exception as e:
                    print(f"Error processing {sample_id}: {e}")
                    raise
    else:
        samples = [build_sample(ctx) for ctx in contexts]
    
    return Tortilla(
        samples=samples,
        pad_to=PAD_TO,
        strict_schema=STRICT_SCHEMA,
    )


if __name__ == "__main__":
    contexts = load_contexts(limit=2)
    print(f"Testing level0 with {len(contexts)} contexts...")
    
    for ctx in contexts:
        sample = build_sample(ctx)
        print(f"\n{ctx['id']}:")
        print(f"  Satellite: {ctx['satellite_type']}")
        print(f"  Children: {[s.id for s in sample.path.samples]}")
    
    print("\nBuilding full tortilla (sequential)...")
    tortilla = build(contexts, parallel=False)
    print(f"Root tortilla: {len(tortilla.samples)} samples")
    print("\nSchema:")
    print(tortilla.export_metadata())