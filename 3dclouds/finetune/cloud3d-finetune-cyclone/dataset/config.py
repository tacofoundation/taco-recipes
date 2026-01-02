"""
TACO Dataset Configuration - Cloud3D Tropical Cyclones

Dataset combining GOES-16 and Himawari-8/9 geostationary imagery with CloudSat
radar profiles for tropical cyclones identified via IBTrACS best track archive.

DO NOT EDIT the COLLECTION dictionary at the bottom.
"""

import tacotoolbox
if tacotoolbox.__version__ < "0.22.0":
    raise ImportError(
        f"tacotoolbox >= 0.22.0 required (found {tacotoolbox.__version__}). "
        "Run: pip install -U tacotoolbox"
    )

from tacotoolbox.datamodel.taco import Provider, Curator, Publication, Publications

# Collection metadata
COLLECTION_ID = "cloud3d-finetune-cyclones"
COLLECTION_VERSION = "0.1.0"
COLLECTION_DESCRIPTION = (
    "Tropical cyclone subset from the Global 3D Cloud Reconstruction Dataset. "
    "Contains colocated pairs of geostationary imagery (GOES-16 ABI and Himawari-8/9 AHI) "
    "with CloudSat radar profiles for tropical cyclones identified via the IBTrACS "
    "(International Best Track Archive for Climate Stewardship) database. Each sample "
    "includes: multispectral geostationary imagery (16 spectral channels + satellite/solar "
    "angles), CloudSat vertical profiles as ground truth, colocation mask, and cyclone "
    "metadata (name, category, basin, distance to center). GOES covers Atlantic and "
    "Eastern Pacific basins; Himawari covers Western Pacific (highest TC frequency globally). "
    "256x256 pixel patches in Cloud-Optimized GeoTIFF format."
)
COLLECTION_LICENSES = ["CC-BY-4.0"]
COLLECTION_PROVIDERS = [
    Provider(name="NOAA", roles=["producer"], url="https://www.noaa.gov"),
    Provider(name="JMA", roles=["producer"], url="https://www.jma.go.jp"),
    Provider(name="NOAA NCEI", roles=["producer"], url="https://www.ncei.noaa.gov/products/international-best-track-archive"),
    Provider(name="Colorado State University", roles=["producer"], url="https://www.cloudsat.cira.colostate.edu"),
    Provider(name="European Space Agency (ESA)", roles=["licensor"], url="https://www.esa.int"),
    Provider(name="source.coop", roles=["host"], url="https://source.coop"),
]
COLLECTION_TASKS = ["regression", "foundation-model"]
COLLECTION_TITLE = "Cloud 3D - Tropical Cyclones Dataset"
COLLECTION_KEYWORDS = [
    "tropical cyclones",
    "hurricanes",
    "typhoons",
    "3d reconstruction",
    "cloud microphysics",
    "geostationary satellites",
    "GOES-16",
    "Himawari-8",
    "Himawari-9",
    "CloudSat",
    "IBTrACS",
    "best track",
    "remote sensing",
    "deep learning",
    "vertical structure",
    "eyewall",
    "rainbands",
]

# Dataset curators
COLLECTION_CURATORS = [
    Curator(name="Cesar Aybar", organization="Universitat de València", email="cesar.aybar@uv.es"),
    Curator(name="Shirin Ermis", organization="University of Oxford"),
    Curator(name="Lilli Freischem", organization="University of Oxford"),
    Curator(name="Stella Girtsou", organization="National Observatory of Athens"),
    Curator(name="Kyriaki-Margarita Bintsi", organization="Harvard Medical School"),
    Curator(name="Emiliano Diaz Salas-Porras", organization="Universitat de València"),
    Curator(name="Michael Eisinger", organization="European Space Agency"),
    Curator(name="William Jones", organization="University of Oxford"),
    Curator(name="Anna Jungbluth", organization="European Space Agency"),
    Curator(name="Benoit Tremblay", organization="Environment and Climate Change Canada"),
]

# Publications related to the dataset
COLLECTION_PUBLICATIONS = Publications(
    publications=[
        Publication(
            doi="10.48550/arXiv.2511.04773",
            citation=(
                "Ermis, S., Aybar, C., Freischem, L., Girtsou, S., Bintsi, K.-M., "
                "Diaz Salas-Porras, E., Eisinger, M., Jones, W., Jungbluth, A., & Tremblay, B. (2025). "
                "Global 3D Reconstruction of Clouds & Tropical Cyclones. "
                "Tackling Climate Change with Machine Learning Workshop at NeurIPS 2025."
            ),
            summary="Primary publication describing the dataset and methodology",
        ),
    ]
)

# DataFrame backend for testing/debugging output
DATAFRAME_BACKEND = "pandas"  # "pyarrow", "polars", "pandas"

# Parallel processing
WORKERS = 32
LEVEL0_PARALLEL = True
LEVEL0_SAMPLE_LIMIT = None  # None = all samples, set number for debugging

# Output settings
OUTPUT_PATH = "/data/databases/CLOUD_3D/pretraining/tacos/finetune/cyclones/cyclones.tacozip"
OUTPUT_FORMAT = "auto"      # "auto", "zip", or "folder"
SPLIT_SIZE = None           # Max size per ZIP file, None = no splitting
GROUP_BY = "majortom:code"  # Column(s) to group by, None = no grouping
CONSOLIDATE = True          # Auto-create .tacocat/ when multiple ZIPs generated

# Build options
CLEAN_PREVIOUS_OUTPUTS = True
VALIDATE_SCHEMA = True

# Documentation
GENERATE_DOCS = True
DOWNLOAD_BASE_URL = "https://source.coop/taco/3dclouds/cyclones/"
CATALOGUE_URL = "https://tacofoundation.github.io/catalogue"
THEME_COLOR = "#e74c3c"  # Red for cyclones/hurricanes
DATASET_EXAMPLE_PATH = "https://data.source.coop/taco/3dclouds/cyclones/"

# Parquet configuration (passed to create() as **kwargs)
PARQUET_ROW_GROUP_SIZE = 122880
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 22
PARQUET_USE_DICTIONARY = False
PARQUET_WRITE_STATISTICS = False
PARQUET_DATA_PAGE_SIZE = 256 * 1024


# INTERNAL: Auto-generated dictionaries (DO NOT EDIT)

COLLECTION = {
    "id": COLLECTION_ID,
    "dataset_version": COLLECTION_VERSION,
    "description": COLLECTION_DESCRIPTION,
    "licenses": COLLECTION_LICENSES,
    "providers": COLLECTION_PROVIDERS,
    "tasks": COLLECTION_TASKS,
    "title": COLLECTION_TITLE,
    "keywords": COLLECTION_KEYWORDS,
}

if COLLECTION_CURATORS is not None:
    COLLECTION["curators"] = COLLECTION_CURATORS

if COLLECTION_PUBLICATIONS is not None:
    COLLECTION["publications"] = COLLECTION_PUBLICATIONS.publications

BUILD_CONFIG = {
    "workers": WORKERS,
    "level0_parallel": LEVEL0_PARALLEL,
    "level0_sample_limit": LEVEL0_SAMPLE_LIMIT,
    "output": OUTPUT_PATH,
    "format": OUTPUT_FORMAT,
    "split_size": SPLIT_SIZE,
    "group_by": GROUP_BY,
    "consolidate": CONSOLIDATE,
    "clean_previous_outputs": CLEAN_PREVIOUS_OUTPUTS,
    "validate_schema": VALIDATE_SCHEMA,
    "generate_docs": GENERATE_DOCS,
    "download_base_url": DOWNLOAD_BASE_URL,
    "catalogue_url": CATALOGUE_URL,
    "theme_color": THEME_COLOR,
    "dataset_example_path": DATASET_EXAMPLE_PATH,
}

PARQUET_CONFIG = {
    "row_group_size": PARQUET_ROW_GROUP_SIZE,
    "compression": PARQUET_COMPRESSION,
    "compression_level": PARQUET_COMPRESSION_LEVEL,
    "use_dictionary": PARQUET_USE_DICTIONARY,
    "write_statistics": PARQUET_WRITE_STATISTICS,
    "data_page_size": PARQUET_DATA_PAGE_SIZE,
}