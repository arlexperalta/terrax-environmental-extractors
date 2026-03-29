"""
TerraCore Data Engine — Landscape Metrics from MapBiomas via GEE.

Computes landscape configuration metrics per municipality using
MapBiomas Collection 9 (30m resolution) processed server-side in GEE.

Metrics computed:
    - FC: Forest/native vegetation cover (% of municipality area)
    - edgeD: Edge density (m of edge per ha of municipality)
    - edgeT: Total edge length (m) of native vegetation patches
    - patchD: Patch density (number of native patches per 100 ha)
    - nPatches: Number of native vegetation patches
    - shannon: Shannon diversity index of land cover classes

Source: MapBiomas Collection 9
    Asset: projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_integration_v1
    Resolution: 30m
    Period: 1985-2023

Usage:
    from terracore.scripts.modules.landscape_metrics import extract_landscape_metrics
    df = extract_landscape_metrics(gdf, year=2020)
"""

import os
import ee
import pandas as pd
import time
import sys

GEE_PROJECT = os.environ.get("GEE_PROJECT", "your-gee-project-id")

# MapBiomas Collection 9 — LULC codes
MAPBIOMAS_ASSET = (
    "projects/mapbiomas-public/assets/brazil/lulc/collection9/"
    "mapbiomas_collection90_integration_v1"
)

# Native vegetation codes (same definition as bee-fenology)
NATIVE_CODES = [3, 4, 5, 6, 49, 11, 12, 32, 29, 50]


def _init_gee():
    """Initialize GEE if not already done."""
    try:
        ee.Number(1).getInfo()
    except Exception:
        ee.Initialize(project=GEE_PROJECT)


def _compute_metrics_for_feature(feature, lulc_binary, lulc_full, scale=30):
    """
    Compute landscape metrics for a single municipality feature.
    All computation happens server-side in GEE.

    Returns an ee.Feature with properties:
        FC, edgeT, edgeD, nPatches, patchD, shannon
    """
    geom = feature.geometry()
    area_m2 = geom.area()
    area_ha = area_m2.divide(10000)

    # --- FC: Native vegetation cover ---
    native_stats = lulc_binary.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=geom,
        scale=scale,
        maxPixels=1e9,
    )
    fc = ee.Number(native_stats.get("classification", 0)).multiply(100)

    # --- Edge detection ---
    # Edge = native pixels adjacent to non-native pixels
    # Use connectedComponents or Canny edge detection
    # Simpler: use focal operations
    # A native pixel is an edge pixel if any of its 8 neighbors is non-native
    kernel = ee.Kernel.square(1)  # 3x3 kernel
    native_sum = lulc_binary.reduceNeighborhood(
        reducer=ee.Reducer.sum(),
        kernel=kernel,
    )
    # Edge pixels: native=1 AND neighbor_sum < 9 (not all neighbors are native)
    edge_pixels = lulc_binary.eq(1).And(native_sum.lt(9))

    edge_count = edge_pixels.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=geom,
        scale=scale,
        maxPixels=1e9,
    )
    n_edge = ee.Number(edge_count.get("classification", 0))
    # Each edge pixel represents ~30m of edge (one pixel side)
    edge_total_m = n_edge.multiply(scale)
    edge_density = edge_total_m.divide(area_ha)

    # --- Patch count using connectedPixelCount ---
    # Label connected components of native vegetation
    patch_id = lulc_binary.selfMask().connectedComponents(
        connectedness=ee.Kernel.square(1),
        maxSize=1024,
    )
    # Count unique patch labels
    unique_labels = patch_id.select("labels").reduceRegion(
        reducer=ee.Reducer.countDistinct(),
        geometry=geom,
        scale=scale,
        maxPixels=1e9,
    )
    n_patches = ee.Number(unique_labels.get("labels", 0))
    patch_density = n_patches.divide(area_ha).multiply(100)  # per 100 ha

    # --- Shannon diversity index ---
    # H = -sum(p_i * ln(p_i)) for each land cover class
    # Compute pixel count per class
    class_hist = lulc_full.reduceRegion(
        reducer=ee.Reducer.frequencyHistogram(),
        geometry=geom,
        scale=scale,
        maxPixels=1e9,
    )
    hist_dict = ee.Dictionary(class_hist.get("classification", ee.Dictionary()))

    total_pixels = ee.Number(hist_dict.values().reduce(ee.Reducer.sum()))

    def compute_shannon_term(key, prev):
        count = ee.Number(hist_dict.get(key))
        p = count.divide(total_pixels)
        term = p.multiply(p.log())
        return ee.Number(prev).add(term)

    shannon = ee.Number(
        hist_dict.keys().iterate(compute_shannon_term, ee.Number(0))
    ).multiply(-1)

    return feature.set({
        "FC": fc,
        "edgeT": edge_total_m,
        "edgeD": edge_density,
        "nPatches": n_patches,
        "patchD": patch_density,
        "shannon": shannon,
    })


def extract_landscape_metrics(gdf, year=2020, scale=30, batch_size=50):
    """
    Extract landscape metrics for municipalities using GEE.

    Parameters
    ----------
    gdf : GeoDataFrame
        Municipality polygons with 'CD_MUN' column.
    year : int
        MapBiomas year to analyze (1985-2023).
    scale : int
        Processing resolution in meters (30 = native MapBiomas).
    batch_size : int
        Number of municipalities per GEE request.

    Returns
    -------
    DataFrame with codigo_ibg + landscape metric columns.
    """
    _init_gee()

    band = f"classification_{year}"
    lulc = ee.Image(MAPBIOMAS_ASSET).select(band).rename("classification")

    # Binary: native=1, non-native=0
    native_codes_ee = ee.List(NATIVE_CODES)
    lulc_binary = lulc.remap(
        NATIVE_CODES,
        [1] * len(NATIVE_CODES),
        defaultValue=0,
    ).rename("classification")

    print(f"Processing {len(gdf)} municipalities (year={year}, scale={scale}m)...")

    all_results = []
    total = len(gdf)

    # Process individually — batch fc.map() triggers GEE rate limits
    # and timeouts for complex computations like connectedComponents
    for idx, (_, row) in enumerate(gdf.iterrows()):
        mun_code = row["CD_MUN"]
        pct = 100 * (idx + 1) / total
        print(f"\r  [{idx+1}/{total}] {pct:.0f}% — {mun_code}...", end="", flush=True)

        retries = 0
        while retries < 3:
            try:
                result = _process_single(row, lulc_binary, lulc, scale)
                all_results.append(result)
                break
            except Exception as e:
                error_msg = str(e)
                if "Too many concurrent" in error_msg or "429" in error_msg:
                    retries += 1
                    wait = 15 * retries
                    print(f" rate-limited, waiting {wait}s...", end="", flush=True)
                    time.sleep(wait)
                elif "Computation timed out" in error_msg:
                    # Try with coarser scale
                    retries += 1
                    if retries == 1 and scale < 100:
                        print(f" timeout, retrying at 60m...", end="", flush=True)
                        try:
                            result = _process_single(row, lulc_binary, lulc, 60)
                            all_results.append(result)
                            break
                        except Exception:
                            pass
                    all_results.append({
                        "codigo_ibg": str(mun_code),
                        "FC": None, "edgeT": None, "edgeD": None,
                        "nPatches": None, "patchD": None, "shannon": None,
                    })
                    print(f" TIMEOUT", end="")
                    break
                else:
                    all_results.append({
                        "codigo_ibg": str(mun_code),
                        "FC": None, "edgeT": None, "edgeD": None,
                        "nPatches": None, "patchD": None, "shannon": None,
                    })
                    print(f" ERROR: {error_msg[:50]}", end="")
                    break

        # Brief pause between requests
        time.sleep(0.5)

    print()  # Newline after progress

    df = pd.DataFrame(all_results)
    return df


def _process_single(row, lulc_binary, lulc, scale):
    """Process a single municipality (fallback for timeouts)."""
    geom_json = row.geometry.__geo_interface__
    feat = ee.Feature(ee.Geometry(geom_json), {"CD_MUN": row["CD_MUN"]})
    result = _compute_metrics_for_feature(feat, lulc_binary, lulc, scale)
    props = result.getInfo()["properties"]
    return {
        "codigo_ibg": str(props.get("CD_MUN", "")),
        "FC": props.get("FC"),
        "edgeT": props.get("edgeT"),
        "edgeD": props.get("edgeD"),
        "nPatches": props.get("nPatches"),
        "patchD": props.get("patchD"),
        "shannon": props.get("shannon"),
    }


def extract_and_save(estados=None, codigos=None, year=2020, filename=None):
    """Extract landscape metrics and save to processed directory."""
    from .base import load_municipalities

    gdf = load_municipalities(codigos=codigos, estados=estados)
    df = extract_landscape_metrics(gdf, year=year)

    validate_output(
        df,
        expected_cols=["codigo_ibg", "FC", "edgeT", "patchD"],
        n_expected=len(gdf),
        name="Landscape",
    )

    fname = filename or "landscape_metrics_municipalities.csv"
    save_processed(df, fname, name="Landscape")
    return df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--estados", nargs="+", default=None, help="State codes (e.g., ES SP)")
    parser.add_argument("--year", type=int, default=2020)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    df = extract_and_save(estados=args.estados, year=args.year, filename=args.output)
    print(f"\nDone: {len(df)} municipalities with landscape metrics")
