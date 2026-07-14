"""
TerraCore Data Engine — Cobertura florestal por município (MapBiomas via GEE).

Extrae el % de cobertura florestal por município desde MapBiomas Collection 9
(30m). Versión liviana: solo forest cover (Dataset 5 del assessment PA/AM). Las
métricas de configuración del paisaje (edge/patch/Shannon) viven en
`landscape_metrics.py` y NO se corren aquí — usan connectedComponents, que se
ahoga con los polígonos gigantes de la Amazonia.

Definición de "floresta" = grupo nivel-1 "1. Floresta" de la leyenda MapBiomas
(codes 3, 4, 5, 6, 49). Ajustable si Adrian prefiere solo Formação Florestal (3).

Robusto para polígonos enormes: reduceRegion con bestEffort=True auto-ajusta la
escala para no timeoutear en municípios amazónicos de decenas de miles de km².

Uso:
    from terracore_engine.forest_cover import extract_forest_cover
    df = extract_forest_cover(estados=["PA", "AM"], year=2023)
"""
from __future__ import annotations

import os
import time

import ee
import pandas as pd

from .base import load_municipalities, validate_output, save_processed

GEE_PROJECT = os.environ.get("GEE_PROJECT", "earthengine-legacy-486401")

MAPBIOMAS_ASSET = (
    "projects/mapbiomas-public/assets/brazil/lulc/collection9/"
    "mapbiomas_collection90_integration_v1"
)

# MapBiomas leyenda nivel-1 "1. Floresta": Formação Florestal (3), Formação
# Savânica (4), Mangue (5), Floresta Alagável (6), Restinga Arbórea (49).
FOREST_CODES = [3, 4, 5, 6, 49]


def _init_gee() -> None:
    try:
        ee.Number(1).getInfo()
    except Exception:
        ee.Initialize(project=GEE_PROJECT)


def _fc_for_geometry(geom: ee.Geometry, forest_binary: ee.Image, scale: int = 30) -> float:
    """% de floresta en la geometría. bestEffort evita timeouts en polígonos enormes."""
    stat = forest_binary.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=geom,
        scale=scale,
        maxPixels=int(1e10),
        bestEffort=True,
    )
    val = stat.get("classification").getInfo()
    return round(val * 100, 3) if val is not None else None


def extract_forest_cover(
    estados: list[str] | None = None,
    year: int = 2023,
    scale: int = 30,
) -> pd.DataFrame:
    """
    Extrae % de cobertura florestal por município (MapBiomas Col9, año `year`).

    Returns
    -------
    DataFrame tidy: code_muni, NM_MUN, SIGLA_UF, forest_cover_pct, year.
    """
    estados = estados or ["PA", "AM"]
    _init_gee()

    munis = load_municipalities(estados=estados)

    band = f"classification_{year}"
    lulc = ee.Image(MAPBIOMAS_ASSET).select(band).rename("classification")
    forest_binary = lulc.remap(FOREST_CODES, [1] * len(FOREST_CODES), defaultValue=0) \
                        .rename("classification")

    total = len(munis)
    print(f"  MapBiomas Col9 {year} — forest cover para {total} municipios...")
    values: list[float | None] = []
    for idx, row in munis.iterrows():
        geom = ee.Geometry(row.geometry.__geo_interface__)
        pct = None
        for attempt in range(3):
            try:
                pct = _fc_for_geometry(geom, forest_binary, scale)
                break
            except Exception as e:
                msg = str(e)
                if ("Too many" in msg or "429" in msg) and attempt < 2:
                    time.sleep(10 * (attempt + 1))
                else:
                    print(f"\n    {row['code_muni']} ({row['NM_MUN']}): {msg[:70]}")
                    break
        values.append(pct)
        if (idx + 1) % 25 == 0 or idx + 1 == total:
            print(f"\r    [{idx + 1}/{total}]", end="", flush=True)
        time.sleep(0.2)
    print()

    out = munis[["code_muni", "NM_MUN", "SIGLA_UF"]].copy()
    out["forest_cover_pct"] = values
    out["year"] = year

    validate_output(out, expected_cols=["code_muni", "forest_cover_pct"],
                    n_expected=len(munis), name="ForestCover")
    save_processed(out, f"pa_am_forest_cover_{year}.csv", name="ForestCover")
    return out


if __name__ == "__main__":
    print("=" * 60)
    print("TerraCore Engine — Forest cover PA/AM (MapBiomas Col9)")
    print("=" * 60)
    df = extract_forest_cover(estados=["PA", "AM"], year=2023)
    print(f"\n✓ {df['forest_cover_pct'].notna().sum()}/{len(df)} municipios con dato")
