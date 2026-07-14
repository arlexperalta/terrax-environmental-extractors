"""
TerraCore Data Engine — Clima presente + proyección futura (WorldClim 2.1 / CMIP6).

Capa climática de PROYECCIÓN del assessment PA/AM: anomalía presente → 2050. Es
la capa 3 del motor (Dataset 3). Complementa —no reemplaza— a `climate_era5.py`,
que da los extremos históricos observados (ondas de calor) que la climatología
promediada de WorldClim no captura.

MÉTODO — alineado 1:1 al paper del equipo (Adrian coautor):
  Gomes, Andrino, Barbosa-Silva, Acosta, González-Chaves, Slätis, Giannini (2025).
  "Food insecurity under global human-induced changes: Plants of the future in the
  Amazonian biome." Biological Conservation 311:111398.
  https://doi.org/10.1016/j.biocon.2025.111398

  - Fuente: WorldClim 2.1, 19 variables bioclimáticas (Fick & Hijmans 2017).
  - Presente: histórico 1970-2000 (el presente sale de WorldClim, NO de ERA5, para
    que viva en el mismo espacio de variables que el futuro CMIP6 → anomalía limpia.
    Resuelve la "Decisión B" del PLAN_MOTOR_PARA.md).
  - Futuro: 2050 (WorldClim lo entrega como período 2041-2060). El paper NO usa 2070.
  - Escenarios: SSP2-4.5 (optimista) + SSP5-8.5 (business-as-usual).
  - GCMs (CMIP6/AR6): BCC-CSM2-MR, MIROC-ES2L, MRI-ESM2-0 — se **ensamblan** (media).
  - Resolución: 10 arc-min (~18 km) ≈ el 0.16° del paper. Gruesa a propósito: capta
    tendencias macroecológicas y evita overfitting en una región vasta.

Salida (dos productos, por diseño):
  1. CSV consolidable (contrato motor, wide) — variables macro interpretables para el
     policy gap: bio1 (temp media anual) y bio12 (precip anual), presente + cada SSP +
     anomalía. Se enchufa al `municipios_gradient_PA_AM.csv`.
  2. Parquet completo — las 19 bioclim × (presente, ssp245, ssp585, anomalías). Insumo
     del SDM de especies (el paper filtra por Spearman |ρ|<0.7; NO filtramos nosotros:
     entregamos las 19 y el análisis decide — T6).

Uso:
    from terracore_engine.climate_worldclim import download_worldclim, extract_worldclim
    download_worldclim()                       # baja histórico + 6 futuros (una vez)
    df = extract_worldclim(estados=["PA", "AM"])
"""
from __future__ import annotations

import zipfile
from pathlib import Path

import pandas as pd
import requests
from rasterstats import zonal_stats

from .base import ENGINE_REPO, load_municipalities, validate_output, save_processed

# --------------------------------------------------------------------------- #
# Parámetros del método (paper Gomes et al. 2025) — no cambiar sin decisión de Adrian
# --------------------------------------------------------------------------- #
RES = "10m"                                     # 10 arc-min ~18 km ≈ 0.16° del paper
PERIOD = "2041-2060"                            # el "2050" en la nomenclatura WorldClim
GCMS = ["BCC-CSM2-MR", "MIROC-ES2L", "MRI-ESM2-0"]   # CMIP6/AR6, se ensamblan
SSPS = ["ssp245", "ssp585"]                     # SSP2-4.5 (optimista) + SSP5-8.5 (BAU)
N_BIO = 19                                      # bio1..bio19

_BASE = "https://geodata.ucdavis.edu"
WC_DIR = ENGINE_REPO / "data/raw/worldclim"

# Variables macro que van al banco consolidado (las 19 quedan en el parquet).
MACRO = {"bio1": "temp media anual (°C)", "bio12": "precip anual (mm)"}


# --------------------------------------------------------------------------- #
# Descarga
# --------------------------------------------------------------------------- #
def _download(url: str, dest: Path) -> None:
    if dest.exists() and dest.stat().st_size > 0:
        print(f"    ya existe: {dest.name}")
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"    bajando: {dest.name} ...", end="", flush=True)
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        tmp = dest.with_suffix(dest.suffix + ".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
        tmp.rename(dest)
    print(f" {dest.stat().st_size / 1e6:.1f} MB")


def download_worldclim(res: str = RES) -> None:
    """Descarga el histórico 1970-2000 + los 6 futuros (3 GCMs × 2 SSP) a data/raw/worldclim/."""
    WC_DIR.mkdir(parents=True, exist_ok=True)
    print(f"WorldClim 2.1 @ {res} → {WC_DIR}")

    # Histórico (zip con 19 tifs) — solo si falta el bio_1.
    hist_marker = WC_DIR / f"wc2.1_{res}_bio_1.tif"
    if not hist_marker.exists():
        zip_path = WC_DIR / f"wc2.1_{res}_bio.zip"
        _download(f"{_BASE}/climate/worldclim/2_1/base/wc2.1_{res}_bio.zip", zip_path)
        print("    descomprimiendo histórico...")
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(WC_DIR)
        zip_path.unlink()
    else:
        print(f"    histórico ya presente ({hist_marker.name} ...)")

    # Futuros CMIP6 (un GeoTIFF multibanda de 19 bandas por GCM×SSP).
    for gcm in GCMS:
        for ssp in SSPS:
            name = f"wc2.1_{res}_bioc_{gcm}_{ssp}_{PERIOD}.tif"
            _download(f"{_BASE}/cmip6/{res}/{gcm}/{ssp}/{name}", WC_DIR / name)


# --------------------------------------------------------------------------- #
# Zonal stats
# --------------------------------------------------------------------------- #
def _zonal_mean(gdf, raster: Path, band: int) -> list[float | None]:
    """Media zonal de una banda por polígono (all_touched → cubre munis chicos a 18 km)."""
    stats = zonal_stats(gdf.geometry, str(raster), band=band,
                        stats="mean", all_touched=True, nodata=-3.4e38)
    return [s["mean"] for s in stats]


def extract_worldclim(estados: list[str] | None = None, res: str = RES) -> pd.DataFrame:
    """
    Extrae el banco climático WorldClim/CMIP6 por município (presente + 2050 + anomalías).

    Returns
    -------
    DataFrame consolidable (contrato motor): code_muni, NM_MUN, SIGLA_UF,
    bio1/bio12 present + por SSP + anomalías. (El banco completo de 19 bio va al parquet.)
    """
    estados = estados or ["PA", "AM"]
    munis = load_municipalities(estados=estados)

    hist = [WC_DIR / f"wc2.1_{res}_bio_{i}.tif" for i in range(1, N_BIO + 1)]
    fut = {(g, s): WC_DIR / f"wc2.1_{res}_bioc_{g}_{s}_{PERIOD}.tif"
           for g in GCMS for s in SSPS}
    missing = [p.name for p in hist + list(fut.values()) if not p.exists()]
    if missing:
        raise FileNotFoundError(
            f"Faltan rasters WorldClim en {WC_DIR}: {missing[:4]}"
            f"{'...' if len(missing) > 4 else ''}. Corre download_worldclim() primero."
        )

    # WorldClim es WGS84 (EPSG:4326); la malha IBGE es SIRGAS 2000 → reproyectar.
    gdf = munis.to_crs(4326)
    wide = munis[["code_muni", "NM_MUN", "SIGLA_UF"]].copy()

    # --- Presente (1970-2000): 19 tifs separados, 1 banda c/u ---
    print(f"  presente (1970-2000) — {N_BIO} bioclim, {len(gdf)} municipios...")
    full = {}
    for i in range(1, N_BIO + 1):
        full[f"bio{i}_present"] = _zonal_mean(gdf, hist[i - 1], band=1)

    # --- Futuro 2050: ensemble (media de los 3 GCMs) por SSP; tif multibanda 19 ---
    for ssp in SSPS:
        print(f"  futuro 2050 {ssp} — ensemble de {len(GCMS)} GCMs...")
        for i in range(1, N_BIO + 1):
            per_gcm = [_zonal_mean(gdf, fut[(g, ssp)], band=i) for g in GCMS]
            # media de los GCMs, celda a celda (ignora None de algún GCM aislado)
            ens = [
                (sum(v for v in vals if v is not None) / len([v for v in vals if v is not None]))
                if any(v is not None for v in vals) else None
                for vals in zip(*per_gcm)
            ]
            full[f"bio{i}_{ssp}"] = ens

    # --- Anomalías: futuro - presente, por SSP, las 19 ---
    for ssp in SSPS:
        for i in range(1, N_BIO + 1):
            p = full[f"bio{i}_present"]
            fu = full[f"bio{i}_{ssp}"]
            full[f"bio{i}_anom_{ssp}"] = [
                (b - a) if (a is not None and b is not None) else None
                for a, b in zip(p, fu)
            ]

    # Banco completo de las 19 → parquet (insumo SDM).
    full_df = munis[["code_muni", "NM_MUN", "SIGLA_UF"]].copy()
    for k, v in full.items():
        full_df[k] = [round(x, 4) if x is not None else None for x in v]
    save_processed(full_df, f"pa_am_worldclim_bio19_{res}.parquet", name="WorldClimFull")

    # Banco consolidable → CSV (contrato motor): solo macro (bio1 temp, bio12 precip).
    for bio in MACRO:
        wide[f"{bio}_present"] = [round(x, 2) if x is not None else None
                                 for x in full[f"{bio}_present"]]
        for ssp in SSPS:
            wide[f"{bio}_{ssp}_2050"] = [round(x, 2) if x is not None else None
                                         for x in full[f"{bio}_{ssp}"]]
            wide[f"{bio}_anom_{ssp}"] = [round(x, 2) if x is not None else None
                                         for x in full[f"{bio}_anom_{ssp}"]]

    validate_output(
        wide,
        expected_cols=["code_muni", "bio1_present", "bio1_anom_ssp585",
                       "bio12_present", "bio12_anom_ssp585"],
        n_expected=len(munis), name="WorldClim",
    )
    save_processed(wide, f"pa_am_worldclim_{res}.csv", name="WorldClim")
    return wide


if __name__ == "__main__":
    print("=" * 60)
    print("TerraCore Engine — Clima WorldClim/CMIP6 PA/AM (paper Gomes 2025)")
    print("=" * 60)
    download_worldclim()
    df = extract_worldclim(estados=["PA", "AM"])
    print(f"\n✓ {len(df)} municipios")
    cols = ["NM_MUN", "SIGLA_UF", "bio1_present", "bio1_anom_ssp245",
            "bio1_anom_ssp585", "bio12_anom_ssp585"]
    print(df[cols].head(6).to_string(index=False))
