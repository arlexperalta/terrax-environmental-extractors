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
_STATS = ["mean", "std", "max"]


def _zonal_stats3(gdf, raster: Path, band: int) -> list[dict]:
    """
    Estadísticos zonales por polígono: media + SD + máximo intra-município
    (all_touched → cubre munis chicos a 18 km). El SD/máx capturan la
    heterogeneidad espacial del clima DENTRO del município (relevante en Pará,
    donde los munis son enormes): la media sola la promedia y la esconde.
    `count` = nº de celdas raster que caen en el polígono (diagnóstico: con 1
    sola celda el SD no es informativo).
    """
    return zonal_stats(gdf.geometry, str(raster), band=band,
                       stats="mean std max count", all_touched=True, nodata=-3.4e38)


def _nanmean(vals: list[float | None]) -> float | None:
    """Media ignorando None (ensemble de GCMs); None si todos son None."""
    present = [v for v in vals if v is not None]
    return sum(present) / len(present) if present else None


def _scope_tag(estados: list[str]) -> str:
    """Prefijo de archivo derivado del scope ('pa', 'pa_am', ...)."""
    return "_".join(sorted(e.strip().lower() for e in estados))


# Subconjunto interpretable que va al CSV consolidable (el parquet lleva las 19):
# temp media anual, estacionalidade térmica, precip anual, precip do mês mais
# úmido/seco, estacionalidade da precip. Capturan el cambio y los extremos /
# lluvias erráticas (el aquecimento medio bio1 satura; estos discriminan).
SUBSET = [1, 4, 12, 13, 14, 15]
SUBSET_DESC = {
    1: "temp media anual", 4: "estacionalidade térmica", 12: "precip anual",
    13: "precip mês mais úmido", 14: "precip mês mais seco", 15: "estacionalidade precip",
}


def extract_worldclim(estados: list[str] | None = None, res: str = RES) -> pd.DataFrame:
    """
    Extrae el banco climático WorldClim/CMIP6 por município: media + SD + máximo
    intra-município del presente y de cada SSP 2050, más la anomalía (delta) de la
    media. El SD/máx dan la variabilidad ESPACIAL dentro del município; la anomalía
    da el cambio TEMPORAL presente→2050 (el foco de Adrian: dónde va a cambiar).

    Returns
    -------
    DataFrame consolidable (contrato motor): code_muni, NM_MUN, SIGLA_UF, y para el
    subconjunto interpretable de bio: anomalía media por SSP + SD/máx intra-muni del
    presente. (El banco completo de 19 bio × {present,ssp} × {mean,std,max} va al parquet.)
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
    full: dict[str, list] = {}
    n_muni = len(gdf)

    # --- Presente (1970-2000): 19 tifs separados, 1 banda c/u ---
    print(f"  presente (1970-2000) — {N_BIO} bioclim × mean/std/max, {n_muni} municipios...")
    for i in range(1, N_BIO + 1):
        s = _zonal_stats3(gdf, hist[i - 1], band=1)
        for st in _STATS:
            full[f"bio{i}_present_{st}"] = [x[st] for x in s]
        if i == 1:                       # nº de celdas por muni (diagnóstico, una vez)
            full["n_cells"] = [x["count"] for x in s]

    # --- Futuro 2050: ensemble (media sobre los 3 GCMs de cada stat) por SSP ---
    for ssp in SSPS:
        print(f"  futuro 2050 {ssp} — ensemble de {len(GCMS)} GCMs × mean/std/max...")
        per_gcm = {g: [_zonal_stats3(gdf, fut[(g, ssp)], band=i) for i in range(1, N_BIO + 1)]
                   for g in GCMS}
        for i in range(1, N_BIO + 1):
            for st in _STATS:
                # ensemble = media de la stat sobre los GCMs, município a município
                full[f"bio{i}_{ssp}_{st}"] = [
                    _nanmean([per_gcm[g][i - 1][k][st] for g in GCMS])
                    for k in range(n_muni)
                ]

    # --- Anomalías (delta) de la MEDIA: futuro − presente, por SSP, las 19 ---
    for ssp in SSPS:
        for i in range(1, N_BIO + 1):
            p = full[f"bio{i}_present_mean"]
            fu = full[f"bio{i}_{ssp}_mean"]
            full[f"bio{i}_anom_{ssp}"] = [
                (b - a) if (a is not None and b is not None) else None
                for a, b in zip(p, fu)
            ]

    # Banco completo de las 19 (× present/ssp × mean/std/max + anom) → parquet (insumo SDM).
    full_df = munis[["code_muni", "NM_MUN", "SIGLA_UF"]].copy()
    for k, v in full.items():
        full_df[k] = [round(x, 4) if x is not None else None for x in v]
    save_processed(full_df, f"{_scope_tag(estados)}_worldclim_bio19_{res}.parquet",
                   name="WorldClimFull")

    # Banco consolidable → CSV (contrato motor): subconjunto interpretable.
    #   anom (delta medio) por SSP + SD y máx intra-muni del presente.
    for i in SUBSET:
        for ssp in SSPS:
            wide[f"bio{i}_anom_{ssp}"] = [round(x, 3) if x is not None else None
                                         for x in full[f"bio{i}_anom_{ssp}"]]
        wide[f"bio{i}_present_sd"] = [round(x, 3) if x is not None else None
                                     for x in full[f"bio{i}_present_std"]]
        wide[f"bio{i}_present_max"] = [round(x, 3) if x is not None else None
                                      for x in full[f"bio{i}_present_max"]]

    validate_output(
        wide,
        expected_cols=["code_muni", "bio1_anom_ssp585", "bio15_anom_ssp585",
                       "bio15_present_sd"],
        n_expected=len(munis), name="WorldClim",
    )
    save_processed(wide, f"{_scope_tag(estados)}_worldclim_{res}.csv", name="WorldClim")
    return wide


if __name__ == "__main__":
    print("=" * 60)
    print("TerraCore Engine — Clima WorldClim/CMIP6 Pará (paper Gomes 2025)")
    print("=" * 60)
    download_worldclim()
    df = extract_worldclim(estados=["PA"])
    print(f"\n✓ {len(df)} municipios")
    cols = ["NM_MUN", "bio1_anom_ssp245", "bio1_anom_ssp585",
            "bio15_anom_ssp585", "bio15_present_sd"]
    print(df[cols].head(6).to_string(index=False))
