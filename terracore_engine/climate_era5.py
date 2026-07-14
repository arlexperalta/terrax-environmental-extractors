"""
TerraCore Data Engine — Clima de referencia ERA5-Land (histórico + extremos).

Capa climática de REFERENCIA del assessment PA/AM (complementa la proyección
futura de WorldClim/CMIP6, que va en otro conector). Adrian la pidió para tener
el clima real observado y, sobre todo, **dónde ocurren los máximos históricos
(ondas de calor)** — algo que la climatología promediada de WorldClim no da.

Fuente: dataset zonal ERA5-Land por município (Zenodo 10.5281/zenodo.10036211,
CC-BY), daily 1950-2022. Los .parquet (~3 GB c/u) se descargan aparte a
`data/raw/era5/` (ver `download_era5_files`); este conector los procesa local.

Métricas por município:
  - era5_tmean_c       : temperatura media (normal climática WMO 1991-2020), °C
  - era5_tmax_hist_c   : máximo histórico de la temperatura máxima diaria, °C
  - era5_tmax_p99_c    : p99 de la máxima diaria (extremo robusto), °C
  - era5_precip_mm     : precipitación anual media (1991-2020), mm

Uso:
    from terracore_engine.climate_era5 import extract_era5_reference
    df = extract_era5_reference(estados=["PA", "AM"])
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from .base import ENGINE_REPO, load_municipalities, validate_output, save_processed

ERA5_DIR = ENGINE_REPO / "data/raw/era5"
BASELINE = (1991, 2020)  # normal climática WMO

FILES = {
    "tmean": "2m_temperature_mean.parquet",
    "tmax": "2m_temperature_max.parquet",
    "precip": "total_precipitation_sum.parquet",
}


def _read_pa_am(path: Path, codes: list[int]) -> pd.DataFrame:
    """Lee un parquet ERA5 local filtrado a los municípios PA/AM (predicate pushdown)."""
    tbl = pq.read_table(path, columns=["code_muni", "date", "value"],
                        filters=[("code_muni", "in", codes)])
    df = tbl.to_pandas()
    df["year"] = pd.to_datetime(df["date"]).dt.year
    df["code_muni"] = df["code_muni"].astype(str).str.zfill(7)
    return df


def extract_era5_reference(estados: list[str] | None = None) -> pd.DataFrame:
    """Clima ERA5 de referencia por município (baseline + extremos históricos)."""
    estados = estados or ["PA", "AM"]
    munis = load_municipalities(estados=estados)
    codes = munis["code_muni"].astype(int).tolist()

    missing = [f for f in FILES.values() if not (ERA5_DIR / f).exists()]
    if missing:
        raise FileNotFoundError(
            f"Faltan .parquet de ERA5 en {ERA5_DIR}: {missing}. "
            f"Descárgalos primero (download_era5_files)."
        )

    out = munis[["code_muni", "NM_MUN", "SIGLA_UF"]].copy()

    # Temperatura media — normal 1991-2020 → °C
    print("  procesando tmean (baseline 1991-2020)...")
    tm = _read_pa_am(ERA5_DIR / FILES["tmean"], codes)
    base = tm[tm["year"].between(*BASELINE)]
    tmean = (base.groupby("code_muni")["value"].mean() - 273.15).round(2)
    out["era5_tmean_c"] = out["code_muni"].map(tmean)

    # Temperatura máxima — máximo histórico + p99 (extremos) → °C
    print("  procesando tmax (extremos históricos 1950-2022)...")
    tx = _read_pa_am(ERA5_DIR / FILES["tmax"], codes)
    g = tx.groupby("code_muni")["value"]
    out["era5_tmax_hist_c"] = out["code_muni"].map((g.max() - 273.15).round(2))
    out["era5_tmax_p99_c"] = out["code_muni"].map((g.quantile(0.99) - 273.15).round(2))

    # Precipitación — total anual medio 1991-2020 → mm (ERA5 tp en metros)
    print("  procesando precip (anual media 1991-2020)...")
    pr = _read_pa_am(ERA5_DIR / FILES["precip"], codes)
    pr = pr[pr["year"].between(*BASELINE)]
    annual = pr.groupby(["code_muni", "year"])["value"].sum()      # total anual (m)
    precip_mm = (annual.groupby("code_muni").mean() * 1000).round(1)
    out["era5_precip_mm"] = out["code_muni"].map(precip_mm)

    validate_output(out, expected_cols=["code_muni", "era5_tmean_c", "era5_tmax_hist_c",
                    "era5_precip_mm"], n_expected=len(munis), name="ERA5ref")
    save_processed(out, "pa_am_era5_reference.csv", name="ERA5ref")
    return out


if __name__ == "__main__":
    print("=" * 60)
    print("TerraCore Engine — Clima ERA5 referencia PA/AM")
    print("=" * 60)
    df = extract_era5_reference(estados=["PA", "AM"])
    print(f"\n✓ {len(df)} municipios")
    print(df[["NM_MUN", "SIGLA_UF", "era5_tmean_c", "era5_tmax_hist_c", "era5_precip_mm"]]
          .head(5).to_string(index=False))
