"""
TerraCore Data Engine — Governança / UAI (ingestión del dataset nacional).

Ingiere el Urban Adaptation Index nacional (Di Giulio et al., Zenodo
10.5281/zenodo.15282393, `UAI table.xlsx`, hoja 'UAI') y lo filtra a los estados
del assessment. Es la capa de gobernanza (Dataset 2 de PA/AM). El UAI cubre los
5.569 municípios de Brasil, así que "extraer PA/AM" = filtrar por UF.

NO es scraping: es ingestión de un dataset publicado (CC-BY) que Adrian aportó.

Nota (replicabilidad): la hoja 'Metadata' del mismo xlsx SÍ documenta el código
IBGE-MUNIC de cada indicador (ej. E-Air = Mmam206) — o sea, la receta para
recalcular el UAI desde el MUNIC crudo existe en el suplemento (aunque no en el
texto del artículo). Eso habilita, a futuro, un conector MUNIC que reproduzca el
índice y valide contra estos valores.

Uso:
    from terracore_engine.uai_ingest import extract_uai
    df = extract_uai(estados=["PA", "AM"])
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from .base import ENGINE_REPO, load_municipalities, validate_output, save_processed

UAI_XLSX = ENGINE_REPO / "data/raw/uai/UAI_table.xlsx"
UAI_ZENODO = "https://zenodo.org/records/15282393/files/UAI%20table.xlsx?download=1"

# Columnas de la hoja 'UAI' → nombres limpios.
COLMAP = {
    "CodMun": "code_muni",
    "UAI": "uai_total",
    "Environ. Management": "uai_environmental",
    "Urban Food System": "uai_food_system",
    "Housing": "uai_housing",
    "Urban Mobility": "uai_mobility",
    "Clim. Imp. Respons.": "uai_climate_response",
}


def _ensure_xlsx() -> Path:
    if UAI_XLSX.exists():
        return UAI_XLSX
    import requests
    UAI_XLSX.parent.mkdir(parents=True, exist_ok=True)
    print(f"  Descargando UAI table.xlsx de Zenodo...")
    r = requests.get(UAI_ZENODO, timeout=120)
    r.raise_for_status()
    UAI_XLSX.write_bytes(r.content)
    return UAI_XLSX


def extract_uai(estados: list[str] | None = None) -> pd.DataFrame:
    """
    Filtra el UAI nacional a `estados`. Sub-índices clave para el assessment:
    Environmental Management y Urban Food System (los que pidió Adrian).

    Returns
    -------
    DataFrame tidy: code_muni + uai_total + uai_environmental + uai_food_system
    + uai_housing + uai_mobility + uai_climate_response.
    """
    estados = estados or ["PA", "AM"]
    xlsx = _ensure_xlsx()
    uai = pd.read_excel(xlsx, sheet_name="UAI")

    uai = uai[uai["UF"].isin([e.upper() for e in estados])].copy()
    uai["CodMun"] = uai["CodMun"].astype(str).str.strip().str.zfill(7)
    out = uai[list(COLMAP)].rename(columns=COLMAP)

    # Cross-check contra la malha del motor (Adrian: "confirmar efetividad da extração").
    munis = load_municipalities(estados=estados)
    faltan = set(munis["code_muni"]) - set(out["code_muni"])
    sobran = set(out["code_muni"]) - set(munis["code_muni"])
    if faltan:
        print(f"  AVISO: {len(faltan)} munis de la malha sin UAI: {sorted(faltan)[:5]}")
    if sobran:
        print(f"  AVISO: {len(sobran)} códigos UAI fuera de la malha: {sorted(sobran)[:5]}")
    print(f"  Cross-check malha↔UAI: {len(out)} filas UAI vs {len(munis)} munis malha.")

    out = out.reset_index(drop=True)
    validate_output(out, expected_cols=["code_muni", "uai_total", "uai_environmental",
                    "uai_food_system"], n_expected=len(munis), name="UAI")
    save_processed(out, "pa_am_uai.csv", name="UAI")
    return out


if __name__ == "__main__":
    print("=" * 60)
    print("TerraCore Engine — UAI nacional → PA/AM (Di Giulio et al.)")
    print("=" * 60)
    df = extract_uai(estados=["PA", "AM"])
    print(f"\n✓ {len(df)} municipios | UAI total medio: {df['uai_total'].mean():.3f}")
