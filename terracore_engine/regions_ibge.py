"""
TerraCore Data Engine — Regiões do IBGE por município (agrupadores espaciais).

Capa de AGRUPACIÓN del banco: aporta, por município, los niveles regionales del
IBGE que sirven como efecto aleatorio en los modelos mixtos (agrupar municípios
vecinos que comparten contexto). La malha `BR_Municipios_2022.shp` solo trae
`SIGLA_UF`, así que este conector completa la jerarquía regional.

Dos jerarquías, a propósito (T-decisión de Adrian: probar cuál ajusta mejor):
  - **Clásica** (pre-2017): mesorregião → microrregião. Es la que se usó en SP.
  - **Nueva** (2017+): região geográfica intermediária → imediata. Reemplazó a la
    clásica en la nomenclatura oficial, pero ambas siguen publicadas.

Fuente: API de localidades del IBGE (sin descarga de archivo, sin credencial):
    https://servicodados.ibge.gov.br/api/v1/localidades/estados/{UF}/municipios

Salida: una fila por município (`code_muni`), códigos + nombres de los 4 niveles.
Los códigos (no los nombres) son la llave estable para el efecto aleatorio.

Uso:
    from terracore_engine.regions_ibge import extract_regions
    df = extract_regions(estados=["PA"])
"""
from __future__ import annotations

import requests
import pandas as pd

from .base import validate_output, save_processed

_API = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios"


def _parse_municipio(m: dict) -> dict:
    """Aplana un município de la API a las 4 llaves regionales (códigos + nombres)."""
    micro = m.get("microrregiao") or {}
    meso = (micro.get("mesorregiao") or {}) if micro else {}
    imediata = m.get("regiao-imediata") or {}
    intermediaria = (imediata.get("regiao-intermediaria") or {}) if imediata else {}
    return {
        "code_muni": str(m["id"]).zfill(7),
        # --- clásica ---
        "cod_meso": meso.get("id"),
        "nome_meso": meso.get("nome"),
        "cod_micro": micro.get("id"),
        "nome_micro": micro.get("nome"),
        # --- nueva (2017+) ---
        "cod_reg_intermediaria": intermediaria.get("id"),
        "nome_reg_intermediaria": intermediaria.get("nome"),
        "cod_reg_imediata": imediata.get("id"),
        "nome_reg_imediata": imediata.get("nome"),
    }


def extract_regions(estados: list[str] | None = None) -> pd.DataFrame:
    """
    Trae los agrupadores regionales del IBGE por município para los `estados` dados.

    Returns
    -------
    DataFrame: code_muni + meso/micro (clásica) + intermediária/imediata (nueva),
    cada una con código y nombre. Una fila por município.
    """
    estados = estados or ["PA"]
    rows: list[dict] = []
    for uf in estados:
        r = requests.get(_API.format(uf=uf.strip().upper()), timeout=120)
        r.raise_for_status()
        munis = r.json()
        rows.extend(_parse_municipio(m) for m in munis)
        print(f"  {uf.upper()}: {len(munis)} municípios")

    df = pd.DataFrame(rows).sort_values("code_muni").reset_index(drop=True)

    validate_output(
        df,
        expected_cols=["code_muni", "cod_meso", "cod_micro",
                       "cod_reg_intermediaria", "cod_reg_imediata"],
        n_expected=len(df), name="RegionsIBGE",
    )
    save_processed(df, "pa_regions_ibge.csv", name="RegionsIBGE")
    return df


if __name__ == "__main__":
    print("=" * 60)
    print("TerraCore Engine — Regiões IBGE (meso/micro + intermediária/imediata)")
    print("=" * 60)
    df = extract_regions(estados=["PA"])
    print(f"\n✓ {len(df)} municípios")
    print(f"  mesorregiões:          {df['cod_meso'].nunique()}")
    print(f"  microrregiões:         {df['cod_micro'].nunique()}")
    print(f"  regiões intermediárias:{df['cod_reg_intermediaria'].nunique():>3}")
    print(f"  regiões imediatas:     {df['cod_reg_imediata'].nunique():>3}")
    print()
    print(df[["code_muni", "nome_micro", "nome_meso",
              "nome_reg_imediata", "nome_reg_intermediaria"]].head(6).to_string(index=False))
