"""
TerraCore Data Engine — Pobreza municipal (CadÚnico / MDS).

Extrae familias en extrema pobreza por município desde el CadÚnico vía la API
MI Social del SAGI/MDS (Solr). Es la capa de pobreza del assessment PA/AM
(Dataset 4a). Fuente reachable (SISVAN nutrición va aparte, ver `nutrition_sisvan.py`).

Detalle de join: SAGI expone `codigo_ibge` de **6 dígitos** (sin dígito verificador);
la malha del motor usa `code_muni` de 7. Se une por los 6 primeros y se conserva
la llave canónica de 7 en la salida.

Métrica principal: proporción de familias en extrema pobreza sobre el total de
familias del CadÚnico (`pct_extrema_pobreza_cadun`). Se conservan conteos crudos
+ población estimada para que aguas abajo se pueda normalizar distinto.

Uso:
    from terracore_engine.poverty_cadunico import extract_poverty
    df = extract_poverty(estados=["PA", "AM"])
"""
from __future__ import annotations

import pandas as pd
import requests

from .base import load_municipalities, validate_output, save_processed

MISOCIAL = "https://aplicacoes.mds.gov.br/sagi/servicos/misocial"

FIELD_TOTAL = "cadun_qtd_familias_atualizadas_i"
FIELD_EXTREMA = "cadun_qtde_fam_sit_extrema_pobreza_s"
FIELD_POP = "populacao_estimada_ibge_ano_i"


def _latest_anomes(uf: str = "PA") -> str:
    """Último anomes (AAAAMM) con dato de CadÚnico en el SAGI."""
    r = requests.get(MISOCIAL, params={
        "q": "*:*", "fq": [f"sigla_uf:{uf}", f"{FIELD_TOTAL}:[1 TO *]"],
        "rows": "1", "sort": "anomes desc", "fl": "anomes", "wt": "json",
    }, timeout=30)
    r.raise_for_status()
    docs = r.json()["response"]["docs"]
    if not docs:
        raise RuntimeError("SAGI misocial no devolvió anomes con dato de CadÚnico.")
    return docs[0]["anomes"]


def _to_num(v) -> float | None:
    if v is None:
        return None
    try:
        return float(str(v).replace(",", "."))
    except (ValueError, TypeError):
        return None


def extract_poverty(
    estados: list[str] | None = None,
    anomes: str | None = None,
) -> pd.DataFrame:
    """
    Extrae pobreza CadÚnico por município para `estados`, en `anomes`
    (default: el más reciente disponible).

    Returns
    -------
    DataFrame tidy: code_muni (7 díg) + cadun_fam_total + cadun_fam_extrema_pobreza
    + pct_extrema_pobreza_cadun + populacao_estimada + anomes.
    """
    estados = estados or ["PA", "AM"]
    munis = load_municipalities(estados=estados)
    munis["code6"] = munis["code_muni"].str[:6]

    anomes = anomes or _latest_anomes(estados[0])
    uf_filter = " OR ".join(estados)
    print(f"  CadÚnico (SAGI/MDS) — anomes {anomes}, {'/'.join(estados)}...")

    r = requests.get(MISOCIAL, params={
        "q": "*:*", "fq": [f"anomes:{anomes}", f"sigla_uf:({uf_filter})"],
        "rows": "600", "wt": "json",
        "fl": f"codigo_ibge,municipio,{FIELD_TOTAL},{FIELD_EXTREMA},{FIELD_POP}",
    }, timeout=60)
    r.raise_for_status()
    docs = r.json()["response"]["docs"]

    sagi = pd.DataFrame([{
        "code6": str(d.get("codigo_ibge", "")).zfill(6),
        "cadun_fam_total": _to_num(d.get(FIELD_TOTAL)),
        "cadun_fam_extrema_pobreza": _to_num(d.get(FIELD_EXTREMA)),
        "populacao_estimada": _to_num(d.get(FIELD_POP)),
    } for d in docs])

    out = munis[["code_muni", "code6", "NM_MUN", "SIGLA_UF"]].merge(
        sagi, on="code6", how="left").drop(columns="code6")

    out["pct_extrema_pobreza_cadun"] = (
        out["cadun_fam_extrema_pobreza"] / out["cadun_fam_total"]
    ).round(4)
    out["anomes"] = anomes

    validate_output(out, expected_cols=["code_muni", "cadun_fam_extrema_pobreza",
                    "pct_extrema_pobreza_cadun"], n_expected=len(munis),
                    name="PovertyCadUnico")
    save_processed(out, "pa_am_poverty_cadunico.csv", name="PovertyCadUnico")
    return out


if __name__ == "__main__":
    print("=" * 60)
    print("TerraCore Engine — Pobreza CadÚnico PA/AM (SAGI/MDS)")
    print("=" * 60)
    df = extract_poverty(estados=["PA", "AM"])
    print(f"\n✓ {df['pct_extrema_pobreza_cadun'].notna().sum()}/{len(df)} municipios con dato")
