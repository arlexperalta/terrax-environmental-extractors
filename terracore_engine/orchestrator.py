"""
TerraCore Data Engine — Orquestador PA/AM.

Última pieza del motor MVP: cruza las 5 capas del banco municipal por `code_muni`
y emite el contrato de salida que consume el análisis `policy_gap` de SP:

    municipios_gradient_PA_AM.csv  — una fila por município, columnas por variable,
                                     keyed por code_muni (7 dígitos IBGE).

Además emite un **relatório de cobertura** (missing data explícito por município y
por variable) — el doc de Belém lo pide para no pasar por alto munis aislados sin
dato. La cobertura NO se falla: se reporta (es esperable en munis remotos del AM).

Diseño:
  - La malha (206 munis PA/AM) es el esqueleto: left-join de cada capa → toda capa
    ausente o incompleta se ve como NULL, nunca como fila perdida.
  - Cada capa aporta sus columnas de valor; NM_MUN/SIGLA_UF vienen de la malha y los
    metadatos temporales constantes (year/anomes/ano) se registran como vintage de
    capa, no por fila.
  - Los nombres de columna ya son únicos entre capas; una colisión es error duro
    (señal de un cambio de esquema que hay que resolver, no silenciar).

Uso:
    python -m terracore_engine.orchestrator
    # o
    from terracore_engine.orchestrator import build_gradient
    banco, cobertura = build_gradient(estados=["PA", "AM"])
"""
from __future__ import annotations

import pandas as pd

from .base import load_municipalities, save_processed, PROCESSED_DIR

# --------------------------------------------------------------------------- #
# Registro de capas. `drop` = columnas a NO traer (redundantes con la malha o
# metadatos constantes). `vintage` documenta la cosecha del dato (va al log, no
# a cada fila). El orden fija el orden de columnas en el banco.
# --------------------------------------------------------------------------- #
# `sparse: True` marca capas de presencia/ausencia esperada: un NaN NO es missing
# data sino "el município no produce ese cultivo" (cero estructural). No cuentan
# como gap de cobertura; se reportan como conteo de ítems presentes. La imputación
# 0-vs-NaN del dato es decisión de Adrian (T6), aquí no se toca.
LAYERS = [
    {"key": "regioes", "file": "pa_regions_ibge.csv",
     "drop": [], "vintage": "IBGE localidades (meso/micro + intermediária/imediata)"},
    {"key": "producao", "file": "pa_am_crop_production.csv", "sparse": True,
     "drop": ["NM_MUN", "SIGLA_UF"], "vintage": "IBGE PAM 5457 + PEVS 289 (2023)"},
    {"key": "diversidade", "file": "pa_am_agri_diversity.csv",
     "drop": [], "vintage": "derivado: Shannon/riqueza polinizador-dependientes (valor R$)"},
    {"key": "forest_cover", "file": "pa_am_forest_cover_2023.csv",
     "drop": ["NM_MUN", "SIGLA_UF", "year"], "vintage": "MapBiomas Col9 (2023)"},
    {"key": "pobreza", "file": "pa_am_poverty_cadunico.csv",
     "drop": ["NM_MUN", "SIGLA_UF", "anomes"], "vintage": "CadÚnico/MDS"},
    {"key": "nutricao", "file": "pa_am_sisvan_panel_2023.csv",
     "drop": ["NM_MUN", "SIGLA_UF"],
     "vintage": "SISVAN/Base dos Dados (2023): crônica (stunting) + aguda (wasting)"},
    {"key": "uai", "file": "pa_am_uai.csv",
     "drop": [], "vintage": "IBGE MUNIC / Di Giulio (nacional)"},
    {"key": "clima", "file": "pa_worldclim_10m.csv",
     "drop": ["NM_MUN", "SIGLA_UF"],
     "vintage": "WorldClim 2.1 / CMIP6 SSP 2050 — delta + SD/máx intra-muni"},
]

# Columnas de identidad del banco (vienen de la malha, no cuentan como "variable").
ID_COLS = ["code_muni", "NM_MUN", "SIGLA_UF", "AREA_KM2"]


def _norm_key(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza code_muni a str de 7 dígitos (robusto ante int64 o '1300029.0')."""
    df = df.copy()
    df["code_muni"] = (
        df["code_muni"].astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.zfill(7)
    )
    return df


def build_gradient(
    estados: list[str] | None = None,
    out_name: str = "municipios_gradient_PA_AM.csv",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Cruza las capas presentes en data/processed → banco consolidado + cobertura.

    Returns
    -------
    (banco, cobertura) : ambos DataFrame. banco = gradient_v4 tidy;
    cobertura = una fila por município con conteo de variables presentes/faltantes.
    """
    estados = estados or ["PA", "AM"]

    base = load_municipalities(estados=estados)
    banco = base[ID_COLS].copy()
    banco["code_muni"] = banco["code_muni"].astype(str).str.zfill(7)

    provenance: dict[str, dict] = {}
    for layer in LAYERS:
        path = PROCESSED_DIR / layer["file"]
        if not path.exists():
            print(f"  AVISO: capa '{layer['key']}' AUSENTE ({layer['file']}) — se omite.")
            continue
        df = _norm_key(pd.read_csv(path))
        cols = [c for c in df.columns if c not in layer["drop"] and c != "code_muni"]
        dup = [c for c in cols if c in banco.columns]
        if dup:
            raise ValueError(
                f"capa '{layer['key']}': colisión de columnas {dup} con el banco. "
                f"Renombrar en el conector o añadir a 'drop'."
            )
        banco = banco.merge(df[["code_muni"] + cols], on="code_muni", how="left")
        provenance[layer["key"]] = {
            "vintage": layer["vintage"], "cols": cols,
            "sparse": layer.get("sparse", False),
        }

    data_cols = [c for c in banco.columns if c not in ID_COLS]

    # --- Relatório de cobertura ------------------------------------------- #
    cobertura = _coverage_report(banco, provenance)

    _print_summary(banco, provenance, data_cols)

    save_processed(banco, out_name, name="ORQUESTADOR")
    cov_name = out_name.replace(".csv", "_cobertura.csv")
    save_processed(cobertura, cov_name, name="COBERTURA")
    return banco, cobertura


def _coverage_report(banco: pd.DataFrame, provenance: dict) -> pd.DataFrame:
    """
    Una fila por município. Cobertura (pct_completo, capas_faltantes) se mide SOLO
    sobre capas densas — el gap real de dato. Las capas sparse (producción) se
    reportan aparte como conteo de ítems presentes, no como falta.
    """
    dense = {k: m for k, m in provenance.items() if not m["sparse"]}
    sparse = {k: m for k, m in provenance.items() if m["sparse"]}
    rows = []
    for _, r in banco.iterrows():
        capas_faltantes = []
        n_present = n_total = 0
        for key, meta in dense.items():
            cols = meta["cols"]
            n_total += len(cols)
            presentes = sum(pd.notna(r[c]) for c in cols)
            n_present += presentes
            if presentes < len(cols):
                capas_faltantes.append(key if presentes == 0 else f"{key}(parcial)")
        row = {
            "code_muni": r["code_muni"],
            "NM_MUN": r["NM_MUN"],
            "SIGLA_UF": r["SIGLA_UF"],
            "n_vars_presentes": n_present,
            "n_vars_total": n_total,
            "pct_completo": round(100 * n_present / n_total, 1) if n_total else 0.0,
            "capas_faltantes": ";".join(capas_faltantes) if capas_faltantes else "",
        }
        # Conteo de productos agrícolas presentes (informativo, no cuenta como gap).
        for key, meta in sparse.items():
            prod_cols = [c for c in meta["cols"] if c.endswith("_prod_mean")]
            row[f"n_{key}"] = int(sum(pd.notna(r[c]) for c in prod_cols))
        rows.append(row)
    return pd.DataFrame(rows)


def _print_summary(banco: pd.DataFrame, provenance: dict, data_cols: list[str]) -> None:
    print("\n" + "=" * 60)
    print(f"BANCO CONSOLIDADO — {len(banco)} municípios, "
          f"{len(data_cols)} variables de {len(provenance)} capas")
    print("=" * 60)
    dense_cols = [c for k, m in provenance.items() if not m["sparse"] for c in m["cols"]]
    print("\nCobertura por capa (municípios con TODAS sus variables):")
    for key, meta in provenance.items():
        cols = meta["cols"]
        completos = int((banco[cols].notna().all(axis=1)).sum())
        tag = " [sparse: presença/ausência esperada]" if meta["sparse"] else ""
        print(f"  {key:14s} {completos:3d}/{len(banco)}  "
              f"({len(cols)} var) — {meta['vintage']}{tag}")
    full = int((banco[dense_cols].notna().all(axis=1)).sum())
    print(f"\nMunicípios com cobertura densa completa (5 capas universais): "
          f"{full}/{len(banco)}")


if __name__ == "__main__":
    print("=" * 60)
    print("TerraCore Engine — Orquestador banco municipal Pará (v2)")
    print("=" * 60)
    banco, cobertura = build_gradient(estados=["PA"], out_name="municipios_gradient_PA.csv")
    gaps = cobertura[cobertura["capas_faltantes"] != ""]
    if len(gaps):
        print(f"\n{len(gaps)} município(s) con gaps de cobertura (primeros 10):")
        print(gaps[["NM_MUN", "SIGLA_UF", "pct_completo", "capas_faltantes"]]
              .head(10).to_string(index=False))
    else:
        print(f"\nSin gaps de cobertura: todas las capas cubren los {len(banco)} municípios.")
