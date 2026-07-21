"""
TerraCore Data Engine — Diversidade agrícola de cultivos dependientes de polinizadores.

Capa DERIVADA (no extrae de fuente externa: computa sobre `crop_production`). Produce
las variables respuesta del assessment v2 de Pará (reencuadre de Adrian, 2026-07-21):
la vulnerabilidad alimentaria se lee, entre otras, por cuán diversa es la canasta de
cultivos dependientes de polinizadores de cada município.

Espécies foco (3), dependientes de polinizadores:
  - açaí (Euterpe oleracea) — cultivado + extrativo SUMADOS (es la misma espécie /
    mismo polinizador; el sistema plantado vs extractivo se mantiene aparte en el banco).
  - cacau (Theobroma cacao)
  - castanha-do-pará (Bertholletia excelsa)

Abundancia = **valor da produção (R$)**, única unidad común a PAM (cultivo) y PEVS
(extração): el área plantada solo existe para las cultivadas, y el volumen (t) sesga
hacia el açaí. Los NaN se cuentan como **0** (Adrian: ausência de produção = 0) — un
município que solo produce una espécie es *menos* diverso, y eso debe reflejarse.

Índices (Shannon es un índice reconocido, se justifica; el resto son conteos reales):
  - shannon_polinizador   — H = -Σ pᵢ·ln(pᵢ) sobre el valor de las 3 espécies.
  - riqueza_polinizador   — nº de espécies con valor > 0 (0–3).
  - pielou_polinizador    — evenness H / ln(riqueza) (NaN si riqueza < 2).
  - valor_polinizador_total — suma R$ (proxy de dependencia económica del município).

Uso:
    from terracore_engine.agri_diversity import extract_agri_diversity
    df = extract_agri_diversity()   # lee pa_am_crop_production.csv del motor
"""
from __future__ import annotations

import math

import pandas as pd

from .base import PROCESSED_DIR, validate_output, save_processed

# Espécie foco → columnas de valor (R$) que la componen. El açaí suma sus dos sistemas.
SPECIES_VALUE_COLS = {
    "acai": ["acai_cultivo_valor_mean", "acai_extrativo_valor_mean"],
    "cacau": ["cacau_valor_mean"],
    "castanha": ["castanha_para_valor_mean"],
}


def _shannon(abundances: list[float]) -> float:
    """Índice de Shannon sobre abundancias absolutas. 0 si el total es 0."""
    total = sum(abundances)
    if total <= 0:
        return 0.0
    h = 0.0
    for a in abundances:
        if a > 0:
            p = a / total
            h -= p * math.log(p)
    return h


def compute_agri_diversity(crop_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula las variables respuesta de diversidade sobre el banco de producción.
    Los NaN de valor se tratan como 0 (ausência de produção).
    """
    df = crop_df.copy()
    df["code_muni"] = df["code_muni"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(7)

    # Valor por espécie foco (suma de sus columnas, NaN → 0).
    species_val = {}
    for sp, cols in SPECIES_VALUE_COLS.items():
        present = [c for c in cols if c in df.columns]
        if not present:
            raise ValueError(f"Faltan columnas de valor para '{sp}': {cols}")
        species_val[sp] = df[present].fillna(0.0).sum(axis=1)

    val = pd.DataFrame(species_val)                    # una columna por espécie
    out = pd.DataFrame({"code_muni": df["code_muni"]})
    out["valor_polinizador_total"] = val.sum(axis=1).round(2)
    out["riqueza_polinizador"] = (val > 0).sum(axis=1).astype("Int64")
    out["shannon_polinizador"] = val.apply(lambda r: _shannon(list(r)), axis=1).round(4)
    # Nota: la evenness de Pielou (H / ln riqueza) NO se emite al banco — es NaN por
    # diseño con < 2 espécies (indefinida) y ensuciaría la cobertura. Se deriva de
    # shannon + riqueza en el análisis si se necesita.
    return out


def extract_agri_diversity(
    crop_file: str = "pa_am_crop_production.csv",
) -> pd.DataFrame:
    """Lee el banco de producción del motor, computa la diversidade y la guarda."""
    path = PROCESSED_DIR / crop_file
    if not path.exists():
        raise FileNotFoundError(
            f"No está {path}. Corre crop_production primero (extract_crop_production)."
        )
    crop_df = pd.read_csv(path)
    out = compute_agri_diversity(crop_df)

    validate_output(
        out,
        expected_cols=["code_muni", "shannon_polinizador", "riqueza_polinizador",
                       "valor_polinizador_total"],
        n_expected=len(out), name="AgriDiversity",
    )
    save_processed(out, "pa_am_agri_diversity.csv", name="AgriDiversity")
    return out


if __name__ == "__main__":
    print("=" * 60)
    print("TerraCore Engine — Diversidade agrícola (açaí + cacau + castanha)")
    print("=" * 60)
    df = extract_agri_diversity()
    print(f"\n✓ {len(df)} municípios")
    print("\nDistribución de riqueza (nº de espécies foco presentes):")
    print(df["riqueza_polinizador"].value_counts().sort_index().to_string())
    print(f"\nShannon — mediana {df['shannon_polinizador'].median():.3f}, "
          f"máx {df['shannon_polinizador'].max():.3f} (tope teórico ln 3 = {math.log(3):.3f})")
    print("\nTop 5 municípios más diversos:")
    top = df.nlargest(5, "shannon_polinizador")
    print(top[["code_muni", "riqueza_polinizador", "shannon_polinizador",
               "valor_polinizador_total"]].to_string(index=False))
