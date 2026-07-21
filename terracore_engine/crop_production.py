"""
TerraCore Data Engine — Producción agrícola municipal (PAM + PEVS).

Extrae quantidade produzida y valor da produção por município para cultivos
dependientes de polinizadores, desde el IBGE vía SIDRA. Generaliza el viejo
`download_ibge_pam.py` (solo café) a un catálogo multi-cultivo y multi-tabla.

Sutileza de fuente (IBGE): no todos los "cultivos" viven en la misma tabla.
  - Lavouras permanentes  → PAM   tabla 5457 (cacau, açaí cultivado, ...).
  - Produção extrativa     → PEVS  tabla 289  (castanha-do-pará, açaí extrativo).
Castanha-do-pará (Bertholletia excelsa) NO está en PAM: es extrativa (PEVS).
Açaí aparece en AMBAS (cultivado vs extrativo) — se traen separadas y se
decide aguas abajo si se suman (decisión de Adrian, no del motor).

Salida: tidy, una fila por município (`code_muni`), columnas
`<cultivo>_prod_mean` (toneladas) y `<cultivo>_valor_mean` (mil reais),
promediadas sobre los últimos N años (suaviza anomalías de safra).

Uso:
    from terracore_engine.crop_production import extract_crop_production
    df = extract_crop_production(estados=["PA", "AM"], n_years=3)
"""
from __future__ import annotations

import time

import pandas as pd
import requests

from .base import load_municipalities, validate_output, save_processed

# Pedimos SOLO los municípios del scope (n6/<códigos>), no `n6/all` (todo Brasil):
# bajar los 5.570 munis eran ~5.6 MB por llamada → resets/IncompleteRead. Con 206
# códigos el payload cae ~27× y la descarga es estable.
SIDRA_VALUES = "https://apisidra.ibge.gov.br/values/t/{t}/n6/{munis}/v/{v}/p/{p}/{cl}/{prod}"
PERIODS_URL = "https://servicodados.ibge.gov.br/api/v3/agregados/{t}/periodos"

# Catálogo de cultivos dependientes de polinizadores para el assessment PA/AM.
# var_qty/var_val/var_area = códigos de variável SIDRA por tabla.
#   PAM 5457:  214 = quantidade (t), 215 = valor (mil R$), 8331 = área plantada (ha)
#   PEVS 289:  144 = quantidade,     145 = valor (mil R$)  — extração NÃO tem área
# El área plantada solo existe para las lavouras permanentes (PAM): la extração
# vegetal (PEVS: castanha, açaí extrativo) es coleta na floresta, sin hectare plantado.
CROPS = [
    {"crop": "cacau",           "source": "PAM",  "table": "5457", "classif": "c782",
     "product": "40138", "var_qty": "214", "var_val": "215", "var_area": "8331"},   # Cacau (em amêndoa)
    {"crop": "acai_cultivo",    "source": "PAM",  "table": "5457", "classif": "c782",
     "product": "45982", "var_qty": "214", "var_val": "215", "var_area": "8331"},   # Açaí (lavoura permanente)
    {"crop": "castanha_para",   "source": "PEVS", "table": "289", "classif": "c193",
     "product": "3405", "var_qty": "144", "var_val": "145"},    # Castanha-do-pará
    {"crop": "acai_extrativo",  "source": "PEVS", "table": "289", "classif": "c193",
     "product": "3403", "var_qty": "144", "var_val": "145"},    # Açaí (fruto), extrativo
]

_NULL_TOKENS = {"...", "..", "-", "X", ""}


def _last_n_periods(table: str, n: int) -> list[str]:
    """Últimos n años disponibles de la tabla (robusto ante actualizaciones)."""
    r = requests.get(PERIODS_URL.format(t=table), timeout=60)
    r.raise_for_status()
    periods = [str(p["id"]) for p in r.json()]
    return periods[-n:]


def _parse_sidra(records: list[dict], value_name: str) -> pd.DataFrame:
    """
    Parsea la respuesta SIDRA a [code_muni, year, value]. Detecta la columna del
    código municipal (7 dígitos) en vez de asumir 'D1C', y limpia tokens nulos.
    """
    rows = records[1:]  # [0] = header con etiquetas
    if not rows:
        return pd.DataFrame(columns=["code_muni", "year", value_name])

    df = pd.DataFrame(rows)

    # Columna del código de município: la que trae códigos de 7 dígitos.
    code_col = None
    for col in df.columns:
        sample = df[col].astype(str).str.strip()
        if sample.str.match(r"^\d{7}$").mean() > 0.5:
            code_col = col
            break
    if code_col is None:
        raise ValueError(f"No hallé columna de código municipal. Columnas: {list(df.columns)}")

    # Año: columna con valores de 4 dígitos tipo 19xx/20xx.
    year_col = None
    for col in df.columns:
        if col == code_col:
            continue
        sample = df[col].astype(str).str.strip()
        if sample.str.match(r"^(19|20)\d{2}$").mean() > 0.5:
            year_col = col
            break

    out = pd.DataFrame({
        "code_muni": df[code_col].astype(str).str.strip(),
        "year": pd.to_numeric(df[year_col], errors="coerce") if year_col else pd.NA,
        value_name: pd.to_numeric(
            df["V"].astype(str).str.strip().replace(list(_NULL_TOKENS), pd.NA),
            errors="coerce",
        ),
    })
    return out[out["code_muni"].str.match(r"^\d{7}$")]


def _fetch(table: str, var: str, classif: str, product: str, periods: list[str],
           value_name: str, munis: str, retries: int = 3) -> pd.DataFrame:
    # Reintenta con backoff ante errores de red transitorios (reset, timeout,
    # descarga incompleta). No es loop ciego: solo errores de transporte, no HTTP 4xx/5xx.
    url = SIDRA_VALUES.format(t=table, v=var, p=",".join(periods), cl=classif,
                             prod=product, munis=munis)
    transient = (requests.ConnectionError, requests.Timeout,
                 requests.exceptions.ChunkedEncodingError)
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=300)
            r.raise_for_status()
            return _parse_sidra(r.json(), value_name)
        except transient as e:
            if attempt == retries:
                raise
            wait = 5 * attempt
            print(f"    reintento {attempt}/{retries - 1} tras {type(e).__name__} "
                  f"(espera {wait}s)...")
            time.sleep(wait)


def extract_crop_production(
    estados: list[str] | None = None,
    n_years: int = 3,
) -> pd.DataFrame:
    """
    Extrae producción y valor por município para los cultivos de `CROPS`,
    promediados sobre los últimos `n_years` años, filtrado a `estados`.

    Returns
    -------
    DataFrame tidy: code_muni + <crop>_prod_mean + <crop>_valor_mean por cultivo.
    """
    estados = estados or ["PA", "AM"]
    munis = load_municipalities(estados=estados)
    valid = set(munis["code_muni"])
    muni_codes = ",".join(sorted(valid))            # n6 explícito: solo el scope, no all
    result = munis[["code_muni", "NM_MUN", "SIGLA_UF"]].copy()

    expected = ["code_muni"]
    for crop in CROPS:
        periods = _last_n_periods(crop["table"], n_years)
        print(f"  {crop['crop']} ({crop['source']} t{crop['table']}, "
              f"prod {periods[0]}-{periods[-1]})...")
        kinds = [("prod", crop["var_qty"]), ("valor", crop["var_val"])]
        if crop.get("var_area"):                     # área plantada: solo PAM (permanentes)
            kinds.append(("area", crop["var_area"]))
        for kind, var in kinds:
            colname = f"{crop['crop']}_{kind}_mean"
            raw = _fetch(crop["table"], var, crop["classif"], crop["product"],
                         periods, colname, munis=muni_codes)
            raw = raw[raw["code_muni"].isin(valid)]
            agg = raw.groupby("code_muni")[colname].mean()
            result[colname] = result["code_muni"].map(agg)
            expected.append(colname)
            time.sleep(0.3)

    validate_output(result, expected_cols=expected, n_expected=len(munis),
                    name="CropProduction")
    save_processed(result, "pa_am_crop_production.csv", name="CropProduction")
    return result


if __name__ == "__main__":
    print("=" * 60)
    print("TerraCore Engine — Producción agrícola PA/AM (PAM + PEVS)")
    print("=" * 60)
    df = extract_crop_production(estados=["PA", "AM"], n_years=3)
    print(f"\n✓ {len(df)} municipios × {len(df.columns) - 3} variables de producción")
