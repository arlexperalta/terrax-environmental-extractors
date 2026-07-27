"""
TerraCore Data Engine — Peso económico y agrícola de los perennes polinizador-dependientes.

Pedido de Adrian (27/07/2026):

    "Quisiera adicionar una etapa acá, y es la de calcular el PIB agrícola para
    cada municipio, y agregar una variable de vulnerabilidad agrícola en función
    de cuánto el PIB del municipio depende de los cultivos dependientes de
    polinizadores:  100 × valor de producción de cultivos dependientes de
    polinizadores / PIB total.  (...) Otra solicitud es calcular la producción
    agrícola total del municipio, y hacer la misma estimación referente a la
    producción de los cultivos perennes."

Da las dos variables que pidió:
  1. `importancia_economica_perenes` — cuánto de la economía del município depende
     de açaí + cacau + castanha.
  2. `importancia_agricola_perenes`  — cuánto de su agricultura son esos cultivos.

La diferencia entre las dos importa: un município puede tener una agricultura
enteramente de perennes (importancia agrícola alta) y aun así una economía que no
depende de ella (importancia económica baja) porque vive de servicios o minería.

FUENTES Y AÑO — la restricción manda:
  - PIB municipal: SIDRA **5938**. `v37` = PIB total; `v513` = valor adicionado
    bruto da agropecuária. **v513 solo está divulgado hasta 2021** (2022 y 2023
    devuelven "..."), así que 2021 es el año de referencia de todo este módulo.
  - Producción: SIDRA **5457** (lavouras, PAM) + **289** (extractiva, PEVS),
    `v215`/`v144` = valor da produção, **del mismo 2021** para que numerador y
    denominador sean del mismo período. Esto es distinto de `crop_production.py`,
    que promedia 3 años para suavizar safras: aquí la coherencia temporal del
    cociente pesa más que la suavidad.

Emite además el **catálogo completo de cultivos del estado** con su valor, que es
el insumo para clasificarlos por dependencia de polinizadores.

Uso:
    from terracore_engine.economic_weight import extract_economic_weight
    df, catalogo = extract_economic_weight(estados=["PA"])
"""
from __future__ import annotations

import pandas as pd
import requests

from .base import load_municipalities, save_processed, validate_output

SIDRA = "https://apisidra.ibge.gov.br/values"
ANO = "2021"
UF_COD = {"PA": "15", "AM": "13", "SP": "35"}

# Las 3 espécies foco del assessment, tal como aparecen en las tablas del IBGE.
# açaí vive en las dos: PAM (cultivado) y PEVS (extrativo). Se suman, como ya
# hace `agri_diversity.py`: misma espécie, mismo polinizador.
PERENES_PAM = ["Açaí", "Cacau (em amêndoa)"]

# CUIDADO — PEVS es JERÁRQUICA: "1 - Alimentícios" es un subtotal que CONTIENE a
# "1.1 - Açaí (fruto)" y "1.3 - Castanha-do-pará". Sumar los dos niveles duplica.
# Aquí se usan siempre las hojas (patrón "N.N - ") y el grupo solo como total.
PERENES_FOCO_PEVS = ["1.1 - Açaí (fruto)", "1.3 - Castanha-do-pará"]

# Grupo de extractivos ALIMENTICIOS. Es el único que entra al total agrícola:
# el grupo 7 (madeira em tora, carvão, lenha) es extractivismo forestal, no
# producción agrícola, y en Pará es justamente el de mayor valor — meterlo
# infla el denominador y desvirtúa el cociente que Adrian pidió.
PEVS_GRUPO_ALIMENTICIO = "1 - Alimentícios"
PEVS_GRUPO_MADEIREIRO = ["7.1 - Carvão vegetal", "7.2 - Lenha", "7.3 - Madeira em tora"]


def _fetch(table: str, variable: str, uf: str, classif: str,
           timeout: int = 300) -> pd.DataFrame:
    cod = UF_COD[uf]
    url = (f"{SIDRA}/t/{table}/n6/in%20n3%20{cod}"
           f"/v/{variable}/p/{ANO}/{classif}/h/y")
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    raw = r.json()
    if len(raw) < 2:
        raise RuntimeError(f"SIDRA vacío: tabla {table}, uf {uf}")
    df = pd.DataFrame(raw[1:])
    df["code_muni"] = df["D1C"].astype(int)
    # "...", "-", ".." = no divulgado / inexistente. No son ceros.
    df["valor"] = pd.to_numeric(df["V"], errors="coerce")
    return df


def _produccion_por_producto(uf: str) -> pd.DataFrame:
    """PAM + PEVS en formato largo: (code_muni, fuente, produto, valor R$ mil)."""
    partes = []
    # PAM 5457: v215 = valor da produção, classificação c782 (produto das lavouras).
    # PEVS  289: v145 = valor da produção (144 é quantidade), classificação c193.
    for table, var, col_prod in [("5457", "215", "D4N"), ("289", "145", "D4N")]:
        d = _fetch(table, var, uf, "c782/all" if table == "5457" else "c193/all")
        d = d.rename(columns={col_prod: "produto"})
        d["fonte"] = "PAM" if table == "5457" else "PEVS"
        partes.append(d[["code_muni", "fonte", "produto", "valor"]])
    return pd.concat(partes, ignore_index=True)


def extract_economic_weight(estados: list[str] | None = None,
                            save: bool = True) -> tuple[pd.DataFrame, pd.DataFrame]:
    estados = estados or ["PA"]

    # --- PIB ---------------------------------------------------------------
    pib = pd.concat([_fetch("5938", "37,513", uf, "") for uf in estados],
                    ignore_index=True)
    pib_w = (pib.pivot_table(index="code_muni", columns="D2C",
                             values="valor", aggfunc="first").reset_index())
    pib_w.columns.name = None
    pib_w = pib_w.rename(columns={"37": "pib_total", "513": "vab_agropecuaria"})

    # --- Producción por producto ------------------------------------------
    prod = pd.concat([_produccion_por_producto(uf) for uf in estados],
                     ignore_index=True)

    es_total = prod["produto"].str.strip().str.lower() == "total"

    # Total agrícola = lavouras (PAM, su fila "Total") + extractivos ALIMENTICIOS
    # (PEVS, grupo 1). Sin doble conteo: se toma el subtotal del grupo, no sus hojas.
    pam_total = (prod[es_total & (prod["fonte"] == "PAM")]
                 .groupby("code_muni")["valor"].sum())
    pevs_alim = (prod[prod["produto"] == PEVS_GRUPO_ALIMENTICIO]
                 .groupby("code_muni")["valor"].sum())
    total_agri = (pam_total.add(pevs_alim, fill_value=0)
                  .rename("valor_agricola_total"))

    # Informativo, no entra al cociente: cuánto pesa el extractivismo maderero.
    madeireiro = (prod[prod["produto"].isin(PEVS_GRUPO_MADEIREIRO)]
                  .groupby("code_muni")["valor"].sum()
                  .rename("valor_extrativo_madeireiro"))

    perenes = prod[(~es_total) & (
        ((prod["fonte"] == "PAM") & (prod["produto"].isin(PERENES_PAM)))
        | ((prod["fonte"] == "PEVS") & (prod["produto"].isin(PERENES_FOCO_PEVS))))]
    valor_perenes = (perenes.groupby("code_muni")["valor"].sum()
                     .rename("valor_perenes_polinizador"))

    out = (pib_w.merge(total_agri, on="code_muni", how="outer")
                .merge(valor_perenes, on="code_muni", how="outer")
                .merge(madeireiro, on="code_muni", how="outer"))

    # --- Las dos variables pedidas ----------------------------------------
    # En %, como él las escribió. Denominador 0 o ausente -> NaN, no infinito.
    #
    # ADVERTENCIA METODOLÓGICA: el numerador es VALOR BRUTO de producción y el
    # PIB es VALOR AGREGADO (descuenta el consumo intermedio). No son la misma
    # magnitud, así que el cociente **puede superar el 100%** en municípios
    # pequeños y muy agrícolas, y de hecho lo hace. Se calcula tal como Adrian
    # lo pidió, y al lado va `importancia_no_vab_agro`, que compara contra el VAB
    # agropecuário — magnitudes homogéneas, cociente interpretable como fracción.
    out["importancia_economica_perenes"] = (
        100.0 * out["valor_perenes_polinizador"]
        / out["pib_total"].replace(0, pd.NA)).astype(float).round(4)
    out["importancia_no_vab_agro"] = (
        100.0 * out["valor_perenes_polinizador"]
        / out["vab_agropecuaria"].replace(0, pd.NA)).astype(float).round(4)
    out["importancia_agricola_perenes"] = (
        100.0 * out["valor_perenes_polinizador"]
        / out["valor_agricola_total"].replace(0, pd.NA)).astype(float).round(4)
    # Complemento útil: cuánto pesa toda la agropecuária en la economía.
    out["part_agropecuaria_pib"] = (
        100.0 * out["vab_agropecuaria"]
        / out["pib_total"].replace(0, pd.NA)).astype(float).round(4)

    munis = pd.DataFrame(load_municipalities(estados=estados))[["code_muni"]]
    munis["code_muni"] = munis["code_muni"].astype(int)
    out = munis.merge(out, on="code_muni", how="left")

    # --- Catálogo de cultivos del estado ----------------------------------
    catalogo = (prod[~es_total].groupby(["fonte", "produto"])
                .agg(municipios_com_producao=("valor", lambda s: int(s.notna().sum())),
                     valor_total_mil_reais=("valor", "sum"))
                .reset_index()
                .sort_values("valor_total_mil_reais", ascending=False))

    validate_output(out,
                    expected_cols=["code_muni", "pib_total", "valor_agricola_total",
                                   "importancia_economica_perenes",
                                   "importancia_agricola_perenes"],
                    n_expected=len(munis), name="economic_weight")
    if save:
        tag = "_".join(e.lower() for e in estados)
        save_processed(out, f"{tag}_economic_weight_{ANO}.csv")
        save_processed(catalogo, f"{tag}_catalogo_cultivos_{ANO}.csv")
        # Largo por município: insumo para cruzar con la clasificación de
        # dependencia de polinizadores y medir qué fracción del valor agrícola
        # de CADA município está en juego.
        save_processed(prod, f"{tag}_produccion_por_producto_{ANO}.csv")
    return out, catalogo


if __name__ == "__main__":
    df, cat = extract_economic_weight(estados=["PA"])
    print("\n--- importancia (%) ---")
    print(df[["importancia_economica_perenes", "importancia_agricola_perenes",
              "part_agropecuaria_pib"]].describe().round(2))
    print(f"\n--- catálogo: {len(cat)} produtos ---")
    print(cat.head(15).to_string(index=False))
