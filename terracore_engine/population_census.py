"""
TerraCore Data Engine — Composición de la población por cor ou raça (Censo 2022).

Pedido de Adrian (27/07/2026): *"Considerando que en el norte de Brasil la
población indígena es mucho mayor, sería interesante conseguir dos variables
nuevas: proporción de población indígena y proporción de población blanca."*

Fuente: IBGE, Censo Demográfico 2022, vía SIDRA tabla **9605** (População
residente, por cor ou raça). Variable 93 = população residente. La clasificación
es **autodeclarada**, que es lo que el Censo pregunta — no es una medición
externa, y conviene decirlo cuando se interprete.

Salida: una fila por município, con conteos y proporciones sobre el total:
  - `pop_total_censo2022`
  - `pop_branca`, `pop_indigena`, `pop_preta`, `pop_parda`, `pop_amarela`
  - `prop_branca`, `prop_indigena`  (0–1, las dos que pidió)
  - `prop_preta`, `prop_parda`      (van gratis en la misma consulta)

Nota sobre la población indígena: el Censo 2022 cambió la forma de captarla
respecto de 2010 (identificación ampliada fuera de tierras indígenas), así que
las series 2010 y 2022 **no son directamente comparables**. Para un corte
transversal como el nuestro no es problema; para una tendencia sí lo sería.

Uso:
    from terracore_engine.population_census import extract_population_race
    df = extract_population_race(estados=["PA"])
"""
from __future__ import annotations

import io

import pandas as pd
import requests

from .base import load_municipalities, save_processed, validate_output

SIDRA = "https://apisidra.ibge.gov.br/values"
TABELA = "9605"          # População residente por cor ou raça (Censo 2022)
VARIAVEL = "93"          # População residente
ANO = "2022"

UF_COD = {"PA": "15", "AM": "13", "SP": "35"}

# Códigos de la clasificação c86 (Cor ou raça) en SIDRA.
CATEGORIAS = {
    "95251": "total",
    "2776": "branca",
    "2777": "preta",
    "2778": "amarela",
    "2779": "parda",
    "2780": "indigena",
}


def _fetch_uf(uf: str, timeout: int = 180) -> pd.DataFrame:
    """Baja la tabla completa de un estado. `in n3 <uf>` = todos sus municípios."""
    cod = UF_COD.get(uf)
    if cod is None:
        raise ValueError(f"UF sin código mapeado: {uf}")
    url = (f"{SIDRA}/t/{TABELA}/n6/in%20n3%20{cod}"
           f"/v/{VARIAVEL}/p/{ANO}/c86/all/h/y")
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    raw = r.json()
    if len(raw) < 2:
        raise RuntimeError(f"SIDRA devolvió una respuesta vacía para {uf}")
    return pd.DataFrame(raw[1:])          # la fila 0 son los rótulos


def extract_population_race(estados: list[str] | None = None,
                            save: bool = True) -> pd.DataFrame:
    estados = estados or ["PA"]
    partes = [_fetch_uf(uf) for uf in estados]
    raw = pd.concat(partes, ignore_index=True)

    raw["code_muni"] = raw["D1C"].astype(int)
    raw["categoria"] = raw["D4C"].map(CATEGORIAS)
    # "-" y ".." son los códigos de SIDRA para dato inexistente o no divulgado.
    raw["valor"] = pd.to_numeric(raw["V"], errors="coerce")
    raw = raw[raw["categoria"].notna()]

    wide = (raw.pivot_table(index="code_muni", columns="categoria",
                            values="valor", aggfunc="sum")
               .reset_index())
    wide.columns.name = None

    total = wide["total"].replace(0, pd.NA)
    out = pd.DataFrame({"code_muni": wide["code_muni"]})
    out["pop_total_censo2022"] = wide["total"]
    for cat in ["branca", "preta", "amarela", "parda", "indigena"]:
        if cat not in wide:
            continue
        out[f"pop_{cat}"] = wide[cat]
        # Proporción 0–1, coherente con el resto del banco (no porcentaje).
        out[f"prop_{cat}"] = (wide[cat] / total).astype(float).round(6)

    # Reancla a la malha para que el scope sea el mismo del motor y se vea si
    # algún município del banco quedó sin dato.
    munis = pd.DataFrame(load_municipalities(estados=estados))[["code_muni"]]
    munis["code_muni"] = munis["code_muni"].astype(int)
    out = munis.merge(out, on="code_muni", how="left")

    validate_output(out,
                    expected_cols=["code_muni", "pop_total_censo2022",
                                   "prop_branca", "prop_indigena"],
                    n_expected=len(munis), name="population_race")
    if save:
        save_processed(out, f"{'_'.join(e.lower() for e in estados)}_population_race.csv")
    return out


TABELA_TI = "9719"   # População residente em terras indígenas (Censo 2022)


def extract_terras_indigenas(estados: list[str] | None = None,
                             save: bool = True) -> pd.DataFrame:
    """
    Presencia de terras indígenas por município, vía población residente en ellas.

    Adrian pidió (27/07/2026): *"determinar si hay o no territorio indígena
    declarado en cada município, y determinar su extensión."*

    **Esto resuelve la primera mitad, no la segunda.** La tabla 9719 del Censo
    2022 da población residente en terras indígenas y admite nivel município
    (N6), así que sirve para saber si hay TI y cuánta gente vive en ella. La
    **extensión en km²** es geometría: exige el shapefile de la FUNAI cruzado con
    la malha municipal, y eso no sale de SIDRA.

    Un município puede tener TI declarada con población muy baja, así que
    `tem_terra_indigena` es un piso, no la verdad cartográfica.
    """
    estados = estados or ["PA"]
    partes = []
    for uf in estados:
        cod = UF_COD[uf]
        url = (f"{SIDRA}/t/{TABELA_TI}/n6/in%20n3%20{cod}"
               f"/v/allxp/p/{ANO}/h/y")
        r = requests.get(url, timeout=300)
        r.raise_for_status()
        raw = r.json()
        if len(raw) > 1:
            partes.append(pd.DataFrame(raw[1:]))
    if not partes:
        raise RuntimeError("SIDRA no devolvió datos de terras indígenas")

    raw = pd.concat(partes, ignore_index=True)
    raw["code_muni"] = raw["D1C"].astype(int)
    raw["valor"] = pd.to_numeric(raw["V"], errors="coerce")

    # Se toma el máximo por município: las variables de la tabla son cortes de
    # la misma población (total / indígena / por quesito), no sumandos.
    pop_ti = (raw.groupby("code_muni")["valor"].max()
              .rename("pop_em_terra_indigena").reset_index())

    munis = pd.DataFrame(load_municipalities(estados=estados))[["code_muni"]]
    munis["code_muni"] = munis["code_muni"].astype(int)
    out = munis.merge(pop_ti, on="code_muni", how="left")
    # Sin dato en esta tabla = el município no tiene TI con población censada.
    out["pop_em_terra_indigena"] = out["pop_em_terra_indigena"].fillna(0)
    out["tem_terra_indigena"] = (out["pop_em_terra_indigena"] > 0).astype(int)

    validate_output(out,
                    expected_cols=["code_muni", "pop_em_terra_indigena",
                                   "tem_terra_indigena"],
                    n_expected=len(munis), name="terras_indigenas")
    if save:
        save_processed(out, f"{'_'.join(e.lower() for e in estados)}_terras_indigenas.csv")
    return out


if __name__ == "__main__":
    df = extract_population_race(estados=["PA"])
    print(df[["code_muni", "pop_total_censo2022",
              "prop_branca", "prop_indigena"]].describe())
    ti = extract_terras_indigenas(estados=["PA"])
    print(f"\nmunicípios con terra indígena: {ti.tem_terra_indigena.sum()}/{len(ti)}")
