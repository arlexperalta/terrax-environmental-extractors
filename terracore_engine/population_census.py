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


if __name__ == "__main__":
    df = extract_population_race(estados=["PA"])
    print(df[["code_muni", "pop_total_censo2022",
              "prop_branca", "prop_indigena"]].describe())
