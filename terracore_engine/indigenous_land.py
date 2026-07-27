"""
TerraCore Data Engine — Extensión de terras indígenas por município (FUNAI).

Completa la segunda mitad del pedido de Adrian (27/07/2026): *"determinar si hay
o no territorio indígena declarado en cada município, y determinar su extensión."*

La presencia ya salía del Censo 2022 (`population_census.extract_terras_indigenas`),
pero eso se apoya en **población censada**, no en el acto de declaración. La
extensión exige la geometría oficial, y esa es la de la FUNAI.

FUENTE
    FUNAI, GeoServer WFS, capa `Funai:tis_poligonais` (661 TI en Brasil).
    https://geoserver.funai.gov.br/geoserver/Funai/ows
    Requiere User-Agent de navegador: sin él el servidor responde 403.

POR QUÉ INTERSECCIÓN ESPACIAL Y NO EL ATRIBUTO
    La capa trae `municipio_nome`, pero una TI **cruza varios municípios** y ese
    campo no reparte el área. La única forma de saber cuántos km² de TI hay en
    cada município es intersectar los polígonos con la malha municipal.

PROYECCIÓN
    Los datos vienen en EPSG:4674 (SIRGAS 2000, geográficas) — calcular áreas ahí
    da grados cuadrados, que no significan nada. Se reproyecta a **EPSG:5880**
    (SIRGAS 2000 / Brazil Polyconic), la proyección oficial para cálculo de áreas
    en Brasil.

UNA ADVERTENCIA QUE VIAJA CON EL DATO
    `fase_ti` distingue el estado del proceso demarcatorio (Regularizada,
    Homologada, Declarada, Delimitada, Em estudo...). No es lo mismo una TI
    regularizada que uma en estudio, y meterlas en la misma bolsa borra esa
    diferencia. Se emiten las dos versiones: todas las fases y solo las que ya
    pasaron el acto de declaración.

Uso:
    from terracore_engine.indigenous_land import extract_indigenous_land
    df = extract_indigenous_land(estados=["PA"])
"""
from __future__ import annotations

import geopandas as gpd
import pandas as pd
import requests

from .base import load_municipalities, save_processed, validate_output

WFS = "https://geoserver.funai.gov.br/geoserver/Funai/ows"
LAYER = "Funai:tis_poligonais"
UA = {"User-Agent": "Mozilla/5.0"}
CRS_AREA = 5880          # SIRGAS 2000 / Brazil Polyconic — áreas en metros
CRS_DATA = 4674          # SIRGAS 2000 geográficas

# Fases en las que la TI ya pasó por el acto formal de declaración.
FASES_DECLARADAS = ["Regularizada", "Homologada", "Declarada"]


def _fetch_tis(uf: str, timeout: int = 600) -> gpd.GeoDataFrame:
    """Baja las TI que tocan un estado. El filtro por atributo es solo para no
    traer las 661 de Brasil; el reparto de área lo hace la intersección."""
    params = {
        "service": "WFS", "version": "1.0.0", "request": "GetFeature",
        "typeName": LAYER, "outputFormat": "application/json",
        "CQL_FILTER": f"uf_sigla LIKE '%{uf}%'",
    }
    r = requests.get(WFS, params=params, headers=UA, timeout=timeout)
    r.raise_for_status()
    gdf = gpd.GeoDataFrame.from_features(r.json()["features"], crs=f"EPSG:{CRS_DATA}")
    if gdf.empty:
        raise RuntimeError(f"FUNAI no devolvió terras indígenas para {uf}")
    return gdf


def extract_indigenous_land(estados: list[str] | None = None,
                            save: bool = True) -> pd.DataFrame:
    estados = estados or ["PA"]
    tis = pd.concat([_fetch_tis(uf) for uf in estados], ignore_index=True)
    tis = gpd.GeoDataFrame(tis, geometry="geometry", crs=f"EPSG:{CRS_DATA}")
    print(f"  FUNAI: {len(tis)} terras indígenas que tocan {', '.join(estados)}")
    print(f"  fases: {tis['fase_ti'].value_counts().to_dict()}")

    munis = load_municipalities(estados=estados).to_crs(epsg=CRS_AREA)
    munis["code_muni"] = munis["code_muni"].astype(int)
    munis["area_muni_km2"] = munis.geometry.area / 1e6

    def _area_por_muni(sub: gpd.GeoDataFrame, sufijo: str) -> pd.DataFrame:
        if sub.empty:
            return pd.DataFrame(columns=["code_muni", f"area_ti_km2{sufijo}"])
        # `union_all` disuelve solapamientos entre TI antes de intersectar: sin
        # esto, dos polígonos superpuestos contarían el área dos veces.
        disuelto = gpd.GeoDataFrame(
            geometry=[sub.to_crs(epsg=CRS_AREA).union_all()], crs=f"EPSG:{CRS_AREA}")
        inter = gpd.overlay(munis[["code_muni", "geometry"]], disuelto,
                            how="intersection")
        if inter.empty:
            return pd.DataFrame(columns=["code_muni", f"area_ti_km2{sufijo}"])
        inter[f"area_ti_km2{sufijo}"] = inter.geometry.area / 1e6
        return (inter.groupby("code_muni")[f"area_ti_km2{sufijo}"]
                .sum().reset_index())

    todas = _area_por_muni(tis, "")
    declaradas = _area_por_muni(tis[tis["fase_ti"].isin(FASES_DECLARADAS)], "_declaradas")

    out = (munis[["code_muni", "area_muni_km2"]].copy()
           .merge(todas, on="code_muni", how="left")
           .merge(declaradas, on="code_muni", how="left"))
    for c in ["area_ti_km2", "area_ti_km2_declaradas"]:
        out[c] = out[c].fillna(0.0).round(3)

    out["pct_area_ti"] = (100 * out["area_ti_km2"]
                          / out["area_muni_km2"]).round(3)
    out["pct_area_ti_declaradas"] = (100 * out["area_ti_km2_declaradas"]
                                     / out["area_muni_km2"]).round(3)
    out["tem_ti_funai"] = (out["area_ti_km2"] > 0).astype(int)
    out["area_muni_km2"] = out["area_muni_km2"].round(3)

    validate_output(out,
                    expected_cols=["code_muni", "area_ti_km2", "pct_area_ti",
                                   "tem_ti_funai"],
                    n_expected=len(munis), name="indigenous_land")
    if save:
        save_processed(out, f"{'_'.join(e.lower() for e in estados)}_indigenous_land.csv")
    return out


if __name__ == "__main__":
    df = extract_indigenous_land(estados=["PA"])
    print(f"\nmunicípios con TI (FUNAI): {int(df.tem_ti_funai.sum())}/{len(df)}")
    print(df[["area_ti_km2", "pct_area_ti", "pct_area_ti_declaradas"]]
          .describe().round(2).to_string())
