"""
TerraCore Data Engine — base layer.

Núcleo compartido por todos los conectores del motor: carga de la malha
municipal, validación de salida y guardado estandarizado. Los módulos de
extracción (landscape_metrics, ibge_census, ...) importan desde aquí:

    from .base import load_municipalities, validate_output, save_processed

Diseño:
  - Llave canónica = `code_muni` (código IBGE de 7 dígitos, string). Se conserva
    `CD_MUN` como alias para compatibilidad con conectores existentes.
  - La malha viene de la shapefile IBGE 2022 ya versionada en el monorepo
    (no requiere geobr ni descarga). Override con env var TERRACORE_ROOT.
  - Un solo lugar decide dónde se leen las geometrías y dónde se guardan los
    datasets procesados, para que ningún conector hardcodee rutas.
"""
from __future__ import annotations

import os
from pathlib import Path

import geopandas as gpd
import pandas as pd

# --------------------------------------------------------------------------- #
# Paths
# --------------------------------------------------------------------------- #
# El motor es un paquete instalable; la malha (dato de proyecto) NO viaja con él.
# Se resuelve así, en orden: (1) env TERRACORE_ROOT, (2) búsqueda hacia arriba del
# marcador de la malha desde este archivo. Falla claro si no la encuentra.
_MALHA_REL = Path("terracore/data/raw/shapefiles/BR_Municipios_2022/BR_Municipios_2022.shp")

# Repo del propio motor (para outputs self-contained): parents[1] = environmental-extractors.
ENGINE_REPO = Path(__file__).resolve().parents[1]


def _resolve_root() -> Path | None:
    """Ubica el root del monorepo (donde vive la malha). Env override o walk-up."""
    env = os.environ.get("TERRACORE_ROOT")
    if env:
        return Path(env)
    for parent in Path(__file__).resolve().parents:
        if (parent / _MALHA_REL).exists():
            return parent
    return None


REPO_ROOT = _resolve_root()
MALHA_BR_2022 = (REPO_ROOT / _MALHA_REL) if REPO_ROOT else None

# Directorio de salida de datasets procesados del motor (self-contained en el repo del motor).
PROCESSED_DIR = ENGINE_REPO / "data/processed"

# Columnas nativas de la shapefile IBGE 2022.
_SHAPE_COLS = ["CD_MUN", "NM_MUN", "SIGLA_UF", "AREA_KM2", "geometry"]


# --------------------------------------------------------------------------- #
# Carga de municipios
# --------------------------------------------------------------------------- #
def load_municipalities(
    estados: list[str] | None = None,
    codigos: list[str | int] | None = None,
    malha_path: Path | None = None,
) -> gpd.GeoDataFrame:
    """
    Carga la malha municipal IBGE 2022 filtrada por estado o por código.

    Parameters
    ----------
    estados : list[str], optional
        Siglas de UF a incluir (p.ej. ["PA", "AM"]). Case-insensitive.
    codigos : list[str|int], optional
        Códigos IBGE de município (7 dígitos) a incluir. Se normalizan a str.
    malha_path : Path, optional
        Ruta alternativa a la shapefile. Default: MALHA_BR_2022.

    Returns
    -------
    GeoDataFrame con columnas:
        code_muni (str, 7 dígitos, llave canónica), CD_MUN (int, alias),
        NM_MUN, SIGLA_UF, AREA_KM2, geometry.

    Notes
    -----
    Si no se pasa ni `estados` ni `codigos`, devuelve Brasil completo
    (5.570 municipios) — usar con intención.
    """
    path = Path(malha_path) if malha_path else MALHA_BR_2022
    if path is None:
        raise FileNotFoundError(
            "No se pudo ubicar la malha municipal (BR_Municipios_2022.shp). "
            "Define la env var TERRACORE_ROOT apuntando al root del monorepo, "
            "o pasa malha_path explícito."
        )
    if not path.exists():
        raise FileNotFoundError(
            f"No se encontró la malha municipal en {path}. "
            f"Ajusta TERRACORE_ROOT o pasa malha_path."
        )

    gdf = gpd.read_file(path)

    missing = [c for c in _SHAPE_COLS if c not in gdf.columns]
    if missing:
        raise ValueError(
            f"La malha {path.name} no tiene las columnas esperadas: faltan {missing}. "
            f"Encontradas: {list(gdf.columns)}"
        )

    # Llave canónica: 7 dígitos como string.
    gdf["code_muni"] = gdf["CD_MUN"].astype(str).str.zfill(7)

    if estados:
        ufs = {e.strip().upper() for e in estados}
        gdf = gdf[gdf["SIGLA_UF"].str.upper().isin(ufs)]
        faltan = ufs - set(gdf["SIGLA_UF"].str.upper().unique())
        if faltan:
            raise ValueError(f"Estados sin municipios en la malha: {sorted(faltan)}")

    if codigos:
        cods = {str(c).zfill(7) for c in codigos}
        gdf = gdf[gdf["code_muni"].isin(cods)]
        faltan = cods - set(gdf["code_muni"])
        if faltan:
            print(f"  AVISO: {len(faltan)} código(s) no están en la malha: "
                  f"{sorted(faltan)[:5]}{'...' if len(faltan) > 5 else ''}")

    if gdf.empty:
        raise ValueError("El filtro no devolvió ningún município.")

    cols = ["code_muni", "CD_MUN", "NM_MUN", "SIGLA_UF", "AREA_KM2", "geometry"]
    gdf = gdf[cols].reset_index(drop=True)
    print(f"  Malha cargada: {len(gdf)} municipios"
          f"{' de ' + '/'.join(sorted(set(gdf['SIGLA_UF']))) if estados else ''}.")
    return gdf


# --------------------------------------------------------------------------- #
# Validación de salida
# --------------------------------------------------------------------------- #
def validate_output(
    df: pd.DataFrame,
    expected_cols: list[str],
    n_expected: int | None = None,
    name: str = "",
) -> None:
    """
    Valida el DataFrame de un conector antes de guardarlo. Lanza en error duro,
    imprime aviso en coberturas parciales (missing data es esperable en munis
    aislados de PA/AM — se reporta, no se falla).
    """
    tag = f"[{name}] " if name else ""

    faltan = [c for c in expected_cols if c not in df.columns]
    if faltan:
        raise ValueError(f"{tag}Faltan columnas esperadas: {faltan}. "
                         f"Presentes: {list(df.columns)}")

    if df.empty:
        raise ValueError(f"{tag}El DataFrame está vacío.")

    if n_expected is not None and len(df) != n_expected:
        print(f"  {tag}AVISO: {len(df)} filas, se esperaban {n_expected} "
              f"(delta {len(df) - n_expected}).")

    # Reporte de cobertura por columna (missing data explícito).
    key_cols = {"code_muni", "CD_MUN"}
    for col in expected_cols:
        if col in key_cols:
            continue
        n_null = df[col].isna().sum()
        if n_null:
            pct = 100 * n_null / len(df)
            print(f"  {tag}Cobertura '{col}': {len(df) - n_null}/{len(df)} "
                  f"({100 - pct:.1f}%) — {n_null} municipios sin dato.")

    print(f"  {tag}Validación OK: {len(df)} filas, {len(df.columns)} columnas.")


# --------------------------------------------------------------------------- #
# Guardado estandarizado
# --------------------------------------------------------------------------- #
def save_processed(
    df: pd.DataFrame,
    filename: str,
    name: str = "",
    out_dir: Path | None = None,
) -> Path:
    """Guarda un dataset procesado en el directorio del motor y devuelve la ruta."""
    tag = f"[{name}] " if name else ""
    dest_dir = Path(out_dir) if out_dir else PROCESSED_DIR
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / filename

    if filename.endswith(".parquet"):
        df.to_parquet(dest, index=False)
    else:
        df.to_csv(dest, index=False)

    print(f"  {tag}Guardado: {dest}  ({len(df)} filas)")
    return dest
