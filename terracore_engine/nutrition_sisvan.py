"""
TerraCore Data Engine — Nutrición SISVAN (BigQuery / Base dos Dados).

Capa 4b del assessment PA/AM: estado nutricional por município, desde los microdados
del SISVAN (Sistema de Vigilância Alimentar e Nutricional, Ministério da Saúde).

POR QUÉ BIGQUERY Y NO UN CSV:
SISVAN no existe agregado por município en ninguna fuente abierta — todas dan
microdados individuais (un registro por acompanhamiento antropométrico, millones
de filas para PA/AM). Base dos Dados ya ingirió esos microdados (2008-2023) como
tabla BigQuery; agregamos server-side con SQL y bajamos solo ~206 filas. Paginar
la API REST oficial serían miles de llamadas. La clasificación nutricional ya viene
oficial por individuo (el MS aplicó las curvas OMS) → aquí solo contamos categorías
(count/total = prevalência), no inventamos metodología.

AUTENTICACIÓN (paso humano, una vez):
Todo job de BigQuery se factura a un proyecto GCP propio (free tier 1 TB/mes cubre
esto de sobra). Configurar por env vars:
  - TERRACORE_BQ_PROJECT      : id del proyecto GCP con billing (el billing project).
  - TERRACORE_BQ_CREDENTIALS  : ruta al service-account JSON (opcional; si falta,
                                usa Application Default Credentials / gcloud).
Los DATOS son públicos (proyecto `basedosdados`); solo el compute se factura a
TERRACORE_BQ_PROJECT.

DECISIÓN DE ADRIAN (T6): qué indicador exponer. Default = desnutrición infantil
(crianças com magreza por IMC-para-idade). Ajustable con `indicator=` sin tocar
la query. Corre `discover_schema()` primero para confirmar nombres reales de columna
y las categorías de diagnóstico de la tabla antes de fijar el indicador.

Uso:
    from terracore_engine.nutrition_sisvan import discover_schema, extract_sisvan
    discover_schema()                              # 1er run: confirma esquema + categorías
    df = extract_sisvan(estados=["PA", "AM"], ano=2023)
"""
from __future__ import annotations

import os

import pandas as pd

from .base import load_municipalities, validate_output, save_processed

# --------------------------------------------------------------------------- #
# Fuente (Base dos Dados). Tabla verificada 2026-07-14 vía BigQuery:
# basedosdados.br_ms_sisvan.microdados (406M filas, 82.5 GB, particionada por ano+uf).
# --------------------------------------------------------------------------- #
BD_PROJECT = "basedosdados"
BD_DATASET = "br_ms_sisvan"
BD_TABLE = "microdados"
FQTN = f"{BD_PROJECT}.{BD_DATASET}.{BD_TABLE}"

# Tope de bytes facturados por query (seguridad free tier): 20 GB. Una query
# filtrada por ano+uf escanea mucho menos; si excede, BigQuery aborta en vez de cobrar.
MAX_BYTES_BILLED = 20 * 1024 ** 3

# Catálogo de indicadores. Cada uno: columna de diagnóstico + categorías "de riesgo".
# Columna y categorías VERIFICADAS 2026-07-14 contra la tabla real (PA/AM 2023):
# estado_nutricional_imc_idade_crianca ∈ {Eutrofia, Risco de sobrepeso, Sobrepeso,
# Obesidade, Magreza, Magreza acentuada, NULL}. El NULL domina (~74%) porque la fila
# es de otra fase de vida (adulto/idoso/gestante) → NO cuenta como criança avaliada.
# La definición fina del indicador (qué categorías = riesgo) la valida Adrian (T6).
INDICATORS = {
    "desnutricao_infantil": {
        "col": "estado_nutricional_imc_idade_crianca",
        "risco": ["Magreza", "Magreza acentuada"],
        "desc": "% de crianças com magreza/magreza acentuada (IMC-para-idade), SISVAN",
    },
    "obesidade_infantil": {
        "col": "estado_nutricional_imc_idade_crianca",
        "risco": ["Sobrepeso", "Obesidade"],
        "desc": "% de crianças com excesso de peso (sobrepeso+obesidade, IMC-para-idade)",
    },
}


def _client():
    """Cliente BigQuery desde service account (TERRACORE_BQ_CREDENTIALS) o ADC."""
    from google.cloud import bigquery

    project = os.environ.get("TERRACORE_BQ_PROJECT")
    if not project:
        raise EnvironmentError(
            "Falta la env var TERRACORE_BQ_PROJECT (id del proyecto GCP con billing). "
            "Los datos son públicos, pero BigQuery factura el compute a TU proyecto."
        )
    cred_path = os.environ.get("TERRACORE_BQ_CREDENTIALS")
    if cred_path:
        from google.oauth2 import service_account
        creds = service_account.Credentials.from_service_account_file(cred_path)
        return bigquery.Client(project=project, credentials=creds)
    # Fallback: Application Default Credentials (gcloud auth application-default login).
    return bigquery.Client(project=project)


def _run(client, sql: str, params: list, dry_run: bool = False):
    from google.cloud import bigquery

    job_config = bigquery.QueryJobConfig(
        query_parameters=params,
        maximum_bytes_billed=MAX_BYTES_BILLED,
        dry_run=dry_run,
        use_query_cache=not dry_run,
    )
    job = client.query(sql, job_config=job_config)
    if dry_run:
        gb = job.total_bytes_processed / 1024 ** 3
        print(f"  [dry-run] la query escaneará ~{gb:.2f} GB "
              f"(tope facturable {MAX_BYTES_BILLED / 1024 ** 3:.0f} GB).")
        return None
    return job.result().to_dataframe()


# --------------------------------------------------------------------------- #
# Discovery — correr PRIMERO con credencial, para confirmar nombres reales
# --------------------------------------------------------------------------- #
def discover_schema() -> None:
    """Imprime columnas de la tabla + años disponibles. Confirma el esquema antes de agregar."""
    from google.cloud import bigquery

    client = _client()
    print(f"Tabla: {FQTN}")
    table = client.get_table(FQTN)
    print(f"  filas: {table.num_rows:,} | tamaño: {table.num_bytes / 1024 ** 3:.1f} GB")
    print("  columnas:")
    for f in table.schema:
        print(f"    - {f.name} ({f.field_type})")

    # Años y UFs presentes (barato: agregación sobre columnas de partición/cluster).
    sql = (f"SELECT ano, COUNT(*) n FROM `{FQTN}` "
           f"WHERE sigla_uf IN ('PA','AM') GROUP BY ano ORDER BY ano DESC LIMIT 5")
    print("  (últimos años con dato PA/AM — dry-run de costo primero)")
    _run(client, sql, [], dry_run=True)


# --------------------------------------------------------------------------- #
# Extracción — prevalência por município
# --------------------------------------------------------------------------- #
def extract_sisvan(
    estados: list[str] | None = None,
    ano: int = 2023,
    indicator: str = "desnutricao_infantil",
    dry_run_first: bool = True,
) -> pd.DataFrame:
    """
    Prevalência do indicador nutricional por município (agregação server-side).

    Returns
    -------
    DataFrame tidy: code_muni, NM_MUN, SIGLA_UF, n_acompanhamentos,
    n_<indicador>, prev_<indicador>, ano. Merge a la malha → 206 filas (munis sem
    acompanhamento quedan con n=0 / prevalência null; el relatório de cobertura los caza).
    """
    from google.cloud import bigquery

    estados = estados or ["PA", "AM"]
    if indicator not in INDICATORS:
        raise ValueError(f"indicator '{indicator}' no está en {list(INDICATORS)}")
    spec = INDICATORS[indicator]
    client = _client()

    # Denominador epidemiológico correcto: crianças efectivamente avaliadas por
    # IMC-para-idade (col NOT NULL), NO todos los acompanhamentos del município
    # (que incluyen adultos/idosos/gestantes). Dividir entre COUNT(*) subestima la
    # prevalência ~3-4x. n_acompanhamentos queda como contexto de cobertura.
    sql = f"""
        SELECT
          id_municipio AS code_muni,
          COUNT(*) AS n_acompanhamentos,
          COUNTIF({spec['col']} IS NOT NULL) AS n_avaliados,
          COUNTIF({spec['col']} IN UNNEST(@risco)) AS n_risco
        FROM `{FQTN}`
        WHERE sigla_uf IN UNNEST(@ufs) AND ano = @ano
        GROUP BY id_municipio
    """
    params = [
        bigquery.ArrayQueryParameter("ufs", "STRING", estados),
        bigquery.ArrayQueryParameter("risco", "STRING", spec["risco"]),
        bigquery.ScalarQueryParameter("ano", "INT64", ano),
    ]

    if dry_run_first:
        _run(client, sql, params, dry_run=True)
    print(f"  consultando SISVAN {ano} — {spec['desc']}...")
    agg = _run(client, sql, params, dry_run=False)

    # Merge a la malha para garantizar las 206 filas + reportar cobertura.
    munis = load_municipalities(estados=estados)
    agg["code_muni"] = agg["code_muni"].astype(str).str.zfill(7)
    out = munis[["code_muni", "NM_MUN", "SIGLA_UF"]].merge(agg, on="code_muni", how="left")
    for c in ("n_acompanhamentos", "n_avaliados", "n_risco"):
        out[c] = out[c].fillna(0).astype(int)
    prev_col = f"prev_{indicator}"
    out[prev_col] = (out["n_risco"] / out["n_avaliados"]).where(
        out["n_avaliados"] > 0).round(4)
    out["ano"] = ano

    validate_output(out, expected_cols=["code_muni", "n_avaliados", prev_col],
                    n_expected=len(munis), name="SISVAN")
    save_processed(out, f"pa_am_sisvan_{indicator}_{ano}.csv", name="SISVAN")
    return out


if __name__ == "__main__":
    import sys
    print("=" * 60)
    print("TerraCore Engine — Nutrición SISVAN PA/AM (BigQuery/Base dos Dados)")
    print("=" * 60)
    if "--discover" in sys.argv:
        discover_schema()
    else:
        df = extract_sisvan(estados=["PA", "AM"], ano=2023)
        print(f"\n✓ {df['n_avaliados'].gt(0).sum()}/{len(df)} municipios con crianças avaliadas")
        print(df[["NM_MUN", "SIGLA_UF", "n_acompanhamentos", "n_avaliados",
                  "prev_desnutricao_infantil"]].head(6).to_string(index=False))
