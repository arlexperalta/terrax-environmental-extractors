#!/usr/bin/env python3
"""
TerraCore — Paso 1: Descargar datos de producción de café del IBGE/PAM.
SIDRA API directa via HTTP. Tabla 5457.
"""

import pandas as pd
import requests
import os
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, 'data', 'raw', 'ibge')
PROCESSED_DIR = os.path.join(BASE_DIR, 'data', 'processed')
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

SIDRA_URL = "https://apisidra.ibge.gov.br/values/t/5457/n6/all/v/{var}/p/{years}/c782/{prod}"


def fetch(var, prod, years, label):
    """Fetch one chunk from SIDRA."""
    url = SIDRA_URL.format(var=var, years=years, prod=prod)
    print(f"  {label}...", end=" ", flush=True)
    try:
        r = requests.get(url, timeout=180)
        r.raise_for_status()
        data = r.json()
        # Row 0 = header, rows 1+ = data
        df = pd.DataFrame(data[1:])
        print(f"{len(df)} reg")
        return df
    except Exception as e:
        print(f"Error: {str(e)[:80]}")
        return None


def download_all():
    """Download coffee data: rendimiento, producción, área."""
    print("Descargando IBGE/PAM café via HTTP...\n")

    # 3-year chunks to stay under 50K per request
    year_chunks = [
        '2002,2003,2004', '2005,2006,2007', '2008,2009,2010',
        '2011,2012,2013', '2014,2015,2016', '2017,2018,2019',
        '2020,2021,2022,2023'
    ]

    variables = [('112', 'rendimento'), ('8331', 'producao'), ('216', 'area')]
    # Café em grão (Total) = 40139. Arabica/canefora (40161/40162) no tienen datos individuales en tabla 5457.
    # La dependencia de polinizadores se calcula como promedio ponderado después.
    products = [('40139', 'cafe_total')]

    all_dfs = []
    total = len(variables) * len(products) * len(year_chunks)
    i = 0

    for var_code, var_label in variables:
        for prod_code, prod_label in products:
            for years in year_chunks:
                i += 1
                yr_short = years[:4] + '-' + years.split(',')[-1]
                label = f"[{i}/{total}] {var_label} {prod_label} {yr_short}"
                df = fetch(var_code, prod_code, years, label)
                if df is not None:
                    all_dfs.append(df)
                time.sleep(0.3)

    if not all_dfs:
        return None

    combined = pd.concat(all_dfs, ignore_index=True)
    path = os.path.join(RAW_DIR, 'pam_5457_cafe.csv')
    combined.to_csv(path, index=False)
    print(f"\nTotal: {len(combined)} registros → {path}")
    return combined


def process(raw_df):
    """Process into time series + stability metrics."""
    if raw_df is None:
        return None

    print("\nProcesando...")
    df = raw_df.copy()

    # SIDRA JSON keys: D1C=mun_code, D1N=mun_name, D2C=var_code, D3C=year, D4C=prod_code, V=value
    df = df.rename(columns={
        'D1C': 'mun_code', 'D1N': 'mun_name',
        'D2C': 'var_code', 'D3C': 'year', 'D4C': 'prod_code',
        'V': 'value'
    })

    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    df['value'] = pd.to_numeric(
        df['value'].astype(str).str.strip().replace({'...': None, '-': None, 'X': None, '..': None}),
        errors='coerce'
    )
    df['mun_code'] = df['mun_code'].astype(str).str.strip()
    df['var_code'] = df['var_code'].astype(str).str.strip()
    df['prod_code'] = df['prod_code'].astype(str).str.strip()

    df = df.dropna(subset=['year', 'value', 'mun_code'])
    print(f"Registros válidos: {len(df)}")

    # Variable and product names
    var_map = {'112': 'rendimento_kg_ha', '8331': 'producao_ton', '216': 'area_colhida_ha'}
    prod_map = {'40161': 'Café arábica', '40162': 'Café canéfora'}
    df['var_name'] = df['var_code'].map(var_map)
    df['prod_name'] = df['prod_code'].map(prod_map)

    # Pivot: one row per mun × year × product
    pivoted = df.pivot_table(
        index=['mun_code', 'mun_name', 'year', 'prod_code', 'prod_name'],
        columns='var_name', values='value', aggfunc='first'
    ).reset_index()
    pivoted.columns.name = None

    ts_path = os.path.join(PROCESSED_DIR, 'ibge_cafe_timeseries.csv')
    pivoted.to_csv(ts_path, index=False)
    print(f"Serie temporal: {len(pivoted)} filas → {ts_path}")

    # Stability per municipality × product
    stability = pivoted.groupby(['mun_code', 'mun_name', 'prod_code', 'prod_name']).agg(
        n_years=('rendimento_kg_ha', 'count'),
        rendimento_mean=('rendimento_kg_ha', 'mean'),
        rendimento_std=('rendimento_kg_ha', 'std'),
        producao_mean=('producao_ton', 'mean'),
        area_mean=('area_colhida_ha', 'mean'),
    ).reset_index()

    stability['rendimento_cv'] = stability['rendimento_std'] / stability['rendimento_mean']
    # Pollinator dependency for coffee (total): weighted average
    # Brazil is ~75% arabica (dep=0.25) + ~25% robusta (dep=0.95) → weighted = 0.425
    # This is a national average; state-level ratios vary (ES is more robusta, MG more arabica)
    stability['pollinator_dep'] = 0.425
    stability = stability[stability['n_years'] >= 5]

    out_path = os.path.join(PROCESSED_DIR, 'ibge_cafe_stability.csv')
    stability.to_csv(out_path, index=False)

    print(f"\nEstabilidad: {len(stability)} filas → {out_path}")
    print(f"Municipios únicos: {stability['mun_code'].nunique()}")

    for name, g in stability.groupby('prod_name'):
        print(f"  {name}: {len(g)} mun, rend={g['rendimento_mean'].mean():.0f} kg/ha, CV={g['rendimento_cv'].mean():.3f}")

    return stability


if __name__ == '__main__':
    print("=" * 60)
    print("TerraCore — Paso 1: IBGE/PAM Café")
    print("=" * 60)
    raw = download_all()
    result = process(raw)
    if result is not None:
        print(f"\n✓ Paso 1 completado: {result['mun_code'].nunique()} municipios cafeteros")
    else:
        print("\n✗ Falló.")
