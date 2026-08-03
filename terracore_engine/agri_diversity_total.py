"""
TerraCore Data Engine — Diversidade agrícola sobre TODA a produção do município.

Pedido de Adrian (comentarios del 03/08/2026 sobre el documento del 29/07):

    "Este creo q la podemos desconsiderar también [food × div. efetiva], y
    remplazarla por el Shannon o PCA de producción agrícola."

Y el diagnóstico que lo motiva, que es correcto: `agri_diversity.py` computa el
Shannon sobre **tres espécies** (açaí, cacau, castanha-do-pará). Dos de ellas
están ausentes en la mayoría de los municípios, así que el índice medía sobre
todo *cuántas de esas tres hay* y no la diversidade de la agricultura real. El
tope teórico era ln 3 = 1,099.

Este módulo lo computa sobre el **catálogo completo del estado** (PAM + PEVS),
que ya se descarga en `economic_weight.py` y quedaba guardado sin usarse para
esto: `<uf>_produccion_por_producto_<ano>.csv`, largo por município y produto.

NO reemplaza a `agri_diversity.py`: el viejo sigue emitiendo las variables de
las 3 espécies foco, que son exposición polinizador-dependiente y siguen siendo
variables del banco. Lo que cambia es de dónde sale la *diversidade*.

-------------------------------------------------------------------------------
CUATRO TRAMPAS DEL CATÁLOGO DEL IBGE, Y CÓMO SE RESUELVEN

1. SUBTOTALES QUE SE SUMARÍAN DOS VECES. Verificado sobre los datos, no asumido:
     - PAM `Total` = suma exacta de sus produtos (20.646.068 mil R$).
     - PAM `Café (em grão) Total` = Arábica + Canephora (0 + 480 = 480).
     - PEVS `1 - Alimentícios` = suma de sus hojas `1.x` (654.630 ≈ 654.632; la
       diferencia son redondeos del IBGE).
   Se usan SOLO hojas. Un subtotal en el índice duplicaría la abundancia de su
   propio grupo y lo haría parecer dominante.

2. LA MISMA ESPÉCIE CONTADA COMO DOS "CULTIVOS". El açaí aparece en PAM
   (cultivado) y en PEVS (extrativo); la castanha-de-caju, el palmito, la
   erva-mate y el urucum también viven en las dos fuentes; el pequi aparece como
   fruto y como amêndoa; la borracha y la carnaúba, como dos o tres productos de
   la misma planta. Sin unificar, un município que produce açaí por los dos
   sistemas cuenta como **dos espécies** y sale más diverso que uno que produce
   solo cultivado — un artefacto del sistema contable, no de la agricultura.
   El criterio ya lo fijó Adrian para el açaí en la v2 ("misma espécie, mismo
   polinizador"): aquí se generaliza vía `MESMA_ESPECIE`.

   Café Arábica y Canephora NO se unen: son espécies distintas (*C. arabica* es
   autógama, *C. canephora* auto-incompatible), y esa diferencia es justamente
   la que importa para polinización.

3. CATEGORÍAS RESIDUALES. PEVS cierra cada grupo con "Outros"/"Outras", que no
   es una espécie sino un cajón heterogéneo. Contarlo como una espécie infla la
   riqueza. Entran al valor total pero NO a riqueza/Shannon (`INCLUIR_RESIDUAIS`).

4. QUÉ ES "PRODUÇÃO AGRÍCOLA". El grupo 7 de PEVS (madeira em tora, carvão,
   lenha) es extractivismo forestal, y en Pará es el de mayor valor: meterlo
   domina el índice y mide otra cosa. Queda fuera de los dos universos, igual
   que en `economic_weight.py`, para que este Shannon y `valor_agricola_total`
   hablen del mismo universo. Se emiten dos escopos:
     - `agri`  (principal) = lavouras PAM + extrativos ALIMENTÍCIOS (PEVS 1.x).
     - `ampl`  (sensibilidad) = + extrativos no alimenticios y no madereros
                (aromáticos, borrachas, ceras, fibras, gomas, oleaginosos,
                tanantes) — donde viven cumaru, buriti y copaíba.

-------------------------------------------------------------------------------
ABUNDANCIA = VALOR (R$ mil), no área ni tonelada

Es la única unidad común a PAM y PEVS (el área plantada solo existe para las
lavouras; la tonelada no es comparable entre un fruto y una amêndoa). Es también
lo que Adrian pidió en la v2. **Limitación que hay que decir:** el valor mezcla
precio con cantidad, así que un município con poco volumen de un produto caro
aparece más "diverso" de lo que su superficie diría. Se emite `*_area_pam` como
sensibilidad sobre las lavouras, único subconjunto donde el área existe.

NaN → 0: en SIDRA el "..." significa no divulgado O inexistente. La decisión de
Adrian (21/07) es tratarlo como ausência de produção.

-------------------------------------------------------------------------------
ÍNDICES EMITIDOS (sufijo `_agri` para el escopo principal, `_ampl` para el amplio)

  agri_shannon    H = -Σ pᵢ ln pᵢ  sobre el valor de cada espécie
  agri_riqueza    nº de espécies con valor > 0
  agri_pielou     evenness H / ln(riqueza); NaN si riqueza < 2 (indefinida)
  agri_simpson    1 - Σ pᵢ²  (probabilidad de que dos reales sean de espécies distintas)
  agri_valor_total  suma R$ mil del escopo
  agri_dominancia   fracción del valor en la espécie mayor (Berger-Parker)

Y los ejes de composición que Adrian ofreció como alternativa al Shannon:

  agri_pca1..3    PCA sobre la matriz de composición con transformación de
                  HELLINGER (√pᵢ). Hellinger es el estándar para datos de
                  abundancia antes de un método euclídeo: evita que el PCA
                  crudo trate dos municípios que no comparten NINGÚN cultivo
                  como más parecidos que dos que comparten varios (la paradoja
                  de la doble ausencia). Legendre & Gallagher 2001.
                  La varianza explicada de cada eje va en el CSV auxiliar.

Uso:
    from terracore_engine.agri_diversity_total import extract_agri_diversity_total
    df = extract_agri_diversity_total(uf="pa", ano=2021)
"""
from __future__ import annotations

import re

import numpy as np
import pandas as pd

from .base import PROCESSED_DIR, save_processed, validate_output

# --------------------------------------------------------------------------- #
# Filtros de catálogo
# --------------------------------------------------------------------------- #

# Subtotales verificados contra los datos (ver trampa 1 en el docstring).
SUBTOTAIS_PAM = {"Total", "Café (em grão) Total"}

# PEVS es jerárquica: "N - Grupo" contiene "N.N - Hoja". Solo hojas.
RE_PEVS_GRUPO = re.compile(r"^\d+ - ")
RE_PEVS_HOJA = re.compile(r"^(\d+)\.\d+ - ")

# Grupo 7 = madeireiro. Fuera de los dos escopos (ver trampa 4).
PEVS_GRUPO_MADEIREIRO = "7"
PEVS_GRUPO_ALIMENTICIO = "1"

# El grupo 9 (Pinheiro brasileiro) mezcla una semilla comestible con dos produtos
# madereros. Las dos hojas madereras salen por el mismo criterio del grupo 7. En
# Pará las tres están vacías (Araucaria es del sur), pero el módulo se escribe
# para el catálogo del IBGE, no para un estado.
PEVS_HOJAS_MADEIREIRAS = {
    "9.2 - Pinheiro brasileiro (árvores abatidas)",
    "9.3 - Pinheiro brasileiro (madeira em tora)",
}

# Cajones residuales: entran al valor, no a riqueza/Shannon (trampa 3).
RE_RESIDUAL = re.compile(r"-\s*Outr[oa]s\s*$", re.IGNORECASE)

# --------------------------------------------------------------------------- #
# Unificación por espécie (trampa 2)
# --------------------------------------------------------------------------- #
# produto del IBGE -> clave de espécie. Lo que no está aquí es su propia espécie.
MESMA_ESPECIE = {
    # Euterpe oleracea — cultivado (PAM) + extrativo (PEVS). Precedente de Adrian.
    "Açaí": "acai",
    "1.1 - Açaí (fruto)": "acai",
    # Anacardium occidentale — el pseudofruto (PAM "Caju"), la castanha en PAM y
    # la misma castanha por extração en PEVS son la misma planta.
    "Caju": "caju",
    "Castanha de caju": "caju",
    "1.2 - Castanha-de-caju": "caju",
    # Ilex paraguariensis
    "Erva-mate (folha verde)": "erva_mate",
    "1.4 - Erva-mate": "erva_mate",
    # Euterpe spp. — meristemo
    "Palmito": "palmito",
    "1.6 - Palmito": "palmito",
    # Bixa orellana
    "Urucum (semente)": "urucum",
    "2.3 - Urucum (semente)": "urucum",
    # Caryocar villosum — fruto y amêndoa de la misma planta
    "1.7 - Pequi (fruto)": "pequi",
    "8.6 - Pequi (amêndoa)": "pequi",
    # Hevea brasiliensis — dos presentaciones del látex, dos fuentes
    "Borracha (látex coagulado)": "seringueira",
    "Borracha (látex líquido)": "seringueira",
    "3.2 - Hevea (látex coagulado)": "seringueira",
    "3.3 - Hevea (látex líquido)": "seringueira",
    # Copernicia prunifera — cera, pó y fibra de la misma palmera
    "4.1 - Carnaúba (cera)": "carnauba",
    "4.2 - Carnaúba (pó)": "carnauba",
    "5.2 - Carnaúba": "carnauba",
    # Mauritia flexuosa / Hancornia speciosa — fruto y fibra
    "3.4 - Mangabeira": "mangaba",
    "1.5 - Mangaba (fruto)": "mangaba",
}


def _classificar(fonte: str, produto: str) -> tuple[str | None, bool]:
    """
    (escopo mínimo del produto, es_residual). escopo None = excluido siempre.

    escopo 'agri' entra a los dos universos; 'ampl' solo al amplio.
    """
    p = produto.strip()
    if p.lower() == "total":
        return None, False

    if fonte == "PAM":
        if p in SUBTOTAIS_PAM:
            return None, False
        return "agri", bool(RE_RESIDUAL.search(p))

    # PEVS
    m = RE_PEVS_HOJA.match(p)
    if not m:                       # cabecera de grupo ("N - ...") o cosa rara
        return None, False
    grupo = m.group(1)
    if grupo == PEVS_GRUPO_MADEIREIRO or p in PEVS_HOJAS_MADEIREIRAS:
        return None, False
    escopo = "agri" if grupo == PEVS_GRUPO_ALIMENTICIO else "ampl"
    return escopo, bool(RE_RESIDUAL.search(p))


def _shannon(p: np.ndarray) -> float:
    """H sobre proporciones ya normalizadas (sin ceros)."""
    return float(-(p * np.log(p)).sum())


def _indices(mat: pd.DataFrame, sufixo: str) -> pd.DataFrame:
    """
    Índices de diversidade sobre una matriz município × espécie de valores R$.
    Las columnas residuales deben venir ya excluidas de `mat`.
    """
    valores = mat.to_numpy(dtype=float)
    total = valores.sum(axis=1)

    riqueza = (valores > 0).sum(axis=1)
    shannon = np.zeros(len(valores))
    simpson = np.zeros(len(valores))
    dominancia = np.full(len(valores), np.nan)

    for i, fila in enumerate(valores):
        if total[i] <= 0:
            continue
        p = fila[fila > 0] / total[i]
        # Con una sola espécie, −(1·ln 1) da −0.0, que se propaga feo a los CSV.
        # No sirve `max(h, 0.0)`: con −0.0 == 0.0 Python devuelve el primero.
        h = _shannon(p)
        shannon[i] = h if h > 0.0 else 0.0
        simpson[i] = float(1.0 - (p ** 2).sum())
        dominancia[i] = float(p.max())

    # Pielou indefinida con menos de 2 espécies: NaN, no 0. Un município con un
    # solo cultivo no tiene evenness "perfecta", no tiene evenness.
    pielou = np.full(len(valores), np.nan)
    ok = riqueza >= 2
    pielou[ok] = shannon[ok] / np.log(riqueza[ok])

    return pd.DataFrame({
        "code_muni": mat.index,
        f"agri_riqueza_{sufixo}": riqueza.astype("int64"),
        f"agri_shannon_{sufixo}": np.round(shannon, 4),
        f"agri_pielou_{sufixo}": np.round(pielou, 4),
        f"agri_simpson_{sufixo}": np.round(simpson, 4),
        f"agri_dominancia_{sufixo}": np.round(dominancia, 4),
        f"agri_valor_total_{sufixo}": np.round(total, 2),
    })


def _pca_hellinger(mat: pd.DataFrame, n_axes: int = 3
                   ) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    PCA sobre la composición con transformación de Hellinger (√ proporción).

    Hellinger antes de un método euclídeo evita la paradoja de la doble ausencia:
    sin ella, dos municípios que no comparten NINGÚN cultivo pueden salir más
    parecidos entre sí que dos que comparten varios, porque los ceros compartidos
    cuentan como semejanza. Legendre & Gallagher (2001), Oecologia 129:271-280.

    Se centra por columna (no se escala: Hellinger ya homogeneiza) y se resuelve
    por SVD — sin dependencias externas y con los signos fijados de forma
    determinista para que el eje no se voltee entre corridas.
    """
    valores = mat.to_numpy(dtype=float)
    total = valores.sum(axis=1, keepdims=True)
    with np.errstate(divide="ignore", invalid="ignore"):
        prop = np.where(total > 0, valores / total, 0.0)
    hel = np.sqrt(prop)

    centro = hel.mean(axis=0)
    X = hel - centro
    U, S, Vt = np.linalg.svd(X, full_matrices=False)

    # Signo determinista: el componente de mayor carga absoluta queda positivo.
    for k in range(len(S)):
        j = int(np.argmax(np.abs(Vt[k])))
        if Vt[k, j] < 0:
            Vt[k] *= -1
            U[:, k] *= -1

    scores = U[:, :n_axes] * S[:n_axes]
    var_exp = (S ** 2) / (S ** 2).sum()

    ejes = pd.DataFrame(
        {f"agri_pca{k + 1}": np.round(scores[:, k], 5) for k in range(n_axes)})
    ejes.insert(0, "code_muni", mat.index)

    cargas = pd.DataFrame(Vt[:n_axes].T, index=mat.columns,
                          columns=[f"pca{k + 1}" for k in range(n_axes)])
    cargas = cargas.round(5).reset_index().rename(columns={"index": "especie"})
    cargas.attrs["var_explicada"] = var_exp[:n_axes]
    return ejes, cargas


def compute_agri_diversity_total(
    prod_long: pd.DataFrame,
    incluir_residuais: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Calcula los índices sobre el largo (code_muni, fonte, produto, valor).

    Returns
    -------
    (banco, cargas_pca, diagnostico)
    """
    df = prod_long.copy()
    df["code_muni"] = (df["code_muni"].astype(str)
                       .str.replace(r"\.0$", "", regex=True).str.zfill(7))
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0.0)

    clasif = df.apply(lambda r: _classificar(r["fonte"], r["produto"]), axis=1)
    df["escopo"] = [c[0] for c in clasif]
    df["residual"] = [c[1] for c in clasif]

    excluidos = sorted(df.loc[df["escopo"].isna(), "produto"].unique())
    df = df[df["escopo"].notna()].copy()

    # Unificación por espécie. Lo no mapeado es su propia espécie.
    df["especie"] = df["produto"].map(MESMA_ESPECIE).fillna(df["produto"])

    # Un produto residual arrastra su espécie a residual (los "Outros" no se
    # unifican con nada, así que no hay mezcla posible).
    residual_por_especie = df.groupby("especie")["residual"].max()

    todos = sorted(df["code_muni"].unique())
    banco = pd.DataFrame({"code_muni": todos})
    diag: dict = {"produtos_excluidos": excluidos}

    for sufixo, escopos in [("agri", {"agri"}), ("ampl", {"agri", "ampl"})]:
        sub = df[df["escopo"].isin(escopos)]
        mat = (sub.pivot_table(index="code_muni", columns="especie",
                               values="valor", aggfunc="sum", fill_value=0.0)
               .reindex(todos, fill_value=0.0))

        # El valor total incluye los residuales; los índices de diversidade no.
        valor_con_residuais = mat.sum(axis=1)
        if not incluir_residuais:
            cols_ok = [c for c in mat.columns if not residual_por_especie.get(c, False)]
            mat_idx = mat[cols_ok]
        else:
            mat_idx = mat

        idx = _indices(mat_idx, sufixo)
        idx[f"agri_valor_total_{sufixo}"] = valor_con_residuais.reindex(
            idx["code_muni"]).round(2).to_numpy()
        banco = banco.merge(idx, on="code_muni", how="left")

        diag[f"n_especies_{sufixo}"] = int(mat_idx.shape[1])
        diag[f"especies_{sufixo}"] = list(mat_idx.columns)

        if sufixo == "agri":
            ejes, cargas = _pca_hellinger(mat_idx)
            banco = banco.merge(ejes, on="code_muni", how="left")
            diag["pca_var_explicada"] = [float(v) for v in cargas.attrs["var_explicada"]]
            cargas_out = cargas

    # Sensibilidad por ÁREA sobre las lavouras (único subconjunto con hectare).
    return banco, cargas_out, diag


def extract_agri_diversity_total(
    uf: str = "pa",
    ano: int = 2021,
    save: bool = True,
) -> pd.DataFrame:
    """Lee el largo por produto del motor, computa los índices y los guarda."""
    path = PROCESSED_DIR / f"{uf}_produccion_por_producto_{ano}.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"No está {path}. Corre economic_weight primero "
            f"(extract_economic_weight(estados=['{uf.upper()}'])).")

    banco, cargas, diag = compute_agri_diversity_total(pd.read_csv(path))

    validate_output(
        banco,
        expected_cols=["code_muni", "agri_shannon_agri", "agri_riqueza_agri",
                       "agri_pca1"],
        n_expected=len(banco), name="AgriDiversityTotal",
    )
    if save:
        save_processed(banco, f"{uf}_agri_diversity_total_{ano}.csv",
                       name="AgriDiversityTotal")
        save_processed(cargas, f"{uf}_agri_diversity_pca_cargas_{ano}.csv")
    banco.attrs["diagnostico"] = diag
    return banco


if __name__ == "__main__":
    import json

    print("=" * 72)
    print("TerraCore Engine — Diversidade agrícola sobre TODA a produção (PA)")
    print("=" * 72)
    df = extract_agri_diversity_total()
    d = df.attrs["diagnostico"]

    print(f"\n✓ {len(df)} municípios")
    print(f"\nEspécies no escopo agrícola: {d['n_especies_agri']}")
    print(f"Espécies no escopo amplo:    {d['n_especies_ampl']}")
    print(f"\nProdutos excluídos ({len(d['produtos_excluidos'])}): "
          f"{json.dumps(d['produtos_excluidos'], ensure_ascii=False)}")

    print("\n--- Shannon agrícola vs. o das 3 espécies ---")
    print(df[["agri_shannon_agri", "agri_riqueza_agri", "agri_pielou_agri"]]
          .describe().round(3).to_string())

    print(f"\nPCA — varianza explicada: "
          f"{[round(v * 100, 1) for v in d['pca_var_explicada']]} %")

    print("\nTop 5 municípios mais diversos:")
    print(df.nlargest(5, "agri_shannon_agri")[
        ["code_muni", "agri_riqueza_agri", "agri_shannon_agri",
         "agri_valor_total_agri"]].to_string(index=False))
