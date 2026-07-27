"""
TerraCore Data Engine — Clasificación de los cultivos de Pará por dependencia de polinizadores.

Pedido de Adrian (27/07/2026): *"Sería muy interesante poder calcular esto con
todos los cultivos agrícolas de la región. ¿Cuál es la lista general de cultivos
de Pará, para clasificarlos de acuerdo al grado de dependencia?"*

REFERENCIA CANÓNICA
    Klein, Vaissière, Cane, Steffan-Dewenter, Cunningham, Kremen & Tscharntke
    (2007). "Importance of pollinators in changing landscapes for world crops."
    *Proc. R. Soc. B* 274:303–313.

Sus cinco clases, por reducción de producción esperada sin polinizadores:
    essential  >= 90%      great  40–90%      modest  10–40%
    little      0–10%      none    0%

TRES NIVELES DE CERTEZA, y conviene no mezclarlos:
  - `klein`        — la especie está en la tabla de Klein et al. 2007.
  - `estructural`  — el producto cosechado **no es fruto ni semilla** (madera,
                     látex, hoja, resina, raíz, palmito, fibra). La dependencia es
                     nula por la parte que se cosecha, no por la biología floral.
                     Es la inferencia más segura de todas.
  - `literatura`   — especie amazónica fuera de Klein, clasificada por literatura
                     regional. **Es la que Adrian debe validar**: incluye açaí,
                     castanha-do-pará y cumaru, que son justamente las que más
                     pesan en el estado.

La columna `certeza` viaja con el dato para que ningún análisis downstream trate
como equivalente un valor de Klein y una inferencia nuestra.

Uso:
    from terracore_engine.pollinator_dependence import build_dependence_table
    df = build_dependence_table()
"""
from __future__ import annotations

import pandas as pd

from .base import save_processed

# (producto IBGE, especie, dependencia, certeza, nota)
DEPENDENCIA: list[tuple[str, str, str, str, str]] = [
    # ---- essential (>=90%) ----
    ("Cacau (em amêndoa)", "Theobroma cacao", "essential", "klein",
     "polinizado por Forcipomyia (Ceratopogonidae); auto-incompatible"),
    ("Dendê (cacho de coco)", "Elaeis guineensis", "essential", "klein",
     "Elaeidobius kamerunicus; su introducción disparó el rendimiento"),
    ("Melancia", "Citrullus lanatus", "essential", "klein", "monoica, requiere vector"),
    ("Maracujá", "Passiflora edulis", "essential", "klein",
     "auto-incompatible; Xylocopa. Sin polinizador se poliniza a mano"),
    ("Melão", "Cucumis melo", "essential", "klein", ""),
    ("Café (em grão) Canephora", "Coffea canephora", "essential", "klein",
     "auto-INCOMPATIBLE, a diferencia de C. arabica que es autógama"),
    ("Guaraná (semente)", "Paullinia cupana", "essential", "literatura",
     "auto-incompatible; abejas. Fuera de Klein"),
    ("1.3 - Castanha-do-pará", "Bertholletia excelsa", "essential", "literatura",
     "flor de acceso restringido: solo abejas grandes (Euglossini, Xylocopa, "
     "Bombus). Base del argumento de que la castanha depende del bosque en pie"),
    ("8.6 - Pequi (amêndoa)", "Caryocar villosum", "essential", "literatura",
     "quiropterofilia (murciélagos)"),
    ("1.7 - Pequi (fruto)", "Caryocar villosum", "essential", "literatura", ""),

    # ---- great (40-90%) ----
    ("Castanha de caju", "Anacardium occidentale", "great", "klein", ""),
    ("1.2 - Castanha-de-caju", "Anacardium occidentale", "great", "klein", ""),
    ("Abacate", "Persea americana", "great", "klein", ""),
    ("Manga", "Mangifera indica", "great", "klein", ""),
    ("Açaí", "Euterpe oleracea", "great", "literatura",
     "FUERA DE KLEIN. Monoica, protogínica; abejas (Apis, meliponinos) y "
     "coleópteros. La magnitud exacta es lo que Adrian debe fijar"),
    ("1.1 - Açaí (fruto)", "Euterpe oleracea", "great", "literatura",
     "misma especie que el cultivado, extracción en açaizal nativo"),
    ("8.3 - Cumaru (amêndoa)", "Dipteryx odorata", "great", "literatura",
     "abejas grandes; fuera de Klein"),
    ("5.1 - Buriti", "Mauritia flexuosa", "great", "literatura",
     "dioica: sin vector no hay fruto. Insectos y viento"),

    # ---- modest (10-40%) ----
    ("Coco-da-baía*", "Cocos nucifera", "modest", "klein", ""),
    ("Feijão (em grão)", "Phaseolus vulgaris", "modest", "klein",
     "autógama con incremento por visita"),
    ("Goiaba", "Psidium guajava", "modest", "klein", ""),
    ("Mamão", "Carica papaya", "modest", "klein",
     "depende del sistema sexual del cultivar; en dioicas sube a essential"),
    ("8.1 - Babaçu (amêndoa)", "Attalea speciosa", "modest", "literatura", ""),
    ("Urucum (semente)", "Bixa orellana", "modest", "literatura",
     "buzz pollination por Bombus/Xylocopa"),

    # ---- little (0-10%) ----
    ("Soja (em grão)", "Glycine max", "little", "klein",
     "autógama; el incremento por abejas existe pero es pequeño. Es el mayor "
     "valor agrícola del estado y NO es polinizador-dependiente"),
    ("Laranja", "Citrus sinensis", "little", "klein", ""),
    ("Limão", "Citrus spp.", "little", "klein", ""),
    ("Tangerina", "Citrus reticulata", "little", "klein", ""),
    ("Tomate", "Solanum lycopersicum", "little", "klein",
     "autógama; buzz pollination mejora calidad, no tanto cantidad"),
    ("Pimenta-do-reino", "Piper nigrum", "little", "klein", ""),

    # ---- none: anemófilas, autógamas estrictas o partenocárpicas ----
    ("Milho (em grão)", "Zea mays", "none", "klein", "anemófila"),
    ("Arroz (em casca)", "Oryza sativa", "none", "klein", "anemófila/autógama"),
    ("Sorgo (em grão)", "Sorghum bicolor", "none", "klein", "anemófila"),
    ("Cana-de-açúcar", "Saccharum officinarum", "none", "klein",
     "se cosecha el tallo; propagación vegetativa"),
    ("Banana (cacho)", "Musa spp.", "none", "klein", "partenocárpica"),
    ("Abacaxi*", "Ananas comosus", "none", "klein",
     "partenocárpica: la polinización produce semillas y DESVALORIZA el fruto"),
    ("Amendoim (em casca)", "Arachis hypogaea", "none", "klein",
     "cleistógama: se autopoliniza en flor cerrada"),

    # ---- none por estructura: no se cosecha fruto ni semilla ----
    ("Mandioca", "Manihot esculenta", "none", "estructural",
     "se cosecha la raíz; propagación por estaca"),
    ("Batata-doce", "Ipomoea batatas", "none", "estructural", "raíz"),
    ("Cebola", "Allium cepa", "none", "estructural",
     "se cosecha el bulbo; la semilla sí dependería"),
    ("Fumo (em folha)", "Nicotiana tabacum", "none", "estructural", "hoja"),
    ("Malva (fibra)", "Urena lobata", "none", "estructural", "fibra"),
    ("Palmito", "Euterpe spp.", "none", "estructural",
     "se cosecha el meristemo apical: mata la planta, no hay fruto"),
    ("1.6 - Palmito", "Euterpe spp.", "none", "estructural", "meristemo"),
    ("Borracha (látex coagulado)", "Hevea brasiliensis", "none", "estructural", "látex"),
    ("3.2 - Hevea (látex coagulado)", "Hevea brasiliensis", "none", "estructural", "látex"),
    ("8.2 - Copaíba (óleo)", "Copaifera spp.", "none", "estructural", "oleorresina del tronco"),
    ("2.2 - Jaborandi (folha)", "Pilocarpus spp.", "none", "estructural", "hoja"),
    ("7.1 - Carvão vegetal", "—", "none", "estructural", "producto maderero"),
    ("7.2 - Lenha", "—", "none", "estructural", "producto maderero"),
    ("7.3 - Madeira em tora", "—", "none", "estructural",
     "producto maderero; es el mayor valor extractivo del estado"),
    ("10.1 - Angico (casca)", "Anadenanthera spp.", "none", "estructural", "corteza"),
    ("10.2 - Barbatimão (casca)", "Stryphnodendron spp.", "none", "estructural", "corteza"),
    ("2.3 - Urucum (semente)", "Bixa orellana", "modest", "literatura",
     "misma especie que el urucum de PAM, aquí por extracción"),

    # ---- sin clasificar ----
    ("8.7 - Tucum (amêndoa)", "Astrocaryum spp.", "unknown", "literatura",
     "palmera, probablemente entomófila; sin cuantificación"),
]

ORDEN = ["essential", "great", "modest", "little", "none", "unknown"]
# Peso para agregados ponderados. Punto medio de cada banda de Klein.
PESO = {"essential": 0.95, "great": 0.65, "modest": 0.25,
        "little": 0.05, "none": 0.0, "unknown": float("nan")}


def build_dependence_table(save: bool = True) -> pd.DataFrame:
    df = pd.DataFrame(DEPENDENCIA, columns=[
        "produto", "especie", "dependencia", "certeza", "nota"])
    df["peso_dependencia"] = df["dependencia"].map(PESO)
    df["dependencia"] = pd.Categorical(df["dependencia"], categories=ORDEN, ordered=True)
    df = df.sort_values(["dependencia", "produto"]).reset_index(drop=True)
    if save:
        save_processed(df, "pollinator_dependence_klein2007.csv")
    return df


if __name__ == "__main__":
    d = build_dependence_table()
    print(d.groupby(["dependencia", "certeza"], observed=True)
           .size().rename("n").reset_index().to_string(index=False))
