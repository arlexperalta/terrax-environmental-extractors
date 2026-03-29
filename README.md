# TerraX Environmental Extractors

Modular toolkit for extracting and processing environmental, demographic, and geospatial data from Brazilian public sources. Built for territorial intelligence research in collaboration with University of São Paulo.

## What's inside

### `ibge/download_ibge_pam.py`
Downloads coffee production data from Brazil's IBGE/PAM agricultural survey via the public SIDRA API. Covers all municipalities, 1985-2023. Extracts area planted, area harvested, production quantity, and yield.

### `landscape/landscape_metrics.py`
Computes landscape configuration metrics per municipality using MapBiomas Collection 9 (30m resolution) via Google Earth Engine. Metrics: forest cover (%), edge density, patch density, Shannon diversity index.

### `census/ibge_census.py`
Extracts municipal-level demographic data from IBGE Census. Covers family agriculture, population, and socioeconomic indicators for 5,570+ Brazilian municipalities.

### `utils/brazilian_coordinates.R`
R utilities for working with Brazilian geospatial data:
- Coordinate cleaning (handles Brazilian CSV format with dots as thousands separators)
- Lat/lon validation for Brazilian territory
- Copernicus CDS/ADS API setup and request helpers
- Regional bounding box generation

## Data sources

All data sources are **publicly available**:

| Source | Data | Access |
|--------|------|--------|
| [IBGE/PAM](https://sidra.ibge.gov.br/) | Agricultural production by municipality | Open API, no auth |
| [MapBiomas](https://mapbiomas.org/) | Land use/cover 30m, 1985-2023 | Open via GEE |
| [IBGE Census](https://www.ibge.gov.br/) | Demographics, socioeconomic | Open API |
| [Copernicus CDS](https://cds.climate.copernicus.eu/) | ERA5 climate, CAMS air quality | Free registration |

## Setup

### Python scripts

```bash
pip install earthengine-api pandas requests
```

For GEE scripts, authenticate with:
```python
import ee
ee.Authenticate()
ee.Initialize(project="your-gee-project-id")
```

Set your GEE project ID:
```bash
export GEE_PROJECT="your-gee-project-id"
```

### R utilities

```r
install.packages(c("glue", "httr", "jsonlite"))
source("utils/brazilian_coordinates.R")
```

## Context

These extractors are part of [TerraX](https://github.com/arlexperalta), an environmental intelligence platform built in collaboration with University of São Paulo researchers. The toolkit supports multiple research projects:

- **TerraRisk** — Territorial risk scoring for 5,570 Brazilian municipalities
- **Air Pollution** — Atmospheric data processing for academic research
- **Bee Phenology** — Pollinator phenology data pipelines
- **Coffee & Biodiversity** — Coffee-pollination models from PNAS/JAE/ERL papers

## License

MIT
