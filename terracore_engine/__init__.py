"""
TerraCore Data Engine.

Extractores modulares de datos ambientales, demográficos y geoespaciales de
fuentes públicas brasileñas, para inteligencia territorial a resolución municipal.

API base (liviana, sin dependencias pesadas):
    from terracore_engine import load_municipalities, validate_output, save_processed

Conectores (importar bajo demanda; algunos requieren extras, p.ej. GEE):
    from terracore_engine.landscape_metrics import extract_landscape_metrics
    from terracore_engine.ibge_census import extract_p_familiar
    from terracore_engine.download_ibge_pam import ...
"""
from .base import load_municipalities, validate_output, save_processed

__all__ = ["load_municipalities", "validate_output", "save_processed"]
__version__ = "0.1.0"
