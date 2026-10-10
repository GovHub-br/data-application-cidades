"""Mascaramento de PII em arquivos tabulares (CSV/TXT/XLSX), sem guardar o original.

Identificadores (CPF, NIS) viram token HMAC determinístico; quasi-identificadores
(nome de pessoa física, endereço, CEP, nascimento) são redigidos. A classificação é
pelo nome da coluna (`rules.classificar`). Usado pelo preparo `MaskPii` (antes do
pouso na raw) e pelo `scripts/mascarar_minio.py` (SharePoint).
"""

from ingestion.masking.keys import MaskingKeys
from ingestion.masking.rules import classificar, targets_por_posicao
from ingestion.masking.tabular import mascarar_tabular, verificar_roundtrip_tabular
from ingestion.masking.xlsx import mascarar_xlsx, xlsx_tem_alvo

__all__ = [
    "MaskingKeys",
    "classificar",
    "mascarar_tabular",
    "mascarar_xlsx",
    "targets_por_posicao",
    "verificar_roundtrip_tabular",
    "xlsx_tem_alvo",
]
