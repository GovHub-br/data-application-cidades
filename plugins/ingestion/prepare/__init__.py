"""Preparos antes do pouso na raw (`DatasetSpec.prepare`).

Cada preparo recebe um arquivo extraído, já em disco, e cede os que o substituem,
um arquivo por vez, apagando o de entrada:

- `Unpack` (`unpack.py`): zip e gzip viram os arquivos de dentro, pelo conteúdo;
- `MaskPii` (`mask.py`): PII mascarada antes do pouso. As peças do mascaramento:
  `mask_rules` (classificação pelo nome da coluna), `mask_keys` (token HMAC e
  redação), `mask_tabular` (CSV/TXT byte a byte) e `mask_xlsx` (XLSX por XML em
  fluxo). Identificadores (CPF, NIS) viram token HMAC determinístico;
  quasi-identificadores (nome de pessoa física, endereço, CEP, nascimento) são
  redigidos. O `scripts/mascarar_minio.py` (SharePoint) usa as mesmas peças.
"""

from ingestion.prepare.mask import MaskPii
from ingestion.prepare.mask_keys import MaskingKeys
from ingestion.prepare.mask_rules import classificar, targets_por_posicao
from ingestion.prepare.mask_tabular import mascarar_tabular, verificar_roundtrip_tabular
from ingestion.prepare.mask_xlsx import mascarar_xlsx, xlsx_tem_alvo
from ingestion.prepare.unpack import Unpack

__all__ = [
    "MaskPii",
    "MaskingKeys",
    "Unpack",
    "classificar",
    "mascarar_tabular",
    "mascarar_xlsx",
    "targets_por_posicao",
    "verificar_roundtrip_tabular",
    "xlsx_tem_alvo",
]
