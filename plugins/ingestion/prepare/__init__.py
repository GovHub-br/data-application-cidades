"""Preparos antes do pouso na raw (`DatasetSpec.prepare`).

Cada preparo recebe um arquivo extraído, já em disco, e cede os que o substituem:
descompactar, mascarar PII... Um arquivo por vez, apagando o de entrada.
"""

from ingestion.prepare.mask import MaskPii
from ingestion.prepare.unpack import Unpack

__all__ = ["MaskPii", "Unpack"]
