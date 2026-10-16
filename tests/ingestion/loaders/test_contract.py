"""Contrato de todo Loader.

Todo loader respeita os três modos de carga (overwrite, merge com keys, append).

Cada implementação registrada entra em IMPLEMENTATIONS e herda estes testes.
Vazio até a Fase 7 (Postgres sem MinIO) e Fase 9 (Iceberg): o pytest reporta como skipped.
"""

import pytest

IMPLEMENTATIONS: list[str] = []


@pytest.mark.parametrize("name", IMPLEMENTATIONS)
def test_contract(name: str) -> None:
    raise NotImplementedError(name)
