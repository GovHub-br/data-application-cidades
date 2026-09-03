"""Guardas do bootstrap do Superset.

O chart do Superset não some quando o dataset por trás dele desaparece: ele
continua na tela e simplesmente para de carregar. Foi o que aconteceu ao
unificar os schemas do conjuntura — `conjuntura_mart.gold_continuo_*` virou
`conjuntura.gld_*`, e 27 charts ficaram apontando para datasets que não
existiam mais, sem nenhum aviso.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]


def _bootstrap():
    """Carrega o script como módulo.

    Ele precisa entrar em `sys.modules` ANTES do `exec_module`: o script usa
    `dataclass`, e a resolução de tipos procura a classe pelo módulo dela — se
    o módulo ainda não está registrado, a busca devolve None e estoura.
    """
    if "bootstrap_conjuntura" in sys.modules:
        return sys.modules["bootstrap_conjuntura"]
    caminho = RAIZ / "scripts" / "superset" / "bootstrap_conjuntura.py"
    spec = importlib.util.spec_from_file_location("bootstrap_conjuntura", caminho)
    assert spec and spec.loader
    modulo = importlib.util.module_from_spec(spec)
    sys.modules["bootstrap_conjuntura"] = modulo
    spec.loader.exec_module(modulo)
    return modulo


class ApiFalsa:
    """Superset de mentira: registra o que foi criado e o que foi atualizado."""

    dry_run = False

    def __init__(self, charts: list[dict]) -> None:
        self._charts = charts
        self.criados: list[dict] = []
        self.atualizados: list[tuple[int, dict]] = []
        self._proximo_id = 900

    def list(self, resource: str) -> list[dict]:
        assert resource == "chart"
        return self._charts

    def create(self, resource: str, payload: dict) -> dict:
        self.criados.append(payload)
        self._proximo_id += 1
        return {"id": self._proximo_id}

    def update(self, resource: str, resource_id: int, payload: dict) -> None:
        self.atualizados.append((resource_id, payload))


def test_chart_orfao_e_reapontado_em_vez_de_ficar_quebrado() -> None:
    """O chart existe com o nome certo, mas o dataset mudou de lugar."""
    modulo = _bootstrap()
    titulo = modulo.nome_do_chart("gld_sinapi")
    api = ApiFalsa([{"slice_name": titulo, "id": 42, "datasource_id": 111}])

    ids = modulo.get_or_create_charts(api, {"gld_sinapi": 222})

    assert ids == [42], "o chart existente deve ser reaproveitado, não recriado"
    assert not api.criados, "reapontar não pode criar chart duplicado"
    assert len(api.atualizados) == 1
    chart_id, payload = api.atualizados[0]
    assert chart_id == 42
    assert payload["datasource_id"] == 222
    assert json.loads(payload["params"])["datasource"] == "222__table"


def test_chart_ja_correto_nao_e_tocado() -> None:
    """Sem divergência não há escrita — o script tem de ser idempotente."""
    modulo = _bootstrap()
    titulo = modulo.nome_do_chart("gld_sinapi")
    api = ApiFalsa([{"slice_name": titulo, "id": 42, "datasource_id": 222}])

    ids = modulo.get_or_create_charts(api, {"gld_sinapi": 222})

    assert ids == [42]
    assert not api.criados and not api.atualizados


def test_chart_ausente_e_criado() -> None:
    modulo = _bootstrap()
    api = ApiFalsa([])

    ids = modulo.get_or_create_charts(api, {"gld_sinapi": 222})

    assert len(ids) == 1 and not api.atualizados
    assert api.criados[0]["datasource_id"] == 222
