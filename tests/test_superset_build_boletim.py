"""main() do build_boletim: páginas reconstruídas, preservadas e a publicação no Superset."""

import json
import sys

import build_boletim as bb


class _SupersetFalso:
    base = "http://superset"

    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        self.chamadas = []
        self.s = self
        _SupersetFalso.ultimo = self

    def listar(self, rota):
        return {
            "database": [{"database_name": bb.DATABASE_NAME, "id": 1}],
            "dataset": [{"table_name": "t", "id": 5}],
            "chart": [],
        }[rota]

    def get(self, rota, ident=None, timeout=None):
        if ident is None:  # requests: lista de charts ligados ao painel
            return type("R", (), {"json": lambda _s: {"result": [1, 2]}})()
        if rota == "dashboard":
            return _SupersetFalso.publicado
        return {"datasource_id": 50, "dashboards": [{"id": 9}] if ident == 11 else []}

    def atualizar(self, rota, ident, payload):
        self.chamadas.append(("atualizar", rota, ident, payload))

    def criar(self, rota, payload):
        self.chamadas.append(("criar", rota, payload))
        return 9


def _rodar(monkeypatch, capsys, publicado, *argv):
    _SupersetFalso.publicado = publicado
    monkeypatch.setattr(bb, "Superset", _SupersetFalso)
    monkeypatch.setattr(
        bb,
        "CONSTRUTORES",
        {
            1: lambda api, cache: [{"tipo": "CHART", "chart": 10, "titulo": "a"}],
            2: lambda api, cache: [{"tipo": "CHART", "chart": 11, "titulo": "b"}],
        },
    )
    monkeypatch.setattr(sys, "argv", ["build_boletim.py", *argv])
    bb.main()
    return _SupersetFalso.ultimo, capsys.readouterr().out


def test_dry_run_nao_grava(monkeypatch, capsys):
    api, saida = _rodar(monkeypatch, capsys, None, "--dry-run")
    assert api.chamadas == [] and "[dry-run] layout não gravado." in saida


def test_cria_o_painel_e_liga_os_graficos_que_faltam(monkeypatch, capsys):
    api, saida = _rodar(monkeypatch, capsys, None)
    criado = [c for c in api.chamadas if c[0] == "criar"]
    assert len(criado) == 1
    assert json.loads(criado[0][2]["json_metadata"])["native_filter_configuration"]
    assert [c[2] for c in api.chamadas if c[1] == "chart"] == [10]
    assert "dashboard criado: 9" in saida and "charts ligados: 2" in saida


def test_pagina_fora_do_alvo_e_preservada_do_publicado(monkeypatch, capsys):
    layout = json.loads(bb.monta_layout({2: [{"tipo": "CHART", "chart": 77, "titulo": "x"}]}))
    publicado = {"id": 9, "position_json": json.dumps(layout)}
    api, saida = _rodar(monkeypatch, capsys, publicado, "--paginas", "1")
    assert "Página 2: preservada (1 blocos)" in saida
    atualizado = [c for c in api.chamadas if c[:2] == ("atualizar", "dashboard")]
    assert atualizado and '"chartId": 77' in atualizado[0][3]["position_json"]
