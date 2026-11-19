"""main() do renomeador de datasets do Superset, com um Superset falso."""

import sys

import pytest

import superset_renomeia_datasets as r

(VELHO_1, NOVO_1), (VELHO_2, NOVO_2) = list(r.MAPA.items())[:2]


class _SupersetFalso:
    def __init__(self, url, user, password, provider):
        self.url = url
        self.chamadas = []
        _SupersetFalso.ultimo = self

    def datasets(self):
        return [
            {"id": 1, "schema": VELHO_1[0], "table_name": VELHO_1[1]},
            {"id": 2, "schema": VELHO_1[0], "table_name": VELHO_1[1]},
            {"id": 3, "schema": NOVO_2[0], "table_name": NOVO_2[1]},
            {"id": 4, "schema": None, "table_name": "outra"},
        ]

    def renomeia(self, ident, schema, tabela):
        if ident == 2:
            raise RuntimeError("falhou")
        self.chamadas.append(("renomeia", ident, schema, tabela))

    def sincroniza_colunas(self, ident):
        self.chamadas.append(("sincroniza", ident))


@pytest.fixture
def rodar(monkeypatch, capsys):
    monkeypatch.setattr(r, "load_dotenv", lambda: None)
    monkeypatch.setattr(r, "Superset", _SupersetFalso)
    for var in ("SUPERSET_URL", "SUPERSET_USER", "SUPERSET_PASSWORD"):
        monkeypatch.setenv(var, "x")

    def _rodar(*argv):
        monkeypatch.setattr(sys, "argv", ["superset_renomeia_datasets.py", *argv])
        return r.main(), capsys.readouterr().out

    return _rodar


def test_sem_variaveis_nao_conecta(rodar, monkeypatch):
    monkeypatch.delenv("SUPERSET_PASSWORD")
    codigo, saida = rodar()
    assert codigo == 2 and "Faltam variáveis de ambiente: SUPERSET_PASSWORD" in saida


def test_procurar_lista_sem_renomear(rodar):
    codigo, saida = rodar("--procurar", "OUTRA")
    assert codigo == 0 and "1 de 4 datasets casam com 'OUTRA'" in saida
    assert _SupersetFalso.ultimo.chamadas == []


def test_dry_run_mostra_o_plano(rodar):
    codigo, saida = rodar()
    assert codigo == 0 and _SupersetFalso.ultimo.chamadas == []
    assert f"-> {NOVO_1[0]}.{NOVO_1[1]}" in saida
    assert "ATENÇÃO — tabela com mais de um dataset" in saida and "ids 1, 2" in saida
    assert f"já no destino (1): {NOVO_2[0]}.{NOVO_2[1]}" in saida
    assert "não encontrados" in saida
    assert "2 dataset(s) seriam renomeados (1 tabelas). Rode com --apply." in saida


def test_apply_segue_depois_de_um_erro_e_reporta(rodar):
    codigo, saida = rodar("--apply", "--sync-colunas")
    assert codigo == 1
    assert _SupersetFalso.ultimo.chamadas == [("renomeia", 1, *NOVO_1), ("sincroniza", 1)]
    assert "ERRO [   2]" in saida and "1 renomeado(s), 1 com erro." in saida


def test_nada_a_fazer(rodar, monkeypatch):
    monkeypatch.setattr(_SupersetFalso, "datasets", lambda self: [])
    codigo, saida = rodar("--apply")
    assert codigo == 0 and "Nada a fazer." in saida
