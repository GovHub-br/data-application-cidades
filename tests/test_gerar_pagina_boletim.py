"""montar() da página do boletim: avisos, páginas, seções, quadros e rodapé, na ordem."""

import copy

import pytest

from conjuntura import gerar_pagina_boletim as g

CREDITO_PIB = "ouro_conjuntura_boletim_p4_credito_imobiliario_pib"
LANCAMENTOS = "ouro_conjuntura_boletim_p1_cbic_lancamentos_e_vendas_totais"
EMPRESAS = "ouro_conjuntura_boletim_p2_totais_empresas_variacao"

QUADROS = [
    {"pagina": 1, "secao": "1. Atividade", "titulo": "a", "nota": "", "tabela": LANCAMENTOS},
    {"pagina": 2, "secao": "1. Atividade", "titulo": "b", "nota": "", "tabela": EMPRESAS},
    {"pagina": 2, "secao": "3. Balanços das Empresas", "titulo": "c", "nota": "", "tabela": "t"},
    {"pagina": 4, "secao": "6. Crédito", "titulo": "d", "nota": "", "tabela": CREDITO_PIB},
]
META = {
    "titulo": "1º trimestre",
    "publicado_em": "2026-04-30",
    "leitura": {"lancamentos_e_vendas": "texto da leitura"},
}


@pytest.fixture
def marcadores(monkeypatch):
    for nome in ("tabela_html", "grafico_credito", "cartoes_dos_totais", "cartoes_das_empresas"):
        monkeypatch.setattr(g, nome, lambda *a, _n=nome: f"[{_n}]")
    monkeypatch.setattr(g, "bloco_leitura", lambda texto, gerado: f"[leitura {texto} {gerado}]")
    monkeypatch.setattr(g, "cabecalho_fixo", lambda edicao: "[cabecalho]")
    monkeypatch.setattr(g, "fechamento", lambda meta: "[fechamento]")
    monkeypatch.setattr(g, "ROTULO_DE_BLOCO", {1: "DADOS TRIMESTRAIS", 2: "DADOS TRIMESTRAIS"})


def _montar(origem, validacao=()):
    editorial = {"edicoes": {"1T2026": dict(copy.deepcopy(META), origem=origem)}}
    return g.montar("1T2026", QUADROS, {}, editorial, list(validacao))


def test_ordem_dos_blocos_de_uma_edicao_revisada(marcadores):
    html = _montar("revisado")
    ordem = [
        "[cabecalho]",
        "DADOS TRIMESTRAIS",
        '<span class="n">1.</span>Atividade',
        "[tabela_html]",
        "[cartoes_dos_totais]",
        "[leitura texto da leitura False]",
        "[cartoes_das_empresas]",
        "<h2>Balanços das Empresas</h2>",
        '<span class="n">6.</span>Crédito',
        "[grafico_credito]",
        "[fechamento]",
        "revisada pela equipe",
    ]
    posicoes = [html.index(trecho) for trecho in ordem]
    assert posicoes == sorted(posicoes)
    assert html.count("DADOS TRIMESTRAIS") == 1  # o rótulo só aparece quando muda
    assert html.count("Atividade</h2>") == 1  # a seção que continua não repete o título
    assert "aviso" not in html


def test_edicao_gerada_avisa_e_lista_secoes_ausentes(marcadores):
    html = _montar("gerado")
    assert "Edição redigida por máquina" in html and "Visão MCid" in html
    assert "[leitura texto da leitura True]" in html
    assert "ainda não revisada.</div>" in html


def test_divergencias_viram_aviso(marcadores):
    validacao = [
        {"status": "DIVERGE", "indicador": "PIB", "esperado": 1.0, "obtido": 2.0},
        {"status": "OK", "indicador": "X", "esperado": 1.0, "obtido": 1.0},
    ]
    html = _montar("transcrito", validacao)
    assert "1 de 2 células divergem" in html and "<b>PIB</b>" in html
    assert "Texto editorial transcrito de <i>boletim publicado</i>" in html
