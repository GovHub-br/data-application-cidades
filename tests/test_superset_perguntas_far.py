from __future__ import annotations

import perguntas_far


def chaves_no_layout(m) -> list[str]:
    usadas = []
    for _, blocos in m.ABAS:
        for bloco in blocos:
            if bloco[0] in ("pair", "chart"):
                usadas.append(bloco[1])
            elif bloco[0] == "dupla":
                usadas.extend(bloco[1:])
            elif bloco[0] == "charts":
                usadas.extend(bloco[1])
    return usadas


def test_todo_grafico_esta_no_layout_uma_vez() -> None:
    m = perguntas_far
    usadas = chaves_no_layout(m)
    assert sorted(usadas) == sorted(c["key"] for c in m.CHARTS)


def test_todo_par_tem_como_ler() -> None:
    m = perguntas_far
    pares = {b[1] for _, blocos in m.ABAS for b in blocos if b[0] == "pair"}
    assert pares == set(m.COMO)


def test_graficos_e_filtros_usam_datasets_declarados() -> None:
    m = perguntas_far
    assert {c["dataset"] for c in m.CHARTS} <= set(m.DATASETS)
    assert {d for _, _, alvos, _ in m.FILTROS for d in alvos} <= set(m.DATASETS)
    assert all(aba < len(m.ABAS) for *_, aba in m.FILTROS)


def test_nao_reescreve_o_painel_far_existente() -> None:
    m = perguntas_far
    assert m.DASHBOARD_SLUG == "perguntas-far"
    assert m.PREFIXO == "FAR ▸ "
    assert "ouro_far_serie_mensal_apf" in m.DATASETS_COMPARTILHADOS


def test_ultima_aba_e_o_que_falta() -> None:
    m = perguntas_far
    nome, blocos = m.ABAS[-1]
    assert nome == "O que falta"
    assert any(b[0] == "chart" and b[1] == "cobertura" for b in blocos)
