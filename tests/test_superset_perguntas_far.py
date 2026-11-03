from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]


def modulo():
    caminho = RAIZ / "scripts" / "superset" / "perguntas_far.py"
    sys.path.insert(0, str(caminho.parent))
    spec = importlib.util.spec_from_file_location("superset_perguntas_far", caminho)
    assert spec and spec.loader
    carregado = importlib.util.module_from_spec(spec)
    sys.modules["superset_perguntas_far"] = carregado
    spec.loader.exec_module(carregado)
    return carregado


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
    m = modulo()
    usadas = chaves_no_layout(m)
    assert sorted(usadas) == sorted(c["key"] for c in m.CHARTS)


def test_todo_par_tem_como_ler() -> None:
    m = modulo()
    pares = {b[1] for _, blocos in m.ABAS for b in blocos if b[0] == "pair"}
    assert pares == set(m.COMO)


def test_graficos_e_filtros_usam_datasets_declarados() -> None:
    m = modulo()
    assert {c["dataset"] for c in m.CHARTS} <= set(m.DATASETS)
    assert {d for _, _, alvos, _ in m.FILTROS for d in alvos} <= set(m.DATASETS)
    assert all(aba < len(m.ABAS) for *_, aba in m.FILTROS)


def test_nao_reescreve_o_painel_far_existente() -> None:
    m = modulo()
    assert m.DASHBOARD_SLUG == "perguntas-far"
    assert m.PREFIXO == "FAR ▸ "
    assert "ouro_far_serie_mensal_apf" in m.DATASETS_COMPARTILHADOS


def test_ultima_aba_e_o_que_falta() -> None:
    m = modulo()
    nome, blocos = m.ABAS[-1]
    assert nome == "O que falta"
    assert any(b[0] == "chart" and b[1] == "cobertura" for b in blocos)
