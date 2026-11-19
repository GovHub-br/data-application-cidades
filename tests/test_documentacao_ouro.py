"""Catraca da documentação da ouro e o contrato de `meta.documentacao`.

A lista de pendentes em `governance/documentacao_pendente.yml` só encolhe:
model novo na ouro já nasce documentado, e model que passou a cumprir o
contrato sai da lista no mesmo PR.
"""

from __future__ import annotations

from pathlib import Path

import yaml

import auditar_metadados as auditoria
import documentacao_dbt

RAIZ = Path(__file__).resolve().parents[1]

PENDENTES = set(
    yaml.safe_load(
        (RAIZ / "dbt" / "mcid" / "governance" / "documentacao_pendente.yml").read_text(
            encoding="utf-8"
        )
    )["pendentes"]
)
ACHADOS = auditoria.auditar_ouro(auditoria.DEFAULT_MODELS)


def test_model_da_ouro_fora_da_lista_cumpre_o_contrato() -> None:
    novos = {nome: p for nome, p in ACHADOS.items() if nome not in PENDENTES}
    assert not novos, (
        "Models da ouro sem documentação completa (veja "
        "`auditar_metadados.py --ouro`): "
        + "; ".join(f"{nome}: {', '.join(p)}" for nome, p in sorted(novos.items()))
    )


def test_model_documentado_sai_da_lista_de_pendentes() -> None:
    resolvidos = sorted(PENDENTES - set(ACHADOS))
    assert not resolvidos, "Já cumprem o contrato; tire de documentacao_pendente.yml: " + ", ".join(
        resolvidos
    )


DOCUMENTACAO = {
    "grao": "Uma linha por empreendimento.",
    "chave": ["apf"],
    "fonte": "CAIXA, acompanhamento de obras.",
    "periodicidade": "mensal",
    "consumo": ["Superset, ficha do empreendimento"],
}


def test_documentacao_completa_nao_tem_problema() -> None:
    assert documentacao_dbt.problemas(DOCUMENTACAO, {"apf", "uf"}) == []


def test_documentacao_aponta_campo_faltando_e_chave_inexistente() -> None:
    incompleta = {**DOCUMENTACAO, "chave": ["apf", "mes"], "periodicidade": "quinzenal"}
    del incompleta["fonte"]

    assert documentacao_dbt.problemas(incompleta, {"apf"}) == [
        "documentacao_sem_fonte",
        "documentacao_chave_mes_nao_declarada",
        "documentacao_periodicidade_fora_do_vocabulario",
    ]
    assert documentacao_dbt.problemas(None, set()) == ["documentacao_ausente"]


def test_documentacao_vem_de_config_meta_ou_meta() -> None:
    assert (
        documentacao_dbt.da_configuracao({"config": {"meta": {"documentacao": DOCUMENTACAO}}})
        == DOCUMENTACAO
    )
    assert (
        documentacao_dbt.da_configuracao({"meta": {"documentacao": DOCUMENTACAO}}) == DOCUMENTACAO
    )
    assert documentacao_dbt.da_configuracao({"meta": {}}) is None


def test_propriedades_no_openmetadata() -> None:
    assert documentacao_dbt.propriedades(
        {**DOCUMENTACAO, "chave": ["apf", "mes"], "consumo": ["painel A", "painel B"]}
    ) == {
        "mcidGrao": "Uma linha por empreendimento.",
        "mcidChave": "apf, mes",
        "mcidFonte": "CAIXA, acompanhamento de obras.",
        "mcidPeriodicidade": "mensal",
        "mcidConsumo": "- painel A\n- painel B",
    }


def _node(documentacao: dict) -> dict:
    return {
        "name": "ouro_far_ficha",
        "schema": "ouro",
        "description": "Ficha do empreendimento.",
        "meta": {
            "governance": {
                "layer": "gold",
                "rag_publication": "eligible_after_security_validation",
            }
        },
        "config": {"meta": {"documentacao": documentacao}},
        "columns": {},
    }


def test_exportador_leva_documentacao_e_barra_texto_improprio() -> None:
    import exportar_catalogo_openmetadata as exportador

    catalogo = {"nodes": {"model.x": {"columns": {"apf": {"type": "text", "index": 1}}}}}
    violacoes: list[str] = []
    exportado = exportador.model_export("model.x", _node(DOCUMENTACAO), catalogo, violacoes)
    assert exportado["documentacao"] == DOCUMENTACAO
    assert violacoes == []

    improprio = {**DOCUMENTACAO, "fonte": "CAIXA, por exemplo a planilha de obras."}
    exportador.model_export("model.x", _node(improprio), catalogo, violacoes)
    assert violacoes == ["model.x: meta.documentacao com exemplo"]


def test_sync_le_as_propriedades_do_catalogo(tmp_path, monkeypatch) -> None:
    import json

    import sincronizar_governanca as gov

    catalogo = tmp_path / "catalogo.json"
    catalogo.write_text(
        json.dumps(
            {
                "models": [
                    {"name": "ouro_far_ficha", "documentacao": DOCUMENTACAO},
                    {"name": "ouro_sem_contrato", "documentacao": {}},
                ]
            }
        )
    )
    monkeypatch.setattr(gov, "CATALOGO", catalogo)

    assert gov.documentacao_por_modelo() == {
        "ouro_far_ficha": documentacao_dbt.propriedades(DOCUMENTACAO)
    }


def _auditar(monkeypatch, capsys, tmp_path, *argv) -> tuple[int, str]:
    import json
    import sys

    modelos = tmp_path / "models"
    modelos.mkdir()
    (modelos / "schema.yml").write_text(
        "models:\n"
        "  - name: documentado\n"
        "    description: Quantidade de contratos assinados no mês.\n"
        "    columns:\n"
        "      - name: sem_texto\n"
    )
    manifesto = tmp_path / "manifest.json"
    nos = {"documentado": "a.sql", "esquecido": "models/esquecido.sql"}
    manifesto.write_text(
        json.dumps(
            {"nodes": {n: {"name": n, "path": p, "resource_type": "model"} for n, p in nos.items()}}
        )
    )
    monkeypatch.setattr(auditoria, "DEFAULT_EXTRAS", ())
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "auditar_metadados.py",
            "--models-root",
            str(modelos),
            "--manifest",
            str(manifesto),
            *argv,
        ],
    )
    return auditoria.main(), capsys.readouterr().out


def test_auditoria_conta_e_lista_os_achados(monkeypatch, capsys, tmp_path) -> None:
    codigo, saida = _auditar(monkeypatch, capsys, tmp_path)
    assert codigo == 0
    assert "Nós do projeto com entrada em YAML: 1/2 (50%)" in saida
    assert "Recursos com descrição efetiva: 1/1 (100%)  ·  Colunas: 0/1 (0%)" in saida
    assert "models.documentado.sem_texto — descricao_ausente" in saida
    assert "models/esquecido.sql: model.esquecido — sem_entrada_yaml" in saida


def test_auditoria_resumo_e_estrita(monkeypatch, capsys, tmp_path) -> None:
    codigo, saida = _auditar(monkeypatch, capsys, tmp_path, "--resumo", "--strict")
    assert codigo == 1
    assert "Achados por regra: descricao_ausente=1, sem_entrada_yaml=1" in saida
    assert "sem_texto" not in saida
