"""Descrição por convenção dos campos sem YAML: cada regra de nome, na ordem de prioridade."""

import pytest

from conjuntura import gerar_doc_pipeline as g

CASOS = {
    "periodo": "Rótulo do período de referência da observação.",
    "var_x": "Variação percentual do indicador, conforme o recorte indicado no nome do campo.",
    "a_var_b": "Variação percentual do indicador, conforme o recorte indicado no nome do campo.",
    "qtd_x": "Quantidade do fenômeno medido no período de referência.",
    "quantidade_y": "Quantidade do fenômeno medido no período de referência.",
    "valor_x": "Valor monetário do indicador no período de referência.",
    "vlr_y": "Valor monetário do indicador no período de referência.",
    "financ_x": "Medida de financiamento habitacional no recorte indicado pelo campo.",
    "financiamento_y": "Medida de financiamento habitacional no recorte indicado pelo campo.",
    "ticket_x": "Ticket médio de lançamentos no recorte indicado pelo campo.",
    "perc_x": "Participação percentual no recorte indicado pelo campo.",
    "x_perc": "Participação percentual no recorte indicado pelo campo.",
    "x_percentual": "Participação percentual no recorte indicado pelo campo.",
    "total_x": "Total agregado do indicador no período de referência.",
    "x_total": "Total agregado do indicador no período de referência.",
    "saldo_x": "Saldo do indicador no período de referência.",
    "taxa_x": "Taxa associada ao indicador no período de referência.",
    "precos_x": "Medida de preço ou custo no período de referência.",
    "custo_y": "Medida de preço ou custo no período de referência.",
    "outro": "Atributo técnico do indicador; consultar a descrição da tabela para o contexto e a fonte.",
    "var_total": "Variação percentual do indicador, conforme o recorte indicado no nome do campo.",
    "perc_total": "Participação percentual no recorte indicado pelo campo.",
    "valor_total": "Valor monetário do indicador no período de referência.",
}


@pytest.mark.parametrize("nome, esperado", CASOS.items())
def test_descricao_semantica(nome, esperado):
    assert g.descricao_semantica(nome) == esperado


def test_gerar_monta_o_bloco_de_cada_model(tmp_path, capsys):
    import json

    fonte = "source.mcid.lake_staging.ibge_x"
    silver, gold = "model.mcid.prata_x", "model.mcid.ouro_x"
    manifest = {
        "sources": {
            fonte: {"source_name": "lake_staging", "name": "ibge_x", "meta": {"caminho": "ibge/x"}}
        },
        "nodes": {
            silver: {
                "resource_type": "model",
                "name": "prata_x",
                "path": "conjuntura_dbt/silver/prata_x.sql",
                "config": {"materialized": "table"},
                "description": "Série da prata.",
                "columns": {"valor_x": {"description": ""}},
                "depends_on": {"nodes": [fonte]},
            },
            gold: {
                "resource_type": "model",
                "name": "ouro_x",
                "path": "conjuntura_dbt/gold/ouro_x.sql",
                "config": {"materialized": "view"},
                "columns": {},
                "depends_on": {"nodes": [silver]},
            },
            "test.mcid.not_null_prata_x": {
                "resource_type": "test",
                "name": "not_null_prata_x_valor_x",
                "depends_on": {"nodes": [silver]},
            },
        },
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    saida = tmp_path / "pipeline.html"
    g.gerar(saida, tmp_path, incluir_contagens=False)
    html = saida.read_text()
    assert html.count('<div class="model ') == 2
    assert "<b>lê do lake:</b> <code>ibge/x</code>" in html
    assert "<b>vem de:</b> <code>prata_x</code>" in html
    assert "<b>alimenta:</b> <code>ouro_x</code>" in html
    assert "<b>1 testes:</b>" in html and "Série da prata." in html
    assert "Campos (1)" in html and g.descricao_semantica("valor_x") in html
    assert "1 fontes" in capsys.readouterr().out
