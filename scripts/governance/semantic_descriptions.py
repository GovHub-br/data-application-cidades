"""Descrições semânticas seguras para metadados ainda não curados.

Não lê tabelas nem tenta inferir valores. Serve somente para não deixar um
campo sem contexto no catálogo enquanto a descrição curada no YAML é criada.
"""

from __future__ import annotations

STANDARD_DESCRIPTIONS = {
    "periodo": "Rótulo do período de referência da observação.",
    "data_referencia": "Data que identifica o período de referência da observação.",
    "data": "Data de referência da observação.",
    "ano": "Ano do período de referência.",
    "trimestre": "Trimestre do período de referência.",
    "mes": "Mês do período de referência.",
    "edicao": "Identificador da edição editorial do boletim.",
    "empresa": "Empresa ou grupo empresarial associado ao indicador.",
    "banco": "Instituição financeira associada ao indicador.",
    "municipio": "Município associado ao recorte geográfico do indicador.",
    "uf": "Unidade da Federação associada ao recorte geográfico do indicador.",
    "dt_ingest": "Momento de ingestão do registro no pipeline.",
    "dt_silver": "Momento de processamento da camada Silver.",
}


def for_column(name: str) -> str:
    """Retorna descrição neutra e sem exemplos para um identificador técnico."""
    if name in STANDARD_DESCRIPTIONS:
        return STANDARD_DESCRIPTIONS[name]
    if name.startswith(("var_", "variacao_")) or "_var_" in name:
        return "Variação do indicador conforme o recorte definido pela tabela."
    if name.startswith(("qtd_", "qt_", "quantidade_")):
        return "Quantidade do fenômeno medido no período de referência."
    if name.startswith(("valor_", "vlr_", "vr_")):
        return "Valor monetário do indicador no período de referência."
    if name.startswith(("percentual_", "pct_", "perc_")) or name.endswith("_perc"):
        return "Participação ou variação percentual no recorte definido pela tabela."
    if name.startswith("total_") or name.endswith("_total"):
        return "Total agregado do indicador no período de referência."
    if name.startswith("saldo_"):
        return "Saldo do indicador no período de referência."
    if name.startswith("taxa_"):
        return "Taxa associada ao indicador no período de referência."
    if name.startswith(("preco_", "precos_", "custo_")):
        return "Medida de preço ou custo no período de referência."
    return "Atributo do indicador no contexto descrito pela tabela."
