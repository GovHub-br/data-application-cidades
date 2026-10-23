"""Contrato de documentação declarativa dos models da ouro.

Cada model da ouro declara, além da `description` em texto livre, o que um
consumidor precisa saber antes de usar a tabela:

    config:
      meta:
        documentacao:
          grao: Uma linha por empreendimento FAR.
          chave: [apf]
          fonte: CAIXA, acompanhamento de obras do FAR.
          periodicidade: mensal
          consumo: [Superset, ficha do empreendimento FAR]

No OpenMetadata cada campo vira uma propriedade customizada da tabela, e não
um trecho anexado à `description`: a descrição volta ao git quando alguém
sugere outra pelo catálogo, e um texto gerado misturado nela viria junto.
"""

from __future__ import annotations

from typing import Any, Mapping

CAMPOS = ("grao", "chave", "fonte", "periodicidade", "consumo")
CAMPOS_DE_TEXTO = ("grao", "fonte")

PERIODICIDADES = (
    "diaria",
    "semanal",
    "mensal",
    "trimestral",
    "semestral",
    "anual",
    "eventual",
)

#: campo do contrato → propriedade customizada da tabela no OpenMetadata
PROPRIEDADES = {
    "grao": "mcidGrao",
    "chave": "mcidChave",
    "fonte": "mcidFonte",
    "periodicidade": "mcidPeriodicidade",
    "consumo": "mcidConsumo",
}


def da_configuracao(recurso: Mapping[str, Any]) -> dict[str, Any] | None:
    """`documentacao` do model, em `config.meta` (forma atual) ou `meta`."""
    config = recurso.get("config") or {}
    for meta in (config.get("meta") or {}, recurso.get("meta") or {}):
        documentacao = meta.get("documentacao")
        if documentacao is not None:
            return documentacao if isinstance(documentacao, dict) else {}
    return None


def problemas(documentacao: Mapping[str, Any] | None, colunas: set[str]) -> list[str]:
    """Regras estruturais; o texto é conferido por quem chama, com as regras dele."""
    if documentacao is None:
        return ["documentacao_ausente"]
    achados = [
        f"documentacao_sem_{campo}" for campo in CAMPOS if not documentacao.get(campo)
    ]
    chave = documentacao.get("chave")
    if chave and not isinstance(chave, list):
        achados.append("documentacao_chave_nao_e_lista")
    elif chave and colunas:
        achados += [
            f"documentacao_chave_{coluna}_nao_declarada"
            for coluna in chave
            if coluna not in colunas
        ]
    consumo = documentacao.get("consumo")
    if consumo and not isinstance(consumo, list):
        achados.append("documentacao_consumo_nao_e_lista")
    periodicidade = documentacao.get("periodicidade")
    if periodicidade and periodicidade not in PERIODICIDADES:
        achados.append("documentacao_periodicidade_fora_do_vocabulario")
    return achados


def textos(documentacao: Mapping[str, Any]) -> list[str]:
    """Todo texto livre do contrato, para passar pelas regras de publicação."""
    valores = [str(documentacao.get(campo) or "") for campo in CAMPOS_DE_TEXTO]
    valores += [str(item) for item in documentacao.get("consumo") or []]
    return [valor for valor in valores if valor]


def propriedades(documentacao: Mapping[str, Any]) -> dict[str, str]:
    """Valores das propriedades customizadas da tabela, só dos campos preenchidos."""
    valores = {
        "grao": documentacao.get("grao"),
        "chave": ", ".join(documentacao.get("chave") or []),
        "fonte": documentacao.get("fonte"),
        "periodicidade": documentacao.get("periodicidade"),
        "consumo": "\n".join(f"- {item}" for item in documentacao.get("consumo") or []),
    }
    return {
        PROPRIEDADES[campo]: str(valor).strip()
        for campo, valor in valores.items()
        if valor
    }
