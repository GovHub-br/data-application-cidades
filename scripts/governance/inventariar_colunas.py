#!/usr/bin/env python3
"""Levanta o inventário real de colunas e propõe o contrato Silver.

Por que existe: documentar coluna e declarar contrato precisavam do mesmo
insumo — a lista verdadeira de colunas com tipo — e esse insumo estava sendo
adivinhado. O resultado foi documentação por casamento de prefixo, que
preenchia o catálogo sem dizer nada, e contrato declarado em um único model
de trinta e seis.

O inventário vem do `information_schema` e das estatísticas do próprio banco,
não do manifesto: tipo declarado no dbt pode divergir do tipo materializado, e
é o materializado que o consumidor encontra.

Além da estrutura, mede o que sustenta o contrato:

  - fração de nulos  -> `not_null_columns` e `min_completeness`
  - cardinalidade    -> distingue chave de atributo

São contagens, não conteúdo: nenhum valor de linha é lido, coletado ou
gravado. A documentação do produto é semântica — descreve o que a coluna
significa, e o significado não depende de olhar o que está dentro dela.

Uso:
    poetry run python scripts/governance/inventariar_colunas.py \\
        --schema conjuntura_continuo_silver --contrato

    poetry run python scripts/governance/inventariar_colunas.py \\
        --schema conjuntura_continuo_mart --tabela gold_continuo_icst
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import textwrap

import psycopg2

RAIZ = pathlib.Path(__file__).resolve().parents[2]

#: Colunas que se repetem em muitos models e têm significado estável no
#: produto. Documentadas uma vez, aqui, em vez de trinta e seis vezes.
#: Não confundir com preenchimento por prefixo: cada uma destas existe de
#: fato com este significado, e a lista é curta e fechada de propósito.
COMUNS = {
    "periodo": "Rótulo do período a que a observação se refere.",
    "data_referencia": "Primeiro dia do período a que a observação se refere.",
    "edicao": "Trimestre a que a observação pertence, no formato usado pelo boletim.",
    "ano": "Ano do período de referência.",
    "mes": "Mês do período de referência.",
    "trimestre": "Trimestre do período de referência.",
    "dt_ingest": "Momento em que o registro entrou no acervo.",
    "variavel_id": "Código da variável pesquisada, conforme a numeração da pesquisa de origem.",
    "variavel": "Nome da variável pesquisada.",
    "unidade": "Unidade de medida em que o valor é expresso.",
    "localidade_id": "Código da localidade a que a observação se refere.",
    "localidade": "Nome da localidade a que a observação se refere.",
    "classificacao_id": "Código do eixo de classificação aplicado ao recorte.",
    "classificacao": "Nome do eixo de classificação aplicado ao recorte.",
    "categoria_id": "Código da categoria dentro do eixo de classificação.",
    "categoria": "Nome da categoria dentro do eixo de classificação.",
}


def conectar():
    for linha in (RAIZ / ".env").read_text(encoding="utf-8").splitlines():
        linha = linha.strip()
        if linha and not linha.startswith("#") and "=" in linha:
            chave, _, valor = linha.partition("=")
            os.environ.setdefault(chave.strip(), valor.strip().strip('"').strip("'"))
    return psycopg2.connect(
        host=os.environ["DB_DW_HOST_MCID"],
        port=os.environ.get("DB_DW_PORT_MCID", 5432),
        dbname=os.environ["DB_DW_DBNAME_MCID"],
        user=os.environ["DB_DW_USER_MCID"],
        password=os.environ["DB_DW_PASSWORD_MCID"],
    )


def colunas_do_schema(cur, schema: str, tabela: str | None) -> dict[str, list[tuple[str, str]]]:
    cur.execute(
        """
        select table_name, column_name, data_type
        from information_schema.columns
        where table_schema = %s and (%s is null or table_name = %s)
        order by table_name, ordinal_position
        """,
        (schema, tabela, tabela),
    )
    saida: dict[str, list[tuple[str, str]]] = {}
    for tab, col, tipo in cur.fetchall():
        saida.setdefault(tab, []).append((col, tipo))
    return saida


def perfil(cur, schema: str, tabela: str, colunas: list[tuple[str, str]]) -> dict:
    """Contagens de preenchimento e cardinalidade. Não lê valores de linha."""
    cur.execute(f'select count(*) from "{schema}"."{tabela}"')
    linhas = cur.fetchone()[0]
    if not linhas:
        return {"linhas": 0, "colunas": {}}

    medidas = ", ".join(
        f'count("{c}") as p_{i}, count(distinct "{c}") as d_{i}'
        for i, (c, _) in enumerate(colunas)
    )
    cur.execute(f'select {medidas} from "{schema}"."{tabela}"')
    valores = cur.fetchone()

    resultado = {"linhas": linhas, "colunas": {}}
    for i, (nome, tipo) in enumerate(colunas):
        preenchidas, distintos = valores[i * 2], valores[i * 2 + 1]
        info = {
            "tipo": tipo,
            "completude": round(preenchidas / linhas, 4),
            "distintos": distintos,
        }
        resultado["colunas"][nome] = info
    return resultado


def bloco_yaml(tabela: str, perfil_tab: dict, com_contrato: bool) -> str:
    colunas = perfil_tab["colunas"]
    linhas = [f"  - name: {tabela}", "    description: >", "      TODO"]

    if com_contrato:
        nomes = list(colunas)
        sem_nulo = [n for n, i in colunas.items() if i["completude"] == 1.0]
        temporal = next(
            (n for n in ("data_referencia", "data", "mes") if n in colunas), None
        )
        linhas += [
            "    data_tests:",
            "      - sem_coluna_sensivel:",
            "          config:",
            "            severity: error",
            "      - silver_contract:",
            "          config:",
            "            severity: warn",
            "          arguments:",
            "            expected_columns:",
        ]
        linhas += [f"              - {n}" for n in nomes]
        linhas.append("            allow_additional_columns: false")
        if sem_nulo:
            linhas.append("            not_null_columns:")
            linhas += [f"              - {n}" for n in sem_nulo]
        if temporal:
            linhas += [
                f"            freshness_column: {temporal}",
                "            freshness_days: 120",
            ]
        linhas.append("            expected_data_types:")
        linhas += [f"              {n}: {i['tipo']}" for n, i in colunas.items()]

    linhas.append("    columns:")
    for nome, info in colunas.items():
        linhas.append(f"      - name: {nome}")
        texto = COMUNS.get(nome)
        if texto:
            linhas.append("        description: >")
            linhas += [f"          {l}" for l in textwrap.wrap(texto, 70)]
        else:
            linhas.append("        description: >")
            linhas.append("          TODO")
    return "\n".join(linhas)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--schema", required=True)
    p.add_argument("--tabela")
    p.add_argument("--contrato", action="store_true", help="inclui o silver_contract")
    p.add_argument("--yaml", type=pathlib.Path, help="grava o YAML proposto")
    p.add_argument("--perfil", type=pathlib.Path, help="grava as contagens (estrutura, sem conteúdo)")
    args = p.parse_args()

    conexao = conectar()
    cur = conexao.cursor()
    inventario = colunas_do_schema(cur, args.schema, args.tabela)
    if not inventario:
        print(f"Nada encontrado em {args.schema}", file=sys.stderr)
        return 1

    perfis, blocos = {}, []
    for tabela, colunas in inventario.items():
        perfis[tabela] = perfil(cur, args.schema, tabela, colunas)
        blocos.append(bloco_yaml(tabela, perfis[tabela], args.contrato))
    conexao.close()

    total = sum(len(v["colunas"]) for v in perfis.values())
    a_descrever = sum(
        1 for v in perfis.values() for n in v["colunas"] if n not in COMUNS
    )
    print(
        f"{args.schema}: {len(inventario)} tabelas, {total} colunas — "
        f"{total - a_descrever} já cobertas pelo vocabulário comum, "
        f"{a_descrever} a descrever."
    )

    if args.yaml:
        args.yaml.write_text("version: 2\n\nmodels:\n" + "\n".join(blocos) + "\n")
        print(f"YAML proposto: {args.yaml}")
    if args.perfil:
        args.perfil.write_text(json.dumps(perfis, ensure_ascii=False, indent=2))
        print(f"Contagens: {args.perfil}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
