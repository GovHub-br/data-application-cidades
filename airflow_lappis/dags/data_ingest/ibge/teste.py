"""
Teste de consulta à API de Dados Agregados do IBGE
===================================================
Busca dados do PIB da Construção Civil trimestral
Fonte: Contas Nacionais Trimestrais (CNT)

Agregado 5932: Taxa de variação do índice de volume trimestral
Classificação 11255: Setores e subsetores
  - Categoria 90694: Construção

Variáveis:
  - 6564: Trim./Trim. Imediatamente Anterior (com ajuste sazonal)
  - 6563: Acumulada ao Longo do Ano
  - 6562: Acumulada Últimos 4 Trimestres
  - 6561: Taxa trimestral (em relação ao mesmo período do ano anterior)
"""

import requests
import json


BASE_URL = "https://servicodados.ibge.gov.br/api/v3"

# Parâmetros da consulta
AGREGADO = 5932  # Taxa de variação do índice de volume trimestral
VARIAVEL = "6564|6563|6562|6561"  # Todas as 4 variáveis de taxa
PERIODOS = "-20"  # Últimos 20 trimestres
NIVEL = "N1"  # Brasil
LOCALIDADE = "1"  # Brasil

# Classificação: Setores e subsetores
CLASSIFICACAO_ID = 11255
CATEGORIA_CONSTRUCAO = 90694  # Construção


def get_pib_construcao():
    """Busca dados do PIB da construção civil trimestral via API IBGE."""

    url = (
        f"{BASE_URL}/agregados/{AGREGADO}"
        f"/periodos/{PERIODOS}"
        f"/variaveis/{VARIAVEL}"
        f"?localidades={NIVEL}[{LOCALIDADE}]"
        f"&classificacao={CLASSIFICACAO_ID}[{CATEGORIA_CONSTRUCAO}]"
    )

    print(f"URL da requisição:\n{url}\n")

    response = requests.get(url)
    response.raise_for_status()

    data = response.json()

    print("=" * 70)
    print("PIB da Construção Civil - Taxa de Variação Trimestral")
    print("=" * 70)

    for variavel in data:
        var_nome = variavel["variavel"]
        var_unidade = variavel["unidade"]
        print(f"\n📊 Variável: {var_nome} ({var_unidade})")
        print("-" * 60)

        # Para cada classificação/resultado
        for resultado in variavel["resultados"]:
            classificacoes = resultado["classificacoes"]
            for clf in classificacoes:
                print(f"   Classificação: {clf['nome']}")
                for cat_id, cat_nome in clf["categoria"].items():
                    print(f"   Categoria: {cat_nome} (ID: {cat_id})")

            # Dados por período
            series = resultado["series"]
            for serie in series:
                localidade = serie["localidade"]["nome"]
                print(f"   Localidade: {localidade}\n")
                print(f"   {'Período':<12} {'Valor':>10}")
                print(f"   {'─' * 12} {'─' * 10}")

                for periodo, valor in serie["serie"].items():
                    # Formatar o período: 199601 -> 1996 Q1
                    ano = periodo[:4]
                    trimestre = periodo[4:]
                    periodo_fmt = f"{ano} Q{int(trimestre)}"
                    valor_fmt = f"{valor}%" if valor and valor != "..." else "N/D"
                    print(f"   {periodo_fmt:<12} {valor_fmt:>10}")

    print("\n" + "=" * 70)
    print("\n📋 Resposta JSON completa salva em 'pib_construcao.json'")

    # Salvar JSON completo para referência
    with open("pib_construcao.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return data


if __name__ == "__main__":
    get_pib_construcao()
