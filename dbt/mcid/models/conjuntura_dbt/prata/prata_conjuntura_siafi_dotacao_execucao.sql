{{ config(materialized='table') }}

-- Prata do conjuntura: OGU — dotação/execução MCID (SIAFI/Tesouro).
-- Página 5, seção 6.
--
-- Reescrita em 2026-08-28 pra nova arquitetura: a bronze materializa o
-- parquet de staging e a prata TIPA. O parquet novo é espelho do raw e
-- traz os valores como texto, além de não trazer `dt_ingest` (o ouro
-- dependia dessa coluna — quebrava com "column dt_ingest does not exist").
--
-- Os valores vêm em formato pt-BR ("1.000.971,15") e com string vazia
-- em linha sem execução, então o cast usa o macro `parse_financial_value`
-- que já existe no projeto e trata os dois casos (vazio vira 0,00).
-- O wrapper `parse_valor_siafi` acrescenta o tratamento de negativo em
-- notação contábil — "(6570011.00)" = -6.570.011,00.
--
-- Ingestão nova (plugins/ingestion, 10/2026): a staging guarda o cabeçalho do
-- relatório como veio. As 20 colunas de dimensão chegam sem nome (`column_1` a
-- `column_20`, na ordem do relatório) e as de valor com o título do Tesouro; o
-- renome é aqui. Os códigos mantêm o zero à esquerda (`0032`, `04`), que a
-- ingestão antiga perdia ao passar pelo pandas. `dt_ingest` é a partição da
-- ingestão, tirada do `filename`.

select
    column_1                                as unidade_orcamentaria_codigo,
    column_2                                as unidade_orcamentaria_nome,
    column_3                                as acao_governo_codigo,
    column_4                                as acao_governo_nome,
    column_5                                as programa_governo_codigo,
    column_6                                as programa_governo_nome,
    column_7                                as plano_orcamentario_codigo,
    column_8                                as plano_orcamentario_funcao,
    column_9                                as plano_orcamentario_subfuncao,
    column_10                               as plano_orcamentario_programa,
    column_11                               as plano_orcamentario_acao,
    column_12                               as plano_orcamentario_medida,
    column_13                               as plano_orcamentario_descricao,
    column_14                               as elemento_despesa_codigo,
    column_15                               as elemento_despesa_nome,
    column_16                               as orgao_uge_codigo,
    column_17                               as orgao_uge_nome,
    column_18                               as uge_matriz_filial,
    column_19                               as ug_executora_codigo,
    column_20                               as ug_executora_nome,
    {{ parse_valor_siafi('"PROJETO INICIAL DA LOA - FIXACAO DESPESA"') }}
        as fixacao_despesa_loa,
    {{ parse_valor_siafi('"DOTACAO INICIAL"') }}
        as dotacao_inicial,
    {{ parse_valor_siafi('"DOTACAO ATUALIZADA"') }}
        as dotacao_atualizada,
    {{ parse_valor_siafi('"CREDITO DISPONIVEL"') }}
        as credito_disponivel,
    {{ parse_valor_siafi('"DESPESAS EMPENHADAS (CONTROLE EMPENHO)"') }}
        as despesas_empenhadas,
    {{ parse_valor_siafi('"DESPESAS EMPENHADAS A LIQUIDAR (CONTROLE EMP)"') }}
        as despesas_empenhadas_a_liquidar,
    {{ parse_valor_siafi('"DESPESAS LIQUIDADAS A PAGAR(CONTROLE EMPENHO)"') }}
        as despesas_liquidadas_a_pagar,
    {{ parse_valor_siafi('"DESPESAS PAGAS (CONTROLE EMPENHO)"') }}
        as despesas_pagas,
    {{ parse_valor_siafi('"RESTOS A PAGAR INSCRITOS (PROC E N PROC)"') }}
        as restos_a_pagar_inscritos,
    {{ parse_valor_siafi('"RESTOS A PAGAR PAGOS (PROC E N PROC)"') }}
        as restos_a_pagar_pagos,
    {{ lake_dt_ingest() }}                  as dt_ingest
from {{ ref('bronze_siafi_dotacao_execucao') }}
