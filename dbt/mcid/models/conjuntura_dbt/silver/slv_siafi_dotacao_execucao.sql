{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: OGU — dotação/execução MCID (SIAFI/Tesouro).
-- Página 5, seção 6.
--
-- Reescrita em 2026-08-28 pra nova arquitetura: a bronze materializa o
-- parquet de staging e a silver TIPA. O parquet novo é espelho do raw e
-- traz os valores como texto, além de não trazer `dt_ingest` (o gold
-- dependia dessa coluna — quebrava com "column dt_ingest does not exist").
--
-- Os valores vêm em formato pt-BR ("1.000.971,15") e com string vazia
-- em linha sem execução, então o cast usa o macro `parse_financial_value`
-- que já existe no projeto e trata os dois casos (vazio vira 0,00).
-- O wrapper `parse_valor_siafi` acrescenta o tratamento de negativo em
-- notação contábil — "(6570011.00)" = -6.570.011,00.

select
    unidade_orcamentaria_codigo,
    unidade_orcamentaria_nome,
    acao_governo_codigo,
    acao_governo_nome,
    programa_governo_codigo,
    programa_governo_nome,
    plano_orcamentario_codigo,
    plano_orcamentario_funcao,
    plano_orcamentario_subfuncao,
    plano_orcamentario_programa,
    plano_orcamentario_acao,
    plano_orcamentario_medida,
    plano_orcamentario_descricao,
    elemento_despesa_codigo,
    elemento_despesa_nome,
    orgao_uge_codigo,
    orgao_uge_nome,
    uge_matriz_filial,
    ug_executora_codigo,
    ug_executora_nome,
    {{ parse_valor_siafi('fixacao_despesa_loa') }}            as fixacao_despesa_loa,
    {{ parse_valor_siafi('dotacao_inicial') }}                as dotacao_inicial,
    {{ parse_valor_siafi('dotacao_atualizada') }}             as dotacao_atualizada,
    {{ parse_valor_siafi('credito_disponivel') }}             as credito_disponivel,
    {{ parse_valor_siafi('despesas_empenhadas') }}            as despesas_empenhadas,
    {{ parse_valor_siafi('despesas_empenhadas_a_liquidar') }} as despesas_empenhadas_a_liquidar,
    {{ parse_valor_siafi('despesas_liquidadas_a_pagar') }}    as despesas_liquidadas_a_pagar,
    {{ parse_valor_siafi('despesas_pagas') }}                 as despesas_pagas,
    {{ parse_valor_siafi('restos_a_pagar_inscritos') }}       as restos_a_pagar_inscritos,
    {{ parse_valor_siafi('restos_a_pagar_pagos') }}           as restos_a_pagar_pagos,
    _ingested_at                            as dt_ingest
from {{ ref('bnz_siafi_dotacao_execucao') }}
