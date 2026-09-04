{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: financiamentos imobiliários PF/PJ (BACEN SGS).
-- Página 2/3, seção 3.
--
-- Contrato de saída: formato longo (data, tipo, valor), que é o que o gold
-- consome. São sete tipos — seis do SGS mais o índice crédito/PIB, que vem
-- de outro endpoint e por isso mora em parquet separado.
--
-- Lê o parquet como a nossa DAG o grava: já achatado em (tipo, data, valor).
-- A versão anterior desta silver despivotava sete colunas de JSON aninhado,
-- partindo de um staging que espelhava o raw da API. Esse pressuposto veio do
-- refactor de 28/08 e foi abandonado em 30/08 para o IBGE, quando as silvers
-- viraram passthrough tipado; BACEN e CAGED ficaram para trás e só não
-- quebraram porque as DAGs não rodaram nesse intervalo. Aqui a correção é a
-- mesma: a Etapa 02 achata, a silver tipa.
--
-- `data` chega como texto DD/MM/YYYY e vira date aqui — sem isso o
-- `order by data desc` do gold ordenaria alfabeticamente, e 01/12/2025
-- passaria a valer mais que 01/07/2026.

with sgs as (
    select
        to_date(data, 'DD/MM/YYYY')             as data,
        tipo                                    as tipo,
        nullif(btrim(valor), '')::numeric       as valor
    from {{ ref('bnz_bacen_financiamentos_imobiliarios') }}
),

credito_pib as (
    select
        data                                    as data,
        'indice_imobiliario_por_pib'            as tipo,
        valor                                   as valor
    from {{ ref('slv_bacen_credito_pib') }}
)

select data, tipo, valor from sgs
union all
select data, tipo, valor from credito_pib
