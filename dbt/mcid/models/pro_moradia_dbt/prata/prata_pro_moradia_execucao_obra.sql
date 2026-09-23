{{ config(materialized="table") }}

-- Prata: Execução de obra do Pró-Moradia, avaliação mensal
-- Fonte: bronze_shpt_fgts_canal_execucoes_obras, restrita aos contratos de
-- prata_pro_moradia_contrato. A tabela de origem não tem dígito verificador: `cod_contrato`
-- sozinho já é único no canal.
--
-- Grão: contrato × mês de avaliação (conferido único em 2026-09: 12.796 linhas, 665 contratos).
--
-- A origem traz meses FUTUROS (até 2027-12): são linhas do cronograma, com o previsto
-- preenchido e o realizado ainda não medido. Ficam, marcadas em `ic_avaliacao_futura`, porque o
-- previsto é o que permite medir atraso; a última medição válida filtra por elas.

with
    contrato as (
        select cod_contrato from {{ ref("prata_pro_moradia_contrato") }}
    ),

    situacao_obra as (
        select trim(codigo::text) as codigo, trim(situacao_da_obra::text) as nome
        from {{ ref("bronze_shpt_fgts_canal_tdom_situacao_obra") }}
    )

select
    trim(x.cod_contrato::text) as cod_contrato,
    to_date(trim(x.dte_ano_mes_avaliacao::text) || '01', 'YYYYMMDD') as dt_avaliacao,
    {{ parse_numeric("x.prc_prev_acum_mes::text", "numeric(6, 2)") }} as percentual_obra_previsto,
    {{ parse_numeric("x.prc_real_acum_mes::text", "numeric(6, 2)") }} as percentual_obra_realizado,
    nullif(trim(x.cod_situacao_obra::text), '') as situacao_obra_codigo,
    so.nome as situacao_obra,
    nullif(trim(x.cod_execucao_obra::text), '') as execucao_obra_codigo,
    case
        when trim(x.dte_ultima_vistoria::text) ~ '^\d{2}/\d{2}/\d{2}'
        then to_date(left(trim(x.dte_ultima_vistoria::text), 8), 'MM/DD/YY')
    end as dt_ultima_vistoria,
    nullif(trim({{ target.schema }}.corrigir_mojibake(x.txt_providencias::text)), '') as providencias,
    to_date(trim(x.dte_ano_mes_avaliacao::text) || '01', 'YYYYMMDD')
    > date_trunc('month', current_date)::date as ic_avaliacao_futura
from {{ ref("bronze_shpt_fgts_canal_execucoes_obras") }} x
join contrato c on c.cod_contrato = trim(x.cod_contrato::text)
left join situacao_obra so on so.codigo = trim(x.cod_situacao_obra::text)
where trim(x.dte_ano_mes_avaliacao::text) ~ '^\d{6}$'
