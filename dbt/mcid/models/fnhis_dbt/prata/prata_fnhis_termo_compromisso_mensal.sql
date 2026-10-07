{{ config(materialized="table") }}

-- Prata: Termos de compromisso do FNHIS Sub-50 em CADA retrato mensal do painel do TransfereGov.
-- Fonte: bronze_shpt_fnhis_sub50_termos_compromisso_serie. Grão: termo × data do retrato.
--
-- Serve para ver a trajetória do instrumento (em execução → anulado, prestação de contas...) e
-- a evolução do empenho. O retrato mais antigo (mar/2025) não tinha `situacao_instrumento`.
-- Valores no formato brasileiro; `-` na origem é ausência.

with
    bruto as (
        select
            nullif(trim(no_proposta), '') as numero_proposta_transferegov,
            nullif(split_part(trim(no_reservado_pac), ' - ', 1), '') as numero_proposta,
            to_date(substring(filename from 'Painel_TG_(\d{8})'), 'YYYYMMDD') as dt_retrato,
            nullif(nullif(trim({{ target.schema }}.corrigir_mojibake(sit_contratacao)), ''), '-') as situacao_contratacao,
            nullif(nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_proposta)), ''), '-') as situacao_proposta,
            nullif(nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_instrumento)), ''), '-') as situacao_instrumento,
            {{ parse_financial_value("valor_de_repasse") }} as valor_repasse,
            {{ parse_financial_value("valor_de_contrapartida") }} as valor_contrapartida,
            {{ parse_financial_value("valor_empenhado_acumulado") }} as valor_empenhado_acumulado,
            nullif(trim(uf), '') as uf,
            regexp_replace(filename, '^.*/', '') as arquivo_de_origem
        from {{ ref("bronze_shpt_fnhis_sub50_termos_compromisso_serie") }}
    ),

    unico as (
        select distinct on (numero_proposta_transferegov, dt_retrato) *
        from bruto
        where numero_proposta_transferegov is not null and dt_retrato is not null
        order by numero_proposta_transferegov, dt_retrato, arquivo_de_origem desc
    )

select
    numero_proposta_transferegov,
    numero_proposta,
    dt_retrato,
    situacao_contratacao,
    situacao_proposta,
    situacao_instrumento,
    valor_repasse,
    valor_contrapartida,
    valor_empenhado_acumulado,
    uf,
    arquivo_de_origem,
    lag(situacao_instrumento) over (
        partition by numero_proposta_transferegov order by dt_retrato
    ) as situacao_instrumento_retrato_anterior
from unico
