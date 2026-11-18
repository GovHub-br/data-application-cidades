{{ config(materialized="table") }}

-- Prata: Andamento de obra do FAR informado pela CAIXA (GEHIS), mês a mês desde jul/2021.
-- Fonte: bronze_sftp_gehis_andamento_obra (a mesma do Rural; o arquivo mistura FAR, Entidades e
-- Rural). Grão: APF × mês de referência.
--
-- Fica só a APF que está na prata_far_empreendimento. A situação de obra da GEHIS é a série
-- semanal que faltava no FAR, que até aqui só tinha a bronze. Quando chega mais de um arquivo
-- no mesmo mês, fica o último pelo nome.

with
    bruto as (
        select
            {{ target.schema }}.normalize_apf(nu_apf::text) as apf,
            coalesce(
                to_date(nullif(regexp_replace(anomes::text, '[^0-9]', '', 'g'), ''), 'YYYYMM'),
                to_date(substring(filename::text from 'OBRA_M(\d{6})'), 'YYYYMM')
            ) as dt_referencia,
            upper(nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_obra::text)), '')) as situacao_obra,
            {{ target.schema }}.parse_date_br(nullif(trim(dt_prevista_conclusao::text), '')) as dt_prevista_conclusao,
            {{ target.schema }}.parse_date_br(nullif(trim(dt_prevista_inauguracao::text), '')) as dt_prevista_inauguracao,
            regexp_replace(filename::text, '^.*/', '') as arquivo_de_origem
        from {{ ref("bronze_sftp_gehis_andamento_obra") }}
        where nullif(trim(nu_apf::text), '') is not null
    ),

    unico as (
        select distinct on (apf, dt_referencia) *
        from bruto
        where dt_referencia is not null
        order by apf, dt_referencia, arquivo_de_origem desc
    ),

    far as (select distinct apf from {{ ref("prata_far_empreendimento") }})

select
    u.apf,
    u.dt_referencia,
    -- "CRONOGRAMA INÃOMPLETO" e variações de encoding da origem
    case when u.situacao_obra ~ '^CRONOGRAMA' then 'CRONOGRAMA INCOMPLETO' else u.situacao_obra end as situacao_obra,
    u.dt_prevista_conclusao,
    u.dt_prevista_inauguracao,
    u.arquivo_de_origem
from unico u
inner join far f on f.apf = u.apf
