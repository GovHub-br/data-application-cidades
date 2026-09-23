{{ config(materialized="table") }}

-- Prata: Termos de compromisso do Novo MCMV FNHIS Sub-50 (TransfereGov)
-- Fonte: bronze_shpt_fnhis_sub50_termos_compromisso (painel mensal; a bronze guarda o retrato
-- mais recente). Grão: um termo, identificado pelo número da proposta no TransfereGov.
--
-- `no_reservado_pac` junta os dois números da proposta ("56000000016/2024 - 32847/2024"): o
-- primeiro é o da SELEÇÃO, e é por ele que o termo se liga à proposta em
-- prata_fnhis_propostas — casou 1.207 de 1.207 em 2026-09. O painel não traz IBGE nem UH;
-- vêm de lá.
--
-- Valores no formato brasileiro ("3.250.000,00"); `-` na origem é ausência, não situação.

with
    termo as (
        select
            nullif(trim(no_proposta::text), '') as numero_proposta_transferegov,
            nullif(split_part(trim(no_reservado_pac::text), ' - ', 1), '') as numero_proposta,
            nullif(trim(codigo_programa::text), '') as codigo_programa,
            nullif(nullif(trim({{ target.schema }}.corrigir_mojibake(sit_contratacao::text)), ''), '-') as situacao_contratacao,
            nullif(nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_proposta::text)), ''), '-') as situacao_proposta,
            nullif(nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_instrumento::text)), ''), '-') as situacao_instrumento,
            nullif(trim({{ target.schema }}.corrigir_mojibake(modalidade::text)), '') as modalidade_instrumento,
            {{ parse_financial_value("vl_repasse_proposta::text") }} as valor_repasse_proposta,
            {{ parse_financial_value("valor_de_repasse::text") }} as valor_repasse,
            {{ parse_financial_value("valor_de_contrapartida::text") }} as valor_contrapartida,
            {{ parse_financial_value("vl_empenhado_pre_convenio::text") }} as valor_empenhado_pre_convenio,
            {{ parse_financial_value("valor_empenhado_acumulado::text") }} as valor_empenhado_acumulado,
            nullif(trim({{ target.schema }}.corrigir_mojibake(objeto::text)), '') as objeto,
            nullif(trim(uf::text), '') as uf,
            upper(nullif(trim({{ target.schema }}.corrigir_mojibake(municipio::text)), '')) as municipio,
            initcap(nullif(trim(regiao::text), '')) as regiao,
            nullif(trim({{ target.schema }}.corrigir_mojibake(nome_proponente::text)), '') as proponente_nome,
            nullif(regexp_replace(cnpj::text, '[^0-9]', '', 'g'), '') as proponente_cnpj,
            {{ target.schema }}.parse_date_br(nullif(trim(data_assinatura::text), '')) as dt_assinatura,
            {{ target.schema }}.parse_date_br(left(nullif(trim(data_consulta::text), ''), 10)) as dt_consulta,
            cast(filename as varchar) as arquivo_de_origem,
            nullif(trim(_ingested_at::text), '')::timestamp as criado_em
        from {{ ref("bronze_shpt_fnhis_sub50_termos_compromisso") }}
    )

select
    *,
    -- Anulado ou rescindido é termo que deixou de existir; sem situação (`-` na origem) é
    -- proposta selecionada que não chegou a virar instrumento.
    situacao_instrumento is not null
    and situacao_instrumento not in ('Convênio Anulado', 'Convênio Rescindido') as ic_instrumento_ativo,
    valor_repasse + valor_contrapartida as valor_investimento
from termo
