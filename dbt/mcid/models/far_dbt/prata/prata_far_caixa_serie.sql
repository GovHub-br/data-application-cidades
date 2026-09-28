{{ config(materialized="table") }}

-- Prata: Série dos dados prioritários da CAIXA (FAR) — uma linha por APF × competência.
-- Fonte: bronze.bronze_sftp_snh_pmcmv_dados_prioritarios_af_caixa_serie
-- Serve de conciliação e de fallback da execução física nos meses sem MONIT.
-- A competência vem do prefixo do arquivo (`<aaaamm>_SNH_...`). `#N/D` e `NULL`
-- (texto literal na origem) viram nulo, não zero.
with
    caixa as (
        select
            nullif(trim(apf), '') as apf,
            to_date(
                substring(filename from '([0-9]{6})_SNH_[^/]*$'), 'YYYYMM'
            ) as competencia,

            -- Identificação
            nullif(trim(nome_empreendimento), '') as empreendimento_nome,
            nullif(trim(municipio), '') as municipio,
            nullif(trim(uf), '') as uf,

            -- Situação
            nullif(trim(situacao_do_empreendimento), '') as situacao,
            nullif(
                trim(detalhamento_da_situacao_do_empreendimento), ''
            ) as situacao_detalhamento,

            -- Execução física
            {{ parse_numeric("nullif(upper(trim(\"exec\")), 'NULL')", "numeric(6, 2)") }}
            as pct_execucao,

            -- Valores
            case
                when trim(valor_contratado) = '#N/D'
                then null
                else {{ parse_financial_value("valor_contratado") }}
            end as valor_contratado,
            case
                when trim(valor_aporte_adicional) = '#N/D'
                then null
                else {{ parse_financial_value("valor_aporte_adicional") }}
            end as valor_aporte_adicional,
            case
                when trim(valor_desembolsado) = '#N/D'
                then null
                else {{ parse_financial_value("valor_desembolsado") }}
            end as valor_desembolsado,

            -- UHs
            {{ parse_int("uh_contratadas") }} as uh_contratadas,
            {{ parse_int("uh_entregues") }} as uh_entregues,

            -- Metadados
            substring(filename from '[^/]+$') as arquivo_de_origem,
            nullif(trim(_ingested_at), '')::timestamp as criado_em

        from {{ ref("bronze_sftp_snh_pmcmv_dados_prioritarios_af_caixa_serie") }}
        where trim(modalidade) = 'FAR' and nullif(trim(apf), '') is not null
    ),

    -- Há APF repetido dentro do mesmo arquivo; fica o de maior desembolso
    deduplicado as (
        select
            *,
            row_number() over (
                partition by apf, competencia
                order by
                    arquivo_de_origem desc,
                    valor_desembolsado desc nulls last,
                    pct_execucao desc nulls last
            ) as rn
        from caixa
    )

select
    apf,
    competencia,
    empreendimento_nome,
    municipio,
    uf,
    situacao,
    situacao_detalhamento,
    pct_execucao,
    valor_contratado,
    valor_aporte_adicional,
    valor_desembolsado,
    uh_contratadas,
    uh_entregues,
    arquivo_de_origem,
    criado_em
from deduplicado
where rn = 1
