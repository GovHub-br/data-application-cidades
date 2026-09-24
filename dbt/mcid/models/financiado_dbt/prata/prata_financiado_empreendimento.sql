{{ config(materialized="table") }}

-- Prata: Empreendimento do Canal FGTS, com município resolvido para código IBGE.
-- Fonte: bronze_sftp_canalfgts_empreendimentos + bronze_sftp_canalfgts_municipios
-- Grão: uma linha por `cod_empreendimento`.
with
    empreendimento as (
        select
            trim(cod_empreendimento) as cod_empreendimento,
            nullif(trim(txt_nome_empreendimento), '') as empreendimento_nome,
            nullif(trim(txt_localidade), '') as localidade,
            nullif(trim(txt_logradouro), '') as logradouro,
            nullif(trim(txt_objeto), '') as objeto,
            nullif(trim(txt_projeto), '') as projeto,
            trim(cod_municipio) as cod_municipio_caixa,

            -- `qtd_unidades_financiadas_cca` é o recorte associativo do total, não
            -- uma parcela a somar: em boa parte das linhas os dois são iguais.
            {{ parse_int("qtd_unidades_financiadas") }} as qt_unidades_financiadas,
            {{ parse_int("qtd_unidades_financiadas_cca") }} as qt_unidades_cca,
            {{ parse_int("qtd_populacao_beneficiada") }} as qt_populacao_beneficiada,
            {{ parse_int("qtd_empregos_gerados") }} as qt_empregos_gerados,

            _source_file as arquivo_de_origem,
            nullif(trim(_ingested_at), '')::timestamp as criado_em

        from {{ ref("bronze_sftp_canalfgts_empreendimentos") }}
    ),

    municipio as (
        select
            trim(codigo) as cod_municipio_caixa,
            nullif(trim(municipio), '') as municipio,
            nullif(trim(uf), '') as uf,
            nullif(trim(ibge_codigo), '') as cod_ibge
        from {{ ref("bronze_sftp_canalfgts_municipios") }}
    )

select
    e.cod_empreendimento,
    e.empreendimento_nome,
    e.localidade,
    e.logradouro,
    e.objeto,
    e.projeto,
    e.cod_municipio_caixa,
    m.municipio,
    m.uf,
    m.cod_ibge,
    e.qt_unidades_financiadas,
    e.qt_unidades_cca,
    e.qt_populacao_beneficiada,
    e.qt_empregos_gerados,
    e.arquivo_de_origem,
    e.criado_em
from empreendimento as e
left join municipio as m on e.cod_municipio_caixa = m.cod_municipio_caixa
