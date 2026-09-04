{{ config(materialized="table") }}

-- SILVER — serie executiva historica do MCMV (pre-2024), contrato comum.
--
-- Le a bronze e aplica o MAPA DE COLUNAS (doc
-- issue-130-proposta-bronze-series-historicas.md) via coalesce_present(), que so
-- referencia as colunas que existem de fato na bronze materializada — cada
-- familia tem 2-3 geracoes de schema.
--
-- Grao de saida: 1 registro de origem (empreendimento ou contrato), tipado e
-- deduplicado por (fonte_familia, chave_natural, dt_referencia) mantendo o
-- snapshot mais recente do mes. dt_referencia = mes-snapshot.
--
-- `linha_ogu_fgts` classifica o registro para a serie do piloto #118:
-- OGU/Subsidiado quando o subsidio OGU domina; FGTS/Financiado quando o FGTS
-- domina; os valores brutos ficam expostos para o gold somar os dois.
--
-- Target obrigatorio: staging_duckdb (gating em dbt_project.yml).
{% set bronze = ref('bronze_mcmv_historico_serie_executiva') %}

with

    bronze as (select * from {{ bronze }}),

    mapeado as (
        select
            'Minha Casa Minha Vida' as programa,
            fonte_familia,
            source_file,
            dt_referencia,
            report_date_parsed,
            hash_linha,

            case
                when
                    lower(source_file) like 'caixa%' or lower(source_file) like '%_caixa%'
                then 'CAIXA'
                when lower(source_file) like 'bb%' or lower(source_file) like '%_bb%'
                then 'Banco do Brasil'
            end as agente_financeiro,

            case when lower(source_file) like '%pnhr%' then 'Rural' end as frente_hint,

            {{ coalesce_present(bronze, [
            'cod_apf','codapf','cod_empreendimento','icodigo_empreendimento',
            'codigo_empreendimento_bb','cod_contrato','nr_prpt','contrato_bb','contrato_caixa'
        ]) }}
            as chave_natural,

            {{ coalesce_present(bronze, ['uf','csigla_uf']) }} as uf_raw,
            {{ coalesce_present(bronze, [
            'cod_munic_ibge','codmunicibge','cod_municipio','codigo_do_ibge',
            'icodigo_municipio_ibge_sem_dv'
        ]) }} as codigo_ibge_raw,
            {{ coalesce_present(bronze, ['municipio','vnome_municipio']) }}
            as municipio_raw,
            {{ coalesce_present(bronze, ['faixa','cfaixa','num_faixa','faixa_divisao']) }}
            as faixa_raw,
            {{ coalesce_present(bronze, [
            'produto','vnome_produto','iprograma_mcmv','num_programa','fase_mcmv','fase_do_pmcmv'
        ]) }}
            as produto_raw,
            {{ coalesce_present(bronze, [
            'vnome_empreendimento','nomeempreendimento','dsc_empreendimento','empreendimento'
        ]) }}
            as nome_empreendimento,
            {{ coalesce_present(bronze, ['vnome_construtora','construtora','nom_proponente','empresa']) }}
            as responsavel_nome,
            {{ coalesce_present(bronze, ['inumero_cnpj','cnpj','cod_cnpj_proponente']) }}
            as responsavel_id,

            {{ coalesce_present(bronze, [
            'uh','unidades','iqde_uh','qtd_unidade_habitacional','qtd_uh','qde_unidades'
        ]) }} as uh_contratadas_raw,
            {{ coalesce_present(bronze, [
            'iqde_unidades_entregues','unidades_entregues','iqde_uh_entregues',
            'qtd_unidade_entregue','qtd_entregue','entregues'
        ]) }} as uh_entregues_raw,
            {{ coalesce_present(bronze, [
            'uh_concluidas','unidades_concluidas','iqde_uh_concluidas',
            'qtd_unidade_concluida','qtd_concluida','uh_concluidos'
        ]) }} as uh_concluidas_raw,
            {{ coalesce_present(bronze, ['uh_em_obras','unidades_em_obras','iqde_uh_em_obras']) }}
            as uh_em_obras_raw,
            {{ coalesce_present(bronze, ['uh_comercializadas','comercializadas','qtd_comercializadas']) }}
            as uh_comercializadas_raw,

            {{ coalesce_present(bronze, [
            'valor_total_do_investimento','mvalor_investimento','vlr_total_operacao','vlr_total_investimento'
        ]) }}
            as valor_investimento_raw,
            {{ coalesce_present(bronze, [
            'valor_do_emprestimo','mvalor_emprestimo','vlr_emprestimo','vlr_financiamento',
            'mvalor_financiamento','valor_global_de_venda_vgv'
        ]) }}
            as valor_emprestimo_raw,
            {{ coalesce_present(bronze, ['valor_total_liberado','mvalor_desembolso']) }}
            as valor_liberado_raw,
            {{ coalesce_present(bronze, [
            'subsidio_fgts','siaci_valorsubsidio_fgts','vlr_subsidio_fgts','complemento_fgts'
        ]) }}
            as subsidio_fgts_raw,
            {{ coalesce_present(bronze, [
            'subsidio_ogu','siaci_valorsubsidio_ogu','vlr_subsidio_ogu','complemento_ogu'
        ]) }} as subsidio_ogu_raw,
            {{ coalesce_present(bronze, ['mvalor_subsidio']) }} as subsidio_total_raw,

            {{ coalesce_present(bronze, [
            'obra_executada','de_obra_executada','prc_execucao_obra','percentual_de_obra',
            'vfaixa_perc_obra','obra'
        ]) }} as pct_execucao_fisica_raw,

            {{ coalesce_present(bronze, ['data_contratacao','dat_contratacao','data_da_contratacao_bb']) }}
            as dt_contratacao_raw,
            {{ coalesce_present(bronze, ['data_conclusao','dat_entregue','entrega_do_empreendimento']) }}
            as dt_entrega_raw,
            {{ coalesce_present(bronze, [
            'data_prevista_termino_obra','data_prevista_termino_obras','dat_prevista_termino',
            'cronograma_datatermino','dataprevistasr'
        ]) }}
            as dt_previsao_termino_raw
        from bronze
    ),

    tipado as (
        select
            programa,
            fonte_familia,
            coalesce(frente_hint, 'Nao classificada') as frente_mcmv,
            agente_financeiro,
            nullif(trim(cast(chave_natural as varchar)), '') as chave_natural,
            upper(nullif(trim(cast(uf_raw as varchar)), '')) as uf,
            regexp_replace(
                nullif(trim(cast(codigo_ibge_raw as varchar)), ''), '\D', '', 'g'
            ) as codigo_ibge_municipio,
            nullif(trim(cast(municipio_raw as varchar)), '') as municipio,
            lower(nullif(trim(cast(faixa_raw as varchar)), '')) as faixa,
            nullif(trim(cast(produto_raw as varchar)), '') as produto,
            nullif(trim(cast(nome_empreendimento as varchar)), '') as nome_empreendimento,
            nullif(trim(cast(responsavel_nome as varchar)), '') as responsavel_nome,
            nullif(trim(cast(responsavel_id as varchar)), '') as responsavel_id,

            {{ parse_hist_bigint('uh_contratadas_raw') }} as uh_contratadas,
            {{ parse_hist_bigint('uh_entregues_raw') }} as uh_entregues,
            {{ parse_hist_bigint('uh_concluidas_raw') }} as uh_concluidas,
            {{ parse_hist_bigint('uh_em_obras_raw') }} as uh_em_obras,
            {{ parse_hist_bigint('uh_comercializadas_raw') }} as uh_comercializadas,

            {{ parse_hist_double('valor_investimento_raw') }} as valor_investimento,
            {{ parse_hist_double('valor_emprestimo_raw') }} as valor_emprestimo,
            {{ parse_hist_double('valor_liberado_raw') }} as valor_liberado,
            {{ parse_hist_double('subsidio_fgts_raw') }} as subsidio_fgts,
            {{ parse_hist_double('subsidio_ogu_raw') }} as subsidio_ogu,
            {{ parse_hist_double('subsidio_total_raw') }} as subsidio_total,
            {{ parse_hist_double('pct_execucao_fisica_raw') }}
            as percentual_execucao_fisica,

            {{ parse_hist_date('dt_contratacao_raw') }} as dt_contratacao,
            {{ parse_hist_date('dt_entrega_raw') }} as dt_entrega,
            {{ parse_hist_date('dt_previsao_termino_raw') }} as dt_previsao_termino,

            dt_referencia,
            report_date_parsed,
            source_file,
            hash_linha
        from mapeado
    ),

    classificado as (
        select
            *,
            case
                when
                    coalesce(subsidio_ogu, 0) > 0
                    and coalesce(subsidio_ogu, 0) >= coalesce(subsidio_fgts, 0)
                then 'OGU/Subsidiado'
                when coalesce(subsidio_fgts, 0) > 0
                then 'FGTS/Financiado'
            end as linha_ogu_fgts
        from tipado
    ),

    util as (
        -- descarta linhas sem grao util: os relatorios agregados antigos de
        -- min_cidades (2011-2013) nao trazem chave nem metrica por empreendimento.
        select *
        from classificado
        where
            dt_referencia is not null
            and (
                chave_natural is not null
                or uh_contratadas is not null
                or uh_entregues is not null
                or valor_investimento is not null
                or valor_emprestimo is not null
            )
    ),

    dedup as (
        select
            *,
            row_number() over (
                partition by
                    fonte_familia, coalesce(chave_natural, hash_linha), dt_referencia
                order by report_date_parsed desc nulls last, source_file desc
            ) as rn
        from util
    )

select * exclude (rn)
from dedup
where rn = 1
