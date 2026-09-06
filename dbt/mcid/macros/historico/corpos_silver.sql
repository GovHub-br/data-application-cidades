{#
    Braco SNH das silvers historicas por frente (FAR / FDS / Rural).

    Depois da separacao das bronzes por familia (D5), a SNH deixou de ser uma
    tabela unica e virou duas — uma por agente (BB, CAIXA). Cada silver de
    frente passa a ter um braco por agente, e os tres arquivos de silver usam
    este mesmo corpo: o que varia entre eles e apenas frente/linha e a
    modalidade filtrada.

    Os schemas dos dois agentes divergem — `uhs_contratadas`/`uhs_entregues`
    so existem no BB, `dt_entrega` so na CAIXA. Antes o `union_by_name` da
    bronze unica preenchia as lacunas com null; agora cada braco so pode
    referenciar as colunas do seu proprio lote, e quem resolve isso e
    coalesce_present_parsed() sobre a relacao da familia.
#}
{% macro silver_historico_snh_arm(rel, frente_mcmv, linha_mcmv, modalidade) %}
        select
            'Minha Casa Minha Vida'::text as programa,
            '{{ frente_mcmv }}'::text as frente_mcmv,
            'Subsidiada'::text as grupo_linha,
            '{{ linha_mcmv }}'::text as linha_mcmv,
            'empreendimento_mes'::text as grao_registro,
            case
                when upper(nullif(trim(agente_financeiro::text), '')) like 'BB%'
                then 'Banco do Brasil'
                when upper(nullif(trim(agente_financeiro::text), '')) like 'CAIXA%'
                then 'CAIXA'
                when agente_arquivo = 'BB'
                then 'Banco do Brasil'
                when agente_arquivo = 'CAIXA'
                then 'CAIXA'
            end::text as agente_financeiro,
            nullif(trim(apf::text), '')::text as apf,
            nullif(trim(apf::text), '')::text as codigo_empreendimento,
            nullif(trim(nome_empreendimento::text), '')::text as nome_empreendimento,
            nullif(trim(codigo_ibge_do_municipio::text), '')::text
            as codigo_ibge_municipio,
            nullif(trim(municipio::text), '')::text as municipio,
            upper(nullif(trim(uf::text), ''))::text as uf,
            null::text as responsavel_id,
            null::text as responsavel_nome,
            {{ coalesce_present_parsed(
                rel, ['uh_contratadas', 'uhs_contratadas'], 'parse_hist_bigint', 'bigint'
            ) }} as quantidade_uh,
            {{ coalesce_present_parsed(
                rel, ['uh_entregues', 'uhs_entregues'], 'parse_hist_bigint', 'bigint'
            ) }} as quantidade_uh_entregues,
            {{ parse_hist_double('valor_contratado') }} as valor_contratado,
            {{ parse_hist_double('valor_desembolsado') }} as valor_desembolsado,
            {{ parse_hist_double('exec') }} as percentual_execucao_fisica,
            nullif(trim(situacao_do_empreendimento::text), '')::text
            as status_operacional,
            {{ parse_hist_date('data_de_contratacao') }} as dt_contratacao,
            null::date as dt_inicio_obra,
            {{ coalesce_present_parsed(rel, ['dt_entrega'], 'parse_hist_date', 'date') }}
            as dt_entrega,
            dt_referencia,
            {{ parse_hist_date('data_de_movimento') }} as dt_movimento,
            'snh'::text as fonte_serie,
            ('SNH_dados_prioritarios_af_' || lower(coalesce(agente_arquivo, 'na')))::text
            as fonte_tabela,
            source_file,
            hash_linha,
            dt_ingest
        from {{ rel }}
        where
            upper(nullif(trim(modalidade::text), '')) = '{{ modalidade }}'
            and nullif(trim(apf::text), '') is not null
{% endmacro %}
