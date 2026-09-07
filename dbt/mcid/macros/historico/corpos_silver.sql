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
            -- strip_float_text: no agente CAIXA ~20% dos códigos IBGE chegam
            -- como "355030.0" (int->float->str). Ver
            -- docs/varredura-sufixo-float-texto.md.
            {{ strip_float_text('codigo_ibge_do_municipio') }}::text
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
            {{ parse_hist_numeric('valor_contratado') }} as valor_contratado,
            {{ parse_hist_numeric('valor_desembolsado') }} as valor_desembolsado,
            {{ parse_hist_double('exec') }} as percentual_execucao_fisica,
            nullif(trim(situacao_do_empreendimento::text), '')::text
            as status_operacional,
            {{ parse_hist_date('data_de_contratacao') }} as dt_contratacao,
            null::date as dt_inicio_obra,
            -- Split semantico de dt_entrega (change enriquecer-datas-acompanhamento-historico):
            -- dt_entrega_uh (entrega de UH) x dt_conclusao_obra (conclusao fisica).
            -- No braco SNH so a CAIXA traz dt_entrega. data_do_termino so existe no
            -- snapshot `disponibilizados` e la e 0% preenchido p/ FAR/FDS/Rural --
            -- dt_conclusao_obra do braco SNH fica null; conclusao de obra vem so do
            -- braco SFTP (change destravar-datas-obra-entrega-silver-historico).
            {{ coalesce_present_parsed(rel, ['dt_entrega'], 'parse_hist_date', 'date') }}
            as dt_entrega_uh,
            null::date as dt_conclusao_obra,
            -- quantidade_uh_concluidas: so o braco SFTP tem; null no SNH.
            null::bigint as quantidade_uh_concluidas,
            -- previsao de entrega: o braco SNH e a unica fonte
            -- (data_da_previsao_da_entrega, ISO). qt_uh_previsao_entrega cai em
            -- unidades_habitacionais_a_serem_entregues quando o agente nao traz a
            -- coluna direta (change destravar-datas-obra-entrega-silver-historico).
            {{ coalesce_present_parsed(
                rel, ['data_da_previsao_da_entrega', 'dt_previsao_entrega'], 'parse_hist_date', 'date'
            ) }} as dt_previsao_entrega,
            {{ coalesce_present_parsed(
                rel, ['qt_uh_previsao_entrega', 'unidades_habitacionais_a_serem_entregues'], 'parse_hist_bigint', 'bigint'
            ) }} as qt_uh_previsao_entrega,
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
