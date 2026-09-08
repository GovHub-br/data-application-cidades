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
{#
    Fragmento das 6 colunas de quantidade de UH e sinais de obra que os braços
    SFTP GEFUS acrescentam ao contrato comum das silvers por frente (change
    enriquecer-quantidades-uh-e-sinais-obra-historico). Emite a lista terminando
    em vírgula — vem logo antes de `dt_referencia` no select do braço, na MESMA
    posição do fragmento equivalente do braço SNH (corpos_silver.sql).

    - distrato / vigencia: sempre NULL no SFTP (só a SNH reporta, D3).
    - ociosas: INT040/INT054 (`qt_unidades_ociosas`; sentinela -1 -> NULL).
    - inicial: INT065 (`qtde_uh_inicial`).
    - pendencia: INT040/INT054 (`cod_pendencia_obra`, texto cru SIM/NAO;
      `cod_pendencia_entrega` ficou fora de escopo -- vazia na fonte).
    - pc_reportada: INT057 (`pc_execucao_financeira_obra`); o derivado
      (desembolsado/contratado) é calculado no select final da silver.

    coalesce_present[_parsed] introspecciona a relação da bronze -> tolera drift
    de schema do parquet (a coluna some numa geração => compila como NULL).
#}
{#
    Fragmento dos sinais de retomada/paralisação e marcos de data (Blocos A/C da
    change colunas-orfas-bronze-historico) que os braços SFTP GEFUS acrescentam
    ao contrato comum das silvers por frente. Emite a lista terminando em vírgula
    — vem logo APÓS o fragmento historico_uh_sinais_sftp e ANTES de
    `dt_referencia`, na MESMA posição do bloco equivalente do braço SNH.

    Colunas (mesmo nome/tipo/ordem nos dois braços):
      - sinal_retomada_bruto   texto cru p/ join ao seed dominio_retomada
                               (INT040/054 `situacao_retomada`; senão NULL)
      - motivo_paralisacao_bruto  NULL no SFTP (`cod_motivo_ociosidade` é 0% real)
      - desc_situacao_contrato   INT040 `desc_situacao_contrato`, cru; senão NULL
      - dt_ultima_liberacao      INT040/054 `dt_ultima_liberacao_recurso`,
                                 INT057/065 `dt_ultima_liberacao`
      - dt_primeira_entrega      INT040/054 `dt_primeira_entrega` (FAR)
      - dt_assinatura_projeto    INT059 `dt_assinatura_projeto` (FDS)

    coalesce_present[_parsed] introspecciona a bronze -> a coluna ausente numa
    família compila como NULL do tipo certo (tolera drift + divergência entre
    frentes).
#}
{% macro historico_bloco_ac_sftp(rel) %}
            nullif(nullif(trim(({{ coalesce_present(rel, ['situacao_retomada']) }})::text), ''), 'NULL')
            as sinal_retomada_bruto,
            null::text as motivo_paralisacao_bruto,
            nullif(nullif(trim(({{ coalesce_present(rel, ['desc_situacao_contrato']) }})::text), ''), 'NULL')
            as desc_situacao_contrato,
            {{ coalesce_present_parsed(
                rel, ['dt_ultima_liberacao_recurso', 'dt_ultima_liberacao'], 'parse_hist_date', 'date'
            ) }} as dt_ultima_liberacao,
            {{ coalesce_present_parsed(rel, ['dt_primeira_entrega'], 'parse_hist_date', 'date') }}
            as dt_primeira_entrega,
            {{ coalesce_present_parsed(rel, ['dt_assinatura_projeto'], 'parse_hist_date', 'date') }}
            as dt_assinatura_projeto,
{% endmacro %}


{% macro historico_uh_sinais_sftp(rel, ociosas=false, inicial=false, pendencia=false, pc_reportada=false) %}
            null::bigint as quantidade_uh_distratadas,
            null::bigint as quantidade_uh_vigentes,
            {% if ociosas -%}
            nullif({{ coalesce_present_parsed(rel, ['qt_unidades_ociosas'], 'parse_hist_bigint', 'bigint') }}, -1)
            {%- else -%}null::bigint{%- endif %} as quantidade_uh_ociosas,
            {% if inicial -%}
            {{ coalesce_present_parsed(rel, ['qtde_uh_inicial'], 'parse_hist_bigint', 'bigint') }}
            {%- else -%}null::bigint{%- endif %} as quantidade_uh_inicial,
            {% if pendencia -%}
            nullif(nullif(trim({{ coalesce_present(rel, ['cod_pendencia_obra']) }}), ''), 'NULL')::text
            {%- else -%}null::text{%- endif %} as cod_pendencia_obra,
            {% if pc_reportada -%}
            {{ coalesce_present_parsed(rel, ['pc_execucao_financeira_obra'], 'parse_hist_double', 'double') }}
            {%- else -%}null::double{%- endif %} as percentual_execucao_financeira_reportada,
{% endmacro %}


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
            -- Quantidades de UH aditivas (change enriquecer-quantidades-uh-e-sinais-obra-historico).
            -- distrato / vigencia: SÓ a SNH reporta (D3) -- NULL nos braços SFTP.
            -- 0 distratos é informação, distinto de NULL (fonte omissa).
            {{ coalesce_present_parsed(
                rel, ['quantidade_de_uhs_distratadas'], 'parse_hist_bigint', 'bigint'
            ) }} as quantidade_uh_distratadas,
            {{ coalesce_present_parsed(
                rel, ['uh_vigentes', 'uhs_vigentes'], 'parse_hist_bigint', 'bigint'
            ) }} as quantidade_uh_vigentes,
            -- ociosas / inicial / pendencia: SÓ os braços SFTP GEFUS -- NULL no SNH.
            null::bigint as quantidade_uh_ociosas,
            null::bigint as quantidade_uh_inicial,
            null::text as cod_pendencia_obra,
            -- execucao financeira reportada: só INT057 (Rural BB) -- NULL no SNH.
            -- O derivado (desembolsado/contratado) é calculado no select final.
            null::double as percentual_execucao_financeira_reportada,
            -- Blocos A/C (change colunas-orfas-bronze-historico) -- mesma posição
            -- que historico_bloco_ac_sftp no braço SFTP.
            -- sinal_retomada: só o `detalhamento` casando 'A RETOMAR%' (os demais
            -- valores são rescisão/desimobilização/ocupação, não retomada).
            case
                when upper(nullif(trim(({{ coalesce_present(rel, ['detalhamento_da_situacao_do_empreendimento']) }})::text), '')) like 'A RETOMAR%'
                then nullif(trim(({{ coalesce_present(rel, ['detalhamento_da_situacao_do_empreendimento']) }})::text), '')
            end as sinal_retomada_bruto,
            -- motivo_paralisacao: `classificacao_dos_paralisados` (só SNH CAIXA).
            nullif(trim(({{ coalesce_present(rel, ['classificacao_dos_paralisados']) }})::text), '')
            as motivo_paralisacao_bruto,
            null::text as desc_situacao_contrato,
            null::date as dt_ultima_liberacao,
            null::date as dt_primeira_entrega,
            null::date as dt_assinatura_projeto,
            -- grão mensal (change dedup-fonte-silver-historico, D1): o braço SNH
            -- ja grava dt_referencia no dia 1 -> date_trunc e idempotente aqui;
            -- explicito p/ o contrato "cada braco normaliza" e simetria com o SFTP.
            date_trunc('month', dt_referencia)::date as dt_referencia,
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
