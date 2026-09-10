{{ config(materialized="table") }}

-- GOLD — linha do tempo (marcos) por EMPREENDIMENTO (change
-- enriquecer-datas-acompanhamento-historico, D).
--
-- Grão: 1 linha por empreendimento — coalesce(id_empreendimento, apf), alinhado
-- ao grão de ouro_dhist_snapshot_empreendimento_atual (FDS multi-fase colapsa em 1;
-- FAR/Rural chave = apf).
--
-- 7 marcos, todos `date`, cada um com proveniência:
--   <marco>_fonte        interface/stream de origem do valor escolhido
--                        ('sftp:INTxxx', 'snh:dados_prioritarios',
--                         'snh:entrega_evento', 'sem_fonte')
--   <marco>_dt_snapshot  dt_referencia (ou dt_ultimo_snapshot da espinha) da
--                        observação de onde o valor veio
--
-- O modelo SÓ SELECIONA entre valores observados — nunca interpola nem calcula
-- um marco. Marco sem fonte = NULL + _fonte = 'sem_fonte' + _dt_snapshot = NULL.
--
-- ORDEM DE COALESCE (igual no schema.yml):
--   dt_contratacao / dt_inicio_obra
--        -> linha "estado" vencedora do empreendimento (fase mais avançada,
--           depois dt_referencia mais recente), fonte = feed dessa linha.
--   dt_conclusao_obra / dt_ultima_entrega
--        -> MAIOR valor observado em qualquer snapshot do empreendimento (a data
--           pode ter sido reportada e depois sumir do feed; queremos a última
--           conhecida). dt_ultima_entrega considera ainda o max da espinha sobre
--           TODOS os APFs de fase. Fonte = a do snapshot que trouxe o max.
--   dt_primeira_entrega  -> espinha prata_dhist_entrega_apf (evento SNH).
--   dt_legalizacao       -> sem fonte nesta fase (NULL / 'sem_fonte').
--   dt_previsao_entrega  -> MAIOR data prevista observada em qualquer snapshot do
--        empreendimento (change destravar-datas-obra-entrega-silver-historico:
--        braco SNH projeta data_da_previsao_da_entrega). Fonte sempre
--        'snh:dados_prioritarios'. Fill baixo (~2% dos APFs).
--
-- `marcos_coerentes` = a cadeia de datas se sustenta (ignorando nulos). É
-- DIAGNÓSTICO — o modelo não corrige nada.
--
-- Destino conforme o target (D2): arquivo local em `staging_duckdb`.
{% set fonte_estado %}
    case
        when sr.fonte_serie = 'sftp'
        then coalesce(
            nullif('sftp:' || regexp_extract(sr.fonte_tabela, '(INT[0-9]+)', 1), 'sftp:'),
            'sftp'
        )
        else 'snh:dados_prioritarios'
    end
{% endset %}

with

    silver_rows as (
        {% for frente in ['far', 'fds', 'rural'] %}
        -- fonte_serie = 'obra_mensal' EXCLUÍDA (change
        -- consolidar-schemas-historico-reloginho, D2): linhas só-de-obra não
        -- trazem marco novo e não devem virar a linha "estado" corrente do
        -- empreendimento (perderia dt_contratacao/dt_inicio_obra). Mantém a
        -- contagem de id_empreendimento.
        select
            frente_mcmv,
            coalesce(id_empreendimento, apf) as chave_empreendimento,
            apf,
            fase_empreendimento,
            dt_referencia,
            dt_contratacao,
            dt_inicio_obra,
            dt_conclusao_obra,
            dt_entrega_uh,
            dt_entrega_uh_fonte,
            dt_previsao_entrega,
            -- Bloco C (change colunas-orfas-bronze-historico): marcos direto da
            -- fonte, hoje no contrato da silver.
            dt_primeira_entrega,
            dt_primeira_entrega_fonte,
            dt_ultima_liberacao,
            fonte_serie,
            fonte_tabela
        from {{ ref('prata_' ~ frente ~ '_historico_empreendimento') }}
        where fonte_serie <> 'obra_mensal'
        {{ "union all" if not loop.last }}
        {% endfor %}
    ),

    -- linha "estado" vencedora por empreendimento: fase mais avançada
    -- (Desligamento > Obra > Projeto), depois dt_referencia mais recente.
    estado as (
        select *
        from silver_rows
        qualify
            row_number() over (
                partition by frente_mcmv, chave_empreendimento
                order by
                    case fase_empreendimento
                        when 'Desligamento' then 0
                        when 'Obra' then 1
                        when 'Projeto' then 2
                        else 3
                    end,
                    dt_referencia desc
            )
            = 1
    ),

    -- maior dt_entrega_uh / dt_conclusao_obra JÁ RESOLVIDAS na silver, em
    -- qualquer snapshot do empreendimento, + a fonte e o snapshot desse max.
    marco_silver as (
        select
            frente_mcmv,
            chave_empreendimento,
            max(dt_entrega_uh) as dt_entrega_uh_max,
            arg_max(dt_entrega_uh_fonte, dt_entrega_uh) as dt_entrega_uh_max_fonte,
            arg_max(dt_referencia, dt_entrega_uh) as dt_entrega_uh_max_snapshot,
            max(dt_conclusao_obra) as dt_conclusao_obra_max,
            arg_max(fonte_serie, dt_conclusao_obra) as _conc_fonte_serie,
            arg_max(fonte_tabela, dt_conclusao_obra) as _conc_fonte_tabela,
            arg_max(dt_referencia, dt_conclusao_obra) as dt_conclusao_obra_max_snapshot,
            max(dt_previsao_entrega) as dt_previsao_entrega_max,
            arg_max(dt_referencia, dt_previsao_entrega) as dt_previsao_entrega_max_snapshot,
            -- Bloco C: menor dt_primeira_entrega direta da silver (FAR/INT040) +
            -- maior dt_ultima_liberacao observada.
            min(dt_primeira_entrega) as dt_primeira_entrega_silver,
            arg_min(dt_primeira_entrega_fonte, dt_primeira_entrega) as dt_primeira_entrega_silver_fonte,
            arg_min(dt_referencia, dt_primeira_entrega) as dt_primeira_entrega_silver_snapshot,
            max(dt_ultima_liberacao) as dt_ultima_liberacao_max,
            arg_max(fonte_serie, dt_ultima_liberacao) as _lib_fonte_serie,
            arg_max(fonte_tabela, dt_ultima_liberacao) as _lib_fonte_tabela,
            arg_max(dt_referencia, dt_ultima_liberacao) as dt_ultima_liberacao_max_snapshot
        from silver_rows
        group by 1, 2
    ),

    -- APFs de cada empreendimento (FDS multi-fase tem 2-3) para agregar a espinha.
    apfs as (
        select distinct frente_mcmv, chave_empreendimento, apf
        from silver_rows
        where apf is not null
    ),

    entrega as (
        select
            a.frente_mcmv,
            a.chave_empreendimento,
            min(e.dt_primeira_entrega) as dt_primeira_entrega,
            max(e.dt_ultima_entrega) as dt_ultima_entrega_espinha,
            sum(e.uh_entregues_acumulada)::bigint as uh_entregues_acumulada,
            max(e.dt_ultimo_snapshot) as dt_ultimo_snapshot_entrega
        from apfs a
        join {{ ref('prata_dhist_entrega_apf') }} e on a.apf = e.apf
        group by 1, 2
    ),

    marcos as (
        select
            sr.frente_mcmv,
            sr.chave_empreendimento as id_empreendimento,
            sr.apf,
            sr.fase_empreendimento,

            sr.dt_contratacao,
            case when sr.dt_contratacao is not null then {{ fonte_estado }} end
                as dt_contratacao_fonte,
            case when sr.dt_contratacao is not null then sr.dt_referencia end
                as dt_contratacao_dt_snapshot,

            sr.dt_inicio_obra,
            case when sr.dt_inicio_obra is not null then {{ fonte_estado }} end
                as dt_inicio_obra_fonte,
            case when sr.dt_inicio_obra is not null then sr.dt_referencia end
                as dt_inicio_obra_dt_snapshot,

            ms.dt_conclusao_obra_max as dt_conclusao_obra,
            case
                when ms.dt_conclusao_obra_max is null
                then null
                when ms._conc_fonte_serie = 'sftp'
                then coalesce(
                    nullif('sftp:' || regexp_extract(ms._conc_fonte_tabela, '(INT[0-9]+)', 1), 'sftp:'),
                    'sftp'
                )
                else 'snh:dados_prioritarios'
            end as dt_conclusao_obra_fonte,
            ms.dt_conclusao_obra_max_snapshot as dt_conclusao_obra_dt_snapshot,

            -- dt_primeira_entrega: menor entre a data direta da silver (FAR/INT040,
            -- change colunas-orfas-bronze-historico) e a da espinha SNH-evento.
            least(ms.dt_primeira_entrega_silver, en.dt_primeira_entrega) as dt_primeira_entrega,
            case
                when least(ms.dt_primeira_entrega_silver, en.dt_primeira_entrega) is null
                then null
                when ms.dt_primeira_entrega_silver is not null
                    and (
                        en.dt_primeira_entrega is null
                        or ms.dt_primeira_entrega_silver <= en.dt_primeira_entrega
                    )
                then ms.dt_primeira_entrega_silver_fonte
                else 'snh:entrega_evento'
            end as dt_primeira_entrega_fonte,
            case
                when least(ms.dt_primeira_entrega_silver, en.dt_primeira_entrega) is null
                then null
                when ms.dt_primeira_entrega_silver is not null
                    and (
                        en.dt_primeira_entrega is null
                        or ms.dt_primeira_entrega_silver <= en.dt_primeira_entrega
                    )
                then ms.dt_primeira_entrega_silver_snapshot
                else en.dt_ultimo_snapshot_entrega
            end as dt_primeira_entrega_dt_snapshot,

            -- dt_ultima_liberacao: marco financeiro (change colunas-orfas-bronze-historico).
            ms.dt_ultima_liberacao_max as dt_ultima_liberacao,
            case
                when ms.dt_ultima_liberacao_max is null then null
                when ms._lib_fonte_serie = 'sftp'
                then coalesce(
                    nullif('sftp:' || regexp_extract(ms._lib_fonte_tabela, '(INT[0-9]+)', 1), 'sftp:'),
                    'sftp'
                )
                else 'snh:dados_prioritarios'
            end as dt_ultima_liberacao_fonte,
            ms.dt_ultima_liberacao_max_snapshot as dt_ultima_liberacao_dt_snapshot,

            -- dt_ultima_entrega: maior entre o max de dt_entrega_uh (já resolvido
            -- na silver, em qualquer snapshot) e o max da espinha sobre TODOS os
            -- APFs de fase (FDS multi-fase). greatest() ignora nulos no DuckDB.
            greatest(ms.dt_entrega_uh_max, en.dt_ultima_entrega_espinha) as dt_ultima_entrega,
            case
                when greatest(ms.dt_entrega_uh_max, en.dt_ultima_entrega_espinha) is null
                then null
                when ms.dt_entrega_uh_max is not null
                    and (
                        en.dt_ultima_entrega_espinha is null
                        or ms.dt_entrega_uh_max >= en.dt_ultima_entrega_espinha
                    )
                then ms.dt_entrega_uh_max_fonte
                else 'snh:entrega_evento'
            end as dt_ultima_entrega_fonte,
            case
                when greatest(ms.dt_entrega_uh_max, en.dt_ultima_entrega_espinha) is null
                then null
                when ms.dt_entrega_uh_max is not null
                    and ms.dt_entrega_uh_max_fonte <> 'snh:entrega_evento'
                    and (
                        en.dt_ultima_entrega_espinha is null
                        or ms.dt_entrega_uh_max >= en.dt_ultima_entrega_espinha
                    )
                then ms.dt_entrega_uh_max_snapshot
                else en.dt_ultimo_snapshot_entrega
            end as dt_ultima_entrega_dt_snapshot,

            -- dt_legalizacao: sem fonte nesta fase (NULL / 'sem_fonte')
            cast(null as date) as dt_legalizacao,

            ms.dt_previsao_entrega_max as dt_previsao_entrega,
            case
                when ms.dt_previsao_entrega_max is not null
                then 'snh:dados_prioritarios'
            end as dt_previsao_entrega_fonte,
            case
                when ms.dt_previsao_entrega_max is not null
                then ms.dt_previsao_entrega_max_snapshot
            end as dt_previsao_entrega_dt_snapshot,

            en.uh_entregues_acumulada
        from estado sr
        left join marco_silver ms
            on sr.frente_mcmv = ms.frente_mcmv
            and sr.chave_empreendimento = ms.chave_empreendimento
        left join entrega en
            on sr.frente_mcmv = en.frente_mcmv
            and sr.chave_empreendimento = en.chave_empreendimento
    )

select
    frente_mcmv,
    id_empreendimento,
    apf,
    fase_empreendimento,

    dt_contratacao,
    coalesce(dt_contratacao_fonte, 'sem_fonte') as dt_contratacao_fonte,
    dt_contratacao_dt_snapshot,

    dt_inicio_obra,
    coalesce(dt_inicio_obra_fonte, 'sem_fonte') as dt_inicio_obra_fonte,
    dt_inicio_obra_dt_snapshot,

    dt_conclusao_obra,
    coalesce(dt_conclusao_obra_fonte, 'sem_fonte') as dt_conclusao_obra_fonte,
    dt_conclusao_obra_dt_snapshot,

    dt_primeira_entrega,
    coalesce(dt_primeira_entrega_fonte, 'sem_fonte') as dt_primeira_entrega_fonte,
    dt_primeira_entrega_dt_snapshot,

    dt_ultima_entrega,
    coalesce(dt_ultima_entrega_fonte, 'sem_fonte') as dt_ultima_entrega_fonte,
    dt_ultima_entrega_dt_snapshot,

    dt_legalizacao,
    'sem_fonte'::text as dt_legalizacao_fonte,
    cast(null as date) as dt_legalizacao_dt_snapshot,

    dt_previsao_entrega,
    coalesce(dt_previsao_entrega_fonte, 'sem_fonte') as dt_previsao_entrega_fonte,
    dt_previsao_entrega_dt_snapshot,

    dt_ultima_liberacao,
    coalesce(dt_ultima_liberacao_fonte, 'sem_fonte') as dt_ultima_liberacao_fonte,
    dt_ultima_liberacao_dt_snapshot,

    uh_entregues_acumulada,

    -- diagnóstico: cadeia de datas coerente, ignorando marcos nulos
    coalesce(dt_contratacao <= dt_inicio_obra, true)
    and coalesce(dt_inicio_obra <= dt_conclusao_obra, true)
    and coalesce(dt_inicio_obra <= dt_primeira_entrega, true)
    and coalesce(dt_primeira_entrega <= dt_ultima_entrega, true)
    and coalesce(dt_contratacao <= dt_ultima_entrega, true) as marcos_coerentes,

    current_timestamp as dt_gold
from marcos
