{{ config(materialized="table") }}

-- SILVER — série histórica mensal de empreendimentos MCMV da frente
-- FDS / Entidades.
--
-- SFTP  — bronze da interface INT059 (FDS CAIXA). Janela 2019-12 → atual.
-- SNH   — bronzes por agente (BB, CAIXA), modalidade = 'ENTIDADES'.
-- Janela 2024-06 → atual.
--
-- Desde a change pipeline-bronze-historica-destino-trocavel (D5) cada fonte é
-- uma TABELA POR FAMÍLIA; a união com projeção explícita acontece aqui.
--
-- Grão: empreendimento × mês — 1 linha por (frente_mcmv, apf, dt_referencia),
-- dt_referencia normalizado ao 1º do mês em cada braço (change
-- dedup-fonte-silver-historico, D1). Precedência SNH na janela sobreposta (D6);
-- LOCF de valor/responsável na cauda (D3). Ver
-- models/docs/entregas/separacao-silver-historico-por-frente.md.
--
-- OBRA_MENSAL / DESCONTINUIDADE (change consolidar-schemas-historico-reloginho,
-- D2/C2): a família MONIT_MOV_OBRA cria linha nos meses só-de-obra
-- (`fonte_serie = 'obra_mensal'`) e adiciona 22 colunas de obra por left join no
-- grão. Nesses meses `quantidade_uh` / `valor_contratado` / `valor_desembolsado`
-- são NULL (cauda de estoque declaradamente nula, sem carry-forward). No FDS o
-- SFTP INT059 vai até 2026-06, então só 2026-07 é mês só-de-obra. Agregações de
-- estoque por mês: filtrar `fonte_serie <> 'obra_mensal'`.
--
-- Destino conforme o target (D2): arquivo local em `staging_duckdb`, Postgres
-- atachado em `prod_duckdb`. Ver models/mcmv_historico_dbt/README.md para a
-- ordem de build exigida por coalesce_present.
{% set int059 = ref('bronze_sftp_empreendimento_int059') %}
{% set snh_familias = familias_snh_empreendimento() %}

with

    fds_caixa as (  -- INT059
        select
            'Minha Casa Minha Vida'::text as programa,
            'Entidades'::text as frente_mcmv,
            'Subsidiada'::text as grupo_linha,
            'FDS / Entidades'::text as linha_mcmv,
            'empreendimento_mes'::text as grao_registro,
            'CAIXA'::text as agente_financeiro,
            nullif(trim(nu_apf), '')::text as apf,
            nullif(trim(nu_apf), '')::text as codigo_empreendimento,
            nullif(trim(no_empreeendmento), '')::text as nome_empreendimento,
            nullif(trim(cod_municipio_ibge), '')::text as codigo_ibge_municipio,
            nullif(trim(no_municipio), '')::text as municipio,
            null::text as uf,
            nullif(trim(cnpj_proponente), '')::text as responsavel_id,
            nullif(trim(razao_social_proponente), '')::text as responsavel_nome,
            {{ parse_hist_bigint('qt_unidade_financiadas') }} as quantidade_uh,
            {{ parse_hist_bigint('qt_unidades_entregues') }} as quantidade_uh_entregues,
            {{ parse_hist_numeric('vr_investimento') }} as valor_contratado,
            {{ parse_hist_numeric('vr_liberado') }} as valor_desembolsado,
            {{ parse_hist_double('percentual_obra_realizado') }}
            as percentual_execucao_fisica,
            coalesce(
                nullif(trim(situacao_gefus), ''), nullif(trim(fase_contrato), '')
            )::text as status_operacional,
            {{ parse_hist_date('dt_assinatura') }} as dt_contratacao,
            {{ parse_hist_date('dt_inicio_obra') }} as dt_inicio_obra,
            -- Split de dt_entrega + destrave do braco SFTP INT059 (change
            -- destravar-datas-obra-entrega-silver-historico, A). INT059 traz
            -- dt_ultima_entrega / dt_termino_obra / dt_legalizacao / as qt_*.
            {{ parse_hist_date('dt_ultima_entrega') }} as dt_entrega_uh,
            coalesce(
                {{ parse_hist_date('dt_termino_obra') }},
                {{ parse_hist_date('dt_legalizacao') }}
            ) as dt_conclusao_obra,
            {{ parse_hist_bigint('qt_unidades_concluidas') }} as quantidade_uh_concluidas,
            null::date as dt_previsao_entrega,
            null::bigint as qt_uh_previsao_entrega,
            {{ historico_uh_sinais_sftp(int059) }}
            {{ historico_bloco_ac_sftp(int059) }}
            -- grão mensal (change dedup-fonte-silver-historico, D1): braço SFTP
            -- GEFUS grava dt_referencia no fim do mês; normaliza ao 1º do mês
            -- ANTES do enriquecido/dedup. Dia exato migra p/ dt_movimento.
            date_trunc('month', dt_referencia)::date as dt_referencia,
            {{ parse_hist_date('dt_movimento') }} as dt_movimento,
            'sftp'::text as fonte_serie,
            fonte_interface::text as fonte_tabela,
            source_file,
            hash_linha,
            dt_ingest
        from {{ int059 }}
        where nullif(trim(nu_apf), '') is not null
    ),

    -- fase 2: min_cidades (grão empreendimento/contrato) traz FDS pré-2019 —
    -- acrescentar CTE lendo a bronze da série executiva filtrada.
{% for f in snh_familias %}
    snh_entidades_{{ f.nome | lower }} as (
{{ prata_dhist_snh_arm(ref(f.modelo), 'Entidades', 'FDS / Entidades', 'ENTIDADES') }}
    ),
{% endfor %}
    -- braço obra_mensal (change consolidar-schemas-historico-reloginho, D2/C2).
    obra_entidades as (
{{ historico_obra_mensal_rows(ref('bronze_shpt_obra_mensal_fds'), 'Entidades', 'FDS / Entidades') }}
    ),
    unioned as (
        select *
        from fds_caixa
        {% for f in snh_familias %}
        union all by name
        select *
        from snh_entidades_{{ f.nome | lower }}
        {% endfor %}
        union all by name
        select *
        from obra_entidades
    ),

    enriquecido as (
        select
            *,
            max(dt_inicio_obra) over grao as dt_inicio_obra_grao,
            max(responsavel_id) over grao as responsavel_id_grao,
            max(responsavel_nome) over grao as responsavel_nome_grao,
            max(dt_movimento) over grao as dt_movimento_grao,
            max(quantidade_uh_entregues) over grao as quantidade_uh_entregues_grao,
            -- entrega/conclusao vem so do braco SFTP (INT059); na janela sobreposta
            -- 2024-06..2024-11 a linha SNH vence a dedup e traz esses campos nulos --
            -- preserva o valor SFTP do mesmo grao; o coalesce no select final cai na
            -- espinha nas demais lacunas (change destravar-datas-obra-entrega-silver-historico).
            max(dt_entrega_uh) over grao as dt_entrega_uh_grao,
            max(dt_conclusao_obra) over grao as dt_conclusao_obra_grao,
            max(quantidade_uh_concluidas) over grao as quantidade_uh_concluidas_grao,
            max(
                case when dt_entrega_uh is not null then fonte_tabela end
            ) over grao as fonte_entrega_uh_grao,
            -- Blocos A/C (change colunas-orfas-bronze-historico).
            max(sinal_retomada_bruto) over grao as sinal_retomada_bruto_grao,
            max(motivo_paralisacao_bruto) over grao as motivo_paralisacao_bruto_grao,
            max(desc_situacao_contrato) over grao as desc_situacao_contrato_grao,
            max(dt_ultima_liberacao) over grao as dt_ultima_liberacao_grao,
            max(dt_primeira_entrega) over grao as dt_primeira_entrega_grao,
            max(dt_assinatura_projeto) over grao as dt_assinatura_projeto_grao,
            max(
                case when dt_primeira_entrega is not null then fonte_tabela end
            ) over grao as fonte_primeira_entrega_grao
        from unioned
        window grao as (partition by frente_mcmv, apf, dt_referencia)
    ),

    dedup as (
        select
            *,
            row_number() over (
                partition by frente_mcmv, apf, dt_referencia
                order by case fonte_serie when 'snh' then 0 else 1 end, source_file
            ) as rn
        from enriquecido
    ),

    -- situacao_canonica + regiao_* por left join aos seeds de referência
    -- (D1/D3 da change serie-historica-situacao-obra-regiao): resolvidas uma
    -- vez, depois da união e da dedup — status_operacional e uf já estão no
    -- contrato comum de todos os braços (SFTP e SNH). status cru preservado.
    -- Obs.: o braço SFTP INT059 não traz uf (regiao_* fica nula nele; o braço
    -- SNH tem uf).
    enriquecido_dominio as (
        select
            d.*,
            -- espinha de entregas por APF (change enriquecer-datas-acompanhamento-historico):
            -- unica fonte de dt_entrega_uh do FDS por ora (INT059 e escopo A/C).
            esp.dt_ultima_entrega as esp_dt_ultima_entrega,
            esp.uh_entregues_acumulada as esp_uh_entregues,
            case
                when nullif(trim(d.status_operacional), '') is null
                then null
                when lower(trim(d.status_operacional)) in ('null', 'nan')
                then null
                when ds.situacao_canonica is not null
                then ds.situacao_canonica
                else 'nao_mapeada'
            end as situacao_canonica,
            dr.regiao_sigla,
            dr.regiao_nome,
            -- Bloco A (change colunas-orfas-bronze-historico).
            case
                when coalesce(d.sinal_retomada_bruto, d.sinal_retomada_bruto_grao) is null then null
                when dret.sinal_retomada is not null then dret.sinal_retomada
                else 'nao_mapeada'
            end as sinal_retomada,
            case
                when coalesce(d.motivo_paralisacao_bruto, d.motivo_paralisacao_bruto_grao) is null then null
                when dmot.motivo_paralisacao is not null then dmot.motivo_paralisacao
                else 'nao_mapeada'
            end as motivo_paralisacao
        from dedup d
        left join {{ ref('dominio_status') }} ds
            on lower(trim(d.status_operacional)) = lower(trim(ds.valor_bruto))
        left join {{ ref('dominio_regiao_uf') }} dr
            on upper(trim(d.uf)) = upper(trim(dr.uf))
        left join {{ ref('prata_historico_entrega_apf') }} esp
            on d.apf = esp.apf
        left join {{ ref('dominio_retomada') }} dret
            on lower(trim(coalesce(d.sinal_retomada_bruto, d.sinal_retomada_bruto_grao)))
               = lower(trim(dret.valor_bruto))
        left join {{ ref('dominio_motivo_paralisacao') }} dmot
            on lower(trim(coalesce(d.motivo_paralisacao_bruto, d.motivo_paralisacao_bruto_grao)))
               = lower(trim(dmot.valor_bruto))
    ),

    -- id_empreendimento + fase_empreendimento (change id-empreendimento-eixo-historico,
    -- D1): a identidade estavel do empreendimento FDS vem da dim do #130
    -- (prata_fds_historico_dim_empreendimento), que liga os APFs de fase Projeto/Obra/
    -- Desligamento de um mesmo empreendimento. APF historico ausente da dim
    -- (~3% da serie, pre-cadastro atual) cai no fallback md5 -- a MESMA formula do
    -- braco de fallback da propria dim, entao um APF single-fase resolve igual
    -- nas duas. O grao da linha continua (frente, apf, dt_referencia).
    enriquecido_id as (
        select
            e.*,
            coalesce(
                dim.id_empreendimento, md5('empreendimento-fds|' || e.apf)
            ) as id_empreendimento,
            dim.fase_empreendimento
        from enriquecido_dominio e
        left join {{ ref('prata_fds_historico_dim_empreendimento') }} dim on e.apf = dim.apf
    ),

    -- 22 colunas de obra_mensal por left join no grão (change
    -- consolidar-schemas-historico-reloginho, D2).
{{ historico_obra_enriquecido(ref('bronze_shpt_obra_mensal_fds'), 'Entidades', 'enriquecido_id') }}

    resolvido as (
        select
    md5(
        concat_ws(
            '|', 'empreendimento', frente_mcmv, coalesce(apf, ''), dt_referencia::text
        )
    ) as id_historico_snapshot,
    md5(concat_ws('|', programa, frente_mcmv, coalesce(apf, ''))) as id_negocio_historico,
    programa,
    frente_mcmv,
    grupo_linha,
    linha_mcmv,
    grao_registro,
    agente_financeiro,
    apf,
    -- codigo_empreendimento = chave estavel do empreendimento (D2): alinha com
    -- silver_mcmv_entidades_base. Era = apf (nu_apf); passa a coalesce(id, apf).
    coalesce(id_empreendimento, apf) as codigo_empreendimento,
    nome_empreendimento,
    codigo_ibge_municipio,
    municipio,
    uf,
    coalesce(responsavel_id, responsavel_id_grao) as responsavel_id,
    coalesce(responsavel_nome, responsavel_nome_grao) as responsavel_nome,
    quantidade_uh,
    -- coalesce so age sobre NULL (0 explicito e informacao). Ordem: braco da
    -- linha > grao > espinha SNH (change enriquecer-datas-acompanhamento-historico).
    coalesce(
        quantidade_uh_entregues, quantidade_uh_entregues_grao, esp_uh_entregues
    ) as quantidade_uh_entregues,
    valor_contratado,
    valor_desembolsado,
    percentual_execucao_fisica,
    status_operacional,
    dt_contratacao,
    coalesce(dt_inicio_obra, dt_inicio_obra_grao) as dt_inicio_obra,
    -- dt_entrega -> dt_entrega_uh + dt_conclusao_obra (BREAKING). Precedencia:
    -- valor do braco SFTP INT059 da linha > mesmo valor preservado no grao >
    -- espinha SNH (change destravar-datas-obra-entrega-silver-historico).
    coalesce(dt_entrega_uh, dt_entrega_uh_grao, esp_dt_ultima_entrega) as dt_entrega_uh,
    coalesce(dt_conclusao_obra, dt_conclusao_obra_grao) as dt_conclusao_obra,
    coalesce(
        quantidade_uh_concluidas, quantidade_uh_concluidas_grao
    ) as quantidade_uh_concluidas,
    dt_previsao_entrega,
    qt_uh_previsao_entrega,
    case
        when coalesce(dt_entrega_uh, dt_entrega_uh_grao) is not null
        then coalesce(
            nullif(
                'sftp:' || regexp_extract(
                    coalesce(
                        case when dt_entrega_uh is not null then fonte_tabela end,
                        fonte_entrega_uh_grao
                    ),
                    '(INT[0-9]+)',
                    1
                ),
                'sftp:'
            ),
            'sftp'
        )
        when esp_dt_ultima_entrega is not null
        then 'snh:entrega_evento'
    end as dt_entrega_uh_fonte,
    dt_referencia,
    coalesce(dt_movimento, dt_movimento_grao) as dt_movimento,
    fonte_serie,
    fonte_tabela,
    source_file,
    hash_linha,
    dt_ingest,
    situacao_canonica,
    regiao_sigla,
    regiao_nome,
    -- id_empreendimento / fase_empreendimento ao fim do contrato comum
    -- (change id-empreendimento-eixo-historico, D1/D2).
    id_empreendimento,
    fase_empreendimento,
    -- quantidades de UH e sinais de obra (change enriquecer-quantidades-uh-e-sinais-obra-historico).
    -- FDS: braço INT059 não reporta ociosas/inicial/pendencia; braço SNH traz
    -- distrato/vigência. NULL onde a fonte não reporta.
    quantidade_uh_distratadas,
    quantidade_uh_vigentes,
    quantidade_uh_ociosas,
    quantidade_uh_inicial,
    cod_pendencia_obra,
    coalesce(
        percentual_execucao_financeira_reportada,
        case
            when valor_contratado > 0 and valor_desembolsado is not null
            then valor_desembolsado / nullif(valor_contratado, 0) * 100
        end
    ) as percentual_execucao_financeira,
    case
        when percentual_execucao_financeira_reportada is not null
        then 'reportada'
        when valor_contratado > 0 and valor_desembolsado is not null
        then 'derivada'
    end as percentual_execucao_financeira_fonte,
    -- Blocos A/C (change colunas-orfas-bronze-historico) — ao fim do contrato.
    sinal_retomada,
    motivo_paralisacao,
    coalesce(desc_situacao_contrato, desc_situacao_contrato_grao) as desc_situacao_contrato,
    coalesce(dt_ultima_liberacao, dt_ultima_liberacao_grao) as dt_ultima_liberacao,
    coalesce(dt_primeira_entrega, dt_primeira_entrega_grao) as dt_primeira_entrega,
    case
        when coalesce(dt_primeira_entrega, dt_primeira_entrega_grao) is not null
        then coalesce(
            nullif(
                'sftp:' || regexp_extract(
                    coalesce(
                        case when dt_primeira_entrega is not null then fonte_tabela end,
                        fonte_primeira_entrega_grao
                    ),
                    '(INT[0-9]+)',
                    1
                ),
                'sftp:'
            ),
            'sftp'
        )
    end as dt_primeira_entrega_fonte,
    coalesce(dt_assinatura_projeto, dt_assinatura_projeto_grao) as dt_assinatura_projeto
    -- 22 colunas de obra_mensal (change consolidar-schemas-historico-reloginho, D2).
    {{ historico_obra_cols_resolvido() }}
from enriquecido_obra
where
    rn = 1
    -- quarentena (change vocabulario-e-qualidade-financeira-historica, D6):
    -- anti-join por (fonte_familia = 'fds_historico', chave_natural = apf).
    and not exists (
        select 1
        from {{ ref('quarentena_valores_financeiros') }} q
        where q.fonte_familia = 'fds_historico' and q.chave_natural = apf
    )
    )
{{ historico_silver_tail() }}
