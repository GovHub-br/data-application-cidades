{{ config(materialized="table", alias="silver_historico_empreendimento", schema="empreendimento_rural") }}

-- SILVER — série histórica mensal de empreendimentos MCMV da frente Rural (PNHR).
--
-- SFTP  — bronzes por interface INT057 (PNHR BB) e INT065 (PNHR CAIXA).
-- Janela 2019-12 → atual.
-- SNH   — bronzes por agente (BB, CAIXA), modalidade = 'RURAL' (cobre
-- 'RURAL' da CAIXA e 'Rural' do BB). Janela 2024-06 → atual.
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
-- Obs.: INT057 tem a coluna temporal com nome inconsistente entre entregas
-- (idt_movimento vs dt_movimento) — tratado com coalesce.
--
-- Destino conforme o target (D2): arquivo local em `staging_duckdb`, Postgres
-- atachado em `prod_duckdb`. Ver models/mcmv_historico_dbt/README.md para a
-- ordem de build exigida por coalesce_present.
{% set int057 = ref('bronze_mcmv_historico_empreendimento_int057') %}
{% set int065 = ref('bronze_mcmv_historico_empreendimento_int065') %}
{% set snh_familias = familias_snh_empreendimento() %}

with

    rural_bb as (  -- INT057
        select
            'Minha Casa Minha Vida'::text as programa,
            'Rural'::text as frente_mcmv,
            'Subsidiada'::text as grupo_linha,
            'PNHR Rural BB'::text as linha_mcmv,
            'empreendimento_mes'::text as grao_registro,
            'Banco do Brasil'::text as agente_financeiro,
            nullif(trim(nu_contrato_empreendimento), '')::text as apf,
            nullif(trim(nu_contrato_empreendimento), '')::text as codigo_empreendimento,
            nullif(trim(no_empreendimento), '')::text as nome_empreendimento,
            nullif(trim(co_municipio_ibge), '')::text as codigo_ibge_municipio,
            nullif(trim(no_municipio), '')::text as municipio,
            nullif(trim(sg_uf), '')::text as uf,
            nullif(trim(nu_cnpj_entidade), '')::text as responsavel_id,
            nullif(trim(no_entidade_organizadora), '')::text as responsavel_nome,
            {{ parse_hist_bigint('qt_unidades') }} as quantidade_uh,
            {{ parse_hist_bigint('qt_unidades_entregues') }} as quantidade_uh_entregues,
            {{ parse_hist_numeric('vr_investimento') }} as valor_contratado,
            {{ parse_hist_numeric('vr_liberado') }} as valor_desembolsado,
            {{ parse_hist_double('pc_execucao_fisica_obra') }}
            as percentual_execucao_fisica,
            nullif(trim(no_situacao_obra), '')::text as status_operacional,
            {{ parse_hist_date('dt_contrato') }} as dt_contratacao,
            null::date as dt_inicio_obra,
            -- INT057 nao tem dt_ultima_entrega -- entrega_uh vem so da espinha
            -- (change destravar-datas-obra-entrega-silver-historico, task 4.4).
            null::date as dt_entrega_uh,
            {{ parse_hist_date('dt_efetiva_conclusao') }} as dt_conclusao_obra,
            {{ parse_hist_bigint('qt_unidades_concluidas') }} as quantidade_uh_concluidas,
            null::date as dt_previsao_entrega,
            null::bigint as qt_uh_previsao_entrega,
            {{ historico_uh_sinais_sftp(int057, pc_reportada=true) }}
            {{ historico_bloco_ac_sftp(int057) }}
            -- grão mensal (change dedup-fonte-silver-historico, D1): braço SFTP
            -- GEFUS grava dt_referencia no fim do mês; normaliza ao 1º do mês
            -- ANTES do enriquecido/dedup. Dia exato migra p/ dt_movimento.
            date_trunc('month', dt_referencia)::date as dt_referencia,
            {{ parse_hist_date('coalesce(idt_movimento, dt_movimento)') }}
            as dt_movimento,
            'sftp'::text as fonte_serie,
            fonte_interface::text as fonte_tabela,
            source_file,
            hash_linha,
            dt_ingest
        from {{ int057 }}
        where nullif(trim(nu_contrato_empreendimento), '') is not null
    ),

    rural_caixa as (  -- INT065
        select
            'Minha Casa Minha Vida'::text as programa,
            'Rural'::text as frente_mcmv,
            'Subsidiada'::text as grupo_linha,
            'PNHR Rural CAIXA'::text as linha_mcmv,
            'empreendimento_mes'::text as grao_registro,
            'CAIXA'::text as agente_financeiro,
            nullif(trim(nu_apf), '')::text as apf,
            nullif(trim(nu_apf), '')::text as codigo_empreendimento,
            nullif(trim(no_empreendimento), '')::text as nome_empreendimento,
            nullif(trim(co_municipio_ibge), '')::text as codigo_ibge_municipio,
            nullif(trim(no_municipio), '')::text as municipio,
            nullif(trim(sg_uf), '')::text as uf,
            nullif(trim(nu_cnpj_entidade), '')::text as responsavel_id,
            nullif(trim(no_entidade_organizadora), '')::text as responsavel_nome,
            {{ parse_hist_bigint('qtde_unidades') }} as quantidade_uh,
            {{ parse_hist_bigint('qt_unidades_entregues') }} as quantidade_uh_entregues,
            {{ parse_hist_numeric('vr_investimento_pnhr') }} as valor_contratado,
            {{ parse_hist_numeric('vr_liberado') }} as valor_desembolsado,
            {{ parse_hist_double('pc_obra_realizado') }} as percentual_execucao_fisica,
            nullif(trim(no_situacao_obra), '')::text as status_operacional,
            {{ parse_hist_date('dt_contrato') }} as dt_contratacao,
            null::date as dt_inicio_obra,
            -- INT065 traz dt_ultima_entrega (~11%) -- OQ2 resolvida "INT065 entra"
            -- (change destravar-datas-obra-entrega-silver-historico, task 4.3).
            {{ parse_hist_date('dt_ultima_entrega') }} as dt_entrega_uh,
            {{ parse_hist_date('dt_efetiva_conclusao') }} as dt_conclusao_obra,
            {{ parse_hist_bigint('qt_unidades_concluidas') }} as quantidade_uh_concluidas,
            null::date as dt_previsao_entrega,
            null::bigint as qt_uh_previsao_entrega,
            {{ historico_uh_sinais_sftp(int065, inicial=true) }}
            {{ historico_bloco_ac_sftp(int065) }}
            -- grão mensal (change dedup-fonte-silver-historico, D1) — ver rural_bb.
            date_trunc('month', dt_referencia)::date as dt_referencia,
            {{ parse_hist_date('dt_movimento') }} as dt_movimento,
            'sftp'::text as fonte_serie,
            fonte_interface::text as fonte_tabela,
            source_file,
            hash_linha,
            dt_ingest
        from {{ int065 }}
        where nullif(trim(nu_apf), '') is not null
    ),

    -- fase 2: bb_*_pnhr_* mensal (2014-10 → 2018-07) — único sinal de Rural pré-2019.
{% for f in snh_familias %}
    snh_rural_{{ f.nome | lower }} as (
{{ silver_historico_snh_arm(ref(f.modelo), 'Rural', 'PNHR Rural', 'RURAL') }}
    ),
{% endfor %}
    unioned as (
        select *
        from rural_bb
        union all
        select *
        from rural_caixa
        {% for f in snh_familias %}
        union all
        select *
        from snh_rural_{{ f.nome | lower }}
        {% endfor %}
    ),

    enriquecido as (
        select
            *,
            max(responsavel_id) over grao as responsavel_id_grao,
            max(responsavel_nome) over grao as responsavel_nome_grao,
            max(dt_movimento) over grao as dt_movimento_grao,
            -- conclusao vem do braco SFTP (dt_efetiva_conclusao); entrega_uh do
            -- INT065 (dt_ultima_entrega ~11%), INT057 so via espinha. Preserva ao
            -- longo do grao p/ a janela sobreposta com o SNH (change
            -- destravar-datas-obra-entrega-silver-historico).
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
    -- Rural: ~8,5 k APF sem status na fonte → situacao_canonica = NULL.
    enriquecido_dominio as (
        select
            d.*,
            case
                when nullif(trim(d.status_operacional), '') is null
                then null
                when lower(trim(d.status_operacional)) in ('null', 'nan')
                then null
                when ds.situacao_canonica is not null
                then ds.situacao_canonica
                else 'nao_mapeada'
            end as situacao_canonica,
            -- espinha de entregas por APF (change enriquecer-datas-acompanhamento-historico):
            -- unica fonte de dt_entrega_uh do Rural (OQ2 lean: INT065 fica p/ A/C).
            esp.dt_ultima_entrega as esp_dt_ultima_entrega,
            esp.uh_entregues_acumulada as esp_uh_entregues,
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
        left join {{ ref('silver_mcmv_historico_entrega_apf') }} esp
            on d.apf = esp.apf
        left join {{ ref('dominio_retomada') }} dret
            on lower(trim(coalesce(d.sinal_retomada_bruto, d.sinal_retomada_bruto_grao)))
               = lower(trim(dret.valor_bruto))
        left join {{ ref('dominio_motivo_paralisacao') }} dmot
            on lower(trim(coalesce(d.motivo_paralisacao_bruto, d.motivo_paralisacao_bruto_grao)))
               = lower(trim(dmot.valor_bruto))
    ),

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
    codigo_empreendimento,
    nome_empreendimento,
    codigo_ibge_municipio,
    municipio,
    uf,
    coalesce(responsavel_id, responsavel_id_grao) as responsavel_id,
    coalesce(responsavel_nome, responsavel_nome_grao) as responsavel_nome,
    quantidade_uh,
    -- coalesce so age sobre NULL (0 explicito e informacao, nao ausencia)
    coalesce(quantidade_uh_entregues, esp_uh_entregues) as quantidade_uh_entregues,
    valor_contratado,
    valor_desembolsado,
    percentual_execucao_fisica,
    status_operacional,
    dt_contratacao,
    dt_inicio_obra,
    -- dt_entrega -> dt_entrega_uh + dt_conclusao_obra (BREAKING). Rural: entrega_uh
    -- do INT065 (dt_ultima_entrega) > grao > espinha; dt_conclusao_obra do
    -- dt_efetiva_conclusao (SFTP INT057/065).
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
    -- id_empreendimento / fase_empreendimento: contrato comum com o FDS
    -- (change id-empreendimento-eixo-historico). No Rural o APF ja e o
    -- empreendimento -- id_empreendimento = apf, sem fase administrativa.
    apf as id_empreendimento,
    null::text as fase_empreendimento,
    -- quantidades de UH e sinais de obra (change enriquecer-quantidades-uh-e-sinais-obra-historico).
    -- Rural: INT065 traz qtde_uh_inicial; INT057 traz pc_execucao_financeira_obra
    -- (reportada); braço SNH traz distrato/vigência. NULL onde a fonte não reporta.
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
from enriquecido_dominio
where
    rn = 1
    -- quarentena (change vocabulario-e-qualidade-financeira-historica, D6):
    -- anti-join por (fonte_familia = 'rural_historico', chave_natural = apf).
    -- Entram aqui os APF com desembolso > 2x o contratado (teste
    -- desembolso_nao_excede_contratado em error).
    and not exists (
        select 1
        from {{ ref('quarentena_valores_financeiros') }} q
        where q.fonte_familia = 'rural_historico' and q.chave_natural = apf
    )
    )
{{ historico_silver_tail() }}
