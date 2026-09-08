{{ config(materialized="table", alias="silver_historico_empreendimento", schema="empreendimento_far") }}

-- SILVER — série histórica mensal de empreendimentos MCMV da frente FAR.
--
-- Une as duas fontes de história por empreendimento, no contrato semântico
-- comum (ver models/docs/entregas/separacao-silver-historico-por-frente.md):
--
-- SFTP  — bronzes por interface INT040 (FAR CAIXA) e INT054 (FAR BB).
-- Janela 2019-12 → atual.
-- SNH   — bronzes por agente (BB, CAIXA), linhas com modalidade = 'FAR'.
-- Janela 2024-06 → atual.
--
-- Desde a change pipeline-bronze-historica-destino-trocavel (D5), cada uma
-- dessas fontes e uma TABELA POR FAMILIA: não há mais bronze unificada, e a
-- união com projeção explícita acontece aqui.
--
-- Grão de saída: empreendimento × mês — 1 linha por (frente_mcmv, apf,
-- dt_referencia), com dt_referencia normalizado ao 1º dia do mês em cada braço
-- (change dedup-fonte-silver-historico, D1: o SFTP grava fim de mês, a SNH dia
-- 1). Na janela sobreposta (2024-06 → 2024-11) prevalece a linha do SNH (D6):
-- mais rica em situação/fase de obra. Colunas que só o SFTP tem (dt_inicio_obra,
-- responsável, valor) são preservadas — no grão via coalesce ANTES da escolha
-- da linha, e entre meses via LOCF na cauda (silver_tail, D3).
--
-- Numéricos em formato brasileiro (13.898.046,25) e dot-decimal são absorvidos
-- por parse_hist_numeric (valores R$) / parse_hist_double (%) / parse_hist_bigint.
--
-- Destino conforme o target (D2): arquivo local em `staging_duckdb`, Postgres
-- atachado em `prod_duckdb`. As bronzes precisam existir no compile
-- (coalesce_present_parsed introspecciona a relação) — ver
-- models/mcmv_historico_dbt/README.md.
{% set int040 = ref('bronze_mcmv_historico_empreendimento_int040') %}
{% set int054 = ref('bronze_mcmv_historico_empreendimento_int054') %}
{% set snh_familias = familias_snh_empreendimento() %}

with

    far_caixa as (  -- INT040
        select
            'Minha Casa Minha Vida'::text as programa,
            'FAR'::text as frente_mcmv,
            'Subsidiada'::text as grupo_linha,
            'FAR'::text as linha_mcmv,
            'empreendimento_mes'::text as grao_registro,
            'CAIXA'::text as agente_financeiro,
            nullif(trim(nu_apf), '')::text as apf,
            nullif(trim(nu_apf), '')::text as codigo_empreendimento,
            nullif(trim(no_empreendimento), '')::text as nome_empreendimento,
            nullif(trim(cod_municipio_ibge), '')::text as codigo_ibge_municipio,
            nullif(trim(no_municipio), '')::text as municipio,
            nullif(trim(sg_uf_muncicipio), '')::text as uf,
            nullif(trim(cnpj_proponente), '')::text as responsavel_id,
            nullif(trim(razao_social_proponente), '')::text as responsavel_nome,
            {{ parse_hist_bigint('qt_unidade_financiadas') }} as quantidade_uh,
            {{ parse_hist_bigint('qt_unidades_entregues') }} as quantidade_uh_entregues,
            {{ parse_hist_numeric('vr_investimento') }} as valor_contratado,
            {{ parse_hist_numeric('vr_liberado') }} as valor_desembolsado,
            {{ parse_hist_double('percentual_obra_realizado') }}
            as percentual_execucao_fisica,
            nullif(trim(situacao_obra_gefus), '')::text as status_operacional,
            {{ parse_hist_date('dt_assinatura') }} as dt_contratacao,
            {{ parse_hist_date('dt_inicio_obra') }} as dt_inicio_obra,
            {{ parse_hist_date('dt_ultima_entrega') }} as dt_entrega_uh,
            coalesce(
                {{ parse_hist_date('dt_termino_obra') }},
                {{ parse_hist_date('dt_legalizacao') }}
            ) as dt_conclusao_obra,
            -- quantidade_uh_concluidas: coluna presente no INT040; null-guard por
            -- coalesce_present_parsed contra drift de schema do parquet (change
            -- destravar-datas-obra-entrega-silver-historico).
            {{ coalesce_present_parsed(
                int040, ['qt_unidades_concluidas'], 'parse_hist_bigint', 'bigint'
            ) }} as quantidade_uh_concluidas,
            null::date as dt_previsao_entrega,
            null::bigint as qt_uh_previsao_entrega,
            {{ historico_uh_sinais_sftp(int040, ociosas=true, pendencia=true) }}
            {{ historico_bloco_ac_sftp(int040) }}
            -- grão mensal (change dedup-fonte-silver-historico, D1): o braço SFTP
            -- GEFUS grava dt_referencia no fim do mês (25-31); a SNH grava dia 1.
            -- Normaliza ao 1º do mês ANTES do enriquecido/dedup p/ colapsar as
            -- duas fontes do mesmo APF/mês. Dia exato migra p/ dt_movimento.
            date_trunc('month', dt_referencia)::date as dt_referencia,
            {{ parse_hist_date('dt_movimento') }} as dt_movimento,
            'sftp'::text as fonte_serie,
            fonte_interface::text as fonte_tabela,
            source_file,
            hash_linha,
            dt_ingest
        from {{ int040 }}
        where nullif(trim(nu_apf), '') is not null
    ),

    far_bb as (  -- INT054
        select
            'Minha Casa Minha Vida'::text as programa,
            'FAR'::text as frente_mcmv,
            'Subsidiada'::text as grupo_linha,
            'FAR'::text as linha_mcmv,
            'empreendimento_mes'::text as grao_registro,
            'Banco do Brasil'::text as agente_financeiro,
            nullif(trim(nu_apf), '')::text as apf,
            nullif(trim(nu_apf), '')::text as codigo_empreendimento,
            nullif(trim(no_empreendimento), '')::text as nome_empreendimento,
            nullif(trim(cod_municipio_ibge), '')::text as codigo_ibge_municipio,
            nullif(trim(no_municipio), '')::text as municipio,
            nullif(trim(sg_uf), '')::text as uf,
            nullif(trim(cnpj_proponente), '')::text as responsavel_id,
            nullif(trim(razao_social_proponente), '')::text as responsavel_nome,
            {{ parse_hist_bigint('qt_unidades_habitacionais') }} as quantidade_uh,
            {{ parse_hist_bigint('qt_unidades_entregues') }} as quantidade_uh_entregues,
            {{ parse_hist_numeric('vr_investimento') }} as valor_contratado,
            {{ parse_hist_numeric('total_liberado_far') }} as valor_desembolsado,
            {{ parse_hist_double('percentual_obra_realizado') }}
            as percentual_execucao_fisica,
            nullif(trim(situacao_obra), '')::text as status_operacional,
            {{ parse_hist_date('dt_contratacao') }} as dt_contratacao,
            {{ parse_hist_date('dt_inicio_obra') }} as dt_inicio_obra,
            {{ parse_hist_date('dt_ultima_entrega') }} as dt_entrega_uh,
            coalesce(
                {{ parse_hist_date('dt_termino_obra') }},
                {{ parse_hist_date('dt_legalizacao') }}
            ) as dt_conclusao_obra,
            {{ coalesce_present_parsed(
                int054, ['qt_unidades_concluidas'], 'parse_hist_bigint', 'bigint'
            ) }} as quantidade_uh_concluidas,
            null::date as dt_previsao_entrega,
            null::bigint as qt_uh_previsao_entrega,
            {{ historico_uh_sinais_sftp(int054, ociosas=true, pendencia=true) }}
            {{ historico_bloco_ac_sftp(int054) }}
            -- grão mensal (change dedup-fonte-silver-historico, D1) — ver far_caixa.
            date_trunc('month', dt_referencia)::date as dt_referencia,
            {{ parse_hist_date('dt_movimento') }} as dt_movimento,
            'sftp'::text as fonte_serie,
            fonte_interface::text as fonte_tabela,
            source_file,
            hash_linha,
            dt_ingest
        from {{ int054 }}
        where nullif(trim(nu_apf), '') is not null
    ),

    -- fase 2: bb_YYYY_*_pj / _pj_pf (2015-2019) — preenche o gap entre entrada_bb
    -- (2014-09) e o início da série SFTP (2019-12).
    -- fase 2: int040/int054_ministeriocidades_* dentro do dump (2018) — estende a
    -- série SFTP ~1 ano para trás (só adicionar glob à bronze SFTP).
{% for f in snh_familias %}
    snh_far_{{ f.nome | lower }} as (
{{ silver_historico_snh_arm(ref(f.modelo), 'FAR', 'FAR', 'FAR') }}
    ),
{% endfor %}
    unioned as (
        select *
        from far_caixa
        union all
        select *
        from far_bb
        {% for f in snh_familias %}
        union all
        select *
        from snh_far_{{ f.nome | lower }}
        {% endfor %}
    ),

    -- D6: preserva colunas complementares (presentes só no SFTP) ao longo do grão
    -- antes de escolher a linha vencedora.
    enriquecido as (
        select
            *,
            max(dt_inicio_obra) over grao as dt_inicio_obra_grao,
            max(responsavel_id) over grao as responsavel_id_grao,
            max(responsavel_nome) over grao as responsavel_nome_grao,
            max(dt_movimento) over grao as dt_movimento_grao,
            -- entrega/conclusao vem so do braco SFTP; na janela sobreposta
            -- 2024-06..2024-11 a linha SNH vence a dedup e traz esses campos
            -- nulos -- preserva o valor SFTP do mesmo grao (change
            -- enriquecer-datas-acompanhamento-historico; quantidade_uh_concluidas
            -- pela change destravar-datas-obra-entrega-silver-historico).
            max(dt_entrega_uh) over grao as dt_entrega_uh_grao,
            max(dt_conclusao_obra) over grao as dt_conclusao_obra_grao,
            max(quantidade_uh_concluidas) over grao as quantidade_uh_concluidas_grao,
            max(
                case when dt_entrega_uh is not null then fonte_tabela end
            ) over grao as fonte_entrega_uh_grao,
            -- Blocos A/C (change colunas-orfas-bronze-historico): preserva o sinal
            -- e os marcos ao longo do grão antes da escolha da linha vencedora.
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
    -- situacao_canonica = NULL quando status ausente/placeholder, 'nao_mapeada'
    -- quando não casa no seed (o dentro_do_dominio em warn lista o que faltou).
    enriquecido_dominio as (
        select
            d.*,
            -- espinha de entregas por APF (change enriquecer-datas-acompanhamento-historico):
            -- fallback/refresh de dt_entrega_uh e quantidade_uh_entregues nas
            -- lacunas do SFTP. Join por apf apenas (a espinha e atributo do APF,
            -- nao serie mensal).
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
            -- Blocos A (change colunas-orfas-bronze-historico): sinal_retomada /
            -- motivo_paralisacao canônicos via seed. NULL quando o bruto (da linha
            -- ou preservado no grão) é ausente; 'nao_mapeada' fora do seed.
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
    coalesce(dt_inicio_obra, dt_inicio_obra_grao) as dt_inicio_obra,
    -- dt_entrega -> dt_entrega_uh + dt_conclusao_obra (BREAKING). Precedencia:
    -- valor do braco SFTP da linha > mesmo valor preservado no grao > espinha SNH.
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
    -- (change id-empreendimento-eixo-historico). No FAR o APF ja e o
    -- empreendimento -- id_empreendimento = apf, sem fase administrativa.
    apf as id_empreendimento,
    null::text as fase_empreendimento,
    -- quantidades de UH e sinais de obra (change enriquecer-quantidades-uh-e-sinais-obra-historico).
    -- Vindas dos braços via historico_uh_sinais_sftp / do braço SNH; NULL onde a
    -- fonte não reporta (ausência, não zero).
    quantidade_uh_distratadas,
    quantidade_uh_vigentes,
    quantidade_uh_ociosas,
    quantidade_uh_inicial,
    cod_pendencia_obra,
    -- execução financeira: reportada (INT057) quando existe, senão derivada
    -- (valor_desembolsado / valor_contratado * 100) quando os dois insumos
    -- existem e valor_contratado > 0; senão NULL.
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
    -- quarentena de registros financeiros invalidos (change
    -- vocabulario-e-qualidade-financeira-historica, D6): anti-join ao seed
    -- por (fonte_familia = 'far_historico', chave_natural = apf).
    and not exists (
        select 1
        from {{ ref('quarentena_valores_financeiros') }} q
        where q.fonte_familia = 'far_historico' and q.chave_natural = apf
    )
    )
{{ historico_silver_tail() }}
