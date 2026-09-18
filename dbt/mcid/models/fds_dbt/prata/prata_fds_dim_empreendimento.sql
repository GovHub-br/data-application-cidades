{{ config(materialized="table") }}

-- Prata: Dimensao de empreendimento FDS (Entidades)
-- Resolve a identidade estavel do empreendimento (id_empreendimento) a partir do
-- APF-ancora (Fase Projeto), ligando APFs de fases distintas (Projeto/Obra/
-- Desligamento) do mesmo empreendimento.
--
-- Sucessora de `silver_atual_dim_empreendimento` (change #130 /
-- id-empreendimento-eixo-historico), reescrita para a arquitetura nova:
-- o cadastro passou a ser lido de `prata_fds_cadastro_pj` (que ja expoe
-- ic_mudanca_fase / apf_mudanca_fase / qt_uh_construcao / dt_inicio_obra /
-- empreendimento_nome), no lugar das bronzes `bronze_fds_cadastro_pj` e
-- `bronze_fds_mudanca_fase_eventos` do desenho antigo. O contrato de saida
-- (colunas e grao) e identico.
--
-- Fontes (em ordem de precedencia, seed curado vence):
-- 1. seed_apf_fase_fds (xlsx RELACAO_APF_FASES_FDS, curado) - mapeamento completo
-- 2. eventos de mudanca de fase (ic_mudanca_fase do cadastro NOVO)
-- 3. fallback: APFs do cadastro atual nao cobertos (single-fase)
--
-- NOTA (INT059): o campo nu_apf_nao_obra do INT059 carrega o vinculo de fase APENAS
-- para o legado PMCMV-E (125 registros, sem duplicatas), ja cobertos pelo seed.
-- No NOVO PMCMV-E (305 registros) nao ha vinculo de fase (0). Por isso o INT059 nao
-- entra como fonte da dim em v1; o vinculo futuro do NOVO vem de ic_mudanca_fase.
--
-- Grao: 1 linha por (id_empreendimento, apf).
-- Regra: id_empreendimento = md5('empreendimento-fds|' || apf_ancora).
--
-- Colunas de auditoria fase_corrigida / dt_correcao / origem_correcao vem do
-- seed_correcao_fase_projeto (GEFUS CORRECAO_FASE_PROJETO) por left join sobre
-- apf -- EVIDENCIA, nao sobrescreve fase_empreendimento (change
-- id-empreendimento-eixo-historico, D4).
with
    cadastro as (select * from {{ ref("prata_fds_cadastro_pj") }}),

    seed as (
        select
            apf::text as apf,
            fase_empreendimento,
            apf_ancora::text as apf_ancora,
            nome_empreendimento,
            arquivo_origem as origem
        from {{ ref("seed_apf_fase_fds") }}
    ),

    -- Eventos de mudanca de fase (ic_mudanca_fase do cadastro NOVO).
    -- OBS: hoje nenhum registro tem ic_mudanca_fase = true (mudancas ja
    -- resolvidas), entao este braco e vazio na pratica. A taxonomia
    -- Projeto -> Obra abaixo e provisoria; quando houver dados, a fase real
    -- deve ser resolvida por outra fonte, nao inferida apenas do flag.
    eventos as (
        select apf::text as apf, nullif(trim(apf_mudanca_fase), '') as apf_mudanca_fase
        from cadastro
        where ic_mudanca_fase
    ),

    eventos_long as (
        select
            apf,
            'Projeto'::text as fase_empreendimento,
            apf as apf_ancora,
            null::text as nome_empreendimento,
            'evento'::text as origem
        from eventos
        union all
        select
            apf_mudanca_fase as apf,
            'Obra'::text as fase_empreendimento,
            apf as apf_ancora,
            null::text as nome_empreendimento,
            'evento'::text as origem
        from eventos
        where apf_mudanca_fase is not null
    ),

    uniao as (
        select *
        from seed
        union all
        select *
        from eventos_long
        where apf not in (select apf from seed)
    ),

    fallback as (
        select
            c.apf::text as apf,
            case
                when coalesce(c.qt_uh_construcao, 0) > 0 or c.dt_inicio_obra is not null
                then 'Obra'
                else 'Projeto'
            end as fase_empreendimento,
            c.apf::text as apf_ancora,
            c.empreendimento_nome as nome_empreendimento,
            'fallback'::text as origem
        from cadastro c
        where c.apf::text not in (select apf from uniao)
    ),

    final as (
        select *
        from uniao
        union all
        select *
        from fallback
    ),

    -- Correcao retroativa de fase (GEFUS CORRECAO_FASE_PROJETO, via seed curado
    -- seed_correcao_fase_projeto -- change id-empreendimento-eixo-historico, D4).
    -- EVIDENCIA APENAS: nao sobrescreve fase_empreendimento; a correcao fica
    -- visivel ao lado para auditoria. Promover a precedencia e uma decisao v2.
    correcao as (
        select
            apf::text as apf,
            fase_corrigida,
            dt_correcao::date as dt_correcao,
            arquivo_origem as origem_correcao
        from {{ ref("seed_correcao_fase_projeto") }}
    )

select
    md5('empreendimento-fds|' || f.apf_ancora) as id_empreendimento,
    f.apf,
    f.fase_empreendimento,
    (f.apf = f.apf_ancora) as apf_ancora,
    f.nome_empreendimento as nome_empreendimento_canonico,
    f.origem as origem_mapeamento,
    c.fase_corrigida,
    c.dt_correcao,
    c.origem_correcao,
    current_timestamp as dt_carga,
    current_timestamp as dt_valid_from,
    null::timestamp as dt_valid_to,
    true as is_current,
    md5(concat_ws('|', f.apf, f.fase_empreendimento, f.apf_ancora)) as hash_linha
from final f
left join correcao c on f.apf = c.apf
