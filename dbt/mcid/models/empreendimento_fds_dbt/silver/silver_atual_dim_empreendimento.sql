{{ config(materialized="table") }}

-- Silver: Dimensao de empreendimento FDS (Entidades)
-- Resolve a identidade estavel do empreendimento (id_empreendimento) a partir do
-- APF-ancora (Fase Projeto), ligando APFs de fases distintas (Projeto/Obra/
-- Desligamento) do mesmo empreendimento.
--
-- Fontes (em ordem de precedencia, seed curado vence):
-- 1. seed_apf_fase_fds (xlsx RELACAO_APF_FASES_FDS, curado) - mapeamento completo
-- 2. fds_mudanca_fase_eventos (ic_mudanca_fase, eventos futuros do NOVO)
-- 3. fallback: APFs do cadastro atual nao cobertos (single-fase)
--
-- NOTA (INT059): o campo nu_apf_nao_obra do INT059 carrega o vinculo de fase APENAS
-- para o legado PMCMV-E (125 registros, sem duplicatas), ja cobertos pelo seed.
-- No NOVO PMCMV-E (305 registros) nao ha vinculo de fase (0). Por isso o INT059 nao
-- entra como fonte da dim em v1; o vinculo futuro do NOVO vem de ic_mudanca_fase.
--
-- Grao: 1 linha por (id_empreendimento, apf).
-- Regra: id_empreendimento = md5('empreendimento-fds|' || apf_ancora).
with
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
    -- OBS: hoje a tabela esta vazia (ic_mudanca_fase = false nos 343 registros).
    -- A taxonomia Projeto -> Obra abaixo e provisoria; quando houver dados, a fase
    -- real deve ser resolvida por outra fonte, nao inferida apenas do flag.
    eventos_long as (
        select
            apf,
            'Projeto'::text as fase_empreendimento,
            apf as apf_ancora,
            null::text as nome_empreendimento,
            'evento'::text as origem
        from {{ ref("bronze_fds_mudanca_fase_eventos") }}
        union all
        select
            apf_mudanca_fase as apf,
            'Obra'::text as fase_empreendimento,
            apf as apf_ancora,
            null::text as nome_empreendimento,
            'evento'::text as origem
        from {{ ref("bronze_fds_mudanca_fase_eventos") }}
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
            c.apf,
            case
                when coalesce(c.qt_uh_construcao, 0) > 0 or c.dt_inicio_obra is not null
                then 'Obra'
                else 'Projeto'
            end as fase_empreendimento,
            c.apf as apf_ancora,
            c.empreendimento_nome as nome_empreendimento,
            'fallback'::text as origem
        from {{ ref("bronze_fds_cadastro_pj") }} c
        where c.apf not in (select apf from uniao)
    ),

    final as (
        select *
        from uniao
        union all
        select *
        from fallback
    )

select
    md5('empreendimento-fds|' || apf_ancora) as id_empreendimento,
    apf,
    fase_empreendimento,
    (apf = apf_ancora) as apf_ancora,
    nome_empreendimento as nome_empreendimento_canonico,
    origem as origem_mapeamento,
    current_timestamp as dt_carga,
    current_timestamp as dt_valid_from,
    null::timestamp as dt_valid_to,
    true as is_current,
    md5(concat_ws('|', apf, fase_empreendimento, apf_ancora)) as hash_linha
from final
