{{ config(materialized="table") }}

-- Snapshot corrente (estado atual) derivado da silver historica por frente.
-- Mantem 1 linha por (frente, codigo_empreendimento) com o ultimo mes. No FDS um
-- empreendimento multi-fase tem 2-3 APFs (Projeto/Obra/Desligamento) e colapsa
-- para 1 pela chave estavel codigo_empreendimento (= id_empreendimento no FDS,
-- = apf nas demais frentes; change id-empreendimento-eixo-historico),
-- prevalecendo a fase mais avancada (Desligamento > Obra > Projeto) e nela o
-- dt_referencia mais recente. FAR/Rural: fase nula -> so dt_referencia (inalterado).
-- Consolidado apenas (filtravel por frente_mcmv) — nao ha versao por frente.
--
-- Uniao direta das 3 silvers por frente (FAR/FDS/Rural) — antes lia do helper
-- silver_mcmv_historico_empreendimento, aposentado na convencao 2026-09-04
-- (cada frente materializa como silver_historico_empreendimento no schema da
-- propria frente; nao ha mais um schema unico onde um union all resolveria
-- sozinho).
with
    consolidado as (
        select *
        from {{ ref('silver_mcmv_historico_empreendimento_far') }}
        union all
        select *
        from {{ ref('silver_mcmv_historico_empreendimento_fds') }}
        union all
        select *
        from {{ ref('silver_mcmv_historico_empreendimento_rural') }}
    ),

    ultimo as (
        select
            *,
            row_number() over (
                partition by frente_mcmv, codigo_empreendimento
                order by
                    case fase_empreendimento
                        when 'Desligamento' then 0
                        when 'Obra' then 1
                        when 'Projeto' then 2
                        else 3
                    end,
                    dt_referencia desc
            ) as rn
        from consolidado
    )

select
    id_historico_snapshot,
    programa,
    frente_mcmv,
    grupo_linha,
    linha_mcmv,
    'empreendimento'::text as grao_registro,
    agente_financeiro,
    apf,
    codigo_empreendimento,
    id_empreendimento,
    fase_empreendimento,
    nome_empreendimento,
    codigo_ibge_municipio,
    municipio,
    uf,
    responsavel_id,
    responsavel_nome,
    quantidade_uh,
    quantidade_uh_entregues,
    -- change destravar-datas-obra-entrega-silver-historico: conclusao fisica de
    -- obra (estagio anterior a entrega da UH) e a previsao de entrega do SNH.
    quantidade_uh_concluidas,
    valor_contratado,
    valor_desembolsado,
    percentual_execucao_fisica,
    status_operacional,
    dt_contratacao,
    dt_inicio_obra,
    -- dt_entrega -> dt_entrega_uh + dt_conclusao_obra (BREAKING; change
    -- enriquecer-datas-acompanhamento-historico). dt_entrega_uh_fonte diz se o
    -- valor veio do feed mensal (sftp[:INTxxx]) ou da espinha (snh:entrega_evento).
    dt_entrega_uh,
    dt_conclusao_obra,
    dt_entrega_uh_fonte,
    dt_previsao_entrega,
    qt_uh_previsao_entrega,
    dt_referencia,
    dt_movimento,
    fonte_tabela,
    source_file,
    situacao_canonica,
    regiao_sigla,
    regiao_nome,
    -- quantidades de UH e sinais de obra + execução financeira
    -- (change enriquecer-quantidades-uh-e-sinais-obra-historico). Aditivas ao
    -- fim do contrato; NULL onde a fonte do último snapshot não reporta.
    quantidade_uh_distratadas,
    quantidade_uh_vigentes,
    quantidade_uh_ociosas,
    quantidade_uh_inicial,
    cod_pendencia_obra,
    percentual_execucao_financeira,
    percentual_execucao_financeira_fonte,
    gap_fisico_financeiro_pp,
    -- fonte_valor = 'carregado' quando o último snapshot do empreendimento é
    -- carry-forward do SNH intermitente (dt_snapshot_efetivo = mês real). D6.
    fonte_valor,
    dt_snapshot_efetivo,
    dt_silver
from ultimo
where rn = 1
