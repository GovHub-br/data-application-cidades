{{ config(materialized="table") }}

-- Prata: Operações paralisadas do setor público, com o motivo em texto livre.
-- Fonte: bronze_sftp_canalfgts_operacoes_paralisadas
-- Grão: uma linha por `cod_contrato`.
--
-- É o único arquivo do pacote que traz o PORQUÊ da paralisação escrito por gente, e
-- o único que mistura TRÊS formatos de data na mesma tabela: `dt_previsao_conclusao_
-- objeto` em DD/MM/AAAA, `dt_ultimo_bm` em ISO e `data_base` em MM/AA. Um parser só
-- para os três devolve nulo em duas das colunas sem erro nenhum aparecer.
select
    trim(cod_contrato) as cod_contrato,
    nullif(trim(cod_contrato_dv), '') as cod_contrato_dv,
    nullif(trim(cod_operacao_agente_financeiro), '') as cod_operacao,
    {{ parse_int("dias_sem_evolucao") }} as dias_sem_evolucao,
    nullif(trim(faixa_paralisacao), '') as faixa_paralisacao,
    nullif(trim(situacao_atual), '') as situacao_atual,
    nullif(trim(ultimo_motivo_paralisacao), '') as ultimo_motivo_paralisacao,
    nullif(
        trim(ultima_descricao_motivo_paralisacao_preenchida), ''
    ) as descricao_motivo_paralisacao,
    nullif(trim(ultimo_motivo_excecao), '') as ultimo_motivo_excecao,
    nullif(trim(ultimo_entrave_preenchido), '') as ultimo_entrave,
    nullif(trim(plano_acao_aprovado_af), '') as plano_acao_aprovado_af,
    nullif(trim(termo_aditivo_assinado_tomador), '') as termo_aditivo_assinado_tomador,
    {{ parse_data_iso("dt_ultimo_bm") }} as dt_ultimo_bm,

    -- Mês de referência do relatório, não um dia: a origem manda `08/26`.
    {{ parse_competencia_mm_aa("data_base") }} as dt_base,
    {{ parse_data_ddmmaaaa("dt_previsao_conclusao_objeto") }}
    as dt_previsao_conclusao_objeto,

    -- Previsão de conclusão já vencida quando o relatório foi tirado: o sinal mais
    -- direto de "fora do prazo" que este arquivo carrega. A régua é o PRIMEIRO dia do
    -- mês de referência, que é o mais conservador — previsão dentro do próprio mês da
    -- data-base não conta como vencida.
    {{ parse_data_ddmmaaaa("dt_previsao_conclusao_objeto") }}
    < {{ parse_competencia_mm_aa("data_base") }} as ic_previsao_vencida,

    _source_file as arquivo_de_origem,
    nullif(trim(_ingested_at), '')::timestamp as criado_em

from {{ ref("bronze_sftp_canalfgts_operacoes_paralisadas") }}
