{{ config(materialized="table") }}

-- Gold: Histórico de cada MUNICÍPIO que apresentou proposta ao FNHIS Sub-50 — habilitação no
-- SNHIS, o que propôs e conseguiu no Sub-50, e a experiência anterior com obra habitacional (MCMV
-- de todas as modalidades na SNH). Réplica, para o FNHIS, das perguntas do Rural sobre a entidade
-- organizadora: como se habilita, histórico/capacidade de execução, se entregou o que contratou e
-- se tinha obra pendente quando concorreu.
-- Grão: município beneficiado da proposta (IBGE de 7 dígitos).
--
-- "Habilitação" no FNHIS é a regularidade do ente no SNHIS (lei do fundo, lei do conselho, termo
-- de adesão, plano habitacional, relatório de gestão) — é ela que o tabela mostra; não há o
-- histórico dessa regularidade (só o retrato atual).
-- "Pendente ao concorrer" usa a data do resultado da seleção (var fnhis_data_selecao, padrão
-- 2024-02-20 = arquivo de resultado) e a situação ATUAL das operações contratadas antes dela:
-- operação que hoje está em andamento ou paralisada e foi contratada antes também estava aberta
-- naquela data; a que hoje está concluída pode ter sido concluída depois — fica como incerteza.

{% set data_selecao = var("fnhis_data_selecao", "2024-02-20") %}

with
    prop as (
        select
            cod_ibge,
            max(cod_ibge_6) as cod_ibge_6,
            max(municipio) as municipio,
            max(uf) as uf,
            string_agg(distinct proponente_esfera, '; ') as proponente_esfera,
            count(*) as qt_propostas,
            count(*) filter (where resultado_selecao = 'Não enquadrada') as qt_nao_enquadradas,
            count(*) filter (where resultado_selecao = 'Cota insuficiente da UF') as qt_cota_insuficiente,
            count(*) filter (where resultado_selecao = 'Município já contemplado') as qt_ja_contemplado,
            count(*) filter (where ic_selecionada) as qt_selecionadas,
            coalesce(sum(quantidade_uh), 0) as uh_propostas,
            coalesce(sum(quantidade_uh) filter (where ic_selecionada), 0) as uh_selecionadas
        from {{ ref("prata_fnhis_propostas") }}
        where cod_ibge is not null
        group by cod_ibge
    ),

    tram as (
        select
            cod_ibge,
            count(*) filter (where etapa_ordem = 4) as qt_termos_anulados_rescindidos,
            count(*) filter (where etapa_ordem in (5, 6, 7)) as qt_termos_ativos,
            coalesce(sum(valor_empenhado_acumulado), 0) as valor_empenhado
        from {{ ref("ouro_fnhis_proposta_tramite") }}
        where cod_ibge is not null
        group by cod_ibge
    ),

    snh as (
        select
            left(regexp_replace(codigo_ibge_do_municipio::text, '[^0-9]', '', 'g'), 6) as cod_ibge_6,
            trim({{ target.schema }}.corrigir_mojibake(modalidade::text)) as modalidade,
            trim({{ target.schema }}.corrigir_mojibake(situacao_da_empreendimento_agrupada::text)) as situacao,
            {{ target.schema }}.parse_date_br(nullif(trim(data_da_contratacao::text), '')) as dt_contratacao,
            {{ parse_int("regexp_replace(unidades_contratadas::text, '[^0-9]', '', 'g')") }} as uh_contratadas,
            {{ parse_int("regexp_replace(unidades_entregues::text, '[^0-9]', '', 'g')") }} as uh_entregues,
            {{ parse_int("regexp_replace(unidades_distratadas::text, '[^0-9]', '', 'g')") }} as uh_distratadas
        from {{ ref("bronze_shpt_dados_prioritarios_snh_empreendimentos") }}
        where upper(trim(modalidade::text)) <> 'FNHIS'
    ),

    hist as (
        select
            cod_ibge_6,
            count(*) as qt_operacoes_his_antes,
            string_agg(distinct modalidade, ', ') as modalidades_his,
            count(*) filter (where situacao ~* 'conclu') as qt_concluidas,
            count(*) filter (where situacao ~* 'andamento|n.o inic|projeto') as qt_em_andamento_hoje,
            count(*) filter (where situacao ~* 'paralis') as qt_paralisadas_hoje,
            count(*) filter (where situacao ~* 'distrat') as qt_distratadas,
            coalesce(sum(uh_contratadas), 0) as uh_contratadas_antes,
            coalesce(sum(uh_entregues), 0) as uh_entregues_antes,
            coalesce(sum(uh_distratadas), 0) as uh_distratadas_antes,
            max(dt_contratacao) as dt_ultima_contratacao_his
        from snh
        where dt_contratacao < date '{{ data_selecao }}'
        group by cod_ibge_6
    ),

    reg as (select * from {{ ref("prata_fnhis_regularidade_entes") }} where ente_esfera is distinct from 'Estado/DF')

select
    p.cod_ibge,
    p.municipio,
    p.uf,
    {{ regiao_da_uf("p.uf") }} as regiao,
    p.proponente_esfera,
    r.populacao,
    -- habilitação (SNHIS, retrato atual)
    r.situacao_ente,
    r.ic_ente_regular,
    r.situacao_lei_fundo,
    r.situacao_lei_conselho,
    r.situacao_termo_adesao,
    r.situacao_plano_habitacional,
    r.situacao_relatorio_gestao,
    r.dt_referencia as dt_referencia_snhis,
    -- Sub-50
    p.qt_propostas,
    p.qt_nao_enquadradas,
    p.qt_cota_insuficiente,
    p.qt_ja_contemplado,
    p.qt_selecionadas,
    p.uh_propostas,
    p.uh_selecionadas,
    coalesce(t.qt_termos_ativos, 0) as qt_termos_ativos,
    coalesce(t.qt_termos_anulados_rescindidos, 0) as qt_termos_anulados_rescindidos,
    coalesce(t.valor_empenhado, 0) as valor_empenhado,
    -- experiência anterior com obra habitacional (MCMV na SNH, contratada antes da seleção)
    coalesce(h.qt_operacoes_his_antes, 0) as qt_operacoes_his_antes,
    h.modalidades_his,
    coalesce(h.qt_concluidas, 0) as qt_his_concluidas,
    coalesce(h.qt_em_andamento_hoje, 0) as qt_his_em_andamento_hoje,
    coalesce(h.qt_paralisadas_hoje, 0) as qt_his_paralisadas_hoje,
    coalesce(h.qt_distratadas, 0) as qt_his_distratadas,
    coalesce(h.uh_contratadas_antes, 0) as uh_his_contratadas,
    coalesce(h.uh_entregues_antes, 0) as uh_his_entregues,
    case
        when coalesce(h.uh_contratadas_antes, 0) - coalesce(h.uh_distratadas_antes, 0) > 0
        then round(100.0 * h.uh_entregues_antes / (h.uh_contratadas_antes - h.uh_distratadas_antes), 1)
    end as percentual_his_entregue,
    h.dt_ultima_contratacao_his,
    coalesce(h.qt_em_andamento_hoje, 0) + coalesce(h.qt_paralisadas_hoje, 0) > 0 as ic_tinha_obra_aberta_ao_concorrer,
    coalesce(h.qt_paralisadas_hoje, 0) > 0 as ic_tem_obra_paralisada,
    date '{{ data_selecao }}' as dt_corte_selecao
from prop p
left join tram t on t.cod_ibge = p.cod_ibge
left join hist h on h.cod_ibge_6 = p.cod_ibge_6
left join reg r on r.cod_ibge = p.cod_ibge
