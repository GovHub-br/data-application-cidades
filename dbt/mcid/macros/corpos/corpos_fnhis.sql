{#
    Corpos SQL de modelos do produto fnhis que outros modelos da MESMA camada precisam reaproveitar.

    Regra das camadas (medallion estrito): bronze lê a fonte; prata lê só bronze; ouro lê só prata.
    Quando uma prata precisava do resultado de outra prata (ou uma ouro de outra ouro), em vez de
    ler a tabela ela chama o corpo da outra como subconsulta. O SQL é o mesmo, então o dado é o
    mesmo; muda só de onde ele é lido: a linhagem passa a ir da camada anterior direto ao modelo.

    Cada modelo listado aqui é uma casca fina (config + chamada do corpo) em models/.
#}

{% macro corpo_prata_fnhis_ficha_termo_compromisso() %}
-- Prata: Ficha do Termo de Compromisso (FNHIS Sub-50)
-- Uma linha por termo, juntando os três lados do ciclo: a SELEÇÃO (proposta, UH, IBGE), o
-- INSTRUMENTO no TransfereGov (repasse, empenho, situação) e a EXECUÇÃO acompanhada pela SNH
-- (situação da obra, UH entregues, desembolso). Mais a regularidade do município no SNHIS.
--
-- Termo × contrato SNH não têm chave comum. O vínculo é por município (IBGE de 6 dígitos) e
-- valor de repasse, desempatado pela data de assinatura; só entra vínculo 1:1 — se sobrar mais
-- de um candidato de qualquer lado, fica sem vínculo em vez de escolher no chute. Conferido
-- em 2026-09: 1.184 dos 1.224 contratos casam por município + valor.

with
    termo as (
        select * from ({{ corpo_prata_fnhis_termo_compromisso() }}) as prata_fnhis_termo_compromisso
    ),

    proposta as (
        select * from ({{ corpo_prata_fnhis_propostas() }}) as prata_fnhis_propostas
    ),

    ente as (
        select * from ({{ corpo_prata_fnhis_regularidade_entes() }}) as prata_fnhis_regularidade_entes
    ),

    snh as (
        select * from ({{ corpo_prata_fnhis_prioritarios_snh() }}) as prata_fnhis_prioritarios_snh
    ),

    base as (
        select
            t.*,
            p.cod_ibge,
            p.cod_ibge_6,
            p.quantidade_uh,
            p.proponente_esfera,
            p.situacao_proposta as situacao_selecao,
            p.ic_proposta_de_risco
        from termo t
        left join proposta p on p.numero_proposta = t.numero_proposta
    ),

    candidato as (
        select
            b.numero_proposta_transferegov,
            s.id_operacao_snh,
            b.dt_assinatura = s.dt_contratacao as ic_mesma_data
        from base b
        join snh s
            on s.cod_ibge_6 = b.cod_ibge_6
            and s.valor_contratado = b.valor_repasse
    ),

    -- quando algum candidato do termo tem a mesma data, só esses valem
    preferido as (
        select *
        from (
            select c.*, bool_or(ic_mesma_data) over (partition by numero_proposta_transferegov) as ic_termo_tem_mesma_data
            from candidato c
        ) x
        where ic_mesma_data or not ic_termo_tem_mesma_data
    ),

    vinculo as (
        select
            numero_proposta_transferegov,
            id_operacao_snh,
            case when ic_mesma_data then 'município + valor + data' else 'município + valor' end as criterio_vinculo_snh
        from (
            select
                p.*,
                count(*) over (partition by numero_proposta_transferegov) as n_por_termo,
                count(*) over (partition by id_operacao_snh) as n_por_contrato
            from preferido p
        ) x
        where n_por_termo = 1 and n_por_contrato = 1
    )

select
    -- Identificação
    b.numero_proposta_transferegov,
    b.numero_proposta,
    b.codigo_programa,
    'Novo MCMV FNHIS Sub-50' as programa,

    -- Localização
    b.uf,
    {{ regiao_da_uf("b.uf") }} as regiao,
    b.municipio,
    b.cod_ibge,
    concat(b.municipio, '/', b.uf) as municipio_uf,
    concat(b.numero_proposta_transferegov, ' - ', b.municipio, '/', b.uf) as termo_municipio,

    -- Proponente
    b.proponente_nome,
    b.proponente_esfera,
    b.objeto,

    -- Seleção
    b.situacao_selecao,
    b.ic_proposta_de_risco,
    b.quantidade_uh,
    floor(b.quantidade_uh * 3.3)::int as pessoas_atendidas,

    -- Instrumento (TransfereGov)
    b.situacao_contratacao,
    b.situacao_proposta,
    b.situacao_instrumento,
    b.ic_instrumento_ativo,
    b.dt_assinatura,
    b.valor_repasse,
    b.valor_contrapartida,
    b.valor_investimento,
    case
        when coalesce(b.quantidade_uh, 0) > 0 then round(b.valor_repasse / b.quantidade_uh, 2)
    end as valor_repasse_por_uh,
    b.valor_empenhado_acumulado,
    case
        when b.valor_repasse > 0 then round(b.valor_empenhado_acumulado / b.valor_repasse * 100, 2)
    end as percentual_empenhado,

    -- Regularidade do município no SNHIS
    e.ic_ente_regular,
    e.situacao_ente,
    e.situacao_plano_habitacional,
    e.situacao_lei_fundo,
    e.situacao_lei_conselho,

    -- Execução (SNH)
    v.id_operacao_snh is not null as ic_contrato_snh_identificado,
    v.criterio_vinculo_snh,
    s.id_operacao_snh,
    s.cod_operacao_agente,
    s.agente_financeiro,
    s.situacao as situacao_obra_snh,
    s.percentual_execucao_fisica,
    s.uh_entregues,
    s.valor_desembolsado,
    s.dt_previsao_termino,
    s.dt_termino,
    case
        when not b.ic_instrumento_ativo then 'Sem instrumento ativo'
        when s.id_operacao_snh is null then 'Sem acompanhamento SNH'
        when s.situacao ~* 'conclu|entregue' or s.percentual_execucao_fisica >= 100 then 'Concluído'
        when s.situacao ~* 'n.o iniciad' or coalesce(s.percentual_execucao_fisica, 0) = 0 then 'Não Iniciado'
        when s.situacao ~* 'paralisad' then 'Paralisado'
        else 'Em Andamento'
    end as status_execucao_simplificado,

    -- Linhagem
    b.dt_consulta,
    s.dt_referencia as dt_referencia_snh
from base b
left join vinculo v on v.numero_proposta_transferegov = b.numero_proposta_transferegov
left join snh s on s.id_operacao_snh = v.id_operacao_snh
left join ente e on e.cod_ibge = b.cod_ibge
{% endmacro %}

{% macro corpo_prata_fnhis_prioritarios_snh() %}
-- Prata: Dados Prioritários SNH (FNHIS)
-- Fonte: bronze_shpt_dados_prioritarios_snh_empreendimentos (a mesma bronze do Rural, que traz
-- todas as modalidades), filtrada para `modalidade = 'FNHIS'`. São os contratos do Sub-50 que a
-- SNH acompanha como empreendimento: 1.224 em 2026-09, todos CAIXA, Novo MCMV.
--
-- Diferenças para o Rural, e por isso este model não reaproveita o dele:
-- - o código do agente é a OPERAÇÃO da CAIXA (7 dígitos), não um APF: `normalize_apf` o
--   deformaria;
-- - o IBGE vem com 6 dígitos (sem verificador), e fica assim em `cod_ibge_6`;
-- - construtora, endereço e coordenadas vêm vazios — é obra pública ainda não iniciada.

select
    -- Identificação
    nullif(trim(identificador_da_operacao_na_snh), '') as id_operacao_snh,
    nullif(trim(codigo_da_operacao_no_agente_financeiro), '') as cod_operacao_agente,
    nullif(trim({{ target.schema }}.corrigir_mojibake(nome_do_agente_financeiro)), '') as agente_financeiro,
    nullif(trim({{ target.schema }}.corrigir_mojibake(nome_do_empreendimento)), '') as empreendimento_nome,
    nullif(trim({{ target.schema }}.corrigir_mojibake(modalidade)), '') as modalidade,
    case when trim(novo_mcmv_sim_nao) = 'Sim' then true else false end as ic_novo_mcmv,

    -- Localização
    upper(nullif(trim({{ target.schema }}.corrigir_mojibake(municipio)), '')) as municipio,
    nullif(trim(sigla_da_uf), '') as uf,
    nullif(trim({{ target.schema }}.corrigir_mojibake(nome_da_uf)), '') as estado_nome,
    nullif(nullif(trim({{ target.schema }}.corrigir_mojibake(nome_da_regiao)), ''), '#N/D') as regiao,
    nullif(trim(codigo_ibge_do_municipio), '') as cod_ibge_6,

    -- Situação
    nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_do_empreendimento)), '') as situacao,
    nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_da_empreendimento_agrupada)), '') as situacao_agrupada,
    case when trim(operacao_vigente_sim_nao) = 'Sim' then true else false end as ic_operacao_vigente,

    -- Execução física (%)
    {{ parse_numeric('percentual_da_obra', 'numeric(6, 2)') }} as percentual_execucao_fisica,

    -- UHs
    {{ parse_int('unidades_contratadas') }} as uh_contratadas,
    {{ parse_int('unidades_entregues') }} as uh_entregues,
    {{ parse_int('unidades_vigentes') }} as uh_vigentes,
    {{ parse_int('unidades_distratadas') }} as uh_distratadas,
    {{ parse_int('unidades_habitacionais_a_serem_entregues') }} as uh_a_entregar,

    -- Valores
    {{ parse_financial_value('valor_contratado_original') }} as valor_contratado_original,
    {{ parse_financial_value('valor_do_aporte_adicional') }} as valor_aporte_adicional,
    {{ parse_financial_value('valor_contratado_total') }} as valor_contratado,
    {{ parse_financial_value('valor_desembolsado_total') }} as valor_desembolsado,

    -- Datas
    {{ target.schema }}.parse_date_br(nullif(trim(data_da_contratacao), '')) as dt_contratacao,
    {{ target.schema }}.parse_date_br(nullif(trim(data_de_previsao_de_termino), '')) as dt_previsao_termino,
    {{ target.schema }}.parse_date_br(nullif(trim(data_do_termino), '')) as dt_termino,
    {{ target.schema }}.parse_date_br(nullif(trim(data_de_referencia), '')) as dt_referencia,

    -- Linhagem
    _source_file as arquivo_de_origem,
    nullif(trim(_ingested_at), '')::timestamp as criado_em

from {{ ref("bronze_shpt_dados_prioritarios_snh_empreendimentos") }}
where trim(upper(modalidade)) = 'FNHIS'
{% endmacro %}

{% macro corpo_prata_fnhis_proposta_tramite() %}
-- Prata: Em que etapa está cada proposta do FNHIS Sub-50 — da apresentação à obra — e como o
-- termo de compromisso mudou entre os retratos mensais do TransfereGov.
-- Grão: proposta apresentada (7.121).
--
-- Etapas, em ordem: 1 fora no enquadramento → 2 fora na seleção → 3 selecionada sem termo →
-- 4 termo anulado/rescindido → 5 termo em execução sem obra na SNH → 6 em obra (SNH) →
-- 7 prestação de contas. O trâmite INTERNO do MCID (SEI + planilha) não chega ao banco; quando
-- chegar, entra entre as etapas 3 e 5.
-- Execução: na SNH as operações FNHIS estão todas com 0% e R$ 0 desembolsado (set/2026); o
-- empenho do TransfereGov é hoje o melhor sinal de andamento.

with
    prop as (select * from ({{ corpo_prata_fnhis_propostas() }}) as prata_fnhis_propostas),
    termo as (select * from ({{ corpo_prata_fnhis_termo_compromisso() }}) as prata_fnhis_termo_compromisso),
    ficha as (select * from ({{ corpo_prata_fnhis_ficha_termo_compromisso() }}) as prata_fnhis_ficha_termo_compromisso),

    traj as (
        select
            numero_proposta_transferegov,
            count(*) as qt_retratos,
            min(dt_retrato) as dt_primeiro_retrato,
            max(dt_retrato) as dt_ultimo_retrato,
            count(distinct situacao_instrumento) as qt_situacoes_instrumento,
            (array_agg(situacao_instrumento order by dt_retrato) filter (where situacao_instrumento is not null))[1] as situacao_instrumento_inicial,
            max(dt_retrato) filter (where situacao_instrumento_retrato_anterior is not null
                                     and situacao_instrumento is distinct from situacao_instrumento_retrato_anterior) as dt_ultima_mudanca_situacao,
            (array_agg(situacao_instrumento_retrato_anterior order by dt_retrato desc)
                filter (where situacao_instrumento_retrato_anterior is not null
                        and situacao_instrumento is distinct from situacao_instrumento_retrato_anterior))[1] as situacao_instrumento_anterior,
            (array_agg(valor_empenhado_acumulado order by dt_retrato))[1] as valor_empenhado_primeiro_retrato
        from ({{ corpo_prata_fnhis_termo_compromisso_mensal() }}) as prata_fnhis_termo_compromisso_mensal
        group by 1
    ),

    base as (
        select
            p.numero_proposta,
            p.uf,
            p.municipio,
            p.cod_ibge,
            p.proponente_esfera,
            p.quantidade_uh,
            p.resultado_selecao,
            p.ic_selecionada,
            p.ic_proposta_de_risco,
            t.numero_proposta_transferegov,
            t.situacao_contratacao,
            t.situacao_instrumento,
            t.ic_instrumento_ativo,
            t.dt_assinatura,
            t.valor_repasse,
            t.valor_empenhado_acumulado,
            f.ic_contrato_snh_identificado,
            f.id_operacao_snh,
            f.situacao_obra_snh,
            f.percentual_execucao_fisica,
            f.uh_entregues,
            f.valor_desembolsado,
            f.ic_ente_regular,
            tr.qt_retratos,
            tr.dt_primeiro_retrato,
            tr.situacao_instrumento_inicial,
            tr.situacao_instrumento_anterior,
            tr.dt_ultima_mudanca_situacao,
            tr.qt_situacoes_instrumento,
            t.valor_empenhado_acumulado - tr.valor_empenhado_primeiro_retrato as variacao_empenho_na_serie
        from prop p
        left join termo t on t.numero_proposta = p.numero_proposta
        left join ficha f on f.numero_proposta_transferegov = t.numero_proposta_transferegov
        left join traj tr on tr.numero_proposta_transferegov = t.numero_proposta_transferegov
    )

select
    b.numero_proposta,
    b.uf,
    b.municipio,
    b.cod_ibge,
    b.proponente_esfera,
    b.quantidade_uh,
    b.resultado_selecao,
    b.ic_proposta_de_risco,
    b.numero_proposta_transferegov,
    b.situacao_contratacao,
    b.situacao_instrumento,
    b.ic_instrumento_ativo,
    b.dt_assinatura,
    b.valor_repasse,
    b.valor_empenhado_acumulado,
    b.ic_contrato_snh_identificado,
    b.id_operacao_snh,
    b.situacao_obra_snh,
    b.percentual_execucao_fisica,
    b.uh_entregues,
    b.valor_desembolsado,
    b.ic_ente_regular,
    b.qt_retratos,
    b.dt_primeiro_retrato,
    b.situacao_instrumento_inicial,
    b.situacao_instrumento_anterior,
    b.dt_ultima_mudanca_situacao,
    b.qt_situacoes_instrumento,
    b.variacao_empenho_na_serie,
    case
        when b.resultado_selecao = 'Não enquadrada' then 1
        when not coalesce(b.ic_selecionada, false) then 2
        when b.numero_proposta_transferegov is null or b.situacao_instrumento is null then 3
        when b.situacao_instrumento in ('Convênio Anulado', 'Convênio Rescindido') then 4
        when b.situacao_instrumento ~* 'presta..o de contas' then 7
        when coalesce(b.percentual_execucao_fisica, 0) > 0 or coalesce(b.valor_desembolsado, 0) > 0 then 6
        else 5
    end as etapa_ordem,
    case
        when b.resultado_selecao = 'Não enquadrada' then 'Fora no enquadramento'
        when not coalesce(b.ic_selecionada, false) then concat('Fora na seleção - ', b.resultado_selecao)
        when b.numero_proposta_transferegov is null or b.situacao_instrumento is null then 'Selecionada - sem termo formalizado'
        when b.situacao_instrumento in ('Convênio Anulado', 'Convênio Rescindido') then 'Termo anulado/rescindido'
        when b.situacao_instrumento ~* 'presta..o de contas' then 'Prestação de contas'
        when coalesce(b.percentual_execucao_fisica, 0) > 0 or coalesce(b.valor_desembolsado, 0) > 0 then 'Em obra (SNH)'
        else 'Termo em execução - sem obra registrada'
    end as etapa_atual
from base b
{% endmacro %}

{% macro corpo_prata_fnhis_propostas() %}
-- Prata: Propostas apresentadas ao Novo MCMV FNHIS Sub-50, com o resultado da seleção
-- Fonte: bronze_shpt_fnhis_sub50_propostas (planilha de todas as propostas, portaria MCID
-- nº 673/2024). Grão: uma proposta (`numero_proposta`, único na origem).
--
-- O município vem como "Nome/UF" num campo só; o IBGE vem com 7 dígitos. `cod_ibge_6` existe
-- porque o SNH grava o código SEM o dígito verificador — é a chave para cruzar com o contrato.
--
-- `resultado_selecao` agrupa as nove situações da origem em cinco. As propostas "de risco"
-- selecionadas contam como selecionadas; "Município já contemplado (RISCO)" não.

with
    proposta as (
        select
            nullif(trim(numero_da_proposta::text), '') as numero_proposta,
            nullif(trim(cod_ibge_munic_beneficiado::text), '') as cod_ibge,
            nullif(trim({{ target.schema }}.corrigir_mojibake(municipio::text)), '') as municipio_uf_origem,
            nullif(trim({{ target.schema }}.corrigir_mojibake(proponente::text)), '') as proponente,
            {{ parse_int("total_de_uh::text") }} as quantidade_uh,
            nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_da_proposta::text)), '') as situacao_proposta,
            nullif(trim({{ target.schema }}.corrigir_mojibake(justificativa_nao_enquadramento::text)), '') as justificativa_nao_enquadramento,
            nullif(trim(_source_file::text), '') as arquivo_de_origem,
            nullif(trim(_ingested_at::text), '')::timestamp as criado_em
        from {{ ref("bronze_shpt_fnhis_sub50_propostas") }}
    )

select
    numero_proposta,
    cod_ibge,
    left(cod_ibge, 6) as cod_ibge_6,
    substring(municipio_uf_origem from '^(.*)/[A-Z]{2}$') as municipio,
    substring(municipio_uf_origem from '/([A-Z]{2})$') as uf,
    case
        when proponente ~* 'municipal' then 'Município'
        when proponente ~* 'estadual|distrito federal' then 'Estado/DF'
        else proponente
    end as proponente_esfera,
    quantidade_uh,
    situacao_proposta,
    case
        when situacao_proposta ~* 'selecionada' then 'Selecionada'
        when situacao_proposta ~* 'j. contemplado' then 'Município já contemplado'
        when situacao_proposta ~* 'n.o enquadrada' then 'Não enquadrada'
        when situacao_proposta ~* 'cota insuficiente' then 'Cota insuficiente da UF'
        else 'Outra'
    end as resultado_selecao,
    situacao_proposta ~* 'selecionada' as ic_selecionada,
    situacao_proposta ~* 'risco' as ic_proposta_de_risco,
    justificativa_nao_enquadramento,
    arquivo_de_origem,
    criado_em
from proposta
{% endmacro %}

{% macro corpo_prata_fnhis_regularidade_entes() %}
-- Prata: Regularidade dos entes no SNHIS
-- Fonte: bronze_shpt_snhis_regularidade_entes (extração semanal da CAIXA; a bronze guarda a
-- mais recente). Grão: um ente — município, estado ou DF —, identificado pelo IBGE de 7 dígitos
-- (os estados usam o código da UF seguido de zeros).
--
-- Estar regular no SNHIS é condição para receber recurso do FNHIS: é a checagem de "ente apto"
-- das Validações/Regras. São cinco exigências — lei do fundo local (FLHIS), lei do conselho
-- gestor, termo de adesão, plano habitacional e relatório de gestão —, e a situação do ente é a
-- síntese que a CAIXA calcula a partir delas.

with
    ente as (
        select
            nullif(trim(coibge::text), '') as cod_ibge,
            nullif(trim({{ target.schema }}.corrigir_mojibake(municipio::text)), '') as ente_nome,
            nullif(trim(uf::text), '') as uf,
            nullif(regexp_replace(cnpj::text, '[^0-9]', '', 'g'), '') as ente_cnpj,
            {{ parse_int("populacao::text") }} as populacao,
            upper(trim({{ target.schema }}.corrigir_mojibake(reg_metrop::text))) = 'SIM' as ic_regiao_metropolitana,
            nullif(trim(situacao_lei_flhis::text), '') as situacao_lei_fundo,
            {{ target.schema }}.parse_date_br(nullif(trim(dt_lei_flhis_analise_cefus::text), '')) as dt_lei_fundo,
            nullif(trim(situacao_lei_cgflhis::text), '') as situacao_lei_conselho,
            {{ target.schema }}.parse_date_br(nullif(trim(dt_lei_cgflhis_analise_cefus::text), '')) as dt_lei_conselho,
            nullif(trim(situacao_termo_adesao::text), '') as situacao_termo_adesao,
            {{ target.schema }}.parse_date_br(nullif(trim(dt_termo_adesao_analise_cefus::text), '')) as dt_termo_adesao,
            nullif(trim(situacao_plano_habit::text), '') as situacao_plano_habitacional,
            {{ target.schema }}.parse_date_br(nullif(trim(dt_plano_habit_analise_cefus::text), '')) as dt_plano_habitacional,
            {{ parse_int("ano_rel_gestao::text") }} as ano_relatorio_gestao,
            nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_rel_gestao::text)), '') as situacao_relatorio_gestao,
            nullif(trim(situacao_municipio::text), '') as situacao_ente,
            {{ target.schema }}.parse_date_br(nullif(trim(dt_referencia::text), '')) as dt_referencia,
            nullif(trim(_source_file::text), '') as arquivo_de_origem,
            nullif(trim(_ingested_at::text), '')::timestamp as criado_em
        from {{ ref("bronze_shpt_snhis_regularidade_entes") }}
    )

select
    cod_ibge,
    left(cod_ibge, 6) as cod_ibge_6,
    case
        when cod_ibge ~ '^\d{2}0{5}$' or ente_nome ~* '^GOVERNO DO' then 'Estado/DF'
        else 'Município'
    end as ente_esfera,
    ente_nome,
    uf,
    ente_cnpj,
    populacao,
    ic_regiao_metropolitana,
    situacao_lei_fundo,
    dt_lei_fundo,
    situacao_lei_conselho,
    dt_lei_conselho,
    situacao_termo_adesao,
    dt_termo_adesao,
    situacao_plano_habitacional,
    dt_plano_habitacional,
    ano_relatorio_gestao,
    situacao_relatorio_gestao,
    situacao_ente,
    situacao_ente = 'Regular' as ic_ente_regular,
    dt_referencia,
    arquivo_de_origem,
    criado_em
from ente
{% endmacro %}

{% macro corpo_prata_fnhis_termo_compromisso() %}
-- Prata: Termos de compromisso do Novo MCMV FNHIS Sub-50 (TransfereGov)
-- Fonte: bronze_shpt_fnhis_sub50_termos_compromisso (painel mensal; a bronze guarda o retrato
-- mais recente). Grão: um termo, identificado pelo número da proposta no TransfereGov.
--
-- `no_reservado_pac` junta os dois números da proposta ("56000000016/2024 - 32847/2024"): o
-- primeiro é o da SELEÇÃO, e é por ele que o termo se liga à proposta em
-- prata_fnhis_propostas — casou 1.207 de 1.207 em 2026-09. O painel não traz IBGE nem UH;
-- vêm de lá.
--
-- Valores no formato brasileiro ("3.250.000,00"); `-` na origem é ausência, não situação.

with
    termo as (
        select
            nullif(trim(no_proposta::text), '') as numero_proposta_transferegov,
            nullif(split_part(trim(no_reservado_pac::text), ' - ', 1), '') as numero_proposta,
            nullif(trim(codigo_programa::text), '') as codigo_programa,
            nullif(nullif(trim({{ target.schema }}.corrigir_mojibake(sit_contratacao::text)), ''), '-') as situacao_contratacao,
            nullif(nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_proposta::text)), ''), '-') as situacao_proposta,
            nullif(nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_instrumento::text)), ''), '-') as situacao_instrumento,
            nullif(trim({{ target.schema }}.corrigir_mojibake(modalidade::text)), '') as modalidade_instrumento,
            {{ parse_financial_value("vl_repasse_proposta::text") }} as valor_repasse_proposta,
            {{ parse_financial_value("valor_de_repasse::text") }} as valor_repasse,
            {{ parse_financial_value("valor_de_contrapartida::text") }} as valor_contrapartida,
            {{ parse_financial_value("vl_empenhado_pre_convenio::text") }} as valor_empenhado_pre_convenio,
            {{ parse_financial_value("valor_empenhado_acumulado::text") }} as valor_empenhado_acumulado,
            nullif(trim({{ target.schema }}.corrigir_mojibake(objeto::text)), '') as objeto,
            nullif(trim(uf::text), '') as uf,
            upper(nullif(trim({{ target.schema }}.corrigir_mojibake(municipio::text)), '')) as municipio,
            initcap(nullif(trim(regiao::text), '')) as regiao,
            nullif(trim({{ target.schema }}.corrigir_mojibake(nome_proponente::text)), '') as proponente_nome,
            nullif(regexp_replace(cnpj::text, '[^0-9]', '', 'g'), '') as proponente_cnpj,
            {{ target.schema }}.parse_date_br(nullif(trim(data_assinatura::text), '')) as dt_assinatura,
            {{ target.schema }}.parse_date_br(left(nullif(trim(data_consulta::text), ''), 10)) as dt_consulta,
            cast(filename as varchar) as arquivo_de_origem,
            nullif(trim(_ingested_at::text), '')::timestamp as criado_em
        from {{ ref("bronze_shpt_fnhis_sub50_termos_compromisso") }}
    )

select
    *,
    -- Anulado ou rescindido é termo que deixou de existir; sem situação (`-` na origem) é
    -- proposta selecionada que não chegou a virar instrumento.
    situacao_instrumento is not null
    and situacao_instrumento not in ('Convênio Anulado', 'Convênio Rescindido') as ic_instrumento_ativo,
    valor_repasse + valor_contrapartida as valor_investimento
from termo
{% endmacro %}

{% macro corpo_prata_fnhis_termo_compromisso_mensal() %}
-- Prata: Termos de compromisso do FNHIS Sub-50 em CADA retrato mensal do painel do TransfereGov.
-- Fonte: bronze_shpt_fnhis_sub50_termos_compromisso_serie. Grão: termo × data do retrato.
--
-- Serve para ver a trajetória do instrumento (em execução → anulado, prestação de contas...) e
-- a evolução do empenho. O retrato mais antigo (mar/2025) não tinha `situacao_instrumento`.
-- Valores no formato brasileiro; `-` na origem é ausência.

with
    bruto as (
        select
            nullif(trim(no_proposta), '') as numero_proposta_transferegov,
            nullif(split_part(trim(no_reservado_pac), ' - ', 1), '') as numero_proposta,
            to_date(substring(filename from 'Painel_TG_(\d{8})'), 'YYYYMMDD') as dt_retrato,
            nullif(nullif(trim({{ target.schema }}.corrigir_mojibake(sit_contratacao)), ''), '-') as situacao_contratacao,
            nullif(nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_proposta)), ''), '-') as situacao_proposta,
            nullif(nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_instrumento)), ''), '-') as situacao_instrumento,
            {{ parse_financial_value("valor_de_repasse") }} as valor_repasse,
            {{ parse_financial_value("valor_de_contrapartida") }} as valor_contrapartida,
            {{ parse_financial_value("valor_empenhado_acumulado") }} as valor_empenhado_acumulado,
            nullif(trim(uf), '') as uf,
            regexp_replace(filename, '^.*/', '') as arquivo_de_origem
        from {{ ref("bronze_shpt_fnhis_sub50_termos_compromisso_serie") }}
    ),

    unico as (
        select distinct on (numero_proposta_transferegov, dt_retrato) *
        from bruto
        where numero_proposta_transferegov is not null and dt_retrato is not null
        order by numero_proposta_transferegov, dt_retrato, arquivo_de_origem desc
    )

select
    numero_proposta_transferegov,
    numero_proposta,
    dt_retrato,
    situacao_contratacao,
    situacao_proposta,
    situacao_instrumento,
    valor_repasse,
    valor_contrapartida,
    valor_empenhado_acumulado,
    uf,
    arquivo_de_origem,
    lag(situacao_instrumento) over (
        partition by numero_proposta_transferegov order by dt_retrato
    ) as situacao_instrumento_retrato_anterior
from unico
{% endmacro %}
