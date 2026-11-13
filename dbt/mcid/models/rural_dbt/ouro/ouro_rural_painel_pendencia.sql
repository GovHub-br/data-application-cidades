{{ config(materialized="table") }}

-- Gold: Pendências de vínculo e de qualidade do Rural, uma linha por registro × regra (R01 a R12).
-- Base da área "Qualidade e integridade" da aba Rural. Uma operação pode aparecer em mais de
-- uma regra; por isso esta tabela conta OCORRÊNCIAS, não operações — a contagem de operações
-- com alerta está na ouro_rural_painel_operacao (qt_alertas).
--
-- Classificação de cada regra (não se marca nada como inválido sem regra que sustente):
--   Inconsistência comprovada — o dado contradiz uma regra objetiva (ex.: desembolso maior que
--                                o contratado; CNPJ "0");
--   Associação ausente        — o vínculo esperado não existe nas bases (ex.: selecionado sem
--                                contrato identificável);
--   Informação ausente        — o campo existe mas não foi preenchido pela origem;
--   Requer investigação       — sinal de problema que depende de confirmação (ex.: EO vinda do
--                                arquivo AF CAIXA; EO diferente da selecionada).

with
    op as (select * from {{ ref("prata_rural_vinculo_operacao") }}),
    sel as (select * from {{ ref("prata_rural_selecao_portaria_162") }}),
    funil as (select * from {{ ref("prata_rural_proposta_funil") }}),
    ts as (
        select apf, 'CAIXA' as agente_ts, portaria_ts, municipio, uf, dt_contratacao
        from {{ ref("prata_rural_trabalho_social_caixa") }}
        union all
        select apf, 'BB', portaria_ts, municipio, uf, dt_contratacao
        from {{ ref("prata_rural_trabalho_social_bb") }}
    ),
    eo_nomes as (
        select eo_cnpj, count(distinct eo_nome) as qt_nomes,
               string_agg(distinct eo_nome, ' | ') as nomes, count(*) as qt_operacoes
        from {{ ref("prata_rural_operacao_eo") }}
        where eo_cnpj is not null and eo_nome is not null
        group by eo_cnpj
        having count(distinct eo_nome) > 1
    ),

    pendencias as (
        -- R01 operação sem EO
        select 'R01' as regra_codigo, 'Operação sem entidade organizadora informada' as regra,
               'Associação ausente' as classificacao, 'Contrato/empreendimento (APF)' as entidade,
               apf as chave, apf, eo_cnpj, eo_nome, agente_financeiro, programa, uf, municipio,
               portaria_ts, portaria_selecao, situacao_vinculo_eo, status_execucao, ano_contratacao,
               'Nem o cadastro de EO nem o arquivo AF CAIXA trazem EO para a APF.' as detalhe
        from op where ic_sem_eo

        union all
        -- R02 CNPJ inválido
        select 'R02', 'CNPJ da EO inválido na origem', 'Inconsistência comprovada',
               'Contrato/empreendimento (APF)', apf, apf, eo_cnpj, eo_nome, agente_financeiro, programa,
               uf, municipio, portaria_ts, portaria_selecao, situacao_vinculo_eo, status_execucao,
               ano_contratacao, 'CNPJ recebido: "' || coalesce(eo_cnpj_origem, '') || '" (só zeros).'
        from op where ic_cnpj_eo_invalido

        union all
        -- R03 EO a confirmar
        select 'R03', 'EO identificada só pelo arquivo AF CAIXA', 'Requer investigação',
               'Contrato/empreendimento (APF)', apf, apf, eo_cnpj, eo_nome, agente_financeiro, programa,
               uf, municipio, portaria_ts, portaria_selecao, situacao_vinculo_eo, status_execucao,
               ano_contratacao,
               'O campo de origem é "CNPJ construtora/entidade"; não confirmado com a CAIXA que é a EO.'
        from op where ic_eo_a_confirmar

        union all
        -- R04 mesmo CNPJ, nomes diferentes
        select 'R04', 'Mesmo CNPJ de EO com nomes diferentes', 'Requer investigação',
               'Entidade organizadora', eo_cnpj, null, eo_cnpj, nomes, null, null, null, null,
               null, null, null, null, null,
               qt_nomes || ' grafias em ' || qt_operacoes || ' operações: ' || left(nomes, 300)
        from eo_nomes

        union all
        -- R05 desembolso acima do contratado
        select 'R05', 'Desembolso acima do contratado + aporte', 'Inconsistência comprovada',
               'Contrato/empreendimento (APF)', apf, apf, eo_cnpj, eo_nome, agente_financeiro, programa,
               uf, municipio, portaria_ts, portaria_selecao, situacao_vinculo_eo, status_execucao,
               ano_contratacao,
               'Desembolsado R$ ' || to_char(valor_desembolsado, 'FM999G999G999G990D00')
               || ' > contratado + aporte R$ '
               || to_char(valor_contratado + coalesce(valor_aporte_adicional, 0), 'FM999G999G999G990D00') || '.'
        from op where ic_desembolso_acima_contratado

        union all
        -- R06 selecionado sem contrato identificável
        select 'R06', 'Selecionado na Portaria 162/2018 sem contrato identificável', 'Associação ausente',
               'Seleção Portaria 162/2018', s.codigo_empreendimento, s.apf, s.entidade_organizadora_cnpj,
               null, null, null, s.uf, s.municipio, null, '162/2018', null, null, null,
               'Código do empreendimento na portaria não encontrado entre as APFs do Rural. Pode não ter '
               || 'sido contratado ou ter sido contratado com outro código.'
        from sel s
        where not exists (select 1 from op where op.apf = s.apf)

        union all
        -- R07 EO diferente da selecionada
        select 'R07', 'Contrato com EO diferente da selecionada na portaria de seleção', 'Requer investigação',
               'Contrato/empreendimento (APF)', apf, apf, eo_cnpj, eo_nome, agente_financeiro, programa,
               uf, municipio, portaria_ts, portaria_selecao, situacao_vinculo_eo, status_execucao,
               ano_contratacao,
               'Seleção ' || coalesce(portaria_selecao, '?') || ': EO selecionada ' || coalesce(eo_cnpj_selecionada, '?') || '; EO do contrato ' || eo_cnpj || '.'
        from op where ic_eo_diferente_da_selecao

        union all
        -- R08 TS sem contrato na visão consolidada
        select 'R08', 'Registro de Trabalho Social sem contrato na visão consolidada', 'Requer investigação',
               'Trabalho Social', t.apf, t.apf, null, null, t.agente_ts, null, t.uf, t.municipio,
               t.portaria_ts, null, null, null, extract(year from t.dt_contratacao)::int,
               'A APF está na planilha de Trabalho Social do agente mas não na visão consolidada do Rural.'
        from ts t
        where not exists (select 1 from op where op.apf = t.apf)

        union all
        -- R09 operação sem TS
        select 'R09', 'Contrato sem registro de Trabalho Social', 'Informação ausente',
               'Contrato/empreendimento (APF)', apf, apf, eo_cnpj, eo_nome, agente_financeiro, programa,
               uf, municipio, portaria_ts, portaria_selecao, situacao_vinculo_eo, status_execucao,
               ano_contratacao, 'A APF não aparece em nenhuma das planilhas de Trabalho Social (CAIXA ou BB).'
        from op where ic_sem_trabalho_social

        union all
        -- R10 portaria de TS não informada
        select 'R10', 'Portaria de Trabalho Social não informada pelo agente', 'Informação ausente',
               'Contrato/empreendimento (APF)', apf, apf, eo_cnpj, eo_nome, agente_financeiro, programa,
               uf, municipio, portaria_ts, portaria_selecao, situacao_vinculo_eo, status_execucao,
               ano_contratacao, 'A planilha de Trabalho Social tem a APF, mas o campo de portaria está vazio.'
        from op where ic_portaria_ts_nao_informada

        union all
        -- R11 autorizada a contratar e sem contrato identificado
        select 'R11', 'Proposta autorizada a contratar sem contrato identificado', 'Associação ausente',
               'Proposta autorizada', f.protocolo_proposta, null, f.eo_cnpj_selecionada, f.eo_nome_selecionada,
               null, null, f.uf, f.municipio, null, replace(f.portaria_codigo, '_', '/'), null, null, null,
               'Autorizada pela Portaria ' || f.portaria_autorizacao || ' em '
               || to_char(f.dt_autorizacao, 'DD/MM/YYYY')
               || '; nenhum contrato da mesma EO no município a partir da seleção. Pode estar no prazo de 180 dias '
               || 'ou ter sido contratada com outra EO/município.'
        from funil f
        where f.ic_autorizada and not f.ic_contratado

        union all
        -- R12 contrato assinado antes da autorização
        select 'R12', 'Contrato vinculado assinado antes da portaria de autorização', 'Requer investigação',
               'Contrato/empreendimento (APF)', f.apf_contrato, f.apf_contrato, f.eo_cnpj_contrato,
               f.eo_nome_contrato, f.agente_financeiro, null, f.uf, f.municipio, null,
               replace(f.portaria_codigo, '_', '/'), null, f.status_operacao,
               extract(year from f.dt_contratacao)::int,
               'Contrato em ' || to_char(f.dt_contratacao, 'DD/MM/YYYY') || ', autorização '
               || f.portaria_autorizacao || ' em ' || to_char(f.dt_autorizacao, 'DD/MM/YYYY')
               || '. O vínculo por CNPJ + município (confiança ' || coalesce(lower(f.confianca_vinculo), '?')
               || ') pode ter pegado outro contrato da mesma EO.'
        from funil f
        where f.ic_contrato_antes_da_autorizacao
    )

select * from pendencias
