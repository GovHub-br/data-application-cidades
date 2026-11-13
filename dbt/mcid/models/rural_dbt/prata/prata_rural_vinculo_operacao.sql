{{ config(materialized="table") }}

-- Prata: Vínculos de cada operação do Rural — portaria, contrato/empreendimento e entidade
-- organizadora (EO) — com a execução física e financeira e as marcas de qualidade.
-- Base da aba Rural do painel do MCID. Grão: APF (uma linha por operação).
--
-- Contrato e empreendimento têm a MESMA chave no Rural: a APF identifica o contrato do
-- empreendimento com o agente financeiro (CAIXA ou BB). Não existe, nas bases, um código de
-- empreendimento separado do contrato; por isso aqui "contrato" e "empreendimento" contam igual.
--
-- Portarias são de três tipos, que NÃO se somam (cada APF tem no máximo uma de cada):
--   - seleção: a portaria que selecionou a proposta que virou este contrato — 162/2018 (PNHR, pela APF)
--     ou 354/2024 / 697/2026 (Novo Rural, por CNPJ da EO + município). Vem do funil
--     (corpo_prata_rural_proposta_funil); se duas propostas caírem na mesma APF, fica a de vínculo mais
--     firme (Exata > Alta > Média > Baixa);
--   - autorização de contratação: a portaria que autorizou contratar a proposta selecionada, ligada pelo
--     número da proposta (hoje 1.133/2024, 1.346/2024, 1.386/2024 e 278/2025);
--   - Trabalho Social: a portaria adotada no trabalho social da operação (21/2014, 464/2018,
--     75/2025), informada pelo agente financeiro (0 ou 1 por APF).
--
-- Junções 1:1 garantidas: a ficha tem uma linha por APF; operação/EO idem; seleção e Trabalho
-- Social são deduplicados por APF antes do join (hoje não há repetição, mas o join não pode
-- multiplicar valor financeiro se um dia houver).
--
-- Fórmulas (mesmas da ficha do empreendimento):
--   percentual_execucao_financeira = valor_desembolsado / valor_contratado × 100 (nulo se
--     valor_contratado for nulo ou zero);
--   percentual_execucao_fisica = medição mais recente informada pelo agente (0 a 100);
--   diferenca_financeiro_fisico_pp = financeiro − físico, em pontos percentuais (nulo se faltar
--     um dos dois).

with
    ficha as (
        select * from ({{ corpo_prata_rural_ficha_empreendimento() }}) as prata_rural_ficha_empreendimento
    ),

    eo as (
        select apf, regiao, eo_cnpj, eo_cnpj_origem, eo_nome, fonte_eo, status_operacao
        from ({{ corpo_prata_rural_operacao_eo() }}) as prata_rural_operacao_eo
    ),

    selecao as (
        select distinct on (apf_contrato)
            apf_contrato as apf,
            portaria_codigo,
            ato_selecao,
            ciclo_selecao,
            dt_portaria,
            eo_cnpj_selecionada,
            quantidade_uh_selecionadas,
            confianca_vinculo,
            metodo_vinculo,
            portaria_autorizacao,
            ato_autorizacao,
            dt_autorizacao,
            ic_autorizada
        from ({{ corpo_prata_rural_proposta_funil() }}) as prata_rural_proposta_funil
        where ic_contratado and apf_contrato is not null
        order by
            apf_contrato,
            case confianca_vinculo when 'Exata' then 1 when 'Alta' then 2 when 'Média' then 3 else 4 end,
            ic_autorizada desc,
            dt_portaria desc nulls last,
            id_selecao
    ),

    ts as (
        select distinct on (apf) apf, agente_ts, portaria_ts, situacao_ts
        from (
            select apf, 'CAIXA' as agente_ts, portaria_ts, situacao_ts
            from ({{ corpo_prata_rural_trabalho_social_caixa() }}) as prata_rural_trabalho_social_caixa
            union all
            select apf, 'BB' as agente_ts, portaria_ts, situacao_ts
            from ({{ corpo_prata_rural_trabalho_social_bb() }}) as prata_rural_trabalho_social_bb
        ) u
        where apf is not null
        order by apf, (portaria_ts is null), agente_ts
    ),

    base as (
        select
            f.apf,
            f.agente_financeiro,
            f.programa,
            f.nome_empreendimento,
            f.municipio,
            f.uf,
            e.regiao,
            f.dt_contratacao,
            extract(year from f.dt_contratacao)::int as ano_contratacao,
            f.situacao_empreendimento,
            f.status_execucao_simplificado as status_execucao,
            e.status_operacao,
            f.quantidade_uh,
            f.quantidade_uh_entregues,
            f.valor_contratado,
            f.valor_aporte_adicional,
            f.valor_desembolsado,
            f.percentual_execucao_fisica,
            f.percentual_execucao_financeira,
            case
                when f.percentual_execucao_financeira is not null and f.percentual_execucao_fisica is not null
                then round(f.percentual_execucao_financeira - f.percentual_execucao_fisica, 2)
            end as diferenca_financeiro_fisico_pp,
            f.ritmo_fisico_financeiro,
            f.consistencia_desembolso,
            f.dt_referencia_execucao_fisica,
            f.dt_referencia_valor_desembolsado,

            -- entidade organizadora
            e.eo_cnpj,
            e.eo_cnpj_origem,
            e.eo_nome,
            e.fonte_eo,
            case
                when e.fonte_eo like 'cadastro EO%' then 'EO do cadastro de EO'
                when e.fonte_eo like 'AF CAIXA%' then 'EO a confirmar (arquivo AF CAIXA)'
                when e.fonte_eo = 'CNPJ inválido na origem' then 'CNPJ da EO inválido na origem'
                else 'Sem EO informada'
            end as situacao_vinculo_eo,

            -- portaria de seleção
            replace(s.portaria_codigo, '_', '/') as portaria_selecao,
            s.ato_selecao,
            s.ciclo_selecao,
            s.dt_portaria as dt_portaria_selecao,
            s.confianca_vinculo as confianca_vinculo_selecao,
            s.metodo_vinculo as metodo_vinculo_selecao,
            coalesce(s.portaria_codigo = '162_2018', false) as ic_selecionado_portaria_162,
            s.eo_cnpj_selecionada,
            case when s.apf is not null and e.eo_cnpj is not null then s.eo_cnpj_selecionada = e.eo_cnpj end
                as ic_mesma_eo_selecao,
            case
                when s.apf is null then 'Sem portaria de seleção identificada'
                else 'Selecionada (vínculo ' || lower(s.confianca_vinculo) || ')'
            end as situacao_portaria_selecao,

            -- portaria de autorização de contratação
            s.portaria_autorizacao,
            s.ato_autorizacao,
            s.dt_autorizacao,
            case
                when s.ic_autorizada then 'Autorização carregada'
                when s.portaria_codigo = '354_2024' then 'Seleção 2023 sem autorização carregada'
                when s.portaria_codigo = '697_2026' then 'Seleção 2025 sem autorização carregada'
                when s.ciclo_selecao like 'Calamidade%' then 'Calamidade (sem portaria de autorização)'
                when s.apf is not null then 'Ciclo sem autorização carregada'
                else 'Sem portaria de seleção identificada'
            end as situacao_portaria_autorizacao,

            -- Trabalho Social
            t.agente_ts,
            t.portaria_ts,
            t.situacao_ts,
            case
                when t.apf is null then 'Sem registro de Trabalho Social'
                when t.portaria_ts is null then 'Portaria não informada pelo agente'
                else 'Portaria informada'
            end as situacao_portaria_ts
        from ficha f
        left join eo e on e.apf = f.apf
        left join selecao s on s.apf = f.apf
        left join ts t on t.apf = f.apf
    )

select
    b.*,
    -- marcas de qualidade (uma por regra; a descrição de cada uma está no schema.yml e na
    -- ouro_rural_painel_pendencia)
    (b.eo_cnpj is null and b.eo_cnpj_origem is null) as ic_sem_eo,
    (b.eo_cnpj is null and b.eo_cnpj_origem is not null) as ic_cnpj_eo_invalido,
    (b.situacao_vinculo_eo = 'EO a confirmar (arquivo AF CAIXA)') as ic_eo_a_confirmar,
    (b.consistencia_desembolso = 'Desembolso Acima do Contratado') as ic_desembolso_acima_contratado,
    (b.ic_mesma_eo_selecao is false) as ic_eo_diferente_da_selecao,
    (b.situacao_portaria_ts = 'Sem registro de Trabalho Social') as ic_sem_trabalho_social,
    (b.situacao_portaria_ts = 'Portaria não informada pelo agente') as ic_portaria_ts_nao_informada,
    -- alertas que pedem ação (inconsistência comprovada ou associação ausente); "a confirmar" e
    -- "não informada" ficam de fora porque não são erro comprovado
    (
        (b.eo_cnpj is null)::int
        + (b.consistencia_desembolso = 'Desembolso Acima do Contratado')::int
        + coalesce((b.ic_mesma_eo_selecao is false)::int, 0)
    ) as qt_alertas
from base b
