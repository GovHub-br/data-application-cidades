{{ config(materialized="table") }}

-- Gold: O que aconteceu com cada empreendimento selecionado pela Portaria MCidades nº 162/2018
-- (único ciclo do Rural com lista nominal de selecionados). Grão: empreendimento selecionado.
--
-- O vínculo seleção → contrato é pela APF (código do empreendimento na portaria). Primeiro na
-- visão consolidada do Rural; se não estiver lá, no INT065 (PNHR CAIXA). `ic_mesma_eo` compara
-- a EO selecionada com a que aparece no contrato. Não há lista de propostas APRESENTADAS nem
-- motivo de não seleção — o ciclo só pode ser lido a partir de quem foi selecionado.

with
    sel as (select * from {{ ref("prata_rural_selecao_portaria_162") }}),
    op as (select * from {{ ref("prata_rural_operacao_eo") }}),
    pnhr as (
        select distinct on (apf) apf, eo_cnpj, situacao_obra, dt_contrato, qt_unidades, qt_unidades_entregues
        from {{ ref("prata_rural_pnhr_caixa") }}
        order by apf, dt_movimento desc nulls last
    )

select
    s.apf,
    s.codigo_empreendimento,
    s.ato_selecao,
    s.dt_portaria,
    s.uf,
    s.regiao,
    s.municipio,
    s.entidade_organizadora_cnpj as eo_cnpj_selecionada,
    s.quantidade_uh_selecionadas,
    (o.apf is not null or p.apf is not null) as ic_contratado,
    case when o.apf is not null then 'visão consolidada Rural' when p.apf is not null then 'INT065 PNHR CAIXA' end as fonte_contrato,
    coalesce(o.dt_contratacao, p.dt_contrato) as dt_contratacao,
    coalesce(o.dt_contratacao, p.dt_contrato) - s.dt_portaria as dias_selecao_ate_contrato,
    coalesce(o.eo_cnpj, regexp_replace(p.eo_cnpj::text, '[^0-9]', '', 'g')) as eo_cnpj_contrato,
    o.eo_nome as eo_nome_contrato,
    coalesce(o.eo_cnpj, regexp_replace(p.eo_cnpj::text, '[^0-9]', '', 'g')) = s.entidade_organizadora_cnpj as ic_mesma_eo,
    coalesce(o.quantidade_uh_contratadas, p.qt_unidades) as uh_contratadas,
    coalesce(o.quantidade_uh_entregues, p.qt_unidades_entregues) as uh_entregues,
    coalesce(o.status_operacao,
        case
            when p.situacao_obra ~* 'conclu' then 'Concluída'
            when p.situacao_obra ~* 'paralis' then 'Paralisada'
            when p.situacao_obra is not null then 'Em obra'
        end,
        'Não contratado (sem vínculo)') as status_operacao,
    coalesce(o.situacao_empreendimento, p.situacao_obra) as situacao_origem
from sel s
left join op o on o.apf = s.apf
left join pnhr p on p.apf = s.apf and o.apf is null
