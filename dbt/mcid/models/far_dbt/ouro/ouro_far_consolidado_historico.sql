{{ config(materialized="table") }}

-- Ouro: propostas do consolidado GFAR em cada remessa semanal, por fase, etapa,
-- status, fluxo, UF e tipo de demanda. Dá o passado da seleção, não só a foto atual.
-- Fase, etapa e status sem rótulo no seed aparecem pelo código.
with dominio as (select campo, codigo, rotulo from {{ ref("dominio_far_perguntas") }})

select
    s.dt_remessa,
    coalesce(fa.rotulo, 'Fase ' || s.co_fase, 'Não informada') as fase,
    coalesce(et.rotulo, 'Etapa ' || s.co_etapa, 'Não informada') as etapa,
    coalesce(st.rotulo, 'Status ' || s.co_status, 'Não informado') as status,
    coalesce(fl.rotulo, 'Não informado') as fluxo,
    coalesce(s.uf, 'Não informada') as uf,
    coalesce(td.rotulo, 'Não informada') as tipo_demanda,
    count(*) as qt_propostas,
    sum(s.qt_uh) as qt_uh,
    sum(s.valor_far) as valor_far
from {{ ref("prata_far_consolidado_serie") }} s
left join dominio fa on fa.campo = 'fase' and fa.codigo = lpad(s.co_fase::text, 3, '0')
left join dominio et on et.campo = 'etapa' and et.codigo = lpad(s.co_etapa::text, 3, '0')
left join dominio st on st.campo = 'status' and st.codigo = lpad(s.co_status::text, 3, '0')
left join dominio fl on fl.campo = 'fluxo' and fl.codigo = lpad(s.co_fluxo::text, 3, '0')
left join
    dominio td on td.campo = 'tipo_demanda' and td.codigo = lpad(s.co_tipo_demanda::text, 3, '0')
group by 1, 2, 3, 4, 5, 6, 7
