{{ config(materialized="table") }}

-- Ouro: contrapartidas pactuadas pelos entes em cada empreendimento FAR, uma linha por
-- APF × ente × tipo, só com valor informado. Responde quem aporta, em quê e quanto.
with
    n as (select * from {{ ref("prata_far_empreendimento_natureza") }}),
    e as (select apf, empreendimento_nome, municipio, uf, quantidade_uh from {{ ref("prata_far_empreendimento") }}),

    longo as (
        select apf, 'Município' as ente, 'Terreno' as tipo, vr_cp_municipio_terreno as valor from n
        union all select apf, 'Município', 'Infraestrutura', vr_cp_municipio_infra from n
        union all select apf, 'Município', 'Complementar', vr_cp_municipio_complementar from n
        union all select apf, 'Estado', 'Terreno', vr_cp_estado_terreno from n
        union all select apf, 'Estado', 'Infraestrutura', vr_cp_estado_infra from n
        union all select apf, 'Estado', 'Complementar', vr_cp_estado_complementar from n
        union all select apf, 'Ente público (outras)', 'Outras', vr_cp_outras_ente_publico from n
        union all select apf, 'Outros', 'Outras', vr_cp_outras from n
    )

select
    l.apf,
    e.empreendimento_nome as nome_empreendimento,
    e.municipio,
    e.uf,
    e.quantidade_uh as qt_uh,
    l.ente,
    l.tipo,
    l.valor
from longo l
join e on e.apf = l.apf
where l.valor > 0
