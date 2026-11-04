{{ config(materialized="table") }}

-- Ouro: uma linha por contrato com Trabalho Social, pronta para o mapa do painel: coordenadas, tipo de
-- edificação, regime, público tradicional estimado, situação e portaria do TS, e o resumo dos alertas
-- de marco. Grão: id_ts. Chave: id_ts.
-- Fontes: prata_ts_acompanhamento, prata_ts_empreendimento_perfil, prata_ts_portaria_contrato e
-- a regra de alertas (corpo_ouro_ts_alerta_marco, a mesma da ouro_ts_alerta_marco).
-- publico_tradicional_estimado é ESTIMATIVA por palavra-chave no nome da EO e do empreendimento.

with
    alertas as (
        select
            id_ts,
            count(*) filter (where ic_descumprido) as qt_marcos_descumpridos,
            count(*) as qt_marcos_vencidos,
            max(marco) filter (where ic_alerta_principal) as marco_principal_descumprido,
            max(evidencia) filter (where ic_alerta_principal) as evidencia_principal
        from ({{ corpo_ouro_ts_alerta_marco() }}) as alerta
        group by id_ts
    )

select
    a.id_ts,
    a.linha,
    a.agente_financeiro,
    a.apf,
    a.empreendimento_nome,
    p.eo_nome,
    a.municipio,
    a.uf,
    'BR-' || a.uf as uf_iso,
    p.cod_ibge,
    p.regiao,
    p.latitude,
    p.longitude,
    p.latitude is not null as ic_tem_coordenada,
    p.origem_coordenada,
    a.qt_uh,
    p.tipo_edificacao,
    p.origem_tipo_edificacao,
    p.regime,
    p.tipo_demanda,
    p.publico_tradicional_estimado,
    p.origem_publico_tradicional,
    p.ic_publico_tradicional,
    a.dt_contratacao,
    a.situacao_ts,
    a.pc_obra,
    a.pc_execucao_ts,
    a.vr_global_ts,
    a.vr_desembolsado_ts,
    c.portaria_ts,
    c.origem_portaria_ts,
    c.portaria_selecao,
    coalesce(al.qt_marcos_vencidos, 0)::integer as qt_marcos_vencidos,
    coalesce(al.qt_marcos_descumpridos, 0)::integer as qt_marcos_descumpridos,
    coalesce(al.qt_marcos_descumpridos, 0) > 0 as ic_fora_do_marco,
    al.marco_principal_descumprido,
    al.evidencia_principal,
    case
        when coalesce(al.qt_marcos_descumpridos, 0) > 0 then 'Fora do marco'
        when not coalesce(c.ic_marcos_transcritos, false) then 'Sem régua de marcos'
        when a.situacao_ts in ('Concluído', 'Distrato') then 'TS encerrado'
        else 'Dentro do marco'
    end as status_marco,
    a.dt_referencia
from {{ ref("prata_ts_acompanhamento") }} a
left join {{ ref("prata_ts_empreendimento_perfil") }} p on p.id_ts = a.id_ts
left join {{ ref("prata_ts_portaria_contrato") }} c on c.id_ts = a.id_ts
left join alertas al on al.id_ts = a.id_ts
