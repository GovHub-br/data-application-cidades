{{ config(materialized="table") }}

-- Ouro: trajetória de cada proposta do consolidado GFAR ao longo das remessas
-- semanais: quando entrou, quando saiu, se sumiu e voltou, quantas vezes mudou de
-- situação, e a situação na primeira e na última remessa em que apareceu.
with
    dominio as (select campo, codigo, rotulo from {{ ref("dominio_far_perguntas") }}),

    numeradas as (
        select dt_remessa, dense_rank() over (order by dt_remessa) as n_remessa
        from (select distinct dt_remessa from {{ ref("prata_far_consolidado_serie") }}) d
    ),

    serie as (
        select
            s.*,
            n.n_remessa,
            coalesce(fa.rotulo, 'Fase ' || s.co_fase, 'Não informada')
            || ' › '
            || coalesce(et.rotulo, 'Etapa ' || s.co_etapa, 'Não informada')
            || ' › '
            || coalesce(st.rotulo, 'Status ' || s.co_status, 'Não informado') as situacao,
            coalesce(et.rotulo, 'Etapa ' || s.co_etapa, 'Não informada') as etapa,
            coalesce(st.rotulo, 'Status ' || s.co_status, 'Não informado') as status,
            coalesce(td.rotulo, 'Não informada') as tipo_demanda
        from {{ ref("prata_far_consolidado_serie") }} s
        join numeradas n using (dt_remessa)
        left join dominio fa on fa.campo = 'fase' and fa.codigo = lpad(s.co_fase::text, 3, '0')
        left join dominio et on et.campo = 'etapa' and et.codigo = lpad(s.co_etapa::text, 3, '0')
        left join dominio st on st.campo = 'status' and st.codigo = lpad(s.co_status::text, 3, '0')
        left join
            dominio td
            on td.campo = 'tipo_demanda'
            and td.codigo = lpad(s.co_tipo_demanda::text, 3, '0')
    ),

    com_anterior as (
        select
            *,
            lag(situacao) over (partition by id_proposta order by dt_remessa) as situacao_anterior
        from serie
    ),

    trajetoria as (
        select
            id_proposta,
            (array_agg(apf order by dt_remessa desc))[1] as apf,
            (array_agg(uf order by dt_remessa desc))[1] as uf,
            (array_agg(municipio order by dt_remessa desc))[1] as municipio,
            (array_agg(tipo_demanda order by dt_remessa desc))[1] as tipo_demanda,
            (array_agg(qt_uh order by dt_remessa desc))[1] as qt_uh,
            (array_agg(valor_far order by dt_remessa desc))[1] as valor_far,
            (array_agg(situacao order by dt_remessa))[1] as situacao_inicial,
            (array_agg(situacao order by dt_remessa desc))[1] as situacao_atual,
            (array_agg(etapa order by dt_remessa desc))[1] as etapa_atual,
            (array_agg(status order by dt_remessa desc))[1] as status_atual,
            min(dt_remessa) as dt_primeira_remessa,
            max(dt_remessa) as dt_ultima_remessa,
            max(dt_remessa) filter (
                where situacao is distinct from situacao_anterior
            ) as dt_ultima_mudanca,
            count(*) as qt_remessas_presente,
            max(n_remessa) - min(n_remessa) + 1 - count(*) as qt_remessas_ausente,
            count(*) filter (
                where situacao_anterior is not null and situacao <> situacao_anterior
            ) as qt_mudancas_situacao
        from com_anterior
        group by id_proposta
    )

select
    t.*,
    t.dt_ultima_remessa = (select max(dt_remessa) from numeradas) as ic_na_remessa_mais_recente,
    t.qt_remessas_ausente > 0 as ic_sumiu_e_voltou,
    coalesce(t.municipio || '/' || t.uf, t.uf, 'Não informado') as municipio_uf
from trajetoria t
