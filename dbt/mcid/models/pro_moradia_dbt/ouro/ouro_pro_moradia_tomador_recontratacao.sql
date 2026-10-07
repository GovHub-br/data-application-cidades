{{ config(materialized="table") }}

-- Gold: Para cada contrato do Pró-Moradia, o que o MESMO tomador (município, estado ou
-- companhia) ainda tinha em aberto quando o assinou. Réplica, para o Pró-Moradia, da pergunta do
-- Rural "concluiu o anterior antes de concorrer ao próximo?".
-- Grão: contrato.
--
-- Contrato anterior "pendente" na data D = assinado antes de D, não cancelado/distratado, e com
-- obra não terminada até D (dt_termino_obra > D, ou sem término e status ainda aberto). A data de
-- assinatura substitui a data da carta-consulta, que não existe no Canal FGTS.

with
    c as (
        select f.*, p.tomador_cnpj
        from {{ ref("ouro_pro_moradia_ficha_contrato") }} f
        left join {{ ref("prata_pro_moradia_contrato") }} p on p.cod_contrato = f.cod_contrato
        where f.tomador_codigo is not null and f.dt_assinatura is not null
    ),

    pendente as (
        select
            n.cod_contrato,
            count(a.cod_contrato) as qt_anteriores,
            count(a.cod_contrato) filter (where
                a.status_execucao_simplificado <> 'Cancelado ou distratado'
                and (a.dt_termino_obra > n.dt_assinatura
                     or (a.dt_termino_obra is null
                         and a.status_execucao_simplificado in ('Em Andamento', 'Paralisado', 'Não Iniciado')))
            ) as qt_anteriores_pendentes,
            coalesce(sum(a.valor_contratado) filter (where
                a.status_execucao_simplificado <> 'Cancelado ou distratado'
                and (a.dt_termino_obra > n.dt_assinatura
                     or (a.dt_termino_obra is null
                         and a.status_execucao_simplificado in ('Em Andamento', 'Paralisado', 'Não Iniciado')))
            ), 0) as valor_anteriores_pendentes,
            count(a.cod_contrato) filter (where a.status_execucao_simplificado = 'Paralisado') as qt_anteriores_hoje_paralisados,
            count(a.cod_contrato) filter (where a.dt_termino_obra is null and a.status_execucao_simplificado in ('Concluído', 'Sem Informação')) as qt_anteriores_sem_data_termino
        from c n
        left join c a on a.tomador_codigo = n.tomador_codigo and a.dt_assinatura < n.dt_assinatura
        group by n.cod_contrato
    )

select
    c.cod_contrato,
    c.contrato,
    c.tomador_codigo,
    c.tomador_nome,
    c.tomador_esfera,
    c.tomador_cnpj,
    c.uf,
    c.municipio,
    c.tipo_intervencao,
    c.identificador_selecao,
    c.dt_assinatura,
    extract(year from c.dt_assinatura)::int as ano_assinatura,
    c.valor_contratado,
    c.status_execucao_simplificado,
    p.qt_anteriores,
    p.qt_anteriores_pendentes,
    p.valor_anteriores_pendentes,
    p.qt_anteriores_hoje_paralisados,
    p.qt_anteriores_sem_data_termino,
    p.qt_anteriores_pendentes > 0 as ic_contratou_com_pendencia,
    case
        when p.qt_anteriores = 0 then 'Primeiro contrato do tomador'
        when p.qt_anteriores_pendentes = 0 then 'Anteriores concluídos'
        when p.qt_anteriores_hoje_paralisados > 0 then 'Contratou com anterior pendente (hoje paralisado)'
        else 'Contratou com anterior pendente'
    end as situacao_recontratacao
from c
join pendente p on p.cod_contrato = c.cod_contrato
