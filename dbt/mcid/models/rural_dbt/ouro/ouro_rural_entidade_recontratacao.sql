{{ config(materialized="table") }}

-- Gold: Para cada operação do Rural, o que a MESMA entidade organizadora ainda tinha em aberto
-- quando ela foi contratada. Responde "a entidade concluiu o anterior antes de concorrer ao
-- próximo?".
-- Grão: operação (APF) com EO identificada.
--
-- Operação anterior "pendente" na data D = contratada antes de D, não distratada, e sem obra
-- concluída até D (dt_conclusao_obra > D, ou nula com status ainda aberto). Anteriores que hoje
-- estão concluídas mas sem data de conclusão não entram como pendentes: são contadas à parte em
-- `qt_anteriores_sem_data_conclusao`, para mostrar a incerteza em vez de escondê-la.
-- A data de contratação é usada no lugar da data em que a EO concorreu, que não existe no dado.

with
    op as (
        select * from {{ ref("prata_rural_operacao_eo") }}
        where eo_cnpj is not null and dt_contratacao is not null
    ),

    pares as (
        select
            n.apf,
            count(a.apf) as qt_anteriores,
            count(a.apf) filter (where
                a.status_operacao <> 'Distratada'
                and (a.dt_conclusao_obra > n.dt_contratacao
                     or (a.dt_conclusao_obra is null and a.status_operacao in ('Em obra', 'Paralisada', 'Não iniciada')))
            ) as qt_anteriores_pendentes,
            coalesce(sum(a.quantidade_uh_contratadas - coalesce(a.quantidade_uh_entregues, 0)) filter (where
                a.status_operacao <> 'Distratada'
                and (a.dt_conclusao_obra > n.dt_contratacao
                     or (a.dt_conclusao_obra is null and a.status_operacao in ('Em obra', 'Paralisada', 'Não iniciada')))
            ), 0) as uh_anteriores_pendentes,
            count(a.apf) filter (where a.status_operacao = 'Paralisada') as qt_anteriores_hoje_paralisadas,
            count(a.apf) filter (where a.dt_conclusao_obra is null and a.status_operacao = 'Concluída') as qt_anteriores_sem_data_conclusao,
            min(a.dt_contratacao) filter (where
                a.status_operacao <> 'Distratada'
                and (a.dt_conclusao_obra > n.dt_contratacao
                     or (a.dt_conclusao_obra is null and a.status_operacao in ('Em obra', 'Paralisada', 'Não iniciada')))
            ) as dt_anterior_pendente_mais_antiga
        from op n
        left join op a on a.eo_cnpj = n.eo_cnpj and a.dt_contratacao < n.dt_contratacao
        group by n.apf
    )

select
    o.apf,
    o.eo_cnpj,
    o.eo_nome,
    o.fonte_eo,
    o.ic_novo_mcmv,
    o.empreendimento_nome,
    o.municipio,
    o.uf,
    o.dt_contratacao,
    extract(year from o.dt_contratacao)::int as ano_contratacao,
    o.quantidade_uh_contratadas,
    o.status_operacao,
    p.qt_anteriores,
    p.qt_anteriores_pendentes,
    p.uh_anteriores_pendentes,
    p.qt_anteriores_hoje_paralisadas,
    p.qt_anteriores_sem_data_conclusao,
    p.dt_anterior_pendente_mais_antiga,
    p.qt_anteriores_pendentes > 0 as ic_contratou_com_pendencia,
    case
        when p.qt_anteriores = 0 then 'Primeira operação da EO'
        when p.qt_anteriores_pendentes = 0 then 'Anteriores concluídas'
        when p.qt_anteriores_hoje_paralisadas > 0 then 'Contratou com anterior pendente (hoje paralisada)'
        else 'Contratou com anterior pendente'
    end as situacao_recontratacao
from op o
join pares p on p.apf = o.apf
