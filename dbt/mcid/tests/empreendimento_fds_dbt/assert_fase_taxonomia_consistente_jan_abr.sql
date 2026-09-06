-- Consistência da taxonomia de fase entre os extratos do seed
-- (change id-empreendimento-eixo-historico, task 6.1).
--
-- O mesmo APF não pode receber `fase_empreendimento` conflitante entre os
-- extratos `JAN26` e `ABR26` do seed_apf_fase_fds. Evolução monotônica
-- Projeto -> Obra -> Desligamento é aceita (o APF avança de fase); o que NÃO é
-- aceito é a mesma APF em duas fases sem ordem, ou uma regressão de fase.
--
-- NOTA: no snapshot atual o seed só tem linhas `ABR26` (0 linhas `JAN26`),
-- então este teste passa vazio até o extrato JAN26 ser incorporado. Fica pronto.
with
    ordem as (
        select 'Projeto' as fase, 1 as rank
        union all
        select 'Obra', 2
        union all
        select 'Desligamento', 3
    ),

    por_apf as (
        select
            s.apf,
            min(o.rank) filter (where s.arquivo_origem = 'JAN26') as rank_jan,
            max(o.rank) filter (where s.arquivo_origem = 'ABR26') as rank_abr
        from {{ ref("seed_apf_fase_fds") }} s
        join ordem o on s.fase_empreendimento = o.fase
        group by s.apf
    )

select apf, rank_jan, rank_abr
from por_apf
where rank_jan is not null and rank_abr is not null and rank_abr < rank_jan
