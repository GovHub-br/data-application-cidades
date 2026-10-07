{{ config(materialized="table") }}

-- Gold: Quantos termos de compromisso do FNHIS Sub-50 estavam em cada situação em cada retrato
-- mensal do TransfereGov, quantos entraram nela no retrato e quanto estava empenhado. Réplica da
-- pergunta do Rural "hoje só chega o dado final". Grão: retrato × UF × situação do instrumento.

select
    dt_retrato,
    coalesce(uf, 'ND') as uf,
    {{ regiao_da_uf("uf") }} as regiao,
    coalesce(situacao_instrumento, 'Sem situação no retrato') as situacao_instrumento,
    count(*) as qt_termos,
    coalesce(sum(valor_repasse), 0) as valor_repasse,
    coalesce(sum(valor_empenhado_acumulado), 0) as valor_empenhado_acumulado,
    count(*) filter (where situacao_instrumento_retrato_anterior is not null
                     and situacao_instrumento is distinct from situacao_instrumento_retrato_anterior) as qt_entraram_no_retrato
from {{ ref("prata_fnhis_termo_compromisso_mensal") }}
group by 1, 2, 3, 4
