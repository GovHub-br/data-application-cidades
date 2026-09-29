{{ config(materialized='table') }}

with fgts as (
    select
        ano_dotacao::integer as ano,
        'FGTS'::text as fonte_recurso,
        programa::text as programa,
        coalesce(agrupamento::text, uf_codigo::text) as agrupamento,
        null::text as unidade_gestora,
        nullif(replace(orcamento_original::text, ',', '.'), '')::numeric as orcamento_original,
        nullif(replace(orcamento_final::text, ',', '.'), '')::numeric as orcamento_atualizado,
        nullif(replace(orcamento_alocado::text, ',', '.'), '')::numeric as orcamento_alocado,
        null::numeric as despesas_empenhadas,
        null::numeric as despesas_pagas,
        null::numeric as restos_a_pagar_pagos,
        'Anual'::text as periodicidade,
        arquivo_de_origem::text as arquivo_origem
    from {{ ref('bronze_sharepoint_tpc_orcamento_final') }}
),
fundo_social as (
    select
        2026::integer as ano,
        'Fundo Social'::text as fonte_recurso,
        acao_governo::text as programa,
        'Nacional'::text as agrupamento,
        ug_executora::text as unidade_gestora,
        nullif(replace(nullif(dotacao_inicial::text, 'None'), ',', '.'), '')::numeric as orcamento_original,
        nullif(replace(nullif(dotacao_atualizada::text, 'None'), ',', '.'), '')::numeric as orcamento_atualizado,
        nullif(replace(nullif(credito_disponivel::text, 'None'), ',', '.'), '')::numeric as orcamento_alocado,
        nullif(replace(nullif(despesas_empenhadas::text, 'None'), ',', '.'), '')::numeric as despesas_empenhadas,
        nullif(replace(nullif(despesas_pagas::text, 'None'), ',', '.'), '')::numeric as despesas_pagas,
        nullif(replace(nullif(restos_a_pagar_pagos_proc_e_n_proc::text, 'None'), ',', '.'), '')::numeric as restos_a_pagar_pagos,
        'Posição do arquivo'::text as periodicidade,
        arquivo_de_origem::text as arquivo_origem
    from {{ ref('bronze_sharepoint_orcamento_resultado_fundo_social') }}
)
select
    md5(concat_ws('|', fonte_recurso, ano::text, programa, agrupamento, coalesce(unidade_gestora, ''))) as id_execucao_orcamentaria,
    *,
    coalesce(despesas_pagas, 0) + coalesce(restos_a_pagar_pagos, 0) as pagamentos_totais,
    case when orcamento_atualizado > 0
         then (coalesce(despesas_pagas, 0) + coalesce(restos_a_pagar_pagos, 0)) / orcamento_atualizado
    end as percentual_execucao_financeira,
    current_timestamp as dt_silver
from (
    select * from fgts
    union all by name
    select * from fundo_social
) u
