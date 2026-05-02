select
    cast(nu_apf as varchar) as apf,
    cast(no_empreendimento as varchar) as no_empreendimento,

    cast(situacao_obra_gefus as varchar) as situacao_obra,
    to_date(dt_assinatura, 'YYYY/MM/DD') as dt_contratacao,
    to_date(dt_inicio_obra, 'YYYY/MM/DD') as dt_inicio_obra,
    to_date(dt_ultima_liberacao_recurso, 'YYYY/MM/DD') as dt_ultima_liberacao_recurso,
    to_date(dt_termino_obra, 'YYYY/MM/DD') as dt_termino_obra,
    to_date(dt_legalizacao, 'YYYY/MM/DD') as dt_legalizacao,
    to_date(dt_primeira_entrega, 'YYYY/MM/DD') as dt_primeira_entrega,
    to_date(dt_ultima_entrega, 'YYYY/MM/DD') as dt_ultima_entrega,

    -- cast(percentual_obra_realizado as numeric(10,2)) as percentual_execucao,
    {{ parse_percentage('percentual_obra_realizado') }} as percentual_execucao,

    {{ parse_financial_value('vlr_operacao') }} as vl_contratado,
    {{ parse_financial_value('vr_investimento') }} as vr_investimento,
    {{ parse_financial_value('vr_liberado') }} as vr_liberado,

    cast(qt_unidade_financiadas as integer) as uh_contratadas,
    cast(qt_unidades_concluidas as integer) as uh_concluidas,
    cast(qt_unidades_ociosas as integer) as uh_ociosas,
    cast(qt_unidades_entregues as integer) as uh_entregues,

    cast(portaria_selecao as varchar) as portaria_selecao,
    cast(situacao_retomada as varchar) as situacao_retomada,
    {{ parse_financial_value('vr_contrapartida_1') }} as vl_contrapartida,
    cast(tipo_aporte as varchar) as tipo_aporte,

    cast('CAIXA' as varchar) as fonte

from {{ source('__dados_brutos', 'int_empreendimentos_int040_far_caixa_pj') }}

union all

select
    cast(nu_apf as varchar) as apf,
    cast(no_empreendimento as varchar) as no_empreendimento,

    cast(situacao_obra as varchar) as situacao_obra,
    to_date(dt_contratacao, 'YYYY/MM/DD') as dt_contratacao,
    to_date(dt_inicio_obra, 'YYYY/MM/DD') as dt_inicio_obra,
    to_date(dt_ultima_liberacao_recurso, 'YYYY/MM/DD') as dt_ultima_liberacao_recurso,
    to_date(dt_termino_obra, 'YYYY/MM/DD') as dt_termino_obra,
    to_date(dt_legalizacao, 'YYYY/MM/DD') as dt_legalizacao,
    to_date(dt_primeira_entrega, 'YYYY/MM/DD') as dt_primeira_entrega,
    to_date(dt_ultima_entrega, 'YYYY/MM/DD') as dt_ultima_entrega,

    {{ parse_percentage('percentual_obra_realizado') }} as percentual_execucao,

    {{ parse_financial_value('vr_emprestimo_far') }} as vl_contratado,
    {{ parse_financial_value('vr_investimento') }} as vr_investimento,
    {{ parse_financial_value('total_liberado_far') }} as vr_liberado,

    cast(qt_unidades_habitacionais as integer) as uh_contratadas,
    cast(qt_unidades_concluidas as integer) as uh_concluidas,
    cast(qt_unidades_ociosas as integer) as uh_ociosas,
    cast(qt_unidades_entregues as integer) as uh_entregues,

    cast(portaria_selecao as varchar) as portaria_selecao,
    cast(situacao_retomada as varchar) as situacao_retomada,
    {{ parse_financial_value('vr_contrapartida_1') }} as vl_contrapartida,
    cast(tipo_aporte as varchar) as tipo_aporte,

    cast('BB' as varchar) as fonte

from {{ source('__dados_brutos', 'int_empreendimentos_int_054_far_bb_pj') }}