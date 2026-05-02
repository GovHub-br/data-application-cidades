select
    cast(uf as varchar) as uf,
    cast(apf as varchar) as apf,
    cast(nome_empreendimento as varchar) as nome_empreendimento,
    cast(situacao_do_empreendimento as varchar) as situacao_do_empreendimento,
    cast(detalhamento_da_situacao_do_empreendimento as varchar) as detalhamento_da_situacao_do_empreendimento,

    to_date(data_de_contratacao, 'DD/MM/YYYY') as dt_contratacao,

    {{ parse_percentage('percentual_exec') }} as percentual_exec,

    {{ parse_financial_value('valor_contratado') }} as vl_contratado,
    {{ parse_financial_value('valor_aporte_adicional') }} as vl_aporte_adicional,

    {{ parse_integer_value('uh_contratadas') }} as uh_contratadas,
    {{ parse_integer_value('uh_entregues') }} as uh_entregues,
    {{ parse_integer_value('uh_vigentes') }} as uh_vigentes,
    {{ parse_integer_value('quantidade_de_uhs_distratadas') }} as uh_distratadas,

    to_date(data_da_previsao_da_entrega, 'DD/MM/YYYY') as dt_previsao_entrega,

    {{ parse_integer_value('unidades_habitacionais_a_serem_entregues') }} as uh_a_entregar,

    cast('BB' as varchar) as fonte

from {{ source('__dados_brutos', 'dados_prioritarios_recebidos_bb_empreendimentos') }}
where modalidade = 'FAR'

union all

select
    cast(uf as varchar) as uf,
    cast(apf as varchar) as apf,
    cast(nome_empreendimento as varchar) as nome_empreendimento,
    cast(situacao_do_empreendimento as varchar) as situacao_do_empreendimento,
    cast(detalhamento_da_situacao_do_empreendimento as varchar) as detalhamento_da_situacao_do_empreendimento,

    to_date(data_de_contratacao, 'DD/MM/YYYY') as dt_contratacao,

    {{ parse_percentage('percentual_exec') }} as percentual_exec,

    {{ parse_financial_value('valor_contratado') }} as vl_contratado,
    {{ parse_financial_value('valor_aporte_adicional') }} as vl_aporte_adicional,

    {{ parse_integer_value('uh_contratadas') }} as uh_contratadas,
    {{ parse_integer_value('uh_entregues') }} as uh_entregues,
    {{ parse_integer_value('uh_vigentes') }} as uh_vigentes,
    {{ parse_integer_value('quantidade_de_uhs_distratadas') }} as uh_distratadas,

    to_date(data_da_previsao_da_entrega, 'DD/MM/YYYY') as dt_previsao_entrega,

    {{ parse_integer_value('unidades_habitacionais_a_serem_entregues') }} as uh_a_entregar,

    cast('CAIXA' as varchar) as fonte

from {{ source('__dados_brutos', 'dados_prioritarios_recebidos_caixa_empreendimentos') }}
where modalidade = 'FAR'