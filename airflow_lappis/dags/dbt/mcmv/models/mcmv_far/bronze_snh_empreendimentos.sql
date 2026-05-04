select
    cast(identificador_da_operacao_na_snh as varchar) as cod_operacao,
    cast(codigo_da_operacao_no_agente_financeiro as varchar) as cod_operacao_agente,

    cast(sigla_da_uf as varchar) as uf,
    upper(trim(nome_do_empreendimento)) as nome_empreendimento,
    cast(situacao_do_empreendimento as varchar) as situacao_empreendimento,

    cast(percentual_da_obra as numeric(10,2)) as percentual_execucao,

    {{ parse_financial_value('valor_do_aporte_adicional') }} as vl_aporte_adicional,
    {{ parse_financial_value('valor_contratado_total') }} as vl_contratado,
    {{ parse_financial_value('valor_desembolsado_total') }} as vl_desembolsado,

    cast(unidades_contratadas as integer) as uh_contratadas,
    cast(unidades_entregues as integer) as uh_entregues,
    cast(unidades_vigentes as integer) as uh_vigentes,
    cast(unidades_distratadas as integer) as uh_distratadas,
    cast(unidades_habitacionais_a_serem_entregues as integer) as uh_a_entregar,

    cast(data_do_termino as date) as dt_termino_obra,
    cast(data_de_previsao_de_termino as date) as dt_previsao_termino,

    cast('SNH' as varchar) as fonte,
    cast(modalidade as varchar) as modalidade

from {{ source('__dados_brutos', 'dados_prioritarios_disponibilizados_snh_empreendimentos') }}
where modalidade = 'FAR'