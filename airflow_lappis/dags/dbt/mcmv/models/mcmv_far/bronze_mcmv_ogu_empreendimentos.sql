select
    cast(cod_operacao as varchar) as cod_operacao,

    cast(txt_sigla_uf as varchar) as uf,
    upper(trim(txt_nome_empreendimento)) as nome_empreendimento,
    cast(txt_situacao_empreendimento as varchar) as situacao_empreendimento,

    to_date(dt_assinatura, 'DD/MM/YYYY') as dt_assinatura,

    -- cast(qtd_uh as integer) as uh_contratadas,
    -- cast(qtd_uh_entregues as integer) as uh_entregues,
    -- cast(qtd_uh_vigentes as integer) as uh_vigentes,
    -- cast(qtd_uh_distratadas as integer) as uh_distratadas,
    {{parse_integer_value('qtd_uh')}} as uh_contratadas,
    {{parse_integer_value('qtd_uh_entregues')}} as uh_entregues,
    {{parse_integer_value('qtd_uh_vigentes')}} as uh_vigentes,
    {{parse_integer_value('qtd_uh_distratadas')}} as uh_distratadas,

    {{ parse_financial_value('val_contratado_total') }} as vl_contratado,
    {{ parse_financial_value('val_desembolsado') }} as vl_desembolsado,

    cast('OGU' as varchar) as fonte,
    cast('FAR' as varchar) as modalidade

from {{ source('__dados_brutos', 'dados_abertos_mcmv_ogu_empreendimentos') }}
where txt_modalidade = 'FAR'