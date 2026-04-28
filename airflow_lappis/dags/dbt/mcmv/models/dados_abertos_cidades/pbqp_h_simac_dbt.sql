select
    cast(psq as varchar) as psq,
    cast(nome_da_empresa as varchar) as nome_da_empresa,
    cast(cnpj as varchar) as cnpj,
    cast(cidade as varchar) as cidade,
    cast(uf as varchar) as uf,
    cast(ncm as varchar) as ncm,
    cast(produto_alvo as varchar) as produto_alvo,
    cast(nome_da_egt as varchar) as nome_da_egt,
    cast(nome_comercial_do_produto_alvo as varchar) as nome_comercial_do_produto_alvo,
    cast(marca_do_produto_alvo as varchar) as marca_do_produto_alvo,
    cast(nome_da_entidade_setorial_nacional as varchar) as nome_da_entidade_setorial_nacional,
    cast(classificacao as varchar) as classificacao,
    cast(validade as date) as validade,
    cast(arquivo_origem as varchar) as arquivo_origem,
    cast(dt_ingest as timestamp) as dt_ingest
from {{ source('dados_abertos_cidades', 'dados_abertos_pbqp_h_simac') }}
