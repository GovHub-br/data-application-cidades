select
    cast(empresa as varchar) as empresa,
    cast(uf as varchar) as uf,
    cast(municipio as varchar) as municipio,
    cast(validade as timestamp) as validade,
    cast(oc as varchar) as oc,
    cast(cnpj as varchar) as cnpj,
    cast(nivel as varchar) as nivel,
    cast(status as varchar) as status,
    cast(arquivo_origem as varchar) as arquivo_origem,
    cast(dt_ingest as timestamp) as dt_ingest
from {{ source('dados_abertos_cidades', 'dados_abertos_pbqp_h_siac') }}
