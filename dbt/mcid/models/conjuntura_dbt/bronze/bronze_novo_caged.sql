select 
    cast(ano as integer) as ano,
    cast(mes as integer) as mes,
    cast(admitidos as bigint) as admitidos,
    cast(desligados as bigint) as desligados,
    cast(saldo as bigint) as saldo,
    cast(estoque as bigint) as estoque,
    cast(variacao as float(5)) as variacao,
    cast(dt_ingest as timestamp) as dt_ingest
from {{ source('novo_caged', 'saldo_estoque_construcao_edificios') }}