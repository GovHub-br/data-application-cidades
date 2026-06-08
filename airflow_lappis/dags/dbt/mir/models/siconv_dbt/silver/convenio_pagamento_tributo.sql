{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenio") }}
    ),
    pagamento_tributo_agg as (
        select
            nr_convenio,
            sum(vl_pag_tributos) as total_pago_tributos,
            count(*) as qtd_pagamentos_tributos,
            min(data_tributo) as primeiro_pagamento_tributo,
            max(data_tributo) as ultimo_pagamento_tributo
        from {{ ref("pagamento_tributo") }}
        group by nr_convenio
    )

select
    c.*,
    t.total_pago_tributos,
    t.qtd_pagamentos_tributos,
    t.primeiro_pagamento_tributo,
    t.ultimo_pagamento_tributo
from convenio c
left join pagamento_tributo_agg t on c.nr_convenio = t.nr_convenio