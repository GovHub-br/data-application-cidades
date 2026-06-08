{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenio") }}
    ),
    pagamento_agg as (
        select
            nr_convenio,
            count(id_dl) as qtd_pagamentos,
            sum(vl_pago) as total_pago,
            string_agg(distinct identif_fornecedor, ', ') as fornecedores,
            string_agg(distinct nome_fornecedor, ', ') as nomes_fornecedores,
            string_agg(distinct tp_mov_financeira, ', ') as tipos_movimento,
            min(data_pag) as primeiro_pagamento,
            max(data_pag) as ultimo_pagamento
        from {{ ref("pagamento") }}
        group by nr_convenio
    )

select
    c.*,
    p.qtd_pagamentos,
    p.total_pago,
    p.fornecedores,
    p.nomes_fornecedores,
    p.tipos_movimento,
    p.primeiro_pagamento,
    p.ultimo_pagamento,
    case 
        when p.ultimo_pagamento > c.dia_fim_vigenc_conv 
        then 'SIM' 
        else 'NÃO' 
    end as pagamento_apos_vigencia
from convenio c
left join pagamento_agg p on c.nr_convenio = p.nr_convenio