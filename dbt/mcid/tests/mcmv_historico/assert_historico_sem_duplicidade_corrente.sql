select
    id_negocio_historico
from {{ ref("silver_mcmv_historico_serie_anual_ogu_fgts") }}
where is_current
group by id_negocio_historico
having count(*) > 1
