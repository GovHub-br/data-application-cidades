select
    id_negocio_historico
from {{ ref("historico_mcmv_serie_temporal_snapshot") }}
where is_current
group by id_negocio_historico
having count(*) > 1
