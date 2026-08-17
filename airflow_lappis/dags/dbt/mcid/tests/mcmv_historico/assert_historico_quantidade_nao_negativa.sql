select *
from {{ ref("historico_mcmv_serie_temporal_snapshot") }}
where quantidade_uh < 0
