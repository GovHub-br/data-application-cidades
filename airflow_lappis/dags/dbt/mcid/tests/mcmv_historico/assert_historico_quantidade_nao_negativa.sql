select *
from {{ ref("silver_mcmv_historico_serie_anual_ogu_fgts") }}
where quantidade_uh < 0
