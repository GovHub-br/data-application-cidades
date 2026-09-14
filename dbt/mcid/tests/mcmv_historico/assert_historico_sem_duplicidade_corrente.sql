-- DESABILITADO nesta branch: o modelo do piloto #118 (prata_dhist_serie_anual_ogu_fgts)
-- está enabled=false (change renomear-camadas-pt-historico-reloginho, D4). Reativar junto com o modelo.

{{ config(enabled=false) }}
select
    id_negocio_historico
from {{ ref("prata_dhist_serie_anual_ogu_fgts") }}
where is_current
group by id_negocio_historico
having count(*) > 1
