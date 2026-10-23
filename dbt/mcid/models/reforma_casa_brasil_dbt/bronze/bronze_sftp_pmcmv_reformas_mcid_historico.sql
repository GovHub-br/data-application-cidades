{{ config(materialized="table") }}

-- Cada arquivo do GEFUS é uma fotografia da carteira. Ao contrário da Bronze
-- corrente, esta tabela preserva todas as competências para o monitoramento
-- temporal. A origem já é staging protegida; identificadores diretos não são
-- consumidos pelas camadas Ouro.
select *
from {{ fonte_lake(
    'reforma_contratos',
    'lake_staging_reforma_casa_brasil',
    filename=true,
    union_by_name=true
) }} as r
where nullif(trim(cast(r['nu_contrato'] as varchar)), '') is not null
