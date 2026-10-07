{{ config(materialized="table") }}

-- Cadastro de famílias do CadÚnico, corte 12/12/2025. O arquivo foi recuperado
-- pelo canal SharePoint, mas pertence à mesma origem lógica CadÚnico/SFTP e mantém
-- o nome físico acordado para a fonte. Endereço e entrevistador chegam protegidos.
select *
from {{ fonte_lake(
    'cadunico_familia',
    'lake_staging_reforma_casa_brasil',
    union_by_name=true
) }}
