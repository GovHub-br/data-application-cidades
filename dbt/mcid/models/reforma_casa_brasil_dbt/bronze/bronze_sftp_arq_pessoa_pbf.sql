{{ config(materialized="table") }}

-- Cadastro de pessoas do CadÚnico, corte 12/12/2025. A origem lógica é o feed
-- CadÚnico recebido no SFTP. CPF/NIS chegam pseudonimizados com HMAC; nomes,
-- documentos e nascimento chegam redigidos. Códigos categóricos de raça e
-- deficiência permanecem somente nas camadas restritas.
select *
from {{ fonte_lake(
    'cadunico_pessoa',
    'lake_staging_reforma_casa_brasil',
    union_by_name=true
) }}
