{#-
    Completude da carga das portarias de autorização do Rural: para cada portaria, a bronze tem de ter o
    mesmo número de propostas e de UH que a extração do PDF registrou na tabela de conferência
    (seeds.rural_portaria_autorizacao_conferencia). Devolve a portaria que não bate.
-#}
{{ config(severity="error") }}

with
    bronze as (
        select portaria_codigo::text as portaria_codigo, count(*) as propostas,
               sum(nullif(trim(qt_uh::text), '')::int) as uh
        from {{ ref("bronze_seed_manual_rural_portaria_autorizacao") }}
        group by portaria_codigo::text
    ),
    conf as (
        select portaria_codigo::text as portaria_codigo,
               nullif(trim(propostas::text), '')::int as propostas,
               nullif(trim(uh::text), '')::int as uh
        from {{ ref("bronze_seed_manual_rural_portaria_autorizacao_conferencia") }}
    )

select c.portaria_codigo, c.propostas as propostas_conferencia, b.propostas as propostas_bronze,
       c.uh as uh_conferencia, b.uh as uh_bronze
from conf c
full join bronze b on b.portaria_codigo = c.portaria_codigo
where b.propostas is distinct from c.propostas or b.uh is distinct from c.uh
