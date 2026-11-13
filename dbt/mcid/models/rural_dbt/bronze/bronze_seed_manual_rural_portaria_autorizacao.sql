{{ config(materialized="table") }}

-- Bronze: anexos das portarias de autorização de contratação do Rural (Seleção 2023 e Seleção 2025), extraídos dos
-- PDFs compilados e carregados no schema seeds do Postgres (uma tabela por portaria, fonte seeds_manuais).
-- As tabelas têm o mesmo layout; aqui só são empilhadas, com o nome da tabela de origem.
-- Nenhuma coluna é tipada nem limpa (isso é da prata). Grão: linha do anexo (proposta autorizada).
--
-- Para incluir uma portaria nova: carregar seeds.rural_portaria_autorizacao_<numero>_<ano>, declarar em
-- models/seeds_manuais.yml e acrescentar um bloco `union all` abaixo.

{% set tabelas = [
    "rural_portaria_autorizacao_1133_2024",
    "rural_portaria_autorizacao_1346_2024",
    "rural_portaria_autorizacao_1386_2024",
    "rural_portaria_autorizacao_278_2025",
    "rural_portaria_autorizacao_562_2025",
    "rural_portaria_autorizacao_1362_2026",
    "rural_portaria_autorizacao_1449_2026",
] %}

{% for t in tabelas %}
select '{{ t }}'::text as tabela_origem, *
from {{ source("seeds_manuais", t) }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
