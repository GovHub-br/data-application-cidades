# ADR-001 — Produto dbt Conjuntura canônico

**Status:** aceito em 2026-08-30.

## Decisão

O produto canônico do boletim habitacional é `conjuntura_dbt`. Ele sucede o
antigo `conjuntura_continuo_dbt` e atende tanto às séries contínuas quanto às
edições trimestrais.

O antigo conjunto de 54 models `conjuntura_dbt` foi aposentado. Nenhum
dashboard ativo de Conjuntura nem outro model dbt o referenciava no momento da
decisão.

## Consequências

- A DAG canônica seleciona `conjuntura_dbt`.
- A antiga DAG que construía exclusivamente o legado é removida.
- Os schemas físicos `conjuntura_continuo_bronze`,
  `conjuntura_continuo_silver` e `conjuntura_continuo_mart` permanecem nesta
  fase, para preservar Superset e demais consumidores.
- A remoção de tabelas e schemas físicos legados exige auditoria própria no
  banco e não faz parte desta decisão.
- Uma futura renomeação de `conjuntura_dbt` para `conjuntura` será feita após
  a refatoração de governança, qualidade e documentação.
