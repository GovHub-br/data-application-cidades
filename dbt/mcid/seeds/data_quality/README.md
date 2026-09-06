# seeds/data_quality/

Seeds de referência consumidos pelos testes genéricos de
`macros/data_quality/`. Change: `testes-data-quality-dbt`.

## `dominio_frente.csv`

Domínio canônico de `frente_mcmv` (FAR / Entidades / Rural + sentinela).
Consumido por `dentro_do_dominio`:

```yaml
- name: frente_mcmv
  data_tests:
    - dentro_do_dominio:
        arguments: { seed: dominio_frente, seed_column: valor_canonico }
```

## `colunas_esperadas.csv` (Fase 3)

`familia, coluna, desde` — o conjunto de colunas aceito por família do eixo
histórico. Semeado do `drift_schema.csv` do change `modelo-carga-drift-schema`.
Consumido por `colunas_esperadas` (nível `warn`).

## `dominio_status.csv` — PENDENTE

O mapa canônico de status de obra/contrato (`valor_bruto → valor_canonico →
classe`) é entregue pelo change `padronizacao-dominio-nulos-duplicados`
(`mapa_status.csv`). Este seed será uma cópia/derivação dele — não inventar a
enumeração aqui (Open Question 2 do design). Até lá, use `accepted_values`
inline onde o domínio for pequeno e estável.
