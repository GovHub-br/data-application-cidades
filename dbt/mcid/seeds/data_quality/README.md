# seeds/data_quality/

Seeds de referência consumidos pelos testes genéricos de
`macros/data_quality/`. Change de origem: `testes-data-quality-dbt`.

Todos materializam no catálogo `cidades`, schema `data_quality` (bloco
`seeds.data_quality` do `dbt_project.yml`).

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

## `dominio_status.csv`

`valor_bruto, situacao_canonica, classe` — mapa canônico de situação de obra.
`situacao_canonica ∈ { nao_iniciada, em_obras, paralisada, concluida,
cancelada }`; `classe ∈ { valido, pendente }` (`pendente` = ainda sem acordo
com o negócio, não bloqueia).

**Fonte de verdade do domínio de situação de obra no dbt.** Criado e mantido
pela change `serie-historica-situacao-obra-regiao` — **não** é derivação do
`mapa_status.csv` de `padronizacao-dominio-nulos-duplicados` (change
desatualizada, não será executada sem revisão). Se aquela change ressuscitar, a
reconciliação converge **para** este seed. O levantamento do domínio real e as
decisões de mapeamento estão em
`models/mcmv_historico_dbt/docs/dominio-status-operacional.md`.

Consumido pelas silvers históricas por frente e pelo reloginho via `left join`
sobre `lower(trim(status_operacional)) = lower(trim(valor_bruto))`, e testado
com `dentro_do_dominio` (nível `warn`) sobre a coluna derivada:

```yaml
- name: situacao_canonica
  data_tests:
    - dentro_do_dominio:
        arguments: { seed: dominio_status, seed_column: situacao_canonica }
        config: { severity: warn }
```

## `dominio_regiao_uf.csv`

`uf, regiao_sigla, regiao_nome` — 27 UFs → 5 macrorregiões IBGE
(`N`/`NE`/`CO`/`SE`/`S`). Cópia estável de `mcmv_staging.api_ibge_uf`, para o
eixo histórico fazer o rollup `uf → regiao` sem depender de `source()` MinIO e
permanecer Postgres-válido (D3 da change `serie-historica-situacao-obra-regiao`).
Consumido pelas silvers históricas (por frente, série executiva, reloginho) e
pelos golds de série mensal via `left join` sobre `uf`; testável com
`relationships` sobre `regiao_sigla`.
