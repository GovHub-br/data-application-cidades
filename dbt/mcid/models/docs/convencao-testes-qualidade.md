# Convenção — testes de qualidade por camada

Change: `testes-data-quality-dbt`. Biblioteca em `macros/data_quality/`.
Política: **sem pacotes dbt** (`dbt_utils`/`dbt_expectations`/`elementary`) —
todo teste é macro caseira `test_<nome>`.

## Princípio: cada camada testa uma coisa diferente

```
bronze  →  DETECÇÃO   — o dado é cópia fiel; o teste só sinaliza, não gateia
silver  →  CONTRATO   — grão, domínio, completude obrigatória; aqui pode gatear
gold    →  RECONCILIAÇÃO — contagem, somatório, unicidade da chave de negócio
```

### Bronze — detecção (`warn`)

O bronze não corrige nada (D5/D6 da arquitetura medalhão). Um defeito de origem
não é regressão — então o teste é `severity: warn`, para dar visibilidade sem
travar o build.

| teste | pega |
|---|---|
| `sem_mojibake` | `São` lido como `SÃ£o` nos valores |
| `sem_sufixo_float_texto` | código inteiro que virou `"3550308.0"` |
| `not_null` (auditoria) | `source_file`, `hash_linha` nulos |
| `unique` / `unique_combinacao` | `hash_linha` duplicado |

### Silver — contrato (`error`, salvo completude informativa)

A silver é o dado tratado. O contrato é gate: falhou, o build para.

| teste | uso |
|---|---|
| `unique_combinacao` | o grão declarado (`apf`, `dt_referencia`, `frente_mcmv`) |
| `not_null` | chaves e campos sempre obrigatórios |
| `completude_minima` | campo de negócio com ausência parcial tolerada — `error` se está no `campos_obrigatorios.csv`, senão `warn` |
| `accepted_values` / `dentro_do_dominio` | domínio pequeno e estável inline; domínio grande/versionado via seed |
| `sem_sufixo_float_texto` | na coluna já limpa por `strip_float_text` — `error` (falha = regressão da limpeza) |

### Gold — reconciliação (`error`)

| teste | uso |
|---|---|
| `row_count_match` | contagem origem × destino |
| `unique` / `unique_combinacao` | uma linha por empreendimento / por mês |
| teste singular | somatórios que não podem regredir entre versões |

### Coerência de séries acumuladas (`warn`)

Change: `enriquecer-quantidades-uh-e-sinais-obra-historico` (D7). Dois testes
que **só listam** — não filtram, não quarentenam, não alteram a materialização:

| teste | args | pega |
|---|---|---|
| `acumulado_nao_regride` | `partition_by`, `order_by` | coluna de acumulado (`quantidade_uh_entregues`, `quantidade_uh_concluidas`, `valor_desembolsado`) que **cai** vs. a observação anterior da mesma partição |
| `quantidade_nao_excede_referencia` | `reference`, `fator` (default 1.0) | `column > reference * fator` linha-a-linha (ex.: `quantidade_uh_entregues > quantidade_uh`); só compara valores presentes (NULL nunca dispara) — espelha `desembolso_nao_excede_contratado` |

Ambos `severity: warn` nesta change (sem threshold de erro). Aplicados nas
silvers históricas por frente (`prata_{far,fds,rural}_historico_empreendimento`).
Contagens esperadas no build local: ~1 k excedências e ~13 k regressões no FAR.

## Tipagem (`verificacao_tipagem`)

Teste de coluna: compara `information_schema.columns.data_type` da coluna
materializada com um `tipo_esperado` (string exata). Resolve a tabela pelo
relation do modelo (`model.schema` / `model.identifier`).

Change `verificar-tipagem-silver-gold-historico` (2026-09-07): os `tipo_esperado`
nos `schema.yml` de `mcmv_historico_dbt` e `indicadores_mcmv_dbt` usam os nomes
do **DuckDB** (`BIGINT`, `VARCHAR`, `DECIMAL(15,2)`, `DOUBLE`, `DATE`,
`TIMESTAMP WITH TIME ZONE`) — o teste roda e passa no **modo A** (build local).
Referência dos tipos canônicos: `mcmv_historico_dbt/docs/inventario-tipagem-silver-gold.md`.

Numa materialização no Postgres (modos B/C) os nomes de `data_type` mudam
(`bigint`, `character varying`, `numeric`, `double precision`, ...); os
`tipo_esperado` precisam de uma revisão nessa ocasião. Como
`mcmv_historico_dbt` / `indicadores_mcmv_dbt` não estão em prod hoje, a versão
DuckDB é a útil.

## Severidade — como declarar

```yaml
- completude_minima:
    arguments: { min_pct: 0.90 }
    config: { severity: warn }   # ou error
```
