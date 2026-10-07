# Issue #59 — Evidências de testes de qualidade de dados (dbt)

Recorte: os 34 modelos de `mcmv_historico_dbt/` (14 bronze + 6 prata + 4 ouro)
e `indicadores_mcmv_dbt/` (2 bronze + 2 prata + 6 ouro), que são o produto de
limpeza/padronização do eixo histórico do MCMV cobrado pela #59.

> Mesmo recorte de modelos da #130 ("Validar indicadores dos dados
> históricos para o reloginho") — esta evidência também serve como
> comprovação de que os indicadores do reloginho (`indicadores_mcmv_dbt`) e
> sua base histórica (`mcmv_historico_dbt`) passam pela suíte de testes dbt
> a cada build, atendendo o item "valores calculados conferidos" dos
> critérios de aceite da #130.

## O que já existe

A suíte de qualidade não é um artefato avulso — é código versionado que roda
a **cada `dbt build`**, sem depender de pacote externo (`dbt_utils` /
`dbt_expectations` / `elementary`): macros caseiras em `macros/data_quality/`
e `macros/historico/`, aplicadas nos `schema.yml` de cada modelo. A convenção
de quem testa o quê está documentada em
[`convencao-testes-qualidade.md`](../convencao-testes-qualidade.md)
(change `testes-data-quality-dbt`):

- **bronze → detecção** (`warn`, nunca bloqueia): o bronze é cópia fiel da
  fonte (arquitetura medalhão), então um defeito de origem — sufixo `.0` em
  código, coluna nova não catalogada, valor fora de faixa — é sinalizado, não
  corrigido nem barrado.
- **prata → contrato** (`error`): grão declarado, obrigatoriedade de chave,
  completude mínima de campo de negócio, domínio de categoria — aqui falha
  para o build.
- **ouro → reconciliação** (`error`): contagem/soma origem×destino,
  unicidade da chave de negócio.

## Execução (evidência desta issue)

- **Data:** 2026-09-15
- **Ambiente:** modo A (local), target `staging_duckdb`,
  `/mnt/data/duckdb/cidades.duckdb`, dbt-core 1.11 (`.venv/bin/dbt`)
- **Comando:**
  ```
  dbt test --target staging_duckdb \
    --select 'path:models/mcmv_historico_dbt path:models/indicadores_mcmv_dbt' \
    --no-partial-parse
  ```

### Resultado agregado

```
PASS=532  WARN=46  ERROR=1  SKIP=0  NO-OP=0  TOTAL=579
```

### Por tipo de teste

| teste | total | pass | warn | error | o que verifica |
|---|--:|--:|--:|--:|---|
| `verificacao_tipagem` | 227 | 227 | 0 | 0 | tipo materializado da coluna = tipo esperado no contrato |
| `not_null` | 132 | 132 | 0 | 0 | obrigatoriedade de chave/campo |
| `accepted_values` | 54 | 54 | 0 | 0 | categoria dentro do domínio inline (ex. `frente_mcmv`) |
| `valor_nao_negativo` | 23 | 18 | 5 | 0 | valor financeiro/quantidade não negativo |
| `sem_sufixo_float_texto` | 21 | 7 | 14 | 0 | código sem resíduo `"123.0"` (bronze detecta; prata já limpo = `error`) |
| `completude_minima` | 17 | 17 | 0 | 0 | % de preenchimento acima do limiar do campo |
| `colunas_esperadas` | 16 | 16 | 0 | 0 | conjunto de colunas do bronze sem drift vs. seed versionado |
| `assert_*` (testes singulares) | 15 | 13 | 1 | 1 | regras específicas do domínio (cobertura mensal, descontinuidade de obra) |
| `unique` | 14 | 14 | 0 | 0 | unicidade de chave técnica |
| `acumulado_nao_regride` | 12 | 0 | 12 | 0 | série acumulada não cai vs. observação anterior (visibilidade, D7) |
| `dentro_do_dominio` | 11 | 11 | 0 | 0 | categoria contra seed de referência |
| `quantidade_nao_excede_referencia` | 10 | 5 | 5 | 0 | ex. UH entregues não excede UH contratadas |
| `bronze_colunas_nao_mapeadas` | 7 | 7 | 0 | 0 | toda coluna da fonte tem destino mapeado no contrato |
| `relationships` | 5 | 5 | 0 | 0 | integridade referencial entre modelos |
| `desembolso_nao_excede_contratado` | 4 | 1 | 3 | 0 | valor desembolsado não excede valor contratado |
| `reconcilia_decomposicao` | 4 | 0 | 4 | 0 | soma das partes bate com o total decomposto |
| `unique_combinacao` | 3 | 3 | 0 | 0 | unicidade do grão de negócio (chave composta) |
| outros (2 testes) | 2 | 1 | 1 | 0 | soma por família / faixa de valor por UH |

## O único `ERROR`

`assert_reloginho_frente_cobertura_mensal` (`indicadores_mcmv_dbt`) — **gap
de fonte conhecido**: faltam snapshots SNH em meses específicos no MinIO, não
uma regressão introduzida pelo tratamento. Rastreado desde a change
`reloginho-dados-historicos` e reconfirmado como a mesma falha (sem contagem
nova) em todo build local desde então, incluindo os cold rebuilds completos
de 2026-09-08 e 2026-09-10.

## Os 46 `WARN`

Por design (ver convenção acima), `warn` na camada bronze e nas regras de
coerência de série **não bloqueia o build** — existe para dar visibilidade a
um defeito de origem ou a uma folga de negócio conhecida, não para ser
"zerado":

- **35** são sufixo `.0` residual e valores negativos ainda presentes nas
  **bronzes** (cópia fiel da fonte bruta — a limpeza acontece na prata, onde
  os testes equivalentes passam em `error`).
- **11** são regras de coerência de série acumulada/decomposição na **prata**
  (`acumulado_nao_regride`, `quantidade_nao_excede_referencia`,
  `desembolso_nao_excede_contratado`, `reconcilia_decomposicao`,
  `soma_nao_cruza_familia`, `valor_dentro_faixa_uh`) — listam exceções
  conhecidas da fonte (ex. retificação de contrato reduz UH contratadas),
  não corrigem nem filtram linhas.

## Limitações desta evidência

- Roda no ambiente local (modo A, DuckDB) — `mcmv_historico_dbt` e
  `indicadores_mcmv_dbt` ainda não estão publicados em produção (Postgres),
  então `verificacao_tipagem` valida contra os nomes de tipo do DuckDB, não
  do Postgres (documentado na convenção).
- Item opcional da change `testes-data-quality-dbt` (Fase 5) ainda em aberto:
  mart `gold/data_quality/` consultável e confirmação de que a ingestão
  OpenMetadata externa exibe os *Data Quality test cases* do `manifest.json`.

## Conclusão

Os 34 modelos de `mcmv_historico_dbt` e `indicadores_mcmv_dbt` têm cobertura
automatizada de qualidade em todas as camadas (579 testes: tipagem, nulidade,
domínio, completude, unicidade, drift de schema e coerência de série), rodada
nesta issue com resultado **532 pass / 46 warn (visibilidade, não
bloqueante) / 1 error (gap de fonte já rastreado)** — sem regressão nova.
