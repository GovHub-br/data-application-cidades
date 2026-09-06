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

## Tipagem (`verificacao_tipagem`)

Só roda no **Postgres** (usa `information_schema`). No modo A (DuckDB local) é
pulado. A checagem de tipo vale na publicação (modos B/C) — fora do escopo da
change `testes-data-quality-dbt` (D7/D9).

## Severidade — como declarar

```yaml
- completude_minima:
    arguments: { min_pct: 0.90 }
    config: { severity: warn }   # ou error
```
