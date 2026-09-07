# Eixo histórico — ordem de build e modos de execução

Domínios cobertos: `mcmv_historico_dbt` (esta pasta) e as bronzes/silvers do
reloginho em `models/indicadores_mcmv_dbt/`.

## As 16 bronzes por família

Desde a change `pipeline-bronze-historica-destino-trocavel` (D5), cada bronze de
série histórica é **uma tabela por família de origem**. O mapa que define nome,
glob e modelo de cada família está em
[`macros/historico/familias.sql`](../../macros/historico/familias.sql); os
corpos ficam em `macros/historico/corpos_bronze.sql`, e cada arquivo em
`bronze/` é uma casca fina que chama o corpo com o nome da família.

| domínio | famílias | tabelas |
|---|---|---|
| série executiva | `bases_relatorio_executivo`, `min_cidades`, `entrada_bb`, `bext` | 4 |
| GEFUS (SFTP) | INT040, INT054, INT057, INT059, INT065 | 5 |
| SNH empreendimento | BB, CAIXA | 2 |
| reloginho entregas | BB, CAIXA | 2 |
| obra mensal (SharePoint) | OBRA_FAR, OBRA_FDS, OBRA_RURAL | 3 |

### Família `obra_mensal` (change `enriquecer-quantidades-uh-e-sinais-obra-historico`)

`MONIT_MOV_OBRA_<FRENTE>_MENSAL_YYYYMM` sob
`staging/sharepoint/Novo MCMV - */` (glob recursivo; frente pela substring **no
nome do arquivo** — os arquivos FDS/RURAL de 202602+ estão misfiled sob
`Novo MCMV - FAR/`). `_LAYOUT_` / `_SEMANAL_` / `_DIARIO_` de fora. Janela real
**202512 → 202607** (não há obra mensal antes disso). Ordem de build:

```
bronze_mcmv_historico_obra_mensal_far
bronze_mcmv_historico_obra_mensal_fds     ->  silver_mcmv_historico_obra_mensal
bronze_mcmv_historico_obra_mensal_rural
```

A silver `silver_mcmv_historico_obra_mensal` (grão `frente_mcmv × apf ×
dt_referencia`, schema `mcmv_historico`) é um **modelo paralelo** — não entra no
`left join` do contrato comum das silvers por frente nesta change (D4). As 3
bronzes têm schemas divergentes (FAR: `dt_movimento` / `co_situacao_obra`;
FDS/RURAL: `dh_movimento` / `co_situacao_operacao`), harmonizados na silver por
`coalesce_present` com lista de aliases (`macros/historico/obra_mensal_arm.sql`).

A **união entre famílias vive na silver**, com projeção explícita por braço:
nenhum modelo usa `union all by name` nem `select * exclude`.

## Ordem de build: as bronzes precisam existir no COMPILE da silver

Isto não é só uma dependência de dados — é uma dependência de **compilação**.

`coalesce_present()` e `coalesce_present_parsed()`
([`macros/coalesce_present.sql`](../../macros/coalesce_present.sql))
introspeccionam a relação no banco (`adapter.get_columns_in_relation`) **no
momento em que a silver é compilada**, para montar o `coalesce` só com as
colunas que aquela família realmente tem. Quem depende disso:

- `silver_mcmv_historico_serie_executiva` → as 4 bronzes da série executiva;
- `silver_mcmv_historico_empreendimento_far` / `_fds` / `_rural` → as 2 bronzes
  SNH (as colunas divergem entre agentes: `uhs_contratadas`/`uhs_entregues` só
  existem no BB, `dt_entrega` só na CAIXA) e as bronzes GEFUS (INT040/054/059/065
  — `qt_unidades_ociosas` / `qtde_uh_inicial` / `cod_pendencia_obra` /
  `pc_execucao_financeira_obra` são resolvidas por `coalesce_present`);
- `silver_mcmv_historico_obra_mensal` → as 3 bronzes `obra_mensal` (schemas
  divergentes por frente).

Consequências práticas:

- **Um `dbt build` numa única invocação já resolve**: o dbt materializa as
  bronzes antes de compilar as silvers que as referenciam.
- **Compilar a silver isoladamente contra um banco vazio não quebra**, mas
  produz `null` no lugar de cada `coalesce` — a macro devolve `null` quando a
  relação não existe. O SQL compila; o resultado é que só vale depois do build
  completo. Por isso `dbt compile`/`--empty` no CI não substituem uma execução
  real como verificação.
- Ao reconstruir **uma família** isoladamente, reconstrua a silver do domínio
  em seguida.

Os scripts `run-historico.sh` e `run-reloginho.sh` já respeitam essa ordem.

## Os três modos de execução (D2)

O corpo de cada modelo é **idêntico nos três**; só o target muda.

| modo | comando | lê | escreve |
|---|---|---|---|
| A — dev | `./run-historico.sh` (`--target staging_duckdb`) | staging MinIO | arquivo local `cidades.duckdb` |
| B — publicação | `./publicar-historico.sh` | arquivo local | Postgres `prod` |
| C — direto | `--target prod_duckdb` | staging MinIO | Postgres `prod` |

Em todos eles o motor DuckDB roda **fora do processo do Postgres** (D1): a
comunicação com o banco é sempre por `ATTACH … (TYPE POSTGRES)`, nunca via
`pg_duckdb`. Desenvolvimento e teste materializam apenas no arquivo local (D7);
contra o `prod` só valem `SELECT` de verificação e as cargas dos modos B/C.

O catálogo se chama `cidades` nos três modos, para que o FQN do nó no
`manifest.json` (`cidades.<schema>.<tabela>`) case com a tabela real que o
OpenMetadata ingere (D3).

## Testes de qualidade

A convenção por camada (bronze detecta / silver contrata / gold reconcilia) e o
catálogo de testes genéricos estão em
[`models/docs/convencao-testes-qualidade.md`](../docs/convencao-testes-qualidade.md)
(change `testes-data-quality-dbt`).

**`verificacao_tipagem` só roda no Postgres** (usa `information_schema`): no
modo A (DuckDB local) o teste é pulado, então a checagem de tipo das colunas
vale só na publicação (modos B/C). Os demais testes de qualidade rodam no modo
A contra o `cidades.duckdb` local.
