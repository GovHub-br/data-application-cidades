# Eixo histórico — ordem de build e modos de execução

Domínios cobertos: `mcmv_historico_dbt` (esta pasta) e as bronzes/pratas do
reloginho em `models/indicadores_mcmv_dbt/`.

## Convenção de schema (change `renomear-camadas-pt-historico-reloginho`, D1)

Schema por **camada do medalhão, em português** — reverte a D1 de
`consolidar-schemas-historico-reloginho` (schema por domínio × frente):

| camada | schema | nome de tabela |
|---|---|---|
| bronze | `bronze` | `bronze_<origem>_<nome>` — origem ∈ `dhist` (`staging/dados_historicos/`), `sftp` (`staging/sftp/`), `shpt` (`staging/sharepoint/`) |
| prata | `prata` | `prata_<domínio>_<nome>` — domínio ∈ `historico`, `far`, `rural`, `fds`, `reloginho` |
| ouro | `ouro` | `ouro_<domínio>_<nome>` |

Vale para os 33 modelos dos dois braços (`mcmv_historico_dbt` exceto `piloto/` +
`indicadores_mcmv_dbt`), cross-frente, por frente ou de gargalo. Nada mais
materializa em `dados_historicos`, `reloginho`, nos schemas de frente
(`empreendimento_far` etc.) nem em `conjuntura`. Os schemas `mcmv_historico` e
`serie_historica` seguem extintos. O nome do arquivo `.sql` == nome do modelo ==
nome da tabela; sem `config(alias=…/schema=…)` por modelo. O invariante D3
(`+database: cidades`) é preservado, então o FQN é `cidades.<bronze|prata|ouro>.<tabela>`.

O piloto OGU/FGTS (`prata_dhist_serie_anual_ogu_fgts`) está `enabled=false` nesta
branch — renomeado ao alvo mas não materializado (D4).

Modelos fundidos por `consolidar-schemas-historico-reloginho` (não são tabela
própria): `dim_empreendimento_historico` → 14 colunas de
`ouro_dhist_snapshot_empreendimento_atual`; o modelo de fluxo YTD autônomo → 9
colunas YTD das 3 pratas de frente (left join); o modelo de obra mensal autônomo
→ braço de criação de linha + left join das 22 colunas de obra nas 3 pratas.

> **Migração das tabelas já em `prod`** (publicadas na convenção antiga):
> `scripts/migracao/` — `mapa_nomenclatura.csv` + `renomear_nomenclatura_prod.sql`
> (manual, fora da aplicação da change). Reconciliação do conector dbt do
> OpenMetadata pendente: o FQN dos 33 nós muda.

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
**202512 → 202607** (não há obra mensal antes disso). Ordem de build: as 3
bronzes `obra_mensal` antes das 3 pratas de frente.

Desde `consolidar-schemas-historico-reloginho` (D2/C2) **não há mais um modelo
de obra mensal autônomo**. A família entra nas 3 pratas de frente:
- um braço mínimo no `union all by name` de `unioned` **cria linha** nos meses
  só-de-obra (2026-04..07, `fonte_serie = 'obra_mensal'`) — sobe o teto do eixo
  de 2026-03 para 2026-07;
- as **22 colunas de obra** entram por `left join` no grão `(frente_mcmv, apf,
  dt_referencia)` num CTE `enriquecido_obra`
  (`macros/historico/obra_mensal_arm.sql`: `historico_obra_mensal_rows()` /
  `_vals()` / `historico_obra_enriquecido()`).

Nos meses só-de-obra `quantidade_uh` / `valor_contratado` / `valor_desembolsado`
ficam **NULL** (cauda de estoque declaradamente nula, C2 — sem carry-forward).
Os 3 ouros filtram `fonte_serie <> 'obra_mensal'`. Consumidor que agrega estoque
por mês deve filtrar `fonte_serie <> 'obra_mensal'` ou `dt_referencia <= '2026-03-01'`.

As 3 bronzes têm schemas divergentes (FAR: `dt_movimento` / `co_situacao_obra`;
FDS/RURAL: `dh_movimento` / `co_situacao_operacao`), harmonizados por
`coalesce_present` com lista de aliases.

Os braços SFTP/SNH das pratas de frente ainda usam projeção explícita por braço;
o `union all by name` do CTE `unioned` só serve para o braço obra completar as
colunas do contrato com NULL sem repetir a lista inteira.

## Ordem de build: as bronzes precisam existir no COMPILE da prata

Isto não é só uma dependência de dados — é uma dependência de **compilação**.

`coalesce_present()` e `coalesce_present_parsed()`
([`macros/coalesce_present.sql`](../../macros/coalesce_present.sql))
introspeccionam a relação no banco (`adapter.get_columns_in_relation`) **no
momento em que a prata é compilada**, para montar o `coalesce` só com as
colunas que aquela família realmente tem. Quem depende disso:

- `prata_dhist_serie_executiva` → as 4 bronzes da série executiva;
- `prata_far_historico_empreendimento` / `_fds` / `_rural` → as 2 bronzes
  SNH (as colunas divergem entre agentes: `uhs_contratadas`/`uhs_entregues` só
  existem no BB, `dt_entrega` só na CAIXA), as bronzes GEFUS
  (INT040/054/059/065 — `qt_unidades_ociosas` / `qtde_uh_inicial` /
  `cod_pendencia_obra` / `pc_execucao_financeira_obra` por `coalesce_present`) e
  as 3 bronzes `obra_mensal` (braço de criação de linha + left join das 22 col).

Consequências práticas:

- **Um `dbt build` numa única invocação já resolve**: o dbt materializa as
  bronzes antes de compilar as pratas que as referenciam.
- **Compilar a prata isoladamente contra um banco vazio não quebra**, mas
  produz `null` no lugar de cada `coalesce` — a macro devolve `null` quando a
  relação não existe. O SQL compila; o resultado é que só vale depois do build
  completo. Por isso `dbt compile`/`--empty` no CI não substituem uma execução
  real como verificação.
- Ao reconstruir **uma família** isoladamente, reconstrua a prata do domínio
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

A convenção por camada (bronze detecta / prata contrata / ouro reconcilia) e o
catálogo de testes genéricos estão em
[`models/docs/convencao-testes-qualidade.md`](../docs/convencao-testes-qualidade.md)
(change `testes-data-quality-dbt`).

**`verificacao_tipagem`** (change `verificar-tipagem-silver-gold-historico`)
confere o `data_type` das colunas-chave (id, UH, valor R$, data, percentual)
das pratas e ouros contra o tipo canônico documentado em
[`docs/inventario-tipagem-silver-gold.md`](docs/inventario-tipagem-silver-gold.md).
Os `tipo_esperado` usam os nomes do DuckDB, então o teste **roda no modo A**
(`cidades.duckdb` local); numa ida a Postgres os nomes de `data_type` mudam e os
valores esperados precisam de revisão.
