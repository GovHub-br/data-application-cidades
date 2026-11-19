# Migração da nomenclatura em produção — `bronze` / `prata` / `ouro`

Runbook do rename das tabelas do **eixo histórico** e do **reloginho/gargalo** já
publicadas no Postgres `prod` na convenção anterior (schema por domínio) para a
convenção nova (**schema por camada em português**), da change
`renomear-camadas-pt-historico-reloginho`.

> **Execução MANUAL.** Nenhum script deste diretório é chamado por
> `publicar_historico.py`, `publicar-historico.sh`, `run-*.sh`,
> `rebuild-local.sh` ou hooks `on-run-*` do dbt. A aplicação da change **só cria
> os arquivos** — o rename contra `prod` é ato do operador, fora da change.

## O que faz

`ALTER TABLE … RENAME TO …` + `ALTER TABLE … SET SCHEMA …` para as **29 tabelas**
do inventário do modo B (`scripts/publicar_historico.py`, listas `BRONZES_SERIE`
+ `SILVERS_GOLDS` na versão **pré-change**; o piloto OGU/FGTS está comentado lá e
fica de fora). É renomeação de **metadado** — instantânea, não copia dados.

O mapa canônico old→new é `mapa_nomenclatura.csv` (única fonte de verdade; também
alimentou a reescrita de `publicar_historico.py`). Os 3 `.sql` são **gerados** a
partir dele por `gerar_migracao.py` e commitados; o operador usa só `psql`.

| script | o quê | idempotente? |
|---|---|---|
| `verificar_nomenclatura_prod.sql` | read-only: inventário dos 8 schemas + `count(*)` por tabela, nomes antigos e novos | sim |
| `renomear_nomenclatura_prod.sql` | forward: schemas antigos → `bronze`/`prata`/`ouro` | não (transação única) |
| `reverter_nomenclatura_prod.sql` | rollback: o inverso exato | não (transação única) |

Cada `.sql` roda numa **transação única** (`BEGIN`/`COMMIT`) com `ON_ERROR_STOP`,
com **preflight** (`RAISE EXCEPTION` se alguma origem não existe, se algum destino
já existe, ou se a contagem de origens ≠ 29) e **postflight** (cada destino
existe, cada origem sumiu). Renomeia **antes** de mover de schema — as 3 silvers
por frente compartilham o nome `silver_historico_empreendimento`.

## Pré-requisitos

1. Os schemas `bronze`, `prata`, `ouro` **já existem** em `prod` (schemas globais).
2. `publicar_historico.py` **já reescrito** para os nomes novos (feito nesta
   change) — para que cargas futuras publiquem direto no lugar certo.
3. As 29 tabelas do inventário **estão** em `prod` na convenção antiga
   (última publicação modo B: 2026-09-08). Rodar `verificar` confirma.
4. Janela combinada com os consumidores externos (Superset / OpenMetadata /
   notebooks) que referenciam as tabelas por string de nome — **eles quebram**
   com o rename.

## Ordem

```bash
export DSN="host=$DB_DW_HOST_MCID port=${DB_DW_PORT_MCID:-5432} \
dbname=${DB_DW_DBNAME_MCID:-cidades} user=$DB_DW_USER_MCID password=$DB_DW_PASSWORD_MCID"

# 1. estado ANTES
psql "$DSN" -f scripts/migracao/verificar_nomenclatura_prod.sql | tee /tmp/verif_antes.txt

# 2. rename (transação única; aborta inteiro em qualquer erro)
psql "$DSN" -v ON_ERROR_STOP=1 -f scripts/migracao/renomear_nomenclatura_prod.sql

# 3. estado DEPOIS — diffar contra o de antes (mesmas contagens, novos nomes)
psql "$DSN" -f scripts/migracao/verificar_nomenclatura_prod.sql | tee /tmp/verif_depois.txt
diff /tmp/verif_antes.txt /tmp/verif_depois.txt

# 4. reingestão do conector dbt do OpenMetadata (FQN dos 29 nós mudou)
```

## Rollback

```bash
psql "$DSN" -v ON_ERROR_STOP=1 -f scripts/migracao/reverter_nomenclatura_prod.sql
psql "$DSN" -f scripts/migracao/verificar_nomenclatura_prod.sql
```

As tabelas antigas **nunca são dropadas** por estes scripts — o `reverter` só
desfaz os `ALTER`. Se a migração falhar no meio, a transação já reverteu tudo.

## Regenerar os `.sql`

Ao editar `mapa_nomenclatura.csv` (modelos) ou `mapa_seeds.csv` (seeds):

```bash
python3 scripts/migracao/gerar_migracao.py        # renomear/reverter/verificar_nomenclatura_prod.sql
python3 scripts/migracao/gerar_migracao_seeds.py  # migrar/reverter/verificar_seeds_prod.sql
```

---

## Migração das seeds → schema `seeds`

Complementa o rename dos modelos: as **10 seeds** consumidas pelos braços
histórico e reloginho/gargalo (9 de `data_quality` + o piloto OGU/FGTS de
`conjuntura`) deixam os schemas antigos e passam a materializar no schema único
`seeds`. As seeds **não mudam de nome** — só de schema (`ALTER TABLE … SET
SCHEMA`, sem RENAME), então a migração é mais simples que a dos modelos.

O mapa é `mapa_seeds.csv` (única fonte de verdade). Os 3 `.sql` são gerados por
`gerar_migracao_seeds.py`:

| script | o quê | idempotente? |
|---|---|---|
| `verificar_seeds_prod.sql` | read-only: inventário de `data_quality`/`conjuntura`/`seeds` + `count(*)` por seed | sim |
| `migrar_seeds_prod.sql` | forward: `data_quality`/`conjuntura` → `seeds` | não (transação única) |
| `reverter_seeds_prod.sql` | rollback: `seeds` → `data_quality`/`conjuntura` | não (transação única) |

Mesma estrutura dos scripts de modelo: transação única com preflight (cada
origem existe, cada destino não existe) e postflight. Pré-requisitos: o schema
`seeds` já existe em `prod`; o `dbt_project.yml` já aponta as seeds para `seeds`.

```bash
# 1. estado ANTES
psql "$DSN" -f scripts/migracao/verificar_seeds_prod.sql | tee /tmp/verif_seeds_antes.txt

# 2. move (transação única; aborta inteiro em qualquer erro)
psql "$DSN" -v ON_ERROR_STOP=1 -f scripts/migracao/migrar_seeds_prod.sql

# 3. estado DEPOIS — diffar contra o de antes (mesmas contagens, novo schema)
psql "$DSN" -f scripts/migracao/verificar_seeds_prod.sql | tee /tmp/verif_seeds_depois.txt
diff /tmp/verif_seeds_antes.txt /tmp/verif_seeds_depois.txt
```

Rollback: `psql "$DSN" -v ON_ERROR_STOP=1 -f scripts/migracao/reverter_seeds_prod.sql`.
