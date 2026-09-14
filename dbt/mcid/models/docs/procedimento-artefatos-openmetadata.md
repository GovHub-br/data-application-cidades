# Procedimento — artefatos do conector dbt do OpenMetadata (eixo histórico MCMV)

> Change `catalogo-dicionario-dados-historicos`. Cobre os **33 modelos** do eixo
> histórico (`mcmv_historico_dbt/` sem `piloto/` + `indicadores_mcmv_dbt/`).
> Convenção de schema: spec `arquitetura-schemas-historico`
> (change `consolidar-schemas-historico-reloginho`).

## 1. O que o conector lê

O conector dbt do OpenMetadata (fonte única de metadados do GovHub —
`graphrag-tais` ADR-0004) ingere **três artefatos** de `dbt/mcid/target/`:

| artefato | gerado por | leva ao OpenMetadata |
|---|---|---|
| `manifest.json` | `dbt docs generate` (ou `dbt parse`) | nós, `description`, `tags`, `meta` (`governance` + `openmetadata`), `depends_on` (**linhagem**, inclusive cross-pasta e cross-schema) |
| `catalog.json` | `dbt docs generate` (**introspecta o warehouse**) | tipo de cada coluna, contagem de linhas |
| `run_results.json` | `dbt build` / `dbt test` | status dos `data_tests` por nó |

O FQN de cada nó no `manifest.json` é `<database>.<schema>.<name>`. O
`dbt_project.yml` fixa `+database: cidades` nos três modos de execução — o FQN é
**invariante** entre o modo A (DuckDB de staging) e o Postgres de produção.

## 2. Modo de geração — modo A (DuckDB de staging)

Os 33 modelos **não existem em produção** (introspecção 2026-09-08: no Postgres
`cidades` @ `10.0.0.50`, `dados_historicos` só tem os 2 pilotos que subiram;
`reloginho` e os frente-schemas do histórico não existem lá). O eixo está
materializado **só** em `/mnt/data/duckdb/cidades.duckdb` (modo A). Portanto a
tríade é gerada a partir do build modo A:

```bash
cd dbt/mcid
# pré-requisito: eixo materializado no arquivo local (uma vez)
./run-historico.sh          # bronzes + silvers + golds de mcmv_historico_dbt
./run-reloginho.sh          # schema reloginho (2 bronze + 2 silver + 6 gold) + upstream FAR/FDS

# 1-2. manifest.json + catalog.json (catalog introspecta o cidades.duckdb)
source _run-common.sh
run_dbt docs generate --target staging_duckdb

# 3. run_results.json — status dos data_tests do escopo
run_dbt test --select mcmv_historico_dbt indicadores_mcmv_dbt \
             --exclude "mcmv_historico_dbt.piloto" --target staging_duckdb
```

Os arquivos saem em `dbt/mcid/target/{manifest,catalog,run_results}.json`.

`dbt-duckdb` gera `catalog.json` normalmente — o DuckDB de staging é warehouse
válido para introspecção.

### Quando o eixo migrar para o Postgres de produção

O mesmo procedimento roda contra o prod **sem mudança de conteúdo** — trocar
`--target staging_duckdb` por `--target prod_duckdb` (modo C) ou rodar após a
publicação (modo B, `./publicar-historico.sh`). O FQN `cidades.<schema>.<alias>`
não muda (D3 de `pipeline-bronze-historica-destino-trocavel`), então o
`manifest.json` é o mesmo; só o `catalog.json` passa a introspectar o Postgres.

## 3. Schemas do eixo que o conector deve ingerir

Desde `renomear-camadas-pt-historico-reloginho` (D1): schema por **camada**.

| schema | nós do eixo | observação |
|---|---|---|
| `bronze` | 16 (14 do eixo + 2 de entrega por evento) | `bronze_<origem>_<nome>`, origem ∈ `dhist`/`sftp`/`shpt` |
| `prata` | 7 (`prata_dhist_serie_executiva`, `prata_dhist_entrega_apf`, `prata_{far,fds,rural}_historico_empreendimento`, `prata_dhist_snh_apf_mes`, `prata_dhist_snh_entregas_mes`) | `prata_<domínio>_<nome>` |
| `ouro` | 10 (4 golds cross-frente `ouro_dhist_*` + 6 `ouro_reloginho_*` — reloginho + gargalo) | `ouro_<domínio>_<nome>` |

O nome do modelo == nome da tabela == basename do `.sql`; não há `alias`. Os
schemas `dados_historicos`, `reloginho`, `empreendimento_far`/`_fds`/`_rural`,
`conjuntura`, `mcmv_historico` e `serie_historica` **não recebem** estes braços.

> **Reconciliação pendente.** O FQN dos 33 nós muda
> (`cidades.{dados_historicos,reloginho,empreendimento_*}.<antigo>` →
> `cidades.{bronze,prata,ouro}.<novo>`). Ao publicar em `prod` (modo B) + rodar
> `scripts/migracao/renomear_nomenclatura_prod.sql`, **reingerir o conector dbt
> do OpenMetadata**: a ingestão anterior fica órfã. Ver
> `dbt/mcid/scripts/migracao/README.md` e reconciliar `issue-130-dicionario`.

## 4. Passo de ingestão do conector — Open Question 1

**Não há hoje pipeline automatizado conhecido** de ingestão do conector dbt do
OpenMetadata para este projeto:

- o `.github/workflows/main.yaml` **não** tem passo "dbt docs deploy" (o
  `AGENTS.md` cita um "dbt docs deploy (on `main`, requires VPN secrets)" que não
  está no workflow atual);
- não foi identificado job de Airflow em prod/homolog que consuma
  `manifest.json` / `catalog.json` / `run_results.json` deste repo.

Portanto, **até que exista tal pipeline**, a ingestão é **manual**: gerar a
tríade pelo procedimento da Seção 2 e apontar o conector dbt do OpenMetadata
(config `dbtConfigSource`) para `dbt/mcid/target/`, filtrando os 5 schemas da
Seção 3. Quando o pipeline existir, esta change deve ser estendida para
referenciá-lo; enquanto isso, o procedimento acima é a entrega.

## 5. Verificação — evidências 2026-09-08 (modo A)

Após `dbt docs generate` + `dbt test` (manifest/catalog `generated_at`
2026-09-08T20:55Z):

- **33 nós em escopo** no `manifest.json` (20 `dados_historicos` + 10
  `reloginho` + 3 frente-schemas); FQN `cidades.<schema>.<alias>` conferido.
- **0 colunas sem `description`** nos 33 nós (baseline G14 era 66 em 8 modelos).
- **0 nós sem `meta.governance` + `meta.openmetadata`** (baseline G9 era 10 —
  todo o `indicadores_mcmv_dbt`; agora `domain: MCid.Habitacao`,
  `owner: mcid-data-engineering`, `product: reloginho`, tier bronze=3 /
  silver=2 / gold=1).
- **33/33 nós no `catalog.json`** com tipos de coluna (o de 2026-09-06 tinha o
  layout de schema antigo e faltavam 6 modelos).
- **Linhagem cross-pasta / cross-schema** presente no `depends_on`:
  - `ouro_reloginho_indicadores_gargalo_desempenho` → `gold_far_ficha_empreendimento`,
    `gold_fds_ficha_empreendimento`, `silver_fds_empreendimento`,
    `silver_far_evolucao_financeira`, `gold_atual_evolucao_financeira_chart`,
    `gold_atual_execucao_fisica_financeira_chart` (todos com `description` de
    camada 0 — nenhuma pendência a registrar);
  - `prata_dhist_snh_apf_mes` → `bronze_dhist_empreendimento_snh_bb`
    / `_snh_caixa` (via `ref()` dinâmico) + `dominio_status` + `dominio_regiao_uf`;
  - `prata_dhist_entrega_apf` → `reloginho.bronze_dhist_snh_entregas_evento_bb`
    / `_caixa` (aresta **cross-schema** `dados_historicos` → `reloginho`).
- **`run_results.json`**: `PASS=519 WARN=46 ERROR=1`. O único `fail`,
  `assert_reloginho_frente_cobertura_mensal`, é **esperado** — lacuna da FONTE
  (snapshots `historico_recente_*` da SNH faltam meses no MinIO), não erro de
  build. O OpenMetadata deve tratar esse teste como known-issue para não
  alarmar. Os 46 `warn` são os soft-checks de DQ conhecidos.
