# Issue #130 — Refatoração do Reloginho em Camadas Medalhão + Cobertura Histórica por Frente

> Data: 2026-09-02. Branch `feat/tratamento-dados-historicos`.
> Objetivo: (1) quebrar os indicadores do reloginho (grupo A) em bronze → silver →
> gold para aderência à arquitetura medalhão; (2) verificar se as frentes FAR,
> Entidades e Rural têm linha histórica mensal completa.
> Contexto: `models/docs/arquitetura-medalhao-mcid.md` e
> `models/docs/entregas/issue-130-aderencia-arquitetura-medalhao-reloginho.md`.

---

## Parte 1 — Refatoração em 3 camadas

### Antes

`indicadores_mcmv_dbt/gold/indicadores_reloginho.sql` era um Gold único que fazia
ingestão (glob de parquet no MinIO), tipagem, deduplicação por APF e regra de
negócio num só arquivo. Sem bronze, sem silver.

### Depois

```text
staging/dados_historicos/*ecente_*snh_pmcmv_dados_prioritarios_af_*.parquet   (MinIO)
        │  read_minio_staging_parquet_series (DuckDB)
        ▼
bronze_reloginho_snh_serie_mensal      ← cópia fiel; 1 linha por linha de origem;
        │                                colunas da fonte preservadas; texto;
        │                                dt_referencia do nome do arquivo;
        │                                multi-mês empilhado; sem dedup; sem regra
        ▼
silver_reloginho_snh_apf_mes           ← tipagem (texto→bigint/date); domínio
        │                                (agente BB/CAIXA, frente_mcmv canônica
        │                                FAR/Entidades/Rural); dedup por APF
        │                                (grão: agente × apf × mês)
        ├───────────────┐
        ▼               ▼
indicadores_reloginho   indicadores_reloginho_frente   ← só regra de negócio
   (agente × mês)          (agente × frente × mês)         (soma mensal, n_apf,
        │                                                   meses observados)
        ▼
resumo_reloginho_dashboard
```

### Arquivos

| Arquivo | Tipo | Papel |
|---|---|---|
| `models/indicadores_mcmv_dbt/bronze/bronze_reloginho_snh_serie_mensal.sql` | novo | Bronze — cópia fiel da série mensal SNH (CAIXA + BB) |
| `models/indicadores_mcmv_dbt/bronze/schema.yml` | novo | Docs + testes `not_null` (source_file, hash_linha) |
| `models/indicadores_mcmv_dbt/silver/silver_reloginho_snh_apf_mes.sql` | novo | Silver — tipado, domínio normalizado, dedup por APF |
| `models/indicadores_mcmv_dbt/silver/schema.yml` | novo | Docs + testes (`not_null`, `accepted_values` agente/frente) |
| `models/indicadores_mcmv_dbt/gold/indicadores_reloginho.sql` | refatorado | Lê a silver; **saída idêntica** à versão anterior |
| `models/indicadores_mcmv_dbt/gold/indicadores_reloginho_frente.sql` | novo | Gold por frente (grão agente × frente × mês) |
| `models/indicadores_mcmv_dbt/gold/schema.yml` | mod | Nova entrada + descrição atualizada da `indicadores_reloginho` |
| `tests/indicadores_mcmv_dbt/assert_reloginho_silver_grain_unique.sql` | novo | Grão da silver = (agente, apf, mês) |
| `tests/indicadores_mcmv_dbt/assert_reloginho_frente_cobertura_mensal.sql` | novo | Série mensal contínua por (agente, frente) |
| `dbt_project.yml` | mod | Config `bronze:`/`silver:` do `indicadores_mcmv_dbt` com gating DuckDB; regating explícito dos golds do reloginho |

Testes preexistentes mantidos e ainda válidos (a saída do Gold não mudou):
`assert_reloginho_grain_unique`, `assert_reloginho_reconcilia_66`.

### Decisões

1. **Materialização continua em DuckDB (`staging_duckdb`), nas 3 camadas.** É onde
   os parquets são legíveis. Promover bronze/silver para tabelas no Postgres
   `cidades` (aderência plena — Bronze no banco) depende do ADR #117 e de um job de
   carga parquet→Postgres; fica como follow-up. O ganho imediato é a **separação de
   responsabilidades** (cada modelo com uma função só).
2. **Schema único `mcmv_indicadores`** para as 3 camadas (igual ao Gold atual).
   Separar em `mcmv_indicadores_bronze` / `_silver` (padrão `conjuntura_dbt`) é
   trivial depois.
3. **Grão do Gold `indicadores_reloginho` inalterado** `(agente_financeiro,
   dt_referencia)` — preserva a reconciliação #66 e o `resumo_reloginho_dashboard`
   (que espera 1 linha por agente/mês). A quebra por frente vai na
   `indicadores_reloginho_frente`, separada.
4. **Dedup por APF idêntica à anterior**: `row_number()` por `(agente_financeiro,
   apf, dt_referencia)`, `rn = 1`. Ordenação ganhou `prioridade_reentrega`
   (correção > vsNN > base) para o mês com reentrega (BB 2024-07); nos demais meses
   o resultado é bit a bit o mesmo.
5. **`agente_financeiro` vem da coluna da fonte** (não do nome do arquivo) para não
   recuperar linhas que a versão anterior descartava — a reconciliação exata
   depende disso. O agente inferido do nome fica em `bronze.agente_arquivo` para
   auditoria.
6. **Glob ampliado** de `historico_recente_*` para
   `*ecente_*snh_pmcmv_dados_prioritarios_af_*` (excluindo `%entrega%`): passa a
   capturar as variantes truncadas do BB (`storico_recente_2024_07…`,
   `ecente_2024_07…_correcao`). CAIXA não tem truncamento → série CAIXA idêntica.
7. **Fluxos de entrega por evento continuam fora** (`o_recente_*_entregas`,
   `*_entrega_da_unidade_af_b`): o reloginho usa o acumulado `uh_entregues` do
   próprio snapshot (decisão D6 da #130). São fonte de um modelo futuro separado.

### Validação executada

- `dbt parse --target staging_duckdb` — OK (só deprecations YAML preexistentes).
- `dbt compile --select bronze_reloginho_snh_serie_mensal+` — OK, linhagem
  bronze → silver → 2 golds → resumo resolvida.
- `dbt ls` — 4 modelos + testes reconhecidos.
- **Pendente** (requer credencial MinIO): `dbt run --target staging_duckdb --select
  bronze_reloginho_snh_serie_mensal+` e `dbt test --select
  indicadores_mcmv_dbt,tag:reloginho`. Espera-se:
  `assert_reloginho_reconcilia_66` PASS (CAIXA 2026-03 = 1.697.630 / 1.391.909),
  `assert_reloginho_silver_grain_unique` PASS,
  `assert_reloginho_frente_cobertura_mensal` — ver Parte 2.

### Como rodar

```bash
cd airflow_lappis/dags/dbt/mcid
export MINIO_ENDPOINT=... MINIO_ACCESS_KEY=... MINIO_SECRET_KEY=... MINIO_BUCKET=data-lake-mcid
export DUCKDB_MCID_PATH=/tmp/mcid_staging.duckdb
dbt run  --target staging_duckdb --select bronze_reloginho_snh_serie_mensal+
dbt test --target staging_duckdb --select bronze_reloginho_snh_serie_mensal+
```

---

## Parte 2 — Cobertura histórica das frentes FAR, Entidades e Rural

### Fonte e método

A série do reloginho vem dos snapshots mensais **`historico_recente_*` de dados
prioritários SNH** (um arquivo por agente por mês). Cada arquivo traz a coluna
`modalidade` com a frente: `FAR`, `Entidades`, `RURAL`/`Rural`.

O que dá para verificar **offline** (inventário de arquivos do dump tratado,
`data-science/dados-historicos-tratamento/data/dados_historicos_formatados/`): quais
meses têm arquivo por agente. O que **só o `dbt run`** confirma: se cada frente tem
linhas em todo mês em que o arquivo existe (as amostras locais têm teto de 200
linhas, insuficiente para afirmar presença/ausência de uma frente num mês).

Janela de referência (#66): **2024-06 a 2026-03 — 22 meses**.

### Inventário de arquivos por mês (verificado)

| Fonte | Arquivos | Meses ausentes na janela |
|---|---:|---|
| `historico_recente_*_af_caixa` (contratação, carrega FAR+Entidades+Rural) | 21/22 | **2024-08** |
| `historico_recente_*_af_bb` (contratação, carrega FAR+Rural) | 16/22 | **2024-08, 2024-09, 2024-12, 2025-02, 2025-04, 2025-11** |
| `o_recente_*_af_caixa_entregas` (fluxo de entrega — não usado pelo reloginho) | 21/22 | 2025-03 |
| `*_entrega_da_unidade_af_b` (fluxo de entrega BB — não usado) | 15/22 | 2024-10, 2025-02, 2025-12, 2026-01 |

### Resultado por frente

| Frente | Agente(s) na série SNH | Cobertura máxima possível | Lacunas conhecidas |
|---|---|---|---|
| **FAR** | CAIXA + BB | CAIXA 21/22, BB 16/22 | Nenhum mês tem os 2 agentes só quando falta arquivo. **2024-08 falta nos dois** → mês sem FAR na série SNH. BB: 6 meses sem arquivo. |
| **Rural** | CAIXA (`RURAL`) + BB (`Rural`) | igual ao FAR (mesmos arquivos) | Mesmas lacunas de arquivo. Além disso: **inconsistência de rótulo** `RURAL` (CAIXA) vs `Rural` (BB) — resolvida na silver (`frente_mcmv = 'Rural'`). |
| **Entidades (FDS)** | **CAIXA apenas** (BB não tem `Entidades` em `historico_recente_*`) | ≤ 21/22 (só meses CAIXA) | Nunca terá linha BB. Falta 2024-08 (sem arquivo CAIXA). Presença de `Entidades` em cada mês CAIXA: **a confirmar no `dbt run`** (amostras de 200 linhas não permitem afirmar). O `issue-130-dicionario-indicadores.md` registra que a SNH cobre FDS de 2024-06 em diante. |

### Conclusão

**Nenhuma das três frentes tem linha histórica mensal 100% completa na janela
2024-06…2026-03 pela série SNH isoladamente:**

- **2024-08** é um buraco geral (não há `historico_recente_*` de nenhum agente) →
  afeta FAR, Entidades e Rural.
- **BB tem 6 meses sem arquivo** → FAR-BB e Rural-BB ficam intermitentes.
- **Entidades depende só da CAIXA** e da presença efetiva de linhas `Entidades` em
  cada snapshot mensal (a verificar).
- A **série de entregues por frente anterior a 2024-06** existe para **FAR e Rural**
  via `historico_mcmv_empreendimentos_snapshot` (GEFUS/SFTP, mensal 2019-12+); a
  **FDS/Entidades não tem entregues no GEFUS** (fica só com a SNH 2024-06+).
  Ver cobertura GEFUS em `issue-130-implementacao-modelos-historicos-empreendimentos.md`
  (FAR e Rural até 2024-11; Entidades/FDS até 2026-06).

### O que fica automatizado

`assert_reloginho_frente_cobertura_mensal` roda a cada `dbt test` e retorna, por
`(agente_financeiro, frente_mcmv)`, se a série mensal tem **buracos internos**
(mês faltando entre o primeiro e o último observado). A matriz completa
mês × frente × agente sai de:

```sql
select agente_financeiro, frente_mcmv, dt_referencia, n_apf, uh_contratadas, uh_entregues
from mcmv_indicadores.indicadores_reloginho_frente
order by 1, 2, 3;
```

### Recomendações

1. Rodar `dbt run` + `dbt test` da linhagem com credencial MinIO e anexar a matriz
   `indicadores_reloginho_frente` como evidência de negócio (Fase 5/6 da #130).
2. Tratar **2024-08** e os **meses BB ausentes** como período incompleto declarado
   (não como “zero contratação”) — mesma diretriz do hiato OGU 2020-2023.
3. Para o reloginho por frente com histórico longo, **unir** `indicadores_reloginho_frente`
   (SNH 2024-06+) com uma agregação mensal de `historico_mcmv_empreendimentos_snapshot`
   (GEFUS 2019-12+, FAR/Rural) — follow-up, fora deste change.
4. Confirmar com a área se a frente **Entidades** deve aparecer no reloginho sem
   contraparte BB (é estrutural: FDS é CAIXA).
