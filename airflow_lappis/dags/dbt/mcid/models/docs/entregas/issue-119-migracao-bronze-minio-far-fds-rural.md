# Migração da bronze FAR/FDS/Rural para MinIO/DuckDB

Change OpenSpec: `migracao-bronze-minio-mcmv`. Issue de arquitetura: #119 (medalhão) / ADR #117.

Objetivo: mover os domínios de empreendimento MCMV (FAR, Entidades/FDS, Rural) para a
arquitetura medalhão — bronze como cópia fiel da **staging MinIO** lida via DuckDB, em vez do
schema Postgres `__dados_brutos` (`source raw`). Aplica o padrão de nomenclatura (Opção A) e
cria a frente **Rural** ponta a ponta.

## 1. O que mudou

### Fonte de dados
- Novo source `mcmv_staging` (`models/sources.yml`) — external source dbt-duckdb,
  `meta.external_location: s3://<bucket>/staging/sharepoint/{name}.parquet`. 28 tabelas.
- Source `raw` (schema `__dados_brutos`) **removido** — nenhum modelo dbt o consome mais.
- `dbt parse` validado nos dois targets (`prod` e `staging_duckdb`) — o external source não
  quebra o adaptador Postgres.

### Nomenclatura — Opção A
Formato canônico `<camada>_<token-dominio>_<assunto>[_<recorte>]`, token logo após a camada.
`padrao-nomenclatura-tabelas-dbt.md` §2/§4/§8 atualizados.

| Antigo | Novo |
|---|---|
| `empreendimento_far_dbt/bronze/consolidado` | `bronze_far_consolidado` |
| `.../silver/empreendimento` | `silver_far_empreendimento` |
| `.../gold/ficha_empreendimento` | `gold_far_ficha_empreendimento` |
| `entidades_dbt` (pasta/domínio) | `empreendimento_fds_dbt` (`product: empreendimento_fds`) |
| `entidades_dbt/bronze/fds_int_059_caixa_pj` | `bronze_fds_int059_caixa` |
| `.../silver/fds_empreendimento` | `silver_fds_empreendimento` |
| `.../silver/dim_empreendimento` | `silver_fds_dim_empreendimento` |
| `.../gold/fds_ficha_empreendimento` | `gold_fds_ficha_empreendimento` |
| `.../gold/fds_panorama_entidade` | `gold_fds_panorama_entidade` |
| — (frente nova) | domínio `empreendimento_rural_dbt` (bronze/silver/gold) |

Schemas: todos os modelos migrados passam a materializar nos schemas globais
`bronze` / `silver` / `gold` (via `get_custom_schema` também no DuckDB).

### Portabilidade Postgres → DuckDB
- Macros novas: `macros/normalize_apf.sql`, `macros/parse_date_br.sql`, `parse_hist_numeric`
  (em `macros/parse_hist_numeric.sql`). **Portadas byte a byte** das UDFs Postgres
  (`{{ target.schema }}.normalize_apf` / `.parse_date_br`) e de `parse_financial_value` —
  não "corrigidas": as silvers dependem do comportamento atual para casar as fontes.
  - `normalize_apf`: `626780-03 → 62678003`, `62678003 → 62678003`, `626780 → 00626780`
    (financeiro 6 díg. **não** converge com o 8 díg. — comportamento de produção).
- `to_char` → `strftime`; operador regex `~` + `to_date` → `try_strptime` / `try_cast`
  (inclusive no `indicadores_gargalo_desempenho`).
- Colunas técnicas por bronze (§6): `source_file` (de `arquivo_de_origem`/`_source_file`),
  `dt_ingest` (`_ingested_at`), `hash_linha` (`_source_hash`), `dt_referencia` (do nome do arquivo).

### Cobertura ampliada por agente (aditivo — contrato só cresce, D6)
- **FAR**: `bronze_far_dados_prioritarios_bb`, `bronze_far_dados_prioritarios_snh`,
  `bronze_far_int040_caixa`, `bronze_far_int054_bb`, `bronze_far_trabalho_social`
  (CAIXA+BB `union all by name`), `bronze_far_contratacao` (fonte ~vazia).
- **FDS**: `bronze_fds_entregas` agora CAIXA+BB; novos `bronze_fds_dados_prioritarios` (CAIXA
  Entidades) e `bronze_fds_dados_prioritarios_snh`.
- **Rural**: `bronze_rural_int065_caixa`, `bronze_rural_int057_bb` (standalone — sem APF),
  `bronze_rural_dados_prioritarios_snh`, `bronze_rural_cadastro_pf`.
- `silver_far_empreendimento` e `silver_fds_empreendimento` ganharam 11 colunas `snh_*`
  (snapshot corrente 30/09/2025), **só acrescentadas**.

**INT040/INT054 NÃO entram na silver**: o `nu_apf` dessas interfaces está num espaço de
identificador distinto do cadastro PJ (0/822 casam por `normalize_apf`, 0 por CNPJ).
Materializados como bronze de cópia fiel para linhagem/uso futuro; a integração precisa de
uma tabela de-para de APF (trabalho separado).

## 2. Fronteira com `separacao-silver-historico-por-frente`

A fonte SNH sharepoint (`dados_prioritarios_disponibilizados_snh_empreendimentos`, snapshot
**corrente** 30/09/2025, 21.458 linhas) é **distinta** de
`bronze_mcmv_historico_empreendimento_snh` (série **mensal** de `staging/dados_historicos`,
1,1 M linhas). Cada frente cria `bronze_<frente>_dados_prioritarios_snh` filtrando o arquivo
sharepoint por `modalidade`, reusando o mapa `modalidade → frente_mcmv` da entrega
`separacao-silver-historico-por-frente.md` §2. `silver_far_empreendimento` e
`silver_mcmv_historico_empreendimento_far` são produtos distintos (estado corrente × série
mensal) e não colidem.

## 3. Frente Rural (domínio novo)

`empreendimento_rural_dbt` — 7 bronzes + 2 silvers + 2 golds. `silver_rural_empreendimento`
tem **contrato de saída idêntico** ao `silver_fds_empreendimento` (78 colunas; colunas sem
equivalente no Rural — fase, GPS, trabalho social mensal — saem NULL/default).

Chaves de APF (Rural é heterogêneo — recorte Novo MCMV Rural pequeno):

| Fonte | Chave | Cobertura vs. cad_pj (127) |
|---|---|---|
| `bronze_rural_cadastro_pj` | `normalize_apf(nu_apf_com_dv)` | espinha (127) |
| `bronze_rural_obra_mensal` | `normalize_apf(nu_apf)` | 84/127 |
| `bronze_rural_financeiro_mensal` | raiz de 6 dígitos | 124/127 |
| `bronze_rural_int065_caixa` | `normalize_apf(nu_apf)` | 42/127 (mistura legado PNHR) |
| `bronze_rural_dados_prioritarios_snh` | `normalize_apf(codigo_da_operacao...)` | **127/127** |
| `bronze_rural_int057_bb` | `nu_contrato_empreendimento` (sem APF) | 0 — standalone |

`silver_mcmv_rural_base` reescrito para `ref('silver_rural_empreendimento')` (sem parsing de
separador pipe das INT057/INT065 no schema `sftp`).

## 4. Impacto no pipeline de produção

Todos os modelos migrados nascem `+enabled: "{{ target.type == 'duckdb' }}"`. O Cosmos
(`cosmos_dag.py`) roda só `target=prod`, então:

- **FAR, Entidades, Rural, reloginho, gargalo (grupo B) saem da carga noturna** até o ADR #117
  promover um alvo DuckDB para produção. Dashboards Superset dessas frentes **congelam** no
  intervalo (aceito).
- Carga noturna `prod` fica com: `conjuntura_dbt`, `metadata`, `mcmv_historico_dbt/piloto`.
- `grupo B` (`indicadores_gargalo_desempenho`, `resumo_gargalo_desempenho_dashboard`) depende
  dos golds FAR/FDS por `ref()` → também `+enabled: duckdb`. Comentário do `dbt_project.yml`
  atualizado.

Verificado: `dbt parse --target prod` verde; `dbt ls --target prod` mostra 0 modelos
far/fds/rural/indicadores habilitados; conjuntura/legado sem mudança de schema.

## 5. Reconciliação (staging_duckdb, MinIO real — 2026-09-03)

### Silver por frente

| Frente | Linhas | UH | Valor contratado | Valor desembolsado |
|---|---:|---:|---:|---:|
| FAR (`silver_far_empreendimento`) | 822 | 123.054 | R$ 23,17 bi | R$ 5,69 bi |
| FDS (`silver_fds_empreendimento`) | 335 | 26.175 | R$ 2,87 bi | R$ 0,16 bi |
| Rural (`silver_rural_empreendimento`) | 127 | 4.139 | R$ 0,30 bi | R$ 0,19 bi |

`unique(apf)` verde nas três silvers. `silver_mcmv_{far,entidades,rural}_base` unem sem erro
(822 + 335 + 127).

### Bronze — contagem vs. fonte

| Bronze | Linhas | Fonte |
|---|---:|---|
| `bronze_far_consolidado` | 15.091 | 15.091 ✓ |
| `bronze_far_cadastro_pj` / `_obra_mensal` | 822 | 822 ✓ |
| `bronze_far_financeiro_mensal` | 6.069 | 6.069 ✓ |
| `bronze_far_dados_prioritarios_caixa` | 4.443 | 4.443 FAR ✓ |
| `bronze_fds_int059_caixa` | 856 | 856 ✓ |
| `bronze_fds_entregas` (CAIXA+BB) | 17.482 | 11.540 + 5.942 |
| `bronze_rural_int065_caixa` | 8.508 | 8.508 (legado + novo) |

### Gold / grupo B

| Modelo | Linhas |
|---|---:|
| `gold_far_ficha_empreendimento` / `gold_fds_*` / `gold_rural_*` | 822 / 335 / 127 |
| `gold_far_mapa_nacional` / `gold_far_panorama_estadual` | 32 / 714 |
| `indicadores_gargalo_desempenho` | 1.157 (FAR 822 + FDS 335) |
| `resumo_gargalo_desempenho_dashboard` | 1.211 |

`dbt build` FAR + FDS + Rural + gargalo + bases → **PASS=120, ERROR=0**.
`dbt docs generate` — linhagem `mcmv_staging.* → bronze_* → silver_* → gold_*` confirmada.

Diferença de valor esperada: `parse_hist_numeric` devolve NULL (não `0.00`) para entrada
vazia/`NaN` — não afeta somas (SUM ignora NULL).

## 6. Pendências

1. **Deploy prod** após o ADR #117: promover um alvo DuckDB, dropar o schema legado
   `empreendimento_far` / `entidades_fds` no Postgres, descongelar os dashboards.
2. **De-para de APF** para INT040/INT054/INT057 entrarem nas silvers.
3. `silver_mcmv_frentes_base` completo depende das outras bases (`classe_media`, `cidades`,
   `reforma`, `pro_moradia`, `sub50`) que exigem staging `sftp` — fora do escopo desta change.
4. `seeds/entidades_fds/` (schema do seed `seed_apf_fase_fds`) segue como resíduo menor.
5. Confirmar ausência de consumidor no repositório de dashboards do Superset antes do merge.
