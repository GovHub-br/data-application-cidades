# Entrega — enriquecer-datas-acompanhamento-historico (B + D)

Change OpenSpec `enriquecer-datas-acompanhamento-historico`. Implementada e
testada local em 2026-09-06 (target `staging_duckdb`, `/mnt/data/duckdb/cidades.duckdb`).

## O que mudou

### B — espinha de entregas por APF

`models/mcmv_historico_dbt/silver/prata_dhist_entrega_apf.sql` (schema
`mcmv_historico`, grão `apf`). Lê `bronze_dhist_snh_entregas_evento_bb` /
`_caixa` (a mesma fonte que o reloginho já ingere — o reloginho segue consumindo
em paralelo), deduplica os eventos por hash de conteúdo **idêntico** ao de
`prata_dhist_snh_entregas_mes` e agrega por APF:

| coluna | conteúdo |
|---|---|
| `dt_primeira_entrega` / `dt_ultima_entrega` | menor / maior `dt_evento` do APF |
| `uh_entregues_acumulada` | soma das UH de todos os eventos deduplicados |
| `n_eventos` | contagem de eventos deduplicados |
| `dt_ultimo_snapshot` | maior `dt_referencia` de snapshot em que o APF apareceu |

Resultado do build: **11.741 APFs**, `uh_entregues_acumulada` somada =
**1.517.820** — reconcilia exatamente com `prata_dhist_snh_entregas_mes`
(`assert_entrega_apf_reconcilia_reloginho`, diff = 0). Eventos com `dt_evento`
nulo: **0** nos dois lotes (taxa de descarte 0 %).

Cobertura do `left join` por `apf` contra as silvers por frente:

| frente | APFs | casam na espinha |
|---|---|---|
| FAR | 5.506 | 3.666 (67 %) |
| FDS | 1.021 | 372 (36 %) |
| Rural | 11.129 | 7.703 (69 %) |

### Split semântico `dt_entrega` → `dt_entrega_uh` + `dt_conclusao_obra` (BREAKING)

Nas 3 silvers por frente e no `ouro_dhist_snapshot_empreendimento_atual`. A coluna
única `dt_entrega` misturava "entrega de UH" (FAR ← `dt_ultima_entrega`) com
"conclusão de obra" (Rural ← `dt_efetiva_conclusao`); FDS era 100 % NULL.

| frente | `dt_entrega_uh` (braço SFTP) | `dt_conclusao_obra` (braço SFTP) |
|---|---|---|
| FAR | `dt_ultima_entrega` (INT040/054) | `coalesce(dt_termino_obra, dt_legalizacao)` |
| FDS | — (NULL; INT059 é escopo da change A/C) | — (NULL) |
| Rural | — (NULL; INT065 é escopo da change A/C) | `dt_efetiva_conclusao` (INT057/065) |

`dt_entrega_uh` final = `coalesce(<braço da linha>, <mesmo valor preservado ao
longo do grão>, espinha.dt_ultima_entrega)`. `quantidade_uh_entregues` idem, mas
o `coalesce` **só age sobre NULL** — um `0` explícito é informação e não é
sobrescrito. `dt_entrega_uh_fonte` registra a origem
(`sftp:INT040` / `sftp:INT054` / `sftp` / `snh:entrega_evento` / NULL).

### Cobertura antes → depois

| frente | `dt_entrega_uh` (linhas) | teto da série | `dt_conclusao_obra` |
|---|---|---|---|
| FAR | 49,5 % → **81,2 %** | 2024‑11‑29 → **2026‑03‑30** | 55,8 % |
| FDS | **0 % → 53,0 %** | — → **2026‑01‑23** | 0 % (A/C) |
| Rural | 66,1 % → **80,3 %** | 2024‑11‑29 → **2026‑03‑31** | 66,1 % |

O congelamento do feed SFTP em 2024‑11 (INT040/054/057/065 pararam;
só INT059 segue) deixou de travar o eixo de entrega — a espinha SNH cobre a
janela 2024‑02 → 2026‑07.

### D — `ouro_dhist_marco_empreendimento`

`models/mcmv_historico_dbt/gold/ouro_dhist_marco_empreendimento.sql` (schema
`dados_historicos` (era `serie_historica`), grão `coalesce(id_empreendimento, apf)` — FDS multi‑fase
colapsa em 1 linha). 7 marcos, cada um com `<marco>_fonte` e `<marco>_dt_snapshot`:

| marco | fonte |
|---|---|
| `dt_contratacao` / `dt_inicio_obra` | linha "estado" vencedora (fase mais avançada → `dt_referencia` mais recente) |
| `dt_conclusao_obra` / `dt_ultima_entrega` | **maior valor observado em qualquer snapshot** do empreendimento (a data pode ter sido reportada e sumido do feed — quer‑se a última conhecida); `dt_ultima_entrega` considera ainda o max da espinha sobre todos os APFs de fase |
| `dt_primeira_entrega` | espinha (`snh:entrega_evento`) |
| `dt_legalizacao` | `sem_fonte` nesta fase |
| `dt_previsao_entrega` | `sem_fonte` nesta fase — Open Question 1 (lean); a change A/C projeta `data_da_previsao_da_entrega` e preenche |

Build: **17.552 empreendimentos** —
`dt_ultima_entrega` preenchida em 11.778 (FAR 3.703 · Rural 7.703 · FDS 372),
`dt_conclusao_obra` em 12.901 (FAR 3.971 · Rural 8.930 · FDS 0),
`dt_primeira_entrega` em 11.741. Fonte de `dt_ultima_entrega`:
`snh:entrega_evento` 8.184 · `sftp:INT040` 3.456 · `sftp:INT054` 138.

`marcos_coerentes` (booleano): a cadeia `contratação ≤ início ≤ conclusão` e
`início ≤ 1ª entrega ≤ última entrega` se sustenta (ignorando nulos). É
**diagnóstico** — o modelo não corrige datas. **188 (1 %)** incoerentes;
0 com `dt_ultima_entrega < dt_primeira_entrega`.

O modelo **só seleciona** entre valores observados — nunca interpola.

## Open Questions — resolvidas (todas "lean")

1. `dt_previsao_entrega` no gold nasce `NULL` (`_fonte` = `sem_fonte`). A change
   `destravar-datas-obra-entrega-silver-historico` (A/C) preenche.
2. Rural `dt_entrega_uh`: só a espinha. INT065 `dt_ultima_entrega` (11 %) fica
   para a A/C.
3. `ouro_dhist_marco_empreendimento` e `ouro_dhist_snapshot_empreendimento_atual` coexistem —
   o snapshot não foi refatorado para ler os marcos.
4. Colunas de proveniência planas (`<m>_fonte` texto, `<m>_dt_snapshot` date).

## Fora de escopo (ficou para outras changes)

- Destravar o INT059 do FDS e projetar as datas SNH cruas
  (`data_da_previsao_da_entrega`, `data_de_termino`, `data_de_contratacao_fds_fase_*`)
  → `destravar-datas-obra-entrega-silver-historico` (A + C).
- Testes de validade de data (ano > 2000, ordenação entre marcos, freshness por
  coluna) → `testes-data-quality-dbt` (Fase 5). Aqui só a flag `marcos_coerentes`
  como metadado.
- Séries de andamento de obra não modeladas (GEHIS/DIEMP `ANDAMENTO_OBRA`,
  `MCidades_AO_3`).

## Nota sobre o cruzamento entrega × situação (FDS)

Um cruzamento fino "transição de status → `concluida` tem sinal de entrega em
±3 meses" ainda fica limitado no FDS (~9 % numa medição grosseira por mês): a
espinha dá **um** `dt_ultima_entrega` por APF, não a série mensal de acumulado.
O sinal mensal do FDS depende do `quantidade_uh_entregues` do INT059 — escopo da
change A/C. FAR e Rural ficam em ~63‑67 % nessa métrica.

## Consumidores externos — pendente (manual)

Varrer exports do Superset e o lineage do OpenMetadata por `dt_entrega` apontando
para `prata.prata_{far,fds,rural}_historico_empreendimento` ou `dados_historicos.ouro_dhist_snapshot_empreendimento_atual`
e repontar para `dt_entrega_uh` (não há mais `dt_entrega` nessas relações).
