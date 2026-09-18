# Entrega — `id_empreendimento` no eixo histórico (FDS/Entidades)

Change OpenSpec: `id-empreendimento-eixo-historico`. Build/teste local em
`staging_duckdb` (`/mnt/data/duckdb/cidades.duckdb`), 2026-09-06.

## O que mudou

A identidade estável de empreendimento FDS (`id_empreendimento`, da
`prata_fds_dim_empreendimento` do #130 — hash do APF-âncora / Fase Projeto)
passou a alcançar a série histórica e os golds de série/snapshot. Antes o eixo
histórico contava **APF**, e um empreendimento FDS multi-fase
(Projeto → Obra → Desligamento, ~19% dos empreendimentos) aparecia 2–3×.

- `prata_fds_historico_empreendimento`: novas colunas `id_empreendimento`
  e `fase_empreendimento` (join na dim por `apf`; fallback
  `md5('empreendimento-fds|' || apf)` para APF fora da dim).
  `codigo_empreendimento` repontado de `nu_apf` para `coalesce(id_empreendimento,
  apf)` (alinha com `silver_mcmv_entidades_base`). `apf`,
  `id_historico_snapshot` e `id_negocio_historico` continuam por APF físico.
- `prata_far_historico_empreendimento` / `_rural`: `id_empreendimento = apf`,
  `fase_empreendimento = NULL` (contrato comum; o APF já é o empreendimento).
- `ouro_dhist_serie_situacao_mensal`: `n_empreendimentos` e o `lag()` de transições
  passam a usar `chave_empreendimento = coalesce(id_empreendimento, apf)`.
  Colapso mensal com precedência de fase Desligamento > Obra > Projeto.
- `ouro_dhist_snapshot_empreendimento_atual`: 1 linha por `(frente, codigo_empreendimento)`;
  no FDS a fase mais avançada vence, depois `dt_referencia`. FAR/Rural inalterados.
- `mcmv_silver_empty_contract`: `+ fase_empreendimento` (as `*_base` já tinham).
- Teste novo `assert_fase_taxonomia_consistente_jan_abr` (vacuous hoje — seed é
  100% `ABR26`).

## Cobertura

- `prata_fds_historico_empreendimento`: 53.442 linhas · **1.021 APF**
  distintos · **992 (97,2%)** casam na dim → só ~29 APFs no fallback `md5`.
- Colapso: **1.021 APF → 917 `id_empreendimento`** (−104, −10,2%). 207 APFs em
  grupos multi-APF.
- `fase_empreendimento` por linha: Projeto 61,8% · Obra 32,2% · Desligamento
  2,7% · NULL 3,3%.

## Antes / depois

| Métrica | Antes | Depois |
|---|---|---|
| `ouro_dhist_snapshot_empreendimento_atual` — Entidades | 1.021 | **917** |
| `ouro_dhist_snapshot_empreendimento_atual` — FAR | 5.506 | 5.506 |
| `ouro_dhist_snapshot_empreendimento_atual` — Rural | 11.129 | 11.129 |
| `ouro_dhist_serie_situacao_mensal` Σ`n_empreendimentos` Entidades 2024 (nacional) | 6.557 | **6.073** |
| … 2025 | 9.469 | **8.630** |
| … 2020–2022 | 6.613 | 6.612 |

Consistência `nacional == Σuf` para `n_empreendimentos` / `entradas` / `saidas`:
**999/999** grãos batem.

## Exemplo multi-fase

`id_empreendimento = 27aa8f72903bd64b94e5b572a94201fe`:

| APF | fase | janela na série |
|---|---|---|
| 42668484 | Projeto | 2020-07 |
| 45593968 | Obra | 2019-12 → 2026-06 |
| 61984964 | Desligamento | 2026-02 → 2026-03 |

Antes: 3 "empreendimentos" no painel mensal. Depois: 1, com a situação da fase
mais avançada em cada mês.

## Correção retroativa de fase (`CORREÇÃO_FASE_PROJETO`)

Seed curado `seeds/entidades_fds/seed_correcao_fase_projeto.csv` (176 APFs
Entidades reclassificados de "EM ANDAMENTO" → "FASE PROJETO" em 2026-04-30, do
xlsx GEFUS). `prata_fds_dim_empreendimento` faz `left join` sobre `apf` e
expõe `fase_corrigida` / `dt_correcao` / `origem_correcao` **ao lado** da
`fase_empreendimento` — não sobrescreve (D4). 176/1.093 linhas com correção;
**1 APF** em que os dois discordam (`fase_empreendimento = Obra`,
`fase_corrigida = Projeto`), agora visível para auditoria. Vira `source`/bronze
quando a fonte entrar na staging MinIO.

## Fronteira com `situacao_canonica`

`fase_empreendimento` (Projeto/Obra/Desligamento) é a **fase administrativa do
APF** — muda quando o empreendimento troca de APF. `situacao_canonica`
(nao_iniciada/em_obras/paralisada/concluida/cancelada — change
`serie-historica-situacao-obra-regiao`) é o **estado físico da obra** reportado
no tempo. São dimensões distintas; nunca combinar numa mesma série sem ressalva.

## Pendente

- **`CORREÇÃO_FASE_PROJETO`**: hoje é seed curado. Migrar para `source`/bronze
  de cópia fiel quando a fonte entrar na staging MinIO.
- `assert_fase_taxonomia_consistente_jan_abr` só terá efeito quando o extrato
  `JAN26` entrar no `seed_apf_fase_fds`.
- Contrato: as `silver_mcmv_*_base` explícitas (far/rural/…) já têm
  `fase_empreendimento`; o macro foi alinhado. Se `silver_mcmv_frentes_base`
  (consolidado, hoje fora do DAG) voltar, revalidar a posição da coluna.
