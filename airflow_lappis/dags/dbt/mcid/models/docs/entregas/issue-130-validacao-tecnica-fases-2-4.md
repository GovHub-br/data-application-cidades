# Issue #130 - Validacao Tecnica (Fases 2-4): Cobertura, Regras e Calculos

## Resumo

Este documento consolida as Fases 2, 3 e 4 da issue #130, executadas com acesso
direto ao banco `cidades` (VPN habilitada). Foram mapeadas a cobertura historica,
validadas as regras de calculo e executados os calculos em amostra, comparando com
os valores de referencia registrados nos docs da #66.

## Fase 2 - Cobertura Historica

### Materializado no banco `cidades` (contagens)

| Schema | Tabela | Linhas | Observacao |
|---|---|---:|---|
| mcmv_silver | silver_mcmv_frentes_base | 223.545 | consolidado por frente |
| mcmv_silver | silver_mcmv_minha_casa_minha_vida_base | 223.498 | |
| mcmv_silver | silver_mcmv_classe_media_base | 110.979 | |
| mcmv_silver | silver_mcmv_reforma_base | 100.859 | |
| mcmv_silver | silver_mcmv_rural_base | 9.474 | |
| mcmv_silver | silver_mcmv_far_base | 1.646 | |
| mcmv_silver | silver_mcmv_entidades_base | 343 | |
| mcmv_silver | silver_mcmv_cidades_base | 197 | |
| mcmv_silver | silver_mcmv_conjuntura_base | 47 | |
| mcmv_silver | silver_mcmv_pro_moradia_base | 0 | lacuna |
| mcmv_silver | silver_mcmv_sub50_base | 0 | lacuna |
| empreendimento_far | empreendimento / ficha_empreendimento | 1.646 | |
| empreendimento_far | dados_prioritarios_caixa | 8.886 | |
| entidades_fds | fds_empreendimento / fds_ficha_empreendimento | 343 | |
| entidades_fds | fds_financeiro_mensal | 16.124 | serie financeira |
| empreendimento_rural | empreendimento_rural | 10.402 | |
| conjuntura_gold / _silver | (23 modelos) | 1..1469 | |

### NAO materializado (lacunas de camada)

| Schema esperado | Modelo | Situacao |
|---|---|---|
| mcmv_indicadores | indicadores_gargalo_desempenho + resumo_* | materializado depois (1.989 linhas) — ver secao Fase 2-4 |
| mcmv_indicadores | reloginho (bronze/silver/gold) | criado em 2026-09-02 (`dbt compile` OK; `dbt run` pendente de credencial MinIO) |
| mcmv_historico | historico_mcmv_serie_temporal_snapshot (piloto #118) | AUSENTE (seed-based; aguarda staging dados_historicos) |
| mcmv_metas | (tabela de metas oficial) | NAO EXISTE |

Consequencia (na data da Fase 2): os indicadores de gargalo (grupo B) e o piloto
historico estavam definidos em codigo dbt mas **nao materializados**. Atualizacao:
o gargalo foi materializado (ver Fase 2-4) e o reloginho (grupo A) foi
implementado em camadas medalhao (ver
`issue-130-refatoracao-medalhao-reloginho.md`).

### Cobertura temporal por fonte

| Fonte | Granularidade | Periodo | Campos | Recorte |
|---|---|---|---|---|
| Piloto #118 (seed) | anual | 2009-2025 | contratadas (OGU/FGTS) | nacional, 2 linhas |
| `dados_historicos.historico_recente_*` | mensal | 2024-06 a 2026-03 | contratadas + entregues | CAIXA+BB, por APF/UF/municipio |
| `__dados_brutos.dados_prioritarios_recebidos_*` | snapshot | 2026-01-31 | contratadas + entregues + vigentes | CAIXA+BB, por APF |
| `historico_mcmv_empreendimentos_snapshot` (GEFUS/SFTP) | mensal | 2019-12 em diante | contratadas + entregues (FAR/Rural); contratadas (FDS, entregues NULL) | FAR/FDS/Rural, grao empreendimento x mes |

Achado relevante: **existe serie mensal de contratadas E entregues de 2024-06 a
2026-03** (22 meses), por APF/UF/municipio. Isso viabiliza `ritmo_medio_mensal`,
`ritmo_necessario` e `projecao_entrega` a partir de 2024-06 — contrariando a
premissa inicial de que nao havia serie mensal. Alem disso, o modelo GEFUS
(`historico_mcmv_empreendimentos_snapshot`) estende a serie mensal de entregues
para **FAR e Rural desde 2019-12**; ver a "Ressalva GEFUS" abaixo.

### Lacunas sinalizadas

1. **Hiato OGU/Subsidiado 2020-2023**: zeros na serie anual (ausencia real vs dado
   nao coletado - a classificar com a area).
2. **Sem serie mensal antes de 2024-06 na fonte SNH**: a fonte SNH
   (`historico_recente_*`) so cobre 2024-06+; o GEFUS
   (`historico_mcmv_empreendimentos_snapshot`) estende a serie mensal para 2019-12
   (ver "Ressalva GEFUS").
3. **Serie de entregues antes de 2024-06 restrita ao GEFUS (FAR/Rural)**: na fonte
   SNH os entregues so aparecem na janela 2024-06+ e no snapshot; o GEFUS tem
   entregues mensais desde 2019-12 para FAR/Rural (FDS sem entregues), ainda sem
   validacao em amostra (ver "Ressalva GEFUS").
4. **Meta oficial ausente**: nao ha tabela de metas; so a "meta visual" 2.214.810.
5. **Pro-Moradia e SUB50/FNHIS com 0 registros** na silver (fonte nao materializada).

### Ressalva GEFUS (serie mensal 2019-12+, ainda NAO validada em amostra)

O modelo `historico_mcmv_empreendimentos_snapshot` (SFTP/GEFUS, MinIO `staging/`)
estende a cobertura de entregues para FAR e Rural desde 2019-12, mas **nao passou
pela validacao em amostra** que foi aplicada a fonte SNH na Fase 4. Ressalvas:

1. **FDS (Entidades) sem entregues no GEFUS**: `quantidade_uh_entregues` e NULL na
   interface INT059 (o campo de entregues nao esta mapeado nessa fonte). A FDS
   tambem nao traz UF (`null`) nem `dt_entrega`. Para a FDS, a serie de entregues
   continua limitada a SNH 2024-06+.
2. **Qualidade ainda nao consolidada**: typos de coluna (`sg_uf_muncicipio`,
   `no_empreeendmento`), coluna temporal inconsistente no INT057 (`idt_movimento` vs
   `dt_movimento`), `qt_unidades` vazio em algumas linhas do INT057 e reentregas
   (`_0000`, `_V2`) + arquivos `VALIDACAO` excluidos por filtro ad-hoc (a
   canonicalizacao definitiva via `_canonicas.csv` e a correcao de encoding/mojibake
   seguem pendentes, P1/P2).
3. **INT065 no grao empreendimento (carta de credito individual)**: ~8x mais
   empreendimentos PNHR que o INT057, com media de 23 UH; sem ajuste necessario,
   mas exige cuidado na agregacao de contratadas/entregues da frente Rural.
4. **Conclusao**: o GEFUS e fonte candidata para a serie historica de entregues do
   reloginho (FAR/Rural), mas so deve ser considerado oficial apos (a) validacao em
   amostra contra os valores de referencia da #66 e (b) conclusao da canonicalizacao
   (P2). Ate la, a fonte SNH (2024-06+) permanece a unica validada empiricamente.

## Fase 3 - Regras de Calculo

### Regra de deduplicacao (CONFIRMADA como critica)

Todas as tabelas de "dados prioritarios" (CAIXA e BB) tem **2 linhas identicas por
APF** (duplicacao de carga):

| Tabela | Linhas | APFs distintos | Fator |
|---|---:|---:|---:|
| dados_prioritarios_recebidos_caixa_empreendimentos | 29.846 | 14.923 | 2x |
| dados_prioritarios_recebidos_bb_empreendimentos | 2.576 | 1.288 | 2x |

Sem deduplicacao, `sum(uh_contratadas)` DOBRA. Regra validada: `distinct on (apf)`
(ou dedup por APF/contrato, como ja sinalizado na #66). A coluna `uh_contratadas`
e `bigint` (valores limpos); o problema e a duplicacao de linhas, nao o formato.

### Regras do reloginho (grupo A)

- `perc_meta_contratada` = `uh_contratadas / uh_meta_total`.
- `perc_meta_entregue` = `uh_entregues / uh_meta_total`.
- `gap_uh_meta` = `uh_meta_total - uh_entregues`.
- `ritmo_medio_mensal` = entregas acumuladas / meses observados (viavel a partir da
  serie mensal 2024-06+).
- `ritmo_necessario` / `projecao_entrega` / `status_relogio` dependem da meta
  oficial e do termino do ciclo (pendentes).

### Regras de gargalo (grupo B)

Regras e limiares documentados (90 dias, 10 p.p., 30%/95%, 365/180 dias, pesos
2/2/1/1/1/1/1, faixas 0/1-2/3-4/>=5). A validacao empirica da distribuicao exige
materializar o gold (`dbt run --select indicadores_mcmv_dbt`).

### Materializacao e teste do gargalo (grupo B) — limitacao de duplicacao FAR

O gold foi materializado e testado (`dbt run` + `dbt test --select indicadores_mcmv_dbt`):

| Modelo | Linhas |
|---|---:|
| `mcmv_indicadores.indicadores_gargalo_desempenho` | 1.989 |
| `mcmv_indicadores.resumo_gargalo_desempenho_dashboard` | 1.161 |

Resultado dos testes: 7 PASS e 1 FAIL — `unique_indicadores_gargalo_desempenho_id_indicador`
(823 linhas duplicadas). **Causa raiz:** a fonte FAR `ficha_empreendimento` tem
**duplicacao 2x exata por APF** (1.646 linhas / 823 APFs distintos; os 823 pares sao
identicos, 0 divergentes). A cadeia nao deduplica por APF em nenhum nivel:

`dados_prioritarios_caixa` (bronze, 2x) -> `empreendimento` (silver) -> `ficha_empreendimento`
(gold) -> `indicadores_gargalo_desempenho`.

A frente FDS esta limpa (343 linhas / 343 APFs), ou seja, nao e um problema do
trabalho APF x fases. **Impacto:** os totais FAR do gargalo (`quantidade_uh`,
`valor_contratado`, `score` e o `resumo_*`) ficam dobrados — mesma duplicacao 2x ja
identificada nas tabelas de dados prioritarios (Fase 3).

**Decisao registrada:** nao corrigir agora; levar como limitacao conhecida aos colegas.
A correcao cabe na camada FAR (dedup por APF no bronze/silver) ou no proprio gargalo.

## Fase 4 - Calculos em Amostra + Comparacao

### Totais deduplicados (snapshot 2026-01-31) vs referencia (30/06/2026)

| Indicador | DB dedup (jan/2026) | Referencia #66 (jun/2026) | Situacao |
|---|---:|---:|---|
| uh_contratadas CAIXA | 1.689.610 | 1.706.861 | consistente (crescimento esperado) |
| uh_contratadas BB | 167.762 | 167.762 | igual |
| **uh_contratadas CAIXA+BB** | 1.857.372 | 1.874.623 | +17k em 5 meses (coerente) |
| uh_entregues CAIXA+BB | 1.534.167 | 1.543.432 | +9k em 5 meses (coerente) |
| uh_vigentes CAIXA | 287.814 | 313.884 (CAIXA+BB) | coerente |
| perc_meta_contratada | 83,9% | 84,64% | coerente |
| perc_meta_entregue | 69,3% | 69,69% | coerente |

Os valores deduplicados do banco (jan/2026) sao consistentes com a referencia
(jun/2026), com o crescimento de ~5 meses. Isso valida: (a) a regra de dedup, e
(b) os valores de referencia da #66.

### Serie mensal CAIXA (deduplicada), 2024-06 a 2026-03

| Mes | Contratadas | Entregues |
|---|---:|---:|
| 2024-06 | 1.483.886 | 1.359.712 |
| 2024-09 | 1.527.689 | 1.362.721 |
| 2024-12 | 1.575.463 | 1.365.758 |
| 2025-03 | 1.603.930 | 1.367.686 |
| 2025-06 | 1.644.494 | 1.371.366 |
| 2025-09 | 1.670.442 | 1.374.230 |
| 2025-12 | 1.688.773 | 1.382.462 |
| 2026-03 | 1.697.630 | 1.391.909 |

Leitura: contratadas cresceram ~214k em 21 meses (~10k/mes); entregues cresceram
~32k (~1,5k/mes). O `ritmo_medio_mensal` de entregas observado e ~1,5k/mes, muito
abaixo do que seria necessario para fechar 2.214.810 (gap ~831k) ate o fim do
ciclo 2026 - o que reforca o alerta "meta em risco" que o reloginho deve apontar.

### Amostra de validacao (cross-check dos ponteiros)

| Fonte | Total contratadas (dedup) | Nota |
|---|---:|---|
| CAIXA (dados_prioritarios, jan/2026) | 1.689.610 | 14.923 APFs |
| BB (dados_prioritarios, jan/2026) | 167.762 | 1.288 APFs |
| Referencia #66 (CAIXA+BB, jun/2026) | 1.874.623 | 84,64% da meta |

## Conclusao

1. **Cobertura**: o reloginho e viavel a partir de junho/2024 (serie mensal de
   contratadas + entregues, por APF/UF/municipio) e de 2009-2025 (serie anual de
   contratadas OGU/FGTS). O modelo GEFUS estende a serie mensal de entregues para
   FAR e Rural desde 2019-12, mas ainda sem validacao em amostra (ver "Ressalva
   GEFUS").
2. **Deduplicacao**: obrigatoria (2x duplicacao em todas as tabelas prioritarias).
   Confirmada como regra de negocio de primeiro nivel.
3. **Regras**: formulas do reloginho validadas; limiares de gargalo documentados
   (aguardam materializacao do gold para validacao empirica).
4. **Valores**: os calculos deduplicados batem com a referencia da #66, com o
   crescimento esperado entre jan e jun/2026.

## Pendentes (bloqueios para fechar as fases 2-4)

- Materializar `mcmv_historico` (piloto #118) e rodar `dbt run`/`dbt test` da
  linhagem do reloginho (bronze/silver/gold) com credencial MinIO. O gargalo
  (`mcmv_indicadores`) ja foi materializado (ver Fase 2-4).
- Definir a meta oficial (desbloqueia perc_meta, gap, ritmo_necessario, projecao,
  status_relogio).
- Definir o termino do ciclo MCMV 2023-2026 (para "meses restantes").
- Classificar o hiato OGU 2020-2023 (ausencia real vs dado nao coletado).
- Validar em amostra a serie GEFUS 2019-12+ (FAR/Rural) contra a referencia #66 e
  concluir a canonicalizacao/encoding (P1/P2) antes de adota-la como fonte oficial
  de entregues do reloginho.
