# Domínio real de `status_operacional` — série histórica por frente + reloginho

Levantamento da tarefa 1.1 da change `serie-historica-situacao-obra-regiao`.

**Fonte:** as 4 silvers já materializadas em `/mnt/data/duckdb/cidades.duckdb`
(build de `run-historico.sh` + `run-reloginho.sh`, 2026-09-05):

| silver | linhas | janela | linhas sem `status_operacional` |
|---|---:|---|---:|
| `empreendimento_far.silver_historico_empreendimento` | 342.977 | 2019-12 → 2026-03 | 1.140 |
| `empreendimentos_fds.silver_historico_empreendimento` | 53.442 | 2019-12 → 2026-06 | 0 |
| `empreendimento_rural.silver_historico_empreendimento` | 736.830 | 2019-12 → 2026-03 | **59.275** (8.470 APF) |
| `reloginho.silver_historico_snh_apf_mes` | 307.731 | 2024-06 → 2026-03 | 0 |

`silver_mcmv_historico_serie_executiva` (2012-04 → 2018-08, 10,16 M linhas) **não
possui** `status_operacional` — ver D2 (`situacao_derivada`).

## Domínio por frente (contagem de linhas · APF distintos)

### FAR — 34 grafias

| valor cru | linhas | APF |
|---|---:|---:|
| `CONCLUIDA_COM_VLR_A_LIBERAR` | 181.250 | 3.514 |
| `CONCLUÍDO E ENTREGUE` | 64.265 | 3.664 |
| `CONCLUIDA` | 28.750 | 1.574 |
| `EM ANDAMENTO` | 14.311 | 948 |
| `CANCELADA/DISTRATADA` | 12.497 | 234 |
| `CONCLUÍDO E ENTREGUE SEM PENDÊNCIAS` | 10.341 | 3.448 |
| `ATRASADA_LIB_ACIMA_90` | 7.209 | 442 |
| `NORMAL` | 6.497 | 841 |
| `OBRA_RETOMADA_COM_SUPL_APORTE` | 3.299 | 143 |
| `NAO_INICIADA` | 3.254 | 245 |
| `PARALISADO_SOLICITANDO_VLR_ADICIONAL` | 2.889 | 173 |
| `PARALISADA` | 1.543 | 39 |
| *(vazio/NULL)* | 1.140 | 24 |
| `VENDIDO_RJ` | 999 | 18 |
| `PARALISADO` | 947 | 82 |
| `DISTRATADO/CANCELADO` | 831 | 99 |
| `Concluído e Entregue` | 825 | 165 |
| `ATRASADA` | 521 | 30 |
| `DESIMOBILIZADO` | 441 | 38 |
| `ADIANTADA` | 245 | 13 |
| `EM ATENÇÃO` | 206 | 70 |
| `OBRA FISICA CONCLUIDA COM LEGALIZACAO PENDENTE - HABITACAO` | 196 | 39 |
| `Paralisado` | 120 | 26 |
| `OBRA FISICA CONCLUIDA` | 81 | 26 |
| `LEGALIZACAO` | 57 | 22 |
| `A_DISTRATAR` | 55 | 1 |
| `OBRA RETOMADA` | 47 | 15 |
| `Em Andamento` | 47 | 11 |
| `OBRA EM ANDAMENTO - SEM MEDICAO NO MES` | 38 | 14 |
| `NAO INICIADA` | 37 | 37 |
| `NULL` *(string literal)* | 22 | 22 |
| `RISCO DE PARALISAÇÃO` | 9 | 3 |
| `Distratado` | 5 | 1 |
| `Desimobilizado` | 3 | 3 |

### FDS / Entidades — 15 grafias

`CONCLUIDA` 20.960·399 · `CONCLUÍDO E ENTREGUE` 6.439·389 · `PARALISADA`
6.129·213 · `NORMAL` 5.500·579 · `EM ANDAMENTO` 5.096·510 ·
`OBRA_FISICA_CONCLUIDA` 3.972·158 · `CANCELADO` 1.766·26 · `CONCLUÍDO E ENTREGUE
SEM PENDÊNCIAS` 1.013·341 · `ATRASADA_LIB_ACIMA_90` 792·168 · `FASE PROJETO`
729·41 · `PARALISADO` 537·41 · `NAO_INICIADA_CLAUSULA_SUSPENSIVA` 185·104 · `EM
ATENÇÃO` 165·58 · `LEGALIZACAO` 158·53 · `NAO_INICIADA` 1·1.

### Rural (PNHR) — 19 grafias

`CONCLUIDA` 435.105·8.930 · `CONCLUÍDO E ENTREGUE` 147.559·8.734 · *(vazio)*
59.275·8.470 · `PARALISADO` 31.501·944 · `CONCLUÍDO E ENTREGUE SEM PENDÊNCIAS`
22.529·7.519 · `EM ANDAMENTO` 19.642·2.000 · `PARALISADA` 7.116·183 · `Concluído
e Entregue` 4.919·985 · `NORMAL` 4.802·797 · `ATRASADA` 2.818·607 · `NAO
INICIADA` 474·10 · `Paralisado` 446·95 · `EM ATENÇÃO` 319·178 ·
`DISTRATADO/CANCELADO` 170·10 · `LEGALIZACAO` 45·24 · `Em Andamento` 40·12 ·
`OBRA NÃO INICIADA` 33·3 · `Distratado` 20·4 · `Obra Não Iniciada` 15·3 · `EM
ANDAMENTO/CONCLUÍDO E ENTREGUE` 2·2.

### Reloginho (SNH mensal) — 19 grafias

`CONCLUÍDO E ENTREGUE` 218.263·12.787 · `EM ANDAMENTO` 39.049·3.458 · `CONCLUÍDO
E ENTREGUE SEM PENDÊNCIAS` 33.883·11.308 · `PARALISADO` 6.607·604 · `Concluído e
Entregue` 5.744·1.150 · `DISTRATADO/CANCELADO` 1.001·109 · `FASE PROJETO` 729·41
· `EM ATENÇÃO` 690·306 · `Paralisado` 566·121 · `DESIMOBILIZADO` 441·38 ·
`NORMAL` 324·278 · `LEGALIZACAO` 260·99 · `Em Andamento` 87·23 · `OBRA NÃO
INICIADA` 33·3 · `Distratado` 25·5 · `Obra Não Iniciada` 15·3 · `RISCO DE
PARALISAÇÃO` 9·3 · `Desimobilizado` 3·3 · `EM ANDAMENTO/CONCLUÍDO E ENTREGUE`
2·2.

## União normalizada (`lower(trim())`) — 34 valores + placeholder

`concluida` 484.815 · `concluído e entregue` 448.014 ·
`concluida_com_vlr_a_liberar` 181.250 · `em andamento` 78.272 · `concluído e
entregue sem pendências` 67.766 · `paralisado` 40.724 · `normal` 17.123 ·
`paralisada` 14.788 · `cancelada/distratada` 12.497 · `atrasada_lib_acima_90`
8.001 · `obra_fisica_concluida` 3.972 · `atrasada` 3.339 ·
`obra_retomada_com_supl_aporte` 3.299 · `nao_iniciada` 3.255 ·
`paralisado_solicitando_vlr_adicional` 2.889 · `distratado/cancelado` 2.002 ·
`cancelado` 1.766 · `fase projeto` 1.458 · `em atenção` 1.380 · `vendido_rj` 999
· `desimobilizado` 888 · `legalizacao` 520 · `nao iniciada` 511 · `adiantada`
245 · `obra fisica concluida com legalizacao pendente - habitacao` 196 ·
`nao_iniciada_clausula_suspensiva` 185 · `obra não iniciada` 96 · `obra fisica
concluida` 81 · `a_distratar` 55 · `distratado` 50 · `obra retomada` 47 · `obra
em andamento - sem medicao no mes` 38 · `risco de paralisação` 18 · `em
andamento/concluído e entregue` 4.

Placeholder textual `null` (22 linhas / 22 APF, só FAR) — tratado como ausência
na silver (`situacao_canonica = NULL`), não entra no seed.

## Enumeração canônica adotada (tarefa 1.2 — D1 do design)

`situacao_canonica ∈ { nao_iniciada, em_obras, paralisada, concluida, cancelada }`.

Mapa completo em `seeds/data_quality/dominio_status.csv` (fonte de verdade). A
coluna `classe` marca `pendente` os itens ainda sem acordo com o negócio
(Open Questions 1 e 2), sem bloquear o build:

| valor_bruto (normalizado) | situacao_canonica | classe | nota |
|---|---|---|---|
| `risco de paralisação` | `em_obras` | `pendente` | OQ1 — é alerta, não parada. Candidato a classe `em_risco`. |
| `concluida_com_vlr_a_liberar` | `concluida` | `pendente` | OQ2 — fisicamente concluída, pendência **financeira**. Candidato a flag `pendencia_financeira`. Domina o FAR (181 k linhas) — validar contagem no doc de entrega. |
| `a_distratar` / `vendido_rj` | `cancelada` | `pendente` | intenção de distrato / imóvel vendido em recuperação judicial; ainda não é distrato consumado. |
| `em andamento/concluído e entregue` | `em_obras` | `pendente` | valor composto ambíguo (4 linhas); classificado conservadoramente como não-concluído. |
| demais 30 valores | conforme tabela do design | `valido` | |

### Decisões provisórias (Open Questions 1 e 2)

1. **`RISCO DE PARALISAÇÃO` → `em_obras`** (não é parada; `classe=pendente`
   sinaliza a revisão). Reclassificar para `em_risco` se o negócio pedir.
2. **`CONCLUIDA_COM_VLR_A_LIBERAR` → `concluida`** para o gráfico de fase
   (`classe=pendente`). Um flag `pendencia_financeira` à parte fica para a
   change de fluxo financeiro histórico.

O mapa é um CSV ajustável a qualquer momento (edição + `dbt seed` + rebuild); o
teste `dentro_do_dominio` (nível `warn`) sobre `situacao_canonica` lista a cada
build o que ficou fora — é o loop de manutenção.
