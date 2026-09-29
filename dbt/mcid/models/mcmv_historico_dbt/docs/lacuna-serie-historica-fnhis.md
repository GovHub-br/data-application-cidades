# Lacuna de série histórica FNHIS — dados disponíveis no staging MinIO

Documento de exploração (não é entrega de código): explica por que a frente
FNHIS não entra na rodada de tabelas ouro históricas das frentes novas
(Classe Média, Reforma Casa Brasil, MCMV Cidades, Pró-Moradia), apesar de já
ter bronze+prata (`bronze_shpt_fnhis_propostas_apresentadas`/`_selecionadas`,
`prata_fnhis_historico_proposta`) desde a change `renomear-sub50-para-fnhis-historico`.

Consultado direto no MinIO (`s3://data-lake-mcid/`, DuckDB, credenciais
`.env`, somente leitura) em 2026-09-29.

## O que existe hoje

Só 2 arquivos em todo o staging (busca por `*fnhis*`/`*sub_50*`/`*sub50*` em
`staging/**` e `raw/**` não encontra mais nada):

| arquivo | linhas | `_ingested_at` (todas as linhas) | `criado_em` (todas as linhas) |
|---|---|---|---|
| `staging/sharepoint/novo_mcmv_fnhis_sub_50_propostas_apresentadas.parquet` | 7.121 | `2026-08-25T17:37:30.167294+00:00` | `2025-09-22 16:23:18` |
| `staging/sharepoint/novo_mcmv_fnhis_sub_50_propostas_selecionadas.parquet` | 1.207 | `2026-08-25T17:37:30.238462+00:00` | `2026-02-02 13:51:17` |

Cada arquivo tem um único valor de `_ingested_at` e um único valor de
`criado_em` — não são snapshots semanais/mensais como as demais 4 frentes
(Classe Média/Reforma Casa Brasil/MCMV Cidades têm dezenas de arquivos
`*_MCID_YYYY_MM_DD.parquet`; Pró-Moradia agora tem os 38 snapshots semanais
GEAVO). São **duas extrações únicas**, feitas em momentos diferentes uma da
outra (`apresentadas` extraída em 2025-09-22; `selecionadas` em 2026-02-02),
sem reingestão periódica observada desde então.

## Por que não é possível montar uma série histórica real

Não é que falte QUALQUER informação de data — é que a informação de data que
existe não sustenta uma série comparável às outras 4 frentes:

1. **`apresentadas` (7.121 linhas, 85,5% do total) não tem nenhuma coluna de
   data de evento.** Colunas: `municipio`, `proponente`, `total_de_uh`,
   `numero_da_proposta`, `situacao_da_proposta`, `justificativa_nao_enquadramento`,
   `cod_ibge_munic_beneficiado`, `arquivo_de_origem`, `criado_em` (fixo, ver
   acima) + colunas de auditoria. Nenhuma data de assinatura, contratação ou
   decisão — só o retrato da fila de propostas no momento da extração.
2. **`selecionadas` (1.207 linhas) tem `data_assinatura`, mas é curta e
   concentrada**: 1.200 de 1.207 linhas (99,4%) têm valor válido (7 vêm nulas,
   vazias ou como a string literal `'None'` — mesmo defeito de exportação já
   visto em Pró-Moradia). Essas 1.200 linhas cobrem só **4 meses distintos**:

   | mês | propostas selecionadas |
   |---|---|
   | 2024-12 | 400 |
   | 2025-04 | 10 |
   | 2025-05 | 778 |
   | 2025-06 | 12 |

   Isso não é uma série mensal — é um retrato histórico de baixa
   profundidade (4 pontos, fortemente concentrado em 2 meses), **congelado no
   momento da extração** (2026-02-02): novas seleções feitas depois dessa data
   não aparecem, e o arquivo não é reingerido para trazê-las.
3. **Nenhum dos dois arquivos tem cadência de reingestão** — diferente de
   Classe Média/Reforma/Cidades (semanal via GEFUS) e Pró-Moradia (semanal
   via GEAVO), onde cada novo snapshot adiciona um ponto real à série. Aqui,
   "atualizar a série" significaria esperar alguém extrair um arquivo novo
   manualmente — não há hoje um pipeline de origem que faça isso.

Resultado: mesmo usando `data_assinatura` (a única data de evento disponível,
só em 14,5% das linhas), qualquer "série mensal" teria 4 pontos, nunca mais
que 4 até uma nova extração acontecer, e não descreveria as 85,5% das
propostas (`apresentadas`) que não têm data alguma. Isso é diferente das
outras 4 frentes, onde a série cresce a cada semana automaticamente.

## O que resolveria isso

- Uma fonte com cadência de reingestão (semanal/mensal) para FNHIS,
  equivalente ao que já existe para as outras 4 frentes — hoje não
  identificada em staging nem em raw.
- Ou, na ausência disso, reextrações periódicas manuais do mesmo relatório
  (`novo_mcmv_fnhis_sub_50_propostas_*`), acumuladas como snapshots datados —
  o que exigiria mudança de processo fora do dbt (upstream da ingestão).

## Recomendação

Manter `prata_fnhis_historico_proposta` como está (proposta única, sem
série) e não construir tabela ouro para FNHIS nesta rodada. Revisitar quando:
(a) uma nova extração de `selecionadas`/`apresentadas` aparecer no staging —
mesmo um segundo ponto no tempo já permitiria uma métrica de variação, ainda
que não uma série mensal densa — ou (b) uma fonte com cadência periódica for
identificada. Este documento serve de registro para não repetir a
investigação do zero na próxima vez que a questão surgir.
