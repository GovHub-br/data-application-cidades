# Comparação da migração: bronze antigo × novo (Fases 5 e 6)

Cada DAG piloto rodou no Airflow local (3.3.2, `make up`) contra a fonte real,
gravando só em `tests/` do MinIO de dev (`INGESTION_STORAGE_PREFIX=tests/`). A comparação é feita com DuckDB sobre o
MinIO. De um lado fica a staging antiga (`staging/<fonte>/<dado>.parquet`, que é o
bronze atual). Do outro fica o `latest/` novo, já com o renome e o filtro da prata.
Conferimos as linhas, as colunas e os valores, em texto e tipados com o mesmo cast
da prata.

| DAG | Situação |
|---|---|
| `incc_m_ingest_dag` | validada em 08/10/2026 (passos direto) e 09/10/2026 (Airflow) |
| `bacen_sgs_ingest_dag` | validada em 09/10/2026 |
| `dotacao_execucao_outras_fontes_mcid_ingest_dag` | validada em 09/10/2026 |

O `latest/` guarda a cópia da última ingestão com a partição de origem no caminho
(`latest/<AAAA-MM-DD>/<HHMMSS>/<arquivo>.parquet`), e a prata tira o `dt_ingest`
dele com o macro `lake_dt_ingest()`. A primeira versão copiava para
`latest/<arquivo>.parquet`, e o piloto da Dotação mostrou que assim o `overwrite`
perdia a data (ver Dotação, abaixo).

Crons fixados na Fase 5 (a Variable `dynamic_schedules` não existe mais):

| DAG | Cron | Por quê |
|---|---|---|
| INCC-M, BACEN | `0 6 * * *` | Fontes mensais sem data fixa de publicação; diário deixa o `latest/` fresco. |
| Dotação | `0 9 * * *` | O relatório chega de terça a sábado entre 04h e 08h (24 e-mails de 28/08 a 08/10/2026). |

A `conjuntura_dag` continua disparando as três na segunda às 08:00, antes do dbt.

## INCC-M (`fgv/incc_m`)

Execução: `tests/raw/fgv/incc_m/2026-10-08/164900/` (xlsx + `_SUCCESS`) →
`tests/staging/fgv/incc_m/2026-10-08/164900/` e `tests/staging/fgv/incc_m/latest/`.

| | Antigo | Novo (staging crua) | Novo (filtro da prata) |
|---|---|---|---|
| Linhas | 383 | 414 | 384 |
| Período | 1994-09 a 2026-07 | — | 1994-08 a 2026-07 |
| Colunas | `mes`, `indice`, `var_*`, `dt_ingest` | `column_1`, `column_2`, `No mês`, `No ano`, `12 meses`, `column_6`…`column_13` | as cinco da prata |

**Diferenças, todas explicadas:**

1. **A linha de 1994-08 (índice 100, mês-base) agora entra.** O código antigo lia
   com `skiprows=3` e `names=[...]` ao mesmo tempo. Com isso, o pandas consumia a
   primeira linha de dados como cabeçalho e a descartava. É uma correção.
   - Efeito no ouro: `ouro_conjuntura_incc_m` usa `lag(indice, 12)`, então 1995-08
     passa a ter o índice de doze meses antes, que antes era nulo.
   - Os meses do boletim não mudam.
2. **Há 30 linhas a mais na staging crua:** 29 linhas vazias no fim da planilha e
   o rodapé `Fonte: FGV`. A prata já descarta as duas coisas
   (`column_1 is not null and column_1 not ilike 'fonte%'`).
3. **As colunas `column_6` a `column_13` vêm de um quadro auxiliar à direita da
   série.** Elas trazem variações de 2021 em diante, com 80 linhas preenchidas. O
   código antigo lia só `A:E`. A staging guarda tudo como veio e a prata seleciona
   as cinco colunas da série.
4. **Inteiros aparecem sem casa decimal:** `2` no novo e `2.0` no antigo, em
   2001-05 e 2021-03. O openpyxl entrega o valor da célula e o pandas passava por
   float. Depois do `::numeric` da prata, os valores são iguais.
5. **`dt_ingest` não existe mais como coluna:** a data da ingestão sai da partição
   no `filename` (`lake_dt_ingest()`), conforme decidido na Fase 0. A prata do
   INCC não expõe `dt_ingest`, como antes.

**Valores tipados** (cast da prata nos dois lados): as 383 linhas antigas estão
todas no novo com valores idênticos. A única linha a mais é 1994-08.

## BACEN SGS (`bacen/financiamentos_imobiliarios`)

Seis séries de `BACEN_SERIES`, um arquivo por série (`<tipo>.json` na raw,
`<tipo>.parquet` na staging). A prata tira o `tipo` do `filename`.

| | Antigo (01/09/2026) | Novo (09/10/2026) |
|---|---|---|
| Linhas por série | 185 (2011-03 a 2026-07) | 186 (2011-03 a 2026-08) |
| Colunas | `tipo`, `data`, `valor`, `dt_ingest` | `data`, `valor` (+ `filename`) |

**Diferenças, todas explicadas:**

1. **Agosto de 2026 entra nas seis séries.** A staging antiga é um retrato de 01/09.
2. **Uma revisão do BACEN:** `pj_inadimplencia_pct` de 07/2026 passou de 1,16 para
   1,17. O `overwrite` traz revisões, como planejado.
3. O resto é idêntico depois da tipagem da prata (`to_date` + `::numeric`).

**Achado na validação:** o SGS às vezes responde **200 com a página HTML
"Requisição inválida!"** no lugar do JSON. A primeira execução gravou essa página
na raw como `pf_concessoes_rs_mi.json` e quebrou na conversão. O cliente antigo,
nesse caso, só registrava um aviso e perdia a série naquela execução. Agora, na
estratégia `api`, um 2xx com `Content-Type` não JSON é repetido como um 5xx e, se
persistir, falha a extração com a mensagem `resposta não é JSON (text/html)`. A
raw nunca guarda página de erro como dado.

## Dotação e execução MCid (`siafi-tesouro-gerencial/dotacao_execucao_outras_fontes_mcid`)

O ZIP do e-mail do dia vai para a raw como chegou. A conversão abre o TSV UTF-16,
pula as 11 linhas de preâmbulo (`skip_rows=11`, conferido no anexo real) e lê o
cabeçalho da linha 12.

| | Antigo (25/08/2026) | Novo (09/10/2026) |
|---|---|---|
| Linhas | 562 | 562 |
| Colunas | 30 de dado, já renomeadas, + `_source_file`, `_ingested_at`, `_source_hash` | `column_1`…`column_20` (dimensões) + 10 de valor com o título do Tesouro |

**Diferenças, todas explicadas:**

1. **O relatório não dá nome às 20 colunas de dimensão:** o cabeçalho traz um
   espaço. O conversor passou a tratar nome só com espaço como vazio
   (`column_<n>`), e a prata renomeia por posição. Os valores são renomeados a
   partir do título do Tesouro (`"DOTACAO INICIAL"` → `dotacao_inicial` etc.).
2. **Os códigos preservam o zero à esquerda**, que o pandas cortava:
   `programa_governo_codigo` (`0032` × `32`, 137 linhas),
   `plano_orcamentario_funcao` (71), `plano_orcamentario_programa` (137) e
   `elemento_despesa_codigo` (83). O ouro do OGU só filtra por
   `acao_governo_codigo`, que não muda.
3. **`dt_ingest`:** antes vinha de `_ingested_at` (UTC); agora é a partição da
   ingestão no horário de Brasília (`2026-10-09 09:24`). O ouro usa
   `max(dt_ingest)::date` como data de referência da extração; a data não muda.

**Valores:** ignorando o zero à esquerda dos códigos, as 562 linhas são idênticas
nas 30 colunas (texto contra texto, antes do `parse_valor_siafi`).

## Fase 6

Em 09/10/2026 as DAGs migradas rodaram no Airflow local contra as fontes reais
(`tests/`), já com `base_url` no lugar de Connection por fonte. Todas terminaram
com sucesso, e o volume de cada `latest/` bate com as conversões locais abaixo.

| Dataset | Linhas na staging | Arquivo publicado |
|---|---|---|
| `fgv/icst` | 195 | `icst.parquet` |
| `fipe/indice_locacao` | 230 | `fipezap-serieshistoricas.parquet` |
| `abecip/poupanca_sbpe_mensal` | 604 | `cp-historico-agosto20261.parquet` |
| `abecip/financiamentos_modalidade` | 307 | `unidades-site81.parquet` |
| `mrv/planilha_interativa` | 377 | `mrve3_base_de_dados_operacionais_e_financeiros.parquet` |
| `bacen/credito_imobiliario_pib` | 148 | `credito_imobiliario_pib.parquet` |

### ICST (`fgv/icst`)

Primeira execução real da estratégia `fgvdados` (login OutSystems + FGVDados).
CSV latin-1 com o cabeçalho da FGV (nome longo de cada série + código).

| | Antigo (01/09/2026) | Novo (09/10/2026) |
|---|---|---|
| Meses | 194 (2010-07 a 2026-08) | 195 (2010-07 a 2026-09) |

Valores idênticos em todos os meses comuns; entra 09/2026.

### Crédito imobiliário / PIB (`bacen/credito_imobiliario_pib`)

A mesma API do Olinda que o `ClienteBacenImobiliario` já usava na `main`
(`bacen_credito_pib_ingest_dag`, disparada pela `conjuntura_dag`). Filtro no
endpoint com `%20` (o Olinda recusa o espaço como `+`).

| | Antigo (01/09/2026) | Novo (09/10/2026) |
|---|---|---|
| Pontos | 146 (2014-04 a 2026-05) | 148 (2014-04 a 2026-07) |

### MRV (`mrv/planilha_interativa`)

Uma DAG no lugar das duas antigas (mesma planilha). Só até o bronze: nenhum ouro
lê a MRV. 377 linhas × 105 colunas (uma por trimestre) na aba de dados
operacionais.

### FipeZap (`fipe/indice_locacao`)

A raw guarda a planilha inteira (59 abas); a staging converte só a aba
`Índice FipeZAP` (`sheet`, `header_row=4`), porque o bronze em `overwrite` lê todo
Parquet do `latest/`. As colunas de locação viram `Total_5`, `Total_6` e `Total_7`.

| | Antigo (01/09/2026) | Novo (09/10/2026, conversão local da planilha real) |
|---|---|---|
| Meses | 224 (2008-01 a 2026-08) | 225 (2008-01 a 2026-09) |

- O histórico é idêntico (mesmo texto de origem, `70.8668233584297` etc.).
- 08/2026 estava vazio no antigo e agora tem valor; 09/2026 entra vazio, como a
  FIPE publica o mês em apuração.
- **A conferência de coerência do `ClienteFipeZap` virou teste do dbt**
  (`tests/conjuntura_fipezap_colunas_coerentes.sql`). Exercitado em DuckDB:
  passa com as colunas certas; falha com índice e variação trocados (222 de 222
  meses fora) e com o índice de venda no lugar do de locação (180 de 223).

### ABECIP poupança e financiamentos (`abecip/poupanca_sbpe_mensal`, `abecip/financiamentos_modalidade`)

O link da edição é achado na página da ABECIP (`link_in_page`); a staging converte
só a aba usada (`SBPE_Mensal` e `BD_Unidades`, cabeçalho na linha 5).

| | Antigo (01/09/2026) | Novo (09/10/2026, conversão local das planilhas reais) |
|---|---|---|
| Poupança | 535 meses (1982-01 a 2026-07) | 536 (1982-01 a 2026-08) |
| Financiamentos | 296 meses (2002-01 a 2026-08) | 296 (2002-01 a 2026-08) |

- Valores idênticos em todas as linhas comuns; a única linha a mais é 08/2026 da
  poupança (edição de agosto).
- A prata da poupança passou a expor `rendimento`, que a staging antiga tinha e a
  prata descartava; ele entra na identidade do saldo.
- **As conferências do `ClienteAbecip` viraram testes do dbt:**
  `conjuntura_abecip_poupanca_identidades` (captação = depósito − retirada;
  evolução do saldo) e `conjuntura_abecip_financiamentos_totais` (Total =
  Construção + Aquisição). Exercitados em DuckDB: passam com as colunas certas e
  falham com colunas trocadas (100% das linhas fora).
- Nota de método: na emulação em DuckDB, `::numeric` vira `DECIMAL(18,3)` e corta
  casas; a comparação usa `double`. No Postgres a prata usa `numeric` sem limite.
