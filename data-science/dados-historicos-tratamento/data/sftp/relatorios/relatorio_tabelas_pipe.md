## Relatório: Tabelas SFTP com Separador `|` (Pipe)

> 304 de 2008 tabelas (15.1%) usam `|` como separador ou possuem campos concatenados com `|`. Essas tabelas são inválidas quando lidas com `sep='\t'` e precisam de tratamento especial de saneamento.

---

### 1. Panorama

```
SFTP: 2008 tabelas
├── 1704 (84.9%) tab-separated válidas ── sem pipes nos dados
└──  304 (15.1%) com pipe ── precisam de saneamento
     ├── Padrão A: 202 tabelas — arquivo inteiro pipe-separated (sep='|')
     └── Padrão B: 102 tabelas — coluna 0 contém campos concatenados com |
```

### 2. Padrão A: Arquivo Inteiro Pipe-Separated (202 tabelas)

Os dados são delimitados por `|` em vez de `\t`. As últimas 2 "colunas" (`dt_ingest`, `arquivo_origem`) usam `\t`, criando um híbrido `|` + `\t`.

**Exemplo real** (`int021_ministeriocidades_far_bb_pf_20191230.csv`):
```
2019-12-30|43321218|1514334|ADRIANA RIBEIRO DA CRUZ|7501921601|2017-03-14|...
        ...|FAR|2027-03-14|||504239|80,00|3170107|FAR BB|1	2026-06-30T13:00:52	/home/fabrica/GEFUS/...
```

**Header típico** (aparece como uma única coluna concatenada quando lido com `\t`):
```
dt_movimento_nu_apf_nu_cpf_no_mutuaria...
```

#### Distribuição por família

| Família | Qtd | Descrição | Período |
|---------|-----|-----------|---------|
| `int021` (FAR BB PF) | 24 | Contratos PF — BB | 2019-12 a 2026-04 |
| `int042` (FDS Caixa PF) | 24 | Contratos PF — Caixa/FDS | 2020-06 a 2026-04 |
| `int054` (FAR BB Empreendimentos) | 23 | Empreendimentos FAR — BB | 2020-01 a 2024-10 |
| `int057` (PNHR BB Empreendimentos) | 26 | Empreendimentos PNHR — BB | 2019-12 a 2024-10 |
| `int058` (PNHR BB PF) | 27 | Contratos PF PNHR — BB | 2019-12 a 2026-04 |
| `int064` (PNHR Caixa PF) | 25 | Contratos PF PNHR — Caixa | 2020-02 a 2026-04 |
| `int065` (PNHR Caixa Empreendimentos) | 20 | Empreendimentos PNHR — Caixa | 2019-12 a 2024-08 |
| `int040` (FAR Caixa Empreendimentos) | 1 | Empreendimentos FAR — Caixa | 2023-12 |
| `base_pj_fgts_*` | 31 | Base FGTS — Pessoa Jurídica | 2001-06 a 2026-01 |
| `base_andamento_obra_*` | 1 | Andamento de obra consolidado | 2025-12 |
| `tab_validacao_*` | 2 | Validação de arquivos | 2020-03/04 |
| **Total** | **204** | | |

**Nota:** As versões `gefus_anteriores_*` dessas mesmas tabelas (409 arquivos) NÃO contêm pipes — foram reformatadas pelo pipeline GEFUS. Apenas as versões diretas (`int0*_*.csv`, `base_pj_fgts_*.csv`) têm o problema.

---

### 3. Padrão B: Coluna 0 Pipe-Delimited (100 tabelas)

A primeira coluna (tab-separated) contém múltiplos campos concatenados com `|`. As colunas 1 e 2 são `dt_ingest` e `arquivo_origem` normais.

**Exemplo real** (`caixa_af_gehis_andamento_obra_m20260108.csv`):
```
Coluna 0: 202601|51102954|2026-03-30||EM ATENCAO
Coluna 1: 2026-06-30T14:02:18.490239
Coluna 2: /home/fabrica/GEFUS/ANTERIORES/CAIXA_AF_GEHIS_ANDAMENTO_OBRA_M20260108.TXT
```

**Header** (coluna 0 com nomes concatenados):
```
anomes_nu_apf_dt_prevista_conclusao_dt_prevista_inauguracao_situacao_obra
```

Os campos dentro da coluna 0 são: `ano_mes`, `nu_apf`, `dt_prevista_conclusao`, `dt_prevista_inauguracao`, `situacao_obra`.

#### Distribuição por família

| Família | Qtd | Descrição | Período |
|---------|-----|-----------|---------|
| `caixa_af_gehis_andamento_obra_*` | 96 | Obras Caixa — GEHIS (semanal) | 2021-09 a 2026-06 |
| `caixa_af_gehis_alienacao_imovel_*` | 2 | Alienação de imóveis | 2021-12, 2022-03 |
| `caixa_af_gehis_operacao_desenquadrada_*` | 2 | Operações desenquadradas | 2021-12, 2022-03 |
| `gefus_anteriores_caixa_af_gehis_andamento_obra_*` | 2 | Versões gefus do andamento (com pipe) | 2023-05, 2023-06 |
| **Total** | **102** | | |

**Nota sobre o Padrão B:** Diferente do Padrão A, aqui as versões `gefus_anteriores` TAMBÉM contêm pipe (2 das 200+ versões), e as versões mais recentes (`bb_af_diemp_andamento_obra_*`) NÃO têm pipe — foram normalizadas.

---

### 4. Impacto no Pipeline Atual

| Etapa | Comportamento Atual | Com `|` |
|-------|-------------------|---------|
| `carregar_csv()` com `sep='\t'` | Padrão A: header vira 1 coluna gigante; Padrão B: coluna 0 com nomes concatenados | ❌ |
| Classificação (`regras.py`, R3) | Detecta `separador_\|` para o dump, mas não é chamada para SFTP | ⚠️ |
| Tratamento (`tratamento_especiais.py`) | Lida com `separador_\|` do dump (pipe no meio de tab), não cobre SFTP | ❌ |
| Matching estrutural | Tabelas com header corrompido não produzem matches úteis | ❌ |

### 5. Recomendações de Tratamento

#### Padrão A (pipe-separated puro)

1. Detectar arquivos onde ≥50% das linhas contêm `|` e o header tem apenas 1-3 colunas quando lido com `\t`
2. Reler com `sep='|'`, descartando as últimas 2 "colunas" (`dt_ingest`, `arquivo_origem`) que são metadados de ingestão
3. Inferir nomes de coluna via `inferencia_colunas.py` (já existe para tabelas sem cabeçalho)
4. Classificar como `separador_|` para rotear ao tratamento especial

#### Padrão B (coluna 0 pipe-delimited)

1. Detectar quando a coluna 0 tem `|` mas as demais colunas são normais
2. Fazer `split('|')` da coluna 0 nos campos individuais
3. Inferir os nomes das sub-colunas a partir do header concatenado (split por `_` como heurística, ou usar nomes canônicos)
4. O resultado são 5-7 colunas + `dt_ingest` + `arquivo_origem`

#### Alternativa pragmática

Como as versões `gefus_anteriores_*` das mesmas tabelas (Padrão A) já estão formatadas corretamente (tab-separated, sem pipes), pode-se simplesmente **usar as versões gefus_anteriores como fonte primária** e ignorar as versões diretas com pipe. Isso reduz o escopo de saneamento de 204 para 102 tabelas (apenas Padrão B).

### 6. Lista Completa

#### Padrão A — 204 arquivos

```
int021_ministeriocidades_far_bb_pf_20191230.csv
int021_ministeriocidades_far_bb_pf_20200131.csv
int021_ministeriocidades_far_bb_pf_20200331.csv
int021_ministeriocidades_far_bb_pf_20200430.csv
int021_ministeriocidades_far_bb_pf_20200630.csv
int021_ministeriocidades_far_bb_pf_20200831.csv
int021_ministeriocidades_far_bb_pf_20201030.csv
int021_ministeriocidades_far_bb_pf_20210226.csv
int021_ministeriocidades_far_bb_pf_20210531.csv
int021_ministeriocidades_far_bb_pf_20210730.csv
int021_ministeriocidades_far_bb_pf_20210930.csv
int021_ministeriocidades_far_bb_pf_20220225.csv
int021_ministeriocidades_far_bb_pf_20220429.csv
int021_ministeriocidades_far_bb_pf_20220531.csv
int021_ministeriocidades_far_bb_pf_20220831.csv
int021_ministeriocidades_far_bb_pf_20221031.csv
int021_ministeriocidades_far_bb_pf_20230331.csv
int021_ministeriocidades_far_bb_pf_20240328.csv
int021_ministeriocidades_far_bb_pf_20240930.csv
int021_ministeriocidades_far_bb_pf_20241031.csv
int021_ministeriocidades_far_bb_pf_20250731.csv
int021_ministeriocidades_far_bb_pf_20251031_contingencia.csv
int021_ministeriocidades_far_bb_pf_20251128_contingencia.csv
int021_ministeriocidades_far_bb_pf_20260430_contingencia.csv
int040_ministeriocidades_far_caixa_empreendimentos_20231228.csv
int042_ministeriocidades_fds_caixa_pf_20200630.csv
int042_ministeriocidades_fds_caixa_pf_20201130.csv
int042_ministeriocidades_fds_caixa_pf_20201230.csv
int042_ministeriocidades_fds_caixa_pf_20210129.csv
int042_ministeriocidades_fds_caixa_pf_20210331.csv
int042_ministeriocidades_fds_caixa_pf_20210630.csv
int042_ministeriocidades_fds_caixa_pf_20210730.csv
int042_ministeriocidades_fds_caixa_pf_20211029.csv
int042_ministeriocidades_fds_caixa_pf_20211230.csv
int042_ministeriocidades_fds_caixa_pf_20220630.csv
int042_ministeriocidades_fds_caixa_pf_20221031.csv
int042_ministeriocidades_fds_caixa_pf_20221229.csv
int042_ministeriocidades_fds_caixa_pf_20230228.csv
int042_ministeriocidades_fds_caixa_pf_20230331.csv
int042_ministeriocidades_fds_caixa_pf_20230531.csv
int042_ministeriocidades_fds_caixa_pf_20231228.csv
int042_ministeriocidades_fds_caixa_pf_20240131.csv
int042_ministeriocidades_fds_caixa_pf_20240531.csv
int042_ministeriocidades_fds_caixa_pf_20240628.csv
int042_ministeriocidades_fds_caixa_pf_20240731.csv
int042_ministeriocidades_fds_caixa_pf_20240830.csv
int042_ministeriocidades_fds_caixa_pf_20241031.csv
int042_ministeriocidades_fds_caixa_pf_20250731.csv
int042_ministeriocidades_fds_caixa_pf_20260430_contingencia.csv
int054_ministeriocidades_far_bb_empreendimentos_20200131.csv
int054_ministeriocidades_far_bb_empreendimentos_20200430.csv
int054_ministeriocidades_far_bb_empreendimentos_20200529.csv
int054_ministeriocidades_far_bb_empreendimentos_20201130.csv
int054_ministeriocidades_far_bb_empreendimentos_20210226.csv
int054_ministeriocidades_far_bb_empreendimentos_20210430.csv
int054_ministeriocidades_far_bb_empreendimentos_20210730.csv
int054_ministeriocidades_far_bb_empreendimentos_20211029.csv
int054_ministeriocidades_far_bb_empreendimentos_20211130.csv
int054_ministeriocidades_far_bb_empreendimentos_20220131.csv
int054_ministeriocidades_far_bb_empreendimentos_20220225.csv
int054_ministeriocidades_far_bb_empreendimentos_20220531.csv
int054_ministeriocidades_far_bb_empreendimentos_20220630.csv
int054_ministeriocidades_far_bb_empreendimentos_20220729.csv
int054_ministeriocidades_far_bb_empreendimentos_20221229.csv
int054_ministeriocidades_far_bb_empreendimentos_20230228.csv
int054_ministeriocidades_far_bb_empreendimentos_20230531.csv
int054_ministeriocidades_far_bb_empreendimentos_20231228.csv
int054_ministeriocidades_far_bb_empreendimentos_20240328.csv
int054_ministeriocidades_far_bb_empreendimentos_20240628.csv
int054_ministeriocidades_far_bb_empreendimentos_20240830.csv
int054_ministeriocidades_far_bb_empreendimentos_20240930.csv
int054_ministeriocidades_far_bb_empreendimentos_20241031.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20191230.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20200331.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20200630.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20200831.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20201030.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20201230.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20210129.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20210430.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20210730.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20211230.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20220429.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20220531.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20220630.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20220831.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20220930.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20221031.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20230131.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20230428.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20230531.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20230630.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20230731.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20230831.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20231130.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20231228.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20240830.csv
int057_ministeriocidades_pnhr_bb_empreendimentos_20241031.csv
int058_ministeriocidades_pnhr_bb_pf_20191230.csv
int058_ministeriocidades_pnhr_bb_pf_20200228.csv
int058_ministeriocidades_pnhr_bb_pf_20200430.csv
int058_ministeriocidades_pnhr_bb_pf_20200731.csv
int058_ministeriocidades_pnhr_bb_pf_20200930.csv
int058_ministeriocidades_pnhr_bb_pf_20201130.csv
int058_ministeriocidades_pnhr_bb_pf_20210430.csv
int058_ministeriocidades_pnhr_bb_pf_20210531.csv
int058_ministeriocidades_pnhr_bb_pf_20210831.csv
int058_ministeriocidades_pnhr_bb_pf_20220429.csv
int058_ministeriocidades_pnhr_bb_pf_20220630.csv
int058_ministeriocidades_pnhr_bb_pf_20220729.csv
int058_ministeriocidades_pnhr_bb_pf_20220831.csv
int058_ministeriocidades_pnhr_bb_pf_20220930.csv
int058_ministeriocidades_pnhr_bb_pf_20230131.csv
int058_ministeriocidades_pnhr_bb_pf_20230228.csv
int058_ministeriocidades_pnhr_bb_pf_20230331.csv
int058_ministeriocidades_pnhr_bb_pf_20230428.csv
int058_ministeriocidades_pnhr_bb_pf_20230630.csv
int058_ministeriocidades_pnhr_bb_pf_20230831.csv
int058_ministeriocidades_pnhr_bb_pf_20231228.csv
int058_ministeriocidades_pnhr_bb_pf_20240131.csv
int058_ministeriocidades_pnhr_bb_pf_20240628.csv
int058_ministeriocidades_pnhr_bb_pf_20240731.csv
int058_ministeriocidades_pnhr_bb_pf_20241031.csv
int058_ministeriocidades_pnhr_bb_pf_20251031_contingencia.csv
int058_ministeriocidades_pnhr_bb_pf_20260430_contingencia.csv
int064_ministeriocidades_pnhr_caixa_pf_20200228.csv
int064_ministeriocidades_pnhr_caixa_pf_20200331.csv
int064_ministeriocidades_pnhr_caixa_pf_20201030.csv
int064_ministeriocidades_pnhr_caixa_pf_20210129.csv
int064_ministeriocidades_pnhr_caixa_pf_20210331.csv
int064_ministeriocidades_pnhr_caixa_pf_20210630.csv
int064_ministeriocidades_pnhr_caixa_pf_20210730.csv
int064_ministeriocidades_pnhr_caixa_pf_20210930.csv
int064_ministeriocidades_pnhr_caixa_pf_20220225.csv
int064_ministeriocidades_pnhr_caixa_pf_20220531.csv
int064_ministeriocidades_pnhr_caixa_pf_20221130.csv
int064_ministeriocidades_pnhr_caixa_pf_20230131.csv
int064_ministeriocidades_pnhr_caixa_pf_20230331.csv
int064_ministeriocidades_pnhr_caixa_pf_20230630.csv
int064_ministeriocidades_pnhr_caixa_pf_20230731.csv
int064_ministeriocidades_pnhr_caixa_pf_20231228.csv
int064_ministeriocidades_pnhr_caixa_pf_20240131.csv
int064_ministeriocidades_pnhr_caixa_pf_20240229.csv
int064_ministeriocidades_pnhr_caixa_pf_20240328.csv
int064_ministeriocidades_pnhr_caixa_pf_20240430.csv
int064_ministeriocidades_pnhr_caixa_pf_20240628.csv
int064_ministeriocidades_pnhr_caixa_pf_20240930.csv
int064_ministeriocidades_pnhr_caixa_pf_20250731.csv
int064_ministeriocidades_pnhr_caixa_pf_20251128_contingencia.csv
int064_ministeriocidades_pnhr_caixa_pf_20260430_contingencia.csv
int065_ministeriocidades_pnhr_caixa_empreendimentos_20191230.csv
int065_ministeriocidades_pnhr_caixa_empreendimentos_20200131.csv
int065_ministeriocidades_pnhr_caixa_empreendimentos_20200430.csv
int065_ministeriocidades_pnhr_caixa_empreendimentos_20200630.csv
int065_ministeriocidades_pnhr_caixa_empreendimentos_20210129.csv
int065_ministeriocidades_pnhr_caixa_empreendimentos_20210430.csv
int065_ministeriocidades_pnhr_caixa_empreendimentos_20210531.csv
int065_ministeriocidades_pnhr_caixa_empreendimentos_20220429.csv
int065_ministeriocidades_pnhr_caixa_empreendimentos_20220531.csv
int065_ministeriocidades_pnhr_caixa_empreendimentos_20220630.csv
int065_ministeriocidades_pnhr_caixa_empreendimentos_20220729.csv
int065_ministeriocidades_pnhr_caixa_empreendimentos_20220831.csv
int065_ministeriocidades_pnhr_caixa_empreendimentos_20230228.csv
int065_ministeriocidades_pnhr_caixa_empreendimentos_20230428.csv
int065_ministeriocidades_pnhr_caixa_empreendimentos_20230531.csv
int065_ministeriocidades_pnhr_caixa_empreendimentos_20230630.csv
int065_ministeriocidades_pnhr_caixa_empreendimentos_20230731.csv
int065_ministeriocidades_pnhr_caixa_empreendimentos_20231228.csv
int065_ministeriocidades_pnhr_caixa_empreendimentos_20240430.csv
int065_ministeriocidades_pnhr_caixa_empreendimentos_20240830.csv
base_pj_fgts_200106.csv
base_pj_fgts_20200305.csv
base_pj_fgts_20201105.csv
base_pj_fgts_20201210.csv
base_pj_fgts_20210109.csv
base_pj_fgts_20210119.csv
base_pj_fgts_20210209.csv
base_pj_fgts_20210609.csv
base_pj_fgts_20210707.csv
base_pj_fgts_20210807.csv
base_pj_fgts_20211007.csv
base_pj_fgts_20211107.csv
base_pj_fgts_20220107.csv
base_pj_fgts_20220307.csv
base_pj_fgts_20220807.csv
base_pj_fgts_20221207.csv
base_pj_fgts_20230207.csv
base_pj_fgts_20230407.csv
base_pj_fgts_20230607.csv
base_pj_fgts_20240107.csv
base_pj_fgts_20240207.csv
base_pj_fgts_20240507.csv
base_pj_fgts_20241007.csv
base_pj_fgts_20241207.csv
base_pj_fgts_20250107.csv
base_pj_fgts_20250307.csv
base_pj_fgts_20250607.csv
base_pj_fgts_20250707.csv
base_pj_fgts_20250807.csv
base_pj_fgts_20250907.csv
base_pj_fgts_20260107.csv
base_andamento_obra_m20251218.csv
tab_validacao_arquivos_pf.csv
tab_validacao_arquivos_pj.csv
```

#### Padrão B — 100 arquivos

```
caixa_af_gehis_andamento_obra_m20210909.csv
caixa_af_gehis_andamento_obra_m20210923.csv
caixa_af_gehis_andamento_obra_m20211007.csv
caixa_af_gehis_andamento_obra_m20211021.csv
caixa_af_gehis_andamento_obra_m20211104.csv
caixa_af_gehis_andamento_obra_m20211111.csv
caixa_af_gehis_andamento_obra_m20211202.csv
caixa_af_gehis_andamento_obra_m20211209.csv
caixa_af_gehis_andamento_obra_m20211223.csv
caixa_af_gehis_andamento_obra_m20220113.csv
caixa_af_gehis_andamento_obra_m20220203.csv
caixa_af_gehis_andamento_obra_m20220217.csv
caixa_af_gehis_andamento_obra_m20220303.csv
caixa_af_gehis_andamento_obra_m20220317.csv
caixa_af_gehis_andamento_obra_m20220331.csv
caixa_af_gehis_andamento_obra_m20220407.csv
caixa_af_gehis_andamento_obra_m20220526.csv
caixa_af_gehis_andamento_obra_m20220602.csv
caixa_af_gehis_andamento_obra_m20220707.csv
caixa_af_gehis_andamento_obra_m20220721.csv
caixa_af_gehis_andamento_obra_m20220804.csv
caixa_af_gehis_andamento_obra_m20220811.csv
caixa_af_gehis_andamento_obra_m20220923.csv
caixa_af_gehis_andamento_obra_m20221006.csv
caixa_af_gehis_andamento_obra_m20221027.csv
caixa_af_gehis_andamento_obra_m20221110.csv
caixa_af_gehis_andamento_obra_m20221208.csv
caixa_af_gehis_andamento_obra_m20221215.csv
caixa_af_gehis_andamento_obra_m20221229.csv
caixa_af_gehis_andamento_obra_m20230106.csv
caixa_af_gehis_andamento_obra_m20230202.csv
caixa_af_gehis_andamento_obra_m20230223.csv
caixa_af_gehis_andamento_obra_m20230330.csv
caixa_af_gehis_andamento_obra_m20230413.csv
caixa_af_gehis_andamento_obra_m20230511.csv
caixa_af_gehis_andamento_obra_m20230525.csv
caixa_af_gehis_andamento_obra_m20230713.csv
caixa_af_gehis_andamento_obra_m20230720.csv
caixa_af_gehis_andamento_obra_m20230728.csv
caixa_af_gehis_andamento_obra_m20230921.csv
caixa_af_gehis_andamento_obra_m20231005.csv
caixa_af_gehis_andamento_obra_m20231019.csv
caixa_af_gehis_andamento_obra_m20240104.csv
caixa_af_gehis_andamento_obra_m20240112.csv
caixa_af_gehis_andamento_obra_m20240411.csv
caixa_af_gehis_andamento_obra_m20240418.csv
caixa_af_gehis_andamento_obra_m20240502.csv
caixa_af_gehis_andamento_obra_m20240620.csv
caixa_af_gehis_andamento_obra_m20240627.csv
caixa_af_gehis_andamento_obra_m20240718.csv
caixa_af_gehis_andamento_obra_m20240808.csv
caixa_af_gehis_andamento_obra_m20240902.csv
caixa_af_gehis_andamento_obra_m20240919.csv
caixa_af_gehis_andamento_obra_m20241205.csv
caixa_af_gehis_andamento_obra_m20241212.csv
caixa_af_gehis_andamento_obra_m20241219.csv
caixa_af_gehis_andamento_obra_m20250103.csv
caixa_af_gehis_andamento_obra_m20250306.csv
caixa_af_gehis_andamento_obra_m20250313.csv
caixa_af_gehis_andamento_obra_m20250320.csv
caixa_af_gehis_andamento_obra_m20250403.csv
caixa_af_gehis_andamento_obra_m20250417.csv
caixa_af_gehis_andamento_obra_m20250515.csv
caixa_af_gehis_andamento_obra_m20250522.csv
caixa_af_gehis_andamento_obra_m20250724.csv
caixa_af_gehis_andamento_obra_m20250814.csv
caixa_af_gehis_andamento_obra_m20250911.csv
caixa_af_gehis_andamento_obra_m20250918.csv
caixa_af_gehis_andamento_obra_m20251002.csv
caixa_af_gehis_andamento_obra_m20251009.csv
caixa_af_gehis_andamento_obra_m20251119.csv
caixa_af_gehis_andamento_obra_m20251204.csv
caixa_af_gehis_andamento_obra_m20251218.csv
caixa_af_gehis_andamento_obra_m20251231_1.csv
caixa_af_gehis_andamento_obra_m20260108.csv
caixa_af_gehis_andamento_obra_m20260115.csv
caixa_af_gehis_andamento_obra_m20260129.csv
caixa_af_gehis_andamento_obra_m20260205.csv
caixa_af_gehis_andamento_obra_m20260212.csv
caixa_af_gehis_andamento_obra_m20260226.csv
caixa_af_gehis_andamento_obra_m20260305.csv
caixa_af_gehis_andamento_obra_m20260319.csv
caixa_af_gehis_andamento_obra_m20260402.csv
caixa_af_gehis_andamento_obra_m20260409.csv
caixa_af_gehis_andamento_obra_m20260416.csv
caixa_af_gehis_andamento_obra_m20260423.csv
caixa_af_gehis_andamento_obra_m20260430.csv
caixa_af_gehis_andamento_obra_m20260507.csv
caixa_af_gehis_andamento_obra_m20260514.csv
caixa_af_gehis_andamento_obra_m20260521.csv
caixa_af_gehis_andamento_obra_m20260528.csv
caixa_af_gehis_andamento_obra_m20260605.csv
caixa_af_gehis_andamento_obra_m20260611.csv
caixa_af_gehis_andamento_obra_m20260618.csv
caixa_af_gehis_andamento_obra_m20260625.csv
caixa_af_gehis_alienacao_imovel_m202112.csv
caixa_af_gehis_alienacao_imovel_m202202_d2022_03_10.csv
caixa_af_gehis_operacao_desenquadrada_m202112.csv
caixa_af_gehis_operacao_desenquadrada_m202202_d2022_03_10.csv
gefus_anteriores_caixa_af_gehis_andamento_obra_m20230511.csv
gefus_anteriores_caixa_af_gehis_andamento_obra_m20230602.csv
```
