# Guia de Revisão — Tratamento de Dados

> **Objetivo:** transformar cada tabela classificada em um formato canônico, normalizado e pronto para consumo por modelos de Machine Learning.

## Visão geral do pipeline de tratamento

```
classificacao_formacao.csv
        │
        ▼
  deduplicação MD5 (deduplicacao.py)
  ├─ agrupar por hash binário
  ├─ eleger 1 canônica por grupo
  └─ gerar _dedup_map.csv
        │
        ▼
  roteador tratar_tabela()
  ├─ bem_formada           → tratar_bem_formada()         (tratamento.py)
  ├─ sem_cabecalho         → tratar_sem_cabecalho()       (tratamento_especiais.py)
  ├─ cabecalho_*           → tratar_cabecalho_deslocado() (tratamento_cabecalho.py)
  ├─ cabecalho_composto_*  → concatenar_header_composto() (tratamento_cabecalho.py)
  ├─ sub_tabelas_*         → tratar_sub_tabelas_N()       (tratamento_subtabelas.py)
  ├─ separador_|           → tratar_separador_pipe()      (tratamento_especiais.py)
  ├─ vazia / sem_utilidade → DESCARTE
  └─ fallback              → tratar_bem_formada()
        │
        ▼
  validação (validacao.py)
  ├─ validar_shape (≥1 linha, ≥1 coluna)
  ├─ validar_chaves_nao_nulas (APF, CNPJ, contrato, etc.)
  └─ calcular missing_pct
        │
        ▼
  data/treated_tables/*_tratado.csv
  data/treated_tables/_quality_report.csv
```

## Pipeline de `bem_formada` (Grupos 1–8)

Este é o pipeline completo aplicado às tabelas com estrutura ideal. Outras categorias (cabeçalho deslocado, composto, pipe) são convertidas para `bem_formada` primeiro e depois passam por este pipeline.

### Group 1 — Normalização de nomes de coluna

**Função:** `normalizar_nome_coluna()` em `tratamento.py`

- lowercase + strip
- Remove acentos via `unicodedata.normalize('NFKD')`
- Substitui espaços e caracteres especiais por `_`
- Colapsa múltiplos underscores
- Fallback: nome vazio ou puramente numérico → `"col"`

**Exemplo:** `"Código do IBGE"` → `"codigo_do_ibge"`

### Group 2 — Separador decimal

**Função:** `detectar_separador_decimal()` / `converter_decimal()`

- Amostra 20 valores por coluna do tipo `float64`
- Se >50% contém `,`, assume vírgula como separador decimal (padrão brasileiro)
- Substitui `,` → `.` e converte para `float64`

### Group 3 — Parsing de datas

**Função:** `detectar_formato_data()` / `parse_datas()`

- Aplica-se a colunas cujo nome normalizado contém `dat_`, `dt_` ou `data_`
- Testa formato ISO (`YYYY-MM-DD`) e brasileiro (`DD/MM/YYYY`)
- Usa `pd.to_datetime()` com `dayfirst=True` para formato BR
- **Regra de segurança:** se >10% dos valores não-nulos falham na conversão, a coluna **reverte ao tipo original**

### Group 4 — Reparo de encoding

**Função:** `reparar_encoding()` / `reparar_encoding_df()`

- Aplica-se a todas as colunas `object` (string)
- Usa `charset-normalizer` para detectar encoding (ex.: Latin1 mal decodificado como UTF-8)
- Redecodifica se encoding ≠ UTF-8/ASCII com confiança > 0.7
- Fallback: `ftfy.fix_text()` para correções de mojibake

### Group 5 — Type canonization

**Função:** `aplicar_tipos_canonicos()` / `tipo_canonico_manual()` / `tipo_canonico_auto()`

1. **Manual** (por prefixo/sufixo do nome normalizado):

| Padrão no nome | Tipo canônico |
|----------------|---------------|
| `cnpj` | `str` |
| prefixo `cod_`, `nr_`, `nu_` | `str` (identificadores) |
| `vlr_`, `valor_`, `total_` | `float64` |
| `qtd_`, `qt_`, `unidades` | `Int64` (nullable) |
| `dat_`, `dt_`, `data_` | `datetime64[ns]` |

2. **Automático** (fallback): busca o tipo mais frequente no inventário de colunas (`data/columns_*.csv`), que agrega tipos de todas as 753 tabelas.

3. Se nenhum match → `str` como safe default.

### Group 6 — Classificação de perfil

**Função:** `classificar_perfil()`

| Perfil | Condição | Tratamento |
|--------|----------|------------|
| `lookup` | ≤ 3 colunas | Apenas reparo de encoding |
| `event_level` | 4–6 colunas + chave repetida (>20% duplicação) | Normalização + datas + encoding |
| `colunar_denso` | 4–6 colunas sem chave repetida, ou 7–9 sem total, ou ≥13 | Pipeline completo (tipos + datas + decimal + encoding) |
| `agregado_uf` | 7–9 colunas + tem linha "Total" | Pipeline completo + coluna `is_aggregate` |

### Group 7 — Extração de período e instituição

**Funções:** `extrair_periodo_filename()` / `inferir_instituicao()`

- **Período:** extrai data do nome do arquivo (5 padrões regex testados em ordem: `DD_MM_YYYY`, `YYYYMMDD`, `YYYYMM`, `YYYY_MM`). Normaliza para `YYYY-MM-DD`.
- **Instituição:** `bb` (prefixo `bb_`), `caixa` (contém `caixa_`), ou `unknown`.

### Group 8 — Metadados

**Função:** `adicionar_metadados()`

Adiciona colunas ao DataFrame tratado: `source_table`, `report_date`, `institution`, `profile`, `content_hash`.

## Tratamentos por categoria

### `sem_cabecalho` → `tratar_sem_cabecalho()`
1. Tenta casar colunas com tabela de referência (`data/columns_*.csv`) por tipos de dados
2. Se matching falhar, infere nomes via `inferencia_colunas.py` (CNPJ, datas, valores monetários, códigos, frentes MCMV, etc.)
3. Converte para estrutura `bem_formada` e aplica pipeline completo

### Cabeçalho deslocado → `tratar_cabecalho_deslocado()`
1. `promover_header()` — move a(s) linha(s) correta(s) para `df.columns`
2. Remove linhas de metadados/posição/totalização do corpo
3. Aplica `tratar_bem_formada()` ao resultado

### Cabeçalho composto → `concatenar_header_composto()`
1. Forward-fill de NaN nas primeiras N linhas (células mescladas)
2. Remove linhas de título (mesmo valor repetido)
3. Concatena no formato `sub (super)` para formar nome final
4. Aplica `tratar_bem_formada()`

### Sub-tabelas → `tratar_sub_tabelas_N()`
Cada subtipo tem sua própria heurística de extração, mas todas:
1. Identificam blocos de dados separados por linhas vazias
2. Extraem cabeçalho de cada sub-tabela
3. Produzem múltiplos DataFrames ou um consolidado
4. Aplicam `tratar_bem_formada()` ao resultado

### `separador_|` → `tratar_separador_pipe()`
1. Expande valores pipe-separados em múltiplas colunas
2. Usa a primeira linha como cabeçalho
3. Aplica `tratar_bem_formada()`

### Descartes
- `vazia` → descartada (reportada com status=discarded)
- `dados_sem_utilidade` → descartada
- Exceções durante tratamento → status=error com mensagem

## Deduplicação

Antes do tratamento, tabelas idênticas (mesmo hash MD5 do conteúdo binário) são agrupadas:

- A primeira em ordem alfabética é eleita **canônica** e recebe tratamento
- As demais são marcadas como **duplicadas** e puladas
- O mapeamento `source_table → canonical_table` é salvo em `_dedup_map.csv`

## Validação pós-tratamento

**Função:** `validar_tabela()` em `validacao.py`

| Métrica | Descrição |
|---------|-----------|
| `shape_ok` | `True` se ≥ 1 linha e ≥ 1 coluna |
| `missing_pct` | % de células com valor NaN no DataFrame tratado |
| `n_rows` / `n_cols` | Dimensões finais |
| `warnings` | Lista de alertas (chaves nulas, colunas vazias removidas, etc.) |

### O que `missing_pct` significa

`(total de células NaN / total de células) × 100`

Exemplo: tabela com 1000 linhas, 10 colunas, 50 células vazias → `50 / 10000 * 100 = 0.5%`

É calculado **após** o tratamento, no DataFrame final. Um valor alto pode indicar:
- Colunas que falharam na coerção de tipo (valores inválidos → NaN)
- Dados originalmente esparsos
- Problemas no pipeline de tratamento

## Relatório de qualidade

Gerado por `saida_tratamento.py:gerar_relatorio_qualidade()` → `_quality_report.csv`

| Coluna | Descrição |
|--------|-----------|
| `table_name` | Nome da tabela original |
| `status` | `treated`, `discarded` ou `error` |
| `n_rows` / `n_cols` | Dimensões do resultado (0 para descartadas) |
| `profile` | Perfil estrutural (lookup, colunar_denso, etc.) ou razão do descarte |
| `institution` | `bb`, `caixa` ou `unknown` |
| `report_date` | Data de referência extraída do nome |
| `missing_pct` | Percentual de valores ausentes |
| `encoding_issues` | Contagem de problemas de encoding |
| `date_parse_errors` | Contagem de falhas no parsing de datas |
| `type_coercion_warnings` | Contagem de warnings de coerção de tipo |
| `error` | Mensagem de exceção (vazia se OK) |

## Saída final

```
data/treated_tables/
├── <nome_tabela>_tratado.csv     (~440 arquivos)
├── _dedup_map.csv                 (mapeamento de duplicatas)
└── _quality_report.csv            (métricas de qualidade)
```

- Formato: **tab-separated** (`sep="\t"`), `index=False`
- Colunas de metadados adicionadas: `source_table`, `report_date`, `institution`, `profile`, `content_hash`

## Onde atuar na revisão

1. **Inspecionar `_quality_report.csv`** — procurar tabelas com:
   - `status=error` (exceções não tratadas)
   - `missing_pct` alto (>30%)
   - `encoding_issues` ou `date_parse_errors` > 0
2. **Abrir CSVs tratados** em `data/treated_tables/` e verificar:
   - Nomes de coluna normalizados corretamente
   - Tipos canônicos aplicados (datas como datetime, valores como float)
   - Encoding reparado (sem caracteres quebrados)
   - Metadados preenchidos (source_table, institution, report_date)
3. **Verificar descartes** — tabelas com `status=discarded`: confirmar que são realmente `vazia` ou `dados_sem_utilidade`
4. **Verificar deduplicação** — `_dedup_map.csv`: confirmar que duplicatas são de fato idênticas
5. **Testar correções**: `uv run python main.py --skip-classify` para reprocessar só o tratamento

## Arquivos relevantes

| Arquivo | Função |
|---------|--------|
| `src/classificacao/tratamento.py` | Pipeline Groups 1–8, roteador `tratar_tabela()` |
| `src/classificacao/tratamento_cabecalho.py` | Cabeçalho deslocado e composto |
| `src/classificacao/tratamento_especiais.py` | Pipe, sem cabeçalho, vazias |
| `src/classificacao/tratamento_subtabelas.py` | Sub-tabelas (4 subtipos) |
| `src/classificacao/inferencia_colunas.py` | Inferência de nomes para `sem_cabecalho` |
| `src/classificacao/deduplicacao.py` | Hash MD5, agrupamento, eleição de canônicas |
| `src/classificacao/validacao.py` | Validação pós-tratamento, `missing_pct` |
| `src/classificacao/saida_tratamento.py` | Escrita de CSVs tratados e relatório de qualidade |
| `main.py` | Entry point — orquestra classificação → dedup → tratamento → qualidade |

## Comandos úteis

```bash
uv run python main.py                        # pipeline completo (classif + dedup + tratamento)
uv run python main.py --skip-classify        # reprocessa tratamento com classificação existente
uv run python main.py --classify-only        # só classificação, sem tratamento
uv run python main.py --inventario           # adiciona inventário e timeline ao final
uv run pytest tests/ -k tratamento           # testes de tratamento
uv run pytest tests/ -k validacao            # testes de validação
```
