# Roteiro de Apresentação — Pipeline MCMV: Classificação, Tratamento e Features para ML


## Introdução 


O pipeline tem 4 estágios:

```
Stage 1: Classificação    → 17 categorias estruturais (R1–R8)
Stage 2: Inventário       → implícito na saída da classificação
Stage 3: Tratamento       → normalização por categoria (Groups 0–8)
Stage 4: Enriquecimento   → inventário + score preditivo + relatório de qualidade
```

Os dados vêm de um dump PostgreSQL com 753 tabelas de duas instituições:
- **Banco do Brasil** (`bb_`): relatórios mensais ao Min. Cidades, empreendimentos, contratos
- **Caixa** (`caixa_`): relatórios executivos PMCMV, contratação, entregas, sínteses

Frentes do MCMV cobertas: FAR, Rural, Entidades, FGTS, PNHR, PNHB, FDS, CCFGTS.

Repositório disponível em [https://github.com/GovHub-br/data-application-cidades/tree/feat/tratamento-dados-historicos](https://github.com/GovHub-br/data-application-cidades/tree/feat/tratamento-dados-historicos).

---

## Classificação das Tabelas (Issue #96)

### O problema

As 753 tabelas têm formatações heterogêneas: algumas são colunares com cabeçalho adequado, outras não têm nomes de coluna, outras têm dados dispersos em formato de relatório. Para tratar cada tabela corretamente, primeiro precisamos **classificar** sua estrutura.

### A árvore de decisão (R1–R8)

A classificação é feita por uma árvore de decisão em `src/classificacao/regras.py`, função `classificar_formacao()`. As regras são aplicadas em ordem fixa — a primeira que der match vence:

| Ordem | Regra | Condição (resumida) | Categoria |
|-------|-------|---------------------|-----------|
| 1 | R2 | Nome contém `tab_arquivos_dados`, `loginfesta`, `novo_relat_rio_executivo` | `dados_sem_utilidade` |
| 2 | R1 | Arquivo < 5KB e ≤ 1 coluna | `vazia` |
| 3 | R3 | Células contêm `\|` nos dados | `separador_\|` |
| 4 | R3b | >80% colunas com nomes reais, <10% com valores de dados → valida consistência de tipos (R4) | `bem_formada` |
| 5 | R3b | >70% colunas com valores de dados → verifica sub-tabelas primeiro | `sem_cabecalho`, `nao_colunares`, ou `sub_tabelas_*` |
| 6 | R3b | Ambíguo → verifica padrão `Posicao:` → depois R5→R6→R7 | várias |
| 7 | R5 | Sub-tabelas (4 subtipos, testados 1→2→3→4) | `sub_tabelas_1` a `_4` |
| 8 | R6 | Cabeçalho composto (2 subtipos) | `cabecalho_composto_1` ou `_2` |
| 9 | R7 | Cabeçalho deslocado (6 subtipos) | `cabecalho_na_primeira_linha_1/2`, `_segunda_linha`, `_terceira_linha_1/2` |
| 10 | R8 | Fallback (>15% linhas vazias → `nao_colunares`, senão `sem_cabecalho`) | `nao_colunares_tipo1` ou `sem_cabecalho` (confidence=low) |

### As 17 categorias

> Mostrar `data/exemplos_por_categoria.md` — tem 1 exemplo visual de cada categoria.

Distribuição atual das 753 tabelas:

| Categoria | Qtd | Categoria | Qtd |
|-----------|-----|-----------|-----|
| `bem_formada` | 351 | `cabecalho_composto_1` | 4 |
| `sub_tabelas_1` | 273 | `dados_sem_utilidade` | 4 |
| `cabecalho_composto_2` | 50 | `sem_cabecalho` | 3 |
| `cabecalho_na_primeira_linha_1` | 36 | `cabecalho_na_segunda_linha` | 2 |
| `sub_tabelas_3` | 19 | demais (1 cada) | 6 |
| `separador_\|` | 5 | | |

### Heurística de profiling (R3a)

Antes da árvore, cada coluna do DataFrame é classificada em 3 tipos (`profiling.py`):

| Tipo | Significado |
|------|-------------|
| `UNNAMED` | Nome é `unnamed_N`, `Unnamed: N` ou vazio |
| `DATA_VALUE` | Nome parece um valor de dados (8 dígitos, timestamp) |
| `REAL_NAME` | Nome contém caracteres alfabéticos — provável nome real |

A proporção desses tipos na tabela determina a decisão R3b.

### O que a issue #96 pede

> "A issue #96 pede para validar se a classificação está correta e coerente. O trabalho é:"

1. **Revisar classificação atual vs realidade** — comparar o que o algoritmo classificou com a estrutura real da tabela
2. **Validar consistência entre classificação automática e manual** — o gabarito está no arquivo `classificacao_formacao_revisado_autoritativo.md`
3. **Identificar tabelas mal classificadas** — foco em `confidence=low` (caíram no fallback R8)
4. **Identificar tabelas sem classificação clara** — categoria `indeterminada`
5. **Documentar divergências** — o relatório `data/relatorio_divergencias.md` já mostra onde o algoritmo diverge do gabarito

### Artefatos para a issue #96

| Artefato | Caminho | Para que serve |
|----------|---------|---------------|
| **Gabarito autoritativo** | `classificacao_formacao_revisado_autoritativo.md` | Referência manual com 17 categorias e exemplos |
| **Classificação automática** | `data/classificacao_formacao.csv` | Saída do algoritmo: `table_name, formacao, confidence, notes` |
| **Relatório de divergências** | `data/relatorio_divergencias.md` | Onde automático ≠ gabarito |
| **Exemplos visuais** | `data/exemplos_por_categoria.md` | 1 CSV real por categoria |
| **Amostras** | `data/table_samples/` (753 CSVs tab-separated) | Dados brutos para inspeção |
| **Schema do dump** | `data/columns_202605211425_perfil_cidades_dados_historicos.csv` | Nomes e tipos de colunas do PostgreSQL |
| **Código da árvore** | `src/classificacao/regras.py` | Regras R1–R8 e thresholds |
| **Profiling** | `src/classificacao/profiling.py` | Heurística R3a (UNNAMED/DATA_VALUE/REAL_NAME) |
| **Guia de revisão** | `docs/guia-revisao-classificacao.md` | Documento de contexto para a equipe |

### Como rodar

```bash
uv run python main.py --db --classify-only     # só classificação
# Depois inspecionar:
#   data/classificacao_formacao.csv
#   data/relatorio_divergencias.md
```

---

## Tratamento das Tabelas (Issue #97)

### O problema

Cada categoria de tabela precisa de um tratamento diferente para chegar a um formato colunar limpo, normalizado e pronto para ML. O roteador `tratar_tabela()` em `tratamento.py` despacha cada tabela para o handler correto.

### Fluxo do tratamento

```
classificacao_formacao.csv
        │
        ▼
  Deduplicação MD5 (deduplicacao.py)
  ├─ agrupa por hash binário
  ├─ elege 1 canônica por grupo
  └─ gera _dedup_map.csv
        │
        ▼
  Roteador tratar_tabela()
  ├─ bem_formada           → tratar_bem_formada()
  ├─ sem_cabecalho         → tratar_sem_cabecalho()
  ├─ cabecalho_*           → tratar_cabecalho_deslocado()
  ├─ cabecalho_composto_*  → concatenar_header_composto()
  ├─ sub_tabelas_*         → tratar_sub_tabelas_N()
  ├─ separador_|           → tratar_separador_pipe()
  ├─ vazia / sem_utilidade → DESCARTE
  └─ fallback              → tratar_bem_formada() + warning
        │
        ▼
  Validação (validacao.py)
  ├─ shape (≥1 linha, ≥1 coluna)
  ├─ chaves não nulas (apf, cod_, cnpj, contrato, nr_)
  └─ missing_pct
        │
        ▼
  data/treated_tables/*_tratado.csv
  data/treated_tables/_quality_report.csv
```

### Pipeline de `bem_formada` (Groups 0–8)

Todas as outras categorias são convertidas para `bem_formada` e depois passam por este pipeline:

| Grupo | Etapa | O que faz |
|-------|-------|-----------|
| **G0** | Limpeza pré-tratamento | Remove linhas vazias/dash-only e colunas 100% NaN |
| **G1** | Normalização de nomes | lowercase, remove acentos (NFKD), substitui especiais por `_`, colapsa underscores |
| **G2** | Separador decimal | Se >50% dos valores têm vírgula, substitui `,`→`.` e converte para `float64` |
| **G3** | Parsing de datas | Colunas com `dat_`/`dt_`/`data_`: detecta ISO ou BR, aplica `to_datetime`. Segurança: se >10% falha, reverte |
| **G4** | Reparo de encoding | `charset-normalizer` (confiança >0.7) + fallback `ftfy.fix_text()` |
| **G5** | Canonização de tipos | Manual: `cnpj`→str, `cod_`/`nr_`/`nu_`→str, `vlr_`/`valor_`/`total_`→float64, `qtd_`/`qt_`→Int64, `dat_`→datetime. Fallback: inventário PostgreSQL |
| **G6** | Classificação de perfil | `lookup` (≤3 cols), `event_level` (4-6 + chave repetida), `agregado_uf` (7-9 + total), `colunar_denso` (demais) |
| **G7** | Período e instituição | Data do nome do arquivo (5 padrões regex) + `bb`/`caixa`/`unknown` |
| **G8** | Metadados | Adiciona: `source_table`, `report_date`, `institution`, `profile`, `content_hash` |

### Tratamentos específicos por categoria

| Categoria | Transformação principal |
|-----------|------------------------|
| `cabecalho_na_primeira_linha_*` | Promove linha 0 → header, descarta linha 0 |
| `cabecalho_na_segunda_linha` | Descarta linha 0 (`Posicao:DD/MM/YYYY`), promove linha 1 → header |
| `cabecalho_na_terceira_linha_*` | Descarta linhas 0-1, promove linha 2 → header |
| `cabecalho_composto_1` | Forward-fill 3 linhas + concatena com `_` + remove rodapé |
| `cabecalho_composto_2` | Forward-fill 2 linhas + concatena com `_` |
| `sem_cabecalho` | Inferência de nomes via `inferencia_colunas.py` (CNPJ, datas, valores, códigos) ou fallback `col_0`, `col_1`, ... |
| `separador_\|` | Expande pipe em múltiplas colunas |
| `sub_tabelas_1` | Split por ≥2 linhas vazias + wide-to-long + classifica `recorte_tipo` (frente/faixa/uf/regiao/total/suat) |
| `sub_tabelas_2` | Split por keywords (`SÍNTESE`, `Faixa`, `Região`, `Quadro de Valores`) |
| `sub_tabelas_3` | Split por 1 linha vazia + reconstrói header multi-linha |
| `sub_tabelas_4` | Split por blocos text-heavy + reconstrói header |

### Deduplicação

Antes do tratamento, tabelas idênticas (mesmo hash MD5 do conteúdo binário) são agrupadas:
- A primeira em ordem alfabética é eleita **canônica** e recebe tratamento
- As demais são marcadas como **duplicadas** e puladas
- **309 duplicatas** (41% do total) — a tabela com mais cópias tem 272

### O que a issue #97 pede

> "A issue #97 pede para validar se os tratamentos estão corretos e suficientes:"

1. **Revisar limpeza dos dados** — G0: linhas vazias/dash-only removidas corretamente?
2. **Revisar padronização de campos** — G1: nomes normalizados? G5: tipos canônicos aplicados?
3. **Revisar tratamento de datas e valores** — G3: datas parseadas? G2: separador decimal?
4. **Revisar duplicidades** — `_dedup_map.csv`: duplicatas são de fato idênticas?
5. **Revisar consistência de encoding e tipos** — G4: sem caracteres quebrados? G5: coerção falhou?
6. **Identificar tratamentos faltantes ou excessivos** — alguma tabela não tratada que deveria? Algum tratamento desnecessário?

### Artefatos para a issue #97

| Artefato | Caminho | Para que serve |
|----------|---------|---------------|
| **Tabelas tratadas** | `data/treated_tables/*_tratado.csv` (~440 arquivos) | Saída do tratamento, tab-separated |
| **Relatório de qualidade** | `data/treated_tables/_quality_report.csv` | 12 colunas com métricas por tabela |
| **Mapa de deduplicação** | `data/treated_tables/_dedup_map.csv` | Mapeamento `source_table → canonical_table` |
| **Schema do dump** | `data/columns_*.csv` | Referência de tipos para G5 (canonização automática) |
| **Código do pipeline** | `src/classificacao/tratamento.py` | Groups 0–8 + roteador `tratar_tabela()` |
| **Tratamento de cabeçalho** | `src/classificacao/tratamento_cabecalho.py` | Cabeçalho deslocado e composto |
| **Tratamentos especiais** | `src/classificacao/tratamento_especiais.py` | Pipe, sem cabeçalho, vazias |
| **Sub-tabelas** | `src/classificacao/tratamento_subtabelas.py` | 4 subtipos |
| **Inferência de colunas** | `src/classificacao/inferencia_colunas.py` | Nomes para `sem_cabecalho` |
| **Validação** | `src/classificacao/validacao.py` | Shape, chaves, missing_pct |
| **Guia de revisão** | `docs/guia-revisao-tratamento.md` | Documento de contexto para a equipe |
| **Relatório detalhado** | `docs/relatorio-tratamento-v1.md` | Exemplos antes/depois de cada categoria |

### Colunas do `_quality_report.csv`

| Coluna | Descrição |
|--------|-----------|
| `table_name` | Nome da tabela original |
| `status` | `treated`, `discarded` ou `error` |
| `n_rows` / `n_cols` | Dimensões finais (0 para descartadas/erro) |
| `profile` | Perfil estrutural ou razão do descarte |
| `institution` | `bb`, `caixa` ou `unknown` |
| `report_date` | Data extraída do nome (`YYYY-MM-DD`) |
| `missing_pct` | % de células NaN: `(NaN / total_cells) × 100` |
| `encoding_issues` | Contagem de problemas de encoding |
| `date_parse_errors` | Colunas com >10% de falha no parse de datas |
| `type_coercion_warnings` | Warnings de coerção de tipo |
| `error` | Mensagem de exceção (vazia se OK) |

### Como rodar

```bash
uv run python main.py --db                        # pipeline completo
uv run python main.py --db --skip-classify        # reprocessa só tratamento
uv run python main.py --db --inventario           # tratamento + inventário + qualidade
```

---

## Identificação de Features para ML

> "Depois de classificar e tratar, o script identifica quais tabelas têm features úteis para o modelo preditivo de paralizações. Isso é feito pelo módulo de inventário."

### Dimensões preditivas detectadas

O módulo `inventario.py` analisa cada tabela tratada e detecta 6 dimensões preditivas:

| Dimensão | Padrões buscados nos nomes de coluna | Peso no score |
|----------|---------------------------------------|---------------|
| **status_obra** | `situacao`, `status_obra`, `paralisad`, `concluid`, `andamento` | +2 |
| **datas** (≥3 colunas) | `dt_`, `data_`, `dat_`, `cronograma_datatermino`, `dataprevista` | +2 |
| **progresso** | `percentual_obra`, `_executada`, `concluidas`, `entregues`, `em_obras`, `prc_execucao` | +2 |
| **financeiro** | `valor_`, `vlr_`, `vr_`, `subsidio`, `emprestimo`, `investimento`, `desembolsado` | +1 |
| **geolocalizacao** | `municipio` + `uf` ou `codigo_ibge` | +1 |
| **granularidade_contrato** | `apf`, `cod_contrato`, `nr_`, `nu_apf`, `idregistro`, `codigo_empreendimento` | +1 |

### Score de utilidade preditiva (0–10)

```
score = 0
+ status_obra presente        → +2
+ ≥3 colunas de data          → +2
+ progresso presente          → +2
+ financeiro presente         → +1
+ geolocalizacao presente     → +1
+ granularidade_contrato      → +1
+ perfil colunar_denso/event_level → +1
- perfil agregado_uf         → -2
- perfil lookup/vazia/sem_utilidade → -3
- tabela duplicada           → -3
- tabela não tratada         → -1

Classificação: 8-10 = Alta | 5-7 = Média | 2-4 = Baixa | ≤1 = Nenhuma
```

### Resultado atual

| Classificação | Qtd tabelas | % |
|---------------|-------------|---|
| **Alta** (8-10) | 163 | 22% |
| **Média** (5-7) | 41 | 5% |
| **Baixa** (2-4) | 208 | 28% |
| **Nenhuma** (≤1) | 341 | 45% |



### Qualidade por instituição

| Instituição | N tabelas | Missing % médio | % com status obra | % com financeiro |
|-------------|-----------|-----------------|-------------------|------------------|
| bb | 231 | 5.82% | 27.7% | 95.7% |
| caixa | 125 | 8.61% | 65.6% | 76.8% |
| unknown | 88 | 6.86% | 61.4% | 70.5% |


### Artefatos para identificação de features

| Artefato | Caminho | Para que serve |
|----------|---------|---------------|
| **Inventário de dados** | `data/inventario_dados.csv` | 753 linhas com 23 colunas: formação, status, frentes, período, dimensões preditivas, score, campos úteis |
| **Relatório de qualidade** | `data/relatorio_qualidade_dados.md` | Sumário executivo com scoring, duplicatas, qualidade por perfil/instituição |
| **Linha do tempo** | `data/linha_do_tempo.png` | Distribuição temporal das tabelas por frente e período |
| **Código do inventário** | `src/classificacao/inventario.py` | Detecção de frentes, dimensões, scoring, geração de relatório |

### Colunas-chave do `inventario_dados.csv`

| Coluna | Descrição |
|--------|-----------|
| `frentes_cobertas` | Frentes MCMV detectadas (FAR, FGTS, Rural, etc.) |
| `periodo_dados_inicio` / `_fim` | Período temporal dos dados na tabela |
| `score_utilidade_preditiva` | Score 0–10 |
| `classificacao_utilidade` | Alta / Média / Baixa / Nenhuma |
| `tem_status_obra` | Bool — tem colunas de status/paralisação? |
| `tem_datas` | Int — quantas colunas de data? |
| `tem_progresso` | Bool — tem colunas de progresso da obra? |
| `tem_financeiro` | Bool — tem colunas financeiras? |
| `tem_geolocalizacao` | Bool — tem município + UF? |
| `tem_granularidade_contrato` | Bool — tem APF/contrato? |
| `campos_uteis_preditiva` | Lista dos nomes de coluna que matcharam dimensões |

> "A coluna `campos_uteis_preditiva` é o ponto de partida para o feature engineering do modelo."

### Como rodar

```bash
uv run python main.py --inventario    # pipeline completo + inventário + qualidade + timeline
```

---

## Bloco 4 — Como as issues se conectam

> "As duas issues se conectam no pipeline:"

```
Issue #96 (classificação)          Issue #97 (tratamento)
        │                                  │
        ▼                                  ▼
  classificacao_formacao.csv    →    tratar_tabela()    →    _quality_report.csv
        │                                  │                      │
        └──────────┬───────────────────────┘                      │
                   ▼                                              ▼
          inventario_dados.csv  ←  consolidação  ←  relatorio_qualidade_dados.md
                   │
                   ▼
          Feature #53 (modelo preditivo de paralizações)
```

- **Issue #96** garante que a classificação está correta → se a categoria estiver errada, o tratamento errado é aplicado
- **Issue #97** garante que o tratamento está correto → se o tratamento falhar, os dados chegam sujos no modelo
- **Ambas** impactam o `inventario_dados.csv` e o `score_utilidade_preditiva` → que determina quais tabelas o modelo vai usar

### Pontos de atenção para a revisão

1. **`confidence=low`** na classificação → tabelas que caíram no fallback R8, precisam de classificação manual
2. **`status=error`** no quality report → exceções durante o tratamento, precisam de debug
3. **`missing_pct` alto** (>30%) → pode indicar problema no tratamento ou dados originalmente esparsos
4. **`sub_tabelas_3` e `_4`** têm missing_pct de 53% e 64% → verificar se o tratamento de sub-tabelas está perdendo dados
5. **309 duplicatas** (41%) → confirmar que são de fato idênticas e que a eleição de canônicas está correta
6. **151 tabelas sem período identificado** e **239 sem frente identificada** → podem ser descartadas ou precisam de ajuste na extração

---

## Encerramento

> "Resumindo: a issue #96 é sobre validar se a classificação automática está correta comparando com o gabarito. A issue #97 é sobre validar se o tratamento está produzindo dados limpos e normalizados. O inventário consolida tudo e identifica quais tabelas têm features preditivas. Os documentos em `docs/` e os relatórios em `data/` são o ponto de partida."

### Próximos passos sugeridos

1. Ler os guias: `docs/guia-revisao-classificacao.md` e `docs/guia-revisao-tratamento.md`
2. Rodar `uv run python main.py --classify-only` e inspecionar `data/relatorio_divergencias.md`
3. Rodar `uv run python main.py --inventario` e inspecionar `data/inventario_dados.csv` e `data/relatorio_qualidade_dados.md`
4. Focar inicialmente em `confidence=low` e `status=error`
5. Dividir as 753 tabelas entre os revisores (por categoria ou por instituição)

### Referências rápidas

| Precisa de... | Onde olhar |
|---------------|-----------|
| Entender as 17 categorias | `classificacao_formacao_revisado_autoritativo.md` |
| Ver exemplos visuais | `data/exemplos_por_categoria.md` |
| Ver classificação automática | `data/classificacao_formacao.csv` |
| Ver divergências | `data/relatorio_divergencias.md` |
| Ver tabelas tratadas | `data/treated_tables/*_tratado.csv` |
| Ver métricas de qualidade | `data/treated_tables/_quality_report.csv` |
| Ver mapa de duplicatas | `data/treated_tables/_dedup_map.csv` |
| Ver features preditivas | `data/inventario_dados.csv` |
| Ver sumário de qualidade | `data/relatorio_qualidade_dados.md` |
| Ver linha do tempo | `data/linha_do_tempo.png` |
