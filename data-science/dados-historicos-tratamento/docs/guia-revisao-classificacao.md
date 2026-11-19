# Guia de Revisão — Classificação de Formação

> **Objetivo:** classificar cada uma das 753 tabelas históricas do MCMV em 1 de 17 categorias estruturais, para que cada categoria receba o tratamento adequado no pipeline seguinte.

## Visão geral do pipeline

```
data/table_samples/ (753 CSVs tab-separated)
        │
        ▼
  classificar_formacao()   ← árvore de decisão R1–R8 (regras.py)
        │
        ▼
  comparar_referencia()    ← compara com gabarito autoritativo
        │
        ▼
  data/classificacao_formacao.csv
  data/relatorio_divergencias.md
        │
        ▼
  deduplicação MD5        ← remove duplicatas byte-identical
        │
        ▼
  tratamento por categoria
```

## Formato dos dados de entrada

- **753 arquivos CSV** em `data/table_samples/`
- **Tab-separated** (`sep="\t"`), **não** comma-separated
- A **primeira coluna é índice** de linha do pandas — sempre descartada com `index_col=0`
- Cada arquivo tem **200 linhas** (amostra do dump PostgreSQL)

## Árvore de decisão (D4)

As regras são aplicadas em ordem fixa. A primeira que der match vence.

| Ordem | Regra | Condição | Categoria |
|-------|-------|----------|-----------|
| 1 | **R2** | Nome da tabela contém `tab_arquivos_dados`, `loginfesta` ou `novo_relat_rio_executivo` | `dados_sem_utilidade` |
| 2 | **R1** | Arquivo < 5KB **E** ≤ 1 coluna | `vazia` |
| 3 | **R3** | Células contêm `\|` nos dados (não no header) | `separador_\|` |
| 4 | **R3b** | Header é `HEADER_IS_REAL` (>80% nomes reais, <10% valores de dados) → valida consistência de tipos via **R4** | `bem_formada` |
| 5 | **R3b** | Header é `HEADER_IS_DATA` (>70% valores de dados) → verifica sub-tabelas primeiro, depois densidade | `sem_cabecalho`, `nao_colunares_tipo1`, ou subtipo de `sub_tabelas` |
| 6 | **R3b** | Header é `AMBIGUOUS` → verifica padrão `Posicao:` → depois R5→R6→R7 | várias |
| 7 | **R5** | Sub-tabelas (4 subtipos, testados 1→2→3→4) | `sub_tabelas_1` a `sub_tabelas_4` |
| 8 | **R6** | Cabeçalho composto (2 subtipos, testados 1→2) | `cabecalho_composto_1` ou `_2` |
| 9 | **R7** | Cabeçalho deslocado (6 subtipos) | `cabecalho_na_primeira_linha_1/2`, `cabecalho_na_segunda_linha`, `cabecalho_na_terceira_linha_1/2` |
| 10 | **R8** | Fallback (>15% linhas vazias → `nao_colunares_tipo1`, senão `sem_cabecalho`) | `nao_colunares_tipo1` ou `sem_cabecalho` (confidence=low) |

## As 17 categorias

### 1. `bem_formada`
Todas as colunas têm nomes descritivos. Cada coluna tem tipo consistente (data, número ou texto). Formato ideal — vai direto pro pipeline de tratamento completo.

### 2. `sem_cabecalho`
O que o pandas interpretou como nome de coluna é, na verdade, a primeira linha de dados. Colunas são inferidas por referência cruzada com `inferencia_colunas.py`.

### 3–4. `cabecalho_na_primeira_linha_1` / `_2`
Primeira coluna tem nome descritivo, demais são `unnamed`. A primeira **linha de dados** contém os nomes reais. Subtipo `_2` difere por ter linhas vazias antes de uma linha final de totalização.

### 5. `cabecalho_na_segunda_linha`
Primeira linha contém `"Posicao: DD/MM/YYYY"`. Segunda linha é o cabeçalho real. Dados começam na terceira linha.

### 6–7. `cabecalho_na_terceira_linha_1` / `_2`
Subtipo `_1`: linhas 0 e 1 esparsas/vazias, linha 2 é o cabeçalho. Subtipo `_2`: similar, mas com palavras-chave específicas (`Público-alvo`, `Concluída`, `Total geral`, faixas percentuais).

### 8–9. `cabecalho_composto_1` / `_2`
Cabeçalho formado por múltiplas linhas concatenadas (células mescladas de Excel). Subtipo `_1`: 3 linhas + metadados no rodapé. Subtipo `_2`: 2 linhas.

### 10–13. `sub_tabelas_1` a `_4`
Múltiplas tabelas independentes dentro do mesmo arquivo, separadas por linhas vazias.

- **`_1`**: colunas com timestamp `YYYYMMDD_hhmmss` + `unnamed_0`
- **`_2`**: todas colunas `unnamed` + palavras-chave como `SÍNTESE`, `Faixa`, `Região`, `Quadro de Valores`
- **`_3`**: 1 coluna descritiva + resto `unnamed`, separadas por 1 linha vazia
- **`_4`**: todas `unnamed` com cabeçalhos compostos por bloco

### 14. `separador_\|`
Dados delimitados por pipe (`|`) dentro das células da primeira coluna. Cabeçalho existe, mas precisa expandir os pipes.

### 15. `vazia`
Sem dados relevantes. Arquivo < 5KB com ≤ 1 coluna.

### 16. `dados_sem_utilidade`
Identificadas pelo nome do arquivo. Contêm metadados ou logs, não dados do MCMV.

### 17. `nao_colunares_tipo1`
Dados não organizados em formato tabular (esparsos, sem estrutura de colunas). Fallback do R8 quando >15% de linhas vazias.

## Heurísticas de profiling (R3a)

Cada coluna do DataFrame é classificada em 3 tipos (arquivo `profiling.py`):

| Tipo | Significado |
|------|------------|
| `UNNAMED` | Nome é `unnamed_N`, `Unnamed: N` ou string vazia |
| `DATA_VALUE` | Nome parece um valor de dados (8 dígitos, timestamp `YYYYMMDD_hhmmss`) |
| `REAL_NAME` | Nome contém caracteres alfabéticos — provável nome real de coluna |

## Onde atuar na revisão

1. **Rodar `uv run python main.py --classify-only`** e inspecionar `data/classificacao_formacao.csv`
2. **Conferir divergências** em `data/relatorio_divergencias.md` — cruzar classificação automática vs. referência autoritativa (`classificacao_formacao_revisado_autoritativo.md`)
3. **Para divergências**: abrir o CSV original em `data/table_samples/`, inspecionar visualmente, decidir se o bug está:
   - Na classificação automática (ajustar thresholds/regras em `regras.py`)
   - Na referência autoritativa (atualizar o .md com a categoria correta)
4. **Foco em `confidence=low`** — são tabelas que caíram no fallback R8 e precisam de classificação manual
5. **Verificar falsos positivos/negativos** de sub-tabelas — a heurística R5 é a mais propensa a erro

## Arquivos relevantes

| Arquivo | Função |
|---------|--------|
| `src/classificacao/regras.py` | Árvore R1–R8, constantes, thresholds |
| `src/classificacao/profiling.py` | R3a (classificação de colunas), inspeção de cabeçalho/rodapé |
| `src/classificacao/classificador.py` | Orquestração: itera amostras, compara referência, gera relatório |
| `src/classificacao/carregamento.py` | Leitura dos CSVs tab-separated |
| `classificacao_formacao_revisado_autoritativo.md` | Gabarito manual de referência |
| `data/exemplos_por_categoria.md` | 1 exemplo visual por categoria |
| `data/classificacao_formacao.csv` | Saída da classificação (gitignored) |
| `data/relatorio_divergencias.md` | Relatório de divergências (gitignored) |

## Thresholds ajustáveis

```python
# regras.py
_VAZIA_MAX_BYTES = 5 * 1024       # 5KB — R1
_R3B_DATA_RATIO = 0.7              # R3b: HEADER_IS_DATA se >70% colunas são data
_R3B_REAL_RATIO = 0.8              # R3b: HEADER_IS_REAL se >80% colunas são nomes
_R3B_DATA_RATIO_LOW = 0.1          # R3b: e <10% são data values
_R5_EMPTY_RATIO = 0.15             # R5/R8: >15% linhas vazias
```

## Comandos úteis

```bash
uv run python main.py --classify-only     # só classificação
uv run python main.py --skip-classify     # pula classificação, carrega CSV existente
uv run pytest tests/ -k classificador     # testes de classificação
```
