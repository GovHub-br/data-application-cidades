# MCMV — Classificação e Tratamento de Dados Históricos

Pipeline de tratamento e classificação de dados históricos do Programa Minha Casa
Minha Vida (MCMV/PMCMV) como base para um sistema de machine learning de previsão,
alerta e sugestão de ações sobre possíveis paralizações nas obras.

Objetivo de responder à perguntas como:
- Quais frentes do MCMV estão cobertas;
- Quais schemas, tabelas e campos estão disponíveis;
- Qual período histórico existe por base;
- Quais dados podem ser usados na análise preditiva;
- Quais limitações de acesso, completude ou qualidade existem.

Este pipeline faz parte de projeto de Inteligência Preditiva de Paralizações de Obras do MCMV.


---

## Contexto

Os dados históricos do MCMV foram disponibilizados como um dump em um banco de
dados **PostgreSQL**. O dump contém bases históricas das diferentes frentes do programa —
FAR, Rural, Entidades, FGTS, PNHR, PNHB e outras — com tabelas provenientes
principalmente de duas instituições financeiras:

| Fonte | Prefixo típico | Descrição |
|---|---|---|
| Banco do Brasil (BB) | `bb_` | Relatórios mensais ao Min. Cidades, tabelas de empreendimentos, propostas, contratos |
| Caixa Econômica Federal | `caixa_` | Relatórios executivos PMCMV, bases de contratação, entregas, sínteses |

As tabelas possuem formatações heterogêneas: algumas são colunares com cabeçalho
adequado, outras não possuem nomes de colunas, outras têm dados dispersos em forma
de relatório. Para viabilizar o uso em ML, é necessário primeiro **classificar** as
tabelas por estrutura e depois **tratar** cada categoria para normalizá-las em
DataFrames colunares limpos.

### Amostras de trabalho

Para o projeto definitivo, serão disponibilizadas **amostras de 200 linhas** em
arquivos CSV de cada tabela, com o objetivo de classificá-las em grupos conforme
formatação e disponibilidade de nomes de colunas.

---

## Objetivo

Construir um pipeline reproduzível que:

1. **Inventarie** os dados históricos disponíveis (schemas, tabelas, campos, períodos)
2. **Classifique** cada tabela em uma categoria estrutural
3. **Trate** cada categoria para normalizar os dados em formato colunar limpo
4. **Enriqueça** com metadados (período, coluna-código, frente PMCMV)
5. **Disponibilize** dados tratados como base para o modelo preditivo de paralizações

> O escopo atual cobre as etapas 1–4 (mapeamento, classificação e tratamento).
> Extração, ingestão via DAG, dbt e modelo preditivo são etapas futuras da feature #53.

---

## Categorias de Classificação quanto à formação

As categorias no arquivo "classificacao_formacao_revisado_autoritativo" foram verificadas manualmente e são a referência autoritativa. O objetivo de classificar quanto à formação é poder definir um tipo de tratamento para cada categoria, fazendo com que as tabelas fiquem formatadas de forma adequada para uso posterior para ML.


---

## Outras Classificações

### Possui APF, código de empreendimento, ou código de identificação
Verificar quais tabelas possuem colunas que podem identificar os empreendimentos. Verificar se coluna tem nome com "apf" ou "empreendimento" e/ou possui registros numéricos que poder utilizados como chaves.

### Frente Coberta
Verificar qual ou quais frentes do projeto estão cobertas nas tabelas: FAR, Rural, Entidades, FGTS, PNHR, PNHB e outras.

### Período Histórico
Qual período histórico (ano-mês) está coberto em cada tabela.

### Qualidade dos dados
Classificação quanto à qualidade dos dados contidos na tabela e se podem ser utilizados em análise preditiva.

---

## Stack do Projeto

O projeto utiliza ambiente Python3, com ambiente virtual gerenciado pelo uv, e pode utilizar pacotes como pandas, numpy, scipy, scikit-learn, SQLAlchemy com psycopg2-binary, matplotlib e outras bibliotecas

---

## Passo a Passo — Uso

### Pré-requisitos

- **Python 3.11+** — versão testada: 3.11. Instalação via pyenv ou sistema.
- **uv** — gerenciador de pacotes e ambientes Python. Instale com:

  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```

- **Dados de entrada** — as amostras CSV devem estar em `data/table_samples/`. O arquivo de schema `data/columns_202605211425_perfil_cidades_dados_historicos.csv` também é necessário para a etapa de inventário.
- **Para modo DB:** arquivo `.env` com credenciais do PostgreSQL (copie de `.env.example`) e network namespace `protected` com túnel OpenVPN + proxy `socat`.

### 1. Clonar e preparar o ambiente

```bash
git clone <url-do-repo> && cd dados-historicos-tratamento
uv sync
```

O comando `uv sync` cria um ambiente virtual e instala todas as dependências (runtime + dev). A versão do Python é lida automaticamente de `.python-version`.

### 2. Executar o pipeline completo (modo CSV — padrão)

```bash
uv run python main.py
```

O pipeline executa sequencialmente:

| Etapa | O que faz | Saída |
|---|---|---|
| Classificação | Classifica as 753 amostras em ~17 categorias estruturais (R1–R8) | `data/classificacao_formacao.csv` |
| Comparação | Compara resultado com a referência autoritativa e gera relatório de divergências | `data/relatorio_divergencias.md` |
| Deduplicação | Agrupa tabelas idênticas por hash MD5 e elege uma versão canônica de cada grupo | `data/treated_tables/_dedup_map.csv` |
| Tratamento | Trata as tabelas canônicas conforme sua categoria (normalização, parsing, encoding, etc.) | `data/treated_tables/<nome>_tratado.csv` |
| Qualidade | Gera relatório com métricas de qualidade (missing%, rows, cols) por tabela tratada | `data/treated_tables/_quality_report.csv` |

Ao final, o terminal exibe um sumário por categoria e as contagens de tabelas tratadas, descartadas e erros.

### 3. Flags do modo CSV

| Flag | Efeito |
|---|---|
| `--skip-classify` | Pula a etapa de classificação e carrega resultados de `data/classificacao_formacao.csv`. Útil para reprocessar apenas o tratamento após ajustes. |
| `--classify-only` | Executa **apenas** a classificação, sem dedup nem tratamento. Gera `data/classificacao_formacao.csv` e `data/relatorio_divergencias.md`. |
| `--inventario` | Após o tratamento, executa etapa adicional de inventário, relatório de qualidade e gráfico de linha do tempo. |

As flags `--skip-classify` e `--classify-only` são **mutuamente exclusivas**. `--inventario` é ignorado com `--classify-only`.

Exemplos:

```bash
uv run python main.py --classify-only          # só classificação
uv run python main.py --skip-classify           # reprocessa tratamento com classificação existente
uv run python main.py --skip-classify --inventario  # reprocessa + inventário
```

### 4. (Opcional) Executar com etapa de inventário

```bash
uv run python main.py --inventario
```

Adiciona após o tratamento:

| Etapa | O que faz | Saída |
|---|---|---|
| Inventário | Consolida classificação, dedup, qualidade e tratamento em um inventário unificado | `data/inventario_dados.csv` |
| Relatório de qualidade | Gera relatório Markdown com análises de completude, cobertura e qualidade dos dados | `data/relatorio_qualidade_dados.md` |
| Linha do tempo | Gera gráfico de distribuição temporal das tabelas por frente e período | `data/linha_do_tempo.png` |

### 5. Executar pipeline no modo DB (PostgreSQL)

Com a flag `--db`, o pipeline opera diretamente sobre o banco PostgreSQL em vez das amostras CSV. As tabelas completas são lidas do schema `dados_historicos`, classificadas, tratadas e persistidas no schema `dados_historicos_formatados`.

**Pré-requisitos adicionais:**

- **Credenciais** no arquivo `.env` (use `.env.example` como template). Nunca comitar `.env`.


```bash
# Comando de execução
uv run main.py --db [flags]
```

**Flags suportadas no modo DB:**

| Flag | Efeito |
|---|---|
| `--db` | Ativa modo PostgreSQL (schema `dados_historicos` → `dados_historicos_formatados`) |
| `--skip-classify` | Carrega classificação do PostgreSQL (tabela `_classificacao`) ou fallback para `data/classificacao_formacao_db.csv` |
| `--classify-only` | Executa apenas classificação e persiste no schema target |

**Pipeline DB — etapas:**

| Etapa | O que faz | Destino |
|---|---|---|
| Classificação | Classifica tabelas completas via PostgreSQL | Schema target (`_classificacao`) + CSV local |
| Comparação | Compara com referência autoritativa e gera relatório de divergências | `data/relatorio_divergencias_db.md` |
| Deduplicação | Agrupa tabelas por hash de conteúdo e elege canônicas | Schema target (`_dedup_map`) |
| Tratamento | Trata tabelas canônicas conforme categoria | Schema target (tabelas `*_tratado`) |
| Qualidade | Métricas de qualidade por tabela processada | Schema target (`_quality_report`) + `data/treated_tables/_quality_report.csv` |

> **Nota:** No modo DB, a classificação pode divergir dos resultados CSV pois opera sobre tabelas completas, não sobre amostras de 200 linhas. As divergências são registradas em `data/relatorio_divergencias_db.md`.

### 6. Executar testes

```bash
uv run pytest
```

### 7. Lint, formatação e type-check

```bash
uv run ruff check .         # lint
uv run ruff format .        # formatação automática
uv run mypy .               # verificação de tipos
```

### Adicionar dependências

```bash
uv add <pacote>             # dependência de runtime
uv add --dev <pacote>       # dependência de desenvolvimento
```

### Estrutura de saída esperada

**Modo CSV:**

```
data/
├── classificacao_formacao.csv          # classificação das 753 tabelas
├── relatorio_divergencias.md           # divergências vs. referência autoritativa
├── table_samples/                      # amostras de entrada (753 CSVs, tab-separated)
├── treated_tables/
│   ├── _dedup_map.csv                  # mapeamento de duplicatas → canônicas
│   ├── _quality_report.csv             # métricas de qualidade por tabela
│   └── <nome>_tratado.csv              # ~440 tabelas tratadas
├── inventario_dados.csv                # (--inventario) inventário unificado
├── relatorio_qualidade_dados.md        # (--inventario) relatório de qualidade
└── linha_do_tempo.png                  # (--inventario) gráfico temporal
```

**Modo DB:**

```
Schema: dados_historicos_formatados (PostgreSQL)
├── _classificacao                      # classificação de todas as tabelas
├── _dedup_map                          # mapeamento de duplicatas
├── _quality_report                     # métricas de qualidade
└── <nome>_tratado                      # tabelas tratadas (~440)

Arquivos locais (para referência offline):
├── data/classificacao_formacao_db.csv  # classificação (modo DB)
├── data/relatorio_divergencias_db.md   # divergências vs. referência
└── data/treated_tables/_quality_report.csv  # métricas de qualidade
```

---

## Guias para a equipe

Documentos de contextualização para quem vai atuar na revisão do pipeline:

| Documento | Público |
|---|---|
| [`docs/guia-revisao-classificacao.md`](docs/guia-revisao-classificacao.md) | Revisão da classificação — árvore R1–R8, 17 categorias, thresholds, heurísticas |
| [`docs/guia-revisao-tratamento.md`](docs/guia-revisao-tratamento.md) | Revisão do tratamento — pipeline Groups 1–8, deduplicação, validação, relatório de qualidade |

---

## Informações Adicionais

A pasta `data/` contém os dados do projeto.

O arquivo `data/columns_202605211425_perfil_cidades_dados_historicos.csv` possui informações das tabelas e os nomes e tipos das colunas, gerado a partir de query ao banco de dados PostgreSQL.

A pasta `data/table_samples/` armazena as amostras das tabelas do dump de dados, no formato CSV **tab-separated** (`sep="\t"`), com 200 linhas cada. Os arquivos possuem uma coluna de índice na primeira posição, que deve ser descartada (`index_col=0`).

A pasta `data/treated_tables/` recebe as tabelas tratadas após o pipeline de classificação e tratamento dos dados.
