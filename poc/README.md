# POC — bronze como parquet tipado no MinIO (DuckDB só na bronze)

Valida a arquitetura em que a **bronze deixa de ser tabela no Postgres e passa a ser parquet
tipado no MinIO**, exposto ao Postgres como view. Silver e gold continuam em dbt-postgres:
o DuckDB entra só onde é preciso ler parquet.

```
SFTP ──► raw/ ──► mascaramento ──► MANIFESTO ──► bronze/ parquet TIPADO ──► view ──► silver/gold
         (inalterado)               (novo)       (dbt-duckdb, ~1 model     (novo)   (dbt-postgres,
                                                  por família)                       inalterado)
```

Some da arquitetura: `staging/*.parquet` (all-text) e as **2.011 tabelas** de `sftp.*`.

**Isolada por construção.** Nada fora de `poc/` foi alterado. Infra própria (MinIO 9100,
Postgres 5442), venv própria, bucket próprio. Produção acessada **somente para leitura**, e
só por `scripts/baixar_amostra_prod.py`, que copia apenas objetos aprovados pelo mascaramento.

---

## Veredito

**Funciona, com dados reais do lake.** A cadeia completa roda ponta a ponta sobre 16 arquivos
reais da família `CAIXA_AF_GEHIS_ANDAMENTO_OBRA` — 2 encodings, 3 delimitadores e um arquivo
com header corrompido — e entrega **4.241 linhas**, contagem idêntica à do pipeline atual.

| critério | resultado |
|---|---|
| bronze parquet tipada a partir de arquivos reais heterogêneos | ✅ 5 grupos de leitura unidos, tipos `INTEGER`/`DATE`/`VARCHAR` |
| view expõe a bronze ao Postgres sem materializar | ✅ 4.241 linhas, tipos corretos no catálogo, **0 bytes** |
| silver em dbt-postgres sobre a view | ✅ 3.738 linhas, `distinct on`, CTE, ILIKE, tabela tipada |
| incremental sem reprocessar o histórico | ✅ 3 partições novas, 8 intactas, 0 reescritas, 0 perdidas |
| contagem preservada em toda a cadeia | ✅ 4.241 = 4.241 (pipeline atual × proposta) |
| UDFs Postgres continuam válidos sobre a bronze | ❌ **não** — ver "A restrição que eu errei" |

### Duas correções ao que eu disse antes

**1. "Schema por arquivo" estava errado — é por família.** O manifesto agrupa os arquivos
por layout: **2.703 arquivos em produção = 641 famílias**, e as 30 maiores cobrem 66% do lake.
`CAIXA_AF_GEHIS_ANDAMENTO_OBRA_M*` são 219 arquivos e **um** model. Além disso, a enumeração
de colunas da view é **gerada do footer do parquet**, não escrita à mão — é o que
`staging_para_db.py` já faz hoje (`_ler_metadados` + `_select_texto`), trocando o
`CAST(... AS VARCHAR)` fixo pelo tipo que vem do parquet. O único artefato humano por família
é um YAML de ~20 linhas (`dbt_poc/tipos/*.yml`).

**2. A restrição que eu errei: os UDFs Postgres NÃO funcionam sobre a bronze.** Eu havia
afirmado que a Opção 2 preservava os UDFs. Medido, não preserva: quando uma query toca a view
do pg_duckdb, ela é executada **inteira no DuckDB**, que não conhece funções definidas no
Postgres.

| construção sobre a view | funciona? |
|---|---|
| `distinct on`, CTE, window function, join | ✅ |
| `ILIKE`, `CASE WHEN`, `upper/trim`, `regexp_replace(...,'g')` | ✅ |
| `~` (regex Postgres), `current_date`, casts | ✅ |
| `~*` (regex case-insensitive) | ❌ `Scalar Function with name ~* does not exist` |
| **UDF Postgres** (`parse_date_br`, `normalize_apf`) | ❌ `Scalar Function with name parse_date_br does not exist` |

O mesmo UDF funciona normalmente sobre uma tabela Postgres comum — o que quebra é a
combinação com a view.

**Na prática isso não dói nesta arquitetura**, e a razão é o ponto central: o trabalho desses
UDFs é transformar texto cru em tipo, e isso agora acontece na **bronze**, no DuckDB. A silver
recebe `DATE` e `INTEGER` prontos. A POC portou os três UDFs para `CREATE MACRO` do DuckDB
(`dbt_poc/macros/poc_duckdb_macros.sql`) e eles rodam lá. O que sobra para a silver é SQL
padrão — que funciona.

Onde isso **dói**: se um model de silver/gold precisar de um UDF Postgres sobre a bronze,
não dá. A saída é converter o UDF em macro Jinja (que expande para SQL inline) ou materializar
antes.

---

## Economia de armazenamento

### Família real (16 arquivos, 4.241 linhas) — `resultados/medicoes_familia.md`

| | artefatos | tamanho |
|---|---|---|
| **hoje** | 16 parquets all-text + **16 tabelas VARCHAR** no Postgres | **1,417 MB** |
| **proposta** | 11 partições de parquet tipado + 1 view | **0,051 MB** |

**Ressalva honesta:** as 16 tabelas somam 1,319 MB para 4.241 linhas — média de 82 KB por
tabela, dominada por **overhead fixo do Postgres por relação**, não por volume de dado. Os 96%
não significam que parquet tipado comprime 28x melhor que texto.

Mas o overhead é **real, não artefato da POC**: a arquitetura de hoje cria *uma tabela por
arquivo*. O schema `sftp` em produção já tem **2.011 tabelas** para 2.703 arquivos; só esta
família teria 219. É esse custo por relação que a consolidação em família + view elimina.

### Efeito isolado da tipagem — `resultados/medicoes.md`

Com o dataset sintético (50.000 linhas, um arquivo, sem overhead de tabela):

| efeito | comparação | resultado |
|---|---|---|
| tipagem no parquet | mesmo engine, com e sem tipo | **14,3%** |
| engine (pyarrow → DuckDB) | ambos sem tipo | 13,1% |
| eliminar a cópia varchar no Postgres | a tabela some | **57,5% do total** |

**Leitura:** tipar o parquet economiza 14% — parquet já comprime texto muito bem com
dictionary+RLE. O ganho dominante é **não materializar o dado numa tabela relacional**.

---

## Como a bronze absorve a bagunça real

Os dados do lake não são uniformes nem dentro de uma mesma família. Medido em
`sftp._staging_log` de produção:

- **encoding varia dentro da família** em 26 de 183 famílias
- **o schema deriva** em 59 de 183

Na família testada, os 16 arquivos exigiram **5 grupos de leitura distintos**:

```
10x  encoding=utf-8   delim='|'
 3x  encoding=cp1252  delim='|'
 1x  encoding=cp1252  delim='\t'
 1x  encoding=cp1252  delim=';'
 1x  encoding=utf-8   delim='|'   [header inutilizável: nomes vindos do YAML]
```

O codegen emite um `read_csv` por grupo e costura tudo com `UNION ALL BY NAME`. Três defeitos
reais foram encontrados e tratados:

**Header com menos campos que os dados** (`M20220908`): header com 4 campos, linhas com 8. Sem
`null_padding=true` o DuckDB nem consegue farejar o arquivo. Com ele, os campos excedentes
viram `column4..column7` e o YAML os reconcilia. **O pandas descarta esses campos em silêncio**
— o `situacao_obra` desse arquivo se perde inteiro hoje; na bronze ele é preservado (1.087/1.087).

**Header e dados com delimitadores diferentes** (`M20230602`): header separado por TAB, dados
por PIPE. `lake_utils.detectar_dialeto` decide olhando só o header, então **o pipeline atual
também produz lixo** para esse arquivo. O manifesto passou a validar o delimitador contra as
linhas de dados: 124 linhas que eram lixo viraram 124 linhas válidas.

**Header repetido como linha de dados** (2 arquivos): sobrevive como 2 linhas com `anomes`
nulo, na partição `NULL`. Preservadas, não descartadas.

---

## Incremental — `resultados/incremental.md`

A materialização `external` faz `COPY <relation> TO <location>`: recomputa o model inteiro a
cada run. Com 219 arquivos por família isso não escalaria. A solução testada:
`partition_by` + `overwrite_or_ignore`, com o model **gerado só sobre os arquivos do lote**.

| lote | arquivos | partições novas | intactas | reescritas | perdidas |
|---|---|---|---|---|---|
| 1 | 13 | 8 | — | — | — |
| 2 | 3 | **3** | **8** | **0** | **0** |

O custo de cada run é proporcional ao que chegou, não ao histórico.

**A tabela de controle não morre.** É o manifesto que decide, por `(minio_key, source_hash)`,
quais arquivos entram no lote — o dbt não faz isso sozinho. É a mesma chave de idempotência
que `_staging_log` usa hoje.

---

## Bloqueador de encoding — explorado e reduzido a um resíduo pequeno

Detalhe completo em `resultados/bloqueador_c1.md`. O README chamava isso de bloqueador
duro; ao testar a mitigação sugerida no plano (reescrever os objetos em UTF-8), apareceu
algo mais importante: **não era 1 problema, eram 2 empilhados.**

**1. Falso alarme (4 dos 5 arquivos do lake com bytes C1).** `lake_utils.detectar_encoding`
testa os 5 bytes indefinidos em cp1252 (`0x81 0x8D 0x8F 0x90 0x9D`) **antes** de tentar
UTF-8. Esses mesmos bytes são o segundo byte de sequências UTF-8 válidas (`Í`=`C3 8D`,
`Ó`=`C3 93`...), então um arquivo **genuinamente UTF-8** com essas letras é classificado
como latin-1 por engano — inclusive `dados_historicos/caixa_001_2016_grafico_mcmv`, o
arquivo real citado no plano original como o exemplo do bloqueio. Testado: dos 5 arquivos
com bytes C1, **4 decodificam como UTF-8 de ponta a ponta**. É a mesma raiz do bug já visto
nesta POC (`Código Cliente` → `ca3digo_cliente`) — até então diagnosticado só como
"mojibake", sem a causa identificada. Uma função corrigida
(`gerar_manifesto.detectar_encoding_corrigido`, só nesta POC) tenta UTF-8 primeiro; os 4
arquivos passam a ser lidos direto de `raw/`, sem reescrever nada.

**2. Resíduo genuíno (1 dos 5): reescrita para UTF-8, testada e com fidelidade provada.**
Só `clientes_cp1252_patologico.csv` (sintético, deliberadamente pathológico) não decodifica
como UTF-8 nem cp1252 de verdade. `gerar_manifesto.reescrever_utf8` baixa o objeto,
decodifica com **latin-1** (mapeia os 256 valores de byte, nunca falha — é o que a produção
já faz hoje) e reencoda como UTF-8 numa cópia em `raw_utf8/` — `raw/` nunca é tocado, mesma
política do `staging/` de hoje. Comparação célula a célula (1.000 linhas × 7 colunas) entre
DuckDB lendo a cópia e pandas lendo o original com `encoding='latin-1'` (a leitura de
produção hoje): **100% idênticas**. É troca de container, não reinterpretação do dado.

**Prova de ponta a ponta:** as duas famílias viraram bronze completa via `dbt run` real
(não só `duckdb.sql`) — `caixa_grafico_mcmv` (16 linhas, direto de `raw/`, `PREPARAÇÃO` e
`CONCLUÍDAS` corretos no Postgres) e `clientes_cp_patologico` (1.000 linhas, de
`raw_utf8/`, tipos `date`/`numeric`/`boolean` corretos). `PASS=3 WARN=0 ERROR=0`.

**Não usar `ignore_errors`**: ele descarta linhas em silêncio e o `count(*)` mente sobre a
perda (sofre pushdown e conta linhas do arquivo, não linhas entregues). Medido:
`cp1252 + ignore_errors` → 990 de 1.000 linhas; `utf-8 + ignore_errors` → 54 de 1.000. A
reescrita substitui esse atalho perigoso por uma cópia auditável.

**O que fica pendente, fora do escopo desta POC (que não altera o pipeline atual):**
corrigir a ordem de `lake_utils.detectar_encoding` em produção (resolve a maioria dos casos
e também o mojibake que hoje entra em produção em silêncio, independente desta arquitetura)
e decidir se a reescrita do resíduo roda automaticamente no manifesto (como aqui) ou fica
marcada para revisão manual — a amostra tem 1 caso, mas 641 famílias reais podem ter mais.

---

## Formatos que continuam exigindo Python — `resultados/formatos_negativos.md`

| formato | DuckDB | conclusão |
|---|---|---|
| `.xls` (OLE2/BIFF) | `IO Error: Failed to open zip for reading` | segue ignorado; `xlrd` leria (testado) |
| `.mdb` / `.accdb` | sem leitor JET | `mdbtools` continua obrigatório |

O pré-passo Python **não desaparece; encolhe**. Continua necessário para converter MDB/XLS,
detectar encoding e delimitador (o DuckDB exige que sejam declarados — só detecta BOM) e
calcular `_source_hash`.

**XLSX pede estratégia própria:** `all_varchar=true` é certo para CSV, mas **destrói datas em
planilha** — a célula já é tipada e `all_varchar` devolve o serial do Excel (`45839`). O
parser de data brasileira recebe um número e devolve NULL em 100% das linhas.

---

## Armadilhas do pg_duckdb (todas custaram tempo)

| sintoma | causa | solução |
|---|---|---|
| view com 1 coluna `duckdb."row"` | `SELECT *` não é decomposto | enumerar `r['coluna']` |
| todas as colunas `USER-DEFINED` | sem CAST o tipo não chega ao catálogo | CAST explícito em cada coluna |
| `syntax error at or near "{"` | `hive_types` recebe struct do DuckDB, o **parser do Postgres** rejeita | não usar; o CAST da view restaura o tipo |
| `column "hive_partitioning" does not exist` | argumento nomeado com `=` vira referência de coluna | usar `:=` |
| chave de partição vem como texto `'NULL'` | valor nulo vira o diretório `col=NULL` | `nullif(r['col'], 'NULL')::tipo` |

---

## Orquestração: o astronomer-cosmos continua servindo, mas precisa ser recomposto

Isso não é um passo da POC — nada aqui foi testado com cosmos, só investigado contra o que
já existe em produção. Registro porque muda a forma como as DAGs precisam ser desenhadas, e
vale alinhar com o time antes de qualquer implementação.

### Duas coisas diferentes, as duas rotuladas de "revalidação"

**1. Versão.** Em produção (`requirements.txt`, o que de fato builda a imagem, não o
`pyproject.toml`/lockfile) está pinado `dbt-postgres==1.7.13` (que traz `dbt-core` 1.7.13
junto) com `astronomer-cosmos==1.9.0`. `dbt-duckdb` **não existe para a linha 1.7** — a
bronze desta arquitetura só existe a partir de um dbt-core mais novo (a POC rodou com
`dbt-core==1.11.12` / `dbt-duckdb==1.10.1` / `dbt-postgres==1.11.0`). O upgrade do dbt-core
é pré-requisito só pra bronze existir, **independente do cosmos**. Mas como o cosmos também
tem sua própria faixa de compatibilidade com a versão do dbt-core — e o lockfile do projeto
já está em `astronomer-cosmos==1.13.1`, mais novo que o 1.9.0 que roda hoje — os dois
upgrades (dbt-core e cosmos) precisam ser validados juntos contra as 3 DAGs que já existem
(`airflow_lappis/dags/dbt/{mcid,ipea,mir}/cosmos_dag.py`), não só contra a bronze nova.

**2. Estrutura — o ponto mais relevante para esta arquitetura.** As 3 DAGs cosmos de hoje
são cada uma um `DbtDag` independente: a granularidade do cosmos ali é **um projeto dbt =
um `ProfileConfig` = um adapter** — os três `profiles.yml` (mcid, ipea, mir) usam
exclusivamente `type: postgres`. Um `DbtDag` sozinho não mistura dois profiles/adapters no
mesmo grafo de tasks.

Com esta arquitetura passam a existir **dois projetos dbt com adapters diferentes na mesma
esteira**: `dbt_poc` (duckdb, escreve a bronze) e `dbt_silver` (postgres, lê a view e
escreve a silver) — exatamente como a POC estruturou, rodando os dois via `dbt run` direto
na CLI, **sem cosmos envolvido em nenhum dos dois**. Isso não é uma limitação do cosmos, é
um limite de como ele é usado hoje neste projeto: a lib também oferece `DbtTaskGroup`, que
permite compor **vários projetos/profiles dentro do mesmo DAG orquestrador** — um
`TaskGroup` duckdb (bronze) e um `TaskGroup` postgres (silver), com uma dependência
explícita entre eles (último task do grupo bronze antes do primeiro do grupo silver). As
DAGs atuais nunca fizeram essa composição — são `DbtDag` isolados, um por projeto, não
task groups combinados num DAG maior. "Revalidar" aqui quer dizer: provar que dois
`DbtTaskGroup` com adapters diferentes convivendo no mesmo DAG se comportam do jeito que a
arquitetura pede (ordem de execução, propagação de falha entre os grupos, logs/retries por
task). Nada disso foi testado nem nesta POC.

**O que fica de fora dos dois `DbtTaskGroup`:** o manifesto (`gerar_manifesto.py`) e a
geração das views (`gerar_views_bronze.py`) não são models dbt — são passos Python comuns.
Eles entram como tasks Airflow normais **entre** os dois task groups (manifesto antes da
bronze, geração de views entre bronze e silver), do jeito que a DAG de SFTP já intercala
passos Python com outras etapas hoje.

### Resumindo para o time

Cosmos não é descartado por esta arquitetura. O que muda é a forma como as DAGs são
montadas: hoje é "1 DAG cosmos = 1 projeto dbt = 1 adapter"; a bronze/silver exige "1 DAG
orquestrador com 2 `DbtTaskGroup` (adapters diferentes) + tasks Python intercaladas". É uma
mudança estrutural nas DAGs, não uma troca de ferramenta — mas precisa de um ciclo de teste
próprio antes de virar produção, separado do teste de viabilidade que esta POC já fechou.

---

## Como reproduzir

```bash
cd poc

uv venv .venv --python 3.11
uv pip install --python .venv/bin/python "dbt-core==1.11.12" "dbt-duckdb==1.10.1" \
  "duckdb==1.4.5" dbt-postgres boto3 pandas pyarrow openpyxl xlrd xlwt \
  psycopg2-binary python-dotenv pyyaml
cp .env.exemplo .env

docker compose up -d

# amostra real do lake (exige VPN); as guardas de PII estão no script
.venv/bin/python scripts/baixar_amostra_prod.py --familia CAIXA_AF_GEHIS_ANDAMENTO_OBRA \
  --limit 12 --apply
.venv/bin/python scripts/semear_minio.py --dir amostra_real

# 1. manifesto: encoding, delimitador, família, bloqueios
.venv/bin/python scripts/gerar_manifesto.py

# 2. codegen do model bronze a partir do manifesto + tipos/<familia>.yml
.venv/bin/python scripts/gerar_models_bronze.py

# 3. bronze: parquet tipado particionado no MinIO
cd dbt_poc && export DBT_PROFILES_DIR=$PWD && set -a && . ../.env && set +a
../.venv/bin/dbt run --select bronze_caixa_andamento_obra && cd ..

# 4. view: expõe a bronze ao Postgres, 0 bytes
.venv/bin/python scripts/gerar_views_bronze.py

# 5. silver: dbt-POSTGRES sobre a view
cd dbt_silver && export DBT_PROFILES_DIR=$PWD && set -a && . ../.env && set +a
../.venv/bin/dbt run && cd ..

# medições e experimentos
.venv/bin/python scripts/medir_familia.py       # as duas arquiteturas, família real
.venv/bin/python scripts/teste_incremental.py   # 2 lotes, partições intactas
.venv/bin/python scripts/matriz_encoding.py     # o bloqueador de encoding
.venv/bin/python scripts/testes_negativos.py    # .xls e .mdb

docker compose down -v   # limpa tudo
```

Versões: dbt-core 1.11.12, dbt-duckdb 1.10.1, dbt-postgres 1.11.0, DuckDB 1.4.5,
pgduckdb 17-v1.1.1, MinIO `RELEASE.2025-04-22T22-12-26Z`.

## Estrutura

```
poc/
├── docker-compose.yml         MinIO 9100 + pgduckdb 5442, volumes próprios
├── dbt_poc/                   projeto dbt-DUCKDB — só a bronze
│   ├── profiles.yml           secret S3 do MinIO
│   ├── tipos/*.yml            ÚNICO artefato humano: a semântica por família
│   ├── macros/                UDFs do mcid portados para CREATE MACRO
│   └── models/bronze/         GERADOS pelo codegen — não editar à mão
├── dbt_silver/                projeto dbt-POSTGRES — silver sobre a view
├── scripts/
│   ├── gerar_manifesto.py     detecção + famílias + reescrita UTF-8 do resíduo de encoding
│   ├── gerar_models_bronze.py codegen manifesto + YAML -> model
│   ├── gerar_views_bronze.py  footer do parquet -> CREATE VIEW tipada
│   ├── medir_familia.py       compara as duas arquiteturas
│   └── teste_incremental.py   dois lotes, verifica partições
└── resultados/                saídas medidas (gitignored)
```

## O que falta decidir antes de produção

1. **Encoding** — deixou de ser bloqueador duro (ver seção acima e
   `resultados/bloqueador_c1.md`), mas duas decisões seguem em aberto: corrigir a ordem de
   `lake_utils.detectar_encoding` em produção, e decidir onde roda a reescrita do resíduo
   genuíno (automática no manifesto, como testado aqui, ou revisão manual por família).
2. **Upgrade do dbt-core** 1.7.13 → 1.11.x e recomposição do `astronomer-cosmos` — ver
   seção dedicada acima. Não existe dbt-duckdb para a linha 1.7, e as DAGs cosmos de hoje
   assumem 1 projeto dbt = 1 adapter por DAG; esta arquitetura precisa de 2 (`DbtTaskGroup`
   duckdb + postgres compostos num DAG orquestrador).
3. **Agrupamento em famílias** — a heurística de `gerar_manifesto.familia()` é regex sobre o
   nome do arquivo. Funcionou nas amostras, mas 641 famílias merecem revisão manual antes de
   virar contrato.
4. **Onde o manifesto e o codegen rodam** — provavelmente tasks da DAG antes do `dbt run`,
   com o lote saindo da comparação `(minio_key, source_hash)`.
5. **Mascaramento de PII** — continua pré-passo Python sobre `raw/`, sem alteração. O que muda
   é o *gate*: o dbt não sabe checar a tag `masked=true`, então a dependência precisa ficar
   explícita no Airflow.
