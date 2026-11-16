# Checkpoint: dados históricos do MCMV (branch `feat/historico-staging`)

Retrato de 16/11/2026: o que está pronto, o que ficou rodando e o que falta. O detalhe de
cada parte está em `dags/dados_historicos/README.md`.

## Objetivo

Reconstruir a série histórica do MCMV a partir do dump congelado do banco antigo (754 CSVs em
`raw/dados_historicos/`, subidos à mão) e levar até o medalhão. A divisão de trabalho é:

- **DAG** (`dags/dados_historicos/dados_historicos_dag.py`): formato. Vai até o cleansed.
- **dbt** (`dbt/mcid/models/dados_historicos_dbt/`): significado. Faz bronze, silver e,
  depois, a gold.

A base serve também ao TCC (prevenção de paralisação), que fica fora deste branch.

## Pronto

### DAG por Asset (raw → staging → analytics → cleansed)

**Commits:** `3f606e75` e `56b5e6bd`, mais o desta rodada.

**Estrutura:**
- Uma DAG só, `dados_historicos_dag`, disparada pelo Asset da raw.
- Cinco tasks: `conferir_raw`, `raw_para_staging`, `staging_para_analytics`,
  `analytics_para_cleansed` e `mapear_colunas`.
- Toda a lógica fica no módulo da DAG, com S3Hook e Assets. Não há scripts nem plugin.

**Cleansed** (`tests/cleansed/dados_historicos/<familia>/`):
- 434 saídas em 54 famílias, mais 16 versões substituídas;
- 14.776.048 linhas, número de antes do reprocessamento descrito abaixo.

**Tratamento:**
- desfaz a duplicação 2× do dump;
- cabeçalho por arquivo (mapa no MinIO);
- descartes com motivo: vazia, total, subtítulo, rodapé, zerada e duplicada;
- competência e data de corte;
- padronização de formato: datas ISO, ponto decimal, sem `.0`, sem ruído de float, CNPJ com
  14 dígitos;
- escolha de versão.

**Conferências:**
- linhas e células conservadas em cada arquivo;
- conferência independente (notebook 05): 0 células não equivalentes.

**Correções desta rodada**, vindas da tipagem da silver:
- **Vírgula decimal sem parte inteira** (`,00`, `,01`). Deixava colunas inteiras do INT040
  sem conversão. Agora vale em qualquer coluna, porque não pode ser milhar.
- **Total sem rótulo apontado no mapa.** Coluna nova `linhas_total` no `cabecalhos.csv`. É
  usada na última linha do relatório executivo de 30/09/2012 (v3): 1.277.386 UH e R$ 71,3 bi,
  99% da soma das linhas, sem rótulo. Se a linha apontada não existir, o tratamento para.
- Testes da DAG: `uv run pytest tests/test_dados_historicos_dag.py`, 50 passando.

**Mapas** (`cabecalhos.csv`, `familias.csv`, `de_para_colunas.csv`): ficam só no MinIO, em
`tests/config/dados_historicos/`, nunca no repositório. O `cabecalhos.csv` de lá já tem a
coluna `linhas_total`.

### Notebooks (`notebooks/dados_historicos/`)

- `00`: visão geral.
- `01`/`02`: entrada BB.
- `03`: grafo de nomes de coluna.
- `04`: estudo das colunas e dicionário. O mapa de chaves foi aprovado.
- `05`: conferência da padronização.
- `06`: validação da silver da cadeia do APF da Caixa. **Ainda não executado** (ver "Falta").

### dbt: bronze (Etapa 1, concluída)

**Modelos:**
- `bronze_dhist_<familia>`, uma por família (54);
- `bronze_dhist_cleansed_log`.

**Como lê:**
- `read_parquet` sobre a pasta da família, com `union_by_name`;
- a raiz vem da var `dhist_cleansed_prefixo`; hoje é `tests/cleansed/dados_historicos/`.

**Validação 1, verde:** as linhas por saída e as colunas por família batem com o log. São 54
bronzes e 14.776.048 linhas, e as versões substituídas ficam de fora.

### dbt: silver da cadeia do APF da Caixa (Etapa 2, construída, falta validar)

**Famílias:**
- `relatorio_executivo_caixa` (2011-12 a 2018-08);
- `datas_entregas_pj_caixa`;
- `int040_far_caixa`;
- `snh_af_caixa`;
- `snh_entregas_af_caixa`.

**De-para:** `dbt/mcid/macros/dados_historicos/de_para.sql`. Cada coluna canônica lista as
colunas de origem e o tipo; tudo vem das equivalências provadas no notebook 04.

**O que a silver faz:**
- `try_cast` por tipo;
- competência e corte, tirados do log;
- IBGE de 7 dígitos, pelo seed `ibge_municipio`;
- situação canônica, pelos seeds `data_quality/dominio_status` (intocado) e
  `seeds/dados_historicos/dominio_status_extra.csv` (os 6 valores que faltavam);
- grão igual ao da fonte.

**Testes** (`dbt/mcid/tests/dados_historicos/`):
- linhas da silver contra a bronze;
- tipagem sem perda;
- cobertura do de-para;
- situação no domínio;
- formato do APF.

### Isolamento

- Tudo do dbt novo só existe no target `staging_duckdb`, um arquivo DuckDB local que lê o
  MinIO.
- Fica fora do `prod`/`prod_duckdb`, do Cosmos, do OpenMetadata e do banco compartilhado. A
  conferência foi feita com `dbt ls --target prod` e com `tests/test_dagbag.py`.
- O `mcmv_historico_dbt` do Vitor não foi tocado.

## Estava rodando no momento do push

- `analytics_para_cleansed` está reprocessando com as duas correções, e `mapear_colunas` roda
  em seguida (Airflow local, `airflow tasks test`).
- Se a execução cair, basta rodar de novo: o cleansed refaz tudo e só publica se todos os
  arquivos passarem.
- A bronze e a silver que estão no DuckDB local são **de antes** dessas correções.

## Falta (em ordem)

1. **Conferir o reprocessamento.** No `_cleansed_log`, o relatório executivo de 30/09/2012 v3
   deve ter 1 `total` a mais; no `_cleansed_padronizacoes`, devem aparecer as colunas do INT040
   com `numero_decimal_br`. Depois, rodar o notebook 05 de novo.
2. **Refazer bronze, silver e testes:**
   ```bash
   cd dbt/mcid   # com MINIO_* e DUCKDB_MCID_PATH no ambiente
   dbt build --target staging_duckdb --indirect-selection=cautious \
     --select path:models/dados_historicos_dbt path:tests/dados_historicos path:seeds/dados_historicos ibge_municipio dominio_status
   ```
   - Se o MinIO der timeout HTTP numa bronze grande (já aconteceu com a `bext`), rodar só ela
     com `DUCKDB_MCID_THREADS=1`.
   - O teste `dhist_prata_tipagem_sem_perda` tem de ficar verde. Antes das correções, ele
     acusava 8 casos, todos tratados.
   - `--indirect-selection=cautious` evita puxar os testes do Vitor que dependem do seed
     `dominio_status` e dos modelos dele, que não existem no DuckDB local.
3. **Executar o notebook 06:**
   ```bash
   uv run -q --with nbconvert --with ipykernel jupyter nbconvert --to notebook --execute --inplace \
     notebooks/dados_historicos/06_validacao_silver_caixa.ipynb
   ```
   Conferir que a saída não tem dado pessoal: o repositório é público.
4. **Validação 2 (Lucas)**, a seção 7 do notebook 06:
   - Nomes canônicos que não vieram do dicionário: `id_registro_caixa`, `nome_proponente`,
     `mes_conclusao` e os `*_ano_ref` do SNH.
   - `mes_conclusao`: a `data_conclusao` da Caixa vem como AAAAMM, de 2008-06 a 2018-09. O
     significado exato ainda está por confirmar.
   - Classificação das situações novas, sobretudo as marcadas `pendente`:
     `critica - somente habitacao`, `obra frustrada` e `novo`.
   - `valor_subsidio_ogu`: só 38% dos valores do mesmo APF se repetem de 2015-04 para 2015-05.
5. **Etapa 3, silver do BB:**
   - famílias: `entrada_bb`, `entrada_bb_aux_far`, `entrega_bb_pj`, `int054_far_bb`,
     `snh_af_bb`, `snh_entregas_af_bb` e `pnhr_bb`;
   - `codigo_empreendimento_bb` e `apf` em colunas separadas, com o INT054 como ponte;
   - percentual 0–1 vira 0–100. O PNHR mistura as duas escalas e pede regra por valor.
6. **Etapa 4, silver da contratação PF:** `bext`, `relatorio_cidades(_entregas)_caixa`,
   `entrega_bb_pf_pf` e `entrega_bb_pj_pf`, todas pela chave `codigo_contrato_pf`. Essas
   famílias têm dado pessoal mascarado; não mostrar valores em notebook.
7. **Etapa 5:** silver das demais famílias (relatórios agregados e arquivos únicos).
8. **Etapa 6, gold** (plano próprio): fatos e dimensões do modelo dimensional, juntando o
   histórico às fontes vivas. Inclui o estoque de UH por APF, que é a soma do movimento do
   relatório executivo.
9. **Sair de `tests/`** quando tudo estiver validado: mudar a var `dhist_cleansed_prefixo` e os
   prefixos da DAG. A ordem e o momento são decisão do Lucas.

## Decisões em aberto, de rodadas anteriores

- **Conteúdo idêntico em meses diferentes** hoje vira versão substituída. São as 6 tabelas
  "Relatório 2" de 2011.
- **Bases 5c e 5c1 de 2012-04:** não há critério de escolha, então ficaram as duas.
- **Pasta diferente do nome:** 11 arquivos de corte do começo do mês arquivados na pasta do mês
  anterior. A recomendação é usar o mês anterior.
- **Arquivos pj dos lotes de 2015.**
- **Questões da seção 8 do notebook 04.**

## Como retomar em outra máquina

- **Lake:** credenciais do MinIO no `.env` (`MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`,
  `MINIO_SECRET_KEY`, `MINIO_BUCKET`).
- **DAG local:** o passo a passo está no README da DAG. Conexão `minio_lake` (tipo `aws`, com
  `endpoint_url`) e `AIRFLOW__CORE__DAGS_FOLDER=dags/dados_historicos`.
- **dbt:**
  - exportar as variáveis do `.env`;
  - `DUCKDB_MCID_PATH` apontando para um arquivo local (o padrão é `/tmp/cidades.duckdb`);
  - se faltar memória, ajustar `DUCKDB_MCID_MEMORY_LIMIT`;
  - na primeira vez numa máquina, a bronze leva cerca de 10 minutos.
- **Regras do trabalho:**
  - no MinIO, só se escreve em `tests/`; `raw/` e a `staging/` de produção não são tocadas;
  - nada roda contra o banco compartilhado;
  - mapas por arquivo nunca entram no repositório;
  - o pre-commit está quebrado: usar `--no-verify` e rodar `uvx ruff` só nos arquivos tocados.
