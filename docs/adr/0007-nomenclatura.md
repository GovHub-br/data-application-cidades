# 0007. Nomenclatura

- **Status:** Proposto
- **Data:** 2026-10-19
- **Revisão:** 2026-10-29 (contagens conferidas com o código; coluna `_load_id`; DAG da entrada manual; sem schema `comum`; nomes de rastreio em inglês; schema `elementary`; alcance da regra do inglês; chaves do contrato)
- **Relacionados:**
  - ADR-0002 (camadas), ADR-0004 (tasks), ADR-0005 (chaves do lake), ADR-0010
  (divisão por secretaria)
  - Deck *GovHub — Arquitetura Medalhão de Referência* seção 7
  - `dbt/mcid/models/docs/padrao-nomenclatura-tabelas-dbt.md`,
  `dbt/mcid/macros/get_custom_schema.sql`, `dbt/mcid/macros/add_metadata_timestamps.sql`

## Contexto

O deck sugere padrões para schemas, tabelas e colunas: o schema leva a sigla da
secretaria em minúsculas (`snh`, `dph`), e a tabela leva a camada no prefixo. Ele
deixa buckets e DAGs para definir com o ministério.

O código segue o deck nas tabelas, mas não nos schemas. Os 267 modelos de bronze,
prata e ouro têm o prefixo da camada (81 `bronze_`, 85 `prata_`, 101 `ouro_`,
contando um piloto desabilitado e os quatro de `conjuntura_dbt/qualidade/`), mas
os schemas são um por camada (`bronze`, `prata`, `ouro`). O documento normativo
(`padrao-nomenclatura-tabelas-dbt.md`) ainda descreve `silver_`/`gold_` e schemas
`silver`/`gold`. Quem lê o documento aprende um padrão que o código abandonou.

O ADR-0010 decidiu que a divisão por secretaria é por schema, com as três camadas
dentro do schema de cada secretaria.

A macro `add_metadata_timestamps` gera `dt_ingest`, `dt_silver` e `dt_gold`, com
os nomes antigos das camadas, e nenhum modelo a chama hoje. As colunas de linhagem
da staging (ADR-0002 R2) já usam prefixo `_` (`_source_file`, `_ingested_at`,
`_source_hash`).

## Decisão

### Idioma

- **As camadas do medalhão ficam em português:** bronze, prata e ouro, nos
  prefixos, nas pastas e nos documentos.
- **Os nomes técnicos de rastreio ficam em inglês:** colunas técnicas, pasta de
  carga no lake, status das tabelas de controle (`lake._*_log`) e tasks do
  Airflow. É a convenção da documentação e das ferramentas de mercado, e é o que
  já existe no código (`_source_file`, `_ingested_at`, `masked`, `converted`,
  `extract`, `transform`).
- As colunas de negócio seguem os glossários do projeto, em português.
- **Alcance da regra:** vale para os nomes que descrevem ou rastreiam o dado e
  aparecem fora do código: colunas técnicas, status das tabelas de controle,
  pastas do lake, chaves e valores do contrato da fonte e ids de DAG e de task.
  Identificadores de código (macros, funções, scripts, nomes de arquivo) e de
  infraestrutura (pools, papéis do Postgres) seguem a língua do código em volta
  e não são renomeados por esta regra.
- **Nomes que já existem** mudam quando o domínio ou a DAG for migrado, no mesmo
  PR. Não há PR só de renomeação. Durante a migração, a `fonte_lake` lê o
  `meta.path` e, na falta dele, o `meta.caminho`.

| Chave do contrato hoje ou proposta | Nome em inglês | Valores |
|---|---|---|
| `meta.caminho` | `meta.path` | — |
| `tipo_carga` | `load_type` | `full`, `incremental` |
| `variacao_maxima_linhas` | `max_row_variation` | — |
| `padrao_arquivo` | `file_pattern` | — |
| `responsavel` | `owner` | — |
| `formato` | `format` | `date_br`, `date_iso`, `decimal_br`, `decimal_dot`, `integer`, `text`, `domain` |

### Camadas

As camadas se chamam **bronze, prata e ouro**, no prefixo das tabelas e dos
arquivos de modelo, nas pastas e na documentação. `silver` e `gold` não aparecem em
nome novo.

### Schemas: um por secretaria

| Schema | Conteúdo | Exemplo |
|---|---|---|
| `<sigla da secretaria>` | Bronze, prata e ouro dos dados da secretaria | `snh.ouro_far_resumo_gerencial` |
| `seeds`, `metadata`, `lake`, `elementary` | Auxiliares técnicos; `elementary` guarda as tabelas do pacote Elementary (ADR-0006) | — |

- A sigla é a da secretaria, em minúsculas (`snh`, `dph`).
- **Não há schema compartilhado.** Cada secretaria tem bronze, prata e ouro no
  próprio schema. Uma fonte usada por mais de uma secretaria é carregada no schema
  de cada uma, e o dado se repete. Hoje só a SNH usa o warehouse, então não há
  repetição; como compartilhar fica para quando outra secretaria chegar (ADR-0010).
- O schema de fallback (`dbt_default`) fica sempre vazio, como já garante o teste
  `sem_recurso_no_schema_de_fallback`.
- Como as três camadas e vários domínios dividem o mesmo schema, o nome da tabela
  precisa ser único dentro dele. O token de fonte ou domínio garante isso.

### Tabelas e arquivos de modelo

O arquivo `.sql` e a tabela têm o mesmo nome.

| Camada | Padrão | Token | Exemplo |
|---|---|---|---|
| Bronze | `bronze_<fonte>_<objeto>` | A fonte ou canal (`sftp`, `shpt`, `ibge`, `bacen`, `fgv`) | `bronze_shpt_monit_cad_pj_far_mensal` |
| Prata | `prata_<dominio>_<objeto>` | O domínio (`far`, `fds`, `rural`, `conjuntura`) | `prata_rural_empreendimento` |
| Ouro | `ouro_<dominio>_<produto>` | O domínio | `ouro_far_resumo_gerencial` |

O token vem logo depois do prefixo da camada. A lista de tokens de domínio, e de
qual secretaria cada domínio é, fica registrada em
`padrao-nomenclatura-tabelas-dbt.md`. Domínio novo entra lá e no `dbt_project.yml`
no mesmo PR.

### Pastas do dbt

`models/<dominio>_dbt/{bronze,prata,ouro}/`. O schema vem da secretaria dona do
domínio, declarado uma vez por pasta de domínio no `dbt_project.yml` (`+schema:
snh`). A pasta de camada não define schema: ela só agrupa os arquivos e carrega as
configurações da camada, como os grants (ADR-0011).

### Colunas

- `snake_case`, minúsculas, sem acento e sem espaço. Na staging, a conversão faz
  isso mecanicamente (ADR-0002 R2).
- Na prata, nomes semânticos por extenso, seguindo `glossario-mcid.md` e
  `glossario-valores-financeiros.md`. Datas com prefixo `dt_`.
- Colunas pseudonimizadas (ADR-0002 R0) levam o sufixo `_token`: `cpf_token`,
  `nis_token`.

### Colunas técnicas

Toda coluna técnica começa com `_`, o que a separa das colunas de negócio em
qualquer `select *`.

| Coluna | Onde nasce | Significado |
|---|---|---|
| `_source_file` | Staging | Objeto de origem na raw |
| `_load_id` | Staging | Identifica a carga: o momento do recebimento, da pasta `load_id=` (ADR-0005); ordena as cargas |
| `_ingested_at` | Staging | Momento da conversão da carga; muda quando a staging é regenerada |
| `_source_hash` | Staging | Hash do objeto de origem |
| `_built_at` | Prata e ouro | Momento em que o modelo foi materializado |

A `add_metadata_timestamps` passa a gerar `_ingested_at` (repassado da camada
anterior) e `_built_at`. Saem `dt_ingest`, `dt_silver` e `dt_gold`. Como nenhum
modelo chama a macro hoje, a troca não quebra nada.

### Lake, DAGs e tasks

| Elemento | Padrão | Exemplo |
|---|---|---|
| Bucket | `data-lake-<órgão>` | `data-lake-mcid` |
| Pasta da carga | `load_id=<AAAAMMDDTHHMMSSZ>` (ADR-0005) | `load_id=20261019T030000Z` |
| Chave do lake | ADR-0005 | `raw/fgv/incc_m/load_id=20261019T030000Z/incc_m.xlsx` |
| DAG de fonte | `<fonte>_<dado>_ingest_dag` | `fgv_incc_m_ingest_dag` |
| DAG da entrada manual | `manual_ingest_dag` (ADR-0004) | — |
| DAG de manutenção | `raw_to_staging_dag` | — |
| DAG do dbt | `mcid_cosmos_dag` | — |
| Tasks da DAG de fonte | `extract`, `verify_structural_data_drift`, `transform` (ADR-0004) | — |

## Alternativas consideradas

**Um schema por camada (`bronze`, `prata`, `ouro`), como hoje.** Separa bem as
camadas, mas não separa secretarias, e o ADR-0010 decidiu dividir por schema. A
camada continua visível no prefixo de cada tabela.

**Camadas em inglês (`silver`, `gold`).** É o padrão mais comum fora do projeto,
mas o projeto já está em português nos 267 modelos. Voltar para o inglês seria
renomear tudo sem ganho.

**Schema `comum` para fontes de mais de uma secretaria (versão anterior deste
ADR).** Evitaria repetir bronze e prata, mas criaria uma área sem secretaria dona
e uma migração a cada fonte que passasse a ser compartilhada. Com uma secretaria
só, não se paga.

**Nomes de rastreio em português (`_carga`, `_arquivo_origem`).** Acompanharia o
idioma das camadas, mas foge da convenção da documentação e das ferramentas de
mercado, e obrigaria renomear o que já existe em inglês na staging e nas tabelas
de controle.

**Manter `dt_silver`/`dt_gold`.** Os nomes contradizem as camadas.

**Uma coluna de tempo por camada (`_prata_at`, `_ouro_at`).** A ouro não precisa
saber quando a prata rodou; a linhagem já mostra isso. Uma coluna por tabela
(`_built_at`) diz o que importa: quando aquela tabela foi construída.

**Token de domínio também na bronze.** A bronze espelha a fonte, e a mesma fonte
pode servir a mais de um domínio (ADR-0005). O domínio aparece da prata em diante.

## Trade-offs

| Ganha | Paga |
|---|---|
| O schema diz de quem é o dado, e o nome diz a camada, a fonte ou o domínio, e o objeto | As três camadas dividem o schema, então o schema sozinho não separa bronze de ouro |
| Coluna técnica nunca se confunde com coluna de negócio | Os 16 modelos de bronze do eixo histórico que geram `dt_ingest` hoje precisam mudar |
| Documento normativo volta a descrever o código | Reescrever `padrao-nomenclatura-tabelas-dbt.md` |
| DAGs de fonte encontráveis pelo nome | DAGs atuais fora do padrão (`incc_m_ingest_dag`, `ibge_ingest_dag`) são renomeadas aos poucos |

## Consequências

- Todas as tabelas saem dos schemas `bronze`, `prata` e `ouro` e vão para `snh`. O
  nome da tabela não muda, só o schema. Mudam o roteamento de schema no
  `dbt_project.yml` e na `get_custom_schema.sql`, os datasets do Superset e o
  endereço das tabelas no OpenMetadata (`cidades.<schema>.<tabela>`).
- `padrao-nomenclatura-tabelas-dbt.md` é reescrito com este padrão, e a parte que
  descreve `silver`/`gold` e o schema por camada sai.
- A macro `add_metadata_timestamps` muda de saída.
- DAG de fonte nova já nasce no padrão. As existentes são renomeadas quando forem
  migradas para `extract` → `transform` (ADR-0004). No Airflow, renomear uma DAG
  cria uma DAG nova, e o histórico de execuções fica na antiga.

## Plano de migração

1. Reescrever `padrao-nomenclatura-tabelas-dbt.md`, com a tabela de domínio para
   secretaria.
2. Mudar o roteamento de schema: `+schema` por pasta de domínio no
   `dbt_project.yml`, em vez de por pasta de camada, e ajustar a
   `get_custom_schema.sql`.
3. Rodar a DAG geral, reapontar os datasets do Superset para `snh` e só então
   remover os schemas `bronze`, `prata` e `ouro`.
4. Mudar a `add_metadata_timestamps` para `_ingested_at` e `_built_at`, e trocar
   `dt_ingest` por `_ingested_at` nos 16 modelos de bronze do eixo histórico que o geram.
5. Renomear cada DAG de fonte no PR que a migrar para o ADR-0004.

## Validação operacional

- Testes sobre o grafo, no CI:
  - todo modelo em `*/bronze/`, `*/prata/` e `*/ouro/` tem o prefixo da sua camada
    (hoje 100% passa; o teste impede regressão);
  - todo modelo materializa no schema de uma secretaria.
- Nenhum modelo novo tem `silver` ou `gold` no nome.
- Toda prata e ouro tem `_ingested_at` e `_built_at`.

## Questões em aberto

Nenhuma.
