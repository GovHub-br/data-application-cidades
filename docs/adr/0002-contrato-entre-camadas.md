# 0002. Contrato entre camadas

- **Status:** Proposto
- **Data:** 2026-10-19
- **Revisão:** 2026-10-29 (estado do projeto conferido com o código; staging das APIs sobrescrita; entrada manual pela raw, R7; ordem das cargas pela `_carga`)
- **Relacionados:**
  - ADR-0001, ADR-0003, ADR-0005
  - Deck *GovHub — Arquitetura Medalhão de Referência*, seção 4
  - `dbt/mcid/models/docs/arquitetura-medalhao-mcid.md` §8 (decisões em aberto)
  - `dbt/mcid/macros/fonte_lake.sql`, `dbt/mcid/macros/add_metadata_timestamps.sql`
  - `scripts/mascarar_minio.py`, `scripts/raw_para_staging.py`
  - Issues #248 (staging das APIs sobrescrita) e #249 (máscara como trava)

## Contexto

O ADR-0001 define onde cada camada mora. Falta definir o que cada camada faz. Sem
esse acordo, a mesma transformação aparece em lugares diferentes: um filtro na
bronze, uma tipagem repetida em duas pratas, uma ouro lendo direto da bronze e
pulando os testes. Quando isso acontece, um erro precisa ser procurado em todas as
camadas, e dois painéis podem chegar a números diferentes a partir do mesmo dado.

O princípio do contrato é que cada tipo de transformação acontece em um lugar só.
Esses acordos entre camadas são contratos de dados no sentido de Jones (2023).

Há também uma restrição que vem antes de qualquer camada: a equipe GovHub não pode
acessar dados pessoais. Hoje o mascaramento acontece *depois* que o dado pousa na
raw, reescrevendo o objeto no lugar (`minio_transform_dag`). Até a tarefa rodar, o
dado pessoal fica legível no lake. E a conversão para a staging não confere se o
objeto foi mascarado: uma falha no mascaramento não interrompe a DAG, e o objeto é
convertido do mesmo jeito (issue #249).

Os dados que a equipe consolida à mão (as séries do boletim de conjuntura) seguem
um terceiro caminho: scripts SQL em `scripts/database/` inserem e corrigem as
linhas direto no schema `bronze`, sem raw nem staging. Uma correção por `UPDATE`
não deixa rastro da versão anterior.

Estado do projeto `mcid`, conferido em 2026-10-29:

| Verificação | Resultado |
|---|---|
| Bronzes que resolvem o caminho pela macro `fonte_lake` | 41 de 81 |
| Bronzes com caminho `s3://` escrito no próprio SQL | 25 de 81 (24 em `far`, `fds` e `rural`; 1 em `reforma_casa_brasil`, só como padrão do `arquivo_mais_recente`) |
| Bronzes que filtram para ficar só com o arquivo mais recente (`arquivo_mais_recente`) | 23 de 81 |
| Caminhos de conversão que gravam `_source_file` e `_ingested_at` no Parquet | 1 de 2: `scripts/raw_para_staging.py` grava; `registros_para_staging_parquet` (15 chamadas, uma por DAG de API) não grava |
| Fontes de API cuja raw é sobrescrita a cada execução (chave fixa `raw/<fonte>/<dado>.<ext>` em `cliente_minio.raw_key`) | Todas as que usam `upload_raw_json` ou `upload_raw_bytes` (16 chamadas, em 16 DAGs) |
| Objetos que pousam na raw com dado pessoal e só depois são mascarados | Todo objeto novo em `raw/` |
| Pratas que leem o lake ou uma `source()` direto, sem passar pela bronze | 5 de 84, todas com `source('conjuntura_manual')` (dados manuais, ver R7) |
| Ouros que leem bronze ou lake direto | 5 de 97: `ouro_historico_snapshot_empreendimento_atual` (bronzes INT040 a INT065) e quatro do boletim que leem `source('conjuntura_manual')` |
| Pratas e ouros que chamam `add_metadata_timestamps` | 0 de 181 |
| Conversões que escrevem por cima da staging das APIs | `scripts/raw_para_staging.py` varre a `raw/` inteira (só ignora `dados_historicos`) e regrava `staging/<fonte>/<dado>.parquet`, o mesmo caminho que as DAGs de API escrevem, só em texto (issue #248) |

## Decisão

### Responsabilidades

| Camada | Onde fica | Faz | Não faz |
|---|---|---|---|
| raw | RustFS, formato original | Guarda uma cópia imutável de cada recebimento, já mascarada | Guardar dado pessoal; qualquer alteração depois de gravada |
| staging | RustFS, Parquet | Converte o formato, normaliza o nome das colunas e acrescenta colunas de linhagem | Tipar, filtrar, deduplicar, alterar valores |
| bronze | Postgres, `select * from read_parquet` | Expõe a staging no warehouse, como está | Qualquer transformação |
| prata | Postgres, dbt | Trata a bronze de forma genérica: tipos, nomes, limpeza, padronização | Regra de negócio: joins entre entidades, filtros, escolha de período, agregações |
| ouro | Postgres, dbt | Aplica as regras de negócio e calcula os indicadores a partir das pratas | Ler bronze, staging ou lake |

### Regras

**R0. Nenhum dado pessoal pousa no lake.** O mascaramento acontece na task
`extract` da DAG da fonte, *antes* de gravar a raw (ADR-0004). A técnica é a que já
existe em `scripts/mascarar_minio.py`:

- identificadores (CPF, NIS) viram um token HMAC-SHA256 determinístico. O mesmo CPF
  gera o mesmo token em qualquer base, então joins e contagens de distintos
  continuam possíveis;
- quasi-identificadores de pessoa física (nome, endereço, CEP, nascimento) são
  redigidos;
- cada mascaramento é auditado (`lake._masking_log`).

O original nunca vai para o lake, mas passa pelo worker do Airflow. Arquivos
grandes não cabem em memória e são mascarados num temporário em disco
(`MASKING_TMPDIR`). Esse temporário é apagado ao fim da task, e nenhum log, XCom ou
prévia contém valores: entre `extract` e `transform` passam só as chaves dos
objetos.

**O token é pseudonimização, não anonimização.** O espaço de CPFs é pequeno (cerca
de 10⁹ valores), e quem tem a chave do HMAC consegue recalcular o token de todos os
CPFs possíveis e desfazer qualquer um. Para a LGPD, dado pseudonimizado continua
sendo dado pessoal para quem tem a chave. Por isso a chave (`MASKING_HMAC_SECRET`)
segue estas regras:

- fica só no ambiente do Airflow, nunca no repositório, em log ou em documento;
- não é lida por ninguém da equipe no dia a dia: entra como segredo do
  ambiente, sem aparecer na interface do Airflow;
- nunca muda depois do primeiro mascaramento. Trocar a chave quebra os joins com
  todo o histórico já mascarado;
- no ambiente do ministério, a custódia da chave fica com o ministério
  (ADR-0011).

Isso vale para todo ambiente que a equipe GovHub acessa. O valor original existe só
na fonte. Se o ministério quiser guardar o original no ambiente dele, a decisão é
dele e fica no ADR-0011.

A única entrada da raw que não passa por esse mascaramento é a entrada manual
(R7), que só aceita dado sem PII.

**R1. A raw é imutável.** Cada recebimento, já mascarado, é gravado em caminho
próprio, na pasta `carga=` do ADR-0005, e nunca é sobrescrito. É a origem de
qualquer reprocessamento.

**R2. A staging muda a forma, não o conteúdo.** A conversão é a mesma para todas
as fontes e roda na task comum `transform`, encadeada em cada DAG de
fonte depois da task que grava a raw e da `verify_structural_data_drift` (ADR-0004). Ela:

- grava o conteúdo da raw em Parquet, na mesma árvore de pastas;
- grava toda coluna como texto;
- normaliza mecanicamente o nome das colunas para snake_case ASCII, sem duplicatas,
  e guarda o cabeçalho original nos metadados do Parquet;
- acrescenta as colunas de linhagem `_source_file` (objeto de origem na raw),
  `_carga` (a pasta `carga=` do objeto, ADR-0005), `_ingested_at` (momento da
  conversão) e `_source_hash` (hash do objeto de origem).

Só é convertido o arquivo que passou pelo drift estrutural (ADR-0006). A staging não
tipa, não filtra, não deduplica e não altera valores. A interpretação começa na
prata.

**R3. A bronze é a staging dentro do warehouse.** Cada modelo de bronze é
`select * from {{ fonte_lake('<fonte>') }}` e nada além disso:

- o caminho fica declarado em `sources.yml`, para que a linhagem comece na fonte e
  não na bronze;
- nomes padronizados e colunas de linhagem já chegam da staging (R2);
- mantém **todas** as cargas presentes na staging;
- a ordem entre as cargas é dada pela `_carga`, o momento do recebimento. O
  `_ingested_at` registra a conversão e muda quando a staging é regenerada
  (ADR-0005), então não serve para dizer qual carga é a mais recente.

**R4. A prata é a bronze tratada, e obrigatoriamente genérica.** A prata não é
moldada para um indicador. Ela deixa cada fonte limpa e padronizada para que
qualquer regra de negócio possa ser aplicada depois, na ouro, com qualquer join.
Cada modelo de prata:

- lê somente bronze ou outras pratas, nunca `source()`, `read_parquet` ou
  `fonte_lake`;
- mantém o grão da fonte: uma linha da prata é uma entidade da fonte num período de
  referência. Não agrega e não descarta períodos;
- converte os tipos (datas, valores em formato brasileiro, inteiros), dá nomes
  semânticos às colunas, normaliza domínios e formatos (por exemplo, o formato do
  APF com `normalize_apf`) e limpa o texto (espaços, nulos, mojibake);
- remove duplicatas **técnicas**: a mesma linha repetida, ou o mesmo período
  reenviado pela fonte, quando fica a carga mais recente pela `_carga`;
- pode unir fontes que descrevem a mesma entidade no mesmo grão (por exemplo, FAR
  CAIXA com FAR BB), com uma coluna que identifica a origem;
- chama `add_metadata_timestamps`;
- só roda quando a bronze passou pelo drift semântico, e tem os testes de qualidade
  descritos no ADR-0006.

Não entra na prata nada que dependa de regra de negócio: join entre entidades
diferentes, filtro por critério de programa, escolha do período vigente, escolha do
registro válido entre duplicatas de negócio, métricas.

**R5. A ouro carrega toda a regra de negócio.** Cada modelo de ouro lê somente
pratas ou outras ouros e é o lugar de:

- joins entre entidades e fontes segundo regra de negócio;
- escolha do período vigente (a carga mais recente, o snapshot do mês);
- deduplicação de negócio, como a regra do APF quando o mesmo empreendimento aparece
  em fases diferentes;
- indicadores, métricas e agregações;
- `add_metadata_timestamps`.

Toda ouro declara o seu grão (por exemplo, um empreendimento por mês) e tem um teste
de unicidade sobre ele. Como a prata guarda todos os períodos, uma ouro que esquecer
de escolher o período vigente soma em dobro sem erro nenhum; o teste de unicidade é o
que pega esse caso.

Quando mais de uma ouro precisa da mesma regra, ela vira uma ouro base que as outras
referenciam, em vez de ser repetida. Se uma ouro precisa de um campo que a prata não
tem, o campo entra na prata primeiro, de forma genérica.

**R6. O consumo só lê a ouro.** Painéis, planilhas, boletins e exportações leem
somente tabelas da ouro.

**R7. A entrada manual é uma fonte como as outras.** Os dados que a equipe
consolida à mão entram pela raw e seguem o mesmo caminho até a ouro, com as
mesmas regras:

- o arquivo é CSV ou XLSX, um por base, com o layout declarado no contrato da
  fonte (ADR-0006). Script SQL não é forma de entrada, e nada é inserido ou
  corrigido direto no banco;
- o envio é pelo `scripts/enviar_manual.py`, que grava em
  `raw/manual/<base>/carga=.../` (ADR-0005). A DAG `manual_ingest_dag` converte o
  que chegou (ADR-0004);
- a carga é completa: cada envio traz a série inteira da base. Uma correção é um
  envio novo, e a prata fica com a carga mais recente de cada período (R4,
  ADR-0009). A versão anterior continua na raw;
- só entra dado sem PII. É a única entrada da raw que não passa pela máscara do
  R0, e o teste de dado sensível em todos os domínios (ADR-0012) é a segunda
  linha de defesa. Os arquivos do SharePoint não são entrada manual neste
  sentido: podem trazer dado pessoal e seguem o caminho das fontes de arquivo.

### Divergências intencionais em relação ao deck

| Tema | Deck | Este ADR | Por quê |
|---|---|---|---|
| Raw | Cópia fiel do que chegou | Cópia do que chegou, já com a PII mascarada (R0) | A equipe GovHub não pode acessar dado pessoal |
| Nomes e colunas técnicas | A bronze padroniza nomes e acrescenta `_source_file`/`_ingested_at` numa CTE | A conversão para a staging faz as duas coisas; a bronze é `select *` puro (R2, R3) | É como o time já trabalha, e a CTE não se repete em cada bronze |
| Prata | Incremental por chave de negócio, com integração entre fontes | Genérica, no grão da fonte, com todos os períodos; a integração por regra de negócio vai para a ouro (R4, R5) | Uma prata serve qualquer indicador futuro |

## Alternativas consideradas

**Mascarar depois de pousar na raw (prática atual).** Deixa o dado pessoal legível
no lake até a tarefa de mascaramento rodar, e obriga a reescrever a raw, o que
contraria R1. Não é compatível com a restrição de acesso da equipe.

**Guardar o original numa raw de acesso restrito e mascarar na conversão.**
Preservaria o valor original para reprocessamento, mas a equipe GovHub opera o lake
e não pode ter acesso a dado pessoal nem de forma restrita. Fica como opção para o
ambiente do ministério (ADR-0011).

**Padronizar nomes e acrescentar colunas técnicas na bronze, como descreve o deck.**
O deck põe essas duas tarefas numa CTE da bronze, sobre o `read_parquet`. Aqui
elas vão para a conversão. Assim a bronze fica um `select *` puro, que é como o time
já trabalha, e a CTE não se repete em 81 modelos. A conversão genérica
(`scripts/raw_para_staging.py`) já faz as duas coisas. Nada se perde, porque o
cabeçalho original fica nos metadados do Parquet.

**Tipar já na bronze.** Deixaria a prata mais simples, mas a tipagem sairia da
camada que tem testes, e um erro de conversão apagaria o valor original sem deixar
rastro. Com tudo em texto até a prata, o valor original fica sempre consultável na
bronze.

**Bronze só com a carga mais recente (prática atual em 23 modelos).** Reduz o volume
e simplifica o que vem depois, mas torna o histórico da fonte invisível no
warehouse e mistura uma decisão de negócio ("qual carga vale") com uma camada que
não deveria tomar decisões. Se o volume virar problema, a resposta é a
materialização incremental por partição de carga (ADR-0008), não o filtro.

**Prata moldada por indicador, com as regras de negócio já aplicadas.** A ouro
ficaria mais simples, mas cada prata passaria a servir um painel só. A próxima
pergunta com outro recorte exigiria uma prata nova, e regras diferentes sobre a
mesma fonte divergiriam sem ninguém perceber.

**Ouro lendo direto a bronze em casos simples.** Economiza um modelo, mas pula a
tipagem e os testes da prata. É o caminho mais curto para dois painéis mostrarem
números diferentes.

**Manter a inserção manual direto na bronze (prática atual).** Dispensa arquivo
e conversão, mas é a única fonte que pula a raw e a staging. Cinco pratas e
quatro ouros precisam ler `source()`, e cada correção por `UPDATE` apaga a versão
anterior sem deixar rastro.

**Arquivo da entrada manual versionado no repositório, lido por uma DAG.** A
correção passaria por revisão em PR, mas o repositório é público, e atualizar um
número exigiria um PR. Com a pasta `raw/manual/`, a entrada fica no lake, como a
de qualquer fonte, e a raw guarda cada versão.

## Trade-offs

| Ganha | Paga |
|---|---|
| A equipe nunca toca dado pessoal, nem por acidente | O valor original só existe na fonte; um erro no mascaramento só se corrige reextraindo |
| Todo erro tem uma camada provável de origem | Mais modelos: toda fonte passa por bronze e prata, mesmo as simples |
| Uma prata serve qualquer indicador futuro | A prata guarda todos os períodos e fica maior nas fontes de carga completa |
| Toda regra de negócio está num lugar só, a ouro | A ouro concentra mais lógica; regras comuns precisam virar ouro base para não se repetirem |
| O valor original (mascarado) fica sempre consultável na bronze | A bronze cresce com cada carga mantida |

## Consequências

- O mascaramento sai da `minio_transform_dag` e vira uma função chamada pela DAG da
  fonte antes de gravar a raw, inclusive na `sftp_ingest_dag`. A lógica de detecção
  e o HMAC de `scripts/mascarar_minio.py` são reaproveitados; a reescrita no lugar e
  a tag `masked=true` deixam de existir para dados novos.
- As DAGs de API deixam de converter por conta própria e passam pela conversão
  comum (R2). Hoje `registros_para_staging_parquet` não normaliza nomes nem grava
  linhagem, e o hook `_tipar` do `IngestorLake` (usado por
  `IngestorBalancoEmpresas`) converte números antes de gravar o Parquet. Essa
  conversão de números vai para a prata.
- As fontes de API sobrescrevem a mesma chave da raw a cada execução, contra R1. O
  layout da raw com uma chave por recebimento se resolve no ADR-0005.
- A escolha da carga vigente sai da bronze e vai para a ouro. A macro
  `arquivo_mais_recente` passa a ser usada na ouro, sobre a coluna de período da
  prata.
- Pratas que hoje aplicam regra de negócio (joins entre entidades, filtros de
  programa, deduplicação por APF entre fases) movem essa lógica para a ouro.
- Toda ouro e toda prata ganham `dt_ingest`, `dt_silver`/`dt_gold` via
  `add_metadata_timestamps`. A macro precisa mudar antes: hoje ela lê uma coluna
  `dt_ingest` da camada anterior, mas a bronze passa a trazer `_ingested_at` (R2,
  R3). Na prata, a macro passa a ler `_ingested_at`. Os nomes das colunas que ela
  gera ainda usam `silver`/`gold`, e isso se resolve no ADR-0007 (nomenclatura).
- A decisão 6 em aberto em `arquitetura-medalhao-mcid.md` (em que camada aplicar a
  regra do APF) fica respondida: a normalização do formato do APF é feita na prata, e
  a regra de deduplicação entre fases, na ouro.
- Manter todos os períodos na prata não multiplica o volume de toda fonte. Quem
  define isso é o tipo de carga, e o drift estrutural o identifica pela estrutura
  do arquivo (ADR-0006): uma fonte incremental só acrescenta o que é novo; uma fonte
  de carga completa reenvia a base inteira a cada período. O tipo de carga fica
  registrado no contrato da fonte, e a materialização da prata segue essa
  classificação (ADR-0008). Isso também atende ao requisito 8 da issue #130
  (identificar quais bases são carga completa e quais são incrementais).
- A entrada manual (R7) substitui os scripts de `scripts/database/` e o
  `IngestorBalancoEmpresas`, que a `conjuntura_dag` usa hoje para gerar parquet
  tipado de um arquivo manual. As cinco pratas e as quatro ouros que leem
  `source('conjuntura_manual')` passam a ler a bronze e a prata das bases
  manuais.

## Plano de migração

Primeiro o mascaramento, porque é restrição de acesso e não melhoria:

1. Extrair de `scripts/mascarar_minio.py` uma função que mascara um objeto em
   memória, e chamá-la nas DAGs de fonte antes de gravar a raw, começando pela
   `sftp_ingest_dag`, que é por onde chegam as bases com dado pessoal.
2. Tirar a tarefa `mascarar_pii` da `minio_transform_dag` quando todas as fontes com
   dado pessoal estiverem mascarando antes de pousar.

Depois as camadas, por domínio, um PR por domínio, nesta ordem:
`conjuntura` → `far` → `fds` → `rural` → `mcmv_historico` → `indicadores_mcmv` →
`linha_financiada` → `reforma_casa_brasil`.

Os três domínios que ainda rodam em DuckDB (`linha_financiada`,
`reforma_casa_brasil` e `mcmv_historico`) mudam de dialeto e de contrato no mesmo
PR, e por isso seguem a ordem do ADR-0003, antes dos demais.

No domínio `conjuntura` entra também a entrada manual (R7):

1. Criar o `scripts/enviar_manual.py`, a `manual_ingest_dag` e o contrato de cada
   base manual.
2. Exportar cada tabela `bronze_manual_*` atual como a primeira carga da sua
   base.
3. Trocar o `source('conjuntura_manual')` das pratas e ouros pela bronze e pela
   prata das bases manuais, e conferir os números da ouro antes e depois.
4. Aposentar os scripts de `scripts/database/` e o `IngestorBalancoEmpresas`, e
   tirar a source `conjuntura_manual` do `sources.yml`.

Em cada domínio:

1. Staging: garantir que os Parquets do domínio saiam da conversão comum (R2), com
   nomes normalizados e colunas de linhagem.
2. Bronze: reduzir a `select * from {{ fonte_lake(...) }}`, declarando a fonte em
   `sources.yml` no lugar do caminho literal; remover o filtro de carga mais
   recente.
3. Prata: trocar leituras diretas do lake por `ref()` da bronze; tirar o que for
   regra de negócio e levar para a ouro; manter todos os períodos; chamar
   `add_metadata_timestamps`.
4. Ouro: trocar leituras de bronze ou lake por `ref()` da prata; receber a escolha do
   período vigente e as regras que saíram da prata; chamar `add_metadata_timestamps`.
5. Conferir que a ouro continua com os mesmos números antes e depois, com uma
   comparação de contagem e soma das métricas principais.

## Validação operacional

R0 se verifica no lake:

- toda execução de ingestão com dado pessoal tem linha em `lake._masking_log`
  *antes* do objeto existir na raw;
- uma varredura periódica da raw pelos detectores de `mascarar_minio.py`, em modo
  só leitura, não encontra coluna sensível sem mascarar.

As regras R3 a R5 viram um teste dbt sobre o grafo, executado no CI:

- modelo em `*/bronze/` que não seja só `select * from {{ fonte_lake(...) }}` →
  falha;
- tabela de bronze sem as colunas `_source_file`, `_carga` e `_ingested_at` →
  falha (é o sinal
  de que a staging não passou pela conversão comum);
- modelo em `*/prata/` com dependência que não seja bronze ou prata → falha;
- modelo em `*/ouro/` com dependência que não seja prata ou ouro → falha;
- modelo de prata ou ouro sem as colunas de `add_metadata_timestamps` → falha;
- modelo de ouro sem teste de unicidade no grão declarado → falha. O teste é o
  `unique_combinacao`, que já existe em `macros/data_quality/` (o projeto não usa
  pacotes dbt).

"Prata genérica" não se verifica só por teste. Na revisão de PR, toda prata nova
responde a uma pergunta: ela serviria a um indicador que ainda não existe? Se a
resposta depende de um filtro ou de um join que só faz sentido para um painel, essa
parte vai para a ouro.

Enquanto a migração não termina, o teste roda com uma lista de exceções versionada
que só pode diminuir.

## Questões em aberto

Nenhuma. O volume da prata com todos os períodos ficou respondido pelo drift
estrutural, que identifica o tipo de carga de cada fonte (ver Consequências), e
os dados manuais, pela R7.
