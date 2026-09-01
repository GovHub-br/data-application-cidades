# Backlog OpenMetadata — produto Conjuntura como molde

## Propósito

O produto **Conjuntura** é o primeiro a ser documentado por inteiro no
OpenMetadata. O que for feito aqui vira o molde para `empreendimento_far` e
`entidades_fds`: os mesmos arquivos declarativos, o mesmo sincronizador, os
mesmos campos preenchidos. Nenhuma HU deste backlog deve resolver algo de um
jeito que só funcione para conjuntura.

O alvo é um catálogo em que, partindo de um número no boletim, se chegue à
DAG que o ingeriu e ao parquet no MinIO de onde ele veio — passando por
produto de dados, domínio, dono, camada, certificação, etiqueta e termo de
glossário, todos preenchidos.

## Princípio que não se negocia

**Nada é criado pela interface.** O que está declarado em
`dbt/mcid/governance/*.yml` é o que existe no catálogo. Quem editar na tela
perde a alteração no próximo sync.

Corolário: a fonte da verdade é o YAML; o meio de aplicação é trocável. Hoje
aplicamos por script REST (`scripts/governance/`), e a decisão é manter assim
por ora. A migração para os conectores nativos do OpenMetadata é planejada
(HU-33) e nenhuma HU pode criar dependência que a impeça.

## Estado atual

**Aplicado na instância em 2026-08-31** — `openmetadata.clusterlab.lappis.rocks`
(OM 1.13.3), service `Cidades`, database `cidades`. Épicos 0 a 3 concluídos e
conferidos tabela a tabela.

| | antes | agora |
|---|---|---|
| Descrições de tabela curadas | 0/140 | **140/140** |
| Colunas fiéis ao dbt | — | **140/140 tabelas** |
| Colunas com `VARCHAR(65535)` inventado | 546+ | **0** |
| Descrição e nome de schema | 0/5 | **5/5** |
| Certificação | 0/140 | **140/140** |
| Tier · permissão de uso · produto · dono | parcial | **140/140** |
| Termo de glossário | 0 | **38 tabelas · 231 colunas** |
| Ativos por produto (índice de busca) | 0 | **116 / 13 / 11** |
| Database Service e Database documentados | ❌ | ✅ |
| Glossário MCID | 40 termos | **48**, com sinônimos e relações |

### Como está organizado

| Comando | Responsabilidade |
|---|---|
| `make openmetadata-sync` | **Estrutura**: schema, tabela, coluna, linhagem |
| `make openmetadata-governanca` | **Governança**: dono, domínio, produto, classificação, etiqueta, tier, certificação, uso, glossário |
| `make openmetadata` | Os dois, nessa ordem |

**A ordem é dependência, não preferência.** Reescrever `/columns` substitui o
array inteiro e leva junto a etiqueta de glossário das colunas. Governança
antes de estrutura perde as 231 colunas etiquetadas.

`scripts/governance/governanca_comum.py` concentra `.env`, cliente da API,
resolução de proprietário e cálculo do patch mínimo. Sem credencial, os
comandos rodam **offline** e imprimem o que está declarado.

### Armadilhas da instância

Nenhuma está na documentação do OpenMetadata; todas custaram diagnóstico. O
detalhe completo está em `docs-conjuntura/MEMORY.md`, entrada de 2026-08-31.

| Armadilha | Consequência se ignorada |
|---|---|
| Certificação é campo próprio, não etiqueta | API devolve 200 e descarta em silêncio |
| `PUT` não sobrescreve descrição preenchida | catálogo parece documentado com texto de rodapé |
| OM escapa `=` `'` `"` como entidade HTML | comparação nunca converge; repatch eterno |
| `relatedTerms` é `TermRelation` | HTTP 500 |
| bot proibido de editar nome de exibição | patch é tudo ou nada: derruba os outros campos |
| serviço mora em `services/databaseServices` | 404; serviço parece inexistente |
| `dbtTags` é compartilhada com o MinC | sobrescrever vocabulário de outro órgão |
| `assets` do produto fica 0 | perseguir um zero que a interface não usa |

### Comandos

| Comando | Faz |
|---|---|
| `make openmetadata-sync` | estrutura: schema, tabela, coluna, linhagem dbt |
| `make openmetadata-governanca` | dono, domínio, produto, etiqueta, tier, certificação, uso, glossário |
| `make openmetadata-lake` | MinIO, containers, linhagem DAG → parquet → Bronze |
| `make openmetadata` | os três, nessa ordem |
| `make governance-audit-om` | confere a instância contra o declarado |

A ordem é dependência. Estrutura antes de governança, porque reescrever
`/columns` substitui o array e leva a etiqueta de glossário das colunas. Lake
por último, porque liga containers a tabelas que precisam existir antes.

**Idempotência verificada:** duas execuções seguidas de cada sync terminam com
`criados=0 atualizados=0`.

### Épico 7 — o que entrou

- `Cidades - MinIO` como Storage Service, **sem credencial**
- 46 containers: `data-lake-mcid → staging → <prefixo> → <arquivo>.parquet`
- 33 arestas parquet → Bronze, do `meta.caminho` e da macro `fonte_lake`
- serviço `airflow` documentado; DAGs de ingestão declaradas criadas e ligadas

Cadeia conferida: `DAG → parquet → bronze → silver → gold`.

### Falta para fechar os épicos já abertos

1. **Nome de exibição do serviço** — mostra "Data Warehouse MCid". Só perfil de
   administrador troca, pela interface.
2. **`make openmetadata-catalog`** (exige VPN e banco) — sem ele
   `ordinalPosition` fica vazio e toda tabela consta `Regular`. Fecha HU-07 e o
   resto da HU-12.
3. **19 de 31 `meta.dag`** ainda vazias no `sources.yml` (HU-25/26). As 5 do
   SFTP não têm DAG; as 14 do IBGE dependem da Airflow Variable
   `IBGE_CONFIGURACOES`, que o repo não enxerga. Preencher no chute publicaria
   linhagem falsa.
4. **HU-30** — a idempotência foi alcançada, mas remover do catálogo o que saiu
   do YAML continua não implementado.

---

# Épico 0 — Fundação

Sem estas três, todas as demais falham ou falham em silêncio.

### ✅ HU-01 · Configuração do OpenMetadata declarada no repo
**Como** engenheiro de dados, **quero** as variáveis do OpenMetadata
documentadas no `.env.example`, **para** que quem clonar o repo consiga rodar
o sync sem adivinhar nomes.

Hoje o `.env` do repo não tem nenhuma variável `OPENMETADATA_*`, e os dois
scripts abortam com "Variáveis ausentes". O sync já rodou uma vez, então as
credenciais existiram e se perderam.

- [ ] `infra/env/.env.example` ganha o bloco `OPENMETADATA_*`: `URL`,
      `JWT_TOKEN`, `DATABASE_SERVICE`, `DATABASE_NAME`, `OWNER_FQN`
- [ ] `README.md` ou `docs/governance/` explica de onde sai cada valor
- [ ] Os dois scripts falham com mensagem que nomeia a variável e o arquivo

**Toca:** `infra/env/.env.example`, `scripts/governance/`

---

### ✅ HU-02 · Classificações e etiquetas existem antes de serem aplicadas
**Como** engenheiro de dados, **quero** que `Uso`, `dbtTags`, `Tier` e
`Certification` sejam criadas na instância pela sincronização, **para** que a
etiquetagem das tabelas não falhe tabela a tabela.

`sincronizar_governanca.py` aplica `Uso.NaoConsumivel`, `dbtTags.mcid`,
`Tier.Tier1` e `Certification.Gold`, mas **nada no repo cria essas
classificações**. `Tier` e `Certification` são nativas do OpenMetadata;
`dbtTags` normalmente vem do conector dbt (que não rodamos); `Uso` é 100%
nossa. Como está, o PATCH de tags devolve erro por tabela.

- [ ] `dominios.yml` ganha a seção `classificacoes:` declarando nome,
      descrição e termos de cada classificação nossa
- [ ] Sincronizador cria/atualiza as classificações **antes** de etiquetar
- [ ] Classificações nativas são verificadas, não recriadas
- [ ] Rodar em simulação lista o que criaria; rodar duas vezes não duplica

**Depende de:** HU-01
**Toca:** `dbt/mcid/governance/dominios.yml`, `scripts/governance/sincronizar_governanca.py`

---

### ✅ HU-03 · Uma única fonte de proprietário
**Como** consumidor do catálogo, **quero** ver o mesmo dono na tabela, no
schema e no produto, **para** não ter dúvida de a quem reclamar.

Hoje há três verdades em conflito: `sincronizar_openmetadata.py` põe o
**usuário** `OPENMETADATA_OWNER_FQN` (padrão `admin`) como dono das tabelas;
`dominios.yml` declara o **time** `mcid-data-engineering` como dono dos
produtos; `schemas.yml` declara `owner_key: admin` por schema e ninguém lê.

- [ ] `dominios.yml` é a única fonte: `proprietarios:` resolve chave → entidade
- [ ] `schemas.yml` passa a referenciar a mesma chave de `dominios.yml`
- [ ] `sincronizar_openmetadata.py` deixa de resolver dono por conta própria
- [ ] Tabela, schema, database, produto e domínio saem com o mesmo dono
- [ ] O segundo sync não sobrescreve o dono posto pelo primeiro

**Toca:** os dois sincronizadores, `dominios.yml`, `schemas.yml`

---

# Épico 1 — Serviço, banco e schema

O alicerce físico que hoje é pressuposto e nunca documentado.

### ✅ HU-04 · Database Service documentado
**Como** consumidor, **quero** que o serviço de banco tenha descrição, dono,
domínio e etiquetas, **para** saber o que é aquele serviço numa instância
compartilhada entre ministérios.

- [ ] `dominios.yml` (ou `servicos.yml`) declara o service: nome, displayName,
      descrição, dono, domínio, etiquetas
- [ ] Sincronizador aplica sem recriar a conexão (não tocamos em credencial)
- [ ] Descrição diz o que o serviço guarda, não como ele é operado

**Depende de:** HU-02, HU-03
**Toca:** `dbt/mcid/governance/`, `scripts/governance/sincronizar_governanca.py`

---

### ✅ HU-05 · Database documentado
**Como** consumidor, **quero** a entidade Database com descrição, dono e
domínio, **para** que o nível entre serviço e schema não fique mudo.

- [ ] Declarado no mesmo arquivo do service
- [ ] Descrição, displayName, dono, domínio e etiquetas aplicados
- [ ] Idempotente

**Depende de:** HU-04

---

### ✅ HU-06 · Descrição de schema vem do YAML, não de heurística
**Como** curador, **quero** que a descrição do schema no catálogo seja a que
está em `schemas.yml`, **para** que o texto curado não seja substituído por
uma frase montada por regra de sufixo.

`sincronizar_openmetadata.py` tem `schema_description()`, que deduz a camada
pelo sufixo do nome e monta um texto genérico — enquanto `schemas.yml` já traz
descrições curadas e é inclusive carregado no catálogo semântico.

- [ ] `build_payload` lê a descrição de `schemas.yml`
- [ ] `schema_description()` sai do código
- [ ] Schema sem entrada no YAML falha o sync em vez de receber texto genérico
- [ ] Schema recebe também dono, domínio e etiqueta de camada

**Depende de:** HU-03
**Toca:** `scripts/governance/sincronizar_openmetadata.py`

---

# Épico 2 — Tabela

### ⚠️ HU-07 · Tabela com os campos de identidade preenchidos
**Como** consumidor, **quero** displayName, tipo e período de retenção nas
tabelas, **para** ler o catálogo sem decifrar nome técnico.

Hoje a tabela sai com `name`, `description`, `tableType: Regular` e colunas.
Nada mais.

- [ ] `displayName` legível derivado do `schema.yml` do dbt
- [ ] `tableType` correto (`Regular`, `View`, `MaterializedView`)
- [ ] `retentionPeriod` onde houver política declarada
- [ ] Nenhum campo recebe valor inventado só para não ficar vazio

---

### ✅ HU-08 · Tabela pertence ao domínio e ao produto de dados
**Como** gestor, **quero** abrir o produto `conjuntura` e ver seus ativos,
**para** que "produto de dados" seja uma entidade real e não um rótulo.

É o que destrava `ativos = 0`. A ordem importa: o OpenMetadata recusa vincular
tabela a produto se ela ainda não pertence ao domínio do produto
(`Data Product Domain Validation`) — domínio primeiro, produto depois.

- [ ] Toda tabela dos schemas listados em `dominios.yml` recebe o domínio
- [ ] Depois, o produto
- [ ] Os três produtos saem com `ativos > 0`
- [ ] Tabela que sair do YAML perde o vínculo no sync seguinte

**Depende de:** HU-03
**Toca:** `scripts/governance/sincronizar_governanca.py` (`catalogar_tabelas`)

---

### ✅ HU-09 · Camada, certificação e permissão de uso visíveis na tabela
**Como** consumidor, **quero** ver na tabela a camada, o Tier, a certificação
de curadoria e se posso consumi-la, **para** não usar bronze achando que é
dado publicável.

As quatro seções já estão declaradas em `dominios.yml`
(`etiquetas_automaticas`, `certificacao_por_camada`,
`certificacao_de_curadoria_por_camada`, `permissao_de_uso_por_camada`).

- [ ] `dbtTags.mcid` + `dbtTags.<produto>` + `dbtTags.<camada>` em 100% das tabelas
- [ ] `Tier1/2/3` conforme a camada
- [ ] `Certification.Gold/Silver/Bronze` conforme a camada
- [ ] `Uso.Consumivel` / `Uso.ApoioInterno` / `Uso.NaoConsumivel`
- [ ] Bronze aparece no catálogo e sai marcada como não consumível

**Depende de:** HU-02, HU-08

---

### ✅ HU-10 · Termos de glossário anexados às tabelas
**Como** analista, **quero** clicar em "FIPE" no glossário e ver as tabelas
que usam o conceito, **para** navegar por assunto e não por nome de tabela.

`termos_mcid.yml` já mapeia 38 modelos via `aplica_a` e
`aplicacao_de_termos_existentes`. Os termos são criados; não são pendurados.

- [ ] Todo modelo listado em `aplica_a` recebe o termo correspondente
- [ ] Termo declarado para modelo inexistente falha o sync (evita mapa podre)
- [ ] Cada termo do glossário tem ao menos um ativo, ou é justificado no YAML

**Depende de:** HU-08

---

### ✅ HU-11 · Chaves e particionamento declarados
**Como** consumidor, **quero** ver chave primária e relacionamentos,
**para** entender a granularidade sem abrir o SQL.

- [ ] `tableConstraints` (PK e, onde houver, FK) derivadas dos testes
      `unique`/`not_null`/`relationships` do dbt
- [ ] `tablePartition` onde a tabela for particionada
- [ ] A derivação não lê dado: só a declaração de teste no YAML

---

# Épico 3 — Coluna

### ✅ HU-12 · Tipo de coluna fiel à origem
**Como** consumidor, **quero** o tipo real da coluna, **para** não planejar
uma integração com base em tipo errado.

`om_column()` colapsa tudo em sete tipos e carimba `dataLength: 65535` em todo
VARCHAR. Precisão e escala de numéricos se perdem.

- [ ] `precision`/`scale` preservados para `numeric`/`decimal`
- [ ] `dataLength` real quando declarado, e não constante
- [ ] `arrayDataType` para colunas de array
- [ ] `ordinalPosition` na ordem física
- [ ] `displayName` quando o nome técnico não for legível

**Toca:** `scripts/governance/sincronizar_openmetadata.py`

---

### HU-13 · Colunas classificadas quanto a dado pessoal
**Como** encarregado de dados, **quero** ver `PII.Sensitive` /
`PII.NonSensitive` nas colunas, **para** que a decisão de sensibilidade esteja
no catálogo e não só no código.

Já existe `dbt/mcid/macros/coluna_sensivel.sql` e a lista
`SENSITIVE_IDENTIFIERS` no exportador — hoje a coluna sensível é **omitida**
do catálogo. Omitir o conteúdo é certo; omitir a existência da coluna deixa o
catálogo mentindo sobre o schema.

- [ ] Decidir e registrar: coluna sensível some do catálogo ou aparece marcada
      e sem descrição? (**decisão do Lucas — bloqueia a HU**)
- [ ] Classificação aplicada a partir de uma única fonte, não de duas listas
- [ ] Nenhum valor de dado é lido para classificar

**Depende de:** HU-02

---

### ✅ HU-14 · Termo de glossário na coluna
**Como** analista, **quero** o termo no nível da coluna, **para** que
"unidade habitacional" aponte para as colunas que a medem, não só para tabelas.

- [ ] `termos_mcid.yml` aceita `aplica_a` no formato `modelo.coluna`
- [ ] Termos aplicados na coluna sem apagar os da tabela

**Depende de:** HU-10

---

### HU-15 · Política das colunas não documentadas
**Como** curador, **quero** uma decisão registrada sobre as 489 colunas da
bronze e as 318 das silvers manuais, **para** que "documentação completa"
tenha um alvo definido.

- [ ] Registrar a política: bronze publica topologia, não dicionário de coluna
- [ ] Silvers manuais ficam de fora enquanto forem transitórias
- [ ] `auditar_metadados.py` deixa de contá-las como pendência e passa a
      contá-las como exceção declarada

---

# Épico 4 — Produto de dados e domínio

O centro do pedido: cada modelo dbt é um produto de dados.

### ⚠️ HU-16 · Produto de dados com todos os campos
**Como** gestor, **quero** o produto com dono, domínio, especialistas,
etiquetas e termos, **para** que a página do produto responda sozinha o que
ele é, de quem é e o que entrega.

Hoje o produto sai com nome, displayName, descrição, domínio e dono. Só.

- [ ] `experts` (quem responde por conteúdo) declarados em `dominios.yml`
- [ ] Etiquetas do produto
- [ ] Termos de glossário do produto
- [ ] `assets` conferidos após HU-08
- [ ] Patch cobre displayName, domínio e dono — hoje só `/description`

**Depende de:** HU-08

---

### ✅ HU-17 · Propriedades customizadas do MCID
**Como** curador, **quero** campos nossos no catálogo, **para** registrar o
que o modelo do OpenMetadata não prevê e que hoje só existe em YAML solto.

Candidatas, todas já declaradas em algum arquivo do repo:

| Propriedade | Onde já existe |
|---|---|
| `fonte_institucional` | `termos_mcid.yml`, descrições |
| `periodicidade` | descrições das séries |
| `defasagem_de_publicacao` | `gaps-de-dados.md` |
| `sujeito_a_revisao_retroativa` | `reconciliacoes.yml` |
| `contrato_de_qualidade` | macro `silver_contract` |
| `edicao_congelada` | snapshots |
| `dag_de_origem` | `sources.yml` |

- [ ] Propriedades criadas nas entidades `table` e `dataProduct`
- [ ] Declaradas em YAML, não pela interface
- [ ] Preenchidas para o produto conjuntura inteiro
- [ ] Propriedade sem valor declarado fica vazia, nunca com valor padrão falso

**Depende de:** HU-16

---

### ⚠️ HU-18 · Domínio completo
**Como** gestor de outro ministério na mesma instância, **quero** ver de quem
é o domínio `MCid`, **para** saber que aquele conjunto não é meu.

- [ ] `MCid` e `MCid.Habitacao` com dono, especialistas e descrição completa
- [ ] Patch cobre displayName e hierarquia, não só descrição
- [ ] O FQN completo (`MCid.Habitacao`) é usado em toda referência a subdomínio

---

# Épico 5 — Glossário

### ✅ HU-19 · Termos com sinônimos, referências e relações
**Como** analista, **quero** achar o termo pelo nome que eu uso, **para** não
depender de saber a sigla oficial.

`termos_mcid.yml` traz hoje apenas nome, displayName, descrição e `aplica_a`.

- [ ] `synonyms` (ex.: "Novo CAGED" / "CAGED")
- [ ] `references` (URL da fonte oficial que define o conceito)
- [ ] `relatedTerms` entre termos do MCID
- [ ] Nada de exemplo de valor de dado na descrição — regra do auditor vale aqui

---

### ⚠️ HU-20 · Glossário como entidade governada
**Como** curador, **quero** dono e revisores no glossário `MCID`, **para** que
termo novo passe por revisão em vez de entrar direto.

- [ ] Dono e revisores declarados
- [ ] Decidir se `mutuallyExclusive` se aplica a algum eixo
- [ ] Fluxo de aprovação registrado em uma linha na documentação

---

# Épico 6 — Qualidade

### ✅ HU-21 · Contratos de qualidade viram Test Cases no catálogo
**Como** consumidor, **quero** ver no catálogo se a tabela passou nos testes,
**para** confiar no número sem perguntar para o time.

Existem hoje: os testes do dbt, o `silver_contract`, o gabarito do boletim e o
`validar_silver_gx.py`. Nada disso chega ao OpenMetadata.

- [ ] Cada teste dbt de um modelo publicado vira Test Case na tabela
- [ ] Resultado da última execução publicado
- [ ] Publicamos passou/falhou e a regra — nunca a linha que falhou
- [ ] Test Suite por camada ou por produto (decidir e registrar)

**Depende de:** HU-08

---

### ⚠️ HU-22 · Reconciliações visíveis
**Como** analista, **quero** saber que duas séries parecidas não são
comparáveis, **para** não montar um cruzamento que o time já sabe que não
fecha.

`reconciliacoes.yml` já classifica cada cruzamento como `active`,
`blocked_not_equivalent` ou `frozen_edition_required`, com justificativa.

- [ ] Reconciliação ativa vira Test Case
- [ ] Reconciliação bloqueada vira propriedade customizada com a justificativa
- [ ] A justificativa aparece na tabela, não só no repo

**Depende de:** HU-17, HU-21

---

# Épico 7 — Linhagem ponta a ponta

Onde entram o MinIO e o bot de ingestão.

### ✅ HU-23 · MinIO como Storage Service
**Como** consumidor, **quero** o lake no catálogo, **para** que a linhagem não
comece na bronze com o parquet aparecendo do nada.

- [ ] Storage Service declarado em YAML (endpoint e bucket; sem credencial)
- [ ] Descrição, dono, domínio e etiquetas
- [ ] Prefixos de staging viram Containers
- [ ] Nenhum conteúdo de objeto é lido ou publicado

**Depende de:** HU-04

---

### ✅ HU-24 · Linhagem parquet → bronze
**Como** analista, **quero** ver de qual arquivo veio a tabela bronze,
**para** rastrear um número até a origem.

O caminho já está declarado: `sources.yml` traz `meta.caminho` por tabela, e a
macro `fonte_lake` o resolve. É só publicar o que já se sabe.

- [ ] Cada source do `lake_staging` vira aresta Container → tabela bronze
- [ ] Source sem `meta.caminho` falha o sync (a macro já falha por isso)

**Depende de:** HU-23

---

### ⚠️ HU-25 · Airflow como Pipeline Service
**Como** operador, **quero** as DAGs de ingestão no catálogo, **para** ver
quem produz cada parquet e quando rodou pela última vez.

- [ ] Pipeline Service declarado
- [ ] Cada DAG de ingestão do conjuntura vira entidade Pipeline com descrição,
      dono, domínio e etiquetas
- [ ] Agendamento e última execução publicados
- [ ] O dono da DAG (hoje um nome solto em `default_args`) resolve para a mesma
      entidade de HU-03

**Depende de:** HU-03

---

### ⚠️ HU-26 · Linhagem DAG → parquet → bronze
**Como** analista, **quero** o caminho inteiro num grafo só, **para** que
"de onde veio esse número" tenha uma resposta clicável.

- [ ] Aresta DAG → Container fechando o grafo com HU-24
- [ ] O grafo do `gold_continuo_fipezap` alcança `fipezap_trimestral_ingest_dag`

**Depende de:** HU-24, HU-25

---

### ✅ HU-27 · Linhagem coluna a coluna
**Como** analista, **quero** saber de qual coluna da silver veio a coluna da
gold, **para** avaliar impacto antes de mudar um modelo.

Hoje a linhagem é só tabela → tabela, sem SQL (decisão deliberada: não
publicamos SQL compilado).

- [ ] Mapeamento coluna a coluna declarado ou derivado sem publicar SQL
- [ ] Aplicado a silver → gold do conjuntura
- [ ] Se não der para derivar sem expor SQL, registrar a recusa e fechar a HU

**Depende de:** HU-12

---

### ⚠️ HU-28 · Superset como Dashboard Service
**Como** gestor, **quero** ver quais dashboards consomem cada tabela gold,
**para** saber o que quebra antes de mexer.

- [ ] Dashboard Service declarado
- [ ] Dashboard do boletim e os dois contínuos como entidades
- [ ] Linhagem gold → chart → dashboard
- [ ] Tabela gold mostra quem a consome

**Depende de:** HU-08

**Feito:** o serviço `Cidades - Superset` está documentado — descrição, dono,
domínio e etiqueta. Antes estava sem descrição e com dono `admin`.

**BLOQUEADO na linhagem, e não por código nosso.** A ingestão do Superset no
OpenMetadata é de **2026-07-22**. Os charts que os nossos scripts criam — os 26
`Conjuntura | …` do `bootstrap_conjuntura.py` e os 21 `Boletim | …` do
`build_boletim.py` — foram criados depois e **não estão no catálogo**. Dos 113
charts que estão, nenhum tem prefixo que a gente declare, e o dashboard do
boletim (id 14) também não está.

Ligar os 113 que estão lá exigiria adivinhar a tabela pelo título livre do
chart, e **linhagem errada é pior que linhagem ausente**.

Dois caminhos, os dois fora do código:

1. **Re-rodar a ingestão do Superset no OpenMetadata** (ação de quem administra
   a instância). É o caminho limpo: traz os charts com o `slice_id` como nome,
   que é a chave que o conector usa.
2. **Credencial do Superset para nós**: aí eu leio a API, descubro o dataset de
   cada chart e publico a linhagem sem esperar o conector. Criar as entidades de
   chart por conta própria foi descartado — o conector as nomeia pelo
   `slice_id`, e as nossas ficariam duplicadas com nome diferente.

---

# Épico 8 — Fechamento do ciclo

### ✅ HU-29 · Auditoria do lado do OpenMetadata
**Como** curador, **quero** um comando que compare a instância com o
declarado, **para** provar que a documentação está completa em vez de supor.

`auditar_metadados.py` audita o YAML do dbt. Ninguém audita o resultado.

- [ ] Lê a instância e compara com `dominios.yml`, `schemas.yml`, `termos_mcid.yml`
- [ ] Relata por produto: % de tabelas com domínio, produto, dono, tier,
      certificação, uso, etiqueta e termo; % de colunas com descrição e PII
- [ ] Aponta o que existe na instância e não está declarado (edição pela tela)
- [ ] `make governance-audit-om`, com modo `--strict` para gate de CI

**Depende de:** HU-09, HU-10

---

### ⚠️ HU-30 · Sync declarativo de verdade
**Como** curador, **quero** que remover algo do YAML remova do catálogo,
**para** que a promessa do arquivo seja verdade.

O docstring de `sincronizar_governanca.py` afirma: "o que não está escrito
neste arquivo some no próximo sync". O código **não faz isso** — ele cria e
atualiza, nunca remove. Além disso, domínios e produtos existentes só recebem
patch em `/description`: mudar displayName ou dono no YAML não muda nada.

- [ ] Patch cobre todos os campos declarados, não só a descrição
- [ ] O que saiu do YAML é removido (ou reportado, se remover for arriscado)
- [ ] Modo simulação mostra criações, alterações **e** remoções
- [ ] Rodar duas vezes seguidas produz `criados=0 atualizados=0`

---

### HU-31 · Identidade do bot de ingestão
**Como** administrador, **quero** que a sincronização use um bot com papel e
escopo próprios, **para** que a auditoria do catálogo mostre o que foi
automação e o que foi pessoa.

Hoje o sync usa um JWT genérico e carimba `admin` como dono padrão.

- [ ] Bot dedicado (`ingestion-bot` ou um nosso) com papel de escopo mínimo
- [ ] Token do bot é a credencial do sync
- [ ] Alterações automáticas aparecem como do bot, não de uma pessoa
- [ ] Registrado quem provisiona o bot na instância compartilhada

**Depende de:** HU-01

---

### HU-32 · Molde aplicado aos outros dois produtos
**Como** engenheiro de dados, **quero** rodar o mesmo sync para
`empreendimento_far` e `entidades_fds`, **para** que o conjuntura tenha sido
molde e não exceção.

- [ ] Nenhum caminho do sincronizador tem `if produto == "conjuntura"`
- [ ] Os dois produtos passam na auditoria de HU-29 nos mesmos critérios
- [ ] O que faltar neles é falta de YAML preenchido, nunca de código

**Depende de:** HU-29

---

### ⚠️ HU-33 · Caminho aberto para os conectores nativos
**Como** engenheiro de dados, **quero** que a troca do script pelos conectores
do OpenMetadata seja uma troca de aplicador, **para** não reescrever a
governança quando ela acontecer.

⚠️ **Reescrita em 2026-09-01, depois de comparar com `data-application-minc`.**

Não é migrar para conectores: é **recuperar o que já era nosso**. A integração
completa existe em `origin/refactor/openmetadata` (2026-08-18) e nunca foi
mergeada — 29 commits atrás da `main`, no layout antigo `airflow_lappis/`. A
MinC portou dali e evoluiu; a versão deles é a descendente mantida e traz uma
seção "Levar para outro projeto".

É também a explicação de a ingestão do `Cidades` não rodar desde 21-23/07: a DAG
que a roda nunca chegou à `main`. Isso bloqueia HU-21 (que o conector já
entregou uma vez), HU-28 e parte da HU-25.

Ver `docs/governance/comparacao-minc-openmetadata.md`.

- [ ] Trazer `helpers/openmetadata/` da versão da MinC, com os 4 ajustes
- [ ] Agendar a DAG de ingestão — é o que mantém o catálogo em dia
- [ ] Migrar domínio/tier/dono para `meta.openmetadata` no `schema.yml` e
      aposentar o push REST dessas três coisas
- [ ] Manter por cima: produto de dados, `Uso`, certificação, lake, linhagem de
      coluna, chaves e a auditoria — nada disso existe do outro lado
- [ ] **`markDeletedTables: false`** antes da primeira execução

Decisão registrada: **hoje ficamos no script próprio**. Esta HU só garante que
a migração continue barata.

- [ ] Fronteira explícita: YAML declara, aplicador aplica
- [ ] Mapeado o que o conector Postgres + dbt passaria a fazer sozinho
      (tabela, coluna, tipo, teste, linhagem) e o que continuaria nosso
      (domínio, produto, uso, propriedades customizadas)
- [ ] Documentado o que o conector sobrescreveria e como preservar o curado
- [ ] Nenhuma decisão das HUs anteriores impede a migração

---

## Ordem sugerida

```
HU-01 → HU-02 → HU-03          fundação; nada anda antes
  ├─ HU-06 → HU-04 → HU-05     serviço, banco, schema
  ├─ HU-08 → HU-09 → HU-10     tabela: produto, etiqueta, glossário
  │            └─ HU-16 → HU-17 → HU-18   produto e domínio completos
  ├─ HU-12 → HU-13 → HU-14     coluna
  └─ HU-23 → HU-24 → HU-25 → HU-26        linhagem até a origem
                     HU-29 → HU-30 → HU-32 → HU-33   fechamento
```

HU-07, HU-11, HU-15, HU-19, HU-20, HU-21, HU-22, HU-27 e HU-28 são
independentes e entram quando couber.

## Decisões pendentes do Lucas

1. **HU-13** — coluna sensível some do catálogo ou aparece marcada e sem
   descrição? Hoje some, e isso faz o catálogo descrever um schema que não é o
   real.
2. **HU-21** — Test Suite por camada ou por produto?
3. **HU-28** — Superset entra agora ou fica para depois? Está fora do que foi
   pedido.
4. **HU-30** — remover do catálogo o que saiu do YAML, ou apenas reportar?
   Remover é o que o arquivo promete; numa instância compartilhada, é também o
   que dá para errar mais feio.

---

### HU-34 · Trazer as fontes SFTP para a Etapa 02 do lake *(aguarda decisão do time)*
**Como** engenheiro de dados, **quero** que o arquivo entregue por SFTP passe
pela mesma Etapa 02 das outras fontes, **para** que o `sources.yml` deixe de
apontar para um arquivo datado.

`plugins/ingestor_lake.py` declara a convenção do lake:

    Etapa 01 (RAW)     -> raw/<fonte>/<dado>.<ext>
    Etapa 02 (STAGING) -> staging/<fonte>/<dado>.parquet     (sem data)

As 26 fontes que passam por DAG seguem isso. As **5 do SFTP não**: caem direto
do agente operador com o nome do sistema de origem, data inclusa —
`staging/sftp/caixa.geavo/GEAVO/Base_PF_FGTS_20260707.parquet`.

**O defeito é silencioso.** Quando a CAIXA entregar a base de agosto, o
`read_parquet` continua lendo a de julho: sem erro, sem alerta, só dado velho
até alguém reparar e editar o YAML.

**Glob não resolve.** Cada entrega é retrato completo do histórico, não
incremento — o silver de FGTS-PF filtra por `dt_assinatura` sobre uma base que
vai até antes de 2020. Um `Base_PF_FGTS_*.parquet` duplicaria a base inteira a
cada entrega acumulada, e leria tudo para descartar quase tudo.

**Proposta a validar:**

| hoje | proposta |
|---|---|
| `staging/sftp/caixa.geavo/GEAVO/Base_PF_FGTS_20260707.parquet` | `raw/caixa_geavo/base_pf_fgts_20260707.parquet` (histórico) |
| — | `staging/caixa_geavo/base_pf_fgts.parquet` (canônico, sobrescrito) |

- [ ] **Decisão arquitetural do time** — é o que trava esta HU
- [ ] Etapa de conversão que escreve o caminho canônico
- [ ] `sources.yml` com caminho estável, nunca mais editado por entrega
- [ ] `meta.dag` das 5 preenchida, fechando parte de HU-25/26
- [ ] Catálogo: um container estável por dataset, sem entidade datada nova a
      cada entrega e sem linhagem apontando para arquivo obsoleto

**Interino, enquanto não se decide:** publicar no catálogo a pasta
`staging/sftp/caixa.geavo/GEAVO` em vez dos 5 arquivos datados. Perde
granularidade e não aponta para arquivo que vai ficar velho. **Não aplicado** —
faz parte da mesma decisão.

**Nota de exposição:** os nomes revelam que existe extração de pessoa física da
CAIXA, com data. Avaliado: baixa severidade — a instância é interna ao governo,
o MinIO foi registrado sem credencial e as tabelas Bronze correspondentes já
estavam publicadas. É higiene, não incidente; o argumento forte para mudar é o
apodrecimento do catálogo, não a segurança.
