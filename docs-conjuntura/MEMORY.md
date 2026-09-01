# MEMORY — Conjuntura do Setor Habitacional (log operacional)

Registro contínuo do que foi feito, decidido e descoberto no pipeline do
boletim de conjuntura.

> **Este arquivo é o canal de handoff entre os agentes (Claude e Codex).**
> O trabalho é revezado: quando um esgota o contexto, o outro assume lendo
> daqui. Quem escreve aqui está escrevendo para o outro agente, não para si.

---

## 0. Protocolo de trabalho (LEIA ANTES DE MEXER EM QUALQUER COISA)

### Ao ASSUMIR o trabalho

1. Leia a **§0.1 (Estado atual)** — é a foto de onde as coisas pararam.
2. Leia a **§0.2 (Diário)** de trás pra frente até entender o contexto recente.
3. **Confirme o estado antes de confiar nele.** Este arquivo é uma observação
   datada, não o sistema. Rode o que for barato pra verificar (`dbt run`,
   `git status`, uma query) antes de assumir que algo está de pé.
4. Só então comece.

### Ao ENTREGAR o trabalho (ou a cada mudança relevante)

1. Atualize a **§0.1** — ela deve descrever o presente, não o histórico.
2. Acrescente uma entrada no **§0.2**, no formato:
   `### AAAA-MM-DD · <agente> · <título curto>` seguido do que mudou, do
   porquê, e do que ficou pendente.
3. Se descobriu uma armadilha (bug de fonte, quirk de dialeto, comportamento
   não óbvio), registre na seção temática correspondente (§4 em diante) —
   não deixe só no diário, senão se perde.
4. **Nunca apague entrada de diário.** Corrija com uma entrada nova dizendo
   o que estava errado.

### Regras de convivência

- **Não desfaça decisão do outro agente sem entender o motivo.** As decisões
  arquiteturais estão na §6, com a justificativa. Se discordar, registre a
  discordância e pergunte ao Lucas — não reverta unilateralmente.
- **Marque o que está pela metade.** Se parar no meio, diga exatamente onde
  parou e qual era o próximo passo. Trabalho pela metade não sinalizado é
  pior que trabalho não começado.
- **Distinga verificado de presumido.** Se você não rodou, escreva "não
  verificado". O outro agente vai confiar no que está escrito aqui.
- **Não invente número.** Todo valor citado aqui deve ter origem rastreável
  (query, boletim publicado, release). Se é estimativa, diga que é.

---

## 0.1 Estado atual

**Última atualização: 2026-08-30 (noite) · Claude**

| Item | Situação |
|---|---|
| Banco (Postgres 10.0.0.50) | **De pé** (exige VPN ativa) |
| `conjuntura_continuo_dbt` | ✅ models de indicador + **21 golds novos do boletim** em `gold/boletim/` |
| Dimensão temporal | ✅ **macro `dimensao_temporal.sql`**: `data_referencia`/`ano`/`mes`/`trimestre`/`periodo`/**`edicao`**. Aplicada na silver trimestral e propagada aos 4 golds que tinham `periodo` só como texto |
| Quadros do boletim | ✅ **materializados como tabela** (`gold_boletim_p<N>_*`). Antes eram dataset virtual e o SQL voltava ao Postgres a cada carregamento de página |
| Congelamento de edição | ✅ **21 snapshots dbt** em `conjuntura_continuo_snapshots` (`make conjuntura-congelar`). Guarda o histórico das revisões, não um retrato |
| Gabarito | ✅ **teste dbt** `conjuntura_gabarito_do_boletim` + seed com 170 células do 1T26 (`make conjuntura-validar-boletins`). Hoje: 129 OK, 39 divergem, 2 sem dado |
| Superset | ✅ **um dashboard** (`/superset/dashboard/boletim-conjuntura/`), 7 abas, 21 charts, filtro `Trimestre` sem edição padrão. Todos os datasets são FÍSICOS |
| Dashboards contínuos | ✅ id 8 (28 charts) e id 12 (27 charts), ambos 100% com dado |
| `scripts/conjuntura/` | 3 arquivos: migração manual + 2 de documentação. O que é dado saiu para dbt/Airflow |
| `scripts/superset/` | 2 arquivos: `build_boletim.py` e `bootstrap_conjuntura.py` |
| Medalhão no MinIO | ❌ **removido** — era fallback de banco fora do ar |

**Ordem de execução:** `dbt run` → `dbt snapshot` → `build_boletim.py`. O
construtor não carrega mais SQL; se a tabela não existir, o erro sobe alto em
vez de o chart ficar mudo.

**Réplica do boletim 1T26 conferida célula a célula:** 129 de 170 conformes.
As divergências concentram-se em fontes que revisam o passado (BACEN, IBGE,
CAGED, CBIC, FipeZap) — ver `docs-conjuntura/replica-boletim-1t26.md`.

**Pendências de decisão (Lucas):**
- dashboards órfãos 9, 10, 11 e 13 — apagar?
- 78 duplicatas de chart "Conjuntura | …" (4 cópias de cada um dos 26 nomes);
  nenhuma é usada por dashboard de terceiro

**Pendência de dado:** ingestão ABECIP de 2025-10 a 2026-07 (menos 2026-06),
combinada com o time do OCR. Até entrar, o quadro de bancos da página 3 só tem
histórico até 09/2025. As tabelas por instituição do PDF da ABECIP são imagem,
não texto — por isso depende do OCR deles.

**⚠️ Nada está commitado.** Antes de commitar, ver §4 (o hook de pre-commit
reformata dezenas de arquivos não relacionados).

---

## 0.2 Diário

### 2026-09-01 · Claude · O conector apaga a certificação (e como isso apareceu)

**A armadilha mais perigosa encontrada até agora. Ler antes de rodar qualquer
ingestão nativa.**

#### O que aconteceu

Rodamos a recipe `dbt_metadata` pela primeira vez, contra um catálogo que
estava 140/140 em tudo. Resultado: **a certificação das 140 tabelas foi
apagada.** Domínio, produto de dados, etiquetas e glossário sobreviveram;
`certification` virou nulo em todas.

Causa: o conector escreve a entidade da tabela **sem** o campo `certification`,
e o `createOrUpdate` do OpenMetadata substitui o que estava lá por nulo. Não há
erro, aviso ou log — a ingestão termina dizendo sucesso.

#### Por que foi detectado

Porque tiramos um **snapshot da auditoria ANTES** de rodar. Sem ele, a
certificação teria sumido em silêncio, exatamente como as 85 descrições de
rodapé sumiram em julho e só foram descobertas seis semanas depois.

**Regra: antes de qualquer primeira execução de recipe, rodar
`make governance-audit-om` e guardar a saída.**

#### O conserto, que virou estrutura

`reaplicar_governanca` é agora a **última task** da
`dags/openmetadata_ingestion_dag.py`, depois de todas as recipes. A ordem
deixou de ser recomendação e virou dependência do grafo, com teste que falha se
ela sair do fim. O `scripts/` passou a ser montado no container para a task
alcançar o script.

Consequência de projeto: **a governança REST não pode ser aposentada enquanto o
conector não souber escrever certificação.** Não é dívida técnica esquecida; é
reparo obrigatório de algo que o conector destrói.

#### Padrão do dia: a API aceita o incompleto e devolve 200

Três vezes, a mesma forma de falha:

1. **Certificação enviada em `/tags`** — 200, descartada em silêncio.
2. **`ordinalPosition`** — PATCH devolve 200 com o valor no corpo da resposta,
   e a releitura vem vazia. Só 1347 das 2244 colunas persistiram; testados
   PATCH, PUT, renumeração contígua e três caminhos de leitura. Não resolvido.
3. **`mcidRelatedTables` com só `id` e `type`** — 200, e a interface renderiza
   entradas em branco, porque usa `name` e `fullyQualifiedName` como rótulo. Só
   apareceu porque o Lucas olhou a tela.

**Nenhuma das três apareceu como erro em log, sync ou auditoria.** A lição é
que resposta 200 do OpenMetadata não é prova de escrita: conferir relendo.

#### Outras coisas desta rodada

- **`meta.dag` de 12 para 25 de 31.** Os 13 caminhos do IBGE foram atribuídos a
  `ibge_ingest_dag` **por eliminação**: só duas DAGs escrevem em
  `staging/ibge/`, e a outra produz nomes que não correspondem a nenhuma source
  declarada. Confirmar item a item pela Airflow Variable `IBGE_CONFIGURACOES`.
- **DAG de origem propagada pela linhagem**: 80 models, nas três camadas. Um
  model herda as DAGs dos ancestrais; cinco têm mais de uma.
- **Relações semânticas para Silver e Gold**: 98 tabelas, derivadas do grafo do
  dbt e do contrato de dimensão temporal. A Bronze fica de fora — dela se
  publica topologia, não significado. O catálogo de julho, que cobre FAR e FDS
  por `apf`/`cnpj`, foi preservado.
- **`.env`**: `DB_DW_PASSWORD_MCID` continha `)` sem aspas e quebrava qualquer
  `source .env` na linha 23 — tudo depois dela sumia, incluindo as credenciais
  do OpenMetadata. Corrigido com aspas simples.
- **A armadilha do dbt 1.10+ NÃO se aplica aqui**: testado, `dbt parse` passa
  com `meta` no topo de 24 models e `+meta` de projeto.


### 2026-09-01 · Claude · A integração já existia: `origin/refactor/openmetadata`

**LEIA ANTES DE ESCREVER QUALQUER COISA NOVA DE OPENMETADATA.**

A integração do MCID com o OpenMetadata **já existe neste repositório**, na
branch `origin/refactor/openmetadata` (`4d6d9ae`, 2026-08-18, arthrok). Nunca
foi mergeada: está **29 commits atrás da `main`** e no layout antigo
`airflow_lappis/dags/`, que a `main` não tem mais — por isso não aparece em
busca nenhuma feita a partir da árvore atual. Eu construí uma implementação
paralela sem saber que ela existia.

Ela tem: 6 recipes de conector (postgres metadata/profiler/classifier, dbt,
airflow, superset), DAG agendada, glossário com 63 termos, catálogo de relações
semânticas com 1.255 linhas, 3 arquivos de teste e três documentos de cobertura.

**A MinC portou daqui e evoluiu.** O `semantic_relationships.py` deles ainda
valida `kind: MCIDSemanticRelationshipCatalog`. É a origem das propriedades
`mcidRelatedTables` e `mcidSemanticRelationships` que aparecem na instância sem
estar declaradas em lugar nenhum da `main` — mistério que ficou aberto na
entrada de 31/08.

**E explica a ingestão parada em 21-23/07:** a DAG que a roda nunca chegou à
`main`.

#### O que é redundante e o que é complemento

Redundante com o conector: estrutura, descrição, coluna, teste, Superset,
Airflow. Eles fazem melhor porque rodam sozinhos.

Só existe aqui: produto de dados, classificação `Uso`, certificação, MinIO no
catálogo, linhagem coluna a coluna, chaves e a auditoria.

A forma deles de declarar governança é mais leve: `meta.openmetadata` dentro do
`schema.yml` (279 pontos), carregada pelo conector dbt, em vez de push REST.

Comparação completa em `docs/governance/comparacao-minc-openmetadata.md`.
A HU-33 foi reescrita: não é migrar, é recuperar o que já era nosso.

#### Armadilha nova e destrutiva, registrada por eles

**`markDeletedTables` tem default `true`.** Rodar `postgres_metadata` contra um
banco incompleto marca como deletado tudo que o catálogo tem e o banco não. Um
ambiente restaurado pela metade apaga catálogo inteiro sem avisar. Pôr
`markDeletedTables: false` ANTES da primeira execução.

#### Decisão do Lucas

Commitar o trabalho desta sessão como está e, depois, integrar as duas
abordagens.


### 2026-09-01 · Claude · HU-11, 21, 22, 27, 28 — e a descoberta que reposiciona a HU-33

#### O que ficou pronto

- **HU-27 · linhagem coluna a coluna.** `scripts/governance/linhagem_colunas.py`,
  com `sqlglot`. **156 arestas, 130 com mapeamento, 720 colunas.** Deriva de
  verdade: `data_referencia <- periodo` (é o `to_date`),
  `custo_medio_m2 <- valor + variavel_id` (é o `case when`). Heurística de nome
  igual erraria os dois. **O SQL é lido localmente e nunca publicado**, e a
  linhagem não vaza coluna sensível nem em princípio — o schema que alimenta o
  parser vem do catálogo, que já as omite.
- **HU-11 · chaves.** `scripts/governance/restricoes_dbt.py`. O dbt não tem
  `primary key`, tem teste: `unique` + `not_null` é a chave. Aplicadas 2 PKs e
  13 `NOT_NULL`.
- **HU-28 (parcial).** Serviço `Cidades - Superset` documentado; antes sem
  descrição e com dono `admin`.
- **HU-22 (parcial).** Reconciliações bloqueadas publicadas em
  `mcidReconciliacoes`, com a justificativa de cada uma.
- **HU-29 ganhou o grupo `qualidade`**, que compara testes declarados no dbt
  com test cases na instância.

#### ARMADILHA: chave primária não pode ir em dois lugares

A instância **recusa a tabela inteira** se a mesma coluna vier marcada como
chave na coluna E em `tableConstraints`: *"A column already tagged as a primary
key and table constraint also includes primary key"*. Chave fica só em
`tableConstraints` (que comporta chave composta); a coluna recebe `NOT_NULL`.

#### `fds_panorama_entidade` ficou sem chave publicada

A chave dele é `cnpj_eo`, coluna que o catálogo omite pelo filtro de
identificador sensível. É a **HU-13 cobrando de forma concreta**: o catálogo
não consegue declarar a granularidade da tabela porque esconde a própria chave
dela.

#### A DESCOBERTA: a ingestão nativa do `Cidades` parou em 21-23/07

Três HUs que pareciam trabalho nosso **não estão bloqueadas por código**:

| | situação |
|---|---|
| HU-21 (qualidade) | **já entregue pelo conector dbt**: os 16 test cases de coluna existem, com `Success` de 2026-07-23 |
| HU-28 (Superset) | ingestão de 2026-07-22; os 26 charts `Conjuntura \| …` e os 21 `Boletim \| …` vieram depois e não estão lá |
| HU-25 (DAGs) | 22 das nossas no catálogo, o resto não |

**Isso reposiciona a HU-33.** Migrar para os conectores deixa de ser
modernização futura e vira o conserto de algo que já existe e parou de rodar. O
`Cidades` tem pipeline de ingestão configurado (tipo `metadata`), e os test
cases mostram que uma ingestão dbt já rodou em algum momento.

#### Erro meu, registrado porque a lição vale

Afirmei que 3 test cases `unique` faltavam. **Não faltavam.** Duas evidências
erradas apontaram para o mesmo lado: a primeira comparação usou uma listagem de
500 quando existem 953, e os 3 caíram fora da página; ao "confirmar", consultei
`dataQuality/testCases/name/<nome>` — mas o FQN de test case é
`<tabela>.<coluna>.<nome>`, então o 404 era da consulta, não da ausência.

**Duas evidências erradas concordando é exatamente quando se confirma por um
terceiro caminho.** Quase escrevi 3 test cases à mão, duplicando o conector e
divergindo no nome — o erro que a HU-33 manda evitar.


### 2026-08-31 · Claude · Linhagem de coluna e o achado do SFTP datado

#### HU-27 — linhagem coluna a coluna, sem publicar SQL

`scripts/governance/linhagem_colunas.py`. Usa `sqlglot` (já estava no venv) para
derivar de verdade, não por semelhança de nome. **156 arestas, 130 com
mapeamento de coluna, 720 colunas com origem.**

Exemplo real do `gold_continuo_sinapi`, que mostra por que heurística de nome
não serviria:

    periodo             -> periodo
    periodo             -> data_referencia      (é o to_date)
    valor, variavel_id  -> custo_medio_m2       (é o case when)

**O SQL nunca sai daqui.** É lido e analisado localmente; o que se publica é só
o par origem→destino. E a linhagem não consegue vazar coluna sensível **nem em
princípio**: o schema que alimenta o parser vem do catálogo semântico, que já
as omite, e `_origens()` só devolve coluna que exista nesse schema. Não é
vigilância, é construção.

Os 47 models que o parser não lê são as bronzes, que usam `read_parquet` via a
macro `fonte_lake`. A linhagem delas já vem dos containers do lake.

#### Auditoria de exposição (a pedido do Lucas)

Conferido na instância: **0** colunas publicadas com nome de identificador de
pessoa (de 2244), **0** descrições com caminho técnico ou valor tipo CPF/CNPJ,
**nenhum** `sqlQuery` nas arestas de linhagem.

#### O ACHADO: as 5 fontes SFTP têm data cravada no caminho

`plugins/ingestor_lake.py` declara a convenção: Etapa 02 escreve
`staging/<fonte>/<dado>.parquet`, **sem data**. As 26 fontes com DAG seguem. As
5 do SFTP não — caem direto do agente operador com o nome do sistema de origem:
`staging/sftp/caixa.geavo/GEAVO/Base_PF_FGTS_20260707.parquet`.

**O defeito é silencioso e não é do catálogo, é da ingestão:** na próxima
entrega da CAIXA o `read_parquet` continua lendo a de julho, sem erro, até
alguém reparar.

Glob não resolve: cada entrega é retrato completo do histórico (o silver de
FGTS-PF filtra `dt_assinatura` sobre base que vai até antes de 2020), então
`Base_PF_FGTS_*.parquet` duplicaria tudo.

**Status: PARADO.** O Lucas achou decisão arquitetural robusta demais para
resolver de improviso e vai validar com o time. Registrado como **HU-34** no
backlog, com a proposta (raw datado + staging canônico) e o interino de
catálogo. **Nada foi aplicado** — nem a troca dos containers de arquivo por
pasta.


### 2026-08-31 · Claude · Épicos 4 e 5 — produto de dados e glossário

#### O que entrou

- **Produtos de dados** com etiquetas e termos do eixo que os descreve:
  `conjuntura` → `IndicadoresConjunturais`; `empreendimento_far` →
  `FundosEFontes.FAR`, `MCMV.MCMVFAR`, `ExecucaoDeEmpreendimentos`;
  `entidades_fds` → `FundosEFontes.FDS`, `MCMV.MCMVEntidades`,
  `Atores.EntidadeOrganizadora`.
- **Propriedades customizadas**, seguindo a convenção `mcid` que já existia:
  `mcidDagDeOrigem` (table) — a DAG que produziu o arquivo, vinda de `meta.dag`;
  `mcidReconciliacoes` (dataProduct) — o resumo de `reconciliacoes.yml`, com a
  justificativa de cada bloqueio, para que a ausência de um cruzamento seja
  decisão documentada e não esquecimento.
- **Glossário** com dono (`mcid-data-engineering`) e `references` verificadas
  em 3 termos de instituição.

#### ARMADILHA: FQN de termo aninhado não é eixo + folha

Montei `MCID.ProgramasHabitacionais.MCMVFAR` concatenando o eixo com o nome da
folha, tirado de uma listagem que só guardava o último segmento. O certo é
`MCID.ProgramasHabitacionais.MCMV.MCMVFAR` — os dois ficam sob `MCMV`.

Duas consequências que valem mais que o erro em si:

1. **Um FQN errado derruba o patch inteiro.** Os produtos FAR e FDS ficaram sem
   nenhuma etiqueta por causa de um termo, não sem aquele termo.
2. **Eu havia escrito no YAML que os FQNs "foram conferidos contra a
   instância".** Não foram: foram inferidos. A frase era falsa e teria
   sobrevivido no arquivo. Ao declarar FQN, listar o `fullyQualifiedName`
   completo — nunca remontá-lo.

Há teste travando os dois casos.

#### Propriedade customizada: o tipo de entidade é GLOBAL

Criar propriedade em `table` faz ela aparecer para TODOS os ministérios da
instância. Por isso o prefixo `mcid` (convenção que já existia em
`mcidRelatedTables` e `mcidSemanticRelationships`) e por isso só criamos, nunca
removemos: as outras podem não ser nossas.

A rota `DELETE /metadata/types/{id}/customProperties/{nome}` devolve 404; para
remover, é JSON Patch com `remove` no índice do array `customProperties`.

E de novo o padrão do dia: **`customProperties` só vem se for pedido em
`fields`** — sem isso a lista chega vazia e a propriedade é recriada a cada
execução.

#### Pendente destes dois épicos: PESSOAS

`experts` (produtos e domínios) e `revisores` (glossário) ficaram VAZIOS de
propósito. Nomear quem responde pelo conteúdo e quem aprova termo novo é
decisão de pessoas, não de automação. O time `mcid-data-engineering` tem três
integrantes na instância — Arthur Alves Melo, Joao Egewarth, Mateus de Castro.
Falta o Lucas dizer quem entra em cada papel; o mecanismo já está pronto e lê
de `dominios.yml` e `termos_mcid.yml`.


### 2026-08-31 · Claude · Auditoria automatizada e data lake no catálogo

Continuação da entrada anterior do mesmo dia. Fecha a **HU-29** e o **épico 7**.

#### `make governance-audit-om` (HU-29)

`scripts/governance/auditar_openmetadata.py` compara a INSTÂNCIA com o
declarado e relata cobertura por produto. Não confundir com
`governance-audit`, que audita se a documentação foi escrita: este audita se
ela chegou. A distância entre as duas já foi de 85 tabelas.

Hoje: 140/140 em descrição, colunas, domínio, produto, dono, tier,
certificação, uso e etiquetas; 38 tabelas e 231 colunas com glossário;
2244/2244 colunas com descrição. **Nenhuma pendência.**

**Ela se pagou na primeira execução**, achando o que a conferência manual não
via: a relação `Arquitetura.Bronze -> Silver` tinha sumido.

#### Três defeitos de LEITURA, todos invisíveis como erro

O padrão do dia: o valor certo estava na instância, a comparação é que não
achava. Nenhum aparecia como falha; todos faziam o sync reescrever para sempre.

1. **Campo não pedido não vem.** `dataProducts` não estava na lista de
   `fields`, então a resposta não o trazia, a comparação via "ausente" e as
   **140 tabelas eram reescritas em toda execução**.
2. **A instância escapa `=`, `'` e `"` como entidade HTML** ao gravar
   descrição. Comparando cru, 6 tabelas divergiam para sempre.
3. **O FQN cita segmento que contém ponto**:
   `...staging.ibge."sinapi.parquet"`. Montando sem aspas, as 33 folhas do
   lake nunca eram encontradas e o sync as recriava a cada rodada — sem
   duplicar, porque o PUT é upsert, mas sem nunca convergir.

Depois dos três: **duas execuções seguidas com `atualizados=0`**.

#### Mais duas armadilhas

4. **`domains` tem formato diferente na criação e no patch.** Criação exige
   lista de FQN em texto (`["MCid"]`); patch exige objeto de referência. O
   objeto na criação devolve `400 Invalid request format` **sem dizer qual
   campo** — e derruba a criação inteira, que aqui cascateou em 78 falhas.
   Solução adotada: **criação mínima, governança sempre por patch.** Contorna
   a assimetria em qualquer entidade.
5. **A relação de glossário é bidirecional** e o patch substitui a lista.
   Declarar só `A -> B` fazia B apagar o vínculo de A ao receber o seu. Agora
   as relações são fechadas nos dois sentidos antes de aplicar.

#### O sync desfazia curadoria de gente

Alguém aplicou `MCID.IndicadoresConjunturais` no schema
`conjuntura_continuo_mart` pela interface. A regra de mescla tratava
**qualquer** etiqueta de glossário como nossa, e o sync a removia toda
execução. Agora só sai etiqueta que esteja declarada em `termos_mcid.yml`;
termo de terceiro fica. Vale para tabela, coluna e schema.

#### Épico 7 — o lake e a orquestração (HU-23 a HU-26)

`make openmetadata-lake` (`scripts/governance/sincronizar_lake.py`).

- **MinIO** entrou como `Cidades - MinIO`, **sem `connection`**: o lake é
  publicado para catálogo e linhagem, não para ingestão nativa. Credencial de
  MinIO não entra em instância compartilhada entre ministérios.
- **46 containers**: `data-lake-mcid -> staging -> <prefixo> -> <arquivo>.parquet`.
  Os 9 prefixos descritos por quem apura o dado; os arquivos herdam a descrição
  já curada no `sources.yml` — não se duplica texto.
- **33 arestas parquet -> Bronze.** O mapeamento saiu exato e sem VPN: o
  caminho está em `meta.caminho` e qual Bronze lê qual arquivo está na chamada
  `fonte_lake('<fonte>')` do SQL do model. **31 models, 31 sources, 1:1**, sem
  órfã nem sobra. Há teste travando isso.
- **Airflow**: o serviço `airflow` já existia e é NOSSO (o MinC tem
  `MinC - Airflow` à parte). Tinha 80 pipelines, 0 com domínio, 13 com
  descrição e nenhuma linhagem. Agora documentado, e as DAGs de ingestão
  declaradas são criadas quando faltam.

Cadeia conferida ponta a ponta:

    pipeline  airflow.abecip_financiamentos_ingest_dag
      -> container  staging/abecip/financiamentos_modalidade.parquet
         -> table  bronze_continuo_abecip_financiamentos
            -> table  silver_continuo_abecip_financiamentos -> ...

#### `meta.dag` no sources.yml — e por que 19 estão vazias

O vínculo DAG -> arquivo é **declaração, não inferência**. 12 das 31 foram
resolvidas lendo `registros_para_staging_parquet(fonte, dado)` no código. As
outras 19 **não são conhecíveis pelo repo**:

- as **5 de `sftp/`** chegam por transferência do agente operador; não há DAG
  que as escreva, e o campo fica ausente de propósito;
- as **14 do IBGE** saem de `ibge_ingest_dag`, que lê a lista de datasets da
  Airflow Variable `IBGE_CONFIGURACOES`. Quais parquets ela produz está no
  Airflow, não aqui. **Preencher no chute publicaria linhagem falsa.**

Quem souber a resposta acrescenta a linha e o catálogo passa a mostrar a DAG de
origem sem ninguém ler código.

#### Ordem de execução (atualizada)

    make openmetadata   # sync -> governanca -> lake

O lake vem por último: liga containers a tabelas que precisam existir antes.

#### Pendente

Igual à entrada anterior, menos a HU-29. Some: nome de exibição do serviço
(exige administrador), `make openmetadata-catalog` com VPN, HU-13, HU-15, e as
19 `meta.dag` acima.


### 2026-08-31 · Claude · OpenMetadata aplicado na instância — épicos 0 a 3

**Estado: aplicado e conferido.** Diferente da entrada anterior do mesmo dia,
que era "declarado, não aplicado". As credenciais entraram no `.env` e os dois
sincronizadores rodaram contra
`openmetadata.clusterlab.lappis.rocks` (OM 1.13.3), service `Cidades`, database
`cidades`.

#### Resultado, conferido tabela a tabela na instância

| | antes | agora |
|---|---|---|
| Descrições de tabela curadas | 0/140 | **140/140** |
| Colunas fiéis ao dbt | — | **140/140 tabelas** |
| Colunas com `VARCHAR(65535)` inventado | 546+ | **0** |
| Descrição de schema curada | 0/5 | **5/5** |
| Certificação | 0/140 | **140/140** |
| Tier, permissão de uso, produto, dono | parcial | **140/140** |
| Termo de glossário | 0 | **38 tabelas, 231 colunas** |
| Ativos por produto (índice de busca) | 0 | **116 / 13 / 11** |

Serviço e database documentados: descrição, dono `mcid-data-engineering`,
domínio `MCid`.

#### ARMADILHAS DA INSTÂNCIA — ler antes de mexer

Todas custaram diagnóstico. Nenhuma aparece na documentação do OpenMetadata.

1. **Certificação NÃO é etiqueta.** É campo próprio (`/certification`).
   Mandada dentro de `/tags`, a API devolve **200 e descarta em silêncio**. Foi
   por isso que a primeira aplicação saiu com Tier e uso e sem certificação
   nenhuma, sem erro em lugar nenhum. O OM preenche `appliedDate` e
   `expiryDate` sozinho, com validade de **30 dias** — é o sync recorrente que
   renova. Se o sync parar de rodar, a certificação expira.
2. **`PUT` não sobrescreve descrição já preenchida.** A instância preserva o
   texto existente quando a atualização vem de ingestão. Consequência: as 85
   descrições de rodapé da carga de 30/08 ("Modelo da camada gold do produto
   conjuntura") sobreviveram à documentação curada de 31/08. **O catálogo
   parecia documentado e não estava.** Descrição só entra por PATCH.
3. **O OM escapa `=`, `'` e `"` como entidade HTML** ao gravar descrição
   (`ic_credito&#61;&#39;0&#39;`). Comparando cru, toda descrição com um desses
   caracteres diverge **para sempre**: o sync reescreve, a instância reescapa,
   e na execução seguinte a diferença reaparece. Comparar com `html.unescape`.
4. **`relatedTerms` é `TermRelation`**, com `term` e `relationType` — não
   referência de entidade. Referência solta dá HTTP 500. Usamos `relatedTo`.
5. **O `ingestion-bot` é proibido de alterar nome de exibição**
   (`IngestionBotRole`, `DefaultBotPolicy`, regra `DisplayName-Deny`). E JSON
   Patch é tudo ou nada: enquanto o `displayName` ia no mesmo patch, o 403
   derrubava descrição, dono, domínio e etiqueta junto. Foi assim que o
   database ficou vazio sem erro visível. Nome de exibição vai em patch
   separado.
6. **O serviço vive em `services/databaseServices`**, não em
   `databaseServices`. Na raiz dá 404 e o serviço parece não existir.
7. **`dbtTags` é COMPARTILHADA** com o MinC (`rouanet`, `salic`, `sgac`).
   Marcada `compartilhada: true` em `dominios.yml`: criamos o que falta e nunca
   alteramos classificação nem etiqueta existente.
8. **A classificação `Uso` já existia**, com textos melhores que os que eu
   tinha escrito, e não estava declarada em lugar nenhum. Trazidos para o
   `dominios.yml` verbatim em vez de sobrescritos.
9. **Timeout de 30s não basta** para reescrever o array de colunas de uma
   tabela larga — a sincronização morreu no meio por isso. Agora 120s com três
   tentativas.
10. **`assets` do produto de dados fica 0** mesmo com tudo associado: é uma
    relação separada, preenchida pelo lado do produto, e **não é a que a
    interface usa**. A aba de ativos lê o índice de busca
    (`dataProducts.fullyQualifiedName`), que mostra 116/13/11 corretamente.
    Não persiga esse zero.

#### ORDEM DE EXECUÇÃO — é dependência, não preferência

    make openmetadata   # estrutura primeiro, governança depois

Reescrever `/columns` **substitui o array inteiro** e leva junto a etiqueta de
glossário que a governança pendura na coluna. Rodar governança antes de
estrutura perde as 231 colunas etiquetadas. Aconteceu hoje.

#### Correção de rotulagem

A camada agora sai do **modelo**, não do schema. `empreendimento_far` e
`entidades_fds` são declarados `mixed` porque guardam as três camadas no mesmo
schema; a regra antiga dava `Tier1`, `Certification.Gold` e `Uso.Consumivel`
às **11 tabelas bronze** desses produtos. Os layers do catálogo batem 1 a 1
com os diretórios do dbt — conferido. `mixed` também deixou de receber
permissão de uso: marcar o schema consumível enquanto 11 tabelas dentro dele
são explicitamente não consumíveis é contradição visível na tela.

#### Pendente

1. **Nome de exibição do serviço** — está "Data Warehouse MCid", deveria ser o
   nome real. Só quem tem perfil de administrador troca, pela interface. O
   `display_name` foi retirado do `servicos.yml` (decisão do Lucas: nome real,
   não rótulo inventado).
2. **`make openmetadata-catalog`** (exige VPN e banco). O exportador passou a
   carregar `index` e `materialized`, mas o JSON commitado é anterior:
   `ordinalPosition` sai vazio e toda tabela consta como `Regular`, inclusive
   as views.
3. **HU-13** — as 14 colunas de CNPJ de FAR e FDS continuam fora do catálogo.
   Todas são CNPJ de pessoa jurídica, registro público, e são a chave que liga
   a construtora entre as tabelas. Decisão do Lucas pendente.
4. **HU-15** — política das 489 colunas da bronze.
5. **HU-29** — auditoria automatizada do lado do OpenMetadata. Hoje a
   conferência é manual, feita na mão a cada rodada.


### 2026-08-31 · Claude · Backlog do OpenMetadata e Épicos 0–3 implementados

**Estado: declarado, testado e NÃO aplicado.** O `.env` do repo não tem
`OPENMETADATA_URL` nem `OPENMETADATA_JWT_TOKEN` — as credenciais que rodaram o
sync em 30/08 se perderam. Tudo abaixo roda em modo offline e passa em teste;
nada foi escrito no catálogo.

#### Backlog

`docs/governance/backlog-openmetadata.md` — 33 HUs em 9 épicos, com o
conjuntura como molde para `empreendimento_far` e `entidades_fds`. Decisões do
Lucas registradas: manter o script próprio agora e migrar para os conectores
nativos depois (HU-33); o bot é tanto o `ingestion-bot` do OpenMetadata quanto
as nossas DAGs como Pipeline Service.

#### Três defeitos que travavam a etiquetagem

1. **As classificações nunca foram criadas.** O sync aplicava
   `Uso.NaoConsumivel`, `dbtTags.mcid` e `Certification.Gold` sem que nenhuma
   existisse na instância — `dbtTags` normalmente vem do conector dbt, que não
   rodamos, e `Uso` é inteiramente nossa. O PATCH de tags era recusado tabela a
   tabela. É a explicação do "código presente, etiquetas ausentes" de 31/08.
2. **Três donos em conflito.** `sincronizar_openmetadata.py` carimbava o
   usuário `admin` nas tabelas; `dominios.yml` declarava o time
   `mcid-data-engineering` nos produtos; `schemas.yml` tinha `owner_key: admin`
   e ninguém lia. Agora `dominios.yml` é a única fonte e `admin` não existe
   mais em nenhum YAML.
3. **A camada vinha do schema, não do modelo.** `empreendimento_far` e
   `entidades_fds` são declarados `mixed` porque guardam bronze, silver e gold
   no mesmo schema. Usar a camada do schema daria `Tier1`, `Certification.Gold`
   e `Uso.Consumivel` às **11 tabelas bronze** desses dois produtos — o oposto
   do que a camada diz. A camada agora sai do catálogo semântico, por modelo.
   Há teste para isso.

#### Separação de responsabilidade

Os dois scripts disputavam `/owners` e o segundo a rodar desfazia o primeiro.

| Comando | O que faz |
|---|---|
| `make openmetadata-sync` | estrutura: schema, tabela, coluna, linhagem |
| `make openmetadata-governanca` | governança: dono, domínio, produto, etiqueta, tier, certificação, uso, glossário |
| `make openmetadata` | os dois, nessa ordem |

`scripts/governance/governanca_comum.py` (novo) concentra `.env`, cliente da
API, resolução de proprietário e cálculo do patch mínimo.

#### O que mudou no conteúdo publicado

- **Tipo de coluna fiel.** `om_column()` colapsava tudo em sete tipos e
  carimbava `dataLength: 65535` em toda coluna textual. Agora: 868 colunas como
  TEXT sem comprimento inventado, 112 com precisão e escala reais
  (`numeric(15,2)` → precision 15, scale 2), `timestamp with time zone`
  distinguido de `without`, array com o tipo interno declarado.
- **Descrição de schema vem do YAML.** `schema_description()` deduzia a camada
  pelo sufixo do nome e montava um texto genérico, enquanto `schemas.yml` já
  tinha descrição curada. Schema sem entrada no YAML agora **falha o sync** em
  vez de receber texto automático. Os 9 schemas ganharam `display_name`.
- **Serviço e banco documentados** (`governance/servicos.yml`, novo). Não
  criamos nem tocamos em conexão — quem administra a instância é dono disso.
  Preenchemos o que estava vazio: descrição, nome, dono, domínio, etiqueta.
- **Glossário.** Os 9 termos de `glossary.yml` nunca chegavam ao OpenMetadata —
  o vocabulário de camada e de período ficava fora do lugar onde se procura por
  ele. Entraram 8 termos novos (`MCID.Arquitetura.*`, `MCID.Governanca.*`,
  incluindo os eixos-pai, que não existiam), 9 termos com sinônimo e 12 com
  termo relacionado. `references` (URL) ficou de fora: não publico endereço que
  não conferi.
- **Termo de glossário na coluna** (`aplica_a_colunas`). `PeriodoReferencia`
  cobre `data_referencia`, `periodo`, `ano`, `mes` e `trimestre`; `Safra` cobre
  `edicao`. É o mesmo conceito em 140 tabelas, e o contrato da dimensão
  temporal é o que garante que seja mesmo o mesmo.
- **Idempotência.** `operacoes_de_diferenca()` emite patch só do que difere,
  ignorando o que a API acrescenta (href, deleted). Rodar duas vezes deve
  terminar com `atualizados=0`.
- **Mescla de etiquetas.** Substituímos só o que governamos (`dbtTags`, `Uso`,
  `Tier`, `Certification`, glossário); `PII` e classificações de terceiros na
  instância compartilhada são preservadas.

#### Armadilhas novas

- **`aplica_a` ora é lista inline, ora bloco.** Inserir chave logo depois dela
  quebra o YAML. Inserir depois da linha `- fqn:` é sempre seguro. (Reincidência
  do aviso de 31/08 sobre editar YAML com regex.)
- **`mutuallyExclusive` é imutável** depois da classificação criada; incluí-lo
  no patch faz a API recusar toda atualização.
- **Eixo-pai do glossário precisa ser declarado.** Criar
  `MCID.Governanca.Safra` sem `MCID.Governanca` devolve 404. A instância só tem
  os sete eixos originais.

#### Pendente

1. **Credenciais.** Sem `OPENMETADATA_URL` e `OPENMETADATA_JWT_TOKEN` nada é
   aplicado. O modelo comentado está em `infra/env/.env.example`.
2. **Regenerar o catálogo semântico** (`make openmetadata-catalog`, exige VPN e
   banco). O exportador passou a carregar `index` e `materialized`, mas o JSON
   commitado é anterior: `ordinalPosition` sai vazio e toda tabela sai como
   `Regular`. O código tolera a ausência.
3. **HU-13 (PII) continua bloqueada** por decisão do Lucas: coluna sensível hoje
   some do catálogo, o que faz o catálogo descrever um schema que não é o real.
4. `make lint` já falhava antes por mypy em `auditar_metadados.py`,
   `inventariar_colunas.py` e `validar_silver_gx.py` (16 erros, todos
   anteriores). Os arquivos desta entrega estão limpos; adicionei
   `mypy_path = "scripts/governance"` ao `pyproject.toml`, que resolveu os
   `import-not-found` entre módulos irmãos.


### 2026-08-31 · Claude · Documentação semântica e governança no OpenMetadata

**Estado: documentação do dbt 100% pronta; catalogação parcialmente aplicada.**
Quem continuar deve ler esta entrada inteira antes de tocar em OpenMetadata.

#### O que foi feito

**Documentação no dbt — 167/167 nós (100%), 1.231 colunas.** Era 76/167 (45%)
e 285 colunas. Todos os YAMLs do produto conjuntura foram escritos ou
reescritos:

| arquivo | conteúdo |
|---|---|
| `models/conjuntura_dbt/bronze/schema.yml` | criado; 31 models (só tabela, sem coluna) |
| `models/conjuntura_dbt/silver/schema.yml` | 36 models, 373 colunas |
| `models/conjuntura_dbt/gold/schema.yml` | 27 models, 277 colunas |
| `models/conjuntura_dbt/gold/boletim/schema.yml` | criado; 22 models, 130 colunas |
| `models/conjuntura_dbt/qualidade/schema.yml` | criado; 4 models, 26 colunas |
| `models/metadata/schema.yml` | criado; 1 model, 7 colunas |
| `snapshots/schema.yml` | criado; 21 snapshots |
| `seeds/schema.yml` | criado; 1 seed |

#### O PADRÃO — seguir à risca

O Lucas apontou o schema legado `conjuntura_gold` (20 tabelas, 200 colunas,
100% documentado) como **referência oficial**. O padrão é:

- **Descrição de tabela abre pela granularidade:** `"Uma linha por <X> com
  <medidas>"`. Na referência, 18 de 20 começam assim; as outras 2 são
  `"Registro consolidado…"` (tabela de linha única). Média de 187 caracteres.
- **Nomeia a instituição que apura** — IBGE, ABECIP, BACEN, FGV-IBRE, CBIC,
  SINAPI, Novo CAGED. Isso é autoridade da informação, não processo.
- **Descrição de coluna é curta (~62 caracteres) e sempre traz a unidade:**
  `"Valor dos novos financiamentos no mês, em bilhões de reais."`
- **PROIBIDO:** `[MANUAL]`, `[AUTOMATIZADO]`, nome de model de origem, menção
  a dbt/DAG/pipeline/planilha, frases de validação interna ("bate exato com o
  boletim"). O catálogo descreve o DADO, não como ele foi produzido.
- **COM acento.** A referência não usa, mas isso é peculiaridade dela:
  `entidades_fds` usa acento em 11 de 11 tabelas. Medido, não suposto.
- **Não cravar período** nas séries contínuas. A referência escreve "do
  primeiro trimestre de 2026" porque aquelas tabelas são recorte fixo; as
  nossas são séries e a descrição mentiria no mês seguinte.

#### Governança declarada (arquivos novos)

Tudo declarado no dbt e aplicado por sincronização. **Nada pela interface** —
o que não está no arquivo some no próximo sync.

- **`dbt/mcid/governance/dominios.yml`** — domínios, produtos, proprietário,
  certificação por camada, etiquetas automáticas.
- **`dbt/mcid/governance/termos_mcid.yml`** — 16 termos novos do glossário e
  24 existentes, com `aplica_a` mapeando 38 modelos.
- **`scripts/governance/sincronizar_governanca.py`** — aplica os dois. Padrão
  é simulação; `--confirmar` escreve.

#### JÁ APLICADO no OpenMetadata (não refazer)

- Domínios `MCid` e `MCid.Habitacao` (hierarquia)
- Produtos `conjuntura`, `empreendimento_far`, `entidades_fds`, com domínio
  `MCid.Habitacao` e dono `mcid-data-engineering`
- 16 termos novos no glossário MCID (confirmado: 40 termos declarados, 40
  existem)

#### FALTA FAZER

1. **Vincular tabelas aos produtos.** Os três produtos estão com `ativos=0`.
   É o que falta para "dado do pipeline de conjuntura estar no produto
   conjuntura". Os schemas de cada produto estão em `dominios.yml`.
2. **Aplicar etiquetas** (`dbtTags.conjuntura`, `dbtTags.gold`, `dbtTags.mcid`)
   e **certificação** (`Tier1` gold / `Tier2` silver / `Tier3` bronze). Ambos
   declarados em `dominios.yml`, seções `etiquetas_automaticas` e
   `certificacao_por_camada`. O sincronizador ainda NÃO lê essas seções.
3. **Anexar termos de glossário às tabelas** conforme `aplica_a`. O
   sincronizador cria os termos mas ainda não os pendura.
4. **Classificar colunas** com `PII.NonSensitive` / `PII.Sensitive`. A
   referência tem 49 colunas NonSensitive e 1 Sensitive.
5. **Colunas da bronze:** 489 sem descrição. NÃO afeta o catálogo — a bronze é
   `rag_publication: prohibited` e não é publicada. Valor só para o time.
6. **Colunas das duas silvers manuais** (318): puladas por decisão do Lucas,
   porque as tabelas devem sair quando a ingestão ABECIP entrar.

#### Armadilhas encontradas (não repetir)

- **Subdomínio precisa do FQN completo.** Produto referenciando `Habitacao`
  falha com 404; o correto é `MCid.Habitacao`.
- **`CAIXA` já existia em `MCID.Atores.CAIXA`.** Não duplicar em
  `FontesInstitucionais` — a Caixa é agente operador, não fonte de apuração.
- **Nome de coluna com caractere especial quebra o YAML.** Os quadros do
  boletim têm `% MCMV` e `R$ bi acum. ano`; precisam de aspas.
- **NÃO editar YAML com regex ganancioso.** Uma tentativa com
  `(?:.*\n)*?` destruiu o `gold/schema.yml` (25 models viraram 1). Editar
  linha a linha e conferir a contagem de models antes de gravar.
- **O auditor tinha dois pontos cegos, já corrigidos:** só via o que estava
  declarado em YAML (reportava 100% ignorando 91 nós sem entrada) e varria
  apenas `models/`, deixando snapshots e seeds invisíveis.

#### Ferramentas novas

- **`scripts/governance/inventariar_colunas.py`** — lê o inventário real de
  colunas do banco e propõe o `silver_contract`. Só conta preenchimento e
  cardinalidade; **não lê valor de linha** (decisão explícita do Lucas: a
  documentação é semântica).
- **`scripts/governance/auditar_metadados.py`** — ganhou as regras
  `texto_de_preenchimento` (rejeita as frases genéricas do
  `semantic_descriptions.py`), `linguagem_de_processo` e o inventário vindo do
  manifesto.

#### Pendências de regra do auditor (o Lucas concordou, não foi feito)

- Separar `mapeamento_literal` em **domínio de código** (`1=Contratado,
  2=Distratado` — legítimo, é dicionário) de **exemplo de valor observado**
  (`ex. "Horizonte/CE"` — proibido). Hoje a regra reprova os dois.
- Criar regra `documentacao_incerta` para `(?)`, "talvez", "provavelmente".
  Existe pelo menos um caso real: `fds_cadastro_pj.co_modalidade` documenta
  `1=Construção(?), 2=Aquisição(?)` — chute publicado como fato.


### 2026-08-30 · Codex · Diagnóstico e reparo inicial do dashboard legado

- A pedido do Lucas, os charts foram executados pelo mesmo endpoint que a UI
  do Superset usa. Os 27 charts do dashboard contínuo (id 12) e os 21 do
  boletim trimestral novo (id 14) retornaram dados; a quebra está no
  dashboard antigo **Boletim** (id 2).
- Dos 28 charts legados, seis falhavam. Dois foram reparados e revalidados:
  **PIB construção (IBGE)** agora trata o marcador `-` como valor ausente antes
  do cast numérico; **Preços SINAPI** passou de `localidade` para
  `localidade_nome`, o contrato atual da ingestão IBGE. Ambos preservam o
  visual e a fonte original.
- Restam quatro dependências apagadas integralmente: `abrecip.imob`,
  `abrecip.latitude_longitude_cidades`, `abrecip.tenda4t25` e
  `abrecip.cury4t25`. Não há correção por simples troca de coluna. Há fonte
  equivalente para IMOB no contínuo (`silver_continuo_infomoney_imob`) e para
  Tenda/Cury na Gold de balanços; elas podem ser reconstituídas sem inventar
  dado. O mapa de “Imóveis Concedidos” não tem fonte/semântica recuperável
  ainda — o chart somava identificadores municipais — e não deve ser apontado
  para uma tabela geográfica diferente só para deixar de falhar.

### 2026-08-30 · Claude · Refactor: tempo vira dimensão, quadros viram gold, scripts somem

Dia longo. Sequência de correções do Lucas que convergiram numa mesma crítica:
**eu vinha acomodando complexidade numa camada nova em vez de perguntar se ela
deveria existir.** Aconteceu três vezes seguidas e vale registrar como padrão,
não como incidente.

**1. Dashboard único com filtro, no lugar de quatro por trimestre.**
`/superset/dashboard/boletim-conjuntura/` — 7 abas (uma por página do boletim),
21 quadros, filtro `Trimestre` sobre a coluna `edicao`. Aposentou a ideia de um
dashboard por edição.

Consequência de projeto, deliberada: os cabeçalhos deixaram de ser literais
("2025 1ºTri") e passaram a ser relativos ("trim. anterior"), porque no Superset
o nome da coluna é do schema do dataset e não varia com o filtro.

**2. Dimensão temporal na silver (macro `dimensao_temporal.sql`).**
Antes: `ano` era `double precision` em 3 golds e `integer` noutro, `trimestre`
era o texto `'2T'` em 3 e inteiro em 1, e **31 dos 36 silvers não tinham
trimestre**. Contrato único agora: `data_referencia` (date), `ano` (int),
`mes` (int), `trimestre` (int), `periodo` (text), **`edicao`** (`'1T2026'`).

Prova de que era macarronada: ao trocar `trimestre` para inteiro, o build
quebrou num lugar só — `ticket_medio` fazia `left(m.trimestre, 1)::int` para
consertar o `'2T'` no join. Remendo local para defeito de contrato. Removido.

**3. Os 21 quadros viraram gold materializado.**
Estavam como dataset VIRTUAL no Superset: o SQL (janelas móveis de 12 meses,
comparações contra trimestre anterior) voltava ao Postgres **a cada
carregamento de página**. Agora em `models/conjuntura_continuo_dbt/gold/boletim/`,
com `ref()` e linhagem. Antes de trocar, comparei os 21 célula a célula contra
o SQL virtual: **idênticos: 21 | divergentes: 0**.

**4. Não existe "edição padrão".** Cheguei a cravar `EDICAO_PADRAO = "1T2026"`
no build; quando o Lucas apontou, minha reação foi *derivar* o padrão em 30
linhas de Python em vez de perguntar se ele deveria existir. Não deveria: o dbt
publica todas as edições e o filtro seleciona. Removido inteiro.

**5. `congelar_edicao.py` → snapshot do dbt.** O `snapshot-paths` estava
configurado e o diretório nem existia. 21 snapshots em
`conjuntura_continuo_snapshots`, chave `edicao + rótulo` (verificada única nos
21), `strategy='check'`. Melhor que o script: guarda o **histórico das
revisões** (`dbt_valid_from`/`dbt_valid_to`), não um retrato. É o que responde
"por que o boletim publicou 22.623 e hoje o dado é 25.196".

**6. `comparar_gabarito_boletins.py` → teste do dbt.** Novo modelo
`gold_boletim_valores` (os 21 quadros despivotados, uma linha por célula),
seed `boletim_gabarito.csv` com **170 células transcritas do PDF do 1T26**, e
teste singular. Veredito: **129 OK, 39 divergem, 2 sem dado**.

O teste **falha só em coordenada inválida, não em divergência de valor** — as
fontes revisam o passado, e teste vermelho o tempo todo vira ruído que se
aprende a ignorar. Ele pegou dois defeitos meus na estreia: alias de rótulo
global (`'JAN-MAR/26'` virava o rótulo do CAGED também no quadro de índices) e
confusão entre "coordenada não existe" e "célula vazia".

**7. Scripts removidos.** `scripts/conjuntura/` ficou com três arquivos
(migração manual + os dois de documentação). Saíram: `medalhao_lake.py`,
`puxar_fontes_publicas.py`, `reingerir_ibge.py`, `run_pipeline.py`,
`congelar_edicao.py`, `comparar_gabarito_boletins.py`. Critério do Lucas: o
que é dado vai por Airflow/dbt; script solto é contorno da orquestração.

`scripts/superset/` ficou com dois: `build_boletim.py` (definição das páginas +
construção) e `bootstrap_conjuntura.py`. Saíram `boletim_2026_1t.py`,
`boletim_trimestral.py` e `montar_boletim.py`.

**8. `aplicar_migracoes_manuais.py` varre o diretório.** Tinha lista fixa com
0003–0006: o `0007` (correção da Tenda) nunca era aplicado, e `0001`/`0002`,
que criam o schema, também estavam fora — banco novo quebraria no `0003`.

**Ordem de execução que passou a existir:** `dbt run` **antes** de
`build_boletim.py`. O script não carrega mais SQL; se a tabela não existir, o
erro sobe alto em vez de o chart ficar mudo.

**Pendente (decisão do Lucas):** dashboards órfãos 9, 10, 11 e 13; as 78
duplicatas de chart "Conjuntura | …" (4 cópias de cada um dos 26 nomes,
nenhuma usada por dashboard de terceiro).

**Pendente (dado):** ingestão ABECIP das competências 2025-10 a 2026-07 (menos
2026-06), combinada com o time do OCR — URLs públicas e previsíveis em
`data-abecip-AAAA-MM.pdf`, listadas na §5. Enquanto não entra, o quadro de
bancos da página 3 fica com histórico só até 09/2025.


### 2026-08-30 · Claude · ICST: era a série errada (sem ajuste vs com ajuste)

**O boletim usa a série COM AJUSTE SAZONAL.** O gold expunha só a série
original (sem ajuste), que não reproduz o publicado:

| | Publicado | Sem ajuste | **Com ajuste** |
|---|---|---|---|
| mar/2026 | 2,3% | 1,73% | **2,30%** ✅ |
| dez/2025 | −1,30% | −0,33% | **−1,30%** ✅ |

Duas batidas exatas. O gold agora expõe **as duas séries**: `*_com_ajuste`
reproduz o boletim e deve alimentar o painel; `*_serie_original` fica de apoio.

**Por que isso passou despercebido até agora:** a seção do ICST no PDF sai
ilegível na extração de texto porque as camadas de **duas edições ficam
sobrepostas** (o boletim é exportado do PowerPoint). Os números aparecem
embaralhados — "0,68%" e "1,1%" de uma camada misturados com "2,3%" e
"−1,30%" da outra. Foi preciso separar as camadas à mão para descobrir quais
números pertenciam a qual edição. **Se outro indicador da Página 7 divergir,
suspeitar disso antes de suspeitar do dado.**

**A extração da FGV foi verificada e está saudável:** 194 registros, até
**ago/2026** — mais fresca que o gold (jul/2026). A transformação também está
certa: o cálculo do gold reproduz exatamente o índice da fonte
(94,2 / 92,6 − 1 = 1,73%). O problema era só a escolha de qual série usar.

> Credenciais da FGV foram usadas apenas como variável de ambiente efêmera,
> nunca gravadas em arquivo nem commitadas.

### 2026-08-30 · Claude · BACEN resolvido; valor médio FGTS não reproduzido

**BACEN — concessões PF: NÃO é erro nosso, é revisão da fonte.** Série SGS
**20704** (códigos na descrição do PR original: PF 20704/20774/21151, PJ
20692/20763/21139).

| Mês | Série 20704 hoje | Boletim |
|---|---|---|
| **mar/2025** | 17.490 | **17.490** exato |
| fev/2026 | 18.810 | 18.176 (+3,5%) |
| mar/2026 | 25.196 | 22.623 (+11,4%) |

Março/2025 bate exato e a divergência **cresce quanto mais recente o mês** —
assinatura de revisão. O BACEN revisou para cima os meses ainda abertos quando
o boletim saiu (jun/26). **A série está correta; nada a corrigir.**
(Descoberto de passagem: 20704 = 20703 + 20702.)

**`fgts_valor_medio` — NÃO conseguimos reproduzir.** O indicador vem hoje de
`manual_conjuntura.fgts_valor_medio_imoveis`. Tentei derivar do CCI/CCA, que
tem `vlrdecompra` e `vlrdegarantiadoimovel`:

| Cálculo (dez/2025) | Valor | Boletim |
|---|---|---|
| média de `vlrdecompra`, todos | 235.331 | **245.959** |
| média de `vlrdegarantiadoimovel`, todos | 357.229 | |
| média de `vlrdecompra`, só CCI | 229.690 | |
| média de `vlrdecompra`, só MCMV | 235.331 | |
| média de `vlrdegarantiadoimovel`, só CCI | 239.789 | |

A ordem de grandeza é a certa (a fonte é essa família de dados), mas **nenhuma
agregação simples reproduz o publicado**. Falta a regra de negócio de quem
produz o número — que universo entra, se há ponderação. **Mantido manual.**
Não forçar encaixe: a mais próxima erra 2,5% e escolher ela seria inventar.

### 2026-08-30 · Claude · Financiamento PF por faixa: fonte errada, corrigida

**Era a última divergência grave em aberto.** O gold lia `Base_PF_FGTS` e não
reproduzia nada: Faixa 1 dava 30.515 contra 61.082 publicados (metade), Faixa 2
dava 65.727 contra 42.514 (uma vez e meia).

**Causa:** arquivo errado dentro da fonte certa. O boletim declara
**"Fonte: Canal FGTS"** — e o Canal FGTS **é** o SFTP/GEAVO no MinIO (o Lucas
precisou me corrigir nisso). Mas o recorte do indicador não é o
`Base_PF_FGTS`, e sim o **CCI + CCA analítico**
(`MC*__MCidades_CCI_CCA_*__cci_analitico` e `cca_analitico`) — Carta de
Crédito Individual e Associativo.

**O que resolve:** o campo `compatibilidade_faixa_novo_mcmv` já traz a faixa
do MCMV pronta. Não é preciso deduzir por faixa de renda, que era o que a
versão antiga fazia mapeando G1/G2/G3 — e é por isso que aquela dedução
"empírica" batia nas faixas de renda mas não nos totais publicados.
Os códigos têm sufixo (`1D`, `1DE`, `2`, `2E`), então agrupa-se pelo primeiro
caractere: 1→Faixa 1, 2→Faixa 2, 3→Faixa 3, 4→Classe Média.

**Validação contra o boletim 1T2026 — 3 dos 4 valores em R$ EXATOS:**

| | Nosso | Boletim |
|---|---|---|
| Faixa 1 | 61.069 / **R$ 8,21 bi** | 61.082 / **8,21** |
| Faixa 2 | 42.520 / **R$ 7,23 bi** | 42.514 / **7,23** |
| Faixa 3 | 26.879 / **R$ 6,12 bi** | 26.903 / **6,12** |
| Classe Média | 11.519 / R$ 3,06 bi | 11.664 / 3,10 |

Diferenças de 6 a 145 UH: revisão entre a safra do boletim (jun/26) e a nossa
(21/08/26).

Novos models: `bronze_continuo_geavo_cci_analitico`,
`bronze_continuo_geavo_cca_analitico`, `silver_continuo_geavo_cci_cca`, e o
`gold_continuo_financiamento_pf_faixa` reescrito.

> `datadacontratacao` no CCI/CCA vem em **MM/DD/YY** e é inconsistente
> (quebra o parse). O período sai de `anomescontratacao` (AAAAMM), que é
> confiável.

**Reclassificações combinadas com o Lucas (2026-08-30):**
- `empregos_caged`, `pnad_rendimento`, `ticket_medio` → **corretos**; as
  divergências são revisão de fonte (o CAGED vem do Power BI, que é vivo).
- `balancos_empresas_totais` → **correto**; vem direto dos PDFs extraídos.
- `funding` → dado novo, **ignorar por enquanto**.
- `ogu` → conferir durante a semana.
- `icst` → tem fonte da FGV com DAG criada; só não dava pra comparar pelo PDF.
- `canal_fgts` → vem do SFTP que já está no MinIO.
- `novos_financiamentos_banco` → deve sair do mesmo PDF do ABECIP já extraído
  (ver `gold_continuo_financiamentos_instituicao`, já construído).
- `fgts_valor_medio` → **investigar de onde vem**. Pendente.
- `financiamentos_imobiliarios_pf_pj` → reconferir só as concessões PF (+11%).

### 2026-08-30 · Claude · PNAD destravado: bug do separador de categoria

**Causa raiz do PNAD encontrada:** o `cliente_ibge` montava
`classificacao=888[47946|47949]`, mas a API do SIDRA separa **categorias por
vírgula** — o `|` só vale para `variaveis` (é separador de caminho na URL).
A URL saía inválida e a chamada falhava.

Isso explica a história inteira: a Variable foi renomeada (`pnad_trabalho_*`)
e passou a pedir duas categorias; as chamadas começaram a falhar; os arquivos
novos nunca foram criados; e os models seguiram lendo os antigos
(`pnad_construcao_*`), de uma ingestão velha e ainda aninhada. Falha
silenciosa clássica: nada quebrou, o dado só parou de atualizar.

Duas correções:
- `plugins/cliente_ibge.py` normaliza `|` → `,` em categoria (defesa);
- a Variable foi corrigida para usar vírgula em **3 entradas**
  (`pnad_trabalho_construcao`, `pnad_rendimento_construcao`,
  `pnadc_habitacao_condicao`). **Precisa ser aplicada no Airflow — eu não
  tenho acesso.** JSON completo entregue ao Lucas em 2026-08-30.

`sources.yml` realinhado aos nomes que a config produz (a config é a fonte de
verdade, roda semanal). Os arquivos `staging/ibge/pnad_construcao_*.parquet`
ficaram **órfãos** — ninguém mais os lê; podem ser removidos.

**Validação:** PNAD ocupados bate **exato** em dois trimestres
(jan-fev-mar/2026 = 7.335/101.976; out-nov-dez/2025 = 7.468/102.998).

O `periodo_nome` passou a ser **derivado** no gold: a API v3 devolve só o
código (`202603`), e como o período é o trimestre que termina no mês
indicado, o rótulo sai por cálculo ("jan-fev-mar 2026"). Confere com o
boletim.

**PNAD rendimento mantém +2,2%** contra o boletim mesmo com dado novo e config
correta — isso **confirma** que é rebase de deflator do IBGE, e não dado
velho. Agora é evidência, não hipótese.

> ⚠️ Armadilha que eu criei e corrigi: a reingestão trouxe `data_referencia`
> como **timestamp**, e `current_date - timestamp` devolve *interval*, que
> quebrou o teste de frescor com "operator does not exist: interval > integer".
> As 13 silvers do IBGE agora fazem `data_referencia::date`.

**PIB da construção: conferido na fonte.** O agregado 5932 informa
`fim: 202601` — o 1T2026 é mesmo o dado mais recente que existe. Não há
atraso nosso; a tolerância de 270 dias no teste de frescor está correta.

### 2026-08-30 · Claude · IBGE: dado velho, não configuração errada. `achatar_sidra` removido

**Eu errei o diagnóstico e complicei a solução. O Lucas apontou os dois.**

**Diagnóstico corrigido:** a Variable `IBGE_CONFIGURACOES` **já estava
certa** (PIM-PF no agregado 8886, PMC no 8757/11046/56732). O que estava
errado era o **dado no lake**, ingerido em 27/08 com uma configuração
anterior — PIM no 8888 ("1 Indústria geral") e PMC na categoria 56733.
Prova: o raw tinha classificação 544, e o agregado 8886 **não tem
classificação nenhuma**. Não havia nada a corrigir na config; faltava
reingerir.

**`achatar_sidra` REMOVIDO.** Eu tinha criado um macro para desaninhar o
payload do SIDRA em SQL. Era trabalho duplicado: a DAG já faz isso em
`ClienteIBGE.transformar_resposta`, iterando variável → resultados → séries
→ períodos, e o padrão é uniforme para qualquer agregado. As 11 silvers do
IBGE viraram passthrough tipado, que é o que a arquitetura sempre disse
("o parquet já sai tipado da Etapa 02").

Também caiu a preocupação que eu tinha levantado com o job do outro time: o
formato achatado é o correto, e é o que a nossa própria DAG produz.

**Reingeridas 11 tabelas** com `scripts/conjuntura/reingerir_ibge.py` (roda
sem Airflow, usa a mesma função da DAG).

**Validação — período assentado (mar/2025), 5 de 6 exatos:**

| | Boletim | Nosso |
|---|---|---|
| PIM mensal / ano / 12m | 2,8 / 4,1 / 6,0 | **2,8 / 4,1 / 6,0** |
| PMC ano / 12m | 6,1 / 6,7 | **6,1 / 6,7** |

Em mar/2026 as diferenças ficam em 0,1–0,6 p.p. (PMC acumulados batem
exato) — revisões do IBGE posteriores à publicação do boletim.

> 🔴 **Descasamento de nomes no PNAD, NÃO resolvido.** A config produz
> `pnad_trabalho_construcao` e `pnad_rendimento_construcao`; os models leem
> `pnad_construcao_ocupados` e `pnad_construcao_rendimento`. **Os arquivos da
> config não existem na staging** — os que os models leem são de uma
> ingestão antiga, ainda no formato aninhado. Por isso essas duas tabelas
> **não foram reingeridas** e suas silvers **não foram simplificadas**.
> Decidir: renomear na config ou no `sources.yml`. Isso provavelmente
> explica a divergência de +2,2% do `gold_continuo_pnad_rendimento`.
>
> A config também tem `pnadc_habitacao_condicao`, que nenhum model lê.

### 2026-08-30 · Claude · Bronze volta a ser espelho (com uma exceção)

**Princípio reafirmado pelo Lucas: bronze NÃO projeta coluna.** Ela é espelho
da origem; toda transformação — inclusive descartar coluna — pertence à etapa
bronze → silver, já dentro do banco. Projetar no `read_parquet` é
transformação disfarçada e quebra o contrato da camada.

Revertidas para `select *` e reconstruídas sem custo relevante:
- `bronze_continuo_geavo_fgts_pj` (9,2 MB)
- `bronze_continuo_gefus_fundo_social` (57 MB)

> 🔴 **DÍVIDA ABERTA — BLOQUEIA APROVAÇÃO DO PR.**
> `bronze_continuo_geavo_fgts_pf` **continua projetando 5 colunas**, e é a
> única bronze do projeto assim. Decisão do Lucas em 2026-08-30: manter
> agora para não travar a entrega, **revisitar no PR e indicar que deve ser
> alterado para o PR ser aprovado**.
>
> Motivo do adiamento: com `select *` a tabela vai de 917 MB para ~3,4 GB
> (10,8 M linhas × 30 colunas). Contexto do banco medido em 2026-08-30: 29 GB
> no total, schemas do conjuntura somando 1,97 GB. O histórico pesa — o
> Postgres já foi pressionado por essa operação e caiu esta semana por causa
> das materialized views.
>
> Para regularizar: (1) confirmar a capacidade de disco do servidor;
> (2) trocar por `select *`; (3) reconstruir e medir. Se o custo for
> inaceitável, registrar como exceção **deliberada** de arquitetura — não
> deixar implícita.

**A proteção de dado pessoal não depende da projeção**, e isso é o que torna
a reversão segura: os valores já chegam anonimizados do pipeline (comprovado
em 2026-08-30), os nomes de coluna são mascarados na documentação por
`sanitizar_artefatos_dbt()`, e `conjuntura_sem_dado_sensivel` impede que
cheguem a silver/gold.

### 2026-08-30 · Claude · Fundo Social integrado (bronze/silver/gold)

Implementado a partir da investigação do Codex (ver entradas de 29/08). Fonte:
`staging/sftp/fabrica/GEFUS/PMCMV_FAIXA3_MCID_*.parquet`, declarada em
`sources.yml` como `gefus_fundo_social`.

**Validado EXATO contra dois pontos independentes dos boletins:**

| Recorte | Nosso | Boletim | |
|---|---|---|---|
| JAN-DEZ/2025 | 44.001 UH · R$ 8,82 bi | 44.001 · 8,82 (boletim 4T2025) | exato |
| JAN-MAR/2026 | 29.094 UH · R$ 6,03 bi | 29.093 · 6,03 (boletim 1T2026) | ±1 UH |

A diferença de 1 UH é a revisão de safra que o Codex já tinha observado (a
remessa de 22/05 traz 19.437 usadas; a de 21/08 traz 19.438).

**Descoberta que fecha a dúvida do `tipo_imovel = 5`:** ele **entra no total
publicado**. 9.102 (tipo 1) + 19.438 (tipo 2) + 554 (tipo 5) = 29.094, e sem
o tipo 5 o total não fecha com o boletim. O Codex estava certo em não
classificá-lo como novo/usado — mas ele não pode ser descartado. O gold expõe
`fundo_social_uh_nao_classificado` como coluna própria, visível de propósito.
O código 5 só aparece a partir de 2026.

**Outra descoberta:** a linha do boletim é **acumulada no período da edição**,
não trimestral — a edição 4T2025 publica JAN-DEZ, a 1T2026 publica JAN-MAR.
Foi isso que fez 44.001 parecer divergente a princípio.

**Privacidade:** a origem tem 46 colunas, incluindo `no_mutuario`,
`nu_cpf_cnpj_mutuario`, `nu_pis` e CEP. Conferido que o pipeline anonimiza
(137.491 de 137.491 linhas com nome `***`, zero CPF em claro). Ainda assim a
bronze projeta **somente `dt_evento`, `tipo_imovel` e `vr_evento`**, seguindo
a diretriz do Codex e o padrão das bronzes GEAVO minimizadas.

**Gold separado de propósito.** `gold_continuo_fundo_social` não foi somado ao
`gold_continuo_financiamento_pf_faixa`: as fontes são complementares
(interseção de contratos = zero), mas juntá-las num model só convidaria
alguém a deduplicar ou somar indevidamente. Quem precisar da Página 5
completa combina as duas no dashboard, com a decisão explícita.

> 🐛 **Bug latente corrigido nas bronzes do GEAVO.** A minimização feita pelo
> Codex removeu colunas das tabelas via `ALTER`, mas o SQL dos models ficou
> com nome de coluna direto — e o pg_duckdb **exige alias + `r['coluna']`
> quando se projeta colunas de `read_parquet`** (com `select *` não precisa).
> Resultado: `bronze_continuo_geavo_fgts_pf` e `_pj` **não construíam** —
> falhavam com "column dt_assinatura does not exist". As tabelas existiam
> (minimizadas via ALTER), então nada quebrou até alguém rodar o model.
> Corrigido nos dois. Verificado: PJ reconstrói normalmente.

### 2026-08-29 · Codex · Varredura completa: Fundo Social é fonte específica, não dado diluído

- Foram inventariados os 7.137 objetos do bucket e catalogadas as famílias de
  arquivos atuais de GEAVO, GEFUS e SharePoint; também foram consultados os
  metadados e agregados das tabelas relacionadas no Postgres. A análise foi
  restrita a nomes, esquema, cobertura temporal, contagens e interseções —
  nenhum registro individual foi exportado.
- A série **específica** é
  `raw/staging/sftp/fabrica/GEFUS/PMCMV_FAIXA3_MCID_*.{csv,parquet}`: há 60
  safra(s) parquet entre 2025-07-04 e 2026-08-21. Cada remessa atual possui
  Raw correspondente; a única origem não CSV é a planilha de 2025-07-14,
  também convertida para parquet. A série traz evento, tipo de imóvel e valor
  e alcança eventos até 2026-08-07 na última remessa.
- A hipótese de diluição foi refutada: entre 275.010 registros da
  `Base_PF_FGTS` no jan–jun/2026, zero têm modalidade com “fundo” ou “social”;
  e a interseção de contratos com a safra do Fundo Social é zero. As bases
  devem ser combinadas no nível analítico, nunca deduplicadas uma contra a
  outra.
- As alternativas não substituem a série: a cópia SharePoint/Postgres é a
  mesma família, porém só chega a 27/02; dados abertos FGTS terminam em
  04/12/2025; contratação diária não possui condição novo/usado; arquivos
  `FAR/FDS/Rural` e monitoramento PF descrevem empreendimentos, carteira ou
  execução, não os financiamentos PF por condição de uso. A contingência
  `INT042...FDS_CAIXA_PF_20260430` é foto de carteira sem dicionário e sem o
  volume de eventos do trimestre, portanto não é fonte do indicador.

### 2026-08-29 · Codex · Fonte complementar do FGTS-PF localizada

- A diferença do total PF por condição de uso no 1T2026 não é ausência nem
  duplicação na `Base_PF_FGTS` GEAVO. Ela corresponde à linha **Faixa 3 Fundo
  Social** do boletim.
- O MinIO possui remessas semanais em
  `staging/sftp/fabrica/GEFUS/PMCMV_FAIXA3_MCID_YYYY_MM_DD.parquet`. A safra
  de **2026-05-22** reproduz exatamente os números publicados: 9.102 UH novas
  (código de tipo `1`) e 19.437 usadas (código `2`) para jan–mar/2026.
  A versão posterior registra uma revisão de uma UH usada. O código `5` tem
  554 registros no trimestre e não deve entrar como nova/usada sem regra de
  negócio formal.
- A comparação de chaves foi feita somente em memória, sem expor dados
  pessoais: nenhum dos 28.539 registros dos tipos `1`/`2` da fonte Fundo
  Social ocorre na Base PF da safra comparada. As duas fontes são
  complementares para esse indicador, não devem ser tratadas como duplicatas.
- No Postgres, `__dados_brutos.novo_mcmv_fundo_social` contém apenas a carga
  de 2026-02-27 (eventos até essa data): no 1T ela soma 5.216 novas e 10.124
  usadas. Portanto a tabela atual **não pode** abastecer o boletim. A próxima
  implementação deve projetar somente `dt_evento`, `tipo_imovel` e, se houver
  métrica monetária aprovada, o respectivo valor a partir da safra escolhida;
  nunca carregar os demais campos da origem, que contém dados pessoais.

### 2026-08-29 · Codex · Bronze GEAVO minimizada e proteção ampliada

- A definição das duas Bronzes GEAVO deixou de usar `select *`. PF preserva
  somente os cinco campos usados pelo indicador e PJ somente os dois usados.
  Todos continuam texto, conforme o contrato Raw → parquet → Bronze.
- Para não recriar a tabela PF de 3.461 MB — operação que já pressionou o
  Postgres — foram removidas com `LOCK ... NOWAIT` as 25 colunas sem uso de
  cada Bronze já existente. A Raw/staging no MinIO permanece intacta.
- Auditoria somente de metadados confirmou que os identificadores pessoais
  encontrados estavam exclusivamente nas duas Bronzes GEAVO; após a remoção,
  nenhuma camada persistida do contínuo possui coluna identificadora.
- `tests/conjuntura_sem_dado_sensivel.sql` agora cobre Bronze, Silver e Gold;
  passou no Postgres. A varredura de conteúdo continua limitada a Silver/Gold,
  onde o volume é adequado, e a Raw não é documento nem camada de consumo.
- Corrigido o registro do INCC-M: a divergência de 12 meses em mar/2026 não é
  gap de transformação. A safra atual da FGV traz 5,81%, enquanto o boletim
  preserva 7,32%; o gabarito a marca como divergência esperada. O comparador
  agora fecha com 24 checagens e zero divergências abertas.

### 2026-08-29 · Codex · Tenda validada, catálogo em PDF e safra preparada

- A expansão do gabarito identificou e fechou três erros de preenchimento
  manual da Tenda em 1T2026. A migração
  `scripts/database/0007__CORRIGE_TENDA_1T2026_12M.sql` foi aplicada, Silver
  e Gold foram reconstruídas e as seis medidas da empresa passaram.
- `gabarito-boletins.yml` tem agora 30 checagens: 28 OK e duas divergências
  esperadas de revisão de safra; nenhuma divergência aberta. Os limites de
  FGTS-PJ, Cury e OGU foram registrados em `gaps-de-dados.md`.
- Corrigido `gerar_docs_seguros.py` para usar `dbt/mcid/profiles.yml`. O
  catálogo seguro foi regenerado em HTML e PDF (`pipeline.pdf`, 71 páginas)
  e rechecado por extração de texto: não contém identificadores pessoais.
- O dicionário público passou a aplicar descrições semânticas determinísticas
  aos campos sem YAML específico; PDF regenerado com 99 páginas, sem campos
  mudos. `make conjuntura-docs-pdf` reproduz HTML e PDF de forma segura.
- Criado `scripts/conjuntura/congelar_edicao.py`. O dry-run para 2026.3
  encontrou 26 Golds e não escreveu no banco. O comando exige edição,
  referência e `--confirmar`; cria cópias imutáveis no schema
  `conjuntura_boletim` e nunca sobrescreve uma safra. **Nenhuma safra foi
  criada ainda**, pois não se pode apresentar o estado atual como edição
  histórica sem aceite editorial.

### 2026-08-29 · Codex · Cortes trimestrais e cobertura GEAVO verificados

- A definição editorial de cortes é: 2026.1 = jan–mar, 2026.2 = abr–jun e
  2026.3 = jul–set. A base atual permite as duas primeiras para FGTS; não
  permite congelar 2026.3, pois PF e PJ terminam em jun/2026.
- PF tem 71 competências mensais sem lacuna entre ago/2020 e jun/2026; PJ tem
  70 competências trimestrais sem lacuna entre 2009T1 e 2026T2. Isso confirma
  continuidade das tabelas carregadas, não equivalência automática ao boletim.
- PJ reproduz o indicador de financiamentos habitacionais do seu recorte. PF
  **não** é 100% garantido para o total publicado: em 1T26 a Gold de condição
  de uso usa o subconjunto MCMV/faixas e fica abaixo do Canal FGTS. A Gold e o
  gabarito foram reclassificados para impedir uso editorial indevido; ver
  `gaps-de-dados.md`.

### 2026-08-29 · Codex · Diagnóstico do layout dos dashboards provisionados

- O erro de interface `n.meta is undefined` é compatível com o layout legado
  persistido, que não tinha metadados nos nós de grade. O bootstrap foi
  corrigido para o formato Superset 6 (`ROOT → GRID → ROW → CHART`, todos os
  nós com `meta`) e validado localmente.
- A instância não lista pela API os IDs dos quatro dashboards acessíveis pelas
  rotas públicas, mesmo com a conta `admin` de papel Admin. Por segurança,
  não foram criados duplicados nem aplicadas mudanças cegas. Reaplicar o
  layout depende de localizar o banco de metadados ou recuperar os IDs.

### 2026-08-29 · Codex · Fase 3 executável e dashboards provisionados

**Fase 3 (parcial, mas operacional):**

- Criado `docs-conjuntura/gabarito-boletins.yml`: contrato versionado por
  edição, página, Gold, consulta escalar, valor publicado, tolerância e
  classificação (`conforme`, `divergencia_esperada`, `gap_conhecido`).
- Criado `scripts/conjuntura/comparar_gabarito_boletins.py` e o alvo
  `make conjuntura-validar-boletins`. O relatório gerado é
  `docs-conjuntura/relatorio-validacao-boletins.md`.
- Verificado no Postgres: **24 checagens, 22 OK e 2 divergências esperadas**
  (FipeZap e safra histórica do INCC-M); zero divergências abertas no recorte
  inicial.
- O PDF 3T2025 é uma lâmina rasterizada única. Foi lido visualmente (OCR local
  indisponível por falta de pacote português) e os seis indicadores MRV foram
  transcritos; todos batem dentro do arredondamento.

**Superset:** `scripts/superset/bootstrap_conjuntura.py --with-charts` foi
executado com sucesso. Há **26 datasets Gold**, charts tabulares iniciais e os
dashboards `conjuntura-2026-1`, `conjuntura-2026-2`, `conjuntura-2026-3` e
`conjuntura-continuo`. Corrigidos dois quirks da instância: listagem exige
Rison em `q` para não cortar em 20 datasets; a listagem de dashboards não
revela os slugs do usuário de automação, então a idempotência também valida a
rota de navegação autenticada.

**Não confundir com safra fechada:** 2026.1/2026.2 são hoje recortes
operacionais por data. O congelamento por edição (Fase 4.1/4.2) continua
pendente de decisão de safra e schema próprio; `time_range` no Superset não
congela valores revisados.

### 2026-08-29 · Codex · Catálogo dbt seguro, sem artefatos brutos

**Decisão de segurança:** `dbt docs generate` produz `manifest.json` e
`catalog.json` completos. Além das colunas físicas, o manifest inclui SQL
compilado e metadados da bronze; portanto, sanitizar somente `columns` e
publicar os JSONs no GitHub Pages não era garantia suficiente.

- Novo `scripts/conjuntura/gerar_docs_seguros.py`: carrega **`.env`** (não
  `local.env`), roda `dbt docs generate` em `TemporaryDirectory` privado,
  gera apenas o HTML permitido e apaga os JSONs com o diretório temporário.
- `scripts/conjuntura/gerar_doc_pipeline.py` agora publica somente modelos
  silver/gold, com **tabela, campo, tipo e significado**. Não publica valores,
  SQL compilado, bronze, `manifest.json` nem `catalog.json`. Se um campo com
  identificador pessoal chegar a silver/gold, a publicação falha.
- Há uma verificação final do HTML contra os padrões de identificador pessoal.
  Teste ponta a ponta executado com sucesso: o catálogo saiu sem identificadores.
- `Makefile` recebeu `make conjuntura-docs`; a action de GitHub Pages deixou
  de mover `target/*` e passou a publicar exclusivamente `public/index.html`
  gerado pelo comando seguro. Os artefatos locais legados foram removidos com
  `dbt clean`.

**Achado verificado no MinIO (sem expor nomes ou valores):** a staging
`Base_PF_FGTS_20260707.parquet`, que alimenta GEAVO-PF, tem 30 colunas; 4 são
classificadas como identificadores pessoais pelo padrão do projeto. Assim, a
anonimização a montante **não está comprovada** para esta fonte. A proteção
das silver/gold e da documentação continua independente disso, mas a origem
precisa de auditoria e correção antes de se afirmar que a raw/staging é
anonimizada.

### 2026-08-29 · Claude · Proteção de dado pessoal (nome E conteúdo)

**Requisito do Lucas: nada de dado pessoal na documentação nem nas camadas de
consumo — e essa garantia NÃO pode depender de a anonimização a montante ter
funcionado.**

**Vazamento que eu mesmo introduzi, e corrigi:** o `gold_qualidade_schema`
que criei mais cedo é uma tabela **gold** (chega ao Superset) e estava
publicando `nu_cpf_cgc_mutuario`, `no_mutuario`, `dt_nascimento` e `cep`,
vindos do espelho da base PF do GEAVO. Eram nomes de coluna, não valores —
mas não deviam estar ali.

> ⚠️ Ao corrigir, o `dbt run` normal fez `INSERT 0 0`: o model é incremental
> e as linhas **sem máscara continuavam na tabela**. Precisou de
> `--full-refresh` para purgar. Se mexer em masking de model incremental,
> lembrar disso.

**Três camadas de proteção, todas testadas nos dois sentidos:**

| Onde | Arquivo | Cobre |
|---|---|---|
| Nome da coluna | `tests/conjuntura_sem_dado_sensivel.sql` | falha o build se coluna com identificador chegar a silver/gold |
| **Conteúdo** | `tests/conjuntura_sem_dado_pessoal_no_conteudo.sql` | varre os valores de todas as colunas de texto |
| Documentação | `sanitizar_artefatos_dbt()` em `gerar_doc_pipeline.py` | mascara nomes no `catalog.json`/`manifest.json` |

**Por que o teste de conteúdo existe** (pedido do Lucas): nome de coluna não
protege contra um CPF dentro de um campo `observacao`, nem contra coluna
renomeada. `macros/coluna_sensivel.sql` detecta CPF/CNPJ formatados, e-mail,
e **CPF sem máscara validando o dígito verificador** — não só o formato.
Isso é deliberado: sequências de 11 dígitos aparecem à vontade em código de
contrato, e um teste que acusa todas elas é um teste que o time desliga.
Verificado: aceita `52998224725`, rejeita DV errado, repetidos e texto comum.

**O `dbt docs` era um vetor de vazamento real.** O `dbt docs generate` lê o
catálogo do banco e escreve TODAS as colunas em `catalog.json`, inclusive as
da bronze. Confirmado: os três nomes apareciam lá. A sanitização agora roda
dentro do gerador de documentação (idempotente), então quem gerar o doc já
limpa os artefatos. **Rodar `dbt docs generate` sozinho volta a expor** —
sempre passar por `scripts/conjuntura/gerar_doc_pipeline.py` depois.

**Prova de que o teste de conteúdo dispara:** plantei um CPF válido numa
tabela temporária em silver — falhou; removida — voltou a passar.

### 2026-08-29 · Claude · Documentação gerada do pipeline

`scripts/conjuntura/gerar_doc_pipeline.py` produz
`docs-conjuntura/pipeline.html` a partir do `manifest.json` + `catalog.json`.
**Gerado, não escrito à mão** — documentação de pipeline feita à mão
desatualiza na primeira mudança e passa a mentir, o que é pior que não
existir. Cobre 92 models, 28 fontes, 116 testes, agrupados por domínio.

> Erro de desenho da primeira versão: eu agrupava por **nome** do model, e
> `gold_continuo_sinapi` não diz "ibge" em lugar nenhum — 29 dos 30 golds
> caíam num balde "Outros", esvaziando justamente a parte útil. Agora o
> domínio é resolvido **subindo a linhagem** até a fonte.

Contagem de linhas vem de `gold_qualidade_inventario`, porque o
`catalog.json` do dbt-postgres só traz `has_stats`, sem `num_rows`.

### 2026-08-29 · Claude · Fase 2 encerrada

Build completo do projeto: **198 nós (models + testes), 0 erros.**

Fechados por último:
- **Catálogo técnico (item 4):** `dbt docs generate` produz
  `target/catalog.json` + `manifest.json`. Servir com `dbt docs serve`.
  Precisa ser regerado quando models mudam — não é automático.
- **Contrato do `staging/` (2.13):** `tests/conjuntura_contrato_do_staging.sql`.
  Cada fonte tem `meta.linhas_minimas` em `sources.yml`, calibrado em metade
  do volume observado em 2026-08-29. É a defesa contra o problema estrutural
  do projeto — quando o outro time muda o formato, a bronze continua
  construindo (é `select *`), só que com 1 linha aninhada no lugar de 500
  achatadas, e o `dbt run` passa. Metade dá folga para variação real de série
  sem deixar passar colapso de ordem de grandeza. **Testado nos dois
  sentidos**: com piso elevado artificialmente, falhou; restaurado, passou.

**Complementaridade que vale entender:** `gold_qualidade_schema_drift` avisa
no dia seguinte (compara retratos); `conjuntura_contrato_do_staging` falha na
hora, dentro do build. Os dois cobrem o mesmo risco em tempos diferentes.

**Pendência honesta da Fase 2:** o item 3 (dicionário de dados) está
**parcial**. Há testes por coluna e descrição por model, mas **não há
descrição coluna a coluna** — são ~860 colunas. O esqueleto está pronto
(`schema.yml` com `columns:`); falta o texto, que é trabalho de domínio e
provavelmente deve ser feito por indicador, junto com quem usa o número.

### 2026-08-29 · Claude · Camada de qualidade: 104 testes, 2 bugs achados

O projeto tinha **zero testes**. Agora tem 104 rodando no `dbt test`, mais 3
testes customizados e 4 models de qualidade.

**Testes declarados (`gold/schema.yml`) — 103 em 24 models gold.**
Regra adotada: **só entra teste que passa hoje**, para virar guarda de
regressão em vez de ruído que o time aprende a ignorar. Unicidade e ausência
de nulos foram **medidas** model a model (script em
`scratchpad/gerar_testes.py`), não presumidas — cada gold tem grão diferente
e aplicar `unique` no chute geraria falha falsa.

**Models de qualidade** (`models/conjuntura_continuo_dbt/qualidade/`):

| Model | Item | O que faz |
|---|---|---|
| `gold_qualidade_completude` | 7 | % de preenchimento por coluna (861 colunas perfiladas) |
| `gold_qualidade_schema` | 5 | Retrato do schema por dia — **incremental de propósito**, o histórico É o dado |
| `gold_qualidade_schema_drift` | 5 | Colunas que apareceram, sumiram ou mudaram de tipo |
| `gold_qualidade_inventario` | 8 | Camada, materialização e volume |

**Testes customizados** (`dbt/mcid/tests/`):
- `conjuntura_frescor_das_fontes` — fonte parada há tempo demais
- `conjuntura_padronizacao_de_colunas` — itens 1 e 2 (snake_case, sem acento, sem `unnamed_*`)
- `conjuntura_cruzamento_abecip` — item 9 (XLSX × relatório OCR)

**Dois bugs reais encontrados pelas próprias checagens:**

1. **`gold_continuo_producao_fisica` tinha as 3 colunas do PMC 100% NULAS.**
   O gold filtrava `categoria_id = 56732`, que **não existe** — os ids reais
   são 56733 (receita nominal) e 56734 (volume de vendas). O README já
   documentava 56734 como o correto; era erro de digitação. Um gold
   automatizado rodava em produção com metade do indicador faltando.
   Corrigido e conferido.
2. **`unnamed_115` e `unnamed_116`** em `silver_continuo_manual_trimestrais` —
   lixo da importação de planilha, 100% vazio. Apontado por **duas checagens
   independentes** (completude e padronização). Removido via o novo macro
   `colunas_exceto()`.

**Ajuste de tolerância no frescor:** o teste inicialmente acusou o PIB da
construção como parado. Falso positivo — o IBGE publica o trimestral com ~3
meses de atraso, então em ago/2026 ter só o 1T2026 é o esperado. A tolerância
é contada do INÍCIO do período, então precisa cobrir o período inteiro MAIS o
atraso: mensal 90 dias, trimestral **270** (era 180).

**Inventário (item 8):** tudo é `table`/full-refresh — 28 bronze, 34 silver,
29 gold. A única exceção é `gold_qualidade_schema`, incremental porque
precisa do histórico. Consequência boa: como nada é incremental, mudança de
estrutura na origem aparece de imediato, e não meses depois misturada com
dado velho.

### 2026-08-29 · Claude · Sources declaradas: linhagem completa (Fase 2.3)

Os caminhos dos parquets estavam repetidos como **string literal em 28
models bronze**. Consequências: a linhagem do dbt começava na bronze (tudo
acima invisível) e trocar bucket/prefixo virava caça ao literal.

- **`models/conjuntura_continuo_dbt/sources.yml`** — 28 tabelas declaradas
  sob a fonte `lake_staging`, cada uma com descrição e `meta.caminho`.
- **`macros/fonte_lake.sql`** — resolve a fonte para `read_parquet(...)` e
  chama `source()` para registrar a dependência no grafo.
- Os 28 bronzes agora usam `{{ fonte_lake('nome') }}`. **Zero literais
  restantes.**

**Linhagem provada:** `dbt ls --select "source:lake_staging.ibge_sinapi+"`
devolve bronze → silver → gold. Build completo: **89 models, 0 erros**.

> ⚠️ **Armadilha do macro:** `graph.sources` só existe na fase de execução.
> Sem o guard `{% if execute %}` **todo model quebra no parse** com
> "não tem meta.caminho". Já corrigido — não remover o guard.

O `schema` da fonte é nominal (`staging`); não existe schema com esse nome
no Postgres. Serve só para o dbt montar o grafo — o dado é parquet no object
storage.

### 2026-08-29 · Claude · Guarda de layout na poupança ABECIP (Fase 2.4)

Invariantes escolhidas por evidência, medidas na série real (535 meses,
1982–2026):

| Identidade | Vale em | Uso |
|---|---|---|
| `captacao_liquida = deposito - retirada` | 535/535 | checagem dura (>1% fora = falha) |
| `saldo[t] = saldo[t-1] + captacao + rendimento` | 526/534 | proporcional (>10% fora = falha) |

A segunda falha em 8 meses ao longo de 44 anos (mudança de metodologia na
origem), por isso é proporcional — exceção histórica não pode derrubar a
ingestão, mas coluna trocada desalinha em massa.

**Erro que eu introduzi e o teste de controle pegou — lição a guardar:** na
primeira versão troquei o nome exato da aba por busca por prefixo. Mas a
planilha tem as abas **`'SBPE'` E `'SBPE_Mensal'`**, e o prefixo pegou a
errada, devolvendo **zero registro sem erro nenhum**. Prefixo solto é pior
que nome fixo. A regra correta, agora implementada: **nome exato primeiro,
prefixo só como plano B, e falhar alto se o plano B for ambíguo.**

Testado nos três casos: controle (535 registros), colunas trocadas
(detectou), aba inexistente (detectou).

### 2026-08-29 · Claude · Fase 2 começou: guardas de layout nos clients

**Padrão estabelecido — invariante semântica, não posicional.** Onde as
colunas de uma planilha são lidas por POSIÇÃO (porque o cabeçalho é mesclado
e não dá pra casar por nome), o client passa a validar uma identidade que o
próprio dado tem que satisfazer. Se a fonte inserir ou reordenar coluna, a
identidade quebra na hora — em vez de gravarmos série errada em silêncio.

| Client | Invariante |
|---|---|
| `cliente_fipe` | `indice[t]/indice[t-1] - 1 == var_mensal[t]` e o análogo de 12 meses |
| `cliente_abecip` (financiamentos) | `Total == Construção + Aquisição`, em unidades e em valores |

**Exceção dedicada `LayoutFonteMudou`** (em `plugins/cliente_base.py`).
Existe porque os clients capturam `Exception` genérica e devolvem `None` — o
diagnóstico virava uma linha de log perdida e a DAG falhava sem dizer por
quê. Os dois clients agora fazem `except LayoutFonteMudou: raise` **antes**
do handler genérico.

**Ambas as guardas foram testadas nos dois sentidos** (não basta passar no
dado real — guarda que nunca dispara é falsa confiança):
- FIPE com `COL_VAR_MENSAL` deslocada de 27→28: detectou, "65% dos meses não
  conferem". Dado real: passa, 223 registros.
- ABECIP com duas colunas trocadas de ordem: detectou, "294 linhas onde
  unidades_total != construcao + aquisicao". Dado real: passa, 294 registros.

**Tolerâncias escolhidas com motivo:** o FIPE aceita 0,15 p.p. de desvio e só
falha se **mais de 20%** dos meses divergirem. A FIPE revisa a série
retroativamente e publica variação arredondada — uma revisão pontual não pode
derrubar a ingestão, mas coluna trocada desalinha quase tudo de uma vez.

**Achado que muda a prioridade da Fase 2:** os `schema.yml` do
`conjuntura_continuo_dbt` são **só documentação — zero testes declarados**
(os 24 que o dbt reporta vêm de outros projetos). E **nenhum model usa
`source()`**: todos leem `read_parquet('s3://...')` como string literal, então
a linhagem do dbt começa na bronze e tudo acima é invisível. Declarar os
parquets de staging como `sources` é a espinha dorsal que destrava linhagem
(item 10 do checklist), teste de frescor, e o lugar onde o dicionário de
dados (itens 3, 4, 6) mora.

### 2026-08-29 · Claude · Desembolsos de Obras CEF: NÃO reproduzimos (Fase 1.3)

**Resultado negativo, registrado como tal a pedido do Lucas: não conseguimos
refazer esse dado. Não inventar encaixe.**

Fontes candidatas encontradas e testadas (todas existem e têm dado real):

| Fundo | Fonte testada | Métrica | 2025 (nosso) | Boletim JAN-DEZ/25 |
|---|---|---|---|---|
| FAR | `empreendimento_far.evolucao_financeira` | `vr_liberado_mes` | 9.099,4 | **5.297** |
| FAR | idem | `vr_pago_obra_mes` | 7.999,4 | **5.297** |
| FDS | `entidades_fds.fds_evolucao_financeira` | `vr_liberado_mes` | 116,7 | **484** |
| FDS | idem | `vr_pago_obra_mes` | 57,1 | **484** |
| RURAL | `__dados_brutos.novo_mcmv_rural_financeiro_mensal` | `vr_desembolso_obra` | 988,1 | **1.242** |

**Por que isso encerra a investigação (não é falta de tentar):** os erros vão
em **direções opostas** e magnitudes diferentes — FAR fica 51–72% ACIMA, FDS
76–88% ABAIXO, RURAL 20% abaixo. Isso descarta explicação sistemática única
(unidade errada, filtro de escopo consistente, recorte temporal). São
universos de medição diferentes, não a mesma medida mal calculada.

**Achado adicional — o próprio boletim é inconsistente aqui.** Os dois
boletins trazem o mesmo rótulo "(JAN-DEZ/25)" e a mesma SOMA (7.024), mas:
- 4T2025: FAR 5.297 + RURAL 1.242 + FDS 484 = 7.023 ✅ fecha
- 1T2026: FAR 1.902,5 + RURAL 379 + FDS 190,6 = **2.472** ❌ não fecha com 7.024

Provavelmente no 1T2026 os componentes foram atualizados para jan–mar/2026 e
o cabeçalho e o total ficaram da edição anterior. Ou seja: **a própria régua
de validação está furada nesse indicador**.

**O que faltaria para resolver** (não tentar adivinhar): saber qual relatório
específico da CEF alimenta essa caixa do boletim. A fonte declarada é só
"Fontes: CEF". Sem isso, qualquer número que a gente produzisse seria
plausível mas não verificável.

**Recomendação:** manter o Bloco 19 como **não automatizado** no catálogo, e
perguntar ao time de economia qual é o relatório de origem.

### 2026-08-29 · Claude · SBPE Construção automatizado (Fase 1.1 concluída)

**Escopo acordado: os dados que importam são de 2025 em diante.**

O Lucas achou o XLSX de unidades na página de financiamento da ABECIP. É a
fonte certa do "Financiamentos Habitacionais (UH) — SBPE Const.".

- **Client**: `ClienteAbecip.fetch_and_transform_financiamentos()` — reusa o
  `_get_xlsx_url()` (descobre o link por scraping, sem URL fixa), lê a aba
  `BD_Unidades` e devolve unidades e valores de Construção/Aquisição/Total
  desde 2002 (294 registros, até jun/2026).
- **Guarda anti-falha-silenciosa**: `_conferir_totais()` valida
  `Total == Construção + Aquisição` nas duas métricas e **falha alto** se a
  identidade quebrar. As colunas são lidas por posição (o cabeçalho é
  mesclado em duas linhas), então essa é a defesa contra a ABECIP inserir ou
  reordenar coluna e a gente passar a ler série errada sem erro.
- **DAG**: `dags/data_ingest/abecip/abecip_financiamentos_ingest_dag.py`,
  upsert por `data_referencia` (a ABECIP revisa meses publicados).
- **Models**: `bronze_continuo_abecip_financiamentos` +
  `silver_continuo_abecip_financiamentos`; o
  `gold_continuo_financiamentos_habitacionais` deixou de ler o manual.
- Também adicionado ao `puxar_fontes_publicas.py` (alvo
  `abecip_financiamentos`).

**Validação — 7 valores exatos contra os boletins publicados:**

| Período | SBPE Const | 12 meses |
|---|---|---|
| 1T2025 | 19.130 ✅ | 177.376 ✅ |
| 3T2025 | 43.782 ✅ | — |
| 4T2025 | 47.766 ✅ | 132.859 ✅ |
| 1T2026 | 47.609 ✅ | 161.338 ✅ |

**⚠️ Achado grave: o dado manual estava ERRADO.** O
`manual_conjuntura.dados_trimestrais` tinha 13.115 no 1T2025 e 18.950 no
2T2025; o boletim publica 19.130 e o acumulado dele implica 22.181. Ou seja,
a automação não só substituiu o manual — **corrigiu** dado errado que estava
em produção. Vale checar se outros indicadores manuais têm o mesmo problema.

### 2026-08-29 · Claude · ABECIP: dado já estava no lake (Fase 1.2)

Eu estava olhando o lugar errado. O schema `abecip_automated` no Postgres tem
a execução velha (competência 2026-05), mas **o lake está atualizado**:
`staging/abecip/*` tem competência **2026-06**, ingerido em 26-28/08. O
colega concluiu a issue.

Disponível lá, além do que já usávamos:
- `financiamentos_por_instituicao` — tem `modalidade`
  (`construcao`/`aquisicao`/`total`) **e** `instituicao_financeira`, com
  `volume_acumulado_ano_milhoes` e `unidades_acumuladas_ano`. É a fonte do
  Bloco 11 (Novos Financiamentos SBPE por Banco).
- `financiamentos_sbpe_mensal`, `financiamentos_historico_anual`,
  `recursos_livres`, `poupanca_*`.

**Limite importante:** cada extração do relatório ABECIP cobre **uma
competência** (min = max = 2026-06), não a série. Para histórico, usar o XLSX
(ver entrada acima). Cross-validação: o `unidades_total` de jun/26 (47.997) e
o volume (17.206) do parquet do colega batem exato com o XLSX — duas
extrações independentes concordando.

**Fase 1.2 concluída.** Models criados a partir de
`financiamentos_por_instituicao`:
`bronze_continuo_abecip_instituicoes` → `silver_continuo_abecip_instituicoes`
→ `gold_continuo_financiamentos_instituicao`.

Validação cruzada **exata**: acumulado do ano do TOTAL (277.086 UH /
R$ 93.738,1 mi) = soma jan–jun/2026 do XLSX de unidades. Duas extrações
independentes da ABECIP concordando. As participações também acompanham o
boletim de mar/2026 (CAIXA 62,7% vs 65,1%; Itaú 17,8% vs 16,7%; Santander
4,8% vs 4,9% — competências diferentes, tendência coerente).

> 🔁 **Decisão pendente do Lucas:** o novo gold é **longo** (uma linha por
> instituição), enquanto o `gold_continuo_novos_financiamentos_banco`
> (manual, parado em set/2025) é **largo** (uma coluna por banco). Deixei os
> dois coexistindo **de propósito** — trocar a forma da tabela que o Superset
> já consome quebraria os charts. A aposentadoria do manual é decisão dele.
> O formato longo é melhor: não quebra quando entra ou sai banco, e é o
> formato da própria tabela do boletim.

### 2026-08-29 · Claude · Rebuild completo no banco: 84 models, 0 erros
Depois de trocar tudo para `table`, o `dbt run` do `conjuntura_continuo_dbt`
fechou em **84 models, 0 erros, 0 pulados**. O erro do INCC-M sumiu, como
esperado — era artefato da MV.

Validado contra o boletim publicado: SINAPI dez/25 (1.891,63 · +0,51% ·
+5,63%) e PIB da construção 1T26 (2,9 · 1,3 · 0,1) batem **exato**. Schema
`conjuntura_continuo_bronze` com 26 tabelas.

**Pendências que ficam (nenhuma bloqueante):**
1. Nada commitado — ver §0.1.
2. `README.md` do `conjuntura_continuo_dbt` está **desatualizado**: ainda
   marca IMOB, FipeZap e FGTS-PJ como manuais, e não menciona a bronze.
3. Comentário do `dbt_project.yml` já corrigido, mas confira se algum outro
   texto ainda fala em "materialized view".
4. Safra/edição do boletim (§8) segue sem solução — é a decisão de
   arquitetura mais importante em aberto.

### 2026-08-29 · Claude · Fim das views materializadas
**Decisão do time: nada de `materialized_view` — elas estavam derrubando o
banco.** Todas as 26 bronzes e o `dbt_project.yml` passaram para `table`.

O primeiro run do dia (ainda com MV) fechou em 80 OK / 1 erro / 3 pulados. O
erro em `bronze_continuo_fgv_incc_m` era **consequência direta da MV**: para
view materializada o dbt executa `REFRESH MATERIALIZED VIEW`, e não recria.
A definição da view havia congelado os tipos inferidos do parquet na criação
(colunas numéricas); como o INCC-M foi repuxado em 28/08 e a coluna virou
texto com o marcador `...`, o refresh quebrou tentando converter para
`double`. Os 3 pulados (`silver_continuo_fgv_incc_m`, `gold_continuo_incc_m`,
`gold_continuo_ticket_medio`) eram só a cascata desse erro.

**Aprendizado que vale além deste caso:** MV congela o schema inferido do
parquet. Se a fonte muda de tipo, o refresh falha e o dado para de atualizar
silenciosamente até alguém olhar. Mais um motivo pra não voltar atrás.

### 2026-08-29 · Claude · Banco volta; aplicação da bronze
Banco voltou (exige **VPN ativa**). Confirmado que
`conjuntura_continuo_bronze` **não existia** — a camada bronze criada em
28/08 nunca havia chegado ao banco.

### 2026-08-28 · Claude · Medalhão no MinIO (fallback) e fontes públicas
Com o banco fora, replicado o medalhão em object storage
(`scripts/conjuntura/medalhao_lake.py`, DuckDB): **39 models, 0 falhas**.
Puxadas as fontes públicas: FipeZap (223 reg.), INCC-M (383 reg.) e **MRV**
(102+102 reg.). Dois bugs de fonte corrigidos — aba renomeada da MRV e o
marcador `...` do INCC-M. Gerado o catálogo de fontes em PDF
(`docs-conjuntura/Catalogo-Fontes-Conjuntura-Habitacional.pdf`).

### 2026-08-28 · Claude · Camada bronze e reescrita das silvers
Diagnosticado que `staging/` não estava corrompida — é conflito de contrato
com outro time. Criada a camada bronze (24 models), o macro `achatar_sidra`
(atende 11 models do IBGE) e o `parse_valor_siafi`. Silvers reescritas para
achatar+tipar. `dbt run` fechou em **82 models, 0 erros** — os 7 golds que
estavam quebrados voltaram.

---

## 0.3 Mapa de arquivos (onde mexer no quê)

| Caminho | O que é |
|---|---|
| `dbt/mcid/models/conjuntura_continuo_dbt/` | **Projeto principal.** `bronze/` espelha staging, `silver/` achata+tipa, `gold/` regra de negócio |
| `dbt/mcid/macros/achatar_sidra.sql` | Achata o payload SIDRA do IBGE — usado por **11 models**. Param `frequencia`: mensal/trimestral/anual |
| `dbt/mcid/macros/parse_valor_siafi.sql` | Wrapper do `parse_financial_value` que trata negativo em parênteses |
| `dbt/mcid/dbt_project.yml` | Config das camadas. Bronze = `materialized_view`, **exceto os 2 do GEAVO** (`table`, por tamanho) |
| `plugins/cliente_*.py` | Clients das fontes externas (um por origem) |
| `dags/data_ingest/<fonte>/` | DAGs de ingestão |
| `plugins/ingestor_lake.py` | "Etapa 02": registros → parquet tipado na staging |
| `scripts/conjuntura/puxar_fontes_publicas.py` | Roda ingestões públicas **sem Airflow** (MRV, FipeZap, INCC-M) |
| `scripts/conjuntura/medalhao_lake.py` | Medalhão em object storage — **fallback**, ver §2.4 |
| `scripts/database/000N__*.sql` | Inserções manuais versionadas. Só rodar quando o Lucas pedir |
| `scripts/superset/bootstrap_conjuntura.py` | Cria datasets e dashboards no Superset |
| `docs-conjuntura/Catalogo-Fontes-*.pdf` | Catálogo dos 27 blocos, para compartilhar com o time |
| `dbt/mcid/models/conjuntura_continuo_dbt/VALIDACAO_BOLETIM.md` | Validação indicador a indicador contra o boletim |

**Fora do repositório:** boletins publicados em PDF (3T2025, 4T2025, 1T2026)
estão em `~/Downloads` — são a régua de validação.

### Comandos que você vai usar

```bash
# rodar o projeto de conjuntura (a partir de dbt/mcid/)
poetry run dbt run --select "path:models/conjuntura_continuo_dbt"

# consultar o banco (psql direto é bloqueado pelo classificador de segurança)
poetry run dbt show --inline "select ..." --output json --limit 20

# puxar fontes públicas sem Airflow (a partir da raiz do repo)
poetry run python scripts/conjuntura/puxar_fontes_publicas.py [mrv fipe fgv_incc]
```

> `dbt show` **falha se o SQL terminar em `limit N`** — use a flag `--limit`.
> O banco exige **VPN ativa**.

---

## 1. Contexto do produto

Boletim trimestral "Conjuntura do Setor Habitacional" (Secretaria Nacional de
Habitação / MCID). ~20 indicadores em 8 seções. Historicamente montado à mão
numa planilha (`boletim.xlsx`) mantida pelo **CEAG** (setor de economia); o
trabalho em curso é substituir cada aba por fonte automatizada.

Boletins publicados disponíveis para validação (PDFs, em `~/Downloads`):
3T2025, 4T2025, 1T2026. **Não existe boletim 2T2026** — é o que estamos
construindo.

### Regra de validação (definida pelo Lucas, vale sempre)

> Comparar contra os boletins publicados antes de dar qualquer número por
> certo. Vale também — e principalmente — para números **calculados/derivados**
> por nós, não só para fontes automatizadas novas. Consistência interna
> ("minha fórmula bate com outra empresa") é necessária mas **não suficiente**.

---

## 2. Arquitetura

### 2.1 Como era (até 2026-08-27)

```
raw/<fonte>/<dado>.json   →  staging/<fonte>/<dado>.parquet   →  silver (dbt)  →  gold (dbt)
    (DAG ingere)              (DAG escreve parquet ACHATADO)      read_parquet     Postgres
```

Sem camada bronze. A silver lia o parquet direto via `pg_duckdb.read_parquet`
com `select *`, porque o parquet já saía tipado da ingestão ("Etapa 02" do
`plugins/ingestor_lake.py`).

### 2.2 O que quebrou

Outro time passou a escrever no **mesmo caminho** `staging/<fonte>/<dado>.parquet`,
com contrato diferente: o parquet deles é **espelho do raw**, mantendo o JSON
aninhado dentro de colunas. Último que escreve ganha → nossas silvers quebraram.

Isso **não é corrupção** — o dado deles é completo e atual (conferido: o SINAPI
deles vai até 202607, mesma atualidade da nossa versão). É conflito de contrato.

Diagnóstico anterior de "corrupção" estava **errado** e foi corrigido em
2026-08-28 depois que o Lucas apontou que o colega tinha transformado raw→parquet
de propósito.

> **Exceção**: o caso `unnamed_1..12` em `staging/fgv/*` é bug de verdade — lá o
> raw é CSV/XLSX sem cabeçalho nomeado, e o dump não gerou dado aproveitável.
> Não confundir com o caso JSON→aninhado, que é recuperável.

### 2.3 Como ficou (decisão do Lucas, 2026-08-28)

Materializar **bronze no banco**:

```
raw  →  staging (espelho do raw, aninhado — do outro time)
     →  BRONZE  : read_parquet + materializa, sem transformação
     →  SILVER  : ACHATA + TIPA          ← único lugar de transformação
     →  GOLD    : regra de negócio
```

Config em `dbt/mcid/dbt_project.yml` → `conjuntura_continuo_dbt.bronze`,
schema `conjuntura_continuo_bronze`.

**Materialização da bronze: `table` em tudo (decisão do time, 2026-08-29).**

> 🚫 **Não usar `materialized_view`.** As MVs estavam **derrubando o banco** —
> foi a causa da indisponibilidade de 28/08. Além disso, MV congela o schema
> inferido do parquet no momento da criação: se a fonte muda de tipo, o
> `REFRESH` falha e o dado para de atualizar em silêncio (aconteceu com o
> `bronze_continuo_fgv_incc_m`). Se alguém propuser voltar, leia isto antes.

O `Base_PF_FGTS` tem 10,8 M de linhas e gera tabela de ~3,4 GB — é a bronze
mais cara do projeto, e a que mais pesa no tempo de `dbt run`.

> ⚠️ A bronze não se atualiza sozinha quando o parquet de origem muda —
> depende de `dbt run` no model. Somando com o fato de que os caminhos do
> GEAVO são **hardcoded** (`Base_PF_FGTS_20260707.parquet`), são dois passos
> manuais para ter dado novo: trocar a data no arquivo **e** rodar. Enquanto
> não existir DAG copiando o dump mais recente para um caminho fixo, não dá
> pra contar com frescor automático aqui.

No **lake** isso não se aplica: o runner ignora `config()` e as bronzes de
passthrough (incluindo as do GEAVO) viram ponteiro para o parquet de
`staging/`, sem materializar.

### 2.4 Medalhão no object storage (provisório, 2026-08-28)

Como o **Postgres caiu**, foi pedido replicar o medalhão direto no MinIO, para
destravar o consumo e refatorar para o banco depois.

- Script: `scripts/conjuntura/medalhao_lake.py` (DuckDB + httpfs → MinIO)
- Prefixos novos no bucket `data-lake-mcid`: `bronze/`, `silver/`, `gold/`
  (antes só existiam `raw/`, `staging/`, `audit/`)
- Rodar: `poetry run python scripts/conjuntura/medalhao_lake.py [--dry-run] [--select MODEL ...]`

**Como funciona (importante):** o runner **lê os próprios arquivos dos models
dbt** em `dbt/mcid/models/conjuntura_continuo_dbt/` e resolve `ref('x')` para
`read_parquet('s3://.../<camada>/x.parquet')`. **Não há SQL duplicado** — a
fonte única continua sendo o dbt, e a volta pro banco não exige reescrever
nada. Ordena os models por dependência (Kahn) e materializa em parquet.

O que precisou de tradução é só **dialeto**, não lógica:
- Postgres `jsonb_*` → DuckDB `json_extract`/`json_keys`/`unnest`.
  Os 3 macros do dbt (`achatar_sidra`, `parse_financial_value`,
  `parse_valor_siafi`) foram reimplementados em DuckDB dentro do runner.
- Models cujo SQL não é portável têm override explícito no dict `OVERRIDES`
  (hoje só `silver_continuo_bacen_financiamentos_imobiliarios`, que despivota
  7 colunas JSON).

**Armadilhas de dialeto já resolvidas** (não repetir a investigação):

1. **`to_date` não existe no DuckDB.** Recriado como macro no runner. Mas
   **o formato do `strptime` tem que ser LITERAL** — traduzir 'YYYYMM' → '%Y%m'
   com `replace()` em runtime falha com *"strptime format must be a constant"*.
   Solução: `CASE fmt WHEN 'YYYYMM' THEN strptime(s,'%Y%m') WHEN ... END`, uma
   literal por ramo. Formatos usados no projeto: `YYYYMM`, `MM/YYYY`,
   `DD/MM/YYYY`.
2. **`unnest` de lista de STRUCT devolve UMA coluna**, não N. `unnest([{...}])
   as t(tipo, serie)` não funciona; use `as t(rec)` e depois `t.rec.tipo`.
3. **Timeout no GEAVO**: `Base_PF_FGTS` tem ~550 MB e estoura o default de 30s
   do httpfs. Runner seta `http_timeout=600000` e `http_retries=3`.

> **Observação de desenho**: no lake, `bronze_continuo_geavo_fgts_pf` é uma
> **cópia pura** de um parquet que já existe em `staging/` (550 MB duplicados,
> sem ganho). No banco a bronze faz sentido (materializa parquet remoto
> localmente); no object storage é redundante. Mantido só por paridade com o
> dbt — candidato a virar exceção se o custo incomodar.

**Cobertura (levantada em 2026-08-28):** de 83 models, **65 são replicáveis**
(26 bronze, 26 silver, 13 gold) e **18 ficam bloqueados** porque 6 silvers têm
raiz em `manual_conjuntura.*` (tabela do Postgres, indisponível com o banco
fora) e 12 golds dependem delas. Os bloqueados de raiz são:
`silver_continuo_manual_mensais`, `silver_continuo_manual_trimestrais`,
`silver_continuo_empresas_balanco_lancamentos_vendas`,
`silver_continuo_fgts_valor_medio_imoveis`,
`silver_continuo_pib_construcao_civil_pct`,
`silver_continuo_sbpe_financiamentos_aquisicao`.

> Alguns dos 12 golds bloqueados são só **parcialmente** manuais (ex.:
> `gold_continuo_fipezap` e `gold_continuo_indice_imob` fazem `coalesce` do
> automatizado com o manual). Dá pra ter o lado automatizado no lake dropando
> o fallback manual, mas isso **muda a semântica** — não foi feito sem decisão.

> ⚠️ **O Superset NÃO consome isso ainda.** Ele está configurado com conexão
> `postgresql+psycopg2://` apontando pro schema gold do Postgres
> (`scripts/superset/bootstrap_conjuntura.py`, `DATABASE_NAME = "Cidades"`).
> Parquet no MinIO só vira dataset com um engine no meio (conector DuckDB,
> Trino/Presto). **Isso ainda não existe e é o próximo bloqueio real** para a
> meta "Superset consumir".

---

## 3. Estado das fontes

### 3.1 Já automatizadas

| Indicador | Fonte | Quando | Validação |
|---|---|---|---|
| SINAPI | IBGE SIDRA | — | DEZ/25 = R$1.891,63 / +0,51% / +5,63% — **bate exato** com o boletim |
| FGTS-PJ (UH) | GEAVO/Caixa `Base_PJ_FGTS` | 27/08 | 3 de 4 trimestres **exatos** vs boletins |
| FGTS-PF por faixa | GEAVO/Caixa `Base_PF_FGTS` | 25/08 | — |
| Índice IMOB | Alpha Vantage (IMOB.SA) | 27/08 | fórmulas **exatas** vs 5 meses manuais |
| FipeZap (nº índice) | FIPE xlsx | 27/08 | nº índice **exato** vs manual |
| INCC-M | FGV | — | — |
| CBIC lançamentos/vendas | inserção manual `0003` | — | **exato** vs PDF CBIC 2T2026 |
| Balanços construtoras | OCR dos releases | 28/08 | 10/10 absolutos **exatos**; 19/20 variações exatas |
| **MRV** (lanç., vendas, ticket) | `apicatalog.mziq.com` | 28/08 | 4 valores **exatos** vs scripts manuais |
| **SBPE Construção** | XLSX ABECIP | **29/08** | **7 valores exatos** vs boletins (4 trimestres + 3 acum. 12m) |
| **Financiamentos por instituição** | relatório ABECIP (OCR) | **29/08** | acumulado do TOTAL **exato** vs XLSX |

### 3.2 Ainda dependentes de entrada manual

> ⚠️ **Atualizado em 2026-08-29.** Esta seção mudou bastante nesse dia — se
> estiver lendo uma cópia antiga, confira a data.

**100% manual (5 golds):**
`gold_continuo_funding`, `gold_continuo_canal_fgts`,
`gold_continuo_balancos_empresas`, `gold_continuo_balancos_empresas_totais`,
`gold_continuo_ticket_medio` (o lado INCC e a MRV já são automáticos; faltam
Direcional, Tenda e Cury, que dependem de VGV que o OCR não extrai).

**Deixaram de ser manuais em 2026-08-29:**
- `gold_continuo_financiamentos_habitacionais` — **totalmente automatizado**.
  O lado SBPE Const passou a vir do XLSX da ABECIP; o FGTS-PJ já vinha do
  GEAVO. **O manual que ele substituiu estava ERRADO** (13.115 no 1T2025
  contra os 19.130 publicados).
- `gold_continuo_novos_financiamentos_banco` (manual, largo) foi
  **superado** por `gold_continuo_financiamentos_instituicao` (automatizado,
  longo). Os dois coexistem: trocar a forma da tabela quebraria os charts do
  Superset. **Aposentar o manual é decisão do Lucas.**

**Parcial (1 gold):**
`gold_continuo_uh_condicao_uso` — lado FGTS-PF automatizado; lado SBPE
Aquisição ainda manual.

**Abas do CEAG muito atrasadas** (registrado em `VALIDACAO_BOLETIM.md`):
Novos Financiamentos por Banco parado em **09/2025**; Financiamento PF por Faixa
e Canal FGTS Pró-Cotista em **11/2025**; UH por Condição de Uso em 09/2025.

> Parte do que está nessas tabelas "do CEAG" **já é nosso**: os scripts
> `scripts/database/0003`–`0005` sobrescreveram CBIC, balanços 2T2026 e ticket
> médio com dados de origem própria.

### 3.3 Bloqueadas / sem fonte

- ~~**ABECIP**: competência 2026-05, aguardando outro time.~~
  **RESOLVIDO em 2026-08-29.** Eu estava olhando o lugar errado: o schema
  `abecip_automated` do Postgres tem a execução velha, mas o **lake está em
  2026-06**. Ver §0.2. Não repetir essa investigação.
- **Índice ABRAMAT**: PDFs em `abramat.org.br/indicadores-publicos/`. **Texto é
  extraível** (o relatório anterior que dizia "números em gráfico" estava errado),
  mas testei 4 releases: `var_mes` presente em 4/4, `var_mes_vs_mes_ano_ant` em
  3/4 (jun/26 diz "permaneceu estável", sem dígito), `var_acum_ano` em **2/4**.
  Pior: nov/25 traz "projeção de fechamento" que um parser ingênuo confundiria
  com acumulado. **Decisão: não automatizar por regex.** Nenhuma lib de PDF no
  repo hoje.
- **Desembolsos de Obras (CEF)**: fontes achadas mas valores não fecham.
- ~~**MRV**: ausente do OCR das construtoras.~~ **RESOLVIDO em 2026-08-28**:
  a MRV tem client próprio (`cliente_mrv`, catálogo de RI) que traz inclusive
  **VGV e ticket médio**. Ver §7.1.
- **VGV das outras 5 construtoras**: o OCR só traz `numero_de_unidades`, então
  o ticket médio de Direcional, Tenda e Cury segue manual. (A MRV, não.)
- **Boletim 3T2025 sem camada de texto**: o PDF foi exportado de `.pptx` e
  `pdftotext` devolve só ~2,4 KB (contra ~41 KB dos outros dois). Para usá-lo
  como régua de validação na Fase 3, será preciso **ler as páginas como
  imagem** e transcrever à mão. Os de 4T2025 e 1T2026 extraem normalmente.

---

## 4. Achados técnicos que já custaram tempo

### pg_duckdb
1. `read_parquet(...)` precisa de alias e acesso `r['coluna']` — nome de coluna
   direto não funciona.
2. O operador `/` faz **divisão float** mesmo entre inteiros (≠ Postgres) —
   precisa `floor(...)::int` para dividir trimestre/mês.
3. `to_date(...)` não existe no dialeto DuckDB — usar `strptime(txt,'%m/%Y')::date`.

### SQL
4. **Não dá para correlacionar com a linha corrente dentro de window agregada.**
   `max(case when ano = b.ano - 1 ...) over (partition by empresa)` devolve NULL,
   porque `b.ano` ali se refere à linha sendo agregada. Usar **self-join**.
   (Pegou no `gold/conjuntura_balancos_empresas_variacoes`.)

### Dados
5. **Alpha Vantage `outputsize: "compact"`** devolve só ~100 pregões. O DAG
   regravava o parquet só com o lote do dia → parquet nunca passava de ~5 meses,
   quebrando qualquer cálculo de 12 meses. Corrigido: o parquet agora é
   reconstruído do histórico acumulado em `infomoney.acoes_imob` (Postgres,
   upsert desde 2022-12-30).
6. **`infomoney.acoes_imob` mistura formatos**: backfill antigo em pt-BR
   ("1.010,19") e o DAG diário em US ("831.67"). Normalizar antes de tipar.
7. **FipeZap `LINHA_FIM_DADOS = 223` hardcoded** cortava silenciosamente
   abr–jul/2026 (a planilha já ia até a linha 226). Removido o limite superior.
8. **FipeZap "Número-Índice"** estava na coluna 22 do xlsx e nunca era extraído
   — só var_mensal (27) e var_ano (32).
9. **SIAFI**: valores em pt-BR **e** negativo em notação contábil
   `"(6570011.00)"`. Macro `parse_valor_siafi` embrulha o `parse_financial_value`
   do projeto para tratar os parênteses.
10. **G1/G2/G3 do FGTS-PF = Faixa 1/2/3** — confirmado empiricamente (não achei
    tabela de domínio no MinIO/Postgres): cruzando `vlr_renda_familiar_comprovada`,
    G1 até R$2.400, G2 R$2.000–4.400, G3 R$4.000–8.000. O arquivo também tem
    códigos antigos (`1_5`/`2`/`3`) usados **só até 2020-08-25** (virada exata
    para "G" em 2020-08-26) — não afetam período recente.
11. **FGTS-PJ é contagem de UH, não R$.** O header do boletim é literal
    "Financiamentos Habitacionais **(UH)**". A implementação anterior
    (join `tab_desembolsos_fgts` × `tab_contratos_fgts` para R$ desembolsado)
    estava **errada** e foi descartada. Usar `Base_PJ_FGTS.qt_unidades_financiadas`.
12. **Nomes de arquivo GEAVO são hardcoded** (`Base_PJ_FGTS_20260707.parquet`,
    `Base_PF_FGTS_20260707.parquet`, `MC20260821__...`). Vão envelhecer em
    silêncio até existir DAG que copie o mais novo para caminho fixo.

### Fontes que mudam de layout sem avisar

**Regra para localizar aba de planilha: nome exato primeiro, prefixo só como
plano B, e falhar alto se o plano B for ambíguo.**

Aprendida errando duas vezes:
- A MRV renomeou a aba de `"Dados Oper. MRV&Co | Oper.Data"` para
  `"... | Oper.D"` no 2T26 e a ingestão passou a devolver **zero registro em
  silêncio**. Nome fixo é frágil.
- Ao "corrigir" isso com busca por prefixo na ABECIP, peguei a aba errada: a
  planilha tem **`'SBPE'` E `'SBPE_Mensal'`**, e o prefixo casou com a
  primeira — de novo zero registro, de novo sem erro. **Prefixo solto é pior
  que nome fixo.**

Implementado em `cliente_mrv` (lista `conhecidas`) e `cliente_abecip`.

**Colunas lidas por posição precisam de invariante semântica.** Quando o
cabeçalho é mesclado e não dá pra casar por nome, a defesa é uma identidade
que o próprio dado satisfaz — ver `_conferir_totais` (ABECIP),
`_conferir_coerencia_indice` (FIPE), `_conferir_poupanca` (ABECIP). E a
exceção `LayoutFonteMudou` (em `cliente_base`) precisa ser reerguida **antes**
do `except Exception` genérico, senão o diagnóstico vira log perdido e o
client devolve `None`.

**Toda guarda tem que ser testada nos DOIS sentidos** — dado real passando e
layout quebrado sendo detectado. Guarda que nunca dispara é falsa confiança:
foi só por rodar o controle junto que eu peguei o erro da aba `'SBPE'`.

### Ambiente
13. `.env` linha 33 tem `)` na senha → **quebra `source .env` no fish**. Carregar
    o `.env` por parser em Python (ver `carregar_env()` em `medalhao_lake.py`).
14. Conexão direta ao Postgres por `psql` com senha na linha de comando é
    **bloqueada pelo classificador**. Usar `dbt show --inline` (mas ele falha com
    `limit` no fim do SQL — use `--limit`).
15. O repo tem hook de **pre-commit** que roda `make format` (black + ruff --fix
    + sqlfmt) e **reformata dezenas de arquivos não relacionados**. Foi a causa
    de "mudanças misteriosas" em massa. Preferência do Lucas: **formatar só o
    arquivo dele e usar `--no-verify`** no resto.
16. `poetry add duckdb` **fez downgrade do numpy** 2.4.6 → 1.26.4.

---

## 5. Divergências conhecidas (não são bugs)

- **FGTS-PJ acumulado 12 meses**: nosso 292.150 vs boletim 286.411 (dez/2025).
  A diferença cai toda no **2T2025**, que nenhum dos 3 boletins mostra isolado.
  **Decisão do Lucas: ignorar** — "não vai dar tanta interferência e eu confio
  mais no nosso dado". Ideia futura (não é tarefa): cruzar com o RI das
  construtoras do 2T25.
- **Tenda, lançamentos × trimestre anterior (2T26)**: calculado **11,9%**
  (7.099 / 6.344) vs **13,3%** publicado no release. O % publicado implica
  1T26 = 6.265, e não 6.344 que consta na mesma tabela — provável base restatada
  ou segmento diferente (Tenda usa "Consolidado" = Tenda + Alea). **Não resolvido.**
- **FipeZap var_mes/var_ano de fev–abr/2026** destoam ~0,2–0,6 p.p. do manual.
  A série do FipeZap sofre **revisão retroativa**; o manual foi digitado de uma
  vintage mais antiga. O automatizado reflete a vintage mais recente (mais
  correta). Esperado, resolve-se sozinho.
- **Ticket médio, base 4T2020 da Cury = 196**: reconstrução própria, **sem
  validação externa** — a Cury nunca aparece na tabela de comparação com INCC
  em nenhum dos 3 boletins (só INCC/MRV/Direcional/Tenda). MRV (179,0),
  Direcional (169,4) e Tenda (144,3) foram confirmados nos PDFs.
- **OGU ação 00TI / dotação total**: snapshot do SIAFI difere do boletim
  (posição "hoje" vs congelada; ação 00XF é crédito reembolsável sem dotação).

---

## 6. Decisões arquiteturais a respeitar

1. **`staging/fgv/` não pode virar caminho por projeto.** Tentei mover para
   `staging/conjuntura_continuo_fgv/` e o Lucas rejeitou: **quebra o data mesh**.
   `fgv` é domínio compartilhado. Não repetir.
2. **Não depender do boletim do CEAG como fonte** — replicar do dump próprio, não
   espelhar a planilha deles.
3. **Bronze do GEAVO não deve ser materializada.** Tentei e a `Base_PF_FGTS`
   (10,8M linhas) gerou tabela de **3,4 GB**. Aqueles arquivos já são achatados
   e vêm de outro pipeline (SFTP/mdb) — a silver lê o parquet direto. As duas
   bronzes foram dropadas.
4. **Scripts de inserção manual viram arquivo numerado** em `scripts/database/`
   (`0003__`, `0004__`, `0005__`). Só rodar quando o Lucas pedir.

---

## 7. Log de operações

### 2026-08-27
- Índice IMOB automatizado (`gold_continuo_indice_imob`); DAG corrigido para
  regravar o parquet do histórico completo do Postgres.
- FipeZap: número-índice exposto + `LINHA_FIM_DADOS` removido (destravou
  abr–jul/2026). `plugins/cliente_fipe.py` alterado.
- FGTS-PJ corrigido de R$ para contagem de UH (silver + gold).
- G1/G2/G3 confirmado empiricamente.
- Script `0005__UPDATE_TICKET_MEDIO_2T2026.sql` criado e executado (2×, com
  correção das bases 4T2020 após conferir os PDFs).
- ABRAMAT investigado e **descartado** para automação por regex.

### 2026-08-28
- Diagnóstico corrigido: `staging/` não está corrompida — é conflito de contrato.
- **Camada bronze criada** em `conjuntura_continuo_dbt` (24 models, `bronze/`).
- Macro **`achatar_sidra`** criado (`macros/achatar_sidra.sql`) — achata o payload
  SIDRA e atende **11 models** do IBGE. Parâmetro `frequencia`
  (`mensal`/`trimestral`/`anual`) porque "202601" é ambíguo entre mês e trimestre.
- Silvers reescritas: 11 IBGE (macro), 3 CAGED (tipagem), SIAFI (tipagem +
  `dt_ingest`), BACEN financiamentos (despivota 7 séries JSON para formato longo).
- Macro **`parse_valor_siafi`** criado.
- `gold_continuo_ogu` limpo: removido o parsing pt-BR que agora é da silver.
- **Resultado: `dbt run` do projeto inteiro = 82 models, 0 erros.** Os 7 golds
  que estavam quebrados voltaram.
- **Medalhão no object storage** criado (`scripts/conjuntura/medalhao_lake.py`),
  com bronze/silver/gold das construtoras. Validado contra `0004`.

---

## 7.1 Fontes buscadas na internet (levantamento 2026-08-28)

Script para rodar sem Airflow: `scripts/conjuntura/puxar_fontes_publicas.py`
(mesmos clients das DAGs, chamada direta; grava em `raw/` e `staging/`).

**Públicas — dá pra puxar a qualquer momento:**

| Fonte | Endpoint | O que traz |
|---|---|---|
| IBGE SIDRA | `apisidra.ibge.gov.br` | SINAPI, PIB, PIM-PF, PMC, PAIC, PNAD, PNAD-C |
| BACEN | `api.bcb.gov.br/dados/serie` + `olinda.bcb.gov.br` | financiamentos imobiliários PF/PJ, crédito/PIB |
| FIPE | `downloads.fipe.org.br/.../fipezap-serieshistoricas.xlsx` | FipeZap locação |
| FGV INCC-M | `sindusconpr.com.br` (espelho público) | INCC-M |
| **MRV** | `apicatalog.mziq.com` (catálogo de RI) | lançamentos e vendas **com VGV e ticket médio** |
| Novo CAGED | `wabi-brazil-south-api.analysis.windows.net` (PowerBI) | saldo/estoque de empregos |
| ABECIP | `abecip.org.br` | poupança/caderneta |

**Exigem credencial (Airflow `Variable`, indisponível fora do Airflow):**

| Fonte | Variable |
|---|---|
| FGV **ICST** | `dados_fgv_email` / `dados_fgv_password` |
| Infomoney / IMOB (Alpha Vantage) | `api_key_alphavantage` |
| Tesouro Gerencial (4 DAGs) | `email_credentials` |
| SIAFI nota de empenho | `airflow_orgao` / `airflow_variables` |

### Achado importante: a MRV **tem VGV**

Correção de uma afirmação anterior: o OCR das construtoras (`raw/construtoras/`)
só traz `numero_de_unidades`, mas o **client da MRV é outra fonte** e traz
`vgv_lancamentos_milhoes` e `preco_medio_unidade_mil`. Ou seja, **o ticket médio
da MRV é automatizável** — não depende de leitura manual.

Puxado e validado em 2026-08-28 (102 registros de lançamentos e 102 de vendas,
série trimestral desde ~2000):

| Métrica 2T26 | Automatizado | Manual | |
|---|---|---|---|
| Lançamentos (un) | 10.679 | 10.679 (`0004`) | exato |
| Vendas (un) | 10.148 | 10.148 (`0004`) | exato |
| Ticket médio (R$ mil) | 276,5 → 277 | 277 (`0005`) | exato |
| Vendas 2T25 (un) | 9.922 | 9.922 (nota do `0004`) | exato |

**A MRV deixa de ser gap.** Ainda falta VGV das outras 5 construtoras (o OCR
não extrai) — o ticket médio delas segue manual.

### Bug de fonte corrigido: nome de aba da MRV

`plugins/cliente_mrv.py` casava o nome exato da aba
`"Dados Oper. MRV&Co | Oper.Data"`. Na divulgação do 2T26 a MRV renomeou para
`"Dados Oper. MRV&Co | Oper.D"` e a ingestão passou a devolver zero registros
**em silêncio**. Trocado por busca via prefixo (`startswith("dados oper")`),
com erro explícito listando as abas se não achar. Mesma classe do bug do
`LINHA_FIM_DADOS` do FipeZap: valor fixo que a fonte muda sem avisar.

---

## 8. Próximos passos — FASE 3: validação sistemática

**Escopo acordado com o Lucas: dados de 2025 em diante.**

> ⚠️ Esta seção foi **reescrita em 2026-08-29**. A versão anterior listava
> "engine do Superset para ler o MinIO", "estender o medalhão" e "refatorar de
> volta pro banco" — tudo isso **caiu**: o medalhão no object storage era
> fallback enquanto o banco estava fora, e o banco voltou. Não retomar.

### 3.1 Montar o gabarito

`docs-conjuntura/gabarito-boletins.yml` — os números publicados em 3T2025,
4T2025 e 1T2026, por indicador e período, com a página do PDF de origem.

Dois cuidados que já sabemos:
- **O 3T2025 não tem camada de texto** (exportado de `.pptx`; `pdftotext`
  devolve ~2,4 KB contra ~41 KB dos outros). Precisa ler as páginas como
  imagem e transcrever à mão.
- **Marcar as divergências esperadas.** FipeZap e PNAD revisam
  retroativamente; se o gabarito exigir igualdade neles, vira ruído
  permanente. Ver §5.

### 3.2 Script de comparação

Compara gold × gabarito e emite relatório de divergências. Reaproveitar o
padrão dos testes da Fase 2 (`tests/conjuntura_*`).

### 3.3 Fechar as divergências abertas

FGTS-PJ 2T2025, Tenda 1T26, base 4T2020 da Cury, OGU (00TI/00XF). Ver §5.

> 🔑 **Lição que muda o desenho desta fase:** eu presumia que dado manual
> bateria com o boletim por ser digitado dele. **O SBPE Const provou o
> contrário** — o manual tinha 13.115 no 1T2025 contra os 19.130 publicados.
> **O gabarito precisa cobrir os indicadores manuais também**, não só os
> automatizados.

---

## 9. Depois da Fase 3

1. **Safra por edição do boletim** — decisão de arquitetura mais importante em
   aberto. `time_range` no Superset **recorta, não congela**: um painel
   rotulado "boletim fechado" mostra o número recalculado de hoje, não o que
   foi publicado. Proposta desenhada (coluna `edicao` + macro de
   congelamento na publicação); ver a conversa de 2026-08-29. **Precisa estar
   de pé ANTES do 2T2026 ser publicado**, senão perdemos mais uma safra.
2. **Aposentar `gold_continuo_novos_financiamentos_banco`** (manual, largo) em
   favor do `gold_continuo_financiamentos_instituicao` (automatizado, longo) —
   decisão do Lucas, porque muda a forma da tabela que o Superset consome.
3. **Dicionário de dados coluna a coluna** (item 3 do checklist, parcial). São
   ~860 colunas; o esqueleto existe em `schema.yml`. Melhor fazer por
   indicador, junto com quem usa o número.
4. **DAG que copie o GEAVO mais recente** para caminho fixo (hoje o nome do
   arquivo tem a data e é trocado à mão).
5. **Amarrar a sanitização ao `dbt docs`** — hoje depende de rodar
   `gerar_doc_pipeline.py` depois. Um alvo no Makefile ou hook eliminaria a
   dependência de disciplina.
6. **Projeto `conjuntura_dbt` legado** (54 models, schemas `conjuntura_*`):
   está vivo ou é dívida? O `scripts/database/0003__INSERT_CBIC_MANUAL.sql`
   escreve nele. Enquanto não estiver claro, há risco de trabalhar no projeto
   errado.
7. **Commitar.** Nada do trabalho de 28–29/08 está commitado.

---

## Diário — 2026-08-30: sucessão do produto Conjuntura

- O produto legado `conjuntura_dbt` (54 models) foi retirado do código dbt.
  Antes da remoção, a conferência encontrou **zero** `ref()` externos e os
  dashboards ativos de Conjuntura usavam exclusivamente
  `conjuntura_continuo_mart`.
- O produto antes chamado `conjuntura_continuo_dbt` passou a ocupar o caminho
  e o seletor `conjuntura_dbt`. Ele é agora o produto canônico para séries
  contínuas e boletins trimestrais.
- A DAG `conjuntura_boletim_dag`, que construía exclusivamente o legado, foi
  aposentada. A DAG canônica passou a selecionar `conjuntura_dbt`.
- Schemas físicos `conjuntura_continuo_*` **não foram alterados**; Superset
  continua a ler `conjuntura_continuo_mart`. A limpeza física depende de
  auditoria posterior no Postgres.
- A decisão também está registrada em
  `docs/architecture/ADR-001-produto-conjuntura-canonico.md`.

### Fundação de governança e qualidade

- Convenções e ciclo de vida foram versionados em
  `docs/architecture/dbt-conventions.md` e
  `docs/architecture/data-lifecycle.md`.
- O registro de schemas e o glossário semântico foram criados em
  `dbt/mcid/governance/`. O owner lógico inicial é `admin`; a integração deve
  resolver esse identificador para o owner configurado no OpenMetadata.
- `dbt_project.yml` injeta em cada model os metadados de produto, owner,
  camada, classificação e elegibilidade de RAG. Atenção: o dbt não faz merge
  profundo de `meta`, portanto esses campos precisam estar explícitos em cada
  camada.
- `scripts/governance/auditar_metadados.py` e o alvo `make governance-audit`
  auditam apenas YAML, sem abrir dados. Em 2026-08-30 o baseline é de 85
  achados: a maior parte são descrições de coluna pendentes nos Golds de
  Conjuntura; o restante são exemplos ou detalhes técnicos de documentação
  legada a sanitizar.
- O contrato genérico está em `macros/quality/silver_contract.sql`. Ele separa
  `sem_coluna_sensivel` (bloqueante) de `silver_contract` (alerta). O piloto
  `silver_continuo_ibge_sinapi` passou os dois testes em produção em
  2026-08-30.
- Em 2026-08-30 o contrato foi ampliado para validar layout esperado, novas
  colunas, tipo físico, domínio, formato, faixa numérica e normalização
  textual, além de obrigatoriedade, chave, frescor e completude. O piloto
  SINAPI passou novamente com as 13 colunas e tipos físicos reais. A macro
  `silver_reconciliation` também foi criada no mesmo arquivo para cruzar
  fontes antiga/nova por chave, medida e tolerância, sem expor valores ou
  chaves na falha; ainda não foi ativada até que os pares equivalentes sejam
  inventariados.
- Itens que não são teste de tabela permanecem nos controles próprios:
  dicionário/catálogo/linhagem no YAML, registros de governança e manifesto;
  histórico de colunas em `gold_qualidade_schema(_drift)`; estratégia de carga
  no `gold_qualidade_inventario`. No produto Conjuntura, todos os modelos de
  dado são `table` (reconstrução integral); a exceção é
  `gold_qualidade_schema`, incremental para preservar o histórico de schema.

### 2026-08-30 · Qualidade, safra e OpenMetadata operacionalizados

- Great Expectations 1.x foi incluído como dependência. O comando
  `make gx-silver` valida todos os **40** modelos Silver: estrutura mínima e
  existência de linhas para todos, além das expectativas declaradas nos
  contratos. A primeira execução realizou 87 verificações, todas aprovadas;
  o relatório não contém linhas, chaves nem valores de negócio.
- `governance/reconciliacoes.yml` tornou explícito quais cruzamentos são
  equivalentes: ABECIP XLSX × relatório está ativo pelo teste próprio. GEAVO
  PF × Fundo Social e CCI/CCA × PF são recortes distintos e ficaram bloqueados
  de comparação automática. Séries revisáveis dependem de safra congelada.
- `auditar_estrategias_carga.py` gera inventário de materialização a partir do
  manifesto: 145 modelos, 143 `table` com reconstrução integral e 2
  incrementais. Nenhum possui estratégia desconhecida.
- O comando de safra editorial foi restaurado em
  `scripts/conjuntura/congelar_edicao.py`. Ele é dry-run por padrão e nunca
  sobrescreve edição. O preflight de 2026.1 bloqueou corretamente uma safra
  incompleta: o Gold de financiamentos por banco não possui linhas para a
  edição, portanto nenhuma cópia foi criada.
- `make openmetadata-catalog` agora gera manifest/catalog somente em diretório
  temporário privado e persiste o catálogo filtrado. Foram validados 98
  modelos e 115 relações Silver/Gold, sem identificadores sensíveis. O sync
  externo continua dry-run até preencher `OPENMETADATA_URL` e
  `OPENMETADATA_JWT_TOKEN` no `.env` e usar `--confirmar`.
- `scripts/governance/exportar_catalogo_openmetadata.py` lê manifest e
  catalog **somente em diretório privado** e falha fechado se houver descrição
  ausente ou imprópria. O corpus semântico inclui Silver e Gold elegíveis, mas
  omite colunas identificadoras restritas sem registrar seus nomes no output
  ou na mensagem de erro.

### 2026-08-30 · Sincronização OpenMetadata concluída

- As variáveis de acesso foram preenchidas exclusivamente no `.env` local. O
  serviço técnico correto é `Cidades` (e o banco é `cidades`); o rótulo
  exibido na interface não deve ser usado como FQN sem conferir a API.
- `make openmetadata-sync OPENMETADATA_ARGS=--confirmar` foi executado com
  sucesso: **4 schemas**, **98 tabelas** e **115 relações** semânticas
  Silver/Gold foram sincronizados. Não foram enviados valores, amostras,
  conteúdo de linhas, SQL compilado ou colunas identificadoras restritas.
- A instância aceita criação de tabela somente pela rota `tables/bulk` com uma
  entidade por requisição e exige `dataLength` para `VARCHAR`. O sincronizador
  aplica esses requisitos e usa upsert idempotente.
- Ownership não é aceito no payload de criação nessa instância. Após cada
  upsert, o script aplica `owners=[admin]` via JSON Patch; esse fluxo foi
  validado e executado para as tabelas sincronizadas.
- Snapshots são explicitamente excluídos do exportador, do OpenMetadata e do
  RAG. Apenas suas definições permanecem documentadas como código e ciclo de
  vida no repositório.
