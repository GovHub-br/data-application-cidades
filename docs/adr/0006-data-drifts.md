# 0006. Data drifts: estrutural e semântico

- **Status:** Proposto
- **Data:** 2026-10-19
- **Revisão:** 2026-10-29 (fatos conferidos com o código; `padrao_arquivo` sobre o caminho espelhado do ADR-0005; drift estrutural numa task própria, `verify_structural_data_drift`; alertas por e-mail; drift semântico obrigatoriamente no dbt; nomes de rastreio em inglês, ADR-0007; catálogo de colunas; dbt-expectations e Elementary; questões em aberto resolvidas; chaves do contrato em inglês)
- **Relacionados:**
  - ADR-0001 (os dois portões), ADR-0002 (R2 a R4), ADR-0004 (tasks `verify_structural_data_drift` e `transform`),
  ADR-0005 (quarentena)
  - Deck *GovHub — Arquitetura Medalhão de Referência* seção 5
  - Issue #130, requisitos 5 (drift de schema) e 8 (carga completa ou incremental)
  - `dbt/mcid/models/docs/convencao-testes-qualidade.md`, `dbt/mcid/macros/data_quality/`,
  `dbt/mcid/macros/registrar_execucao_dbt.sql`

## Contexto

Fontes governamentais mudam sem avisar. O ADR-0001 pôs dois portões no caminho:
o drift estrutural, entre a raw e a staging, e o drift semântico, entre a bronze e
a prata. Este ADR define como cada um funciona.

O que existe hoje:

- Não há checagem antes da staging. Uma mudança de layout entra na bronze pelo
  `select *`.
- Na bronze, `sem_drift_de_colunas` (55 declarações em 4 arquivos de schema: `conjuntura`,
  `far`, `fds` e `rural`) e `colunas_esperadas` (16 declarações, no eixo histórico)
  comparam as colunas da tabela já materializada com uma
  lista. Ambos são `warn`, de propósito: "layout novo na origem não é motivo para
  derrubar a DAG". O resultado vai para `lake._dbt_log` pelo
  `registrar_execucao_dbt`, que registra todo resultado diferente de sucesso, e só no
  target do Postgres: o eixo histórico, que roda no `prod_duckdb` (ADR-0003), nunca é
  registrado.
- A convenção de testes (`convencao-testes-qualidade.md`) divide o trabalho: a
  bronze **detecta** (`warn`), a prata tem o **contrato** (`error`) e a ouro
  **reconcilia** (`error`). O projeto não usa pacotes dbt; todo teste é macro
  própria em `macros/data_quality/`.

Falta um lugar para o schema esperado de cada fonte, uma reação definida para cada
tipo de mudança, e a distinção entre "defeito da origem", que a prata trata, e
"a origem mudou", que precisa barrar.

## Decisão

### 1. O contrato da fonte fica no `sources.yml`

A entrada da fonte no `sources.yml` do dbt é o contrato. É um lugar só, já lido
pela macro `fonte_lake` e já publicado no OpenMetadata (ADR-0012).

```yaml
sources:
  - name: lake_staging
    tables:
      - name: fgv_incc_m
        meta:
          path: staging/fgv/incc_m/**/*.parquet
          load_type: full               # full | incremental
          max_row_variation: 0.5        # só para carga completa
          file_pattern: null            # SFTP/SharePoint: regex sobre o caminho espelhado e o nome, com o período
          owner: <área do ministério que responde pela base>
        columns:
          - name: mes
            meta: { format: date_br, term: dt_referencia }
          - name: indice
            meta:
              format: decimal_br
              term: incc_indice
              aliases: [numero_indice]  # outros nomes com que a coluna já chegou
```

Os nomes das colunas são os nomes já normalizados pela conversão (ADR-0002 R2).
As chaves do contrato ficam em inglês, como os demais nomes de rastreio
(ADR-0007).
Mudar o contrato é um PR: aceitar um layout novo é uma decisão explícita e revisada.

**Catálogo de colunas.** Duas chaves de cada coluna do contrato formam um grafo de
nomes:

- `term`: o termo do glossário que a coluna representa (`apf`, `contrato`,
  `dt_referencia`). Colunas de fontes diferentes com o mesmo `term` significam a
  mesma coisa.
- `aliases`: os outros nomes com que a mesma coluna já chegou da fonte, em
  layouts anteriores, inclusive erros de digitação da origem
  (`nu_contrato_emprendimento`).

Os nós do grafo são os termos, as colunas das fontes e as colunas dos modelos, que
a linhagem do dbt liga às da fonte. O catálogo é usado em três lugares: no drift
estrutural (§2); na prata, que monta o `coalesce` dos nomes de uma coluna pela
macro `coalesce_aliases` (ADR-0003); e no OpenMetadata, onde a página de cada
termo mostra todas as colunas ligadas a ele (ADR-0012).

### 2. Drift estrutural, na task `verify_structural_data_drift`, antes de converter

O drift estrutural é uma task própria da DAG de fonte, a `verify_structural_data_drift`,
entre a `extract` e a `transform` (ADR-0004). Como a `transform`, ela é escrita
uma vez em `plugins/lake_tasks.py` e usada por todas as DAGs de fonte. Ela lê o
contrato e compara com o cabeçalho normalizado de cada arquivo novo. Os dados só
são lidos na carga completa, para contar as linhas.

| Situação | Reação |
|---|---|
| Igual ao contrato | Converte |
| Coluna nova | Converte, registra `additive_drift` e alerta |
| Coluna do contrato ausente, e a coluna nova é um dos `aliases` dela | Converte, registra `known_rename` e informa no e-mail |
| Coluna do contrato ausente | Não converte: `quarantined`, alerta |
| Coluna renomeada sem `aliases` que a explique | Não converte: `quarantined`, alerta com a sugestão de renomeação |
| Arquivo sem cabeçalho, ou encoding e separador não detectados | Não converte: `quarantined` |
| Fonte ou arquivo sem contrato | Não converte: `uncataloged`, alerta |
| Carga completa com linhas fora de `max_row_variation` em relação à carga anterior | Não converte: `quarantined` (arquivo truncado, ou a fonte passou a mandar só o incremental) |

- Uma coluna "renomeada" aparece como uma ausente mais uma nova. Se a nova está
  nos `aliases` da ausente, a renomeação já é conhecida. Se não está, a
  `verify_structural_data_drift` sugere a renomeação no alerta pelo catálogo:
  primeiro as colunas de outras fontes ligadas ao mesmo `term`, depois a
  semelhança dos nomes. A decisão é humana: aceitar a renomeação é acrescentar o
  nome em `aliases`, por PR.
- A `verify_structural_data_drift` confere todas as chaves recebidas, registra o status de
  cada uma em `lake._staging_log` e passa adiante só as aprovadas: iguais ao
  contrato, com coluna nova ou com renomeação conhecida.
- Se algum arquivo foi barrado (`quarantined` ou `uncataloged`), ela manda o
  e-mail (§5) e termina em falha. A `transform` roda mesmo assim e converte só
  as aprovadas. O problema aparece em vermelho no Airflow sem bloquear os outros
  arquivos da mesma carga.
- O tipo de carga declarado (`load_type`) é conferido pela estrutura do arquivo.
  Na carga completa, o arquivo cobre a base inteira e o número de linhas fica
  perto do da carga anterior. Na incremental, traz só o período novo. Isso
  responde ao requisito 8 da issue #130, e a materialização segue essa
  classificação (ADR-0008).

### 3. Drift semântico, nos testes da bronze

O drift semântico é obrigatoriamente dbt: roda no `dbt build`, como testes da
bronze. O portão fica entre a bronze e a prata, que só existem no warehouse, e é o
`dbt build` que impede a prata de rodar quando um teste `error` falha. Nenhuma task
Python do Airflow faz checagem semântica. No Airflow, cada teste é uma task da DAG geral: o
Cosmos gera uma task de teste depois de cada modelo. Assim, os dois drifts são
interceptados dentro de uma DAG: o estrutural na DAG de fonte, o semântico na
DAG geral.

| Teste | De onde vem | Pega | Severidade |
|---|---|---|---|
| `formato_valido` (novo) | Macro própria sobre o `expect_column_values_to_match_regex` do dbt-expectations (§7) | Valor fora do `meta.format` da coluna: data que virou `aaaa-mm-dd`, separador decimal trocado, inteiro com texto | `error` |
| `accepted_values` e `dentro_do_dominio` (existentes) | dbt e macro própria | Código de domínio novo numa coluna de domínio fechado | `error` |
| `volume_anomalies` | Elementary (§7) | Carga com volume fora do que o histórico da tabela mostra como normal; pega carga parcial (ADR-0004) | `error` |
| `sem_perda_na_tipagem` (novo, na prata) | Macro própria | Valor preenchido na bronze que virou nulo depois do cast na prata | `error` (bloqueia a ouro) |
| `schema_changes_from_baseline` | Elementary (§7) | Colunas da bronze diferentes das declaradas no contrato; rede de segurança atrás do drift estrutural | `warn` |

Formatos iniciais de `meta.format`: `date_br`, `date_iso`, `decimal_br`,
`decimal_dot`, `integer`, `text`, `domain`. A macro `formato_valido` traduz
cada formato numa expressão regular e chama o teste do dbt-expectations. A
tolerância é o `mostly` dele: a fração de valores que precisa passar, por padrão
1, ou seja, nenhum valor inválido.

**Como isso concilia com a convenção atual.** A convenção de testes continua
valendo para o que ela trata: **defeito da origem**, como mojibake ou código
`"3550308.0"`. Isso é `warn` na bronze, porque a prata trata. **Drift** é outra
coisa: a origem passou a mandar diferente do contrato. Isso é `error`, porque a
tipagem da prata transformaria em nulo, sem aviso, o que não reconhece. A
convenção ganha essa distinção.

### 4. Anomalias de negócio na ouro: monitoramento, não drift

Variação de um indicador acima de um limite (por exemplo, UH entregues que caem
pela metade de um mês para o outro) é `warn` na ouro. O teste alerta e não
interrompe, como no deck. Os testes de anomalia do Elementary (`volume_anomalies`
e `column_anomalies`) aprendem a faixa normal com o histórico da tabela, em vez de
um limite fixo. Não é drift da fonte: o dado pode estar certo e o mundo
ter mudado.

### 5. Registro e alerta por e-mail

Todo alerta de drift é um e-mail.

| O que | Onde fica registrado | Quem manda o e-mail |
|---|---|---|
| Drift estrutural: arquivo em `quarantined` ou `uncataloged`, coluna nova | `lake._staging_log` | A `verify_structural_data_drift`, um e-mail por execução, com os arquivos, o motivo e a sugestão de renomeação, se houver |
| Drift semântico: teste `error` da bronze que falha | Tabelas do Elementary (`elementary_test_results`) | O `on_failure_callback` da DAG geral, quando a task do teste falha |
| Avisos: testes `warn` da bronze e anomalias da ouro (§4) | Tabelas do Elementary | A task `notify_dbt_warnings`, no fim da DAG geral: lê os avisos da execução e manda um e-mail só, se houver algum |

- O e-mail sai pelo provider SMTP do Airflow, que já está na imagem. Servidor e
  credencial ficam numa Connection (ADR-0013).
- O destinatário é um endereço de grupo do time, configurado no ambiente do
  Airflow e não no repositório, que é público.
- O e-mail traz nomes de arquivo, de coluna e de teste, nunca valores do dado.
- Os alertas da CLI do Elementary (`edr monitor`) não são usados: o e-mail sai
  pelo Airflow.

### 6. Quem decide o que é quebra

Os limites técnicos padrão são os deste ADR. Os limites de negócio de cada fonte
(variação de volume aceitável, domínios válidos, limiar de anomalia) são definidos
pelo `owner` declarado no contrato, que é quem conhece o dado, e registrados
no próprio contrato. Enquanto o ministério não indica quem responde por cada base, o
`owner` registra a área de origem conhecida, e valem os limites técnicos
padrão.

### 7. Pacotes de qualidade: dbt-expectations e Elementary

Os testes de qualidade usam dois pacotes do dbt, além das macros próprias:

| Pacote | O que traz | Uso |
|---|---|---|
| dbt-expectations | Os testes do Great Expectations como testes genéricos do dbt: expressão regular, faixas, conjuntos, unicidade composta | Base do `formato_valido` e de testes de faixa e de conjunto onde a regra for simples |
| Elementary | Testes de anomalia (`volume_anomalies`, `column_anomalies`) e de mudança de schema (`schema_changes_from_baseline`). Grava em tabelas do warehouse o resultado de cada teste e de cada execução de modelo | Volume da carga, anomalias da ouro, rede de segurança de schema; registro das execuções (ADR-0008) e fonte da `notify_dbt_warnings` |

- É o dbt-expectations, e não o Great Expectations em Python. O drift semântico é
  obrigatoriamente dbt (§3), e o Great Expectations rodaria fora do `dbt build`.
- As tabelas do Elementary ficam no schema técnico `elementary` (ADR-0007), só com o
  `cidades` (ADR-0011). Elas substituem o `registrar_execucao_dbt` e o
  `lake._dbt_log`.
- Continuam como macros próprias os testes sem equivalente nos pacotes:
  `sem_perda_na_tipagem`, `dentro_do_dominio`, `unique_combinacao` e o teste de dado
  sensível (ADR-0012).
- O Elementary roda um hook no fim de cada invocação do dbt, e o Cosmos invoca o
  dbt uma vez por task. O custo disso é medido na homologação antes de ligar o
  pacote em todos os domínios; o envio dos artefatos a cada invocação pode ser
  desligado pelas variáveis do pacote.
- Os dois pacotes são fixados e instalados como define o ADR-0014.

## Alternativas consideradas

**Drift estrutural dentro da `transform` (versão anterior deste ADR).** Uma task
a menos, mas uma falha de drift e uma falha de conversão apareceriam na mesma
task, e a conversão passaria a ter duas responsabilidades.

**Alerta num canal de chat (Teams, Slack).** Exigiria um webhook e mais uma
integração para manter. O e-mail sai pelo provider que já está na imagem.

**Contrato num arquivo próprio por fonte (`contratos/<fonte>.yml`).** Separaria o
contrato do dbt, mas duplicaria o `sources.yml`, que já tem caminho, colunas e
descrições e já vai para o OpenMetadata.

**Usar o schema da última staging como referência automática.** Dispensa escrever
o contrato, mas aceita qualquer mudança em silêncio depois da primeira execução.
Aceitar layout novo deixa de ser uma decisão.

**Contratos de modelo do dbt (`contract: enforced`).** Valem para os tipos da saída
de um modelo, não para o arquivo que chega. E a staging é toda texto.

**Great Expectations ou Soda em Python, fora do dbt.** Seriam mais um serviço para
operar e tirariam o drift semântico do `dbt build`, que é quem impede a prata de
rodar (§3). O dbt-expectations traz os mesmos testes como macros do dbt.

**Só macros próprias, sem pacotes (versão anterior deste ADR).** Evita dependência
externa, mas obriga a escrever e manter o que os pacotes já trazem prontos:
testes por expressão regular, anomalias aprendidas com o histórico, mudança de
schema e o registro dos resultados.

**Banco de grafos próprio (Neo4j) para o catálogo de colunas.** Daria consultas de
grafo mais ricas, mas seria mais um serviço para operar. O OpenMetadata já é um
grafo de metadados, e o contrato já é revisado em PR.

**Manter tudo em `warn`.** É o que o time faz hoje para layout, e funciona para
dar visibilidade. Mas quando o formato de uma data muda, a prata grava nulos e o
painel mostra número errado. Visibilidade depois do estrago não basta.

## Trade-offs

| Ganha | Paga |
|---|---|
| Mudança de layout ou de formato barrada antes de chegar ao painel | Mais execuções interrompidas, que alguém precisa destravar |
| Aceitar layout novo vira decisão revisada em PR | Toda fonte precisa de contrato antes de ser convertida |
| Um lugar só para o contrato, também publicado no catálogo | O `sources.yml` fica maior |
| Carga parcial ou truncada não passa como dado bom | Limites de volume mal calibrados geram falso alarme no começo |
| Renomeação conhecida não barra a carga, e a desconhecida chega com sugestão | `term` e `aliases` a manter em cada coluna do contrato |
| Testes prontos e registro de resultados sem código próprio | Dois pacotes externos a atualizar, e o custo do hook do Elementary por task |

## Consequências

- As entradas de `sources.yml` ganham `meta.load_type`, `meta.owner`, o
  `meta.format` das colunas e, nas fontes de arquivo, `meta.file_pattern`. O
  `meta.caminho` vira `meta.path` (ADR-0007).
- As colunas do contrato ganham `meta.term` e `meta.aliases` (catálogo de colunas).
- Duas macros novas: `formato_valido`, sobre o dbt-expectations, e
  `sem_perda_na_tipagem`. O volume vem do `volume_anomalies` do Elementary.
- O projeto passa a ter `packages.yml`, com o dbt-expectations e o Elementary
  (ADR-0014).
- `plugins/lake_tasks.py` ganha a task `verify_structural_data_drift`, e a DAG de fonte passa
  a ter três tasks (ADR-0004).
- A DAG geral ganha o `on_failure_callback` com e-mail e a task `notify_dbt_warnings`.
- O `sem_drift_de_colunas` e o `colunas_esperadas` dão lugar ao
  `schema_changes_from_baseline` do Elementary, como rede de segurança na bronze
  (`warn`), porque o portão estrutural passa a estar antes da staging.
- `convencao-testes-qualidade.md` ganha a distinção entre defeito da origem
  (`warn`) e drift (`error`).

## Plano de migração

1. Instalar o dbt-expectations e o Elementary (ADR-0014) e medir na homologação o
   custo do hook do Elementary.
2. Escrever o contrato das fontes do domínio `conjuntura` (as fontes de API são as
   mais estáveis) e criar as duas macros.
3. Montar o catálogo de colunas inicial: um script extrai as listas de nomes do
   `coalesce_present` nas macros de `macros/historico/`, os termos do
   `glossario-mcid.md` e o inventário de `colunas-orfas-bronze-historico.md`, e
   propõe o `term` e os `aliases` de cada coluna, para revisão em PR.
4. Implementar a task `verify_structural_data_drift`, com o e-mail, e encadeá-la nas DAGs de
   fonte (ADR-0004). Na DAG geral, ligar o `on_failure_callback` e a task
   `notify_dbt_warnings`.
5. Estender os contratos às fontes do SFTP e do SharePoint, com
   `file_pattern`, domínio a domínio, na ordem do plano do ADR-0002.
6. Calibrar `max_row_variation` e as tolerâncias com o histórico já presente
   na raw antes de ligar o `error`.

## Validação operacional

- Toda fonte em `sources.yml` tem `load_type`, `owner` e `meta.format` em
  todas as colunas (teste sobre o grafo, no CI).
- Toda bronze tem `formato_valido` e `volume_anomalies`; toda prata tem
  `sem_perda_na_tipagem`.
- Nenhum nome aparece nos `aliases` de duas colunas da mesma fonte, e todo `term`
  existe no glossário (teste sobre o grafo, no CI).
- Toda DAG de fonte encadeia `extract` → `verify_structural_data_drift` → `transform`.
- Nenhum objeto da staging foi gerado a partir de arquivo com status `quarantined`
  ou `uncataloged` em `lake._staging_log`.

## Questões em aberto

Nenhuma.
