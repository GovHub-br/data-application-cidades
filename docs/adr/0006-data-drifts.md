# 0006. Data drifts: estrutural e semântico

- **Status:** Proposto
- **Data:** 2026-10-19
- **Revisão:** 2026-10-29 (fatos conferidos com o código; `padrao_arquivo` sobre o caminho espelhado do ADR-0005; drift estrutural numa task própria, `verify_structural_data_drift`; alertas por e-mail)
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
          caminho: staging/fgv/incc_m/**/*.parquet
          tipo_carga: completa          # completa | incremental
          variacao_maxima_linhas: 0.5   # só para carga completa
          padrao_arquivo: null          # SFTP/SharePoint: regex sobre o caminho espelhado e o nome, com o período
          responsavel: <área do ministério que responde pela base>
        columns:
          - name: mes
            meta: { formato: data_br }
          - name: indice
            meta: { formato: decimal_br }
```

Os nomes das colunas são os nomes já normalizados pela conversão (ADR-0002 R2).
Mudar o contrato é um PR: aceitar um layout novo é uma decisão explícita e revisada.

### 2. Drift estrutural, na task `verify_structural_data_drift`, antes de converter

O drift estrutural é uma task própria da DAG de fonte, a `verify_structural_data_drift`,
entre a `extract` e a `transform` (ADR-0004). Como a `transform`, ela é escrita
uma vez em `plugins/lake_tasks.py` e usada por todas as DAGs de fonte. Ela lê o
contrato e compara com o cabeçalho normalizado de cada arquivo novo. Os dados só
são lidos na carga completa, para contar as linhas.

| Situação | Reação |
|---|---|
| Igual ao contrato | Converte |
| Coluna nova | Converte, registra `drift_aditivo` e alerta |
| Coluna do contrato ausente | Não converte: `quarentena`, alerta |
| Coluna renomeada | Não converte: `quarentena`, alerta com a sugestão de renomeação |
| Arquivo sem cabeçalho, ou encoding e separador não detectados | Não converte: `quarentena` |
| Fonte ou arquivo sem contrato | Não converte: `nao_catalogado`, alerta |
| Carga completa com linhas fora de `variacao_maxima_linhas` em relação à carga anterior | Não converte: `quarentena` (arquivo truncado, ou a fonte passou a mandar só o incremental) |

- Uma coluna "renomeada" aparece como uma ausente mais uma nova. A `verify_structural_data_drift`
  compara os nomes por similaridade e sugere a renomeação no alerta. A decisão é
  humana, pelo contrato.
- A `verify_structural_data_drift` confere todas as chaves recebidas, registra o status de
  cada uma em `lake._staging_log` e passa adiante só as aprovadas: iguais ao
  contrato ou com coluna nova.
- Se algum arquivo foi barrado (`quarentena` ou `nao_catalogado`), ela manda o
  e-mail (§5) e termina em falha. A `transform` roda mesmo assim e converte só
  as aprovadas. O problema aparece em vermelho no Airflow sem bloquear os outros
  arquivos da mesma carga.
- O tipo de carga declarado (`tipo_carga`) é conferido pela estrutura do arquivo.
  Na carga completa, o arquivo cobre a base inteira e o número de linhas fica
  perto do da carga anterior. Na incremental, traz só o período novo. Isso
  responde ao requisito 8 da issue #130, e a materialização segue essa
  classificação (ADR-0008).

### 3. Drift semântico, nos testes da bronze

O drift semântico roda no `dbt build`, como testes da bronze. Teste `error` que
falha impede a prata de rodar. No Airflow, cada teste é uma task da DAG geral: o
Cosmos gera uma task de teste depois de cada modelo. Assim, os dois drifts são
interceptados dentro de uma DAG: o estrutural na DAG de fonte, o semântico na
DAG geral.

| Teste | Pega | Severidade |
|---|---|---|
| `formato_valido` (novo) | Valor fora do `meta.formato` da coluna: data que virou `aaaa-mm-dd`, separador decimal trocado, inteiro com texto | `error` |
| `dentro_do_dominio` / `accepted_values` (existentes) | Código de domínio novo numa coluna de domínio fechado | `error` |
| `volume_da_carga` (novo) | Carga com volume fora da faixa da anterior; pega carga parcial (ADR-0004) | `error` |
| `sem_perda_na_tipagem` (novo, na prata) | Valor preenchido na bronze que virou nulo depois do cast na prata | `error` (bloqueia a ouro) |

Formatos iniciais de `meta.formato`: `data_br`, `data_iso`, `decimal_br`,
`decimal_ponto`, `inteiro`, `texto`, `dominio`. Cada formato é uma expressão
regular na macro `formato_valido`, com tolerância configurável
(`pct_max_invalido`, padrão 0).

**Como isso concilia com a convenção atual.** A convenção de testes continua
valendo para o que ela trata: **defeito da origem**, como mojibake ou código
`"3550308.0"`. Isso é `warn` na bronze, porque a prata trata. **Drift** é outra
coisa: a origem passou a mandar diferente do contrato. Isso é `error`, porque a
tipagem da prata transformaria em nulo, sem aviso, o que não reconhece. A
convenção ganha essa distinção.

### 4. Anomalias de negócio na ouro: monitoramento, não drift

Variação de um indicador acima de um limite (por exemplo, UH entregues que caem
pela metade de um mês para o outro) é `warn` na ouro. O teste alerta e não
interrompe, como no deck. Não é drift da fonte: o dado pode estar certo e o mundo
ter mudado.

### 5. Registro e alerta por e-mail

Todo alerta de drift é um e-mail.

| O que | Onde fica registrado | Quem manda o e-mail |
|---|---|---|
| Drift estrutural: arquivo em `quarentena` ou `nao_catalogado`, coluna nova | `lake._staging_log` | A `verify_structural_data_drift`, um e-mail por execução, com os arquivos, o motivo e a sugestão de renomeação, se houver |
| Drift semântico: teste `error` da bronze que falha | `lake._dbt_log` (`registrar_execucao_dbt`) | O `on_failure_callback` da DAG geral, quando a task do teste falha |
| Avisos: testes `warn` da bronze e anomalias da ouro (§4) | `lake._dbt_log` | A task `alertar_avisos`, no fim da DAG geral: lê os avisos da execução e manda um e-mail só, se houver algum |

- O e-mail sai pelo provider SMTP do Airflow, que já está na imagem. Servidor e
  credencial ficam numa Connection (ADR-0013).
- O destinatário é um endereço de grupo do time, configurado no ambiente do
  Airflow e não no repositório, que é público.
- O e-mail traz nomes de arquivo, de coluna e de teste, nunca valores do dado.

### 6. Quem decide o que é quebra

Os limites técnicos padrão são os deste ADR. Os limites de negócio de cada fonte
(variação de volume aceitável, domínios válidos, limiar de anomalia) são definidos
pelo `responsavel` declarado no contrato, que é quem conhece o dado, e registrados
no próprio contrato.

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

**Ferramenta externa de qualidade (Great Expectations, Soda).** Seria mais um
serviço para operar e iria contra a política de não usar pacotes; os testes cabem
como macros próprias.

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

## Consequências

- As entradas de `sources.yml` ganham `meta.tipo_carga`, `meta.responsavel`, o
  `meta.formato` das colunas e, nas fontes de arquivo, `meta.padrao_arquivo`.
- Três macros novas: `formato_valido`, `volume_da_carga` e `sem_perda_na_tipagem`.
- `plugins/lake_tasks.py` ganha a task `verify_structural_data_drift`, e a DAG de fonte passa
  a ter três tasks (ADR-0004).
- A DAG geral ganha o `on_failure_callback` com e-mail e a task `alertar_avisos`.
- O `sem_drift_de_colunas` e o `colunas_esperadas` ficam como rede de segurança na
  bronze (`warn`), porque o portão estrutural passa a estar antes da staging.
- `convencao-testes-qualidade.md` ganha a distinção entre defeito da origem
  (`warn`) e drift (`error`).

## Plano de migração

1. Escrever o contrato das fontes do domínio `conjuntura` (as fontes de API são as
   mais estáveis) e criar as três macros.
2. Implementar a task `verify_structural_data_drift`, com o e-mail, e encadeá-la nas DAGs de
   fonte (ADR-0004). Na DAG geral, ligar o `on_failure_callback` e a task
   `alertar_avisos`.
3. Estender os contratos às fontes do SFTP e do SharePoint, com
   `padrao_arquivo`, domínio a domínio, na ordem do plano do ADR-0002.
4. Calibrar `variacao_maxima_linhas` e as tolerâncias com o histórico já presente
   na raw antes de ligar o `error`.

## Validação operacional

- Toda fonte em `sources.yml` tem `tipo_carga`, `responsavel` e `meta.formato` em
  todas as colunas (teste sobre o grafo, no CI).
- Toda bronze tem `formato_valido` e `volume_da_carga`; toda prata tem
  `sem_perda_na_tipagem`.
- Toda DAG de fonte encadeia `extract` → `verify_structural_data_drift` → `transform`.
- Nenhum objeto da staging foi gerado a partir de arquivo com status `quarentena`
  ou `nao_catalogado` em `lake._staging_log`.

## Questões em aberto

1. **Limites de negócio por fonte.** Quem, em cada área do ministério, responde
   como `responsavel` e valida os limites. Depende do ministério (pendência da
   seção 5 do deck).
