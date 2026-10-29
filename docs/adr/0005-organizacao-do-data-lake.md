# 0005. Organização do data lake

- **Status:** Proposto
- **Data:** 2026-10-19
- **Revisão:** 2026-10-29 (layout trocado: o lake espelha a fonte, com a pasta `carga=` antes do arquivo; entrada manual em `raw/manual/`; carga na coluna `_carga`; migração da raw antiga sem PII)
- **Relacionados:**
  - ADR-0001 (arquitetura), ADR-0002 (R0 e R1), ADR-0004 (ingestão e conversão),
    ADR-0006 (contrato da fonte), ADR-0009 (histórico)
  - Deck *GovHub — Arquitetura Medalhão de Referência*, seção 2
  - `plugins/cliente_minio.py`, `scripts/sftp_para_minio.py`, `scripts/raw_para_staging.py`

## Contexto

O deck organiza raw e staging "por domínio e por fonte, espelhando a origem", com a
staging na mesma árvore da raw, e deixa o padrão de nomes de bucket para definir
com o ministério. O ADR-0002 decidiu que a raw é imutável (R1): cada recebimento
tem caminho próprio e nunca é sobrescrito.

Hoje o lake tem três layouts diferentes:

- **Fontes de API** gravam numa chave fixa, `raw/<fonte>/<dado>.<ext>`
  (`cliente_minio.raw_key`), sobrescrita a cada execução. Gravam também uma segunda
  cópia do mesmo dado em `raw/<fonte>/fallback_json/<dado>.json`. A staging segue a
  mesma regra (`staging/<fonte>/<dado>.parquet`). O histórico da fonte se perde a
  cada execução.
- **SFTP e SharePoint** espelham o caminho do servidor
  (`raw/sftp/fabrica/GEFUS/ANTERIORES/x.TXT`). O nome do arquivo costuma trazer a
  data, mas a mesma base aparece em mais de uma pasta (`GEFUS/` e
  `GEFUS/ANTERIORES/`, SFTP e SharePoint), e a conversão precisa de uma lógica de
  "gêmeos" para não gerar tabelas repetidas. A data em que o arquivo chegou não
  aparece no caminho: um reenvio com o mesmo nome sobrescreve o anterior.
- **ABECIP, construtoras e MRV** usam a pasta como competência ou entidade
  (`abecip/2025-10/`).

Dos 44 caminhos declarados em `sources.yml` (`meta.caminho`), 7 apontam para um
arquivo com data fixa no nome (por exemplo, `PMCMV_FAIXA3_MCID_2026_08_21.parquet`
e `Base_PF_FGTS_20260707.parquet`). Quando a fonte manda o arquivo seguinte, o
modelo continua lendo o antigo.

A versão de 19/10 deste ADR reorganizava os arquivos de terceiros por base lógica
(`raw/<fonte>/<dado>/carga=.../`). Em 29/10 a decisão foi outra: o lake espelha a
fonte, e a data da ingestão entra no caminho, para rastreio.

## Decisão

### 1. Bucket e prefixos de topo

Um bucket, `data-lake-mcid`, organizado por fonte. A divisão por secretaria
acontece no warehouse, por schema (ADR-0010), não no lake: uma fonte pode servir a
mais de uma secretaria.

| Prefixo | Conteúdo | Quem escreve |
|---|---|---|
| `raw/` | Cada recebimento, como chegou, com a PII mascarada (ADR-0002 R0) | Task `extract` das DAGs de fonte; `scripts/enviar_manual.py`, em `raw/manual/` |
| `staging/` | O mesmo conteúdo em Parquet, na mesma árvore | Task `transform` e DAG de manutenção |
| `audit/` | Auditoria do mascaramento e da conversão, um Parquet por execução | As mesmas tasks |
| `dryrun/` | Prévias de execuções de teste, descartáveis | Execuções com `dry_run` |

Nada fica fora desses quatro prefixos.

### 2. O lake espelha a fonte, com a carga antes do arquivo

```text
Arquivos de terceiros (SFTP, SharePoint):
raw/<canal>/<caminho no servidor>/carga=<AAAAMMDDTHHMMSSZ>/<arquivo original>
staging/<canal>/<caminho no servidor>/carga=<AAAAMMDDTHHMMSSZ>/<arquivo original sem extensão>.parquet

Fontes de API:
raw/<fonte>/<dado>/carga=<AAAAMMDDTHHMMSSZ>/<arquivo>
staging/<fonte>/<dado>/carga=<AAAAMMDDTHHMMSSZ>/<arquivo sem extensão>.parquet

Entrada manual (ADR-0002 R7):
raw/manual/<base>/carga=<AAAAMMDDTHHMMSSZ>/<arquivo>
staging/manual/<base>/carga=<AAAAMMDDTHHMMSSZ>/<arquivo sem extensão>.parquet
```

Exemplos:

```text
raw/sftp/fabrica/GEFUS/carga=20261029T030000Z/PMCMV_FAIXA3_MCID_2026_08_21.TXT
staging/sftp/fabrica/GEFUS/carga=20261029T030000Z/PMCMV_FAIXA3_MCID_2026_08_21.parquet
raw/fgv/incc_m/carga=20261029T030000Z/incc_m.xlsx
```

- **O caminho é o da fonte.** Quem conhece o servidor acha o arquivo no lake pelo
  mesmo caminho, e nenhuma regra decide onde gravar. Fonte de API não tem caminho; a
  DAG define `<fonte>/<dado>`. Os arquivos do SharePoint, inseridos à mão, seguem o
  mesmo layout.
- **`carga=`** é o momento do recebimento, em UTC, no formato de partição Hive. É o
  que dá o rastreio de quando cada arquivo chegou. A conversão copia esse valor
  para a coluna `_carga` (ADR-0002 R2), que chega à bronze pelo `select *`. Duas
  cargas nunca colidem, então nada é sobrescrito.
- **A ordem das cargas é a da `_carga`.** No formato `AAAAMMDDTHHMMSSZ`, a ordem
  do texto é a ordem do tempo. O `_ingested_at` registra a conversão e muda
  quando a staging é regenerada (§6), então não ordena cargas.
- **O nome original do arquivo é preservado.** Em arquivos de terceiros, o nome
  carrega data e versão, e é o que se usa para derivar o período de referência
  (ADR-0009).
- **A entrada manual** (ADR-0002 R7) só chega pelo `scripts/enviar_manual.py`,
  que monta a pasta `carga=`, confere o hash (§3) e registra o envio. Ninguém
  grava à mão em `raw/manual/`.

### 3. Recebimento repetido não gera carga nova

O servidor pode mover o mesmo arquivo de pasta (`GEFUS/` para `GEFUS/ANTERIORES/`)
ou reenviá-lo sem mudança. A ingestão é incremental pelo hash do conteúdo,
registrado em `lake._ingest_minio_log`:

- arquivo cujo hash já tem carga não é gravado de novo. O registro anota o caminho
  novo e aponta para a carga que já existe;
- arquivo com o mesmo nome e conteúdo diferente é uma carga nova.

Assim, um arquivo movido de pasta não aparece duas vezes na bronze, e os "gêmeos"
deixam de existir para dados novos.

### 4. Identificação da base

O caminho espelhado não diz qual base lógica o arquivo é. Isso vem do
`padrao_arquivo` do contrato da fonte (ADR-0006), que casa com o caminho e o nome do
arquivo. Arquivo que não casa com nenhum padrão fica onde chegou, com status
`nao_catalogado` no `lake._staging_log`, gera alerta e não é convertido até alguém
catalogar a base.

### 5. A raw não aceita sobrescrita nem remoção

- O versionamento do bucket fica ligado, como rede de segurança.
- A política do usuário `app-cidades` no RustFS permite gravar em `raw/`, mas não
  apagar. Remover um objeto da raw é operação de manutenção, com outro usuário,
  registrada.

### 6. A staging é derivada

A staging pode ser apagada e regenerada da raw a qualquer momento pela DAG de
manutenção (ADR-0004). Ela não é fonte de verdade de nada.

### 7. Quarentena sem mover arquivo

O arquivo barrado no drift estrutural (ADR-0006) **fica onde está** na raw. Movê-lo
seria alterar a raw. A quarentena é um status no `lake._staging_log`: o arquivo não
ganha staging, e a staging da carga anterior continua valendo. Depois de ajustar o
contrato, a DAG de manutenção converte o arquivo.

### 8. Retenção

| Prefixo | Retenção |
|---|---|
| `raw/` | Indeterminada: é a evidência para auditoria, backtest e reprocessamento (issue #118) |
| `audit/` | Indeterminada |
| `staging/` | Enquanto for útil; pode ser regenerada da raw |
| `dryrun/` | Descartável a qualquer momento |

### 9. Caminhos em `sources.yml` sem data fixa

O `meta.caminho` de cada fonte aponta para todas as cargas da base, com glob sobre a
pasta `carga=` e o padrão do nome (`staging/sftp/fabrica/GEFUS/**/PMCMV_FAIXA3_MCID_*.parquet`),
nunca para um arquivo datado. Escolher a carga vigente é regra da ouro (ADR-0002 R5).

### Exceção: `dados_historicos`

`raw/dados_historicos/` tem tratamento próprio da equipe de ciência de dados e não
passa pela conversão (`PASTAS_IGNORADAS`). Continua como está, documentada como
exceção, até ser trazida para este layout.

## Alternativas consideradas

**Reorganizar por base lógica (`raw/<fonte>/<dado>/carga=.../`), como na versão de
19/10 deste ADR.** Agrupa cada base numa pasta, mas obriga a catalogar o arquivo antes
de gravar, e quem procura o arquivo do servidor não o acha pelo mesmo caminho.

**Espelhar sem data no caminho, com a data só em metadado.** Mantém o caminho
idêntico ao do servidor, mas um reenvio com o mesmo nome sobrescreve o anterior. As
versões antigas ficariam só no versionamento do bucket, invisíveis para a bronze, o
que contraria a R1.

**Data numa pasta acima do caminho (`raw/carga=.../sftp/...`).** Também evita a
colisão, mas quebra o espelho e impede o glob por base.

**Chave fixa com versionamento do bucket como histórico (prática atual das APIs).**
Mantém os caminhos de hoje, mas as versões antigas ficam invisíveis para o
`read_parquet` e para a bronze.

**Partição pelo período de referência (`periodo=2026-08`) em vez da carga.** O
período é interpretação do dado. Muitas vezes não se sabe na ingestão, e um reenvio
do mesmo período cairia no mesmo caminho e sobrescreveria o anterior. O período é
derivado depois, na prata (ADR-0009).

**Primeiro nível por domínio.** Obrigaria a decidir o domínio na ingestão e a
duplicar bases que servem a mais de um domínio.

**Pasta de quarentena separada.** Exigiria mover o objeto e alterar a raw.

## Trade-offs

| Ganha | Paga |
|---|---|
| O arquivo do servidor é achado no lake pelo mesmo caminho | O caminho não diz qual base é: isso depende do `padrao_arquivo` do contrato |
| Data de chegada de cada arquivo no próprio caminho | O lake cresce a cada carga |
| Nada é sobrescrito, nem por engano | Remover um objeto vira operação de manutenção |
| Arquivo movido ou reenviado sem mudança não duplica | A ingestão depende do registro de hashes em `lake._ingest_minio_log` |

## Consequências

- `cliente_minio.raw_key` e `staging_key` passam a receber a carga, e
  `upload_fallback_json` deixa de existir.
- `sftp_para_minio.py` continua espelhando o caminho do servidor e passa a gravar
  dentro de `carga=<AAAAMMDDTHHMMSSZ>/`, com o controle por hash que já existe.
- Os 7 `meta.caminho` com arquivo datado passam a usar glob.
- A lógica de "gêmeos" de `raw_para_staging.py` fica só para o legado, na DAG de
  manutenção.
- O `scripts/enviar_manual.py` é o único caminho de escrita em `raw/manual/`.

## Plano de migração

1. Mudar `raw_key`/`staging_key` e as DAGs de API, uma a uma, junto com a migração
   para `extract` → `transform` (ADR-0004).
2. Mudar o `sftp_para_minio.py` para gravar dentro de `carga=`, junto com o
   mascaramento antes de pousar (ADR-0002).
3. Objetos já existentes: um script único copia cada objeto para
   `<mesmo caminho>/carga=<data>/<arquivo>`, com duas condições:
   - **Só copia objeto sem PII.** Um arquivo tabular (`.csv`, `.txt`, `.xlsx`)
     precisa ter como último registro em `lake._masking_log` `masked`,
     `skipped_no_pii` ou `skipped_already`, e passar pela varredura dos
     detectores de `mascarar_minio.py` em modo só leitura. Os `.mdb`/`.accdb`,
     que o mascaramento lê mas não reescreve, não são copiados. Os JSON das
     APIs, que o mascaramento não lê, são copiados, porque trazem agregados
     públicos. Objeto que não passa é removido pelo usuário de manutenção e,
     se ainda for necessário, extraído de novo pelo caminho novo, com a máscara
     antes de pousar.
   - **A data é a do recebimento.** Vem do `finished_at` em
     `lake._ingest_minio_log`. A última modificação do objeto só vale na falta
     dele, porque, nos objetos mascarados no lugar, ela é a data da máscara.

   O original fica até a conferência e depois é removido pelo usuário de
   manutenção. A staging é regenerada a partir da raw nova.
4. Trocar os `meta.caminho` datados por glob.
5. Ligar o versionamento do bucket e passar a criar o usuário `app-cidades` e a
   política dele pelo playbook do RustFS (`infra/playbooks/rustfs/`). Hoje o usuário
   é criado à mão.

## Validação operacional

- A DAG de manutenção, em conferência, não encontra objeto fora dos quatro prefixos
  nem objeto de `raw/` sem uma pasta `carga=` logo acima do arquivo.
- Nenhum hash tem mais de uma carga em `lake._ingest_minio_log`.
- O usuário `app-cidades` recebe erro ao tentar apagar em `raw/`.
- Nenhum `meta.caminho` em `sources.yml` aponta para arquivo com data no nome.

## Questões em aberto

Nenhuma.
