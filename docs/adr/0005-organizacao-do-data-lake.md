# 0005. Organização do data lake

- **Status:** Proposto
- **Data:** 2026-10-19
- **Relacionados:**
  - ADR-0001 (arquitetura), ADR-0002 (R0 e R1), ADR-0004 (ingestão e conversão),
  ADR-0006 (contrato da fonte)
  - Deck *GovHub — Arquitetura Medalhão de Referência* seção 2
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
  "gêmeos" para não gerar tabelas repetidas.
- **ABECIP, construtoras e MRV** usam a pasta como competência ou entidade
  (`abecip/2025-10/`).

Dos 44 caminhos declarados em `sources.yml` (`meta.caminho`), alguns apontam para
um arquivo com data fixa no nome (`PMCMV_FAIXA3_MCID_2026_08_21.parquet`). Quando a
fonte manda o arquivo do mês seguinte, o modelo continua lendo o antigo.

## Decisão

### 1. Bucket e prefixos de topo

Um bucket, `data-lake-mcid`, organizado por fonte. A divisão por secretaria
acontece no warehouse, por schema (ADR-0010), não no lake: uma fonte pode servir a
mais de uma secretaria.

| Prefixo | Conteúdo | Quem escreve |
|---|---|---|
| `raw/` | Cada recebimento, como chegou, com a PII mascarada (ADR-0002 R0) | Task `extract` das DAGs de fonte |
| `staging/` | O mesmo conteúdo em Parquet, na mesma árvore | Task `transform` e DAG de manutenção |
| `audit/` | Auditoria do mascaramento e da conversão, um Parquet por execução | As mesmas tasks |
| `dryrun/` | Prévias de execuções de teste, descartáveis | Execuções com `dry_run` |

Nada fica fora desses quatro prefixos.

### 2. Uma chave por recebimento

```text
raw/<fonte>/<dado>/carga=<AAAAMMDDTHHMMSSZ>/<arquivo original>
staging/<fonte>/<dado>/carga=<AAAAMMDDTHHMMSSZ>/<arquivo original sem extensão>.parquet
```

- **`<fonte>`** é quem entrega o dado: o provedor ou o canal (`ibge`, `bacen`,
  `fgv`, `sftp/fabrica/GEFUS`, `sharepoint`). Pode ter subníveis quando o canal tem
  áreas diferentes.
- **`<dado>`** é a base lógica (`incc_m`, `int059_fds_caixa_empreendimentos`). Em
  fontes de API, quem define é a DAG. Em arquivos enviados por terceiros (SFTP,
  SharePoint), vem do padrão de nome de arquivo declarado no contrato da fonte
  (ADR-0006). Arquivo que não casa com nenhum padrão vai para
  `raw/<fonte>/_nao_catalogado/carga=.../` e gera alerta. Ele não é convertido até
  alguém catalogar a base.
- **`carga=`** é o momento do recebimento, em UTC, no formato de partição Hive.
  O `read_parquet` com `hive_partitioning` expõe a coluna `carga` para a bronze sem
  nenhum código. Duas cargas nunca colidem, então nada é sobrescrito.
- **O nome original do arquivo é preservado.** Em arquivos de terceiros, o nome
  carrega data e versão, e é o que se usa para derivar o período de referência
  (ADR-0009).

O deck fala em organizar "por domínio e por fonte". Aqui o primeiro nível é a
fonte, porque ela é conhecida no momento da ingestão e não muda. O domínio é uma
leitura do dado: a mesma base do IBGE serve à conjuntura e à habitação. O domínio
aparece no warehouse, na pasta do dbt e no `meta.governance.product` (ADR-0007).

### 3. A raw não aceita sobrescrita nem remoção

- O versionamento do bucket fica ligado, como rede de segurança.
- A política do usuário `app-cidades` no RustFS permite gravar em `raw/`, mas não
  apagar. Remover um objeto da raw é operação de manutenção, com outro usuário,
  registrada.

### 4. A staging é derivada

A staging pode ser apagada e regenerada da raw a qualquer momento pela DAG de
manutenção (ADR-0004). Ela não é fonte de verdade de nada.

### 5. Quarentena sem mover arquivo

O arquivo barrado no drift estrutural (ADR-0006) **fica onde está** na raw. Movê-lo
seria alterar a raw. A quarentena é um status no `lake._staging_log`: o arquivo não
ganha staging, e a staging da carga anterior continua valendo. Depois de ajustar o
contrato, a DAG de manutenção converte o arquivo.

### 6. Retenção

| Prefixo | Retenção |
|---|---|
| `raw/` | Indeterminada: é a evidência para auditoria, backtest e reprocessamento (issue #118) |
| `audit/` | Indeterminada |
| `staging/` | Enquanto for útil; pode ser regenerada da raw |
| `dryrun/` | Descartável a qualquer momento |

### 7. Caminhos em `sources.yml` sem data fixa

O `meta.caminho` de cada fonte aponta para todas as cargas da base
(`staging/<fonte>/<dado>/**/*.parquet`), nunca para um arquivo datado. Escolher a
carga vigente é regra da ouro (ADR-0002 R5).

### Exceção: `dados_historicos`

`raw/dados_historicos/` tem tratamento próprio da equipe de ciência de dados e não
passa pela conversão (`PASTAS_IGNORADAS`). Continua como está, documentada como
exceção, até ser trazida para este layout.

## Alternativas consideradas

**Chave fixa com versionamento do bucket como histórico.** Mantém os caminhos de
hoje, mas as versões antigas ficam invisíveis para o `read_parquet` e para a bronze.
O histórico existiria, mas ninguém o leria.

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
| Histórico completo da fonte, legível pela bronze | O lake cresce a cada carga |
| Nada é sobrescrito, nem por engano | Remover um objeto vira operação de manutenção |
| Fim dos "gêmeos" para dados novos | Arquivos de terceiros precisam de padrão de nome no contrato |
| Fonte nova não exige decidir domínio na ingestão | A relação entre fonte e domínio fica no warehouse, não no lake |

## Consequências

- `cliente_minio.raw_key` e `staging_key` passam a receber a carga, e
  `upload_fallback_json` deixa de existir.
- `sftp_para_minio.py` deixa de espelhar o caminho do servidor e grava em
  `raw/sftp/<área>/<dado>/carga=.../`.
- Os `meta.caminho` com arquivo datado passam a usar glob.
- A lógica de "gêmeos" de `raw_para_staging.py` fica só para o legado, na DAG de
  manutenção.

## Plano de migração

1. Mudar `raw_key`/`staging_key` e as DAGs de API, uma a uma, junto com a migração
   para `extract` → `transform` (ADR-0004).
2. Mudar o `sftp_para_minio.py` para o layout novo, junto com o mascaramento antes
   de pousar (ADR-0002).
3. Objetos já existentes: um script único copia cada objeto de chave fixa para
   `carga=<data da última modificação>`. O original fica até a conferência e depois
   é removido pelo usuário de manutenção.
4. Trocar os `meta.caminho` datados por glob.
5. Ligar o versionamento do bucket e ajustar a política do `app-cidades`
   (`infra/playbooks/rustfs/`).

## Validação operacional

- A DAG de manutenção, em conferência, não encontra objeto fora dos quatro prefixos
  nem objeto de `raw/` fora do padrão `<fonte>/<dado>/carga=.../`.
- O usuário `app-cidades` recebe erro ao tentar apagar em `raw/`.
- Nenhum `meta.caminho` em `sources.yml` aponta para arquivo com data no nome.

## Questões em aberto

Nenhuma.
