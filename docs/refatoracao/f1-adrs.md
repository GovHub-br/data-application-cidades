# F1 · Organização do desenvolvimento via ADRs

> Parte do [guia das refatorações](README.md) · Frente prevista · Status: PR aberto · Issue: [#243](https://github.com/GovHub-br/data-application-cidades/issues/243) · PR: [#264](https://github.com/GovHub-br/data-application-cidades/pull/264)

## Objetivo

Registrar em ADR as decisões de arquitetura antes das refatorações que mudam contrato, para que a ingestão (F2), o dbt (F3) e o tooling (F4) partam de regras escritas, não de memória.

## Onde

Os ADRs ficam em `docs/adr/`, na branch `feat/adrs-arquiteturais` (PR #264). Eles começaram como um rascunho de 19/10 e foram revisados em 29/10, com os fatos conferidos com o código e as decisões tomadas depois. Todos estão `Proposto`. Depois da aprovação, mudam para `Aceito` no próprio PR, antes do merge (`docs/adr/README.md`).

## Mapa: decisão → ADR

| Decisão | ADR | O que ficou decidido |
|---|---|---|
| Contrato da `staging/` | 0002 (R2) e 0004 | Uma conversão comum para todas as fontes, a task `transform`. Toda coluna vai como texto, com nomes normalizados e colunas de linhagem. A tipagem começa na prata. |
| Destino das tabelas Postgres raw das APIs | 0001 e 0004 | As DAGs de fonte gravam só no lake. O `insert_data` sai, e os schemas são removidos depois de conferir o Superset e de um mês sem uso. |
| Nomenclatura | 0007 | `bronze/prata/ouro` no prefixo das tabelas, e um schema por secretaria (`snh`). |
| Dialeto e targets do dbt | 0003 | Padrão único: SQL de Postgres, com o pg_duckdb só na leitura do lake, e um target por ambiente (`homolog` e `prod`). |
| Layout do lake | 0005 | O lake espelha a fonte, com uma pasta `carga=<UTC>` antes do arquivo. Arquivo reenviado sem mudança não gera carga nova. |
| Como a `extract` fala com a fonte | 0004 | Hooks dos providers primeiro, cliente próprio só onde o hook não basta. Sem operators de transferência. |
| Orquestração | 0013 | Nada no parse, cron literal, configuração versionada, segredo em Connection e sem dado nunca verde. |
| Dependências, imagem e CI | 0014 | uv com lock versionado, imagem única publicada pela CI e CI blocante. |

## Questões em aberto nos ADRs

- **0002:** os dados manuais do boletim ficam como exceção ou passam pela raw?
- **0003:**
  - o ambiente local sem o `staging_duckdb`;
  - os valores dos limites do pg_duckdb.
- **0006:** o canal de alerta e os responsáveis por fonte (dependem do ministério).
- **0008:** o limite de 15 minutos.
- **0010:** as siglas das secretarias.
- **0011:** as políticas do ministério.
- **0014:** como o `uv.lock` respeita as constraints da imagem base.

## Critério de aceite

- Os ADRs estão `Aceito` e o PR #264 entrou na `main`, o que fecha a #243.
- O [registro de decisões do guia](README.md#8-registro-de-decisões) aponta para cada ADR.
