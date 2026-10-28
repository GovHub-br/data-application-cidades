# Architecture Decision Records (ADRs)

Decisões de arquitetura da aplicação de dados do Ministério das Cidades. Cada ADR
registra uma decisão, o contexto que levou a ela e o que muda no código.

A fonte da verdade para o conteúdo é o contrato de dados apresentado no deck
**GovHub — Arquitetura Medalhão de Referência** (versão de 2026-10-06). Onde o
código atual diverge do contrato, o ADR decide pelo contrato e traz o plano de
migração do que já existe.

Os documentos em `dbt/mcid/models/docs/` continuam valendo como guias de
implementação. Quando um deles conflitar com um ADR aceito, o ADR prevalece e o
documento deve ser atualizado.

## Índice

A numeração é sequencial e segue a ordem de leitura: cada ADR pode depender dos
anteriores. Para entender a arquitetura, leia na ordem.

| ADR | Título | Status |
|---|---|---|
| [0001](0001-arquitetura-de-referencia.md) | Arquitetura de referência: data lake e data warehouse | Proposto |
| [0002](0002-contrato-entre-camadas.md) | Contrato entre camadas | Proposto |
| [0003](0003-ambientes-targets-e-dialetos.md) | Ambientes, targets e dialetos | Proposto |
| [0004](0004-ingestao-e-conversao.md) | Ingestão e conversão para a staging | Proposto |
| [0005](0005-organizacao-do-data-lake.md) | Organização do data lake | Proposto |
| [0006](0006-data-drifts.md) | Data drifts: estrutural e semântico | Proposto |
| [0007](0007-nomenclatura.md) | Nomenclatura | Proposto |
| [0008](0008-materializacao-e-execucao.md) | Materialização e execução | Proposto |
| [0009](0009-historico-e-versionamento.md) | Histórico e versionamento | Proposto |
| [0010](0010-divisao-do-warehouse-por-secretaria.md) | Divisão do warehouse por secretaria | Proposto |
| [0011](0011-controle-de-acesso.md) | Controle de acesso | Proposto |
| [0012](0012-catalogo-linhagem-e-dados-pessoais.md) | Catálogo, linhagem e dados pessoais | Proposto |

## Ciclo de vida

| Status | Significado |
|---|---|
| Proposto | Rascunho em revisão no PR. Ainda não obriga ninguém. |
| Aceito | Aprovado pelo time. Código novo segue; código antigo migra pelo plano do ADR. |
| Aguardando ministério | A decisão depende de definição do ministério. O ADR registra as opções e o que já vale enquanto isso. |
| Substituído por NNNN | Outra decisão tomou o lugar. O texto antigo fica, para manter o histórico. |

Um ADR aceito não é editado para mudar a decisão. Muda-se escrevendo outro ADR que
o substitui.

## Como escrever um ADR

1. Copie o modelo abaixo para `NNNN-titulo-curto.md`. O número segue a ordem de
   leitura: se o ADR novo entra no meio, os seguintes são renumerados e as
   referências `ADR-NNNN` nos textos, atualizadas.
2. Abra um PR com o status `Proposto` e inclua a linha no índice.
3. Quando o time aprovar, mude para `Aceito` no mesmo PR, antes do merge.

```markdown
# NNNN. Título

- **Status:** Proposto
- **Data:** AAAA-MM-DD
- **Relacionados:** issues, ADRs, documentos

## Contexto

O problema, as forças em jogo e o que existe hoje.

## Decisão

O que passa a valer, em regras verificáveis.

## Alternativas consideradas

Cada alternativa e por que não foi escolhida.

## Trade-offs

O que se ganha e o que se paga.

## Consequências

O que muda para quem desenvolve, opera e consome.

## Plano de migração

Do estado atual ao decidido, por etapa e por domínio.

## Validação operacional

Como saber, no dia a dia, que a decisão está sendo cumprida.

## Questões em aberto

O que ainda falta definir e quem define.
```
