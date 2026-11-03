# Protocolo de Aprovacao de Pull Requests

Este protocolo orienta a revisao de Pull Requests (PRs) no `data-application-cidades`.

## Fluxo

1. Crie uma branch a partir da `main` e use um titulo de PR no formato Conventional Commits.
2. Descreva a mudanca, a issue relacionada e como ela foi validada no template de PR.
3. Aplique manualmente `team:mcid` para mudancas do dominio de Cidades e/ou `team:infra` para infraestrutura transversal.
4. A workflow solicita revisao dos times associados quando a label e adicionada a um PR nao draft ou quando um PR com label sai de draft. Ela nao cria labels e nao roda para labels de issues.
5. Revise os comentarios, obtenha as aprovacoes exigidas pela protecao da `main` e so entao faca o merge.

## Labels de revisao

| Label | Time solicitado |
| --- | --- |
| `team:mcid` | `@GovHub-br/mcid` |
| `team:infra` | `@GovHub-br/infra` |

Solicitar revisao a um time **nao equivale a receber uma aprovacao**. As labels apenas roteiam o pedido de review; elas nao mudam as regras de aprovacao ou de protecao da branch. Se o time solicitado nao corresponder a mudanca, ajuste a label ou explique a escolha no PR.

A workflow precisa do secret `TEAM_REVIEW_TOKEN` com permissao para solicitar revisores. Se ele estiver ausente ou o GitHub recusar a solicitacao, o job falha para tornar o problema visivel. Nesse caso, um mantenedor deve solicitar a revisao manualmente enquanto a configuracao e corrigida. O job so bloqueia o merge se estiver configurado como check obrigatorio na protecao da branch.

Para codigo e dados, valide com `make lint`, `make test` e testes especificos quando aplicavel. Para documentacao, confira links, formatacao e aderencia ao estado atual do repositorio. Justifique no PR os testes nao executados.
