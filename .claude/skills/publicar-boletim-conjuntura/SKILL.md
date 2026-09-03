---
name: publicar-boletim-conjuntura
description: Use para publicar o Boletim de Conjuntura do Setor Habitacional como página no claude.ai, com a identidade visual do PPTX oficial e os números lidos das tabelas gold. Também quando pedirem para republicar uma edição, atualizar a edição corrente, conferir a página contra o boletim publicado, ou quando alguém perguntar como o boletim vira link.
---

# Publicar o boletim de conjuntura

Gera a página do boletim a partir das mesmas tabelas que alimentam o dashboard
do Superset, com a identidade visual do PPTX. São **dois destinos do mesmo
dado**: o Superset para exploração, a página para leitura e compartilhamento.

**Layout, paleta e componentes:** [references/layout.md](references/layout.md).
Leia antes de escrever qualquer HTML. Carregue também a skill `artifact-design`
antes de publicar, e `dataviz` antes de desenhar o gráfico de crédito.

## O princípio

A página é **híbrida**, como o `docs-pages`: o número é lido, o texto é escrito.

| O que | De onde | Pode ser gerado? |
|---|---|---|
| Quadros de dados | tabelas `gld_boletim_p<N>_*` | não — é leitura |
| Leitura dos números | modelo, ancorado nos deltas da própria página | **sim** |
| Projeção de terceiro (ABECIP, SINDUSCON, CBIC) | `boletim-editorial.yml` | **NÃO** |
| Visão MCID | `boletim-editorial.yml` | **NÃO** |

As duas últimas linhas não são preferência de estilo. Projeção de terceiro é
número que alguém publicou e que não está em tabela nenhuma nossa — inventar é
atribuir a instituições declarações que elas não fizeram. Visão MCID é posição
do Ministério; gerar é pôr palavra na boca do órgão. **Sem entrada declarada,
a seção não sai** — e a página diz que não saiu, em vez de fingir completude.

## Onde estão as coisas

| | |
|---|---|
| Dados | `conjuntura.gld_boletim_p<N>_*` — 21 quadros, 7 páginas |
| Estrutura | `scripts/superset/build_boletim.py`, lista `Quadro` (página, seção, título, colunas, nota) |
| Filtro de edição | coluna `edicao` (`'1T2026'`) em todos os quadros |
| Validação | `scripts/conjuntura/dados/gabarito-boletins.yml` — 32 checagens contra 3 edições |
| Editorial | `scripts/conjuntura/dados/boletim-editorial.yml` (por edição) |

O acesso ao banco exige VPN. Sem ela, **pare** e diga que precisa — não publique
com dado parcial.

## O processo

1. **Descubra a edição.** Sem edição informada, use a mais recente com dado nos
   quadros. Confirme com quem pediu antes de publicar edição passada.

2. **Leia os 21 quadros**, filtrando por `edicao`. A ordem de página e seção vem
   do `build_boletim.py`, não da ordem das tabelas no banco.

3. **Valide contra o gabarito** antes de escrever qualquer HTML. Rode as
   checagens da edição, se houver. Divergência **não impede** publicar — fontes
   como BACEN, IBGE, CAGED e FipeZap revisam o passado —, mas cada uma tem de
   aparecer na página, num aviso, dizendo qual célula diverge e de quanto.
   Publicar em silêncio um número que não bate com o boletim impresso é o pior
   resultado possível.

4. **Escreva a leitura dos números.** Uma frase por seção, descrevendo o que a
   variação mostra. Regras:
   - toda afirmação tem de ser conferível num número **que está na página**;
   - não afirme causa ("por causa da Selic") — o dado não diz causa;
   - não projete futuro;
   - o tom é o do boletim: caixa alta, direto, sem adjetivo de opinião.

5. **Monte a página** seguindo `references/layout.md`.

6. **Publique como Artifact**, com o título `Conjuntura do Setor Habitacional
   <edição>`. Republicar a mesma edição atualiza a mesma URL: passe o mesmo
   caminho de arquivo, ou a `url` da página existente.

## O que marca a página como gerada

Todo bloco de texto escrito por modelo entra com marca visível de rascunho e a
data de geração. Não é disclaimer decorativo: é um boletim oficial, e quem lê
tem de saber o que foi apurado e o que foi redigido por máquina. A marca só sai
quando alguém do time revisar e disser que sai.

## Armadilhas

- **O PPTX não é fonte de número.** Ele tem valores quebrados por ajuste de
  caixa (`110.72 2`). O número vem da tabela gold.
- **A estrutura do PPTX não é a do dashboard.** O PPTX tem cartões, comentários
  e uma página de fechamento que os 21 quadros não cobrem. Não invente quadro
  para preencher; deixe a seção de fora e diga que ficou.
- **Trimestre é inteiro, edição é texto.** `trimestre` é `1`, `edicao` é
  `'1T2026'`. Filtrar pelo campo errado devolve vazio sem erro.
- **Fonte revisa o passado.** Republicar uma edição antiga com dado corrente
  muda números já impressos. Para reproduzir a edição como publicada, leia dos
  snapshots (`conjuntura`), não do mart.
