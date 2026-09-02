# Identidade visual do boletim

Extraída do `Boletim de Conjuntura 2026 1T Final.pptx`, convertido e lido
página a página. A página publicada tem de ser reconhecível como **o mesmo
documento** — quem abre o link e quem abre o PPTX veem a mesma coisa.

## Paleta

| Papel | Cor | Onde |
|---|---|---|
| Âmbar da marca | `#F5B800` | banner da capa, header de tabela, rótulo de bloco, barra do comentário |
| Âmbar escuro (borda) | `#D99E00` | contorno de tabela e de cartão |
| Creme | `#FFF8E7` | corpo das linhas de tabela |
| Cinza do cabeçalho | `#E3E3E3` | barra superior das páginas internas |
| Verde positivo | `#00843D` | número e seta de alta |
| Verde de fundo | `#C6EFCE` | pílula e cartão de variação positiva |
| Vermelho negativo | `#C00000` | número e seta de baixa |
| Vermelho de fundo | `#FFC7CE` | pílula e cartão de variação negativa |
| Tinta | `#1A1A1A` | texto corrente |
| Fonte/nota | `#1F4E79` | "Fonte: XXX", menor e sublinhado |

**A cor carrega significado, e por isso nunca vai sozinha.** Variação negativa
é vermelha *e* traz o sinal `-`; positiva é verde *e* traz `+`. Quem não
distingue as duas cores continua lendo o boletim.

## Anatomia da página

**Capa.** Banner âmbar ocupando o topo à direita. "Secretaria Nacional de
Habitação" em versalete pequeno, acima. Título em duas linhas — "Conjuntura do"
em peso normal, "Setor Habitacional" em negrito. Selo da edição (`1T26`) à
direita, grande. Linha de data abaixo: "Boletim do primeiro trimestre de 2026
– DD/MM/AAAA". Logo de setas ascendentes verdes no canto.

**Internas.** Barra cinza no topo com "Conjuntura do Setor Habitacional" e, em
menor, "1º TRIMESTRE DE 2026". Mesmo logo à direita.

**Rótulo de bloco.** `DADOS TRIMESTRAIS` / `DADOS MENSAIS` em caixa alta, fundo
âmbar, apenas quando a natureza do dado muda.

**Seção.** Número grande à esquerda, título em caixa alta ao lado:
`4. EMPREGOS`. A numeração vem do boletim, não da ordem de renderização — o
original repete o `6.` em duas seções, e reproduzir isso é fidelidade, não erro.

## Componentes

**Tabela.** Header âmbar com texto escuro em caixa alta; corpo creme; primeira
coluna em negrito (é o rótulo da linha, não dado); números alinhados à direita;
borda âmbar de 1px. Linha de total em negrito.

**Cartão de destaque.** Caixa com título em caixa alta, valor grande (percentual
ou absoluto), legenda pequena embaixo dizendo contra o quê ("Em relação ao 4º
TRI/2025"). Fundo verde ou vermelho conforme o sinal.

**Pílula de variação.** Pequena, inline, colada ao número que ela qualifica.
Verde, vermelha ou âmbar (neutro). Some quando não há comparação possível.

**Comentário.** Fundo cinza claro, barra âmbar de 4px à esquerda, texto em
CAIXA ALTA. É leitura do dado, não legenda.

**Nota de fonte.** Pequena, `#1F4E79`, logo abaixo do quadro: "Fonte: IBGE".
Todo quadro tem uma. Sem fonte declarada, o quadro não sai.

## Gráfico

Só a página de crédito tem um: barras âmbar verticais, rótulo do valor **acima**
de cada barra, eixo x com `MM/AA`, sem eixo y, sem grade. Título em caixa alta
dentro do quadro. Antes de desenhar qualquer gráfico, carregar a skill
`dataviz`.

## Armadilhas do original

- **Texto sobreposto.** No PPTX, vários rótulos se sobrepõem aos quadros
  (página 1, "Fonte: CBIC" em cima do comentário). É defeito do slide, não
  padrão a reproduzir. A página deve respirar onde o slide aperta.
- **Números como texto.** No PPTX, `110.72 2` e `422.52 6` aparecem quebrados
  por ajuste de caixa. O valor certo vem da tabela gold, nunca do slide.
- **Numeração de seção inconsistente.** Há duas seções `6.` e duas `7.`.
  Reproduzir.
