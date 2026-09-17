# Padrões de layout do site docs-pages

Referência do sistema visual em `docs-pages/src/assets/tema.css`. Consulte ao
criar página nova, componente novo, ou ao mexer em espaçamento e alinhamento.

**Regra que atravessa tudo:** valor solto em regra é dívida. Use os tokens. Se o
valor que você precisa não existe na escala, o problema quase sempre é a escala,
não o caso.

## Escala de espaçamento

Toda distância vertical e horizontal sai daqui. Mexer nos tokens muda o respiro
do site inteiro; mexer numa regra isolada cria exceção que ninguém encontra
depois.

| Token | Valor | Uso típico |
|---|---|---|
| `--esp-1` | 0.5rem | folga entre rótulo e valor, itens de lista compacta |
| `--esp-2` | 0.85rem | margem de título, célula de tabela, gap de chips |
| `--esp-3` | 1.25rem | parágrafo, gap de grade, padding de bloco pequeno |
| `--esp-4` | 1.75rem | padding de card e do container, folga acima de `h3` |
| `--esp-5` | 2.5rem | padding de seção, folga acima de `h2`, rodapé |
| `--esp-6` | 4rem | topo do hero, gap entre colunas |

## Cor, forma e tipografia

| Grupo | Tokens |
|---|---|
| Marca | `--primary-purple` `#7a34f3` (assinatura), `--secondary-purple` `#8b5cf6`, `--purple-600` (hover), `--purple-700` (texto sobre claro) |
| Acento | `--accent-orange` `#f97316` — um destaque por seção, nunca mais |
| Texto | `--text-strong` títulos, `--text-body` corpo, `--text-muted` apoio |
| Fundo | `--bg-white` cartão, `--bg-light` página, `--bg-subtle` faixa alternada |
| Camadas | `--camada-bronze` `--camada-silver` `--camada-gold` |
| Forma | `--radius-sm` 6px, `--radius-md` 10px (padrão de card), `--radius-lg` 16px |
| Sombra | `--shadow-md` repouso, `--shadow-lg` hover, `--shadow-xl` reservada |
| Fonte | `--font-family-base` Inter com fallback de sistema, `--font-mono` para código e nome de modelo |

Contraste: texto pequeno em roxo sobre branco usa `--purple-700`. `--primary-purple`
só em texto grande ou como fundo com texto branco.

## Medida de leitura

Texto corrido nunca ocupa a largura total do container — 1120px dá linhas longas
demais para ler com conforto.

| Contexto | Largura |
|---|---|
| `.secao__intro` | 70ch |
| `.prosa` | 68ch |
| `.hero p` | 62ch |
| `.hero h1` | 22ch |
| Lista de destaque | 80ch |
| Texto dentro de `.frente` | 76ch |

Ao criar bloco de texto novo dentro de um card largo, limite a medida. O sintoma
de esquecer é uma linha que atravessa a tela inteira.

## Grades

```
.cards          repeat(auto-fit, minmax(270px, 1fr))   uso geral
.cards--2col    repeat(auto-fit, minmax(min(100%, 400px), 1fr))
.numeros        repeat(auto-fit, minmax(160px, 1fr))
.colunas        minmax(0, 2fr) / minmax(240px, 1fr)    texto + lateral
```

**Conjunto de quatro itens usa `.cards--2col`.** Com a grade padrão, quatro itens
quebram em 3+1 e o quarto card fica órfão e estreito na segunda linha. Duas
colunas largas resolvem, e 2×2 lê melhor que 3+1.

`.colunas` vira uma coluna abaixo de 820px, e a lateral perde o `sticky`.

## Ritmo vertical

- `section` tem `--esp-5` em cima e embaixo.
- `section + section` reduz o topo para `--esp-4`, para não somar os dois
  paddings sem colar o título de uma no fim da outra.
- `* + h2` recebe `--esp-5` de topo; `* + h3` recebe `--esp-4`. Título que abre
  uma seção não precisa disso — não tem irmão anterior.

## Sobreposição sobre o hero

`.numeros--hero` sobe 2rem sobre o hero. **Esse modificador é obrigatório e
exclusivo desse caso.** `.numeros` sem ele tem margem normal.

Uma margem negativa fixa aplicada ao componente base cobre o texto anterior
quando o bloco aparece no meio da página. Foi assim que a vitrine quebrou.

`.numeros--hero + *` recebe `--esp-4` de topo, porque o bloco que subiu comeu o
espaço do que vem depois.

## Marcadores posicionados

Marcador absoluto deve derivar o recuo do espaçamento do pai, não de um número
fixo:

```css
.entrega::before {
  left: calc(-1 * var(--esp-4) - 6px);  /* padding da lista + metade do ponto */
}
```

Valor fixo desalinha silenciosamente quando a escala muda — os pontos da linha do
tempo já saíram da linha uma vez por causa disso.

## Componentes

| Classe | Papel | Cuidado |
|---|---|---|
| `.hero` | abertura em gradiente da marca | `.hero--dominio` reduz a margem do `h1` |
| `.numeros` / `.numero` | indicadores | `--acento` troca a borda de topo para laranja: um por página |
| `.card` | bloco clicável ou informativo | `.card__tag` acima do `h3`; `.card .fonte` sem margem inferior |
| `.frente` | domínio na visão de gestão | números logo abaixo do título, com régua |
| `.gold` + `.fluxo` | tabela gold e seu diagrama | diagrama em `<details>`, recolhido por padrão |
| `.linha-tempo` / `.entrega` | histórico de entregas | `.entrega--rica` marca a que tem descrição do autor |
| `.tabela-wrap` | tabela com rolagem | sempre envolver a tabela; nunca deixar tabela solta |
| `.chip` | rótulo curto | `.chip--link` quando clicável, `.chip--fonte` para schema de origem |
| `.badge--bronze/silver/gold` | camada do modelo | cores em `--camada-*` |
| `.citacao` | fala do diagnóstico | acento laranja à esquerda; atribuir por cargo |
| `.fonte` | procedência do texto | obrigatório em bloco da vitrine |
| `.trilha` | volta para a capa | sai do `base.html.j2`, não repetir no template |
| `.legenda` | camadas do diagrama | as cores espelham `tooling/mermaid.py` → `ESTILOS` |
| `.topo` / `.abas` / `.aba` | cabeçalho fixo com as três visões | cada aba tem rótulo e público; `aria-current` marca a ativa |
| `.marca` / `.marca__escopo` | assinatura Gov Hub com a sigla do escopo | a sigla vem de `escopo.sigla`, não é texto fixo |
| `.hero__nota` | texto de apoio dentro do hero | régua de topo translúcida; não usar para texto longo |
| `.linha--descoberta` / `.pendente` | linha do programa ainda sem pipeline | sinaliza ausência sem esconder a linha |
| `.grafico` | SVG gerado em `tooling/graficos.py` | os gráficos são SVG estático, sem biblioteca de charts |
| `.filtro` | busca instantânea sobre uma tabela | ligado por `data-filtro-alvo`; some na impressão |
| `.aviso` | ressalva em destaque | acento laranja; um por página, no máximo |

## Conteúdo que transborda

- Tabela sempre dentro de `.tabela-wrap` (`overflow-x: auto`).
- Nome longo em célula usa `overflow-wrap: anywhere`.
- Coluna de descrição tem `min-width` — sem isso a linha dobra de altura.
- SVG de diagrama fica no **tamanho natural**, com rolagem no container.
  Encolher para caber torna os rótulos ilegíveis.

## Diagramas Mermaid

- `classDef` sempre com prefixo `mm-`. Sem ele, nomes como `fonte` e `gold`
  colidem com classes do tema e o CSS do site passa a estilizar o diagrama — o
  sintoma são rótulos em caixa alta e fora do lugar.
- Sem `subgraph` por camada: o Mermaid ignora `direction` quando há aresta
  cruzando e enfileira os nós na horizontal. Medido: 2403×169 com subgraph
  contra 1031×454 deixando o dagre empilhar sozinho.
- A legenda das camadas vive no HTML, escrita uma vez, não repetida em cada SVG.

## Responsivo

| Ponto | O que muda |
|---|---|
| 820px | `.colunas` vira uma coluna; lateral perde `sticky` |
| 720px | abas escondem o público e a legenda "Visões" |
| 640px | `h1` do hero diminui; abas ocupam a linha inteira |

## Impressão

`print.css` está embutido no tema. Escondem-se topo, trilha, rodapé, filtros e a
definição Mermaid; o hero perde o gradiente e o título vira roxo sobre branco;
cards e tabelas ganham borda e `break-inside: avoid`.

Ao criar componente interativo, esconda-o na impressão — a aba Gestão é usada
como PDF de prestação de contas.

## Como verificar

Não confie em impressão. Duas checagens que já pegaram defeito real:

**Colisão geométrica.** Percorra a página comparando o retângulo de cada par de
elementos de texto e reporte os que se cruzam. Ignore o interior de `<svg>`, que
tem sistema de layout próprio. Rode em 1440, 1024, 768 e 390px.

**Screenshot.** Renderize a página e olhe. Foi assim que apareceram os rótulos em
caixa alta do diagrama, os marcadores fora da linha e os chips cortados — nenhum
deles quebra o build nem aparece no HTML.

Antes de confiar num detector, reintroduza o defeito e confirme que ele acusa.
Detector que nunca falhou não foi testado.
