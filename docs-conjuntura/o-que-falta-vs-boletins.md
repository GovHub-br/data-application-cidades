# O que falta — comparação com os 3 boletins publicados

Levantamento de **2026-08-30**, contra 3T2025, 4T2025 e 1T2026.
Escopo: dados de 2025 em diante.

Base: 27 golds de indicador (os 4 `gold_qualidade_*` não entram).

> Cada número abaixo foi medido, não estimado. Onde não consegui medir, está
> escrito que não consegui.

---

## 1. Indicadores que o boletim publica e nós NÃO temos

Não existe gold. Não é atraso: é ausência de fonte.

| Indicador | Página | Situação |
|---|---|---|
| **CBIC — lançamentos e vendas por região** | 1 | Sem acesso à base. Hoje entra por inserção manual em lote (`0003`). |
| **Desembolsos de Obras — FAR / Rural / FDS** | 6 | Fontes candidatas testadas e **descartadas**: os erros vão em direções opostas (FAR +51 a +72%, FDS −76 a −88%, Rural −20%). Ver §1.3 do MEMORY. |
| **Índice ABRAMAT** | 7 | Automação avaliada e descartada: em 4 releases, o acumulado do ano só aparece em 2. |

---

## 2. Golds que temos mas que NÃO batem com o boletim

Este é o grupo mais grave: o dado existe, alimenta dashboard, e está errado
ou não corresponde ao que foi publicado.

### 2.1 `gold_continuo_producao_fisica` — AGREGADO ERRADO NA INGESTÃO 🔴

**Causa confirmada em 2026-08-30 com os metadados do SIDRA: a DAG do IBGE pede
os agregados errados.** Não é erro de transformação — o dbt está certo; o dado
que chega é de outra pesquisa.

| | Agregado correto | O que estamos puxando |
|---|---|---|
| **PIM-PF** | **8886** — "Produção Física Industrial dos insumos típicos da construção civil" (sem classificação) | **8888** — "por seções e atividades industriais", classif. 544, categoria 129314 = "1 Indústria geral" |
| **PMC** | **8757** — "receita nominal e volume de vendas de materiais de construção", classif. 11046, categoria **56732** | agregado de varejo geral, categorias 56733/56734 |

Ou seja: publicamos a **indústria brasileira inteira** e o **varejo em geral**
sob rótulo de construção.

**Prova — os agregados corretos batem com o boletim:**

PIM 8886, período 202503: mensal 2,8 · acum. ano 4,1 · 12 m 6,0 →
**os três exatos**. Em 202602, 12 meses = −3,1, também exato.

PMC 8757 + categoria 56732: **seis batidas exatas** nos acumulados —
202503 (6,1 / 6,7), 202602 (−5,5 / −2,1), 202603 (−1,0 / −1,8).

As linhas "mensal" divergem 0,1–0,6 p.p. em alguns meses; são revisões do
IBGE posteriores à publicação, o padrão já conhecido.

**Onde corrigir:** na Variable do Airflow `IBGE_CONFIGURACOES`, que define os
agregados pedidos. Não dá para corrigir no dbt.

```
PIM-PF:  agregado 8886, sem classificação
PMC:     agregado 8757, classificação 11046, categoria 56732
```

> ⚠️ **Erro meu, registrado para não se repetir.** Ao ver as colunas do PMC
> 100% nulas em 2026-08-30, troquei o filtro `categoria_id = 56732` por 56734
> supondo erro de digitação, porque 56732 não existia na silver. Estava
> errado: 56732 é o código certo e sumia justamente porque vem de outro
> agregado. A troca fez as colunas preencherem com **varejo geral** —
> **troquei uma falha visível por dado errado silencioso**, que é pior. O
> filtro foi revertido para 56732 no mesmo dia; as colunas voltam a ficar
> nulas, e nulo é o comportamento correto enquanto a ingestão estiver errada.
>
> Lição: coluna nula com filtro que "não casa nada" pode ser sintoma de fonte
> errada, não de filtro errado. Conferir o agregado antes de mexer no filtro.

**Correção também no diagnóstico anterior:** eu havia afirmado que o gold
mapeava `variavel_id = 11602` erradamente como "variação mensal". **Não é
bug.** O boletim rotula essa linha como "variação percentual mensal" e usa
mesmo a M/M-12 — conferido: 202503 = 2,8 nos dois. O mapeamento está certo.

### 2.2 `gold_continuo_empregos_caged` — revisão da fonte, não erro 🟢

| Medida (mar/2026) | Boletim | Nosso |
|---|---|---|
| Saldo — total construção | 38.316 | 37.811 |
| Estoque — total construção | 3.063.821 | 3.070.220 |

**Não é divergência a resolver.** O CAGED é lido do painel **Power BI, que é
vivo**: declarações entregues fora do prazo entram depois e revisam meses já
fechados. O boletim congelou a safra de ~jun/2026; temos a atual, com o mesmo
mar/2026 já revisto (e mais abr–jun). Estoque para cima e saldo do mês para
baixo são movimentos independentes, normais numa revisão.

**A fonte é a referência válida; o boletim é uma foto anterior.**

### 2.3 `gold_continuo_pnad_rendimento` — divergência esperada, nunca quantificada 🟡

| Medida (jan-fev-mar/2026) | Boletim | Nosso | Diferença |
|---|---|---|---|
| Rendimento construção | R$ 2.858 | R$ 2.922 | +2,2% |
| Rendimento total | R$ 3.610 | R$ 3.690 | +2,2% |

O desvio é **idêntico nas duas séries**, o que é consistente com o rebase de
deflator que o IBGE aplica a cada divulgação. É divergência esperada — mas
não estava medida em lugar nenhum, e não está no gabarito.

### 2.4 `gold_continuo_ogu` — não comparável sem congelamento 🟡

O boletim publica a posição **congelada em jan–mar/2026**; nosso gold traz a
**posição corrente** (agosto). Dotação: 16.028 milhões nossos contra 33.958
publicados; pagamento 38.990 contra 5.682. Não é erro de cálculo — são
recortes temporais diferentes. **Só será comparável quando existir o
congelamento por edição** (§9 do MEMORY).

---

## 3. Golds com dado defasado

| Gold | Último dado | Atraso |
|---|---|---|
| `gold_continuo_novos_financiamentos_banco` (manual) | **set/2025** | ~11 meses |
| `gold_continuo_canal_fgts` (manual) | **nov/2025** | ~9 meses |

Ambos são manuais. O `novos_financiamentos_banco` **já tem substituto
automatizado pronto** (`gold_continuo_financiamentos_instituicao`), aguardando
decisão de aposentadoria.

---

## 4. Comparação dos golds que não tinham checagem (feita em 2026-08-30)

### 4.1 Batem com o boletim

| Gold | Verificação | Resultado |
|---|---|---|
| `pnad_ocupados` | jan-fev-mar/26: 7.335/101.976; out-nov-dez/25: 7.468/102.998 | **exato nos 2** |
| `credito_pib` | fev/2026: 10,90 vs 10,91 | **exato** (arredondamento) |
| `saldo_poupanca` | mar/26 −9,1; fev/26 −4,1; mar/25 −9,2 | **exato nos 3** |
| `fundo_social` | 2025: 44.001/8,82 bi; 1T26: 29.094/6,03 bi | **exato** |
| `producao_fisica` | mar/2025: PIM 2,8/4,1/6,0; PMC 6,1/6,7 | **exato** após reingestão |
| `financiamentos_imobiliarios_pf_pj` | PJ concessões 3.555; juros e inadimplência PF e PJ | **5 de 6 exatos** |
| `ticket_medio` | MRV 1T26 −2,6% e acum. 49,16%; 4T25 acum. 53,07% | **exato** (colunas MRV) |
| `balancos_empresas_totais` | vendas 1T26 vs 4T25: +1% | **exato** |

### 4.2 Divergem

> **Padrão recorrente:** boa parte das divergências não é erro nosso — é
> **fonte que revisa depois da publicação**. Já confirmado em FipeZap, PIM/PMC,
> INCC, PNAD e agora CAGED. Nesses casos o número corrente é o correto, e o
> boletim é uma safra congelada. É exatamente o argumento a favor de guardar a
> safra por edição (§9 do MEMORY): sem isso, "bater com o boletim" e "estar
> certo" passam a ser coisas diferentes conforme o tempo passa.


| Gold | Boletim | Nosso | Situação |
|---|---|---|---|
| **`financiamento_pf_faixa`** 🔴 | F1 61.082/8,21 · F2 42.514/7,23 · F3 26.903/6,12 · CM 11.664/3,10 | F1 **30.515/3,68** · F2 **65.727/10,41** · F3 **31.005/6,66** · CM **7.252/1,92** | **Nenhuma faixa bate.** F1 fica na metade e F2 uma vez e meia — padrão que sugere recorte ou mapeamento diferente, não revisão. **Não investigado.** |
| `financiamentos_imobiliarios_pf_pj` 🟡 | PF concessões 22.623 | 25.196 (+11%) | Só a concessão PF; as outras 5 medidas batem. Possível série SGS diferente. |
| `balancos_empresas_totais` 🟡 | lançamentos +6% | +8% | Vendas bate exato; só lançamentos difere 2 p.p. |
| `empregos_caged` 🟢 | saldo 38.316 · estoque 3.063.821 | 37.811 · 3.070.220 | **Divergência esperada — revisão da fonte.** O CAGED é lido do painel Power BI, que é **vivo**: declarações fora do prazo entram depois e revisam meses já fechados. O boletim congelou a safra de ~jun/2026; nós temos a atual (o mesmo mar/26 revisado, mais abr–jun). O estoque subiu (+6.399) e o saldo do mês caiu (−505) — movimentos independentes e normais numa revisão. **A fonte é a referência válida; o boletim é uma foto anterior.** |
| `pnad_rendimento` 🟡 | 2.858 / 3.610 | 2.922 / 3.690 | +2,2% nas duas séries. **Confirmado como rebase de deflator do IBGE** — persiste com dado novo e config correta. |
| `ticket_medio` (INCC) 🟡 | INCC 1T26 tri 1,0% · acum 46,4% | 1,3% · 47,4% | Colunas do INCC; revisão de safra da FGV (mesmo padrão já registrado pelo Codex). |

### 4.3 Não comparáveis por limitação da fonte

| Gold | Motivo |
|---|---|
| `ogu` | O boletim congela jan–mar; nosso gold traz posição corrente. Só compara com congelamento por edição. |
| `icst` | A seção do ICST no PDF sai **ilegível** na extração de texto (sobreposição do PowerPoint). Nosso mar/26 dá 1,73%; o número que consegui ler (0,68%) pode ser de outra linha. Precisa leitura visual. |
| `funding`, `canal_fgts`, `novos_financiamentos_banco`, `fgts_valor_medio` | Não localizei os valores publicados no texto extraído. São manuais e/ou defasados; exigem leitura visual do PDF. |

## 5. Cobertura do gabarito

**O gabarito cobre 8 dos 27 golds.** Ficam sem nenhuma checagem automatizada:

`balancos_empresas_totais`, `canal_fgts`, `credito_pib`, `empregos_caged`,
`fgts_valor_medio`, `financiamento_pf_faixa`, `financiamentos_imobiliarios_pf_pj`,
`financiamentos_instituicao`, `funding`, `fundo_social`, `icst`,
`novos_financiamentos_banco`, `ogu`, `pib_construcao_civil`, `pnad_ocupados`,
`pnad_rendimento`, `producao_fisica`, `saldo_poupanca`, `ticket_medio`.

**Por edição**, a cobertura é desigual: o **3T2025 tem poucas checagens e todas
transcritas visualmente**, porque aquele PDF não tem camada de texto
(`pdftotext` devolve 2,4 KB contra ~41 KB dos outros dois).

---

## 6. Ordem sugerida

1. **`financiamento_pf_faixa`** — única divergência grave ainda sem explicação
   (nenhuma das 4 faixas bate; F1 na metade, F2 uma vez e meia). Suspeita a
   testar: o recorte do boletim pode não ser só a Base PF do GEAVO, ou o
   mapeamento G1/G2/G3 → Faixa 1/2/3 pode não corresponder ao do boletim.
2. **Estender o gabarito** aos 8 golds da §4.1, que já sabemos que batem — é
   barato e transforma verificação pontual em trava de regressão.
3. **Registrar as divergências esperadas no gabarito** (CAGED, PNAD
   rendimento, INCC do ticket médio) com a medição e o motivo, para não serem
   reinvestigadas.
4. **Leitura visual** dos indicadores que o PDF não entrega por texto: ICST,
   funding, canal FGTS, novos financiamentos por banco, valor médio FGTS.
5. **Concessões PF do BACEN** (+11%) — conferir se o boletim usa outra série
   do SGS.
6. **Aposentar `novos_financiamentos_banco`** em favor do automatizado.
7. **`ogu`** — depende do congelamento por edição.
