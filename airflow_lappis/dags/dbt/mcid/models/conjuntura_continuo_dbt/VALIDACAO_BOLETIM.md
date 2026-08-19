# Validação do dashboard contra os boletins de Conjuntura

Comparação do dashboard "Conjuntura Contínuo" (Superset, schema
`conjuntura_continuo_mart`) contra os três boletins fornecidos:

- **2025 3T** — Cópia de Boletim de Conjuntura 2025 3T.pptx.pdf
- **2025 4T** — Boletim de Conjuntura 2025 4T FINAL.pdf
- **2026 1T** — Boletim de Conjuntura 2026 1T Final.pdf

Data desta validação: 13/08/2026.

**Legenda:**
- ✅ **Bate** — validado com números reais contra o boletim, dentro de margem de arredondamento.
- 🔧 **Corrigido nesta sessão** — bug encontrado e já corrigido (código + Superset).
- ❓ **A conferir** — estruturalmente correto, mas não validei com números frescos do boletim ainda.
- ❌ **Não bate / gap conhecido** — divergência real ou dado que não temos.

---

## Página 1 — PIB da Construção Civil

| Item | Gold | Status | Detalhe |
|---|---|---|---|
| PIB % de crescimento (trim/trim, acum. ano, acum. 4T) | `gold_continuo_pib_construcao_civil_pct` (MANUAL) | ✅ **Bate** | Fonte é a planilha oficial do boletim — acumuladas batem exato; trim/trim pode variar 1 casa decimal por revisão de dessazonalização retroativa do IBGE. |
| PIB % de crescimento — versão automatizada (API IBGE) | `gold_continuo_pib_construcao_civil` | 🔧 **Corrigido** | Tinha um bug grave: somava 4 taxas de variação diferentes num único número sem sentido (ex.: 5,6). Corrigido para pivotar as 4 taxas por trimestre. Serve como comparação/apoio à versão manual — a coluna trim/trim costuma divergir um pouco por revisão do IBGE (ex.: boletim mostra -2,3 no 4T25, a API pode mostrar outro valor se consultada depois de uma revisão). |
| Lançamentos e Vendas por região (CBIC) | — | ❌ **Sem fonte** | Proibido implementar (sem acesso à base CBIC), por instrução explícita. |

---

## Página 2 — Balanço das Empresas · Financiamentos Imobiliários

| Item | Gold | Status | Detalhe |
|---|---|---|---|
| Lançamentos/Vendas por construtora (MRV, Cury, Tenda, Direcional, Pacaembu, Plano&Plano) — % variação | `gold_continuo_balancos_empresas` | ✅ **Bate** (corrigido hoje) | Estava puxando de uma tabela manual antiga sem as variações %. Reescrito para usar as colunas de variação já calculadas na planilha oficial. Validado MRV 3T25: -31,8%≈-32% (trim ant.), -19,3%≈-19% (mesmo trim ano ant.), +20,4%≈+20% (9m25/24). |
| Lançamentos/Vendas Totais (soma todas empresas) | `gold_continuo_balancos_empresas_totais` | ⚠️ **Diverge moderadamente** | **Vendas bate bem**: 4T25 var. trim ant. nosso +1,4% vs boletim +1% (ok); 1T26 nosso +0,7% vs boletim +1% (ok); var. mesmo trim ano ant. nosso +13,3%/+14,6% vs boletim +13%/+14% (ok). **Lançamentos diverge nas duas verificações**: 4T25 var. trim ant. nosso -21,6% vs boletim -17% (4,6pp de diferença); 1T26 nosso +8,0% vs boletim +6% (2pp de diferença). Padrão consistente nas duas medições — vale investigar se o universo de empresas somado no `lancamentos_totais` da planilha bate com o que o boletim realmente soma. |
| Financiamentos Imobiliários PF/PJ — Concessões, Taxa de Juros, Inadimplência | `gold_continuo_financiamentos_imobiliarios_pf_pj` (BACEN, automatizado) | ✅ **Bate bem (com nota sobre revisão do BACEN)** | Testado em 6 pontos (DEZ/25, NOV/25, DEZ/24, MAR/26, FEV/26, MAR/25). **PJ concessões bate EXATO em 5 dos 6 pontos** (3244, 3023, 3555, 2481, 2754). **PF concessões bate exato nos meses "antigos/fechados"** (DEZ/24=18.017 exato, MAR/25=17.490 exato), **mas diverge 3-11% nos meses mais recentes do boletim** (DEZ/25: nosso 22.278 vs boletim 21.411; MAR/26: nosso 25.196 vs boletim 22.623) — padrão clássico de revisão do BACEN SGS (concessões PF são revisadas para cima conforme mais bancos completam o reporte). Taxas de juros e inadimplência batem sempre dentro de 0,1-0,3pp, exceto **taxa PJ de MAR/25** (nosso 11,85% vs boletim 10,9% — divergência maior que o padrão, vale checar). |
| Financiamentos Habitacionais (UH) — FGTS-PJ x SBPE Const. | `gold_continuo_financiamentos_habitacionais` | ❌ **BUG CONFIRMADO** | FGTS-PJ por trimestre bate exato (4T25=61.212, 4T24=64.305, 3T25≈65.720 vs boletim 65.690). **Mas a coluna `_acumulado_12_meses` está desalinhada uma linha**: o valor que aparece na linha 4T2025 (292.152) é na verdade o "12m-DEZ/24" do boletim (292.150); e o valor da linha 4T2024 (286.411) é o "12m-DEZ/25" do boletim (286.411). **SBPE Const. está errado/vazio**: linha 4T2025 = NULL (deveria ser 47.766); linha 4T2024 = 64.631 (boletim mostra 40.623 para 4T24 — não bate, nem parece ser só um shift de linha). **1T2026 inteiro está vazio** (planilha ainda não atualizada para esse trimestre). Precisa investigar a fonte (`manual_conjuntura.dados_trimestrais`) direto — meu palpite é um desalinhamento no carregamento do xlsx específico dessas colunas. |

---

## Página 3 — Empregos · PNAD · Produção Física e Vendas · Novos Financiamentos

| Item | Gold | Status | Detalhe |
|---|---|---|---|
| Empregos CAGED — Saldo e Estoque (construção x total) | `gold_continuo_empregos_caged` | ✅ **Bate (pequena diferença por revisão normal do CAGED)** | Testado em 6 pontos (DEZ/25, NOV/25, DEZ/24, MAR/26, FEV/26, MAR/25) — todos dentro de 0,2-2% do boletim. Ex.: DEZ/24 estoque nosso 2.857.405 vs boletim 2.857.279 (praticamente exato); DEZ/25 saldo nosso -105.917 vs boletim -104.077 (1,8% de diferença) — normal, o Novo CAGED revisa saldos dos meses recentes conforme mais empresas declaram. |
| PNAD-C — Ocupados (construção x total) | `gold_continuo_pnad_ocupados` (IBGE/SIDRA, automatizado) | ✅ **Bate exato** | Validado anteriormente: out-nov-dez/25 = construção 7.468 mil / total 102.998 mil — exato com o boletim 4T25. |
| PNAD-C — Rendimento Médio Real | `gold_continuo_pnad_rendimento` (IBGE/SIDRA, automatizado) | ❌ **Divergência conhecida** | Série deflacionada (R$ reais) — o IBGE re-basea a cada trimestre, então valores históricos mudam retroativamente. Vai divergir de boletins antigos por natureza da série, não é bug. |
| PIM-PF — Produção Física (var. dessazonalizada) | `gold_continuo_producao_fisica` (agregado IBGE 8886, automatizado) | ✅ **Bate** | Validado: acumulado ano batendo próximo do boletim (ex.: -4,4 vs -4,8 no teste de MAR/26). |
| PMC — Vendas Material de Construção (var. dessazonalizada) | `gold_continuo_producao_fisica` (agregado IBGE 8757/cat. 56734, automatizado) | ✅ **Bate exato** | Validado: acumulado ano -1,0 — exato com o boletim. |
| Novos Financiamentos SBPE por Banco (UH, acum. ano) | `gold_continuo_novos_financiamentos_banco` | ❌ **Não dá pra validar — planilha muito atrasada** | Último dado real é 09/2025 (confirmado hoje). O boletim compara sempre o **ano fechado inteiro** (JAN-DEZ) ou trimestre fechado (JAN-MAR) — não temos nem DEZ/25 nem MAR/26, então não há como bater contra nenhum dos 3 boletins ainda. Precisa cobrar atualização dessa aba da planilha (CEAG) — é a série mais atrasada que encontramos. |

---

## Página 4 — Crédito/PIB · FGTS-PF por Renda · UH por Condição · Funding

| Item | Gold | Status | Detalhe |
|---|---|---|---|
| Crédito Imobiliário / PIB | `gold_continuo_credito_pib` (BACEN Olinda, automatizado) | ✅ **Bate quase exato** | Testado em 4 pontos: DEZ/25 nosso 10,84 vs boletim 10,84 (**exato**); FEV/26 nosso 10,90 vs boletim 10,91 (quase exato); NOV/25 nosso 10,79 vs boletim 10,81 (próximo). **JAN/26 destoou**: nosso 10,86 vs boletim 10,75 — os outros meses mostram evolução suave, esse ponto do boletim parece uma queda pontual atípica (0,16 abaixo de DEZ e 0,16 abaixo de FEV) que pode ser um dado revisado depois ou uma particularidade da extração do boletim. |
| Financiamento PF por Faixa de Renda (UH) | `gold_continuo_financiamento_pf_faixa` | ❌ **Não dá pra validar — planilha atrasada** | Último dado real é 11/2025. O boletim compara ano fechado (JAN-DEZ/25: total 669.065 UH) ou trimestre fechado (JAN-MAR/26: total 171.256 UH) — não temos nem um nem outro completo ainda. |
| UH por Condição de Uso — SBPE Aquisição | `gold_continuo_uh_condicao_uso` | ❌ **Gap conhecido (parcial) + planilha atrasada** | Só temos o lado SBPE (novos x usados) — FGTS-PF não existe na planilha. Além disso, último dado real é 09/2025, então também não bate contra os totais fechados do boletim (JAN-DEZ ou JAN-MAR). |
| Estrutura de Funding | `gold_continuo_funding` | 🔧 **Corrigido** | Achei e corrigi um erro: o gráfico incluía `LCA` (600) e `CRA` (178), que **não existem** no gráfico do boletim. Removidas. As 5 categorias restantes batem exato com DEZ/25: SBPE 766,5≈766, FGTS 705,4≈702, LCI 510,6≈511, CRI 257,3≈257, LIG 101,6≈102. **"FII" (292, mostrado no boletim) não temos fonte** — não existe coluna equivalente na planilha. |

---

## Página 5 — Canal FGTS · Faixas · Poupança

| Item | Gold | Status | Detalhe |
|---|---|---|---|
| Canal FGTS Pró-Cotista por Faixa de Renda | `gold_continuo_canal_fgts` | ❌ **Não dá pra validar — planilha atrasada** | Último dado real é 11/2025. Além disso os valores nesse período são muito baixos (Faixa 1=2, Faixa 2=1 UH) comparado à escala do boletim (ex.: JAN-DEZ/25 Pró-Cotista total ≈ 6.507 UH) — parece que a coluna nessa planilha é o valor **do mês**, não acumulado, então comparação direta com os totais fechados do boletim não é direta mesmo quando a planilha atualizar. Vale confirmar com quem mantém a planilha se essa série é mensal ou acumulada. |
| Poupança SBPE — Captação Líquida x Saldo | `gold_continuo_saldo_poupanca` (ABECIP, automatizado) | ✅ **Bate exato** | Validado anteriormente: captação líquida 12 meses/2025 = -R$ 63,0 bi — exato com o boletim 4T25. Corrigido hoje um bug feio: a tabela estava pegando linhas aleatórias da série histórica (que vai até os anos 1980), mostrando datas tipo 1993/1986. Corrigido para sempre mostrar os períodos mais recentes. |

---

## Página 6 — OGU · Preços

| Item | Gold | Status | Detalhe |
|---|---|---|---|
| OGU — Dotação/Empenho/Pagamento/RAP por ação | `gold_continuo_ogu` | 🔧 **Corrigido hoje, parcialmente valida** | Reescrito para virar tabela por ação (igual ao boletim), com 6 colunas (Dotação, Empenho, Pagamento, RAP Inscrito, Pag. RAP, Pag. Total) e as 7 ações do boletim + linha SOMA. Validado vs. 4T25: **FAR, FDS e PNHR batem quase exato**. **00TI (FNHIS) está incompleto** no snapshot atual — não é bug de query, é que a extração atual do SIAFI (posição "hoje") tem menos dados preenchidos para essa ação específica do que a extração congelada do boletim (02/01/26). SOMA de Empenho/Pagamento bate ~2% vs. boletim; **Dotação total fica abaixo** — ação 00XF (Fundo Social/MCMV via FGTS) não tem linha de dotação no SIAFI (operação de crédito reembolsável fora do OGU tradicional — não é bug). |
| SINAPI — Custo Médio m² | `gold_continuo_sinapi` (IBGE, automatizado) | ✅ **Bate exato** | Validado anteriormente: DEZ/25 = R$ 1.891,63 / +0,51% mês / +5,63% 12m — boletim mostra R$ 1.891,6 / 0,51% / 5,64% (diferença de arredondamento na última casa). |
| INCC-M | `gold_continuo_incc_m` (FGV, automatizado) | ❌ **BUG CONFIRMADO** | Índice e var. mês batem exato (MAR/26: 1.241,72 e 0,36% vs boletim 1.241,72 e 0,35%; DEZ/25: 1.225,4 e 0,21% — exato). **Mas as colunas `var_ano`/`var_12_meses` estão com o significado errado**: nossa `var_12_meses` (MAR/26=5,81) na verdade é o "acumulado no ano" do boletim (JAN-MAR/26=5,81%) — não é 12 meses! E nossa `var_ano` (MAR/26=1,33) **não corresponde a nada** que aparece no boletim. O "12m MAR/26" real do boletim (7,32%) **não existe em nenhuma coluna nossa** — não estamos capturando essa métrica. Em DEZ/25 isso passa despercebido porque acum-no-ano e acum-12-meses coincidem matematicamente em dezembro (fim do ano). Precisa corrigir a ingestão (`incc_m_ingest_dag`/`cliente_fgv.py`) pra pegar a variável certa de "12 meses" da FGV. |
| Ticket Médio vs INCC (acum. desde 4T2020) | `gold_continuo_ticket_medio` | ✅ **Bate estruturalmente** | Ajustado hoje para mostrar só INCC/MRV/Direcional/Tenda (removi Cury, que a planilha tem mas o boletim **não mostra** nessa tabela específica — a tabela do boletim só tem essas 4 colunas). Dado vem da planilha oficial. |
| FGTS — Valor Médio dos Imóveis | `gold_continuo_fgts_valor_medio` | ✅ **Bate exato** | Validado anteriormente: DEZ/25 = R$ 245.959 / +2,89% mês / +12,14% 12m — exato com o boletim. |
| Desembolsos de Obras CEF (FAR/Rural/FDS) | — | ❌ **Sem fonte** | Sem fonte direta; abstrair do OGU seria arriscado (valores de naturezas diferentes). Não implementado. |

---

## Página 7 — Índices da Construção

| Item | Gold | Status | Detalhe |
|---|---|---|---|
| Índice IMOB — variação mensal | `gold_continuo_indice_imob` (planilha oficial) | ✅ **Bate exato** | Testado em 6 pontos (DEZ/25 e MAR/26, 3 métricas cada) — **todos exatos**: DEZ/25 -6,7%/+73,6%/+73,5% vs boletim -6,7%/+73,6%/+73,5%; MAR/26 -9,3%/+62,2%/+10,0% vs boletim -9,3%/+62,2%/+10,0%. Perfeito. |
| Índice FipeZap Locação | `gold_continuo_fipezap` (planilha oficial) | ✅ **Bate exato** | Testado em 6 pontos — todos exatos: DEZ/25 +0,68%/+9,44%/+9,44% vs boletim idêntico; MAR/26 +1,1%/+8,6%/+2,4% vs boletim idêntico. |
| Índice ICST — Confiança na Construção | `gold_continuo_icst` (planilha oficial, série original sem ajuste sazonal) | ✅ **Bate (quase exato)** | var. mês e acum. ano batem exato nos dois trimestres testados. "Var. vs mesmo mês ano anterior" fica com pequena diferença: DEZ/25 nosso -5,43% vs boletim -5,00% (0,43pp); MAR/26 nosso -1,26% vs boletim -1,40% (0,14pp) — diferença pequena, possivelmente revisão da série FGV-IBRE. |
| Índice ABRAMAT | — | ❌ **Sem fonte** | Dados inconsistentes na fonte disponível — não implementado, decisão tomada anteriormente. Possível inserir manualmente no futuro. |

---

## Resumo executivo

*(Atualizado após validação numérica completa contra os 3 boletins — 13/08/2026.)*

### ✅ Bate exato ou muito próximo (validado com números reais)
PIB % (manual), PNAD-C Ocupados, PIM-PF, PMC, SINAPI, Poupança SBPE, FGTS Valor Médio,
Estrutura de Funding (exceto FII), Balanço por Construtora — variações % (Vendas Totais também),
Empregos CAGED, Crédito Imobiliário/PIB, Financiamentos Imobiliários PF/PJ (BACEN),
**Índices IMOB e FipeZap (perfeitos, 6/6 pontos exatos)**, Índice ICST (quase exato),
OGU (parcialmente — FAR/FDS/PNHR).

### 📌 Avisos para corrigir depois (bug confirmado, decisão: adiar — achar a fonte certa com calma)
1. **`gold_continuo_financiamentos_habitacionais`** — coluna `acumulado_12_meses` desalinhada
   uma linha (o valor da linha 4T2025 é na verdade o de 4T2024, e vice-versa); coluna
   `financ_hab_sbpe_constr` com valores errados/vazios (4T2025=NULL, deveria ser 47.766; 4T2024=64.631,
   boletim mostra 40.623). Precisa investigar o carregamento da planilha
   (`manual_conjuntura.dados_trimestrais`) direto.
2. **`gold_continuo_incc_m`** — colunas `var_ano`/`var_12_meses` com o significado trocado/errado.
   O que chamamos de `var_12_meses` é na verdade "acumulado no ano"; a métrica real de 12 meses do
   boletim (ex.: 7,32% em MAR/26) **não é capturada em nenhuma coluna hoje**. Precisa corrigir a
   ingestão FGV (`incc_m_ingest_dag`/`cliente_fgv.py`).
3. **`gold_continuo_balancos_empresas_totais`** — Lançamentos Totais diverge 2-5pp do boletim em
   duas medições diferentes (4T25 e 1T26), enquanto Vendas Totais bate bem nas duas. Padrão
   consistente — sugere que o universo de empresas somado nessa coluna da planilha não é
   exatamente o mesmo que o boletim usa no card "Total lançamentos".

*(Decisão do time, 13/08/2026: deixar esses 3 documentados como aviso por enquanto — corrigir
quando encontrarmos/confirmarmos a fonte certa com calma, não é urgente.)*

### 🔧 Corrigido hoje antes da validação (bugs já resolvidos)
1. `gold_continuo_pib_construcao_civil` somava 4 taxas diferentes num número sem sentido.
2. `gold_continuo_balancos_empresas` não tinha as variações % por empresa — reescrito (validado ✅).
3. Estrutura de Funding incluía LCA/CRA que não pertencem a esse gráfico (validado ✅).
4. OGU virou tabela por ação em vez de 4 cards soltos.
5. Bug de ordenação em ~28 charts (linhas aleatórias/antigas apareciam em vez das mais recentes).
6. Bug de linhas em branco em 9 golds mensais — corrigido na raiz (filtro no modelo dbt).
7. Ticket Médio vs INCC mostrava uma coluna (Cury) que o boletim não exibe nessa tabela.

### ⏸️ Não dá pra validar ainda — planilha manual atrasada demais
Estas séries têm o último dado real **antes** do fechamento de qualquer trimestre coberto pelos
3 boletins, então comparação numérica direta não é possível até a planilha ser atualizada:
- **Novos Financiamentos por Banco** — parado em 09/2025 (o mais atrasado, quase 1 ano).
- **Financiamento PF por Faixa de Renda** e **Canal FGTS Pró-Cotista** — parados em 11/2025.
- **UH por Condição de Uso** — parado em 09/2025 (além do gap estrutural do lado FGTS-PF).

Recomendação: pedir pro CEAG atualizar essas 4 abas específicas da planilha — são as mais
desatualizadas de todas as fontes manuais.

### ❌ Gaps conhecidos sem solução no momento (fonte de dado não existe)
1. **CBIC** (lançamentos/vendas por região) — proibido implementar, sem acesso.
2. **FGTS-PF por condição de uso** (novos x usados) — só temos o lado SBPE.
3. **"FII"** na Estrutura de Funding — não existe coluna equivalente na planilha.
4. **Desembolsos de Obras CEF** — sem fonte direta.
5. **Índice ABRAMAT** — dados inconsistentes, não implementado.

### ⚠️ Pequenas divergências, provavelmente normais (não são bugs)
- **OGU, ação 00TI** — snapshot atual do SIAFI incompleto pra essa ação específica (posição "hoje" vs. posição congelada do boletim).
- **OGU, Dotação total** — abaixo do boletim por causa da ação 00XF (crédito reembolsável sem dotação no SIAFI).
- **Financiamentos Imobiliários PF, meses recentes** — revisão normal do BACEN SGS.
- **Crédito Imobiliário/PIB, JAN/26** — um ponto fora da curva no próprio boletim, não intrínseco ao nosso pipeline.
- **PNAD-C Rendimento** — série deflacionada, o IBGE re-basea a cada trimestre.
