# Issue #130 - Checklist de Validacao de Negocio (Fase 5)

## Resumo

Checklist para levar a area responsavel pelo MCMV, consolidando as 19 decisoes de
negocio pendentes identificadas na Fase 0-1 da issue #130 (dicionario de indicadores
e matriz indicador x fonte x campo x regra). Cada item e uma decisao a ser tomada ou
confirmada; a resposta define parametros, regras e limiares dos indicadores do
reloginho e de gargalo/desempenho.

## Como usar

- Cada item abaixo e uma decisao de negocio. Preencha os campos "Resposta" e
  "Responsavel" para cada item.
- Os itens do Bloco 3 (12 a 18) sao CONFIRMACAO de limiar: os valores ja estao
  implementados em SQL no modelo `indicadores_gargalo_desempenho`; a area deve
  confirmar se o valor atual esta correto ou informar o valor oficial.
- "Resposta esperada" indica o formato minimo da resposta (ex.: valor numerico +
  documento/fonte).
- Ao final, devolva o checklist preenchido para consolidacao e materializacao dos
  parametros na modelagem.

## Bloco 1 - Metas (decisao de negocio)

- [ ]

### 1. Meta oficial total de UHs do ciclo MCMV 2023-2026

**Pergunta:** Qual o valor numerico oficial da meta total de unidades habitacionais
do ciclo MCMV 2023-2026 e qual o documento/fonte oficial que o define?

**Contexto:** Nao ha meta no MinIO (varredura nao encontrou arquivos com
meta/ciclo/retomada). Existe apenas uma "meta visual" de 2.214.810 UHs usada como
referencia de progresso nos docs.

**Resposta esperada:** valor numerico + documento/fonte oficial.

**Indicadores impactados:** uh_meta_total; perc_meta_contratada; perc_meta_entregue;
gap_uh_meta; ritmo_necessario; projecao_entrega.

**Resposta:** `(preencher)`
**Responsavel:** `(preencher)`

- [ ]

### 2. Meta por frente/programa/ano

**Pergunta:** Existe tabela oficial de metas por frente (FAR, Entidades/FDS, Rural,
FNHIS, Reforma, Faixa 3/FGTS, Faixa 2/SBPE) e por ano? Se sim, fornecer.

**Contexto:** A recomendacao tecnica (issue-66) e criar uma tabela pequena de metas
oficiais por ciclo, faixa, frente e ano; hoje so existe a meta visual agregada.

**Resposta esperada:** tabela oficial ou "nao existe".

**Indicadores impactados:** visao por frente do reloginho.

**Resposta:** `(preencher)`
**Responsavel:** `(preencher)`

- [ ]

### 3. Regra FNHIS/SUB50

**Pergunta:** Para a frente FNHIS/SUB50, o ponteiro do reloginho deve ser proposta
apresentada, proposta selecionada, contrato ou UH? Qual a regra de conversao para UH?

**Contexto:** A fonte existe (`raw/novo_mcmv_fnhis_sub_50_propostas_apresentadas.csv`
e `..._propostas_selecionadas.csv`), mas a granularidade e de proposta; a regra de
conversao para UH esta pendente.

**Resposta esperada:** escolha do ponteiro + regra de conversao para UH.

**Indicadores impactados:** meta/progresso da frente FNHIS.

**Resposta:** `(preencher)`
**Responsavel:** `(preencher)`

- [ ]

### 4. Campo oficial de valor financiado

**Pergunta:** Qual e o campo oficial de valor financiado: `vr_evento`,
`vr_investimento` ou outro?

**Contexto:** As bases de Faixa 3/FGTS e Reforma apresentam os dois campos com
totais diferentes; a decisao define a meta de valor dessas frentes.

**Resposta esperada:** campo oficial (vr_evento; vr_investimento; outro).

**Indicadores impactados:** meta de valor de Reforma e Faixa 3.

**Resposta:** `(preencher)`
**Responsavel:** `(preencher)`

- [ ]

### 5. Total de entregas oficial

**Pergunta:** Qual e o caminho oficial para o total de UHs entregues: 1.543.432
(bases mensais CAIXA + BB) ou 1.518.598 (arquivos de entrega por evento)?

**Contexto:** Os dois caminhos produzem totais diferentes no snapshot 30/06/2026; a
regra oficial de qual caminho usar esta pendente.

**Resposta esperada:** caminho oficial (1.543.432 ou 1.518.598).

**Indicadores impactados:** uh_entregues.

**Resposta:** `(preencher)`
**Responsavel:** `(preencher)`

- [ ]

### 6. Equivalencia contratos/valor para UH

**Pergunta:** Para Faixa 3/FGTS e Reforma, a meta fica em UH ou em contratos/valor?
Se em UH, qual a regra de equivalencia?

**Contexto:** A recomendacao da issue-66 e usar contratos e valor financiado (nao
UH) ate haver regra oficial de equivalencia para unidade habitacional.

**Resposta esperada:** unidade da meta (UH ou contratos/valor) + regra de
equivalencia.

**Indicadores impactados:** reloginho das frentes financiadas.

**Resposta:** `(preencher)`
**Responsavel:** `(preencher)`

- [ ]

### 7. Normalizacao de rotulos

**Pergunta:** Confirmar a padronizacao dos rotulos `RURAL`/`Rural` e dos nomes de
faixa `001`/`002`/`003`?

**Contexto:** Na base mensal a frente Rural aparece com dois rotulos (`RURAL` e
`Rural`), que devem ser normalizados antes da gold para evitar duplicidade visual e
de contagem.

**Resposta esperada:** confirmacao da padronizacao.

**Indicadores impactados:** deduplicacao e agregacao por frente.

**Resposta:** `(preencher)`
**Responsavel:** `(preencher)`

## Bloco 2 - Regras do reloginho a definir

- [ ]

### 8. Janela do ritmo_medio_mensal

**Pergunta:** Quais meses entram no denominador de "entregas acumuladas / meses
observados"? (desde o inicio do ciclo, desde a primeira entrega, ou janela movel?)

**Contexto:** `ritmo_medio_mensal` e definido como entregas acumuladas divididas
pelos meses observados; a janela de observacao nao esta definida.

**Resposta esperada:** definicao da janela (inicio do ciclo; 1a entrega; janela
movel).

**Indicadores impactados:** ritmo_medio_mensal.

**Resposta:** `(preencher)`
**Responsavel:** `(preencher)`

- [ ]

### 9. Janela do ritmo recente (projecao_entrega)

**Pergunta:** Qual a janela do "ritmo recente" usado na projecao de entrega? (3
meses, 6 meses, media movel?)

**Contexto:** `projecao_entrega` usa entregas observadas mais o ritmo recente
projetado; a janela do ritmo recente nao esta definida.

**Resposta esperada:** janela do ritmo recente (3 meses; 6 meses; media movel).

**Indicadores impactados:** projecao_entrega.

**Resposta:** `(preencher)`
**Responsavel:** `(preencher)`

- [ ]

### 10. Fim do ciclo / meses restantes

**Pergunta:** Qual a data (mes/ano) de termino do ciclo 2023-2026, para calcular os
"meses restantes"?

**Contexto:** `ritmo_necessario` (unidades restantes / meses restantes) e
`projecao_entrega` dependem da data de fim do ciclo; a data nao esta definida.

**Resposta esperada:** data mes/ano de termino do ciclo.

**Indicadores impactados:** ritmo_necessario; projecao_entrega.

**Resposta:** `(preencher)`
**Responsavel:** `(preencher)`

- [ ]

### 11. Faixas de corte do status_relogio

**Pergunta:** Quais limiares definem No prazo / Atencao / Risco? (ex.: percentual da
meta, gap, ritmo)

**Contexto:** `status_relogio` tem valores possiveis No prazo, Atencao e Risco, mas
as faixas de corte nao estao definidas nos docs.

**Resposta esperada:** limiares de corte (percentual da meta; gap; ritmo).

**Indicadores impactados:** status_relogio.

**Resposta:** `(preencher)`
**Responsavel:** `(preencher)`

## Bloco 3 - Limiares do gargalo a validar (ja implementados em SQL)

- [ ]

### 12. Limiar de atualizacao recente (90 dias)

**Pergunta:** O limiar de 90 dias sem liberacao/medicao para `flag_sem_atualizacao_recente`
esta correto? Qual e o valor oficial?

**Contexto:** Regra implementada: obra nao concluida sem atualizacao ha mais de 90
dias ou sem data de atualizacao.

**Resposta esperada:** confirmar 90 dias ou valor oficial.

**Indicadores impactados:** flag_sem_atualizacao_recente.

**Resposta:** `(preencher)`
**Responsavel:** `(preencher)`

- [ ]

### 13. Diferenca minima fisico-financeira (10 p.p.)

**Pergunta:** O limiar de 10 pontos percentuais de diferenca entre execucao fisica e
financeira esta correto? Qual e o valor oficial?

**Contexto:** Regra implementada: baixa execucao fisica e financeira acionadas quando
a diferenca e maior que 10 p.p. (financeira abaixo da fisica, ou fisica abaixo do
previsto).

**Resposta esperada:** confirmar 10 p.p. ou valor oficial.

**Indicadores impactados:** flag_baixa_execucao_fisica; flag_baixa_execucao_financeira.

**Resposta:** `(preencher)`
**Responsavel:** `(preencher)`

- [ ]

### 14. Saldo a desembolsar + execucao fisica (30% + 95%)

**Pergunta:** O limiar de 30% do contrato ainda nao desembolsado combinado com
execucao fisica abaixo de 95% esta correto? Quais sao os valores oficiais?

**Contexto:** Regra implementada: `flag_gargalo_financeiro` acionada quando pelo
menos 30% do contrato ainda nao foi desembolsado e a execucao fisica esta abaixo de
95%.

**Resposta esperada:** confirmar 30% saldo + 95% fisico ou valores oficiais.

**Indicadores impactados:** flag_gargalo_financeiro.

**Resposta:** `(preencher)`
**Responsavel:** `(preencher)`

- [ ]

### 15. Contrato antigo abaixo de 30% (365 dias)

**Pergunta:** O limiar de 365 dias de contrato com execucao abaixo de 30% esta
correto? Qual e o valor oficial?

**Contexto:** Regra implementada: baixa execucao fisica e financeira acionadas quando
o contrato tem mais de 365 dias e execucao abaixo de 30%.

**Resposta esperada:** confirmar 365 dias + 30% ou valores oficiais.

**Indicadores impactados:** flag_baixa_execucao_fisica; flag_baixa_execucao_financeira.

**Resposta:** `(preencher)`
**Responsavel:** `(preencher)`

- [ ]

### 16. Contrato sem evolucao (180 dias)

**Pergunta:** O limiar de 180 dias sem execucao fisica nem financeira para
`flag_contrato_sem_evolucao` esta correto? Qual e o valor oficial?

**Contexto:** Regra implementada: contrato com mais de 180 dias sem execucao fisica
nem financeira.

**Resposta esperada:** confirmar 180 dias ou valor oficial.

**Indicadores impactados:** flag_contrato_sem_evolucao.

**Resposta:** `(preencher)`
**Responsavel:** `(preencher)`

- [ ]

### 17. Pesos do score de gargalo

**Pergunta:** Os pesos atuais do score estao corretos? Quais sao os pesos oficiais?

**Contexto:** Regra implementada: atraso 2, paralisacao 2, sem atualizacao 1, baixa
execucao fisica 1, baixa execucao financeira 1, gargalo financeiro 1, contrato sem
evolucao 1.

**Resposta esperada:** confirmar pesos ou pesos oficiais.

**Indicadores impactados:** score_gargalo; classificacao_gargalo.

**Resposta:** `(preencher)`
**Responsavel:** `(preencher)`

- [ ]

### 18. Faixas de classificacao do score

**Pergunta:** As faixas de classificacao atuais estao corretas? Quais sao as faixas
oficiais?

**Contexto:** Regra implementada: Baixo = 0, Medio = 1-2, Alto = 3-4, Critico = >= 5.

**Resposta esperada:** confirmar faixas ou faixas oficiais.

**Indicadores impactados:** classificacao_gargalo.

**Resposta:** `(preencher)`
**Responsavel:** `(preencher)`

## Bloco 4 - Outras decisoes

- [ ]

### 19. Separacao de dashboards

**Pergunta:** Confirmar a separacao entre "relogio executivo" (metas) e "mesa
operacional de alertas" (gargalo)?

**Contexto:** A recomendacao da issue-66 e separar os dois paineis: relogio executivo
de metas e mesa operacional de alertas.

**Resposta esperada:** confirmar separacao sim/nao.

**Indicadores impactados:** status_relogio; indicadores de gargalo (apresentacao).

**Resposta:** `(preencher)`
**Responsavel:** `(preencher)`
