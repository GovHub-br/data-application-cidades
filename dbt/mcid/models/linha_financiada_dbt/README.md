# Linha Financiada

Produto único que integra Financiada geral, Classe Média, MCMV Cidades e
Pró-Moradia. Os recortes não são excludentes: um contrato pode ser Classe
Média e receber aporte do MCMV Cidades.

## Regra para apoio à produção PJ

Registros PJ representam apoio à produção e não são contabilizados como
contratação do MCMV. A contratação é medida no contrato PF. Um indicador de
empreendimento/entrega só pode ser publicado quando a chave de operação PF
estiver comprovadamente vinculada ao APF PJ; a data de término da obra é
referência prospectiva de entrega, nunca data de contratação. Na carga atual,
`Operação` PF e `nu_apf` PJ não conciliam diretamente; por isso os dados PJ
permanecem fora dos totais publicados até o recebimento do de-para validado.

## Perguntas atendidas

- contratação mensal por FGTS e Fundo Social;
- orçamento oneroso e descontos, respeitando a periodicidade das fontes;
- contrapartidas conhecidas e contratos beneficiados, com lacuna explícita;
- base agregada automatizada;
- consolidação automatizada da Base PF/FGTS com Fundo Social, sem dupla
  contagem com o recorte oficial CCI/CCA;
- relatório semanal para BI;
- painel e mapa de empreendimentos;
- atributos temporais para análise preditiva.

## Limitações conhecidas

1. Não existe base institucional completa de contrapartidas. Os modelos usam
   somente `vlrcontrapartidaparceria` e aportes do MCMV Cidades recebidos.
2. O orçamento FGTS disponível é anual (2021–2024), não uma execução mensal.
3. A posição do Fundo Social possui execução financeira, mas ainda não forma
   série mensal longa para previsão robusta.
4. A Gold de features prepara dados para IA; ela não publica previsões sem
   treino, backtest temporal e métricas de erro documentadas.
5. A Base PF/FGTS e CCI/CCA são recortes diferentes. A primeira alimenta a
   consolidação PF + Fundo Social; CCI/CCA permanece como fonte dos indicadores
   oficiais de contratação para não somar contratos potencialmente sobrepostos.

## Fontes principais

- GEAVO CCI/CCA;
- Base PF e Base PJ do FGTS;
- GEFUS/Fundo Social;
- MCMV Cidades;
- contratos, domínios e empreendimentos do Canal FGTS;
- orçamento final FGTS e execução da ação 00XF.
