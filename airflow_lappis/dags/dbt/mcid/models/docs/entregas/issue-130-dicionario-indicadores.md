# Issue #130 - Dicionario de Indicadores do Reloginho (Fase 0-1)

## Resumo

Este documento consolida os indicadores do reloginho (componente de acompanhamento
temporal de metas do MCMV) e os indicadores/flags de gargalo e desempenho, com base
nos entregaveis das issues #66, #118 e #119 (trabalho do juan-ricarte). E a Fase 0-1
da issue #130: dicionario de indicadores + matriz indicador x fonte x campo x regra.

Pontos-chave:

- Grupo A (reloginho): 10 indicadores DOCUMENTADOS e NAO materializados em gold/mart.
- Grupo B (gargalo/desempenho): 9 itens JA IMPLEMENTADOS em
  `mcmv_indicadores.indicadores_gargalo_desempenho`, com regras, pesos do score
  (atraso 2, paralisacao 2, sem atualizacao 1, baixa execucao fisica 1, baixa
  execucao financeira 1, gargalo financeiro 1, contrato sem evolucao 1) e faixas de
  classificacao Baixo/Medio/Alto/Critico.
- Validacao de negocio e a Fase 5 (nao executada); "Responsavel pela validacao" =
  "A definir (area de negocio)" para todos os indicadores.
- Nenhum modelo dbt foi criado ou alterado nesta fase.

## Fonte dos dados

Arquivos-fonte utilizados nesta consolidacao:

- `models/docs/entregas/issue-66-entrega-indicadores-historicos-relogio-alertas.md`
- `models/indicadores_mcmv_dbt/docs/issue-66-indicadores-gargalo-desempenho.md`
- `models/indicadores_mcmv_dbt/docs/issue-66-matriz-frentes-relogio-alertas.md`
- `models/indicadores_mcmv_dbt/docs/issue-66-fontes-minio-relogio-alertas.md`
- `models/indicadores_mcmv_dbt/gold/schema.yml`
- `models/docs/glossario-mcid.md`
- `models/indicadores_mcmv_dbt/docs/evidencias/issue-66-matriz-frentes-dashboard.csv`
- `models/docs/evidencias/issue-119-matriz-glossario-campos.csv`
- `models/mcmv_historico_dbt/piloto/schema.yml`
- `seeds/mcmv_historico/issue_118_mcmv_serie_temporal_piloto.csv`
- `models/docs/entregas/issue-118-entrega-final.md`

## Notas transversais (leitura obrigatoria)

1. **Meta oficial pendente**: a meta de UHs do ciclo (`uh_meta_total`) e decisao de
   negocio PENDENTE. A varredura do MinIO nao encontrou arquivos com `meta`, `ciclo`
   ou `retomada` no nome; a meta deve ser carregada como parametro/tabela de metas
   oficial, nao inferida do MinIO. Nos docs existe apenas uma "meta visual" de
   2.214.810 UHs (MCMV ciclo 2023-2026), usada somente como referencia de progresso.
2. **Serie historica limitada**: o piloto #118 cobre somente UH CONTRATADAS, com
   granularidade anual 2009-2025, separando OGU/Subsidiado e FGTS/Financiado. NAO
   existe serie historica de entregues, nem granularidade mensal, nem granularidade
   territorial. Isso afeta `uh_entregues`, `ritmo_medio_mensal`, `ritmo_necessario` e
   `projecao_entrega`.
3. **Hiato OGU/Subsidiado 2020-2023**: a serie apresenta zeros nesse periodo, que
   devem ser classificados como periodo ausente/incompleto (ausencia real vs dado nao
   coletado) antes de qualquer leitura de tendencia.
4. **Snapshot pontual 30/06/2026**: contratadas 1.874.623, entregues 1.543.432 e
   vigentes 313.884, por APF/UF/municipio/agente financeiro. E um ponto no tempo, nao
   uma serie.
5. **Materializacao**: grupo A documentado e nao materializado em gold/mart; grupo B
   implementado em `indicadores_gargalo_desempenho` (regras e score definidos).
6. **Campos fisicos**: campos preferenciais seguem `glossario-mcid.md` e
   `issue-119-matriz-glossario-campos.csv` (ex.: `quantidade_uh`,
   `quantidade_uh_entregues`, `valor_contratado`, `valor_desembolsado`,
   `percentual_execucao_fisica`, `percentual_execucao_financeira`, `dt_referencia`,
   `apf`, `contrato`, `municipio`, `uf`, `codigo_ibge_municipio`).
7. **Duplicidades**: regra de deduplicacao por APF/contrato citada na issue-66 para
   evitar dupla contagem entre arquivos de contratacao e entrega, onde aplicavel.
8. **Nulos**: para o hiato OGU, classificacao em aberto; para flags booleanas, false
   quando nao acionada; para `uh_entregues` em frentes sem entrega, NULL (ver silver).
   Quando nao ha regra documentada, o campo registra "nao definido".

## Grupo A - Indicadores do Reloginho (10)

### uh_meta_total

| Campo | Valor |
|---|---|
| Nome | uh_meta_total |
| Definicao | Meta oficial de unidades habitacionais do ciclo MCMV 2023-2026. |
| Objetivo | Referencia do reloginho para medir o progresso de contratacao e entrega contra a meta do ciclo. |
| Fonte | Tabela oficial de metas (a definir pela area). Varredura MinIO nao encontrou arquivos com meta/ciclo/retomada; a meta deve ser carregada como parametro/tabela oficial, nao inferida do MinIO. |
| Tabelas e campos utilizados | Nao aplicavel ainda (pendente). Referencia visual usada nos docs: 2.214.810 UHs (ciclo 2023-2026). |
| Regra de calculo | Pendente: decisao de negocio para o valor por ciclo/programa/frente/ano. Valor de referencia visual: 2.214.810 UHs. |
| Granularidade temporal | Ciclo (2023-2026), conforme tabela oficial de metas. |
| Granularidade territorial | Nacional; a definir por frente/UF conforme tabela oficial. |
| Filtros aplicaveis | Ciclo, programa, frente. |
| Periodo historico disponivel | Sem serie historica de meta; apenas referencia visual 2.214.810 UHs. |
| Tratamento de valores nulos | Nao definido (depende da tabela oficial de metas). |
| Tratamento de duplicidades | Nao aplicavel: valor unico parametrizado por ciclo. |
| Unidade de medida | Unidades habitacionais (UH). |
| Responsavel pela validacao | A definir (area de negocio). |

### uh_contratadas

| Campo | Valor |
|---|---|
| Nome | uh_contratadas |
| Definicao | Unidades habitacionais contratadas ate a data de referencia. |
| Objetivo | Ponteiro principal de contratacao do reloginho; mede o andamento de contratacao contra a meta. |
| Fonte | Bases mensais SNH dados prioritarios: `raw/202606_SNH_PMCMV_DADOS_PRIORITARIOS_AF_CAIXA.csv` e `raw/202606_SNH_PMCMV_DADOS_PRIORITARIOS_AF_BB.txt` (campo UH Contratadas). Serie historica: seed `issue_118_mcmv_serie_temporal_piloto` (2009-2025). |
| Tabelas e campos utilizados | Silver mcmv (a definir). Campos preferenciais: `quantidade_uh` (aliases: `qt_uh`, `uh_contratadas`, `unidades_qt`, `uh`), `dt_referencia`, `apf`, `uf`, `municipio`, `codigo_ibge_municipio`. |
| Regra de calculo | Soma de UHs contratadas ate a data de referencia. Snapshot 30/06/2026: 1.874.623 UHs (CAIXA + BB), 84,64% da meta visual. |
| Granularidade temporal | Mensal (snapshots) para o dado atual; anual (2009-2025) na serie historica. |
| Granularidade territorial | APF, UF, municipio, agente financeiro (bases mensais); a serie historica e nacional por linha (OGU/Subsidiado e FGTS/Financiado). |
| Filtros aplicaveis | Frente, UF, municipio, agente financeiro, modalidade, ciclo. |
| Periodo historico disponivel | Serie anual 2009-2025 (piloto #118, somente contratadas, separando OGU/Subsidiado e FGTS/Financiado); snapshot pontual 30/06/2026. |
| Tratamento de valores nulos | Nao definido. Hiato OGU/Subsidiado 2020-2023 aparece como zeros na serie: periodo ausente/incompleto a classificar (ausencia real vs dado nao coletado). |
| Tratamento de duplicidades | Deduplicacao por APF/contrato para evitar dupla contagem entre arquivos de contratacao e entrega (issue-66). |
| Unidade de medida | Unidades habitacionais (UH). |
| Responsavel pela validacao | A definir (area de negocio). |

### uh_entregues

| Campo | Valor |
|---|---|
| Nome | uh_entregues |
| Definicao | Unidades habitacionais entregues ate a data de referencia. |
| Objetivo | Ponteiro de entrega do reloginho; deve ser exibido separadamente de contratadas para evitar leitura otimista. |
| Fonte | Arquivos de entrega: `raw/202606_SNH_PMCMV_DADOS_PRIORITARIOS_AF_CAIXA_ENTREGAS.csv` (`QT_UH_ENTREGUES`) e `raw/202606_SNH_PMCMV_DADOS_PRIORITARIOS_DA_ENTREGA_DA_UNIDADE_AF_BB.csv` (Numero de Unidades Entregues); e campo UH Entregues nas bases mensais 202606 CAIXA + BB. |
| Tabelas e campos utilizados | Silver mcmv (a definir). Campo preferencial `quantidade_uh_entregues` (aliases: `qt_uh_alienadas`, `qt_unidades_entregues`, `numero_de_unidades_entregues`), `dt_referencia`, `apf`, `uf`, `municipio`. |
| Regra de calculo | Soma de UHs entregues ate a data de referencia. Snapshot 30/06/2026: 1.543.432 (bases mensais CAIXA + BB) ou 1.518.598 (arquivos de entrega por evento), 69,69% da meta visual. Dois caminhos com totais diferentes: definir qual e o oficial. |
| Granularidade temporal | Mensal (snapshot pontual). NAO existe serie historica de entregues. |
| Granularidade territorial | APF, UF, municipio, agente financeiro (snapshot). NAO existe serie historica territorial de entregues. |
| Filtros aplicaveis | Frente, UF, municipio, agente financeiro, modalidade. |
| Periodo historico disponivel | Somente snapshot 30/06/2026; nao existe serie historica de entregues (piloto #118 cobre apenas contratadas). |
| Tratamento de valores nulos | NULL em frentes sem entrega (ver silver); demais casos nao definido. |
| Tratamento de duplicidades | Deduplicacao por APF/contrato para evitar dupla contagem entre arquivos de contratacao e entrega (issue-66). |
| Unidade de medida | Unidades habitacionais (UH). |
| Responsavel pela validacao | A definir (area de negocio). |

### perc_meta_contratada

| Campo | Valor |
|---|---|
| Nome | perc_meta_contratada |
| Definicao | Percentual da meta oficial ja contratada. |
| Objetivo | Card de progresso de contratacao do reloginho. |
| Fonte | Derivado de uh_contratadas e uh_meta_total. |
| Tabelas e campos utilizados | `quantidade_uh`; `uh_meta_total` (parametro da tabela oficial de metas). |
| Regra de calculo | `uh_contratadas / uh_meta_total`. Referencia com a meta visual: 1.874.623 / 2.214.810 = 84,64%. |
| Granularidade temporal | Mensal (snapshot). |
| Granularidade territorial | Nacional; por frente/UF conforme grao de uh_contratadas. |
| Filtros aplicaveis | Frente, UF, municipio, agente financeiro, ciclo. |
| Periodo historico disponivel | Calculavel a partir da serie anual de contratadas 2009-2025 contra a meta do respectivo ciclo (regra pendente; meta oficial nao definida). |
| Tratamento de valores nulos | Nao definido; depende de uh_meta_total (se meta nao definida, indicador sem calculo). |
| Tratamento de duplicidades | Herda a deduplicacao de uh_contratadas. |
| Unidade de medida | Percentual (%). |
| Responsavel pela validacao | A definir (area de negocio). |

### perc_meta_entregue

| Campo | Valor |
|---|---|
| Nome | perc_meta_entregue |
| Definicao | Percentual da meta oficial ja entregue. |
| Objetivo | Card de progresso de entrega do reloginho. |
| Fonte | Derivado de uh_entregues e uh_meta_total. |
| Tabelas e campos utilizados | `quantidade_uh_entregues`; `uh_meta_total` (parametro da tabela oficial de metas). |
| Regra de calculo | `uh_entregues / uh_meta_total`. Referencia com a meta visual: 1.543.432 / 2.214.810 = 69,69%. |
| Granularidade temporal | Mensal (snapshot). |
| Granularidade territorial | Nacional; por frente/UF conforme grao de uh_entregues. |
| Filtros aplicaveis | Frente, UF, municipio, agente financeiro, ciclo. |
| Periodo historico disponivel | Somente snapshot 30/06/2026; nao existe serie historica de entregues. |
| Tratamento de valores nulos | Nao definido; depende de uh_meta_total e de uh_entregues (NULL em frentes sem entrega). |
| Tratamento de duplicidades | Herda a deduplicacao de uh_entregues. |
| Unidade de medida | Percentual (%). |
| Responsavel pela validacao | A definir (area de negocio). |

### gap_uh_meta

| Campo | Valor |
|---|---|
| Nome | gap_uh_meta |
| Definicao | Unidades habitacionais faltantes para atingir a meta (gap para a meta). |
| Objetivo | Card de desvio/gap do reloginho para tomada de decisao. |
| Fonte | Derivado de uh_entregues e uh_meta_total. |
| Tabelas e campos utilizados | `quantidade_uh_entregues`; `uh_meta_total` (parametro). |
| Regra de calculo | `uh_meta_total - uh_entregues`. |
| Granularidade temporal | Mensal (snapshot). |
| Granularidade territorial | Nacional; por frente/UF conforme grao de uh_entregues. |
| Filtros aplicaveis | Frente, UF, municipio, agente financeiro, ciclo. |
| Periodo historico disponivel | Depende de uh_entregues (somente snapshot) e da meta oficial (pendente). |
| Tratamento de valores nulos | Nao definido. |
| Tratamento de duplicidades | Herda a deduplicacao de uh_entregues. |
| Unidade de medida | Unidades habitacionais (UH). |
| Responsavel pela validacao | A definir (area de negocio). |

### ritmo_medio_mensal

| Campo | Valor |
|---|---|
| Nome | ritmo_medio_mensal |
| Definicao | Entregas acumuladas divididas pelos meses observados (ritmo medio de entrega). |
| Objetivo | Medir o ritmo medio de entrega; base para comparacao com o ritmo necessario. |
| Fonte | Derivado de uh_entregues (snapshots mensais). |
| Tabelas e campos utilizados | `quantidade_uh_entregues`; `dt_referencia`. A gold do relogio tambem preve `ritmo_recente` (media movel semanal/mensal). |
| Regra de calculo | Entregas acumuladas / meses observados. |
| Granularidade temporal | Mensal. |
| Granularidade territorial | Nao definido; aplicavel por frente/UF conforme grao de uh_entregues. |
| Filtros aplicaveis | Frente, UF, municipio, agente financeiro. |
| Periodo historico disponivel | NAO existe serie mensal de entregues; o calculo depende de snapshots mensais que ainda nao existem como serie. |
| Tratamento de valores nulos | Nao definido. |
| Tratamento de duplicidades | Herda a deduplicacao de uh_entregues. |
| Unidade de medida | UHs/mes. |
| Responsavel pela validacao | A definir (area de negocio). |

### ritmo_necessario

| Campo | Valor |
|---|---|
| Nome | ritmo_necessario |
| Definicao | Unidades restantes divididas pelos meses restantes do ciclo. |
| Objetivo | Ritmo minimo mensal necessario para fechar a meta no prazo; base do alerta "ritmo insuficiente". |
| Fonte | Derivado de uh_meta_total, uh_entregues e dt_referencia. |
| Tabelas e campos utilizados | `quantidade_uh_entregues`; `uh_meta_total` (parametro); `dt_referencia`. |
| Regra de calculo | `(uh_meta_total - uh_entregues) / meses restantes do ciclo`. |
| Granularidade temporal | Mensal. |
| Granularidade territorial | Nao definido; aplicavel por frente/UF conforme grao. |
| Filtros aplicaveis | Frente, UF, municipio, agente financeiro, ciclo. |
| Periodo historico disponivel | Depende da meta oficial (pendente) e de uh_entregues (somente snapshot). |
| Tratamento de valores nulos | Nao definido. |
| Tratamento de duplicidades | Herda a deduplicacao de uh_entregues. |
| Unidade de medida | UHs/mes. |
| Responsavel pela validacao | A definir (area de negocio). |

### projecao_entrega

| Campo | Valor |
|---|---|
| Nome | projecao_entrega |
| Definicao | Projecao de entregas: entregas observadas mais o ritmo recente projetado para o restante do ciclo. |
| Objetivo | Base do alerta "meta em risco" (projecao menor que a meta oficial). |
| Fonte | Derivado de uh_entregues e ritmo_medio_mensal/ritmo_recente. |
| Tabelas e campos utilizados | `quantidade_uh_entregues`; `dt_referencia`; ritmo (derivado). |
| Regra de calculo | Entregas observadas + (ritmo recente x meses restantes do ciclo). |
| Granularidade temporal | Mensal. |
| Granularidade territorial | Nao definido. |
| Filtros aplicaveis | Frente, UF, municipio, agente financeiro, ciclo. |
| Periodo historico disponivel | NAO ha serie mensal de entregues para projetar; o calculo depende de uma serie de snapshots. |
| Tratamento de valores nulos | Nao definido. |
| Tratamento de duplicidades | Herda a deduplicacao de uh_entregues. |
| Unidade de medida | Unidades habitacionais (UH). |
| Responsavel pela validacao | A definir (area de negocio). |

### status_relogio

| Campo | Valor |
|---|---|
| Nome | status_relogio |
| Definicao | Classificacao do andamento do reloginho: No prazo, Atencao ou Risco. |
| Objetivo | Status executivo/visual do reloginho por frente e no total. |
| Fonte | Derivado dos demais indicadores do grupo A (progresso, gap, ritmo e projecao). |
| Tabelas e campos utilizados | Derivado; a definir na gold do relogio (`status_meta` na matriz de frentes da issue-66). |
| Regra de calculo | Classificacao em No prazo / Atencao / Risco; faixas de corte NAO definidas nos docs. |
| Granularidade temporal | Mensal (snapshot). |
| Granularidade territorial | Nacional e por frente; extensao por UF/municipio a definir. |
| Filtros aplicaveis | Frente, UF, municipio, agente financeiro. |
| Periodo historico disponivel | Sem serie historica; visao pontual por snapshot. |
| Tratamento de valores nulos | Nao definido. |
| Tratamento de duplicidades | Nao aplicavel (status derivado agregado). |
| Unidade de medida | Categorico (No prazo / Atencao / Risco). |
| Responsavel pela validacao | A definir (area de negocio). |

## Grupo B - Indicadores de Gargalo e Desempenho (9 itens)

Contexto comum do grupo B:

- Fonte: gold `mcmv_indicadores.indicadores_gargalo_desempenho` (uma linha por
  empreendimento/APF) e `mcmv_indicadores.resumo_gargalo_desempenho_dashboard`
  (agregacoes por nacional, frente, UF, municipio e responsavel). Origem das bases
  FAR (`empreendimento_far.ficha_empreendimento`,
  `empreendimento_far.evolucao_financeira`,
  `empreendimento_far.execucao_fisica_financeira_chart`) e FDS
  (`entidades_fds.fds_ficha_empreendimento`, `entidades_fds.fds_empreendimento`,
  `entidades_fds.fds_evolucao_financeira_chart`).
- Granularidade temporal: pontual por `dt_calculo`; sem serie historica documentada.
- Granularidade territorial: empreendimento/APF (`id_indicador` = `frente:apf`);
  agregacoes por nacional, frente, UF, municipio e responsavel.
- Filtros aplicaveis: `frente`, `uf`, `municipio`, `responsavel_nome`,
  `classificacao_gargalo`, `indicadores_acionados`.
- Periodo historico disponivel: visao pontual do snapshot atual.
- Tratamento de valores nulos: flags booleanas false quando nao acionadas.
- Tratamento de duplicidades: chave unica `id_indicador` (`frente:apf`) com testes
  `unique` e `not_null` no schema.yml.
- Responsavel pela validacao: A definir (area de negocio) para todos os itens.

### flag_atraso

| Campo | Valor |
|---|---|
| Nome | flag_atraso |
| Definicao | True quando o status de prazo indica atraso ou `dias_atraso` e maior que zero. |
| Objetivo | Identificar obras atrasadas por empreendimento/APF para alertas e ranking. |
| Fonte | Gold `mcmv_indicadores.indicadores_gargalo_desempenho` (origem golds FAR/FDS). |
| Tabelas e campos utilizados | `flag_atraso`, `status_prazo`, `dias_atraso`, `percentual_execucao_fisica`, `dt_referencia`. |
| Regra de calculo | True quando a previsao de conclusao/entrega esta vencida e a execucao fisica e menor que 100%; ou quando `status_prazo` indica atraso / `dias_atraso` maior que zero. |
| Granularidade temporal | Pontual por `dt_calculo`. |
| Granularidade territorial | Empreendimento/APF; agregacao por nacional, frente, UF, municipio, responsavel. |
| Filtros aplicaveis | `frente`, `uf`, `municipio`, `responsavel_nome`, `classificacao_gargalo`, `indicadores_acionados`. |
| Periodo historico disponivel | Visao pontual do snapshot atual; sem serie historica documentada. |
| Tratamento de valores nulos | False quando nao acionada. |
| Tratamento de duplicidades | Chave unica `id_indicador` (`frente:apf`). |
| Unidade de medida | Booleano (true/false). |
| Responsavel pela validacao | A definir (area de negocio). |

### flag_paralisacao

| Campo | Valor |
|---|---|
| Nome | flag_paralisacao |
| Definicao | True quando ha data ou situacao textual de paralisacao. |
| Objetivo | Identificar obras paralisadas por empreendimento/APF. |
| Fonte | Gold `mcmv_indicadores.indicadores_gargalo_desempenho` (origem golds FAR/FDS). |
| Tabelas e campos utilizados | `flag_paralisacao`, `dias_paralisacao`, `status_operacional`. |
| Regra de calculo | True quando ha data de paralisacao ou situacao textual contendo paralisacao. |
| Granularidade temporal | Pontual por `dt_calculo`. |
| Granularidade territorial | Empreendimento/APF; agregacao por nacional, frente, UF, municipio, responsavel. |
| Filtros aplicaveis | `frente`, `uf`, `municipio`, `responsavel_nome`, `classificacao_gargalo`, `indicadores_acionados`. |
| Periodo historico disponivel | Visao pontual do snapshot atual; sem serie historica documentada. |
| Tratamento de valores nulos | False quando nao acionada. |
| Tratamento de duplicidades | Chave unica `id_indicador` (`frente:apf`). |
| Unidade de medida | Booleano (true/false). |
| Responsavel pela validacao | A definir (area de negocio). |

### flag_sem_atualizacao_recente

| Campo | Valor |
|---|---|
| Nome | flag_sem_atualizacao_recente |
| Definicao | True para obra nao concluida sem atualizacao ha mais de 90 dias ou sem data de atualizacao. |
| Objetivo | Alertar sobre obras sem movimento/medicao recente. |
| Fonte | Gold `mcmv_indicadores.indicadores_gargalo_desempenho` (origem golds FAR/FDS). |
| Tabelas e campos utilizados | `flag_sem_atualizacao_recente`, `dias_sem_atualizacao`. |
| Regra de calculo | True para obra nao concluida sem liberacao/medicao ha mais de 90 dias ou sem data de atualizacao. |
| Granularidade temporal | Pontual por `dt_calculo`. |
| Granularidade territorial | Empreendimento/APF; agregacao por nacional, frente, UF, municipio, responsavel. |
| Filtros aplicaveis | `frente`, `uf`, `municipio`, `responsavel_nome`, `classificacao_gargalo`, `indicadores_acionados`. |
| Periodo historico disponivel | Visao pontual do snapshot atual; sem serie historica documentada. |
| Tratamento de valores nulos | False quando nao acionada. |
| Tratamento de duplicidades | Chave unica `id_indicador` (`frente:apf`). |
| Unidade de medida | Booleano (true/false). |
| Responsavel pela validacao | A definir (area de negocio). |

### flag_baixa_execucao_fisica

| Campo | Valor |
|---|---|
| Nome | flag_baixa_execucao_fisica |
| Definicao | True quando a execucao fisica esta pelo menos 10 p.p. abaixo do previsto, a previsao esta vencida sem conclusao, ou a obra contratada ha mais de 365 dias ainda esta abaixo de 30% fisico. |
| Objetivo | Identificar baixo avanco fisico por empreendimento/APF. |
| Fonte | Gold `mcmv_indicadores.indicadores_gargalo_desempenho` (origem golds FAR/FDS). |
| Tabelas e campos utilizados | `flag_baixa_execucao_fisica`, `percentual_execucao_fisica`, `dt_referencia`. |
| Regra de calculo | True quando execucao fisica esta 10 p.p. abaixo do previsto; previsao vencida sem conclusao; ou contrato com mais de 365 dias abaixo de 30% fisico. |
| Granularidade temporal | Pontual por `dt_calculo`. |
| Granularidade territorial | Empreendimento/APF; agregacao por nacional, frente, UF, municipio, responsavel. |
| Filtros aplicaveis | `frente`, `uf`, `municipio`, `responsavel_nome`, `classificacao_gargalo`, `indicadores_acionados`. |
| Periodo historico disponivel | Visao pontual do snapshot atual; sem serie historica documentada. |
| Tratamento de valores nulos | False quando nao acionada. |
| Tratamento de duplicidades | Chave unica `id_indicador` (`frente:apf`). |
| Unidade de medida | Booleano (true/false). |
| Responsavel pela validacao | A definir (area de negocio). |

### flag_baixa_execucao_financeira

| Campo | Valor |
|---|---|
| Nome | flag_baixa_execucao_financeira |
| Definicao | True quando a execucao financeira esta mais de 10 p.p. abaixo da fisica, ou o contrato com mais de 365 dias tem execucao financeira abaixo de 30%. |
| Objetivo | Identificar desembolso abaixo da execucao fisica (divergencia fisico-financeira). |
| Fonte | Gold `mcmv_indicadores.indicadores_gargalo_desempenho` (origem golds FAR/FDS). |
| Tabelas e campos utilizados | `flag_baixa_execucao_financeira`, `percentual_execucao_financeira`, `percentual_execucao_fisica`, `gap_fisico_financeiro_pp`. |
| Regra de calculo | True quando execucao financeira esta mais de 10 p.p. abaixo da fisica; ou contrato com mais de 365 dias com execucao financeira abaixo de 30%. |
| Granularidade temporal | Pontual por `dt_calculo`. |
| Granularidade territorial | Empreendimento/APF; agregacao por nacional, frente, UF, municipio, responsavel. |
| Filtros aplicaveis | `frente`, `uf`, `municipio`, `responsavel_nome`, `classificacao_gargalo`, `indicadores_acionados`. |
| Periodo historico disponivel | Visao pontual do snapshot atual; sem serie historica documentada. |
| Tratamento de valores nulos | False quando nao acionada. |
| Tratamento de duplicidades | Chave unica `id_indicador` (`frente:apf`). |
| Unidade de medida | Booleano (true/false). |
| Responsavel pela validacao | A definir (area de negocio). |

### flag_gargalo_financeiro

| Campo | Valor |
|---|---|
| Nome | flag_gargalo_financeiro |
| Definicao | True quando pelo menos 30% do contrato ainda nao foi desembolsado e a execucao fisica esta abaixo de 95%. |
| Objetivo | Identificar gargalos financeiros (saldo/desembolso distante do contratado). |
| Fonte | Gold `mcmv_indicadores.indicadores_gargalo_desempenho` (origem golds FAR/FDS). |
| Tabelas e campos utilizados | `flag_gargalo_financeiro`, `saldo_contratado_a_desembolsar`, `percentual_saldo_a_desembolsar`, `percentual_execucao_fisica`, `valor_contratado`, `valor_desembolsado`. |
| Regra de calculo | True quando pelo menos 30% do contrato ainda nao esta desembolsado e a execucao fisica esta abaixo de 95%. |
| Granularidade temporal | Pontual por `dt_calculo`. |
| Granularidade territorial | Empreendimento/APF; agregacao por nacional, frente, UF, municipio, responsavel. |
| Filtros aplicaveis | `frente`, `uf`, `municipio`, `responsavel_nome`, `classificacao_gargalo`, `indicadores_acionados`. |
| Periodo historico disponivel | Visao pontual do snapshot atual; sem serie historica documentada. |
| Tratamento de valores nulos | False quando nao acionada. |
| Tratamento de duplicidades | Chave unica `id_indicador` (`frente:apf`). |
| Unidade de medida | Booleano (true/false). |
| Responsavel pela validacao | A definir (area de negocio). |

### flag_contrato_sem_evolucao

| Campo | Valor |
|---|---|
| Nome | flag_contrato_sem_evolucao |
| Definicao | True quando o contrato tem mais de 180 dias sem execucao fisica nem financeira. |
| Objetivo | Identificar contratos estagnados (sem evolucao fisica/financeira). |
| Fonte | Gold `mcmv_indicadores.indicadores_gargalo_desempenho` (origem golds FAR/FDS). |
| Tabelas e campos utilizados | `flag_contrato_sem_evolucao`, `dias_sem_atualizacao`, `percentual_execucao_fisica`, `percentual_execucao_financeira`. |
| Regra de calculo | True quando contrato com mais de 180 dias nao tem execucao fisica nem financeira. |
| Granularidade temporal | Pontual por `dt_calculo`. |
| Granularidade territorial | Empreendimento/APF; agregacao por nacional, frente, UF, municipio, responsavel. |
| Filtros aplicaveis | `frente`, `uf`, `municipio`, `responsavel_nome`, `classificacao_gargalo`, `indicadores_acionados`. |
| Periodo historico disponivel | Visao pontual do snapshot atual; sem serie historica documentada. |
| Tratamento de valores nulos | False quando nao acionada. |
| Tratamento de duplicidades | Chave unica `id_indicador` (`frente:apf`). |
| Unidade de medida | Booleano (true/false). |
| Responsavel pela validacao | A definir (area de negocio). |

### flag_entrega_em_risco

| Campo | Valor |
|---|---|
| Nome | flag_entrega_em_risco |
| Definicao | True quando o empreendimento nao concluido tem atraso, paralisacao, baixa execucao ou falta de atualizacao. |
| Objetivo | Sinalizar entregas em risco para priorizacao e mapa de risco. |
| Fonte | Gold `mcmv_indicadores.indicadores_gargalo_desempenho` (origem golds FAR/FDS). |
| Tabelas e campos utilizados | `flag_entrega_em_risco`, `indicadores_acionados`, `flag_atraso`, `flag_paralisacao`, `flag_baixa_execucao_fisica`, `flag_sem_atualizacao_recente`. |
| Regra de calculo | True quando empreendimento nao concluido apresenta atraso, paralisacao, baixa execucao ou falta de atualizacao. |
| Granularidade temporal | Pontual por `dt_calculo`. |
| Granularidade territorial | Empreendimento/APF; agregacao por nacional, frente, UF, municipio, responsavel. |
| Filtros aplicaveis | `frente`, `uf`, `municipio`, `responsavel_nome`, `classificacao_gargalo`, `indicadores_acionados`. |
| Periodo historico disponivel | Visao pontual do snapshot atual; sem serie historica documentada. |
| Tratamento de valores nulos | False quando nao acionada. |
| Tratamento de duplicidades | Chave unica `id_indicador` (`frente:apf`). |
| Unidade de medida | Booleano (true/false). |
| Responsavel pela validacao | A definir (area de negocio). |

### score_gargalo + classificacao_gargalo

| Campo | Valor |
|---|---|
| Nome | score_gargalo + classificacao_gargalo (tratados como um item so) |
| Definicao | Score ponderado de gargalo por empreendimento/APF e classificacao em faixas (Baixo, Medio, Alto, Critico). |
| Objetivo | Priorizacao por gravidade: cards de casos criticos, rankings por responsavel e mapas por UF. |
| Fonte | Gold `mcmv_indicadores.indicadores_gargalo_desempenho` (origem golds FAR/FDS). |
| Tabelas e campos utilizados | `score_gargalo`, `classificacao_gargalo`, `indicadores_acionados`, `flag_atraso`, `flag_paralisacao`, `flag_sem_atualizacao_recente`, `flag_baixa_execucao_fisica`, `flag_baixa_execucao_financeira`, `flag_gargalo_financeiro`, `flag_contrato_sem_evolucao`. |
| Regra de calculo | Score = atraso (2) + paralisacao (2) + sem atualizacao recente (1) + baixa execucao fisica (1) + baixa execucao financeira (1) + gargalo financeiro (1) + contrato sem evolucao (1). Classificacao: Baixo = 0; Medio = 1 a 2; Alto = 3 a 4; Critico = maior ou igual a 5. |
| Granularidade temporal | Pontual por `dt_calculo`. |
| Granularidade territorial | Empreendimento/APF; agregacao por nacional, frente, UF, municipio, responsavel. |
| Filtros aplicaveis | `frente`, `uf`, `municipio`, `responsavel_nome`, `classificacao_gargalo`, `indicadores_acionados`. |
| Periodo historico disponivel | Visao pontual do snapshot atual; sem serie historica documentada. |
| Tratamento de valores nulos | False quando nao acionada (flag componente); score 0 resulta em classificacao Baixo. |
| Tratamento de duplicidades | Chave unica `id_indicador` (`frente:apf`). |
| Unidade de medida | Score numerico inteiro + categorico (Baixo / Medio / Alto / Critico). |
| Responsavel pela validacao | A definir (area de negocio). |

## Pontos de atencao / ambiguidades

- **Meta oficial pendente**: todos os indicadores percentuais, de gap, ritmo e
  projecao dependem de `uh_meta_total`, que nao esta definida pela area.
- **Dois totais de entrega**: as bases mensais CAIXA + BB somam 1.543.432 UHs
  entregues, enquanto os arquivos de entrega por evento somam 1.518.598 UHs; a
  regra oficial de qual caminho usar esta pendente.
- **Ausencia de serie de entregues**: `uh_entregues`, `ritmo_medio_mensal`,
  `ritmo_necessario` e `projecao_entrega` nao podem ser calculados como serie hoje.
- **Hiato OGU/Subsidiado 2020-2023**: zeros na serie devem ser classificados como
  ausencia real vs dado nao coletado.
- **Granularidade mensal/territorial historica**: inexistente no piloto #118
  (somente anual e nacional, para contratadas).
- **Faixas de corte do `status_relogio`**: nao definidas nos docs.
