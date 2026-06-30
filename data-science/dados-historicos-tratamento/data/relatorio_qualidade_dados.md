# Relatório de Qualidade dos Dados

Relatório gerado automaticamente pelo módulo de inventário após a classificação e tratamento das tabelas MCMV.

## 1. Resumo Executivo

- **Total de tabelas analisadas:** 753
- **Período coberto:** 2000-01-01 a 2026-11-02
- **Frentes identificadas:** CCFGTS, FAR, FDS, FGTS, PF, PJ, PNHR, Rural
- **Alta utilidade:** 163
- **Média utilidade:** 41
- **Baixa utilidade:** 208
- **Nenhuma utilidade:** 341

## 2. Metodologia de Scoring

A pontuação de utilidade preditiva (0-10) é calculada como: +2 para presença de colunas de status de obra (situacao_, status_obra, paralisad, concluid, andamento); +2 para >=3 colunas de data; +2 para colunas de progresso (percentual_obra, concluidas, entregues, em_obras); +1 para colunas financeiras (valor_, vlr_, subsidio, investimento); +1 para geolocalização (municipio + UF/IBGE); +1 para granularidade de contrato (APF, cod_contrato); +1 para perfil colunar_denso ou event_level. Penalidades: -2 para agregado_uf/agregado_municipal; -3 para lookup/dados_sem_utilidade/vazia; -3 para tabelas duplicadas; -1 para tabelas não tratadas. Classificação: 8-10 = Alta, 5-7 = Média, 2-4 = Baixa, <=1 = Nenhuma.

## 3. Duplicatas

- **Total de duplicatas:** 309 (41.0% do total de 753 tabelas)
- **Tabela com mais cópias:** 001_2012_02_fevereiro_base_pmcmv_relatorio_executivo_10_02_2012 (272 cópias)

**Top 5 exemplos de duplicatas:**
  1. 001_2012_03_marco_pmcmv_relatorio_executivo_27_03_2012___cópia
  2. 001_2012_04_abril_cópia_de_pmcmv_relatorio_executivo_16_04_201
  3. 001_2012_10_outubro_20121009_pmcmv_relatório_executivo_0910201
  4. 001_2014_12_dezembro_rel_executivo_resumo_31122014_reprocessado
  5. 01_2012_04_abril_cópia_de_pmcmv_relatorio_executivo_16_04_2012

## 4. Qualidade por Perfil

| Perfil | N Tabelas | Missing % Médio | Encoding Errors | Date Parse Errors |
|--------|-----------|---------------------|-----------------|-------------------|
| agregado_uf | 2 | 9.6 | 0 | 0 |
| colunar_denso | 388 | 5.09 | 0 | 0 |
| dados_sem_utilidade | 3 | 0.0 | 0 | 0 |
| event_level | 22 | 0.45 | 0 | 0 |
| lookup | 3 | 5.56 | 0 | 0 |
| separador_pipe | 5 | 0.0 | 0 | 0 |
| sub_tabelas_1 | 1 | 0.0 | 0 | 0 |
| sub_tabelas_2 | 1 | 28.82 | 0 | 0 |
| sub_tabelas_3 | 17 | 53.4 | 0 | 0 |
| sub_tabelas_4 | 1 | 64.64 | 0 | 0 |
| vazia | 1 | 0.0 | 0 | 0 |

## 5. Qualidade por Instituição

| Instituição | N Tabelas | Missing % Médio | % com Status Obra | % com Financeiro |
|--------------|-----------|---------------------|-------------------|------------------|
| bb | 231 | 5.82 | 27.7% | 95.7% |
| caixa | 125 | 8.61 | 65.6% | 76.8% |
| unknown | 88 | 6.86 | 61.4% | 70.5% |

## 6. Distribuição de Scores

### Histograma de Scores (0-10)
- **0**: 10 tabelas ##########
- **1**: 15 tabelas ###############
- **2**: 50 tabelas ##################################################
- **3**: 62 tabelas ##################################################
- **4**: 96 tabelas ##################################################
- **5**: 32 tabelas ################################
- **6**: 7 tabelas #######
- **7**: 2 tabelas ##
- **8**: 101 tabelas ##################################################
- **9**: 1 tabelas #
- **10**: 61 tabelas ##################################################

### Classificação agregada
- **Alta (8-10):** 163 tabelas
- **Média (5-7):** 41 tabelas
- **Baixa (2-4):** 208 tabelas
- **Nenhuma (<=1):** 341 tabelas

## 7. Limitações Conhecidas

- **Tabelas sem período identificado:** 151 tabelas
- **Tabelas com frente não identificada:** 239 tabelas
- Os dados analisados são amostras de 200 linhas do dump original. Datas mínima e máxima podem não representar a totalidade do período real.
- Formatos de data não padronizados (ex.: "Pronto - Julho/2010") não são interpretados pelo parser atual.
