## Resumo dos Achados — Batimento SFTP × Dump (#95)

> Gerado a partir da exploração das amostras dos 3 schemas (sftp, dados_historicos e dados_historicos_formatados).

### 1. Panorama dos Dados

```
SFTP:              2008 tabelas (2020–2026)
                    ├─ gefus_* (1459): snapshots mensais, andamento obra, integrações
                    ├─ int0* (215): feeds institucionais (Ministério das Cidades)
                    ├─ caixa_af_gehis_* (180): obras Caixa (semanal)
                    ├─ bb_af_diemp_* (237): obras BB (semanal)
                    └─ pmcmv_* (60): Faixa3, Reformas

Dados Históricos:   754 tabelas (2009–2026)
                    ├─ 2009–2018 (~700): históricos BB e Caixa (sem correspondente SFTP)
                    └─ 2020–2026 (~54): dados prioritários (sobreposição com SFTP)

Formatados:         388 tabelas (colunas normalizadas, acentos removidos)
                    └─ 82 tabelas têm data_de_movimento na coluna 0
```

### 2. Principais Achados da Exploração

#### ✅ Os dados são idênticos onde há sobreposição

Para `202406_af_caixa`, 200/200 APFs são idênticos entre SFTP e formatados. Mesmos valores, mesma ordem, apenas formatação diferente (datas `DD/MM/YYYY` vs `YYYY-MM-DD`, floats com/sem `.0`).

#### 🔴 `data_de_movimento` estava sendo descartada

82 tabelas (10.9%) armazenam `data_de_movimento` na **coluna 0** do CSV — posição que o `index_col=0` do pipeline descartava como row index. Nenhuma das 388 tabelas formatadas preservava essa coluna. **Corrigido.**

#### 🔴 `report_date` falhava para prefixos `historico_recente_*`

~60 tabelas com prefixos `historico_recente_*`, `o_recente_*` ou nomes truncados (`ecente_`, `storico_`) ficavam com `report_date = null`. **Corrigido** com 3 novos padrões de regex.

#### 🟡 O matching estrutural subestima as conexões

- 1299 matches "hash exato" são na verdade poucas famílias SFTP (≈16) que batem com muitas tabelas dump — relação 1:N, não 1:1
- ~87% das tabelas SFTP não têm correspondente estrutural no dump (principalmente andamento_obra, FGTS, Faixa3 — dados que o dump nunca teve)
- ~356 tabelas dump (2009-2018) não têm correspondente SFTP (anteriores à existência do gefus)

### 3. O Que Foi Implementado

| Mudança | Arquivo |
|---------|---------|
| Detecção e preservação de `data_de_movimento` na col0 | `carregamento.py` |
| 3 novos padrões de `report_date` (prefixos e truncados) | `tratamento.py` |
| `data_de_movimento` nos metadados de saída | `tratamento.py`, `saida_tratamento.py` |
| Batimento estrutural contra `dados_historicos_formatados` | `leitura_artefatos.py`, `matching.py`, `batimento_estrutura.py` |
| **Novo**: Cruzamento por conteúdo via APF | `cruzamento_conteudo.py` |
| **Novo**: Verificação de consistência temporal | `verificar_consistencia_temporal.py` |
| Relatório de batimento v2 (5 seções + conteúdo) | `relatorio.py` → `relatorio_batimento_dump_sftp_v2.md` |
| 11 testes unitários | `test_preservacao_data_movimento.py` |

### 4. Resultados das Análises

#### Cruzamento por Conteúdo (APF)

| Métrica | Valor |
|---------|-------|
| Pares analisados | 4.446 |
| Pares com confiança alta (overlap APF ≥ 90%) | **1.282** |
| Pares já conhecidos pelo batimento estrutural | 76 |
| **Novos pares descobertos apenas por conteúdo** | **1.206** |
| Famílias cobertas | 2 (`snh_pmcmv_dados_prioritarios`, com variantes `_entregas` e `_entrega_da_unidade`) |

O cruzamento por conteúdo mais que **triplicou** o número de conexões de alta confiança entre os schemas.

#### Consistência Temporal

| Status | Quantidade |
|--------|-----------|
| Ok (diferença ≤ 31 dias) | 397 |
| **Divergente** (> 31 dias) | **6** |
| Indeterminado (sem data) | 739 |

Maior discrepância: `202402_snh_pmcmv_dados_prioritarios_da_entrega_da_unidade_af_bb` — nome indica Fev/2024 mas `data_de_movimento` = `28/02/2025` (393 dias de diferença).

#### Batimento Estrutural (contra formatados)

Resultados similares ao batimento original: 1.510 pares, 8.834 campos em comum, 2 chaves candidatas. A diferença está na camada 2 (stem canônico) e 3 (Jaccard) onde colunas sem acentos e sem o bug `_exec` melhoram o matching.

### 5. Chaves de Cruzamento Identificadas

| Chave | Schemas | Validação |
|-------|---------|-----------|
| `apf` (ou `nu_apf`) | Todos dados_prioritarios + andamento_obra | ✅ Dupla (estrutural + conteúdo) |
| `codigo_ibge_do_municipio` | dados_prioritarios af_caixa, af_bb | ✅ Estrutural |
| `nome_empreendimento` | dados_prioritarios | ✅ Estrutural (texto, requer normalização) |

### 6. Recomendações para Fechamento da Issue

1. **SFTP complementa o dump** — use dump para série histórica 2009–2018 e SFTP para dados operacionais 2020–2026
2. **Família `snh_pmcmv_dados_prioritarios`**: usar `apf` + `data_de_movimento` como chave composta para JOIN entre schemas na zona de sobreposição (2024–2025)
3. **`andamento_obra`**: SFTP tem 237 snapshots semanais; dump tem apenas 1 tabela de 2013 (pipe-separated, malformada). Usar SFTP como fonte primária
4. **FGTS, Faixa3, Reformas**: dados exclusivos do SFTP — não existem no dump
5. **Reprocessar** as tabelas formatadas com a correção de `data_de_movimento` antes de qualquer JOIN temporal
6. **Investigar** a discrepância `202402 → 2025-02-28` — possível erro de nomeação ou de extração da amostra

### 7. Artefatos

- Relatório completo v2: `data/sftp/relatorios/relatorio_batimento_dump_sftp_v2.md`
- Cruzamento por conteúdo: `data/sftp/analise/cruzamento_conteudo.csv`
- Consistência temporal: `data/sftp/analise/consistencia_temporal.csv`
- Batimento formatados: `data/sftp/analise/*_formatados.csv`
- Testes: `tests/test_preservacao_data_movimento.py` (11 testes, todos passando)
