# Relatório de Classificação e Tratamento — Dados Históricos MCMV

**Issue:** [#59 — Organizar, limpar e padronizar os dados](https://github.com/GovHub-br/data-application-cidades/issues/59)  
**Feature:** [#53 — Inteligência preditiva e alertas estratégicos do MCMV](https://github.com/GovHub-br/data-application-cidades/issues/53)  
**Data do relatório:** Julho 2026  
**Versão do pipeline:** Stages 1–4 implementados + SFTP (em andamento)

---

## 1. Visão Geral

O pipeline processa duas fontes de dados históricos do MCMV:

| Fonte | Schema | Tabelas | Período | Destino |
|-------|--------|---------|---------|---------|
| **Dump PostgreSQL** | `dados_historicos` | 753 | 2009–2026 | `dados_historicos_formatados` |
| **SFTP/GEFUS** | `sftp` | 2008 | 2020–2026 | `sftp_tratado` (em implantação) |

O fluxo completo: **Classificação estrutural → Deduplicação → Tratamento por categoria → Relatório de qualidade**.

### Modos de execução

| Comando | Descrição |
|---------|-----------|
| `uv run python main.py` | CSV: classifica + trata amostras de 200 linhas |
| `uv run python main.py --db` | DB: classifica + trata tabelas completas no PostgreSQL |
| `uv run python main.py --db --sftp` | DB + SFTP: pipeline completo incluindo canonicalização GEFUS |
| `--skip-classify` | Pula classificação, carrega de fonte existente |
| `--classify-only` | Apenas classificação, sem tratamento |

---

## 2. Classificação Estrutural — Schema `dados_historicos`

**Total:** 753 tabelas classificadas em 16 de 17 categorias possíveis.

### Distribuição

| Categoria | Qtd | % | Descrição resumida |
|-----------|-----|----|--------------------|
| `bem_formada` | 351 | 46.6% | Colunas nomeadas, tipos consistentes. Formato ideal. |
| `sub_tabelas_1` | 273 | 36.3% | Múltiplas sub-tabelas com timestamps YYYYMMDD_hhmmss |
| `cabecalho_composto_2` | 50 | 6.6% | Cabeçalho mesclado de 2 linhas (Excel merge) |
| `cabecalho_na_primeira_linha_1` | 36 | 4.8% | Header real na primeira linha de dados |
| `sub_tabelas_3` | 19 | 2.5% | Sub-tabelas com header multi-linha |
| `separador_\|` | 5 | 0.7% | Dados pipe-separated em coluna única |
| `cabecalho_composto_1` | 4 | 0.5% | Cabeçalho mesclado de 3 linhas |
| `dados_sem_utilidade` | 4 | 0.5% | Logs/metadados, sem valor analítico |
| `sem_cabecalho` | 3 | 0.4% | Header ausente, nomes inferidos |
| `cabecalho_na_segunda_linha` | 2 | 0.3% | Linha 0 = "Posicao: DD/MM/YYYY" |
| `cabecalho_na_terceira_linha_1` | 1 | 0.1% | Header na 3ª linha |
| `cabecalho_na_terceira_linha_2` | 1 | 0.1% | Header na 3ª linha + palavras-chave |
| `cabecalho_na_primeira_linha_2` | 1 | 0.1% | Header na 1ª linha + totalização |
| `sub_tabelas_2` | 1 | 0.1% | Split por keywords |
| `sub_tabelas_4` | 1 | 0.1% | Blocos text-heavy |
| `vazia` | 1 | 0.1% | Sem dados |

### Árvore de decisão (implementada em `regras.py`)

```
R2 (nome → dados_sem_utilidade) → R1 (vazia) → R3 (pipe: separador_|)
→ R3b (header: bem_formada | sem_cabecalho | nao_colunares)
→ "Posicao:" check → R5 (sub-tabelas 1-4) → R6 (composto 1-2)
→ R7 (deslocado 1-6) → R8 (fallback)
```

**Referência autoritativa:** `classificacao_formacao_revisado_autoritativo.md` (197 linhas, 17 categorias com exemplos).

---

## 3. Tratamento por Categoria — Schema `dados_historicos`

### Pipeline `bem_formada` (8 grupos)

Aplicado a 351 tabelas + todas as demais categorias após conversão estrutural:

| Grupo | Etapa | Descrição |
|-------|-------|-----------|
| G0 | Limpeza prévia | Remove linhas/colunas 100% vazias |
| G1 | Normalização de nomes | lowercase, remove acentos (NFKD), `_` para especiais, colapsa duplos |
| G2 | Separador decimal | Detecta vírgula (padrão BR), converte para `.` e `float64` |
| G3 | Parsing de datas | Formatos ISO (`YYYY-MM-DD`) e BR (`DD/MM/YYYY`); segurança: reverte se >10% falham |
| G4 | Reparo de encoding | `charset-normalizer` + `ftfy.fix_text()` para mojibake Latin1→UTF-8 |
| G5 | Canonização de tipos | Regras por prefixo (`cod_`→str, `vlr_`→float64, `dt_`→datetime) + fallback do inventário |
| G6 | Classificação de perfil | `lookup`, `event_level`, `agregado_uf`, `colunar_denso` |
| G7 | Período e instituição | Extrai `report_date` do nome (5 padrões regex) + `bb`/`caixa`/`unknown` |
| G8 | Metadados | Adiciona `source_table`, `report_date`, `institution`, `profile`, `content_hash` |

### Tratamentos especiais

| Categoria | Módulo | Transformação |
|-----------|--------|---------------|
| Cabeçalho deslocado (7 subtipos) | `tratamento_cabecalho.py` | Promover header para `df.columns`, remover metadados |
| Cabeçalho composto (2 subtipos) | `tratamento_cabecalho.py` | Forward-fill + concatenação de linhas de header |
| Sub-tabelas (4 subtipos) | `tratamento_subtabelas.py` | Split, wide→long, reconstrução de header multi-linha |
| `separador_\|` | `tratamento_especiais.py` | Expansão pipe em múltiplas colunas + normalização |
| `sem_cabecalho` | `tratamento_especiais.py` | Inferência de nomes via `inferencia_colunas.py` |
| Pipe single-col (DB) | `tratamento_pipe_single_col.py` | Split + coerção de tipos para 5 tabelas BB 2013 |

### Resultados do tratamento

| Métrica | Valor |
|---------|-------|
| Tabelas de entrada | 753 |
| Após deduplicação (canônicas) | ~445 |
| Tabelas tratadas | 440 |
| Descartes (`vazia` + `dados_sem_utilidade`) | 5 |
| Status `treated` no DB | 385/395 (97.5%) |
| Erros no banco | 0 |

### Perfis de saída

- `colunar_denso` — tabelas com 7+ colunas, pipeline completo
- `event_level` — 4-6 colunas com chave repetida
- `agregado_uf` — 7-9 colunas com totalizações por UF
- `lookup` — ≤3 colunas, apenas reparo de encoding
- `sub_tabelas_1` a `_4` — formato long com `recorte_tipo`, `recorte_valor`, `data_referencia`, `valor`
- `separador_pipe` — colunas expandidas de pipe
- `pipe_single_col` — 5 tabelas BB 2013 com mapeamento de colunas fixo

---

## 4. Problemas Identificados e Correções Aplicadas

### Correções concluídas (changes arquivadas)

| Change | Problema | Solução |
|--------|----------|---------|
| `corrigir-pipeline-db` | `data_de_movimento` descartada (coluna 0 como row index); `report_date` ausente para prefixos truncados | Detecção e preservação da col0; 3 novos padrões regex |
| `tratar-tabelas-pipe-single-col` | 5 tabelas BB 2013 classificadas como `vazia`/`separador_\|` com dados reais | Módulo dedicado com mapeamento fixo de colunas |
| `limpeza-dados-tratamento` | 35 discrepâncias amostradas; encoding não-UTF-8; datas textuais não detectadas | Correções de encoding, ampliação de regex de data, limpeza de sub-tabelas |
| `adicionar-comentarios-postgres` | Falta de rastreabilidade no schema target | Metadados como comentários de tabela no PostgreSQL |

### Em andamento

| Change | Status | Descrição |
|--------|--------|-----------|
| `sftp-tratado` | Implementado, pendente verificação DB | Schema dedicado `sftp_tratado` com `_classificacao`, `_canonicas`, `_qualidade`; correção de canonical quebrada; redução do Padrão A |

---

## 5. Schema SFTP — Tratamento de Formação

O schema `sftp` contém **2008 tabelas** (2020–2026) ingeridas do GEFUS/MinIO, complementando a série histórica do dump (2009–2018).

### Panorama SFTP

| Categoria | Qtd | % |
|-----------|-----|----|
| Tab-separated válidas (`bem_formada`) | 1704 | 84.9% |
| Com pipe — precisam de saneamento | 304 | 15.1% |
| **Total** | **2008** | **100%** |

### Tabelas com pipe (304)

| Padrão | Qtd | Descrição | Status |
|--------|-----|-----------|--------|
| **Padrão A** — arquivo pipe-separated | 204 | `int0*` (169), `base_pj_fgts_*` (31), `tab_validacao_*` (2), `base_andamento_obra` (1), `int040` (1) | 171 cobertas por GEFUS limpa; 33 precisam de reconstrução |
| **Padrão B** — coluna 0 pipe-delimited | 100 | `caixa_af_gehis_andamento_obra_*` (96), outros (4) | 94 cobertas por GEFUS; 6 precisam de split pipe |

### Estratégia de tratamento SFTP

1. **Canonicalização GEFUS** — detecta pares original↔GEFUS, elege versão limpa, copia para `sftp_tratado`
2. **Correção de canonical quebrada** — 2 tabelas com GEFUS ainda pipe-concatenada recebem split antes da cópia
3. **Padrão B** — split da coluna 0 por `|` para tabelas sem GEFUS
4. **Padrão A** — reconstrução de colunas fragmentadas para `base_pj_fgts_*` + `tab_validacao_*`

### Estrutura do `sftp_tratado`

```
sftp_tratado
├── _classificacao    (2008 linhas — todas as tabelas SFTP)
├── _canonicas        (~1500 mapeamentos GEFUS)
├── _qualidade        (~500 tabelas tratadas)
└── <tabelas tratadas>
```

---

## 6. Batimento SFTP × Dump

Análise estrutural entre os schemas para identificar sobreposições e complementaridades:

| Métrica | Valor |
|---------|-------|
| Pares hash exato (confiança alta) | 1299 |
| Pares stem canônico (confiança média) | 88 |
| Pares Jaccard (confiança baixa) | 123 |
| **Total de pares** | **1510** |
| Tabelas SFTP relacionadas ao dump | 273 |
| Tabelas dump relacionadas ao SFTP | 54 |
| Tabelas SFTP sem correspondente no dump | 1747 |
| Tabelas dump sem correspondente no SFTP | ~700 (2009–2018) |

### Chaves de cruzamento identificadas

| Chave | Schemas | Validação |
|-------|---------|-----------|
| `apf` / `nu_apf` | Todos `dados_prioritarios` + `andamento_obra` | Dupla (estrutural + conteúdo) |
| `codigo_ibge_do_municipio` | `dados_prioritarios` af_caixa, af_bb | Estrutural |
| `nome_empreendimento` | `dados_prioritarios` | Estrutural (requer normalização) |

### Cruzamento por conteúdo (APF)

- **1282 pares** com confiança alta (overlap APF ≥ 90%)
- **1206 novos pares** descobertos apenas por conteúdo (além dos 76 do batimento estrutural)
- **6 divergências temporais** (>31 dias entre `report_date` e `data_de_movimento`)

---

## 7. Validação e Qualidade

### Métricas do dump (`dados_historicos_formatados`)

| Métrica | Status anterior (v1) | Status atual |
|---------|---------------------|--------------|
| Status `treated` | 385/395 (97.5%) | 385/395 (97.5%) |
| Erros no banco | 0 | 0 |
| Descartes | 10 | 10 |
| `missing_pct > 30%` | 13 tabelas | Reduzido (correções de encoding e sub-tabelas) |
| `institution` desconhecida | 80 tabelas | Reduzido (ampliação de regex de período) |
| `report_date` ausente | 99 tabelas | Reduzido (3 novos padrões regex) |

### O que foi melhorado desde a v1

- **Preservação de `data_de_movimento`**: coluna 0 não é mais descartada como row index
- **Extração de `report_date`**: 3 novos padrões regex cobrem prefixos truncados (`ecente_`, `storico_`, `o_recente_`)
- **5 tabelas pipe BB 2013**: resgatadas de descarte com mapeamento de colunas fixo
- **Correções de encoding**: `charset-normalizer` + `ftfy` em todas as colunas string
- **Limpeza de sub-tabelas**: remoção de colunas 100% vazias, blocos melhor separados

### Pendências conhecidas

| Item | Prioridade |
|------|-----------|
| 35 discrepâncias amostradas na auditoria — algumas `sub_tabelas_3` com missing alto | Média |
| `institution` ainda desconhecida para ~80 tabelas (nomes sem prefixo `bb_`/`caixa_`) | Baixa |
| `report_date` ainda ausente para tabelas com datas textuais/abreviadas (`25jan2011`, `22mar11`) | Baixa |
| Verificação final do schema `sftp_tratado` (bloqueado por disco cheio no servidor) | Alta |

---

## 8. Artefatos e Documentação

### Código-fonte

| Módulo | Função |
|--------|--------|
| `src/classificacao/regras.py` | Árvore de decisão R1–R8, 17 categorias |
| `src/classificacao/tratamento.py` | Pipeline G0–G8, roteador principal |
| `src/classificacao/tratamento_cabecalho.py` | Cabeçalho deslocado (7 subtipos) e composto (2 subtipos) |
| `src/classificacao/tratamento_especiais.py` | Pipe, sem cabeçalho, vazias |
| `src/classificacao/tratamento_subtabelas.py` | Sub-tabelas (4 subtipos) |
| `src/classificacao/tratamento_pipe_single_col.py` | Pipe single-column BB (DB mode) |
| `src/classificacao/tratamento_sftp.py` | Canonicalização GEFUS, Padrão B, metadados SFTP |
| `src/classificacao/reconstrucao_colunas.py` | Reconstrução Padrão A |
| `src/classificacao/deduplicacao.py` | Hash MD5, agrupamento, eleição de canônicas |
| `src/classificacao/validacao.py` | Validação pós-tratamento |
| `src/classificacao/saida_tratamento.py` | Escrita de CSVs e relatório de qualidade |
| `src/sftp/` | Batimento estrutural, matching, relatórios SFTP↔dump |
| `main.py` | Entry point — orquestra todos os pipelines |

### Documentação

| Documento | Conteúdo |
|-----------|----------|
| `docs/relatorio_classificacao.md` | Distribuição das 753 tabelas por categoria |
| `docs/relatorio-tratamento-v1.md` | Detalhamento de transformações por categoria (603 linhas) |
| `docs/guia-revisao-classificacao.md` | Árvore de decisão, thresholds, heurísticas |
| `docs/guia-revisao-tratamento.md` | Pipeline Groups 0–8, validação, qualidade (255 linhas) |
| `classificacao_formacao_revisado_autoritativo.md` | Gabarito manual de referência (197 linhas) |
| `data/sftp/relatorios/relatorio_batimento_dump_sftp_v2.md` | Batimento estrutural SFTP × dump |
| `data/sftp/relatorios/relatorio_tabelas_pipe.md` | Análise de 304 tabelas SFTP com pipe |
| `data/sftp/relatorios/resumo_batimento.md` | Resumo executivo do batimento |
| `docs/resposta-revisao-tratamento-dados.md` | Resultado da auditoria de tratamento |

### Artefatos de dados

| Arquivo | Descrição |
|---------|-----------|
| `data/classificacao_formacao.csv` | Classificação das 753 tabelas |
| `data/treated_tables/_quality_report.csv` | Métricas de qualidade (445 entradas) |
| `data/treated_tables/_dedup_map.csv` | Mapeamento de duplicatas |
| `data/sftp/analise/mapeamento_gefus.csv` | Pares de canonicalização GEFUS (1453 pares) |
| `data/sftp/analise/tabelas_relacionadas.csv` | Batimento estrutural SFTP↔dump (1510 pares) |
| `data/sftp/analise/tabelas_identicas_sftp_dump.csv` | Tabelas SFTP com equivalente exato no dump (1299) |
| `data/sftp/analise/cruzamento_conteudo.csv` | Cruzamento por conteúdo via APF |

---

## 9. Como Executar e Verificar

```bash
# Pipeline completo do dump (classificação + dedup + tratamento)
uv run python main.py --db

# Apenas classificação
uv run python main.py --classify-only

# Reprocessar tratamento com classificação existente
uv run python main.py --db --skip-classify

# Pipeline SFTP (requer --db)
uv run python main.py --db --skip-classify --sftp

# Testes
uv run pytest tests/ -q
uv run ruff check .
uv run mypy .
```

### Verificação de qualidade

```sql
-- Tabelas tratadas no dump
SELECT status, COUNT(*) FROM dados_historicos_formatados._qualidade GROUP BY status;

-- Tabelas SFTP classificadas (quando schema disponível)
SELECT treatment, COUNT(*) FROM sftp_tratado._classificacao GROUP BY treatment;

-- Tabelas com canonical quebrada
SELECT * FROM sftp_tratado._canonicas WHERE canonical_has_pipe = true;

-- Tabelas SFTP com equivalente no dump
SELECT * FROM sftp_tratado._classificacao WHERE dump_equivalent IS NOT NULL;
```

---

## 10. Resumo para a Issue #59

### Critérios de aceite — Status

| Critério | Status |
|----------|--------|
| Dados foram tratados conforme regras definidas | ✅ 440 tabelas tratadas (dump); SFTP em andamento |
| Inconsistências foram reduzidas ou justificadas | ✅ Correções de encoding, datas e tipos aplicadas |
| Campos críticos foram validados | ✅ Validação de shape, chaves e missing_pct |
| Regras aplicadas foram documentadas | ✅ 4 guias + 2 relatórios + código comentado |
| Dados estão prontos para cruzamento e modelagem | ⚠️ Dump: sim (97.5% treated, 0 erros). SFTP: pendente verificação DB |

### Próximos passos

1. **Liberar espaço em disco** no servidor PostgreSQL (10.0.0.50) para concluir verificação do `sftp_tratado`
2. **Executar** `uv run python main.py --db --skip-classify --sftp` para validar pipeline SFTP completo
3. **Verificar** as 2 canônicas quebradas (`m20211028`, `m20230511`) recebendo split corretamente
4. **Revisar** 35 discrepâncias remanescentes da auditoria de tratamento
5. **Preencher** `institution` e `report_date` para tabelas com metadados ausentes (baixa prioridade)
