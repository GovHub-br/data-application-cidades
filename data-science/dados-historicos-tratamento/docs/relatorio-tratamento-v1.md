# Relatório de Tratamento de Tabelas — Versão 1

**Projeto:** Pipeline de classificação e tratamento de dados históricos do MCMV
**Escopo:** Transformação de ~753 amostras CSV do dump PostgreSQL + modo PostgreSQL direto (`--db`)
**Data da versão:** Junho 2026

---

## 1. Visão Geral

O pipeline de tratamento (Stage 3) recebe tabelas classificadas estruturalmente pelo Stage 1 (17 categorias de formação) e aplica transformações específicas conforme o tipo estrutural. O roteamento é feito por `tratar_tabela()` em `tratamento.py`, que despacha cada tabela para o handler especializado correspondente.

### Modos de execução

| Modo | Comando | Fonte de dados | Destino |
|---|---|---|---|
| **CSV** (padrão) | `uv run python main.py` | Amostras de 200 linhas em `data/table_samples/` | `data/treated_tables/` |
| **DB** | `sudo ip netns exec protected sudo -u $USER ./.venv/bin/python main.py --db` | Tabelas completas no PostgreSQL (schema `dados_historicos`) | PostgreSQL (schema `dados_historicos_formatados`) + CSV local |
| **Skip classify** | `--skip-classify` | Carrega classificação existente, reprocessa tratamento | Igual ao modo base |
| **Classify only** | `--classify-only` | Só classifica, sem tratamento | `data/classificacao_formacao.csv` |

No modo DB, a classificação e o tratamento operam sobre tabelas completas (não amostras), podendo gerar divergências registradas em `data/relatorio_divergencias_db.md`.

### Distribuição das 753 tabelas por categoria de formação

| Categoria | Quantidade | Tratamento |
|---|---|---|
| `bem_formada` | 351 | Pipeline completo de normalização |
| `sub_tabelas_1` | 273 | Wide-to-long + classificação de recortes |
| `cabecalho_composto_2` | 50 | Concatenação de header (2 linhas) + normalização |
| `cabecalho_na_primeira_linha_1` | 36 | Promoção de header + normalização |
| `sub_tabelas_3` | 19 | Reconstrução de header multi-linha + normalização |
| `separador_\|` | 5 | Expansão de pipe + normalização |
| `cabecalho_composto_1` | 4 | Concatenação de header (3 linhas) + normalização |
| `dados_sem_utilidade` | 4 | Descartes (sem saída) |
| `sem_cabecalho` | 3 | Inferência de colunas + normalização |
| `cabecalho_na_segunda_linha` | 2 | Promoção de header (2ª linha) + normalização |
| `sub_tabelas_2` | 1 | Split por keywords + concatenação |
| `cabecalho_na_primeira_linha_2` | 1 | Promoção + remoção de totalização + normalização |
| `cabecalho_na_terceira_linha_1` | 1 | Promoção de header (3ª linha) + normalização |
| `cabecalho_na_terceira_linha_2` | 1 | Promoção de header (3ª linha) + normalização |
| `sub_tabelas_4` | 1 | Split por blocos text-heavy + normalização |
| `vazia` | 1 | Descarte (sem saída) |

**Resultado:** 440 tabelas tratadas + 5 descartes = 445 entradas no `_quality_report.csv`.

---

## 2. Tipos de Tratamento

### 2.1 Tabelas `bem_formada` — Pipeline Completo de Normalização

**Aplica-se a:** 351 tabelas (46.6% do total). Inclui os perfis `colunar_denso`, `event_level`, `agregado_uf` e `lookup`.

**Transformações aplicadas (em ordem):**

| Grupo | Etapa | Descrição |
|---|---|---|
| G0 | Limpeza pré-tratamento | Remove linhas totalmente vazias ou compostas apenas por `-`; remove colunas 100% NaN |
| G1 | Normalização de nomes de coluna | lowercase, remoção de acentos (NFKD), substituição de caracteres especiais por `_`, colapso de underscores múltiplos |
| G2 | Detecção e conversão de separador decimal | Amostra 20 valores; se >50% contêm vírgula, substitui `,` por `.` e converte para `float64` |
| G3 | Parsing de datas | Detecta formato ISO (`YYYY-MM-DD`) ou BR (`DD/MM/YYYY`); aplica `pd.to_datetime` em colunas com prefixo `dat_`/`dt_`/`data_`. Segurança: se >10% dos valores não-nulos falham, reverte ao tipo original |
| G4 | Reparo de encoding | Usa `charset-normalizer` para detectar encoding não-UTF-8 (confiança > 0.7) e redecodificar; fallback com `ftfy.fix_text()` |
| G5 | Canonização de tipos | Regras manuais (ex.: `cod_`/`nr_`/`nu_` → `str`, `vlr_`/`valor_`/`total_` → `float64`, `qtd_`/`qt_`/`unidades` → `Int64`, `dat_`/`dt_`/`data_` → `datetime64[ns]`, `cnpj` → `str`) + fallback automático via inventário PostgreSQL (`data/columns_*.csv`) |
| G6 | Classificação de perfil | Heurística por número de colunas: `lookup` (≤3), `event_level` (4-6 com chaves repetidas), `agregado_uf` (7-9 com totalizações), `colunar_denso` (demais) |
| G7 | Extração de período e instituição | Data do nome do arquivo (5 padrões regex) + instituição (`bb_`/`caixa_`/`unknown`) |
| G8 | Adição de metadados | Colunas: `source_table`, `report_date`, `institution`, `profile`, `content_hash` |

**Exemplo: `001_2012_10_outubro_20121009_bases_relatório_executivo_0910201`**
Classificação: `bem_formada` → Perfil: `colunar_denso`

#### Antes (original — tab-separated, 35 colunas, 200 linhas)

```
uf  codmunicibge  municipio        recortes_territoriais  produto                    faixa    faixa_divisao                              unidades
MG  314310        Monte Carmelo    Demais                 CCFGTS Imóvel na planta    Faixa 2  Faixa 2 - De R$ 1.600,00 à R$ 3.100,00  23
RJ  330455        RIO DE JANEIRO   Capital                CCFGTS Apoio à produção    Faixa 2  Faixa 2 - De R$ 1.600,00 à R$ 3.100,00  139
SE  280030        ARACAJU          Capital                CCFGTS Imóvel na planta    Faixa 2  Faixa 2 - De R$ 1.600,00 à R$ 3.100,00  192

Colunas originais (excertos):
  - 'valor_do_empréstimo'          → contém acento, mantido como string
  - 'endereço_do_empreendimento'   → contém acentos e ç
  - 'administração_por_condomínio' → acentos
  - 'data_prevista_término_obra'   → acentos
  - 'valor_total__liberado'        → underscore duplo
  - '_de_obra_executada'           → underscore inicial
  - 'trabalho_técnico_social_tts'  → acentos
```

#### Depois (tratado — tab-separated, 40 colunas, 200 linhas)

```
uf  codmunicibge  municipio        recortes_territoriais  produto                    faixa    faixa_divisao                              unidades  ...  source_table                                            report_date  institution  profile        content_hash
MG  314310        Monte Carmelo    Demais                 CCFGTS Imóvel na planta    Faixa 2  Faixa 2 - De R$ 1.600,00 à R$ 3.100,00  23        ...  001_2012_10_outubro_20121009_bases_relatório_executivo_0910201  2012-10-01   unknown       colunar_denso  284f3369e56056afb2e2444f8b489342
RJ  330455        RIO DE JANEIRO   Capital                CCFGTS Apoio à produção    Faixa 2  Faixa 2 - De R$ 1.600,00 à R$ 3.100,00  139       ...  001_2012_10_outubro_20121009_bases_relatório_executivo_0910201  2012-10-01   unknown       colunar_denso  284f3369e56056afb2e2444f8b489342
SE  280030        ARACAJU          Capital                CCFGTS Imóvel na planta    Faixa 2  Faixa 2 - De R$ 1.600,00 à R$ 3.100,00  192       ...  001_2012_10_outubro_20121009_bases_relatório_executivo_0910201  2012-10-01   unknown       colunar_denso  284f3369e56056afb2e2444f8b489342

Transformações aplicadas nas colunas:
  'valor_do_empréstimo'           → 'valor_do_emprestimo'           (acentos removidos)
  'endereço_do_empreendimento'    → 'endereco_do_empreendimento'    (ç → c)
  'administração_por_condomínio'  → 'administracao_por_condominio'  (acentos removidos)
  'data_prevista_término_obra'    → 'data_prevista_termino_obra'    (acentos removidos)
  'valor_total__liberado'         → 'valor_total_liberado'          (underscore duplo colapsado)
  '_de_obra_executada'            → 'de_obra_executada'             (underscore inicial removido)
  'trabalho_técnico_social_tts'   → 'trabalho_tecnico_social_tts'  (acentos removidos)

Colunas de metadados adicionadas:
  + source_table, report_date, institution, profile, content_hash
```

**Destaques das transformações:**
- **G1 – Nomes:** Acentos removidos via `unicodedata.normalize('NFKD')`, caracteres especiais substituídos por `_`, múltiplos underscores colapsados
- **G5 – Tipos:** Colunas com prefixo `cod_`/`nr_` mantidas como `str` (ex.: `codmunicibge`, `cnpj`); colunas com prefixo `vlr_`/`valor_` convertidas para `float64`; colunas com prefixo `dat_` convertidas para `datetime64[ns]`
- **G7 – Período:** Extraído do nome do arquivo via regex — `20121009` → `2012-10-01` (padrão YYYYMM no início)
- **G7 – Instituição:** Nome não contém `bb_` nem `caixa_` → `unknown`

---

### 2.2 Cabeçalho Deslocado — Promoção e Normalização

**Aplica-se a:** 95 tabelas com header em posição incorreta (7 subtipos).

#### 2.2.1 `cabecalho_na_primeira_linha_1` (36 tabelas)

**Problema:** Primeira coluna nomeada, demais `unnamed_N`. O cabeçalho real está na linha 0 (primeira linha de dados).

**Transformação:** Promove a linha 0 para cabeçalho, descarta a linha 0 dos dados. Depois aplica o pipeline `bem_formada`.

**Exemplo: `aixa_001_2015_12_dezembro_bases_relatório_executivo_31122015v2`**

*Antes (35 colunas, 200 linhas):*
```
contratação_empreendimentos_mcmv_1__2_e_3__caixa  unnamed_1  unnamed_2       unnamed_3          unnamed_4                unnamed_5                   unnamed_6  unnamed_7
cMCMV                                              UF          MUNICIPIO        COD_MUNIC_IBGE      Recortes Territoriais     PRODUTO                     FAIXA      Faixa_Divisao          ← linha 0: header real
II                                                 RJ          RIO DE JANEIRO   330455              Capital                   CCFGTS Imóvel na planta     Faixa 2    Faixa 2 - De R$ 0,00...
II                                                 RJ          RIO DE JANEIRO   330455              Capital                   CCFGTS Imóvel na planta     Faixa 2    Faixa 2 - De R$ 0,00...
```

*Depois (39 colunas, 199 linhas):*
```
cmcmv  uf  municipio        cod_munic_ibge  recortes_territoriais  produto                    faixa    faixa_divisao                           ...
II     RJ  RIO DE JANEIRO   330455          Capital                 CCFGTS Imóvel na planta    Faixa 2  Faixa 2 - De R$ 0,00 à R$ 3.275,00   ...
II     RJ  RIO DE JANEIRO   330455          Capital                 CCFGTS Imóvel na planta    Faixa 2  Faixa 2 - De R$ 0,00 à R$ 3.275,00   ...
II     SP  JARDINÓPOLIS     352510          Demais                  CCFGTS Imóvel na planta    Faixa 2  Faixa 2 - De R$ 0,00 à R$ 3.275,00   ...
```

**O que mudou:** `unnamed_1` → `uf`, `unnamed_2` → `municipio`, etc. (nomes promovidos da linha 0). Nomes normalizados (G1). Tipos canonizados (G5).

---

#### 2.2.2 `cabecalho_na_primeira_linha_2` (1 tabela)

**Problema:** Igual ao `_1`, mas com linha de totalização no final.

**Transformação adicional:** Remove a última linha de dados se >50% dos seus valores forem numéricos (linha de total).

**Exemplo: `bb_2011_02_fevereiro_dados_22022011`**

*Antes:* Header na linha 0, última linha com valores como `Total`, `12345`, `67890` (totalização).

*Depois:* Header promovido; linha de totalização removida; pipeline `bem_formada` aplicado.

---

#### 2.2.3 `cabecalho_na_segunda_linha` (2 tabelas)

**Problema:** Linha 0 contém metadado `"Posicao: DD/MM/YYYY"`; linha 1 contém o cabeçalho real. Todas as colunas exceto a primeira são `unnamed`.

**Transformação:** Descarta linha 0, promove linha 1 para cabeçalho. Pipeline `bem_formada`.

**Exemplo: `caixa_001_2011_11_novembro_relat_rio_executivo_mcid_21_11_11`**

*Antes (9 colunas, 200 linhas):*
```
relatório_executivo__pmcmv__ministério_das_cidades  unnamed_1  unnamed_2  unnamed_3  unnamed_4  unnamed_5  unnamed_6  unnamed_7
nan                                                  nan         nan         nan         nan         nan         nan         Posicao:21/11/2011     ← linha 0: metadado
codIBGE                                              Municipio   UF          Faixa       Produto     UH          Valor       UH_Concluidas         ← linha 1: header real
110001                                               Alta Floresta D'Oeste  RO  2    CCFGTS - Faixa 1  2  122157  2
```

*Depois (13 colunas, 198 linhas):*
```
codibge  municipio                uf  faixa  produto             uh  valor    uh_concluidas  uh_entregues  source_table  ...  institution
110001   Alta Floresta D'Oeste    RO  2      CCFGTS - Faixa 1    2   122157   2              2             ...           caixa
110001   Alta Floresta D'Oeste    RO  2      CCFGTS - Faixa 2    2   140267   2              4             ...           caixa
110002   Ariquemes                RO  2      CCFGTS - Faixa 1    16  1246335  16             26            ...           caixa
```

**O que mudou:** Linha com `"Posicao:21/11/2011"` removida; `unnamed_*` → nomes reais (`Municipio`, `UF`, etc.); nomes normalizados (`codIBGE` → `codibge`); metadados adicionados; instituição `caixa` inferida.

---

#### 2.2.4 `cabecalho_na_terceira_linha_1` (1 tabela)

**Problema:** Linhas 0-1 vazias; linha 2 contém o cabeçalho real. Todas as colunas `unnamed`.

**Transformação:** Descarta linhas 0-1, promove linha 2 para cabeçalho. Pipeline `bem_formada`.

**Exemplo: `caixa_001_2009_12_dezembro_2009_pmcmv_24_12_2009_parte2`**

*Antes (14 colunas, 200 linhas):*
```
unnamed_0  unnamed_1  unnamed_2  unnamed_3  unnamed_4  unnamed_5  unnamed_6  unnamed_7  unnamed_8
nan        nan         nan         nan         nan         nan         nan         nan         nan        ← linhas 0-1: vazias
nan        nan         nan         nan         nan         nan         nan         nan         nan
Nome Empreendimento  Quant. Unidades - Total  Valor Unidades - Total  Produto  Código IBGE  Nome Municipio  REGIAO  UF  ← linha 2: header
PMCMV - RES NORTH PARK                       64                      4800000   CCFGTS       240810          NATAL   NORDESTE  RN
```

*Depois (18 colunas, 197 linhas):*
```
nome_empreendimento                             quant_unidades_total  valor_unidades_total  produto     codigo_ibge  nome_municipio  regiao    uf
PMCMV - RES NORTH PARK                          64                    4800000.0             CCFGTS      240810       NATAL           NORDESTE  RN
SPAZIO YPE BRANCO                               110                   3296000.46            CCFGTS      355030       SÃO PAULO       SUDESTE   SP
RES PAULO FONTELES II - PMCMV -RECURSOS FAR     224                   9620654.4             FAR ALIENACAO 150080     ANANINDEUA      NORTE     PA
```

**O que mudou:** 2 linhas vazias removidas; nomes de coluna normalizados (`Nome Empreendimento` → `nome_empreendimento`); `Valor Unidades - Total` convertido para `float64` (vírgula → ponto decimal).

---

#### 2.2.5 `cabecalho_composto_1` (4 tabelas)

**Problema:** Header mesclado de 3 linhas (células merge do Excel representadas por NaN). Exemplo: `"Unidade da Federação"` na linha 0, `"Contratados"` na linha 1 com sub-colunas `"Empr"` e `"UH"` na linha 2.

**Transformação:** Forward-fill horizontal nas 3 linhas de header (preenche células mescladas). Concatena as 3 linhas com `_` para formar nome único. Remove rodapé de metadados se detectado. Pipeline `bem_formada`.

**Exemplo: `001_2011_08_agosto_previsão_de_conclusão_e_entrega_far_modelo`**

*Antes (14 colunas, 38 linhas):*
```
unnamed_0  unnamed_1  unnamed_2  tabela_1__distribuição_pmcmvfar_por_uf  unnamed_4    unnamed_5  unnamed_6   unnamed_7
nan        nan         nan         Unidade da Federação                    Contratados  nan        Concluídos  nan               ← linha 0
nan        nan         nan         nan                                     Empr         UH         Empr        UH                ← linha 1
nan        nan         nan         nan                                     nan          nan        nan         nan               ← linha 2 (vazia)
nan        nan         nan         Norte                                   89           35123      2           350               ← dados
```

*Depois (18 colunas, 33 linhas):*
```
col_0  col_1  col_2  unidade_da_federacao  contratados_empr  contratados_uh  concluidos_empr  concluidos_uh  entregues_empr  entregues_uh  ...
nan    nan    nan    Norte                 89                35123           2                350            2               350           ...
nan    nan    nan    RO                    9                 1502            0                0              0               0             ...
nan    nan    nan    AC                    10                1873            0                0              0               0             ...
```

**O que mudou:** Header mesclado de 3 linhas concatenado: `Unidade da Federação` + `Contratados` + `Empr` → `contratados_empr`; `Unidade da Federação` + `Concluídos` + `UH` → `concluidos_uh`. Rodapé removido. Pipeline `bem_formada` aplicado.

---

#### 2.2.6 `cabecalho_composto_2` (50 tabelas)

**Problema:** Header mesclado de 2 linhas (células merge do Excel). Primeira linha contém títulos de seção (ex.: `"Desligamento - Plano Piloto"`, `"Aprovado CEF - Financ à Produção PJ e Repasse"`). Segunda linha contém os nomes de coluna (`"UF"`, `"Público Alvo"`, `"Unidades cadastradas"`, etc.).

**Transformação:** Forward-fill horizontal nas 2 linhas de header, concatena com `_`. Pipeline `bem_formada`.

**Exemplo: `b_2012_04_abril_cópia_de_rel_min_cidades_valores_25_04_2012_bd`**

*Antes (12 colunas, 89 linhas):*
```
relatório_01_total_das_contratações_por_ufregião_geográfic  unnamed_1   unnamed_2              unnamed_3       unnamed_4       ...
Desligamento - Plano Piloto                                  nan          nan                     nan             nan             Aprovado CEF - Financ à Produção PJ e Repasse  ...  ← linha 0 (seções)
UF                                                           Público Alvo Unidades cadastradas   Unid Financ PF  Total Financ PF UH Produzidas                                  ...  ← linha 1 (colunas)
DF                                                           Faixa I      0                       0               0               0                                               ...  ← dados
```

*Depois (16 colunas, 87 linhas):*
```
desligamento_plano_piloto_uf  desligamento_plano_piloto_publico_alvo  desligamento_plano_piloto_unidades_cadastradas  ...  aprovado_cef_financ_a_producao_pj_e_repasse_uh_produzidas  ...
DF                            Faixa I                                 0                                                ...  0                                                           ...
nan                           Faixa II                                0                                                ...  79                                                          ...
nan                           Faixa III                               4565                                             ...  0                                                           ...
```

**O que mudou:** Header mesclado concatenado. Exemplo: `"Desligamento - Plano Piloto"` + `"UF"` → `desligamento_plano_piloto_uf`; `"Aprovado CEF - Financ à Produção PJ e Repasse"` + `"UH Produzidas"` → `aprovado_cef_financ_a_producao_pj_e_repasse_uh_produzidas`. Nova estrutura mantém a hierarquia original de seções.

---

### 2.3 Tabelas `sem_cabecalho` — Inferência de Nomes de Coluna

**Aplica-se a:** 3 tabelas. A primeira linha de dados foi tratada como cabeçalho pelo carregamento CSV, mas na verdade não existe cabeçalho — todas as linhas são dados.

**Transformação:**
1. Tenta encontrar uma tabela de referência no inventário PostgreSQL (mesmo número de colunas, mesma instituição). Se encontrada, usa os nomes de coluna da referência.
2. Se não encontrada, atribui nomes genéricos: `col_0`, `col_1`, `col_2`, ...
3. Aplica pipeline `bem_formada`.

**Exemplo: `bb_2019_2019_05_07_2019_05_07_pj`**

*Antes (21 colunas, 200 linhas):*
```
600182  354260  1  far  214  1498000000  2  100  000  jardim_virgínia  antiga_colonia_de_registro_gleba_c  construtora_registrense_ltda__me  ...
466787  210675  1  FAR  660  35640000,00 2  72,33  ...                                                                    ← dados reais usados como header falso
702536  211270  1  FAR  600  34200000,00 2  85,69  ...
```

*Depois (25 colunas, 200 linhas):*
```
col_0    col_1    col_2  col_3  col_4  col_5        col_6  col_7  col_8  col_9            ...
466787   210675   1      FAR    660    35640000,00  2      72,33  ...                      ← referência não encontrada: nomes genéricos
702536   211270   1      FAR    600    34200000,00  2      85,69  ...
541088   412240   1      FAR    47     2834387,76   2      100    ...
```

**O que mudou:** Primeira linha `(600182, 354260, ...)` que era falsamente tratada como cabeçalho agora é linha de dados. Nomes genéricos `col_0`, `col_1`, ... atribuídos (referência não encontrada no inventário). Warning adicionado: `"nomes genericos (col_0...) - sem referencia no inventario"`.

---

### 2.4 `separador_|` — Expansão de Valores com Pipe

**Aplica-se a:** 5 tabelas onde os dados estão em uma única coluna, separados por `|` (pipe).

**Transformação:**
1. Carrega o CSV com tab (formato padrão)
2. Toma a primeira coluna de dados e expande cada valor via `str.split("|", expand=True)`
3. Nomeia as colunas resultantes como `campo_0`, `campo_1`, `campo_2`, ...
4. Aplica pipeline `bem_formada`

**Exemplo: `bb_2013_06_junho_pmcmv_18062013_tab_andamento_obras`**

*Antes (1 coluna, 200 linhas):*
```
cod_operacaocod_sit_obracod_regime_construcaocod_pendencia_obra
23157|0|0|0|||||||2013-04-30|2010-12-28|0.00|100.00|531480.52|2011-05-12|2012-05-30
27175|6|0|0|||||||2011-09-13|2011-08-03|0.00|100.00|560888.97|2011-09-13|2012-02-29
29302|0|0|0||||||||2010-07-05|0.00|0.00|742279.93|2012-05-09|2010-08-18
```

*Depois (21 colunas, 200 linhas):*
```
campo_0  campo_1  campo_2  campo_3  campo_4  campo_5  ...  campo_12   campo_13   campo_14     campo_15     campo_16     source_table  ...  profile
23157    0        0        0        nan       nan       ...  2010-12-28 0.00       100.00       531480.52   2011-05-12   ...           separador_pipe
27175    6        0        0        nan       nan       ...  2011-08-03 0.00       100.00       560888.97   2011-09-13   ...           separador_pipe
29302    0        0        0        nan       nan       ...  2010-07-05 0.00       0.00         742279.93   2012-05-09   ...           separador_pipe
```

**O que mudou:** Uma única coluna com valores pipe-separados → 21 colunas individuais. Campos vazios (`||||`) tornam-se `NaN`. Perfil reportado como `separador_pipe`.

---

### 2.5 Sub-Tabelas — Extração e Reconstrução

**Aplica-se a:** 294 tabelas com múltiplas sub-tabelas embutidas (4 subtipos).

#### 2.5.1 `sub_tabelas_1` (273 tabelas) — Wide-to-Long com Classificação de Recortes

**Problema:** Tabelas com rótulos na primeira coluna (ex.: `FGTS`, `FAR`, `0 a 3 SM`) e timestamps nas colunas restantes (`YYYYMMDD_hhmmss`). Sub-tabelas separadas por ≥2 linhas vazias.

**Transformação:**
1. Divide o DataFrame em sub-tabelas por ≥2 linhas vazias consecutivas
2. Identifica `recorte_tipo` pelo rótulo da primeira coluna: `frente` (FGTS/FAR/Entidades/PNHR/PNHB), `faixa` (X a Y SM), `suat`, `uf`, `regiao`, `total`, `outro`
3. Transforma cada sub-tabela do formato wide (rótulo + timestamps) para long (`recorte_tipo`, `recorte_valor`, `data_referencia`, `valor`)
4. Concatena todas as sub-tabelas

**Exemplo: `001_2012_02_fevereiro_base_pmcmv_relatorio_executivo_10_02_2012`**

*Antes (4 colunas, 200 linhas — formato wide):*
```
unnamed_0  20090401_000000  20090501_000000  20090601_000000
FGTS       12               36               46                   ← Sub-tabela 1: frentes
FAR        22               62               68
(linhas vazias)
0 a 3 SM   34               98               114                  ← Sub-tabela 2: faixas
3 a 6 SM   122              184              186
6 a 10 SM  112              108              109
(linhas vazias)
Total      268              390              409                  ← Sub-tabela 3: total
(linhas vazias)
SUAT A     35               35               35                   ← Sub-tabela 4: suat
SUAT B     35               35               35
```

*Depois (8 colunas, ~200 linhas — formato long):*
```
recorte_tipo  recorte_valor  data_referencia  valor  source_table                                            report_date  institution  profile
frente        FGTS           2009-04-01       12.0   001_2012_02_fevereiro_base_pmcmv_relatorio_executivo_10_02_2012  2012-02-10   unknown       sub_tabelas_1
frente        FGTS           2009-05-01       36.0   ...
frente        FGTS           2009-06-01       46.0   ...
frente        FAR            2009-04-01       22.0   ...
frente        FAR            2009-05-01       62.0   ...
frente        FAR            2009-06-01       68.0   ...
faixa         0 a 3 SM       2009-04-01       34.0   ...
faixa         0 a 3 SM       2009-05-01       98.0   ...
faixa         0 a 3 SM       2009-06-01       114.0  ...
faixa         3 a 6 SM       2009-04-01       122.0  ...
faixa         3 a 6 SM       2009-05-01       184.0  ...
faixa         3 a 6 SM       2009-06-01       186.0  ...
faixa         6 a 10 SM      2009-04-01       112.0  ...
faixa         6 a 10 SM      2009-05-01       108.0  ...
faixa         6 a 10 SM      2009-06-01       109.0  ...
total         Total          2009-04-01       268.0  ...
suat          SUAT A         2009-04-01       35.0   ...
suat          SUAT B         2009-04-01       35.0   ...
```

**O que mudou:** 4 colunas (rótulo + 3 timestamps) × várias sub-tabelas → 4 colunas canônicas (`recorte_tipo`, `recorte_valor`, `data_referencia`, `valor`) com todas as medições em formato long. Timestamps `20090401_000000` → `2009-04-01`. Valores convertidos para `float`. Recortes classificados automaticamente.

---

#### 2.5.2 `sub_tabelas_2` (1 tabela) — Split por Palavras-Chave

**Problema:** Múltiplas sub-tabelas delimitadas por palavras-chave (`SÍNTESE`, `Faixa`, `Região`, `Quadro de Valores`) na primeira coluna.

**Transformação:** Divide nos pontos onde a primeira coluna contém uma keyword. Adiciona `sub_table_index`. Concatena.

**Exemplo: `_001_2012_11_novembro_sintese_20121128_evento_1_milhao_entregas`**

*Antes (7 colunas, 42 linhas):*
```
unnamed_0  unnamed_1                                                           unnamed_2  unnamed_3 ...
nan        1 milhão de moradias entregues e 2 milhões de contratadas           nan         nan        ← Bloco 0
nan        SÍNTESE                                                             nan         nan
nan        2012-11-28 00:00:00                                                 nan         nan
...
nan        Faixa                                                                nan         nan        ← Bloco 1: nova keyword
...
```

*Depois (12 colunas, ~42 linhas):*
```
unnamed_0  unnamed_1                                                           unnamed_2  unnamed_3  ...  sub_table_index  source_table  ...
nan        1 milhão de moradias entregues e 2 milhões de contratadas           nan         nan        ...  0                ...
nan        SÍNTESE                                                             nan         nan        ...  0                ...
```

**O que mudou:** Coluna `sub_table_index` adicionada para identificar a qual sub-tabela cada linha pertence. Metadados de origem, data, instituição e perfil adicionados.

---

#### 2.5.3 `sub_tabelas_3` (19 tabelas) — Reconstrução de Header Multi-Linha

**Problema:** Sub-tabelas separadas por linhas vazias, cada uma com header multi-linha (2-3 linhas com células mescladas representadas por NaN).

**Transformação:**
1. Divide o DataFrame em sub-tabelas por ≥1 linha vazia
2. Reconstrói o header de cada bloco: forward-fill nas linhas de header (células mescladas), concatena com `_`, garante unicidade
3. Adiciona `sub_table_index`
4. Alinha colunas entre todos os blocos
5. Aplica pipeline `bem_formada`

**Exemplo: `bb_2011_01_janeiro_rel_11jan2011`**

*Antes (12 colunas, 161 linhas):*
```
unnamed_0  relatório_01_total_das_contratações_por_ufregião_geográfic  unnamed_2                  unnamed_3                                      unnamed_4     ...
nan        nan                                                          Desligamento - Plano Piloto nan                                            nan           ...  ← linha 0 (seção)
nan        UF                                                           Quantidade unidades         Total de Financ PF Contratados (Desligamento)  Meta R$       ...  ← linha 1 (colunas)
nan        DF                                                           nan                         0                                              15750000      ...  ← dados
```

*Depois (39 colunas, ~150 linhas — com `sub_table_index`):*
```
...  aprovado_cef_meta_r_20000000  aprovado_cef_quantidade_unidades_0  contratacao_total_quantidade_unidades_0  ...  sub_table_index  source_table  ...
...  nan                            nan                                  nan                                      ...  0                ...
```

**O que mudou:** Header mesclado de 2-3 linhas reconstruído (ex.: `Aprovado CEF` + `Meta R$` + `20000000` → nome composto). Colunas alinhadas entre todos os blocos via union. `sub_table_index` preservado.

---

#### 2.5.4 `sub_tabelas_4` (1 tabela) — Blocos Text-Heavy

**Problema:** Sub-tabelas delimitadas por blocos text-heavy (3+ linhas consecutivas com ≥2 valores de texto cada).

**Transformação:** Similar a `sub_tabelas_3`: detecta blocos, reconstrói header multi-linha, adiciona `sub_table_index`, pipeline `bem_formada`.

**Exemplo: `caixa_001_2012_07_julho_bases_relatório_executivo_24_07_12`**

*Antes (15 colunas, 45 linhas):*
```
unnamed_0  unnamed_1                           unnamed_2  unnamed_3  unnamed_4  unnamed_5                          unnamed_6  ...
nan        PROGRAMA MINHA CASA MINHA VIDA      nan         nan         nan         nan                                 nan         ← Bloco 0: título
nan        UF                                   PMCMV 1    nan         nan         nan                                 nan         ← Bloco 0: header
nan        Município                            nan         nan         nan        POSIÇÃO DE CONTRATAÇÃO E ENTREGA:   nan         ← Bloco 0: header
```

*Depois (47 colunas, ~40 linhas):*
```
pmcmv_2_2012_07_24_00_00_00_estagio_de_execucao  pmcmv_2_contratacoes_uh  ...  sub_table_index  source_table  ...
nan                                                nan                      ...  0                ...
```

**O que mudou:** Header multi-linha de cada bloco concatenado. Exemplo: `PMCMV 2` + `2012-07-24 00:00:00` + `Estágio de Execução` → nome composto com `_`. Blocos text-heavy identificados e extraídos.

---

### 2.6 Descartes — Tabelas Descartadas

**Aplica-se a:** 5 tabelas que não geram CSV de saída.

#### 2.6.1 `vazia` (1 tabela)

**Exemplo: `caixa_001_2016_bext_31102016`**

*Antes:* DataFrame com 0 linhas e 1 coluna (`unnamed_0`). Sem dados.

*Ação:* Registrada no relatório de qualidade com `status: discarded`, `reason: vazia`. Nenhum CSV gerado.

---

#### 2.6.2 `dados_sem_utilidade` (4 tabelas)

**Exemplo: `bb_2013_06_junho_pmcmv_18062013_tab_arquivos_dados`**

*Antes (1 coluna, 1 linha):*
```
datacnpj_bbresp_arqtel_resp_arqe_mail_resp_arqdata_pos_arq
2013-06-17|00000000|CARLOS THIAGO DE AGUIAR NOGUEIRA GOMES|6131026187|c.thiago.gomes@bb.com.br|2013-06-17
```

*Ação:* Detectada como dados de log/login (metadados de arquivo, sem valor analítico). Registrada no relatório de qualidade com `status: discarded`, `reason: dados_sem_utilidade`. Nenhum CSV gerado.

---

## 3. Validação Pós-Tratamento

Após o tratamento, cada tabela passa por verificações de qualidade (`validacao.py`):

| Verificação | Descrição |
|---|---|
| Shape | DataFrame deve ter ≥1 linha e ≥1 coluna |
| Chaves não nulas | Colunas com `apf`, `cod_`, `cnpj`, `contrato`, `nr_` não podem ter nulos |
| Missing % | Percentual de células vazias: `(total de células NaN / total de células) × 100` |

As métricas são compiladas no `_quality_report.csv` gerado por `saida_tratamento.py:gerar_relatorio_qualidade()` (445 entradas: 440 tratadas + 5 descartes).

### Colunas do relatório de qualidade (`_quality_report.csv`)

| Coluna | Tipo | Descrição |
|---|---|---|
| `table_name` | str | Nome da tabela original |
| `status` | str | `treated` — tratada com sucesso; `discarded` — descartada (vazia/sem utilidade); `error` — exceção durante o tratamento |
| `n_rows` | int | Número de linhas no DataFrame tratado (0 para descartadas/erro) |
| `n_cols` | int | Número de colunas no DataFrame tratado (0 para descartadas/erro) |
| `profile` | str | Perfil estrutural (`colunar_denso`, `lookup`, `event_level`, `agregado_uf`, `sub_tabelas_1-4`, `separador_pipe`) ou razão do descarte |
| `institution` | str | `bb`, `caixa` ou `unknown` |
| `report_date` | str | Data de referência no formato `YYYY-MM-DD`, extraída do nome do arquivo |
| `missing_pct` | float | Percentual de células com valor ausente (NaN) no DataFrame tratado |
| `encoding_issues` | int | Contagem de problemas de encoding detectados |
| `date_parse_errors` | int | Contagem de colunas com falha no parsing de datas (>10% de valores não convertidos) |
| `type_coercion_warnings` | int | Contagem de warnings de coerção de tipo (colunas que falharam no cast) |
| `error` | str | Mensagem da exceção para tabelas com `status=error` (string vazia caso contrário) |

### Tratamento de erros

Tabelas que lançam exceção durante o pipeline são capturadas e registradas com `status=error`. O DataFrame problemático **não** gera saída, mas a falha é documentada no `_quality_report.csv` com a mensagem da exceção na coluna `error`. O pipeline continua processando as demais tabelas normalmente.

---

## 4. Resumo das Transformações por Categoria

| Categoria de formação | Transformação principal | Saída |
|---|---|---|
| `bem_formada` | Limpeza (G0) + Normalização G1-G8 (nomes, tipos, datas, encoding, metadados) | CSV tratado |
| `cabecalho_na_primeira_linha_1` | Promover linha 0 → header + G0-G8 | CSV tratado |
| `cabecalho_na_primeira_linha_2` | Promover header + remover linha de totalização + G0-G8 | CSV tratado |
| `cabecalho_na_segunda_linha` | Descartar linha 0 (Posicao:), promover linha 1 → header + G0-G8 | CSV tratado |
| `cabecalho_na_terceira_linha_[1/2]` | Descartar linhas 0-1, promover linha 2 → header + G0-G8 | CSV tratado |
| `cabecalho_composto_1` | Forward-fill 3 linhas + concatenar com `_` + remover rodapé + G0-G8 | CSV tratado |
| `cabecalho_composto_2` | Forward-fill 2 linhas + concatenar com `_` + G0-G8 | CSV tratado |
| `sem_cabecalho` | Inferir nomes do inventário (ou `col_0`, `col_1`, ...) + G0-G8 | CSV tratado |
| `separador_\|` | Expandir pipe (`\|`) em múltiplas colunas + G0-G8 | CSV tratado |
| `sub_tabelas_1` | Split por ≥2 linhas vazias + wide-to-long + classificar `recorte_tipo` | CSV tratado |
| `sub_tabelas_2` | Split por keywords + `sub_table_index` + concat | CSV tratado |
| `sub_tabelas_3` | Split por linhas vazias + reconstruir header multi-linha + G0-G8 | CSV tratado |
| `sub_tabelas_4` | Split por blocos text-heavy + reconstruir header multi-linha + G0-G8 | CSV tratado |
| `vazia` | Nenhum — descartada (`status=discarded`) | Sem saída |
| `dados_sem_utilidade` | Nenhum — descartada (`status=discarded`) | Sem saída |
| `indeterminada` / outras | Fallback: tratada como `bem_formada` com warning | CSV tratado |

---

## 5. Estatísticas da Versão 1

- **Total de amostras de entrada:** 753
- **Tabelas após deduplicação:** ~445 (canônicas, as demais são duplicatas MD5 removidas)
- **Tabelas tratadas:** 440
- **Tabelas descartadas:** 5 (`vazia` + `dados_sem_utilidade`)
- **Categorias com mais tabelas:** `bem_formada` (351) e `sub_tabelas_1` (273)
- **Módulos de tratamento:** 4 (`tratamento.py`, `tratamento_cabecalho.py`, `tratamento_especiais.py`, `tratamento_subtabelas.py`)
- **Perfis de saída:** `colunar_denso`, `event_level`, `agregado_uf`, `lookup`, `sub_tabelas_1-4`, `separador_pipe`
- **Colunas de metadados adicionadas:** 5 (`source_table`, `report_date`, `institution`, `profile`, `content_hash`)

### Arquivos relevantes

| Módulo | Função |
|---|---|
| `src/classificacao/tratamento.py` | Pipeline completo G0-G8, roteador `tratar_tabela()` |
| `src/classificacao/tratamento_cabecalho.py` | Cabeçalho deslocado (7 subtipos) e composto (2 subtipos) |
| `src/classificacao/tratamento_especiais.py` | Pipe-separator, sem cabeçalho, vazias, dados sem utilidade |
| `src/classificacao/tratamento_subtabelas.py` | Sub-tabelas (4 subtipos), extração, reconstrução |
| `src/classificacao/inferencia_colunas.py` | Inferência de nomes de coluna para `sem_cabecalho` |
| `src/classificacao/deduplicacao.py` | Hash MD5, agrupamento de duplicatas, eleição de canônicas |
| `src/classificacao/validacao.py` | Validação pós-tratamento (shape, chaves, missing_pct) |
| `src/classificacao/saida_tratamento.py` | Escrita de CSVs tratados e relatório de qualidade |
| `main.py` | Entry point — orquestra classificação → dedup → tratamento → qualidade |

### Guias complementares

| Guia | Conteúdo |
|---|---|
| [`docs/guia-revisao-classificacao.md`](guia-revisao-classificacao.md) | Árvore de decisão R1–R8, 17 categorias, thresholds, heurísticas de profiling |
| [`docs/guia-revisao-tratamento.md`](guia-revisao-tratamento.md) | Pipeline Groups 0–8, deduplicação, validação, relatório de qualidade, DB mode |
