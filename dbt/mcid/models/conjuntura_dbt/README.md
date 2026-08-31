# conjuntura_dbt

Produto dbt do **boletim de conjuntura habitacional**, contínuo e trimestral.
Roda no adapter `dbt-postgres`, mas as fontes vêm do data lake via
**pg_duckdb**.

## Arquitetura (pipeline por dado)

```
Etapa 01 (RAW)     arquivo recebido, imutável                         -> MinIO
Etapa 02 (STAGING) parquet fiel, com valores textuais                 -> MinIO
Etapa 03 (BRONZE)  cópia fiel do parquet, ainda textual                -> Postgres
Etapa 04 (SILVER)  tratamento, tipagem e conformação                   -> Postgres
Etapa 05 (GOLD)    modelos de consumo do boletim e do Superset         -> Postgres
```

- **Etapas 02 e 03** preservam a fidelidade à origem: o parquet de staging e
  a Bronze mantêm os valores como texto. Tratamento, achatamento e tipagem
  acontecem somente na Silver.
- **Etapa 05 (gold)**: 1 modelo por gráfico/card do boletim, no schema
  `conjuntura_continuo_mart`. Cada modelo já sai pronto para virar um dataset
  do Superset (ver seção "Gold → gráfico do Superset" abaixo).

### Dados manuais (`manual_conjuntura`)

Duas origens:
- **`manual_conjuntura.dados_trimestrais` / `dados_mensais`** — tabelas largas
  carregadas direto do `boletim.xlsx` oficial (abas "Dados Trimestrais" /
  "Dados Mensais"). É a fonte mais confiável: os valores já vêm calculados
  pela equipe que monta o boletim (inclui dessazonalização, variações etc).
  A maioria dos golds `[MANUAL]` lê daqui via
  `silver_continuo_manual_trimestrais` / `silver_continuo_manual_mensais`.
- **Tabelas soltas** (`empresas_balanco_lancamentos_vendas`,
  `sbpe_financiamentos_aquisicao_bancos`, `fgts_valor_medio_imoveis`,
  `ibge_pib_construcao_civil`) — inseridas via
  `scripts/database/0001__INSERT_CONJUNTURA.sql`, mantidas porque cobrem
  dados que não estão nas abas do xlsx.

## Mapa por página do boletim (status final)

Boletim de referência: **Boletim de Conjuntura do Setor Habitacional**
(validado contra 2025 3T, 2025 4T e 2026 1T). Legenda:
**[OK-A]** automatizado (API/e-mail) · **[OK-M]** manual (boletim.xlsx ou
tabela solta) · **[SEM FONTE]** não temos acesso / não vamos tentar.

### Página 1 — PIB da Construção Civil · Lançamentos e Vendas
| Gold | Status | Observação |
|---|---|---|
| `gold_continuo_pib_construcao_civil_pct` | **[OK-M]** | % crescimento (trim/trim, acum. ano, acum. 4T) — bate com o boletim nas acumuladas. |
| `gold_continuo_pib_construcao_civil` | **[OK-A]** | % de crescimento direto da API do IBGE (trim/trim, trim vs mesmo trim ano ant., acum. ano, acum. 4T) — apoio/comparação à versão manual (trim/trim pode divergir por dessazonalização). |
| Lançamentos e Vendas (CBIC), por região/MCMV | **[SEM FONTE]** | Sem acesso ao CBIC — proibido implementar. |

### Página 2 — Balanço das Empresas · Financiamentos Imobiliários
| Gold | Status | Observação |
|---|---|---|
| `gold_continuo_balancos_empresas` | **[OK-M]** | Lançamentos/vendas por construtora (MRV, Cury, Tenda, Direcional, Pacaembu, Plano&Plano). |
| `gold_continuo_balancos_empresas_totais` | **[OK-M]** | Totais + variações (trim. anterior, mesmo trim. ano anterior, acumulada). |
| `gold_continuo_financiamentos_imobiliarios_pf_pj` | **[OK-A]** | Concessões, taxa de juros e inadimplência PF/PJ (BACEN SGS). |
| `gold_continuo_financiamentos_habitacionais` | **[OK-M]** | UH — FGTS-PJ e SBPE Construção, trimestral + acum. 12m. |

### Página 3 — Empregos · PNAD · Produção Física e Vendas · Novos Financiamentos
| Gold | Status | Observação |
|---|---|---|
| `gold_continuo_empregos_caged` | **[OK-M]** | Saldo/estoque construção x total (CAGED). `silver_continuo_novo_caged` (API, recorte "edifícios") fica como apoio. |
| `gold_continuo_pnad_ocupados` | **[OK-A]** | Ocupados construção x total (IBGE/SIDRA). Bate exato com o boletim. |
| `gold_continuo_pnad_rendimento` | **[OK-A]** | Rendimento médio real construção x total (IBGE/SIDRA). Série re-basada a cada trimestre pelo IBGE. |
| `gold_continuo_producao_fisica` | **[OK-A]** | PIM-PF (agregado 8886) + PMC volume de vendas (agregado 8757/cat. 56734), dessazonalizados. |
| `gold_continuo_novos_financiamentos_banco` | **[OK-M]** | SBPE por banco (Caixa, Bradesco, Itaú, Santander, BB), acumulado no ano. |

### Página 4 — Crédito/PIB · FGTS-PF por Renda · UH por Condição · Funding
| Gold | Status | Observação |
|---|---|---|
| `gold_continuo_credito_pib` | **[OK-A]** | Crédito Imobiliário / PIB (%). Fonte: BCB Olinda MercadoImobiliario. |
| `gold_continuo_financiamento_pf_faixa` | **[OK-M]** | Financiamento PF por faixa de renda (Faixa 1/2/3, Classe Média, Fora MCMV). Usado também na pág. 5. |
| `gold_continuo_uh_condicao_uso` | **[OK-M]** | UH por condição de uso — só lado SBPE Aquisição (novos x usados); FGTS-PF por condição de uso não está na planilha oficial. |
| `gold_continuo_funding` | **[OK-M]** | Estrutura de Funding — SBPE, FGTS, LCI, LCA, CRI, CRA, LIG. |

### Página 5 — Canal FGTS · Faixas · Poupança
| Gold | Status | Observação |
|---|---|---|
| `gold_continuo_canal_fgts` | **[OK-M]** | Canal FGTS Pró-Cotista, por faixa de renda. |
| `gold_continuo_financiamento_pf_faixa` | **[OK-M]** | (mesma tabela da pág. 4) Financiamento PF Total por faixa. |
| `gold_continuo_saldo_poupanca` | **[OK-A]** | Captação líquida e saldo SBPE (ABECIP). Bate exato com o boletim. |

### Página 6 — OGU · Preços
| Gold | Status | Observação |
|---|---|---|
| `gold_continuo_ogu` | **[OK-A]** | Dotação/Empenho/Pago/RAP do MCID (SIAFI/Tesouro Gerencial), filtrado pelas 7 ações do gráfico (00AF/00CY/00CX/00TI/00CW/0E64/00XF). Empenho/Pago batem ~2% vs boletim 4T25; Dotação fica abaixo — ação 00XF (MCMV/FGTS) não tem dotação orçamentária no SIAFI (crédito reembolsável fora do OGU tradicional). |
| `gold_continuo_sinapi` | **[OK-A]** | Custo médio m² (IBGE). Bate exato com o boletim. |
| `gold_continuo_incc_m` | **[OK-A]** | INCC-M mensal (FGV). |
| `gold_continuo_ticket_medio` | **[OK-M]** | Ticket médio (MRV/Direcional/Tenda/Cury) vs INCC trimestral, base 4T2020. |
| `gold_continuo_fgts_valor_medio` | **[OK-M]** | Valor médio dos imóveis financiados via FGTS. Bate exato com o boletim. |
| Desembolsos de Obras CEF (FAR/Rural/FDS) | **[SEM FONTE]** | Sem fonte direta; abstrair do OGU seria arriscado. |

### Página 7 — Índices da Construção
| Gold | Status | Observação |
|---|---|---|
| `gold_continuo_indice_imob` | **[OK-M]** | Índice IMOB, variações mensais. `silver_continuo_infomoney_imob` (Alpha Vantage) fica como apoio/nível bruto. |
| `gold_continuo_fipezap` | **[OK-M]** | Índice FipeZap de locação. `silver_continuo_fipezap_locacao` (FIPE) fica como apoio. |
| `gold_continuo_icst` | **[OK-M]** | Índice ICST (série original, sem ajuste sazonal). `silver_continuo_fgv_icst` fica como apoio. |
| Índice ABRAMAT | **[SEM FONTE]** | Dados inconsistentes — não implementado; possível inserir manualmente depois. |

## Guia: cadastrar os datasets no Superset

Cada gold é uma tabela física em `conjuntura_continuo_mart` — precisa virar
**um Dataset no Superset** antes de qualquer chart poder ser criado em cima
dele. Passo a passo (repetir para os 25 golds):

1. **Data → Datasets → + Dataset**.
2. **Database**: `Cidades` (conexão que já existe, apontando para o mesmo
   Postgres do data warehouse — não usar "Analytics", "cidades" minúsculo
   nem "Cidades-Conjuntura", que são outras conexões).
3. **Schema**: `conjuntura_continuo_mart`.
4. **Table**: escolher o `gold_continuo_*` (a lista completa está nas
   tabelas por página acima).
5. Salvar. Repetir para os 25.

Depois de cadastrado, entrar em **Edit** (lápis) → aba **Columns** e marcar a
coluna de tempo como **"Is temporal"** — isso é o que faz o Superset oferecer
filtro de intervalo de tempo e granularidade nos charts. Qual coluna marcar
varia por gold:

| Coluna a marcar "Is temporal" | Golds |
|---|---|
| `data_referencia` | Todos os golds **exceto** os três abaixo (é a coluna padrão, calculada a partir de `ano`+`mes`/`trimestre` — ver seção anterior sobre por que não usar `periodo`) |
| `data` | `gold_continuo_credito_pib`, `gold_continuo_financiamentos_imobiliarios_pf_pj` |
| `mes` | `gold_continuo_incc_m` (aqui `mes` já é do tipo `date`, não precisa de `data_referencia`) |
| — (nenhuma) | `gold_continuo_ogu` (tabela de 1 linha só, sem série temporal) |

Se um gold já foi cadastrado **antes** de eu ter adicionado a coluna
`data_referencia` (ou antes de qualquer mudança de schema nos modelos), o
Superset guarda em cache a lista de colunas antiga — nesse caso, em vez de
recriar o dataset, usar **Edit → Columns → "Sync columns from source"** pra
puxar as colunas atuais.

> Status atual (13/08/2026): os 25 datasets já foram cadastrados e os 34
> charts da lista abaixo já foram criados no Superset de deploy
> (`govhub.mcid.lablivre.rocks`), usando a conexão `Cidades`. Falta apenas
> organizá-los dentro do dashboard "Conjuntura Contínuo" (arrastar cada
> chart pra lá em Edit Dashboard, agrupados por página do boletim).

## Gold → gráfico do Superset

Cada gold já está pronto para virar **um dataset físico no Superset**
(`conjuntura_continuo_mart.gold_continuo_*`). Sugestão de chart type, eixo X
e métricas/séries para cada um:

| Gold | Chart type | Eixo X | Métricas / séries |
|---|---|---|---|
| `gold_continuo_pib_construcao_civil_pct` | Line (multi-série) | `data_referencia` | `var_trim_trim_anterior`, `var_acumulada_ano`, `var_acumulada_4_trimestres` |
| `gold_continuo_pib_construcao_civil` | Line (apoio/comparação) | `data_referencia` | `var_trim_trim_anterior`, `var_trim_mesmo_trim_ano_anterior`, `var_acumulada_ano`, `var_acumulada_4_trimestres` |
| `gold_continuo_balancos_empresas` | Bar agrupado (2 charts: lançamentos e vendas) | `data_referencia` | Group by `empresa`; métrica `lancamentos` (chart 1) / `vendas` (chart 2) |
| `gold_continuo_balancos_empresas_totais` | Big Number w/ trendline (2) ou Bar | `data_referencia` | `lancamentos_totais`, `vendas_totais` — variações como subtítulo/comparação |
| `gold_continuo_financiamentos_imobiliarios_pf_pj` | Line (3 charts) | `data` | (1) `concessoes_pf_rs_mi`/`concessoes_pj_rs_mi`; (2) `taxa_juros_pf_aa`/`taxa_juros_pj_aa`; (3) `inadimplencia_pf_pct`/`inadimplencia_pj_pct` |
| `gold_continuo_financiamentos_habitacionais` | Bar | `data_referencia` | `financ_hab_fgts_pj`, `financ_hab_sbpe_constr` (+ `_acumulado_12_meses` em eixo secundário) |
| `gold_continuo_empregos_caged` | Line/Bar (2 charts: saldo e estoque) | `data_referencia` | (1) `emprego_const_saldo` vs `caged_total_saldo`; (2) `emprego_const_estoque` vs `caged_total_estoque` — eixo duplo (escalas diferentes) |
| `gold_continuo_pnad_ocupados` | Line | `data_referencia` | `ocupados_construcao_mil`, `ocupados_total_mil` (eixo duplo) |
| `gold_continuo_pnad_rendimento` | Line | `data_referencia` | `rendimento_construcao_rs`, `rendimento_total_rs` |
| `gold_continuo_producao_fisica` | Line (2 charts: PIM-PF e PMC) | `data_referencia` | (1) `pim_pf_var_mes`/`pim_pf_var_acum_ano`/`pim_pf_var_12_meses`; (2) `pmc_var_mes`/`pmc_var_acum_ano`/`pmc_var_12_meses` |
| `gold_continuo_novos_financiamentos_banco` | Bar empilhado ou Pizza | `data_referencia` | `abecip_sbpe_fin_uh_acum_caixa/bradesco/itau/santander/bb` |
| `gold_continuo_credito_pib` | Line | `data` | `credito_imobiliario_pib_pct` |
| `gold_continuo_financiamento_pf_faixa` | Bar empilhado | `data_referencia` | `financiamento_pf_uh_total_faixa_1/2/3`, `financiamento_pf_uh_total_classe_media` |
| `gold_continuo_uh_condicao_uso` | Bar | `data_referencia` | `abecip_sbpe_fin_uh_aq_novos`, `abecip_sbpe_fin_uh_aq_usados` |
| `gold_continuo_funding` | Bar empilhado / Area | `data_referencia` | `funding_sbpe`, `funding_fgts`, `anbima_estoque_lci/lca/cri/cra/lig` |
| `gold_continuo_canal_fgts` | Bar empilhado | `data_referencia` | `financiamento_pf_uh_pro_cotista_faixa_1/2/3`, `financiamento_pf_uh_pro_cotista_classe_media` |
| `gold_continuo_saldo_poupanca` | Combo (bar + line, eixo duplo) | `data_referencia` | `captacao_liquida_valor` (bar), `saldo` (line) |
| `gold_continuo_ogu` | Big Number (4 cards) | — (1 linha só, sem série temporal) | `dotacao_atualizada`, `despesas_empenhadas`, `despesas_pagas`, `restos_a_pagar_inscritos` |
| `gold_continuo_sinapi` | Line | `data_referencia` | `custo_medio_m2` (eixo 1); `var_mes`/`var_acum_ano`/`var_12_meses` como cards |
| `gold_continuo_incc_m` | Line | `mes` (já é `date`) | `indice`; `var_mes`/`var_ano`/`var_12_meses` como cards |
| `gold_continuo_ticket_medio` | Line (multi-série, base 4T2020=0) | `data_referencia` | `ticket_medio_lancamentos_mrv/direcional/tenda/cury_var_acum_4t2020` vs `incc_var_acum_4t2020` |
| `gold_continuo_fgts_valor_medio` | Line | `data_referencia` | `valor_medio_fgts`, `valor_medio_fgts_f1` |
| `gold_continuo_indice_imob` | Line/Bar | `data_referencia` | `indice_imob_var_mes` (+ `indice_imob_var_acum_ano` como card) |
| `gold_continuo_fipezap` | Line | `data_referencia` | `indice_fipezap_numero_indice_locacao` (nível); `indice_fipezap_locacao_var_mes` (variação) |
| `gold_continuo_icst` | Line | `data_referencia` | `icst_serie_original_sem_ajuste_sazonal` (nível); `indice_icst_var_mes_serie_original` (variação) |

Observações gerais:
- Todo gold com série temporal tem uma coluna `data_referencia` (tipo `date`)
  calculada a partir de `ano`+`mes`/`trimestre` — use ela como eixo X/Time
  column no Superset e marque como **"Is temporal"**. Não use `periodo`
  como eixo: é texto com formatos herdados do xlsx (ex.: `"1T2022"`,
  `"11/2025"`, ou até timestamp em algumas linhas) que ordena errado
  (alfabético, não cronológico) se usado direto como dimensão.
- Séries com escalas muito diferentes (ex.: ocupados construção ~7 mil x
  total ~103 mil; saldo caderneta em R$ mi x captação líquida em R$ mi mas
  sinal oposto) — usar eixo Y secundário no Superset.

## Apoio / não plotado direto no boletim

- `silver_continuo_ibge_paic_resultados` / `_pessoal_salarial` / `_obras` —
  PAIC (IBGE, estrutural anual).
- `silver_continuo_ibge_pib_consolidado_trimestral` / `_ibge_pib_corrente` —
  apoio à seção 1.
- `silver_continuo_ibge_pnadc_rendimento_domiciliar` /
  `_pnadc_populacao_decis_renda` — outros recortes PNAD-C, não usados
  diretamente no boletim.
