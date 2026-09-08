# Evidências — change `catalogo-dicionario-dados-historicos`

Fechamento da gap-list do `design.md`, verificado no `manifest.json` /
`catalog.json` / `run_results.json` regerados no modo A em 2026-09-08 20:55Z
(`dbt docs generate` + `dbt test --select mcmv_historico_dbt indicadores_mcmv_dbt
--exclude mcmv_historico_dbt.piloto`).

| # | achado | estado |
|---|---|---|
| G1 | `gold_indicadores_reloginho` citava `silver_reloginho_snh_apf_mes` | corrigido → `silver_historico_snh_apf_mes` |
| G2 | `issue-130-dicionario-indicadores.md` com nomes/schemas obsoletos | reconciliado (schema `reloginho`, `gold_indicadores_*`, bronzes por agente); estado de materialização por grupo/indicador anotado |
| G3 | `gold_indicadores_gargalo_desempenho.frente` = {FAR, FDS} | description declara `FDS` ≡ `Entidades` e por que a coluna se chama `frente` |
| G4 | reloginho `uh_contratadas`/`uh_entregues`/`uh_vigentes` | nota D3 (conceito canônico `quantidade_uh*`) em silver + 4 golds |
| G5 | `gold_indicadores_gargalo_desempenho.dt_calculo` | description declara equivalência a `dt_gold` |
| G6 | `status_prazo` / `status_ritmo` vs `situacao_canonica` | domínio e eixo de cada um documentados |
| G7 | `gold_resumo_gargalo_desempenho_dashboard` sem unidade/escala | unidade (UH / R$ / 0-100 / dias) e grão por `nivel_agregacao` em cada coluna; +15 colunas físicas antes não documentadas |
| G8 | "UH entregues" em `gold_serie_mensal` vs `gold_indicadores_reloginho` | nota de `natureza_serie` / não-soma alinhada nas duas |
| G9 | 10 modelos de `indicadores_mcmv_dbt` sem `meta` de governança | `+meta` (governance + openmetadata) no `dbt_project.yml`; `dbt ls` confirma os 10 nós e nada mais |
| G10 | bronzes `snh_{bb,caixa}` / `obra_mensal_{far,fds,rural}` | description por coluna conferida (0 lacunas); "sem regra de negócio" + divergência de schema entre agentes explícitas |
| G11 | `silver_historico_snh_apf_mes` `ref()` dinâmico | `depends_on` lista as 2 bronzes SNH + 2 seeds — OK |
| G12 | golds de gargalo → fichas FAR/FDS | `depends_on` cross-pasta presente; os 6 modelos-pai têm `description` (0 pendências) |
| G13 | schema sprawl | resolvido pela `consolidar-schemas`; eixo em 5 schemas (`dados_historicos`, `reloginho`, `empreendimento_far`, `empreendimentos_fds`, `empreendimento_rural`) — ver `procedimento-artefatos-openmetadata.md` §3 |
| G14 | 66 colunas sem `description` em 8 modelos | **0** colunas sem `description` nos 33 nós |
| G15 | endereço/CNPJ em `gold_snapshot_empreendimento_atual` | descritas com nota explícita "empreendimento/entidade, não pessoa física"; decisão de publicar registrada no `design.md` (OQ5) |
| G16 | `run_results.json` 1 fail | `PASS=519 WARN=46 ERROR=1`; o fail é o known-OK `assert_reloginho_frente_cobertura_mensal` (lacuna de fonte) |
| G17 | cauda `fonte_serie = 'obra_mensal'` com estoque NULL | nota propagada para `quantidade_uh` / `valor_contratado` / `valor_desembolsado` e para a description das 3 silvers de frente |
| G18 | `ref()` cross-schema `silver_mcmv_historico_entrega_apf` → `reloginho` | verificado no `depends_on` — OK |

## Inventário dos 33 nós

`catalogo-dicionario-33-nos.csv` (camada, schema, modelo, alias, nº colunas,
product, layer, tier).

## Metadados de governança (`dbt ls --output json`, seletor `indicadores_mcmv_dbt`)

Os 10 nós de `indicadores_mcmv_dbt` recebem `meta.governance.product = reloginho`,
`meta.openmetadata.domain = MCid.Habitacao`,
`owner = mcid-data-engineering`, `tier` = Tier3 (bronze) / Tier2 (silver) /
Tier1 (gold — inclusive os 2 golds de gargalo). `meta.tags` de cada modelo
preservado. Nenhum outro nó do projeto muda (o `+meta` fica sob a chave
`indicadores_mcmv_dbt` no `dbt_project.yml`).
