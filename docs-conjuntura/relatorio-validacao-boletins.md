# Validação Gold × gabarito dos boletins

Relatório gerado automaticamente. Valores esperados e páginas estão em `gabarito-boletins.yml`.

- OK: 28
- Divergências abertas: 0
- Divergências esperadas: 2
- Gaps conhecidos: 2

| Status | Edição | Indicador | Gold | PDF | Publicado | Gold atual | Diferença |
|---|---|---|---|---:|---:|---:|---:|
| OK | 2025_3t | MRV — lançamentos, variação contra trimestre anterior | `gold_continuo_balancos_empresas` | p. 1 | -32 | -31.8152 | 0.1848 |
| OK | 2025_3t | MRV — lançamentos, variação contra mesmo trimestre do ano anterior | `gold_continuo_balancos_empresas` | p. 1 | -19 | -19.324 | -0.324 |
| OK | 2025_3t | MRV — lançamentos, acumulado de nove meses | `gold_continuo_balancos_empresas` | p. 1 | 20 | 20.3744 | 0.3744 |
| OK | 2025_3t | MRV — vendas, variação contra trimestre anterior | `gold_continuo_balancos_empresas` | p. 1 | -12 | -11.7054 | 0.2946 |
| OK | 2025_3t | MRV — vendas, variação contra mesmo trimestre do ano anterior | `gold_continuo_balancos_empresas` | p. 1 | -10 | -9.7787 | 0.2213 |
| OK | 2025_3t | MRV — vendas, acumulado de nove meses | `gold_continuo_balancos_empresas` | p. 1 | -5 | -4.7135 | 0.2865 |
| OK | 2026_1t | PIB da construção — variação trimestre contra trimestre | `gold_continuo_pib_construcao_civil_pct` | p. 1 | 2.9 | 2.9 | 0 |
| OK | 2026_1t | PIB da construção — acumulado no ano | `gold_continuo_pib_construcao_civil_pct` | p. 1 | 1.3 | 1.3 | 0 |
| OK | 2026_1t | PIB da construção — acumulado em quatro trimestres | `gold_continuo_pib_construcao_civil_pct` | p. 1 | 0.1 | 0.1 | 0 |
| OK | 2025_4t | SINAPI Brasil — custo médio por m² | `gold_continuo_sinapi` | p. 6 | 1891.63 | 1891.63 | 0 |
| OK | 2025_4t | SINAPI Brasil — variação mensal | `gold_continuo_sinapi` | p. 6 | 0.51 | 0.51 | 0 |
| OK | 2025_4t | SINAPI Brasil — variação em 12 meses | `gold_continuo_sinapi` | p. 6 | 5.64 | 5.63 | -0.01 |
| OK | 2025_4t | Financiamentos habitacionais — FGTS PJ trimestral (UH) | `gold_continuo_financiamentos_habitacionais` | p. 2 | 61212 | 61212 | 0 |
| OK | 2025_4t | Financiamentos habitacionais — SBPE construção trimestral (UH) | `gold_continuo_financiamentos_habitacionais` | p. 2 | 47766 | 47766 | 0 |
| OK | 2026_1t | Financiamentos habitacionais — SBPE construção trimestral (UH) | `gold_continuo_financiamentos_habitacionais` | p. 2 | 47609 | 47609 | 0 |
| OK | 2026_1t | Financiamentos habitacionais — FGTS PJ trimestral (UH) | `gold_continuo_financiamentos_habitacionais` | p. 2 | 59836 | 59862 | 26 |
| OK | 2026_1t | Financiamentos habitacionais — FGTS PJ acumulado em 12 meses (UH) | `gold_continuo_financiamentos_habitacionais` | p. 2 | 270364 | 270390 | 26 |
| OK | 2026_1t | INCC-M — número índice | `gold_continuo_incc_m` | p. 6 | 1241.72 | 1241.721 | 0.001 |
| OK | 2026_1t | INCC-M — variação mensal | `gold_continuo_incc_m` | p. 6 | 0.35 | 0.36 | 0.01 |
| DIVERGENCIA_ESPERADA | 2026_1t | INCC-M — variação em 12 meses | `gold_continuo_incc_m` | p. 6 | 7.32 | 5.81 | -1.51 |
| ↳ |  | A FGV revisou retroativamente a série corrente: o Gold calcula 5,81% a partir do XLSX atual, enquanto o boletim preserva a safra publicada de 7,32%. |  |  |  |  |  |
| OK | 2026_1t | Índice IMOB — variação mensal | `gold_continuo_indice_imob` | p. 7 | -9.3 | -9.3581 | -0.0581 |
| OK | 2026_1t | Índice IMOB — variação em 12 meses | `gold_continuo_indice_imob` | p. 7 | 62.2 | 62.2471 | 0.0471 |
| DIVERGENCIA_ESPERADA | 2026_1t | FipeZap locação — variação mensal | `gold_continuo_fipezap` | p. 7 | 1.1 | 0.8385 | -0.2615 |
| ↳ |  | A série FipeZap é revisada retroativamente; comparar safra publicada, não série corrente. |  |  |  |  |  |
| OK | 2026_1t | FipeZap locação — variação em 12 meses | `gold_continuo_fipezap` | p. 7 | 8.6 | 8.6261 | 0.0261 |
| OK | 2026_1t | Tenda — lançamentos, variação contra trimestre anterior | `gold_continuo_balancos_empresas` | p. 2 | -15 | -15.479 | -0.479 |
| OK | 2026_1t | Tenda — lançamentos, variação contra mesmo trimestre do ano anterior | `gold_continuo_balancos_empresas` | p. 2 | 67 | 67 | 0 |
| OK | 2026_1t | Tenda — lançamentos, variação acumulada em 12 meses | `gold_continuo_balancos_empresas` | p. 2 | 0 | 0 | 0 |
| OK | 2026_1t | Tenda — vendas, variação contra trimestre anterior | `gold_continuo_balancos_empresas` | p. 2 | 14 | 13.9002 | -0.0998 |
| OK | 2026_1t | Tenda — vendas, variação contra mesmo trimestre do ano anterior | `gold_continuo_balancos_empresas` | p. 2 | 30 | 29.9678 | -0.0322 |
| OK | 2026_1t | Tenda — vendas, variação acumulada em 12 meses | `gold_continuo_balancos_empresas` | p. 2 | 9 | 9 | 0 |
| GAP_CONHECIDO | 2026_1t | FGTS-PF — unidades usadas, condição de uso | `gold_continuo_uh_condicao_uso` | p. 5 | 46506 | 25566 | -20940 |
| ↳ |  | A Base_PF_FGTS do GEAVO é contínua até jun/2026, mas o recorte MCMV/faixas não cobre o total Canal FGTS publicado. Integrar fonte complementar ou regra de cobertura antes de uso editorial. |  |  |  |  |  |
| GAP_CONHECIDO | 2026_1t | FGTS-PF — unidades novas, condição de uso | `gold_continuo_uh_condicao_uso` | p. 5 | 124507 | 108933 | -15574 |
| ↳ |  | A Base_PF_FGTS do GEAVO é contínua até jun/2026, mas o recorte MCMV/faixas não cobre o total Canal FGTS publicado. Integrar fonte complementar ou regra de cobertura antes de uso editorial. |  |  |  |  |  |
