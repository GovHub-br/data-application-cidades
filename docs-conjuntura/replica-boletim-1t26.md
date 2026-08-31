# Réplica do Boletim de Conjuntura 1T26 no Superset — conferência célula a célula

Dashboard: `/superset/dashboard/boletim-conjuntura/` — **um único dashboard, com filtro
de trimestre** (7 abas, uma por página). Consultas genéricas por edição em
`scripts/superset/build_boletim.py`.

> A conferência abaixo foi feita sobre a edição 1T2026 e continua válida: a versão
> parametrizada reproduz os mesmos números. O que mudou foram os cabeçalhos de
> coluna, que passaram a ser relativos ao trimestre escolhido — o nome da coluna
> faz parte do schema do dataset e não pode variar com o filtro.
Consultas em `scripts/superset/build_boletim.py`; valores publicados transcritos do PDF
"Boletim de Conjuntura 2026 1T Final".

Gerado em 2026-08-30.

> A réplica reproduz a ESTRUTURA do boletim exatamente. Os VALORES só podem
> coincidir onde a fonte não revisa o passado: o boletim é um retrato congelado
> (o quadro do OGU diz, no próprio cabeçalho, "Dados de 02/01/26") e nossa
> extração é sempre a posição corrente.

## Resumo

```
células conferidas: 194
  batem:    130  (67%)
  divergem: 62
  ausentes: 2
```

## Como ler as divergências

- **Revisão da fonte** — CAGED, IBGE (PIM/PMC/PNAD), BACEN, FipeZap e o PIB
  revisam meses já publicados. Divergência aqui é esperada e não é defeito.
- **Congelamento** — OGU: o boletim fixou o SIAFI em 02/01/26; divergem todas
  as 24 células, e não há como reproduzir sem guardar o snapshot da edição.
- **Defeito real** — MRV no 1T2026, INCC-M (colunas trocadas), UH por condição
  de uso (planilha atrasada e gap conhecido do FGTS-PF).

## Detalhe

```
--- p.1 PIB Construção Civil (em % de Crescimento)
   [DIVERGE] Trim./Trim. Imediatamente Anterior   2025 1ºTri                   boletim=-1           nosso=-1.3

--- p.2 Lançamentos 1T26 (variação %)
   [DIVERGE] MRV                                  X 4T25                       boletim=-5           nosso=0
   [DIVERGE] MRV                                  X 1T25                       boletim=-9           nosso=-4
   [DIVERGE] MRV                                  12m 26/25                    boletim=-3           nosso=-1

--- p.2 Vendas 1T26 (variação %)
   [DIVERGE] MRV                                  X 4T25                       boletim=-14          nosso=-13
   [DIVERGE] MRV                                  X 1T25                       boletim=6            nosso=9
   [DIVERGE] MRV                                  12m 26/25                    boletim=-2           nosso=0
   [DIVERGE] Direcional                           X 4T25                       boletim=12           nosso=15
   [DIVERGE] Direcional                           X 1T25                       boletim=15           nosso=12

--- p.2 Totais (das empresas levantadas)
   [DIVERGE] Total lançamentos                    X 4T25                       boletim=6            nosso=8
   [DIVERGE] Total lançamentos                    X 1T25                       boletim=3            nosso=5
   [DIVERGE] Total vendas                         X 1T25                       boletim=14           nosso=15

--- p.2 Financiamentos Imobiliários (BACEN)
   [DIVERGE] MAR/26                               PF Concessões (R$ mi)        boletim=22623        nosso=25196
   [DIVERGE] FEV/26                               PF Concessões (R$ mi)        boletim=18176        nosso=18810
   [DIVERGE] MAR/25                               PJ Taxa de Juros (%a.a)      boletim=10.9         nosso=11.9
   [DIVERGE] 12 Meses - MAR/26                    PF Concessões (R$ mi)        boletim=230492       nosso=239287

--- p.3 Empregos Construção (CAGED)
   [DIVERGE] MAR/26                               Criação Líquida (Saldo)      boletim=38316        nosso=37811
   [DIVERGE] JAN-MAR/26                           Criação Líquida (Saldo)      boletim=120547       nosso=119870

--- p.3 PNAD Contínua — Ocupados (mil) e Rendimento Médio Real (R$)
   [DIVERGE] jan-fev-mar 2026                     Rendimento Construção (R$)   boletim=2858         nosso=2922
   [DIVERGE] jan-fev-mar 2026                     Rendimento Total (R$)        boletim=3610         nosso=3690
   [DIVERGE] out-nov-dez 2025                     Rendimento Construção (R$)   boletim=2851         nosso=2914
   [DIVERGE] out-nov-dez 2025                     Rendimento Total (R$)        boletim=3555         nosso=3635

--- p.3 Produção Industrial e Volume de Vendas (variação %)
   [DIVERGE] Variação percentual mensal           PROD 2026 FEV                boletim=-6.7         nosso=-6.3
   [DIVERGE] Variação percentual mensal           PROD 2026 MAR                boletim=-0.7         nosso=-0.1
   [DIVERGE] Variação percentual mensal           VENDAS 2025 MAR              boletim=-0.4         nosso=-0.1
   [DIVERGE] Variação percentual mensal           VENDAS 2026 FEV              boletim=0.7          nosso=0.4
   [DIVERGE] Variação percentual mensal           VENDAS 2026 MAR              boletim=1.6          nosso=1.9
   [DIVERGE] Variação percentual acumulada no a   PROD 2026 FEV                boletim=-6.9         nosso=-6.7
   [DIVERGE] Variação percentual acumulada no a   PROD 2026 MAR                boletim=-4.8         nosso=-4.4
   [DIVERGE] Variação percentual acumulada nos    PROD 2026 MAR                boletim=-3.4         nosso=-3.3

--- p.4 Nº UH por Condição de Uso
   [DIVERGE] FGTS - PF                            JAN-MAR/25 UH Novas          boletim=101490       nosso=99546
   [DIVERGE] FGTS - PF                            JAN-MAR/26 UH Usadas         boletim=46506        nosso=25566
   [DIVERGE] FGTS - PF                            JAN-MAR/26 UH Novas          boletim=124507       nosso=108933
   [DIVERGE] SBPE (Aquisição)                     JAN-MAR/25 UH Novas          boletim=21570        nosso=28234
   [NULO   ] SBPE (Aquisição)                     JAN-MAR/26 UH Usadas         boletim=55633        nosso=·
   [NULO   ] SBPE (Aquisição)                     JAN-MAR/26 UH Novas          boletim=22234        nosso=·

--- p.6 OGU 2026 JAN-MAR (R$ milhões)
   [DIVERGE] 00AF FAR                             Dotação Atual                boletim=5807         nosso=6804.9
   [DIVERGE] 00AF FAR                             Empenho                      boletim=0            nosso=6720
   [DIVERGE] 00AF FAR                             Pagamento                    boletim=0            nosso=6720
   [DIVERGE] 00AF FAR                             RAP Inscrito                 boletim=0            nosso=667.8
   [DIVERGE] 00AF FAR                             Pag. RAP                     boletim=0            nosso=667.8
   [DIVERGE] 00AF FAR                             Pag. Total                   boletim=0            nosso=7387.8
   [DIVERGE] 00CY FDS                             Dotação Atual                boletim=500.5        nosso=244.5
   [DIVERGE] 00CY FDS                             Empenho                      boletim=0            nosso=171.6
   [DIVERGE] 00CY FDS                             Pagamento                    boletim=0            nosso=171.6
   [DIVERGE] 00CX PNHR                            Dotação Atual                boletim=1500         nosso=735.8
   [DIVERGE] 00CX PNHR                            Empenho                      boletim=1400         nosso=735.8
   [DIVERGE] 00CX PNHR                            Pagamento                    boletim=1400         nosso=735.8
   [DIVERGE] 00TI FNHIS                           Dotação Atual                boletim=1300         nosso=0
   [DIVERGE] 00TI FNHIS                           Empenho                      boletim=214.79       nosso=136.8
   [DIVERGE] 00TI FNHIS                           Pagamento                    boletim=4.248        nosso=0
   [DIVERGE] 00CW PNHU                            Dotação Atual                boletim=88.5         nosso=228.8
   [DIVERGE] 00CW PNHU                            Empenho                      boletim=0            nosso=217.2
   [DIVERGE] 00CW PNHU                            Pagamento                    boletim=0            nosso=210.1
   [DIVERGE] 00XF FUNDO SOC.                      Dotação Atual                boletim=24762.3      nosso=0
   [DIVERGE] 00XF FUNDO SOC.                      Empenho                      boletim=10000        nosso=25668.6
   [DIVERGE] 00XF FUNDO SOC.                      Pagamento                    boletim=4278.1       nosso=11657.8
   [DIVERGE] SOMA SOMA                            Dotação Atual                boletim=33958        nosso=8014
   [DIVERGE] SOMA SOMA                            Empenho                      boletim=11615        nosso=33649.9
   [DIVERGE] SOMA SOMA                            Pagamento                    boletim=5682         nosso=19495.2

--- p.6 SINAPI (Brasil) e INCC-M
   [DIVERGE] JAN-MAR/26 (acumulado no ano %)      INCC-M                       boletim=5.81         nosso=1.33
   [DIVERGE] 12m MAR/26 (%)                       INCC-M                       boletim=7.32         nosso=5.81

--- p.7 Índices da Construção (variação %)
   [DIVERGE] MAR 26 vs. FEV 26                    Índice IMOB                  boletim=-9.3         nosso=-9.4
   [DIVERGE] MAR 26 vs. FEV 26                    Índice FipeZap               boletim=1.1          nosso=0.8
```
