# Issue #163 — Indicadores e curva S do FAR

## Resumo

Série mensal por APF do Novo MCMV FAR, com execução física (prevista e realizada),
execução financeira (liberado no mês e acumulado), situação da obra e colunas da CAIXA
para conciliação. A partir dela sai a curva S física × financeira, nacional e por UF.

| Model | Grão | Para quê |
|---|---|---|
| `prata_far_obra_serie` | APF × competência | MONIT de obra empilhado |
| `prata_far_caixa_serie` | APF × competência | Dados prioritários CAIXA (FAR) empilhados |
| `prata_far_serie_mensal` | APF × mês | Série contínua com todas as regras abaixo |
| `ouro_far_serie_mensal_apf` | APF × mês | Dashboard e modelo preditivo |
| `ouro_far_curva_s` | mês × nível (`nacional`/`uf`) | Curva S agregada |
| seed `far_situacao_obra` | código | Rótulo de `co_situacao_obra` (dicionário do LAYOUT) |

## Fontes

| Fonte | Tipo de arquivo | Como é lida | Cobertura |
|---|---|---|---|
| `MONIT_MOV_FINANC_FAR_MENSAL` | livro-razão cumulativo | só o arquivo mais recente | liberações de 2024-06 a hoje |
| `MONIT_MOV_OBRA_FAR_MENSAL` | retrato do mês | todas as competências empilhadas | 2025-12 a hoje; **2026-06 não existe na origem** |
| `<aaaamm>_SNH_PMCMV_DADOS_PRIORITARIOS_AF_CAIXA` | retrato do mês | todas as competências empilhadas | 2024-06 a hoje, contínuo |
| `MONIT_CAD_PJ_FAR_MENSAL` | retrato do mês | só o mais recente | universo de APFs e valor FAR |

O financeiro dispensa empilhamento: cada arquivo repete todo o histórico, e os meses
em comum batem ao centavo entre arquivos. Obra e CAIXA não repetem o passado, então a
série só existe empilhando. Todas as tabelas são full refresh; o histórico está
preservado nos arquivos do lake.

A série começa em 2024: a primeira contratação FAR do Novo MCMV é de 02/02/2024 e a
primeira liberação, de 13/06/2024. Entre contratação e primeira liberação passam, na
mediana, 270 dias.

## Regras

**Universo.** APFs do cadastro MONIT, mais os que têm liberação no livro-razão mas não
estão no cadastro (7 APFs, R$ 133,8 mi em 2026-07, todos EM ANDAMENTO na CAIXA). Esses
entram com `ic_cadastro_monit = false`, identificação da CAIXA e sem % financeiro, porque
não há valor FAR para eles. Os contratos FAR do legado (anteriores a 2024) ficam fora;
estão no domínio `mcmv_historico_dbt`.

**Calendário.** Um mês por linha, do primeiro sinal do APF (contratação, liberação ou
medição) até a última competência de qualquer fonte, inclusive meses sem movimento.

**Competência.** Vem do nome do arquivo (`_MENSAL_<aaaamm>_`, `<aaaamm>_SNH_`), não de
`dt_movimento`, que cai no mês seguinte. Reenvio da mesma competência: vale o arquivo
mais recente.

**Financeiro.** `vr_liberado_mes` é a soma das liberações do mês; `vr_liberado_acum`, o
acumulado. `pct_financeiro = vr_liberado_acum / vr_emprestimo_far × 100`, sem teto: o
INCC é pago acima do valor contratado e leva alguns APFs acima de 100% (20 em 2026-08);
`vr_pago_incc_acum` permite descontá-lo. O denominador é o valor FAR, não o investimento
total, porque o livro-razão só registra recurso FAR.

**Físico.** Prioridade por mês: MONIT → CAIXA → último valor observado. `fonte_fisica`
diz de onde veio cada ponto (`monit`, `caixa`, `carregado`). A CAIXA só entra a partir
da primeira competência em que mede o Novo MCMV (2025-04): antes disso manda `exec` = 0
para todos os APFs, o que é "não informado", não 0% de obra. O previsto
(`pct_obra_prevista`) só existe no MONIT. Mês sem arquivo entre dois observados é
interpolado em linha reta; depois do último observado, o último valor é arrastado.
`fonte_prevista` diz qual (`monit`, `interpolado`, `carregado`).

**Situação.** `co_situacao_obra` (MONIT) segue o mesmo arraste do previsto. O rótulo vem
do seed `far_situacao_obra` — ver abaixo. `situacao_caixa` é a situação por extenso
informada pela CAIXA no mês.

**Curva S.** Percentuais ponderados pelo valor FAR de cada APF. Ficam fora os APFs cuja
situação mais recente na CAIXA é DISTRATADO/CANCELADO (78 APFs, R$ 0,9 mi liberados),
que ficariam em 0% para sempre. A carteira cresce com as contratações, então cada mês
mede os APFs já existentes naquele mês.

## `co_situacao_obra`

O domínio vem do arquivo `MONIT_MOV_OBRA_FAR_LAYOUT_*`, que acompanha as remessas de
obra ("tabela Posição da obra/Empreendimento", 16 códigos), transcrito no seed. Em
2026-07 aparecem 7 deles:

| Código | Situação | APFs | Previsto | Realizado |
|---|---|---|---|---|
| 1 | Não iniciada | 105 | 0% | 0% |
| 2 | Em andamento | 168 | 47% | 56% |
| 3 | Atrasada | 124 | 59% | 45% |
| 4 | Paralisada | 3 | 55% | 39% |
| 5 | Concluída | 29 | 88% | 100% |
| 11 | Normal | 438 | 52% | 52% |
| 16 | Outras | 9 | 28% | 25% |

O MONIT não usa o código 8 (cancelada ou distratada): 77 dos 105 APFs em "não iniciada"
estão DISTRATADO/CANCELADO na CAIXA. Por isso a curva S usa a situação da CAIXA para
excluir distratos.

## Validação

- **Financeiro × fonte:** total da série = total do livro-razão (R$ 9.927.586.515,29 em
  2026-08), diferença zero em todos os 886 APFs.
- **Físico × fonte:** as 6.726 linhas do MONIT de obra aparecem na série com
  `fonte_fisica = 'monit'`.
- **Curva S × livro-razão:** 25 de 26 meses batem; o mês que difere é explicado pela
  exclusão dos distratados.
- **Testes dbt:** grão único em todas as tabelas, acumulado financeiro que nunca
  regride, domínio de `fonte_fisica` e `nivel`, drift de layout nas bronzes.

## Limitações

- **Previsto só a partir de 2025-12.** Antes disso não há cronograma em nenhuma fonte.
- **2026-06 não foi publicado** no SharePoint (obra nem cadastro PJ; o financeiro sim). O
  realizado desse mês vem da CAIXA e o previsto é interpolado entre maio e julho. Os
  semanais de 202606 não substituem o mensal: são 9 APFs recém-incluídos, a 0%.
- **O previsto é o cronograma vigente, não a linha de base.** Apesar de o LAYOUT falar em
  percentual "inicialmente previsto", 5 a 9% dos APFs têm o previsto reduzido a cada mês
  (queda média de 5 a 11 p.p.), o que indica reprogramação. A curva planejada mostra o
  plano do mês, e atraso contra o plano original não aparece nela.
- **Sem curva financeira planejada.** Nenhuma fonte traz cronograma de desembolso.
- **Físico antes de 2025-04 é nulo:** nem MONIT nem CAIXA medem o Novo MCMV nesse período.
- **MONIT × CAIXA divergem.** Em 2026-07 o acumulado MONIT é R$ 9,14 bi e o CAIXA,
  R$ 10,0 bi, com 609 APFs acima de R$ 1 de diferença; no físico, a diferença média é
  de 1,3 p.p. O MONIT é a fonte oficial da série; as colunas `_caixa` e `dif_*` ficam
  para auditoria.
