# Mapeamento político-operacional das linhas MCMV

## Conclusão executiva

O fluxo do **FNHIS SUB50** é fortemente centrado no **TransfereGov** para proposta, instrumento, medição, comprovação e liberação. Ele não é exclusivamente TransfereGov: a validação/seleção de beneficiários passa pelo SIGDH e CadÚnico, e a verificação física envolve Caixa, prefeitura e assistência social.

As quatro linhas financiadas foram mantidas separadas: MCMV Financiada, MCMV Cidades, Pró-Moradia e Classe Média. Entidades/FDS, FNHIS SUB50 e Rural/PNHR aparecem somente no anexo de fronteira.

## Como identificar a fonte do dinheiro

Não existe uma única tabela suficiente para todos os níveis. Use a combinação:

- **Macro/alocação:** `tpc_2024_fgts_canal_tab_dbo_pc_2024_02_orcamento_final` para FGTS; `orcamento_resultado_ogu`/`dados_abertos_orcamento_2024` para ações OGU; `orcamento_resultado_fundo_social` para Fundo Social; tabelas de emendas para MCMV Cidades.
- **Operação/contrato:** `fgts_canal_tab_ao_1_contratos_fgts`, operações PF/PJ e desembolsos. É onde a fonte planejada precisa ser reconciliada com o contrato e o dinheiro efetivamente liberado.
- **Aporte MCMV Cidades:** campos como `vlr_contrapartida_parceria`, `vlr_capital_proprio`, `valor_contrapartida`, emenda e terreno. Terreno é aporte não financeiro e não deve ser somado como desembolso sem regra de valoração.

Portanto, `orcamento_resultado_ogu` responde **autorização/dotação**; não responde sozinho **quanto foi gasto**. Para gasto, procure empenhado/liquidado/pago na execução orçamentária e desembolso/liberação no nível contratual.

## Chaves de integração recomendadas

1. `cod_contrato` como eixo do contrato, preservado como texto.
2. `cod_empreendimento` como eixo físico da obra.
3. `numero_proposta`/instrumento para TransfereGov.
4. `codigo_da_emenda`/`numero_da_emenda` para MCMV Cidades.
5. CNPJ/IBGE para ente; CPF/NIS apenas em ambiente restrito.
6. Posições e desembolsos exigem chave composta com data/snapshot; não declarar PK antes de teste de unicidade.

## Disponibilidade observada

A consulta de leitura ao bucket `data-lake-mcid/raw` confirmou 107 objetos cujo nome contém MCMV Cidades e quatro arquivos de regularidade SNHIS em `raw/sharepoint`. A maioria dos nomes `fgts_canal_*`, `tpc_*`, `orcamento_resultado_*` e `int_*` é uma referência lógica do banco/dump e não apareceu como objeto individual com o mesmo nome no raw. Isso está explicitado linha a linha no CSV.

## Quantidade de etapas

- MCMV Financiada: 10
- MCMV Cidades: 7
- Pró-Moradia: 7
- Classe Média: 7
- FNHIS SUB50: 4
- Entidades/FDS: 1
- Rural/PNHR: 1

## Lacunas prioritárias

- Confirmar os esquemas reais das tabelas lógicas no dump e testar chaves/PKs.
- Obter execução orçamentária com empenhado, liquidado e pago para não confundir dotação com gasto.
- Inventariar as fórmulas e versões das cartas-consulta Pró-Moradia; `documento de origem` deve guardar arquivo, versão, aba e célula/faixa que gerou o registro.
- Materializar indicadores PPA somente com fórmula, período, fonte e data de corte documentados.
- Tratar CPF, NIS e CadÚnico sob LGPD; produtos de gestão devem ser agregados.

## Arquivo detalhado

O CSV contém uma linha por etapa, com ator, sistema, fonte de recurso, tabela, chave candidata, evidência de disponibilidade, confiança e lacuna.
