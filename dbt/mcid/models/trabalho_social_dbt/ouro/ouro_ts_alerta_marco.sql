{{ config(materialized="table") }}

-- Ouro: alertas de marco temporal do Trabalho Social. Uma linha por contrato × marco da portaria
-- de TS do contrato que JÁ ESTÁ VALENDO para ele (o gatilho foi atingido). ic_descumprido diz se os
-- dados mostram que o marco não foi cumprido, e `evidencia` diz por quê, em texto.
-- Grão: id_ts × id_marco. Chave: id_alerta.
-- Fontes: prata_ts_portaria_contrato (portaria resolvida), prata_ts_acompanhamento (obra, relatórios,
-- desembolso, situação) e prata_ts_marco_portaria (réguas da 464/2018 e da 75/2025).
--
-- Só entram os marcos que dá para verificar com o que a planilha traz (% de obra, entrega, datas de
-- relatório, execução e desembolso do TS). Ficam de fora as penalidades, a pré-obras (sem prazo na
-- norma), o 464-RURAL-02 (a norma não fixa intervalo) e as linhas SUB50 e Pró-Moradia (sem planilha).
-- Contratos com portaria 21/2014, 168/2013, anterior à 168 ou sem portaria não têm régua e não entram.
--
-- Sinais usados (dt_referencia = competência da planilha, jul/2026):
--   ts_encerrado   situação Concluído ou Distrato: nenhum marco é cobrado.
--   ts_com_sinal   há execução do TS > 0, desembolso > 0 ou algum relatório. Sem nenhum desses, o TS
--                  não deu sinal de ter começado (indício de convênio/PTS ausente; a planilha não traz
--                  a data do convênio nem da aprovação do PTS).
--   dias_sem_relatorio  dt_referencia - último relatório (ou o primeiro, no BB, se só houver ele).
--
-- O SQL está em macros/corpos/corpos_trabalho_social.sql (corpo_ouro_ts_alerta_marco), para o
-- mapa usar a mesma regra sem ler outra ouro.

{{ corpo_ouro_ts_alerta_marco() }}
