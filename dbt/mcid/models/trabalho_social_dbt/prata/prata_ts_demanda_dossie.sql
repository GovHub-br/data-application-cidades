{{ config(materialized="table") }}

-- Prata: indicação de demanda e dossiês dos empreendimentos FAR da lista "empreendimentos sem
-- indicação de demanda" (SNH), nas duas remessas recebidas (02/09/2026 e 06/10/2026), tipadas.
-- Grão: APF × remessa. Chave: id_demanda_remessa.
-- A remessa de 06/10 não traz UF, município, UH, data de contratação, ente, construtora nem a
-- situação do TS: essas colunas vêm nulas nela (a ouro completa pela remessa de 02/09 e pelo TS).
--
-- Leitura dos campos:
--   qt_pesquisas_sigdh   grupos familiares indicados pelo ente e pesquisados no SIGDH (a indicação
--                        de demanda). Zero = o ente ainda não indicou famílias.
--   qt_dossies_enviados  dossiês das famílias enviados pelo aplicativo; aprovados = validados.
--   qt_contratos_assinados  contratos de pessoa física já assinados.
--
-- O SQL está em macros/corpos/corpos_trabalho_social.sql (corpo_prata_ts_demanda_dossie).

{{ corpo_prata_ts_demanda_dossie() }}
