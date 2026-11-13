{{ config(materialized="table") }}

-- Prata: perfil de cada contrato com Trabalho Social para o mapa: coordenadas, tipo de edificação
-- (casa, apartamento...), regime (condomínio ou loteamento), tipo de demanda e público tradicional.
-- Grão: id_ts (linha × agente × APF). Chave: id_ts.
--
-- Fontes e regras:
--   coordenadas     1º bronze_shpt_dados_prioritarios_snh_empreendimentos (SNH, por APF, retrato mais
--                   recente). Só valem pontos dentro do retângulo do Brasil (há valor sem ponto
--                   decimal e pontos no oceano, ex.: -0,0154 / -0,4845, que ficam de fora).
--                   2º centro do município (seed ibge_municipio_centroide, API de malhas do IBGE),
--                   pelo código IBGE ou, sem ele, pelo nome do município + UF. origem_coordenada diz qual.
--   tipo de edificação  FAR: INT040 (dsc_tipologia); FDS: INT059 (dsc_tipologia); Rural: casa
--                   construída no terreno da família (regra do PNHR, não vem da base).
--   regime          a tipologia da planilha de TS (CONDOMINIO, LOTEAMENTO).
--   tipo de demanda FAR: Cad PJ (co_tipo_demanda) com o seed ts_dominio_tipo_demanda.
--   público tradicional  ESTIMATIVA por palavra-chave (seed ts_publico_tradicional) no nome da entidade
--                   organizadora (FDS e Rural) e no nome do empreendimento. Não há campo próprio na
--                   origem. origem_publico_tradicional diz onde a palavra apareceu: no nome da EO é
--                   indício mais forte que no nome do empreendimento.
--
-- O SQL está em macros/corpos/corpos_trabalho_social.sql (corpo_prata_ts_empreendimento_perfil).

{{ corpo_prata_ts_empreendimento_perfil() }}
