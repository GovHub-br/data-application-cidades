"""Onde cada camada dos dados históricos fica no lake (bucket e prefixos)."""

import os

BUCKET = "data-lake-mcid"
RAW = "raw/dados_historicos/"  # o dump congelado, subido à mão (o Asset de entrada)

# Tudo sob staging/historico_mcmv/: staging/dados_historicos/ é da conversão antiga do dump.
# HISTORICO_MCMV_PREFIXO desvia as camadas (não a raw) para outro prefixo, para rodar a DAG
# sem tocar nas oficiais (ex.: tests/historico_mcmv/, com os mapas copiados em config/).
BASE = os.environ.get("HISTORICO_MCMV_PREFIXO", "staging/historico_mcmv/")
if not BASE.endswith("/"):
    raise ValueError(f"HISTORICO_MCMV_PREFIXO precisa terminar em '/': {BASE!r}")
STAGING = BASE + "convertido/"
ANALYTICS = BASE + "analytics/"
CLEANSED = BASE + "cleansed/"

# Os mapas de tratamento ficam no lake, não no repositório (o repositório é público).
CONFIG = BASE + "config/"
MAPA_CABECALHOS = CONFIG + "cabecalhos.csv"
MAPA_FAMILIAS = CONFIG + "familias.csv"

# Controle de cada camada
MANIFESTO = STAGING + "_manifesto.csv"
LOG_ANALYTICS = ANALYTICS + "_analytics_log.parquet"
LOG_CLEANSED = CLEANSED + "_cleansed_log.parquet"
DESCARTES_CLEANSED = CLEANSED + "_cleansed_descartes.parquet"
PADRONIZACOES = CLEANSED + "_cleansed_padronizacoes.parquet"
EVOLUCAO = CLEANSED + "_colunas_evolucao.parquet"
CANDIDATOS = CLEANSED + "_colunas_candidatos.parquet"
CONCEITOS_ARQ = CLEANSED + "_colunas_conceitos.parquet"
GRAFO = CLEANSED + "_grafo_colunas.md"
# versão substituída fica no cleansed, fora da pasta da família (a bronze não lê)
SUBSTITUIDAS = CLEANSED + "_versoes_substituidas/"
# arquivos de controle do cleansed; tudo o mais é saída (há saída que começa com "_")
CONTROLE_CLEANSED = frozenset(
    {LOG_CLEANSED, DESCARTES_CLEANSED, EVOLUCAO, CANDIDATOS, CONCEITOS_ARQ, PADRONIZACOES}
)
