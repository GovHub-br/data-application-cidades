"""FGV: INCC-M (pela Sinduscon-PR)."""

from ingestion.extractors import HttpRequest

#: Sem User-Agent de navegador, o servidor da Sinduscon recusa a requisição.
NAVEGADOR = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

SINDUSCON_PR = "https://sindusconpr.com.br"

#: Link estável da série histórica; redireciona para o xlsx da edição corrente.
INCC_M = HttpRequest(
    name="incc_m",
    endpoint="/economia/indices-economicos/incc-m-fgv/9547-serie-historica-incc-m-fgv/",
    headers=NAVEGADOR,
)
