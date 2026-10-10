"""Perfil da FGV: a mecânica das fontes, sem escolha de pipeline."""

from ingestion.sources import fgv


def test_incc_m_is_the_stable_link_of_the_sinduscon_pr() -> None:
    assert fgv.SINDUSCON_PR == "https://sindusconpr.com.br"
    assert fgv.INCC_M.name == "incc_m"
    assert fgv.INCC_M.endpoint == (
        "/economia/indices-economicos/incc-m-fgv/9547-serie-historica-incc-m-fgv/"
    )
    # Sem User-Agent de navegador o servidor recusa a requisição.
    assert "Mozilla" in fgv.INCC_M.headers["User-Agent"]
