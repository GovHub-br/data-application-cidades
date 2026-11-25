"""Erros do tratamento dos dados históricos."""


class ErroHistorico(ValueError):
    """O dado não pode seguir sem perder, inventar ou expor informação."""


class ErroDadoPessoal(ErroHistorico):
    """Dado pessoal em claro onde o histórico deveria trazer só hash: para tudo."""
