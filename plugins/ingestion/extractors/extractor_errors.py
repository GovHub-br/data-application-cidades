"""Erros da extração, independentes da fonte."""


class ExtractionError(Exception):
    """A fonte respondeu, mas não entregou o que a extração precisa."""


class SourceNotFoundError(ExtractionError):
    """Não há o que extrair: recurso inexistente (404) ou e-mail do dia ausente.

    Separado de ExtractionError porque, em várias fontes, é esperado: o passo do
    pipeline transforma em skip em vez de falha.
    """
