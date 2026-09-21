"""Batimento entre dump histórico MCMV e SFTP.

Este módulo implementa a análise de correspondência entre o schema
``dados_historicos`` (~5 GB, 753 tabelas) e o schema ``sftp`` (~70 GB,
2008 tabelas) para a issue #95 do projeto Cidades.

Fornece funções para leitura de artefatos SQL, normalização de nomes,
matching estrutural em 3 camadas (hash exato, stem canônico, similaridade
de colunas) e geração de relatórios de batimento.

Módulos
-------
leitura_artefatos
    Leitura dos CSVs de artefatos extraídos do PostgreSQL.
normalizacao
    Normalização de nomes de tabela e coluna para matching fuzzy.
matching
    Matching estrutural em 3 camadas e identificação de chaves candidatas.
relatorio
    Geração do relatório markdown final com os 5 entregáveis da issue.
"""

__all__ = [
    "leitura_artefatos",
    "normalizacao",
    "matching",
    "relatorio",
]
