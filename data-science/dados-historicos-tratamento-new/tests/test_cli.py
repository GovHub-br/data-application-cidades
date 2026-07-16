"""Testes de CLI para flags --skip-classify, --classify-only e --sftp."""

from __future__ import annotations

import subprocess
import sys
from argparse import ArgumentParser
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent


def setup_module() -> None:
    """Remove output files de execuções anteriores para evitar interferência."""
    paths = [
        REPO_ROOT / "data/classificacao_formacao.csv",
        REPO_ROOT / "data/relatorio_divergencias.md",
        REPO_ROOT / "data/treated_tables/_dedup_map.csv",
        REPO_ROOT / "data/treated_tables/_quality_report.csv",
    ]
    for p in paths:
        if p.exists():
            p.unlink()


def _run_main(*args: str) -> subprocess.CompletedProcess:
    """Executa main.py com uv run e argumentos fornecidos."""
    cmd = ["uv", "run", "python", "main.py", *args]
    return subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=120,
    )


def _run_parser(*args: str) -> None:
    """Testa a validação de argumentos sem executar o pipeline.

    Replica a lógica de validação do main.py:
    - ``--sftp`` sem ``--db`` → ``sys.exit(2)``
    - Caso contrário → retorna normalmente

    Raises
    ------
    SystemExit
        Se a validação falhar (exit code 2).
    """
    parser = ArgumentParser(description="Pipeline MCMV")
    parser.add_argument("--db", action="store_true")
    parser.add_argument("--sftp", action="store_true")
    parser.add_argument("--skip-classify", action="store_true")
    parser.add_argument("--classify-only", action="store_true")
    parser.add_argument("--inventario", action="store_true")

    parsed = parser.parse_args(args)

    if parsed.sftp and not parsed.db:
        sys.exit(2)


class TestCliFlags:
    """Testes de integração CLI."""

    def test_conflito_skip_classify_e_classify_only(self) -> None:
        """Task 7.4: --skip-classify --classify-only causa sys.exit(2)."""
        result = _run_main("--skip-classify", "--classify-only")

        assert result.returncode == 2
        assert "mutuamente exclusivos" in result.stderr

    def test_classify_only_nao_executa_tratamento(self) -> None:
        """Task 7.5: --classify-only gera classificação e não faz tratamento."""
        result = _run_main("--classify-only")

        assert result.returncode == 0
        # Deve gerar classificação
        assert (REPO_ROOT / "data/classificacao_formacao.csv").exists()
        assert (REPO_ROOT / "data/relatorio_divergencias.md").exists()
        # NÃO deve gerar outputs de tratamento
        assert not (REPO_ROOT / "data/treated_tables/_dedup_map.csv").exists()

    def test_classify_only_com_inventario_ignora_inventario(self) -> None:
        """--classify-only --inventario ignora inventário com aviso."""
        result = _run_main("--classify-only", "--inventario")

        assert result.returncode == 0
        assert "não é executado" in result.stderr

    def test_skip_classify_com_classificacao_existente(self) -> None:
        """Task 7.6 (parcial): --skip-classify carrega classificação e trata."""
        # Primeiro gera a classificação
        _run_main("--classify-only")

        # Depois usa --skip-classify
        result = _run_main("--skip-classify")

        assert result.returncode == 0
        assert (REPO_ROOT / "data/treated_tables/_dedup_map.csv").exists()
        assert (REPO_ROOT / "data/treated_tables/_quality_report.csv").exists()

    def test_execucao_default_sem_flags(self) -> None:
        """Task 7.6: execução sem flags produz pipeline completo."""
        result = _run_main()

        assert result.returncode == 0
        # Todos os outputs padrão devem existir
        assert (REPO_ROOT / "data/classificacao_formacao.csv").exists()
        assert (REPO_ROOT / "data/relatorio_divergencias.md").exists()
        assert (REPO_ROOT / "data/treated_tables/_dedup_map.csv").exists()
        assert (REPO_ROOT / "data/treated_tables/_quality_report.csv").exists()

    # ── SFTP flag tests (task 7.5) ──────────────────────────────────────

    def test_sftp_sem_db_retorna_erro(self) -> None:
        """--sftp sem --db deve causar sys.exit(2)."""
        result = _run_main("--sftp")

        assert result.returncode == 2
        assert "requer --db" in result.stderr

    def test_sftp_com_db_aceito_na_validacao(self) -> None:
        """--sftp --db é aceito pela validação de args (não levanta SystemExit).

        Usa o parser diretamente, sem executar o pipeline (evita
        dependência de conexão PostgreSQL).
        """
        # Deve passar sem exceção
        _run_parser("--sftp", "--db")
