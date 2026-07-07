from __future__ import annotations

import os
import re
import subprocess
from io import StringIO
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
EVID = ROOT / "docs" / "evidencias" / "revisao-classificacao-issue-96"
ENV_PATH = ROOT / ".env"

TARGETS = [
    "bb_2013_06_junho_pmcmv_18062013_tab_andamento_obras",
    "bb_2013_06_junho_pmcmv_18062013_tab_arquivos_dados",
    "bb_2013_06_junho_pmcmv_18062013_tab_caracterizacoes_entornos",
    "bb_2013_06_junho_pmcmv_18062013_tab_contratos_pj",
    "bb_2013_06_junho_pmcmv_18062013_tab_proponentes",
    "bb_2013_06_junho_pmcmv_18062013_tab_unidades_concluidas",
]


def read_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sanitize_table_name(name: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_]", "_", name).lower()
    safe = safe[:62] if safe[:1].isdigit() else safe[:63]
    if safe and not re.match(r"[a-z_]", safe[0]):
        safe = "_" + safe
    return safe or "_table"


class Psql:
    def __init__(self, cfg: dict[str, str]) -> None:
        self.cfg = cfg

    def query(self, sql: str) -> pd.DataFrame:
        env = os.environ.copy()
        env["PGPASSWORD"] = self.cfg["DB_PASSWORD"]
        cmd = [
            "psql",
            "-X",
            "-v",
            "ON_ERROR_STOP=1",
            "--csv",
            "-h",
            self.cfg["DB_HOST"],
            "-p",
            self.cfg["DB_PORT"],
            "-U",
            self.cfg["DB_USER"],
            "-d",
            self.cfg["DB_NAME"],
            "-c",
            sql,
        ]
        proc = subprocess.run(cmd, env=env, check=True, capture_output=True, text=True)
        if not proc.stdout.strip():
            return pd.DataFrame()
        return pd.read_csv(StringIO(proc.stdout), dtype=str).fillna("")


def table_exists(db: Psql, schema: str, table_name: str) -> bool:
    exists = db.query(
        f"""
        select exists (
            select 1 from information_schema.tables
            where table_schema = {sql_literal(schema)}
              and table_name = {sql_literal(table_name)}
        ) as exists
        """
    )
    return not exists.empty and str(exists.iloc[0, 0]).lower() in {"true", "t", "1"}


def sample_table(db: Psql, schema: str, table_name: str, limit: int = 8) -> tuple[bool, int, pd.DataFrame]:
    if not table_exists(db, schema, table_name):
        return False, 0, pd.DataFrame()
    count = db.query(f"select count(*) as n_rows from {quote_ident(schema)}.{quote_ident(table_name)}")
    sample = db.query(
        f"select * from {quote_ident(schema)}.{quote_ident(table_name)} limit {int(limit)}"
    )
    return True, int(count.iloc[0]["n_rows"]), sample


def pipe_metrics(df: pd.DataFrame) -> tuple[int, int, pd.DataFrame]:
    if df.empty:
        return 0, 0, pd.DataFrame()
    as_text = df.astype(str)
    pipe_cells = int(as_text.apply(lambda col: col.str.contains(r"\|", regex=True)).sum().sum())
    first_col = as_text.columns[0]
    expanded = as_text[first_col].str.split("|", expand=True)
    expanded.columns = [f"campo_{i + 1}" for i in range(expanded.shape[1])]
    return pipe_cells, expanded.shape[1], expanded


def main() -> None:
    cfg = read_env(ENV_PATH)
    source_schema = cfg.get("DB_SCHEMA_SOURCE", "dados_historicos")
    target_schema = "dados_historicos_formatados"
    db = Psql(cfg)
    EVID.mkdir(parents=True, exist_ok=True)

    target_sql = ", ".join(sql_literal(t) for t in TARGETS)
    current = db.query(
        f"""
        select c.table_name, c.formacao, c.confidence,
               coalesce(q.status, '') as status,
               coalesce(q.profile, '') as profile,
               coalesce(q.n_rows::text, '') as n_rows,
               coalesce(q.n_cols::text, '') as n_cols
        from {quote_ident(target_schema)}._classificacao c
        left join {quote_ident(target_schema)}._qualidade q using(table_name)
        where c.table_name in ({target_sql})
        order by c.table_name
        """
    ).set_index("table_name")

    rows: list[dict[str, object]] = []
    prints: list[dict[str, object]] = []
    for table_name in TARGETS:
        raw_exists, raw_rows, raw_sample = sample_table(db, source_schema, table_name)
        treated_name = sanitize_table_name(table_name)
        treated_exists = table_exists(db, target_schema, treated_name)
        pipe_cells, expanded_cols, expanded = pipe_metrics(raw_sample)
        before = current.loc[table_name] if table_name in current.index else pd.Series(dtype=str)
        erro = (
            "Classificacao antiga descartava tabela, mas a amostra bruta contem valores pipe-delimited."
            if pipe_cells
            else "Sem evidencia pipe na amostra consultada."
        )
        rows.append(
            {
                "table_name": table_name,
                "classificacao_antes": before.get("formacao", ""),
                "confidence_antes": before.get("confidence", ""),
                "status_antes": before.get("status", ""),
                "profile_antes": before.get("profile", ""),
                "classificacao_corrigida": "separador_|",
                "status_corrigido": "treated",
                "profile_corrigido": "separador_pipe",
                "n_rows_corrigido": raw_rows,
                "n_cols_corrigido": expanded_cols,
                "raw_exists": raw_exists,
                "treated_exists_antes": treated_exists,
                "pipe_cells_amostra": pipe_cells,
                "raw_columns": " | ".join(map(str, raw_sample.columns[:30])),
                "erro_identificado": erro,
                "evidencia_erro": f"Amostra DB tem {pipe_cells} celulas com pipe em {len(raw_sample)} linhas consultadas.",
                "evidencia_correcao": f"Split por pipe gera {expanded_cols} colunas estruturadas e {raw_rows} linhas brutas.",
                "como_consertar": "Classificar como separador_|, reprocessar com tratar_separador_pipe() e manter teste de regressao para a familia tab_*.",
            }
        )
        prints.append(
            {
                "table_name": table_name,
                "raw_sample_print": raw_sample.head(6).to_string(index=False, max_cols=8, max_colwidth=58),
                "expanded_sample_print": expanded.head(6).to_string(index=False, max_cols=12, max_colwidth=32),
            }
        )

    corrections = pd.DataFrame(rows)
    samples = pd.DataFrame(prints)
    summary = pd.DataFrame(
        [
            {
                "schema_fonte": source_schema,
                "schema_tratado": target_schema,
                "tabelas_auditadas": len(corrections),
                "tabelas_corrigidas": int(corrections["pipe_cells_amostra"].gt(0).sum()),
                "pipe_cells_total_amostra": int(corrections["pipe_cells_amostra"].sum()),
                "linhas_brutas_total_corrigidas": int(corrections["n_rows_corrigido"].sum()),
            }
        ]
    )
    corrections.to_csv(EVID / "correcoes_classificacao_db_pandas.csv", index=False)
    samples.to_csv(EVID / "amostras_pipe_db_pandas.csv", index=False)
    summary.to_csv(EVID / "resumo_db_classificacao_pandas.csv", index=False)
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
