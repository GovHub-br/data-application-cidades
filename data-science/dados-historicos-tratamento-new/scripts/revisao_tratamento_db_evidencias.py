from __future__ import annotations

import os
import re
import subprocess
from io import StringIO
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
EVID = ROOT / "docs" / "evidencias" / "revisao-tratamento-dados"
ENV_PATH = ROOT / ".env"


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


def sanitize_table_name(name: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    if safe and safe[0].isdigit():
        safe = safe[:62]
    else:
        safe = safe[:63]
    safe = safe.lower()
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


def sample_table(
    db: Psql, schema: str, table_name: str, limit: int = 8
) -> tuple[bool, str, list[str]]:
    exists_sql = f"""
        select exists (
            select 1 from information_schema.tables
            where table_schema = '{schema.replace("'", "''")}'
              and table_name = '{table_name.replace("'", "''")}'
        ) as exists
    """
    exists = db.query(exists_sql).iloc[0, 0].lower() in ("true", "t", "1")
    if not exists:
        return False, "Tabela nao encontrada.", []

    df = db.query(
        f"select * from {quote_ident(schema)}.{quote_ident(table_name)} limit {int(limit)}"
    )
    sample = df.head(limit).to_string(index=False, max_cols=12, max_colwidth=42)
    return True, sample, [str(c) for c in df.columns]


def classify_reason(row: pd.Series) -> tuple[str, str, str]:
    status = str(row.get("status", ""))
    missing = float(row.get("missing_pct", 0) or 0)
    institution = str(row.get("institution", ""))
    report_date = str(row.get("report_date", ""))
    profile = str(row.get("profile", ""))

    if status == "discarded":
        return (
            "descarte_revisar",
            "Tabela descartada precisa ter justificativa estrutural verificavel.",
            "Confirmar se a categoria e vazia/dados_sem_utilidade; caso tenha dado util, ajustar regra de descarte.",
        )
    if missing > 30:
        return (
            "missing_alto",
            f"missing_pct={missing:.2f}% em tabela {profile}; risco de subtabelas esparsas ou colunas mantidas em excesso.",
            "Revisar extracao de subtabelas, remover colunas vazias/rodapes ou transformar blocos em formato longo antes do uso preditivo.",
        )
    if institution in ("", "unknown") or report_date == "":
        return (
            "metadados_incompletos",
            "Tabela tratada, mas institution/report_date incompletos enfraquecem linhagem temporal e uso preditivo.",
            "Estender inferir_instituicao() e extrair_periodo_filename() com padroes dessas tabelas; adicionar teste de regressao.",
        )
    return (
        "revisao_amostral",
        "Tabela escolhida para leitura amostral de consistencia.",
        "Manter como evidencia ou ampliar amostragem se entrar no conjunto de features.",
    )


def main() -> None:
    cfg = read_env(ENV_PATH)
    source_schema = cfg.get("DB_SCHEMA_SOURCE", "dados_historicos")
    target_schema = "dados_historicos_formatados"
    db = Psql(cfg)
    EVID.mkdir(parents=True, exist_ok=True)

    quality = db.query(f"select * from {quote_ident(target_schema)}._qualidade")
    quality["missing_pct_num"] = pd.to_numeric(
        quality["missing_pct"], errors="coerce"
    ).fillna(0)

    high_missing = quality[quality["missing_pct_num"].gt(30)].sort_values(
        "missing_pct_num", ascending=False
    )
    metadata_missing = quality[
        quality["institution"].isin(["", "unknown"]) | quality["report_date"].eq("")
    ].sort_values(
        ["institution", "report_date", "n_rows"], ascending=[True, True, False]
    )
    discarded = quality[quality["status"].eq("discarded")]

    selected = pd.concat(
        [high_missing, metadata_missing.head(12), discarded],
        ignore_index=True,
    ).drop_duplicates("table_name")

    rows: list[dict[str, object]] = []
    for _, row in selected.iterrows():
        table_name = str(row["table_name"])
        treated_name = sanitize_table_name(table_name)
        reason, evidence_note, fix_note = classify_reason(row)
        treated_exists, treated_sample, treated_cols = sample_table(
            db, target_schema, treated_name
        )
        raw_exists, raw_sample, raw_cols = sample_table(db, source_schema, table_name)
        if (
            str(row.get("status", "")) == "discarded"
            and raw_exists
            and "|" in raw_sample
        ):
            reason = "descarte_incorreto_pipe"
            evidence_note = (
                "Tabela foi descartada como vazia/dados_sem_utilidade, mas a amostra bruta "
                "tem registros pipe-delimited; ha dados estruturaveis."
            )
            fix_note = (
                "Corrigir classificacao para separador_|, reprocessar com tratar_separador_pipe(), "
                "e adicionar teste cobrindo esta familia de tabelas tab_*."
            )
        rows.append(
            {
                "table_name": table_name,
                "treated_table_name": treated_name,
                "status": row.get("status", ""),
                "profile": row.get("profile", ""),
                "n_rows": row.get("n_rows", ""),
                "n_cols": row.get("n_cols", ""),
                "institution": row.get("institution", ""),
                "report_date": row.get("report_date", ""),
                "missing_pct": row.get("missing_pct", ""),
                "motivo_discordancia": reason,
                "evidencia": evidence_note,
                "como_consertar": fix_note,
                "treated_exists": treated_exists,
                "raw_exists": raw_exists,
                "treated_columns": " | ".join(treated_cols[:30]),
                "raw_columns": " | ".join(raw_cols[:30]),
                "treated_sample_print": treated_sample,
                "raw_sample_print": raw_sample,
            }
        )

    out = pd.DataFrame(rows)
    out.to_csv(EVID / "discordancias_tratamento_db_pandas.csv", index=False)

    summary = pd.DataFrame(
        [
            {
                "schema_fonte": source_schema,
                "schema_tratado": target_schema,
                "qualidade_linhas": len(quality),
                "tratadas": int(quality["status"].eq("treated").sum()),
                "descartadas": int(quality["status"].eq("discarded").sum()),
                "erros": int(quality["status"].eq("error").sum()),
                "missing_gt_30": int(quality["missing_pct_num"].gt(30).sum()),
                "institution_unknown": int(
                    quality["institution"].isin(["", "unknown"]).sum()
                ),
                "report_date_missing": int(quality["report_date"].eq("").sum()),
                "discordancias_amostradas": len(out),
            }
        ]
    )
    summary.to_csv(EVID / "resumo_db_tratamento_pandas.csv", index=False)
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
