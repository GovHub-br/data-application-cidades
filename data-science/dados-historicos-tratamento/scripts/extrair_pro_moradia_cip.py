#!/usr/bin/env python3
"""Extrai o recorte Pró-Moradia (cod_linha=26) do CIP MC20260306."""

from __future__ import annotations

import csv
import io
import os
import subprocess
from pathlib import Path

import pandas as pd


MDB_DIR = Path("/tmp/mcid_cip_inspecao")
MDB_BIN = Path("/tmp/mdbtools-local/usr/bin")
MDB_LIB = "/tmp/mdbtools-local/usr/lib/x86_64-linux-gnu"
ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs/evidencias/inventario-linhas-mcmv-com-sub50"

DATABASES = {
    "MCidades_AO_1.mdb": [
        "Linha", "Modalidade", "Municípios", "Entidades", "SituaçãoDaObra",
        "SituaçãoDoContrato", "tab_empreendimentos", "tab_Empreendimentos_Construtor",
        "tab_empreendimentos_GPS", "tab_empreendimentos_posicoes", "OperaçõesPJ_PF",
        "tab_contratos_fgts",
    ],
    "MCidades_AO_2.mdb": [
        "tab_execucoes_obras", "operações_paralisadas_FGTS_setorpublico",
        "tab_desembolsos_fgts",
    ],
    "MCidades_AO_3.mdb": ["acompanhamento_termino_obra"],
}


def command(name: str) -> str:
    path = MDB_BIN / name
    if not path.is_file():
        raise SystemExit(f"Ferramenta não encontrada: {path}")
    return str(path)


def run(args: list[str]) -> str:
    env = dict(os.environ)
    env["LD_LIBRARY_PATH"] = MDB_LIB
    return subprocess.run(args, check=True, capture_output=True, text=True, env=env).stdout


def export(db: str, table: str) -> pd.DataFrame:
    raw = run([command("mdb-export"), str(MDB_DIR / db), table])
    return pd.read_csv(io.StringIO(raw), dtype=str, keep_default_na=False)


def normalize_key(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.replace(r"\D", "", regex=True).str.lstrip("0")


def latest(df: pd.DataFrame, key: str, period: str, prefix: str) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out[key] = out[key].str.strip()
    out = out.sort_values(period).drop_duplicates(key, keep="last")
    return out.rename(columns={c: f"{prefix}_{c}" for c in out.columns if c != key})


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    frames: dict[str, pd.DataFrame] = {}
    inventory = []
    for db, tables in DATABASES.items():
        for table in tables:
            df = export(db, table)
            frames[table] = df
            inventory.append({
                "objeto_minio": "raw/sharepoint/Canal FGTS/MC20260306.zip",
                "arquivo_interno": db,
                "tabela_access": table,
                "linhas_total": len(df),
                "colunas_total": len(df.columns),
                "colunas": "; ".join(df.columns),
                "chave_pro_moradia": "cod_linha=26" if "cod_linha" in df.columns else "join por cod_contrato/cod_empreendimento",
            })

    linha = frames["Linha"].rename(columns={"Código": "cod_linha", "Linha": "descricao_linha"})
    linha["cod_linha"] = linha["cod_linha"].str.strip()
    contratos = frames["tab_contratos_fgts"].copy()
    for c in contratos.columns:
        contratos[c] = contratos[c].str.strip()
    pro = contratos.loc[contratos["cod_linha"].eq("26")].copy()
    pro = pro.merge(linha, on="cod_linha", how="left", validate="many_to_one")

    emp = frames["tab_empreendimentos"].copy()
    emp["cod_empreendimento"] = emp["cod_empreendimento"].str.strip()
    pro = pro.merge(emp, on="cod_empreendimento", how="left", validate="many_to_one", suffixes=("", "_empreendimento"))

    sit = frames["SituaçãoDoContrato"].rename(columns={"Código": "cod_situacao_contrato", "SituaçãoDoContrato": "situacao_contrato"})
    sit["cod_situacao_contrato"] = sit["cod_situacao_contrato"].str.strip()
    pro = pro.merge(sit, on="cod_situacao_contrato", how="left", validate="many_to_one")

    mod = frames["Modalidade"].rename(columns={"Código": "cod_modalidade", "Modalidade": "modalidade"})
    mod["cod_modalidade"] = mod["cod_modalidade"].str.strip()
    pro = pro.merge(mod, on="cod_modalidade", how="left", validate="many_to_one")

    pos = latest(frames["tab_empreendimentos_posicoes"], "cod_empreendimento", "dte_ano_mes", "posicao")
    pro = pro.merge(pos, on="cod_empreendimento", how="left", validate="many_to_one")

    exe = latest(frames["tab_execucoes_obras"], "cod_contrato", "dte_ano_mes_avaliacao", "execucao")
    pro = pro.merge(exe, on="cod_contrato", how="left", validate="many_to_one")

    desemb = frames["tab_desembolsos_fgts"].copy()
    desemb["cod_contrato"] = desemb["cod_contrato"].str.strip()
    desemb["vlr_liberado_num"] = pd.to_numeric(desemb["vlr_liberado"], errors="coerce")
    dagg = desemb.groupby("cod_contrato", as_index=False).agg(
        qtd_desembolsos=("vlr_liberado", "size"),
        vlr_liberado_total=("vlr_liberado_num", "sum"),
        ultimo_ano_desembolso=("dte_ano", "max"),
    )
    pro = pro.merge(dagg, on="cod_contrato", how="left", validate="many_to_one")

    paral = frames["operações_paralisadas_FGTS_setorpublico"].copy()
    paral["cod_contrato"] = paral["cod_contrato"].str.strip()
    paral = paral.drop_duplicates("cod_contrato", keep="last")
    pro = pro.merge(paral.add_prefix("paralisacao_").rename(columns={"paralisacao_cod_contrato": "cod_contrato"}), on="cod_contrato", how="left", validate="many_to_one")

    termino = frames["acompanhamento_termino_obra"].copy()
    termino["contrato_join"] = normalize_key(termino["NúmeroDoContrato"])
    pro["contrato_join"] = normalize_key(pro["cod_contrato"] + pro["cod_contrato_dv"])
    termino = termino.drop_duplicates("contrato_join", keep="last")
    pro = pro.merge(termino.add_prefix("termino_").rename(columns={"termino_contrato_join": "contrato_join"}), on="contrato_join", how="left", validate="many_to_one")

    pro.insert(0, "fonte_objeto", "raw/sharepoint/Canal FGTS/MC20260306.zip")
    pro.insert(1, "fonte_arquivo", "MCidades_AO_1.mdb + AO_2.mdb + AO_3.mdb")
    pro.to_csv(OUT / "pro_moradia_cod_linha_26_contratos_enriquecidos.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(inventory).to_csv(OUT / "cip_mc20260306_inventario_tabelas.csv", index=False, encoding="utf-8-sig")

    summary = pd.DataFrame([{
        "cod_linha": "26", "descricao_linha": "HAB / PRO-MORADIA",
        "contratos": len(pro), "empreendimentos_identificados": pro["cod_empreendimento"].nunique(),
        "valor_contratado_total": pd.to_numeric(pro["Vlr_contratado"], errors="coerce").sum(),
        "valor_investimento_total": pd.to_numeric(pro["Vlr_investimento"], errors="coerce").sum(),
        "contratos_com_posicao": pro["posicao_dte_ano_mes"].ne("").sum(),
        "contratos_com_execucao": pro["execucao_dte_ano_mes_avaliacao"].ne("").sum(),
        "contratos_com_desembolso": pro["qtd_desembolsos"].fillna(0).gt(0).sum(),
        "contratos_em_base_paralisacao": pro.get("paralisacao_dias_sem_evolução", pd.Series(index=pro.index, dtype=str)).fillna("").ne("").sum(),
        "data_corte_cip": "2026-03-06",
    }])
    summary.to_csv(OUT / "pro_moradia_cod_linha_26_resumo.csv", index=False, encoding="utf-8-sig")
    print(summary.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
