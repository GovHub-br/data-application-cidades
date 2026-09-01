# scripts/diagnostico_qualidade.py

"""Perfil de qualidade das camadas bronze/silver/gold de um domínio.

Responde as perguntas que um teste do dbt só consegue responder DEPOIS que alguém
decidiu o que asseverar:

  - sobrou mojibake em algum valor? (verifica se o reprocessamento do encoding pegou)
  - quais colunas são 100% nulas? (coluna morta: ou a origem mudou, ou o model erra o nome)
  - a chave natural duplica?
  - os left joins da silver casam, ou estão caindo no coalesce em silêncio?
  - quais os valores reais dos campos de corte? (o join por 'CAIXA' só funciona se a
    origem escrever exatamente 'CAIXA')

Gera um relatório markdown. A saída ORIENTA os testes do dbt — ela não os substitui:
o que for regra permanente vira not_null/unique/relationships/accepted_values no
schema.yml, que roda em todo build.

Uso:
    python scripts/diagnostico_qualidade.py --dominio rural > diagnostico_rural.md
"""

import argparse
import os
import sys
from typing import Dict, List, Tuple

import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Marcadores do round-trip utf-8 -> latin-1, mais o U+FFFD que o errors="replace" deixa.
MOJIBAKE = ["Ã", "Â", "â€", "�"]

DOMINIOS: Dict[str, dict] = {
    "rural": {
        "bronze": [
            "novo_mcmv_rural_cad_pj_mensal",
            "novo_mcmv_rural_cadastro_pf_mensal",
            "novo_mcmv_rural_obra_mensal",
            "novo_mcmv_rural_financeiro_mensal",
            "dados_prioritarios_disponibilizados_snh_empreendimentos",
            "dados_prioritarios_recebidos_caixa_empreendimentos",
            "dados_prioritarios_recebidos_bb_empreendimentos",
            "int_empreendimentos_int_065_pnhr_caixa_pj",
            "int_empreendimentos_int_057_pnhr_bb_pj",
            "int_financeiro_int055_liberacoes_caixa_bb",
            "base_trabalho_social_pnhr_rural_caixa",
            "base_trabalho_social_pnhr_bb",
        ],
        "schema_dbt": "empreendimento_rural",
        # model -> chave natural esperada
        "chaves": {
            "rural_empreendimento": ["apf"],
            "rural_prioritarios_snh": ["apf"],
            "rural_prioritarios_caixa": ["apf"],
            "rural_prioritarios_bb": ["apf"],
            "rural_cadastro_pj": ["apf"],
            "rural_pnhr_caixa": ["apf"],
            "rural_pnhr_bb": ["apf"],
            "rural_trabalho_social_caixa": ["apf"],
            "rural_trabalho_social_bb": ["apf"],
            "rural_obra_mensal": ["apf"],
            "ficha_empreendimento_rural": ["apf"],
            "evolucao_financeira_rural": ["apf", "mes"],
            "ficha_trabalho_social": ["apf", "agente_financeiro"],
            "perfil_beneficiarios": ["apf"],
            "infraestrutura_agua_saneamento": ["apf"],
        },
        # campos de corte: os valores reais importam porque viram condição de join
        "cortes": [
            ("rural_prioritarios_snh", "agente_financeiro"),
            ("rural_prioritarios_snh", "modalidade"),
            ("rural_prioritarios_snh", "situacao"),
            ("rural_empreendimento", "agente_financeiro"),
            ("ficha_empreendimento_rural", "programa"),
            ("ficha_empreendimento_rural", "status_execucao_simplificado"),
            ("ficha_empreendimento_rural", "status_prazo"),
        ],
        # cobertura: (esquerda, direita, coluna de junção) — quantos da esquerda acham par
        "joins": [
            ("rural_prioritarios_snh", "rural_prioritarios_caixa", "apf"),
            ("rural_prioritarios_snh", "rural_prioritarios_bb", "apf"),
            ("rural_prioritarios_snh", "rural_cadastro_pj", "apf"),
            ("rural_prioritarios_snh", "rural_pnhr_caixa", "apf"),
            ("rural_prioritarios_snh", "rural_pnhr_bb", "apf"),
            ("rural_empreendimento", "rural_cadastro_pf", "apf"),
            ("rural_empreendimento", "rural_trabalho_social_caixa", "apf"),
            ("rural_empreendimento", "rural_obra_mensal", "apf"),
        ],
    }
}


def conectar():  # type: ignore[no-untyped-def]
    return psycopg2.connect(
        host=os.environ["DB_DW_HOST_MCID"],
        port=int(os.environ.get("DB_DW_PORT_MCID", 5432)),
        user=os.environ["DB_DW_USER_MCID"],
        password=os.environ["DB_DW_PASSWORD_MCID"],
        dbname=os.environ["DB_DW_DBNAME_MCID"],
    )


def colunas(cur, schema: str, tabela: str) -> List[Tuple[str, str]]:
    cur.execute(
        """select column_name, data_type from information_schema.columns
           where table_schema = %s and table_name = %s order by ordinal_position""",
        (schema, tabela),
    )
    return cur.fetchall()


def n_linhas(cur, schema: str, tabela: str) -> int:
    """Linhas da tabela, ou -1 se ela não existe.

    Tabela ausente é situação normal aqui: o diagnóstico costuma rodar logo depois de um
    `dbt build` que falhou no meio, e é justamente aí que ele é mais útil. Abortar o
    relatório inteiro porque um model não materializou seria o pior momento para abortar.
    """
    try:
        cur.execute(f'select count(*) from "{schema}"."{tabela}"')
        return int(cur.fetchone()[0])
    except psycopg2.Error:
        cur.connection.rollback()
        return -1


def secao_mojibake(cur, schema: str, tabelas: List[str]) -> List[str]:
    """Conta linhas com marcador de mojibake em cada coluna textual."""
    out, achou = [], False
    for tab in tabelas:
        if n_linhas(cur, schema, tab) < 0:
            out.append(f"| `{tab}` | **(não materializada)** | — |")
            achou = True
            continue
        cols = [c for c, t in colunas(cur, schema, tab) if t in ("text", "character varying")]
        if not cols:
            continue
        cond = " or ".join(
            f"""("{c}" like '%%{m}%%')""" for c in cols for m in MOJIBAKE
        )
        try:
            cur.execute(f'select count(*) from "{schema}"."{tab}" where {cond}')
        except psycopg2.Error:
            cur.connection.rollback()
            continue
        n = int(cur.fetchone()[0])
        total = max(n_linhas(cur, schema, tab), 0)
        if n:
            achou = True
            piores = []
            for c in cols:
                sub = " or ".join(f"""("{c}" like '%%{m}%%')""" for m in MOJIBAKE)
                cur.execute(f'select count(*) from "{schema}"."{tab}" where {sub}')
                k = int(cur.fetchone()[0])
                if k:
                    piores.append((c, k))
            piores.sort(key=lambda x: -x[1])
            det = ", ".join(f"{c} ({k})" for c, k in piores[:6])
            out.append(f"| `{tab}` | {n}/{total} | {det} |")
    if not achou:
        return ["Nenhum marcador de mojibake encontrado. O reprocessamento pegou."]
    return ["| tabela | linhas afetadas | colunas |", "|---|---|---|"] + out


def secao_nulos(cur, schema: str, tabelas: List[str], limiar: float) -> List[str]:
    out = ["| tabela | coluna | % nulo/vazio |", "|---|---|---|"]
    for tab in tabelas:
        total = n_linhas(cur, schema, tab)
        if total < 0:
            out.append(f"| `{tab}` | **(não materializada)** | — |")
            continue
        if total == 0:
            out.append(f"| `{tab}` | (tabela vazia) | — |")
            continue
        for c, t in colunas(cur, schema, tab):
            if c.startswith("_"):
                continue
            vazio = (
                f"""count(*) filter (where "{c}" is null or trim("{c}"::text) = '')"""
                if t in ("text", "character varying")
                else f"""count(*) filter (where "{c}" is null)"""
            )
            cur.execute(f'select {vazio} from "{schema}"."{tab}"')
            pct = 100.0 * int(cur.fetchone()[0]) / total
            if pct >= limiar:
                marca = " **(morta)**" if pct == 100.0 else ""
                out.append(f"| `{tab}` | `{c}` | {pct:.1f}%{marca} |")
    return out if len(out) > 2 else ["Nenhuma coluna acima do limiar."]


def secao_duplicidade(cur, schema: str, chaves: Dict[str, List[str]]) -> List[str]:
    out = ["| model | chave | linhas | chaves distintas | duplicadas |", "|---|---|---|---|---|"]
    for model, ks in chaves.items():
        total = n_linhas(cur, schema, model)
        if total < 0:
            out.append(f"| `{model}` | — | **(não materializada)** | — | — |")
            continue
        cols = ", ".join(f'"{k}"' for k in ks)
        cur.execute(f'select count(*) from (select distinct {cols} from "{schema}"."{model}") d')
        distintas = int(cur.fetchone()[0])
        dup = total - distintas
        marca = " **DUPLICA**" if dup else ""
        out.append(f"| `{model}` | {'+'.join(ks)} | {total} | {distintas} | {dup}{marca} |")
    return out


def secao_cortes(cur, schema: str, cortes: List[Tuple[str, str]]) -> List[str]:
    out = []
    for model, col in cortes:
        try:
            cur.execute(
                f'''select coalesce("{col}"::text, '(null)'), count(*)
                    from "{schema}"."{model}" group by 1 order by 2 desc limit 12'''
            )
        except psycopg2.Error:
            cur.connection.rollback()
            continue
        vals = cur.fetchall()
        out.append(f"\n**`{model}.{col}`** — {len(vals)} valor(es) no top 12:\n")
        out += [f"- `{v}` — {n}" for v, n in vals]
    return out


def secao_joins(cur, schema: str, joins: List[Tuple[str, str, str]]) -> List[str]:
    out = ["| esquerda | direita | chave | esq. | casam | cobertura |", "|---|---|---|---|---|---|"]
    for esq, dir_, k in joins:
        try:
            cur.execute(f'select count(distinct "{k}") from "{schema}"."{esq}"')
            n_esq = int(cur.fetchone()[0])
            cur.execute(
                f'''select count(distinct e."{k}") from "{schema}"."{esq}" e
                    join "{schema}"."{dir_}" d on e."{k}" = d."{k}"'''
            )
            n_ok = int(cur.fetchone()[0])
        except psycopg2.Error:
            cur.connection.rollback()
            out.append(f"| `{esq}` | `{dir_}` | {k} | — | — | (erro) |")
            continue
        pct = 100.0 * n_ok / n_esq if n_esq else 0.0
        marca = " **ZERO**" if n_ok == 0 else (" **baixa**" if pct < 5 else "")
        out.append(f"| `{esq}` | `{dir_}` | {k} | {n_esq} | {n_ok} | {pct:.1f}%{marca} |")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dominio", default="rural", choices=sorted(DOMINIOS))
    ap.add_argument("--schema-bronze", default="bronze")
    ap.add_argument(
        "--limiar-nulo",
        type=float,
        default=50.0,
        help="só reporta coluna com pelo menos este %% de nulo/vazio (default 50).",
    )
    args = ap.parse_args()
    cfg = DOMINIOS[args.dominio]
    sd = cfg["schema_dbt"]

    with conectar() as conn, conn.cursor() as cur:
        p = print
        p(f"# Diagnóstico de qualidade — domínio {args.dominio}\n")

        p("## 1. Mojibake residual (bronze)\n")
        for l in secao_mojibake(cur, args.schema_bronze, cfg["bronze"]):
            p(l)
        p(f"\n## 2. Mojibake residual (silver/gold, schema {sd})\n")
        for l in secao_mojibake(cur, sd, sorted(cfg["chaves"])):
            p(l)

        p(f"\n## 3. Colunas com >= {args.limiar_nulo:.0f}% de nulo/vazio\n")
        for l in secao_nulos(cur, sd, sorted(cfg["chaves"]), args.limiar_nulo):
            p(l)

        p("\n## 4. Duplicidade na chave natural\n")
        for l in secao_duplicidade(cur, sd, cfg["chaves"]):
            p(l)

        p("\n## 5. Cobertura dos joins\n")
        for l in secao_joins(cur, sd, cfg["joins"]):
            p(l)

        p("\n## 6. Valores reais dos campos de corte\n")
        for l in secao_cortes(cur, sd, cfg["cortes"]):
            p(l)


if __name__ == "__main__":
    sys.exit(main())
