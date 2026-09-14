# scripts/diagnostico_qualidade.py

"""Perfil de qualidade das camadas bronze/silver/gold de um domínio.

Responde as perguntas que um teste do dbt só consegue responder DEPOIS que alguém
decidiu o que asseverar:

  - sobrou mojibake em algum valor?
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

# O relatório sai por stdout e é redirecionado para um .md. No Windows o stdout usa
# cp1252 por padrão, o que substitui todo acento por "?" no arquivo — justamente num
# relatório sobre encoding.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

load_dotenv()

# Marcadores do round-trip utf-8 -> latin-1, mais o U+FFFD que o errors="replace" deixa.
MOJIBAKE = ["Ã", "Â", "â€", "�"]

DOMINIOS: Dict[str, dict] = {
    "rural": {
        "bronze": [
            "bronze_shpt_monit_cad_pj_rural_mensal",
            "bronze_shpt_monit_cadastro_pf_rural_mensal",
            "bronze_shpt_monit_mov_obra_rural_mensal",
            "bronze_shpt_monit_mov_financ_rural_mensal",
            "bronze_shpt_dados_prioritarios_disponibilizados_snh_empreendimentos",
            "bronze_sftp_snh_pmcmv_dados_prioritarios_af_caixa",
            "bronze_sftp_snh_pmcmv_dados_prioritarios_af_bb",
            "bronze_sftp_int065_pnhr_caixa_empreendimentos",
            "bronze_sftp_int057_pnhr_bb_empreendimentos",
            "bronze_sftp_int055_liberacoes_caixa_bb",
            "bronze_shpt_base_trabalho_social_pnhr_rural_caixa",
            "bronze_shpt_base_trabalho_social_pnhr_bb",
        ],
        # Um schema só para o domínio; a camada está no prefixo do nome da tabela.
        # Padrão do projeto: três schemas por camada, compartilhados entre as
        # linhas (far, fds, rural). A linha está no nome da tabela.
        "schema_bronze": "bronze",
        "schema_silver": "prata",
        "schema_gold": "ouro",
        # Todos os models de cada camada. Separado das chaves de propósito: nem todo
        # model tem chave natural declarável (prata_rural_financeiro_mensal são várias
        # liberações por APF), mas todos entram no perfil de nulos e de mojibake.
        "models_silver": [
            "prata_rural_empreendimento", "prata_rural_prioritarios_snh", "prata_rural_prioritarios_caixa",
            "prata_rural_prioritarios_bb", "prata_rural_cadastro_pj", "prata_rural_cadastro_pf",
            "prata_rural_pnhr_caixa", "prata_rural_pnhr_bb", "prata_rural_pnhr_liberacoes",
            "prata_rural_trabalho_social_caixa", "prata_rural_trabalho_social_bb",
            "prata_rural_obra_mensal", "prata_rural_financeiro_mensal",
        ],
        "models_gold": [
            "ouro_rural_ficha_empreendimento", "ouro_rural_resumo_gerencial",
            "ouro_rural_panorama_estadual", "ouro_rural_mapa_nacional",
            "ouro_rural_evolucao_financeira", "ouro_rural_execucao_fisica_financeira",
            "ouro_rural_ficha_trabalho_social", "ouro_rural_perfil_beneficiarios",
            "ouro_rural_infraestrutura_agua_saneamento",
        ],
        # model -> chave natural esperada, por camada (o schema vem de qual bloco)
        "chaves_silver": {
            "prata_rural_empreendimento": ["apf"],
            "prata_rural_prioritarios_snh": ["apf"],
            "prata_rural_prioritarios_caixa": ["apf"],
            "prata_rural_prioritarios_bb": ["apf"],
            "prata_rural_cadastro_pj": ["apf"],
            "prata_rural_pnhr_caixa": ["apf"],
            "prata_rural_pnhr_bb": ["apf"],
            "prata_rural_trabalho_social_caixa": ["apf"],
            "prata_rural_trabalho_social_bb": ["apf"],
            "prata_rural_obra_mensal": ["apf"],
        },
        "chaves_gold": {
            "ouro_rural_ficha_empreendimento": ["apf"],
            "ouro_rural_evolucao_financeira": ["apf", "mes"],
            "ouro_rural_ficha_trabalho_social": ["apf", "agente_financeiro"],
            "ouro_rural_perfil_beneficiarios": ["apf"],
            "ouro_rural_infraestrutura_agua_saneamento": ["apf"],
        },
        # campos de corte: os valores reais importam porque viram condição de join
        "cortes": [
            ("prata_rural_prioritarios_snh", "agente_financeiro"),
            ("prata_rural_prioritarios_snh", "modalidade"),
            ("prata_rural_prioritarios_snh", "situacao"),
            ("prata_rural_empreendimento", "agente_financeiro"),
            ("ouro_rural_ficha_empreendimento", "programa"),
            ("ouro_rural_ficha_empreendimento", "status_execucao_simplificado"),
            ("ouro_rural_ficha_empreendimento", "status_prazo"),
        ],
        # cobertura: (esquerda, direita, coluna de junção) — quantos da esquerda acham par
        "joins": [
            ("prata_rural_prioritarios_snh", "prata_rural_prioritarios_caixa", "apf"),
            ("prata_rural_prioritarios_snh", "prata_rural_prioritarios_bb", "apf"),
            ("prata_rural_prioritarios_snh", "prata_rural_cadastro_pj", "apf"),
            ("prata_rural_prioritarios_snh", "prata_rural_pnhr_caixa", "apf"),
            ("prata_rural_prioritarios_snh", "prata_rural_pnhr_bb", "apf"),
            ("prata_rural_empreendimento", "prata_rural_cadastro_pf", "apf"),
            ("prata_rural_empreendimento", "prata_rural_trabalho_social_caixa", "apf"),
            ("prata_rural_empreendimento", "prata_rural_obra_mensal", "apf"),
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


def secao_duplicidade(cur, schema_de: Dict[str, str], chaves: Dict[str, List[str]]) -> List[str]:
    out = ["| model | chave | linhas | chaves distintas | duplicadas |", "|---|---|---|---|---|"]
    for model, ks in chaves.items():
        schema = schema_de[model]
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


def secao_cortes(cur, schema_de: Dict[str, str], cortes: List[Tuple[str, str]]) -> List[str]:
    out = []
    for model, col in cortes:
        schema = schema_de[model]
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



# ---------------------------------------------------------------------------------------
# Procedência e divergência entre fontes
# ---------------------------------------------------------------------------------------
# Mede quantos empreendimentos têm fontes que discordam da mesma grandeza, de quanto, e
# qual fonte acabou valendo na consolidação.
# Mede quantos empreendimentos têm fontes que discordam da mesma grandeza, de quanto, e
# qual fonte acabou valendo na consolidação.

# medida -> [(tabela, coluna de valor, coluna de data, rótulo)]
FONTES_POR_MEDIDA: Dict[str, List[Tuple[str, str, str, str]]] = {
    "percentual_execucao_fisica": [
        ("prata_rural_prioritarios_snh", "percentual_execucao_fisica", "dt_referencia", "snh"),
        ("prata_rural_prioritarios_caixa", "percentual_execucao_fisica", "dt_movimento", "caixa"),
        ("prata_rural_prioritarios_bb", "percentual_execucao_fisica", "dt_movimento", "bb"),
        ("prata_rural_obra_mensal", "percentual_obra_realizada", "dt_movimento", "obra_mensal"),
    ],
    "valor_desembolsado": [
        ("prata_rural_prioritarios_snh", "valor_desembolsado", "dt_referencia", "snh"),
        ("prata_rural_prioritarios_caixa", "valor_desembolsado", "dt_movimento", "caixa"),
        ("prata_rural_prioritarios_bb", "valor_desembolsado", "dt_movimento", "bb"),
    ],
}

# medida -> coluna de procedência correspondente na prata_rural_empreendimento
COLUNA_FONTE: Dict[str, str] = {
    "percentual_execucao_fisica": "fonte_execucao_fisica",
    "valor_desembolsado": "fonte_valor_desembolsado",
}

# Colunas de procedência expostas pela prata_rural_empreendimento.
PROCEDENCIA = [
    ("prata_rural_empreendimento", "fonte_execucao_fisica"),
    ("prata_rural_empreendimento", "fonte_valor_desembolsado"),
    ("prata_rural_empreendimento", "fonte_valor_contratado"),
    ("prata_rural_empreendimento", "fonte_situacao"),
]


def secao_divergencia(cur, schema: str, tolerancia: float) -> List[str]:
    """Quantos APFs têm fontes que discordam da mesma medida, e de quanto."""
    out = [
        "| Medida | APFs com >1 fonte | Discordam | Discordância máx. | Fonte que venceu (top 3) |",
        "|---|---|---|---|---|",
    ]
    for medida, fontes in FONTES_POR_MEDIDA.items():
        partes = []
        for tab, col, dt, rot in fontes:
            if n_linhas(cur, schema, tab) < 0:
                continue
            partes.append(
                f'select apf, {col}::numeric as v, \'{rot}\' as fonte '
                f'from "{schema}"."{tab}" where {col} is not null'
            )
        if len(partes) < 2:
            out.append(f"| `{medida}` | (fontes não materializadas) | — | — | — |")
            continue
        uniao = " union all ".join(partes)
        try:
            cur.execute(
                f"""
                with cand as ({uniao}),
                agg as (
                    select apf, count(distinct fonte) as n_fontes,
                           max(v) - min(v) as amplitude
                    from cand group by apf
                )
                select
                    count(*) filter (where n_fontes > 1),
                    count(*) filter (where n_fontes > 1 and amplitude > %s),
                    max(amplitude) filter (where n_fontes > 1)
                from agg
                """,
                (tolerancia,),
            )
            com_varias, discordam, amp = cur.fetchone()
        except psycopg2.Error as e:
            cur.connection.rollback()
            out.append(f"| `{medida}` | erro: {str(e).strip().splitlines()[0]} | — | — | — |")
            continue

        vencedor = "—"
        col_fonte = COLUNA_FONTE[medida]
        try:
            cur.execute(
                f'select {col_fonte}, count(*) from "{schema}"."prata_rural_empreendimento" '
                f"where {col_fonte} is not null group by 1 order by 2 desc limit 3"
            )
            vencedor = ", ".join(f"{r[0]} ({r[1]})" for r in cur.fetchall()) or "—"
        except psycopg2.Error:
            cur.connection.rollback()

        out.append(
            f"| `{medida}` | {com_varias or 0} | **{discordam or 0}** | "
            f"{('%.2f' % amp) if amp is not None else '—'} | {vencedor} |"
        )
    out.append("")
    out.append(
        f"Discordância = diferença entre o maior e o menor valor das fontes para o mesmo "
        f"APF, acima da tolerância de {tolerancia:g}. Não é erro nosso: é a origem "
        f"divergindo. O que a consolidação faz é escolher pela data de medição em vez de "
        f"por ordem de fonte fixa — e registrar qual escolheu."
    )
    return out


def secao_atualidade(cur, schema: str) -> List[str]:
    """Idade das medições consolidadas: um número velho publicado sem aviso é um número falso."""
    out = []
    if n_linhas(cur, schema, "prata_rural_empreendimento") < 0:
        return ["`prata_rural_empreendimento` não materializada."]
    try:
        cur.execute(
            f"""
            select
                count(*),
                count(dt_referencia_consolidada),
                min(dt_referencia_consolidada),
                max(dt_referencia_consolidada),
                count(*) filter (where current_date - dt_referencia_consolidada > 180),
                count(*) filter (where current_date - dt_referencia_consolidada > 365)
            from "{schema}"."prata_rural_empreendimento"
            """
        )
        total, com_data, mais_antiga, mais_nova, m6, m12 = cur.fetchone()
        out += [
            "| Métrica | Valor |",
            "|---|---|",
            f"| Empreendimentos | {total} |",
            f"| Com data de medição | {com_data} |",
            f"| Medição mais antiga | {mais_antiga} |",
            f"| Medição mais recente | {mais_nova} |",
            f"| Com mais de 6 meses | {m6} |",
            f"| Com mais de 12 meses | {m12} |",
            "",
        ]
    except psycopg2.Error as e:
        cur.connection.rollback()
        return [f"erro: {str(e).strip().splitlines()[0]}"]

    out.append("Distribuição da fonte escolhida, por medida:")
    out.append("")
    out.append("| Coluna | Fonte | APFs |")
    out.append("|---|---|---|")
    for tab, col in PROCEDENCIA:
        try:
            cur.execute(
                f'select {col}, count(*) from "{schema}"."{tab}" group by 1 order by 2 desc'
            )
            for fonte, n in cur.fetchall():
                out.append(f"| `{col}` | {fonte or '(nulo)'} | {n} |")
        except psycopg2.Error:
            cur.connection.rollback()
            out.append(f"| `{col}` | (coluna ausente — rode o dbt build) | — |")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dominio", default="rural", choices=sorted(DOMINIOS))
    ap.add_argument(
        "--schema-bronze",
        default=None,
        help="sobrepõe o schema_bronze do domínio (default: o do DOMINIOS)",
    )
    ap.add_argument(
        "--limiar-nulo",
        type=float,
        default=50.0,
        help="só reporta coluna com pelo menos este %% de nulo/vazio (default 50).",
    )
    ap.add_argument(
        "--tolerancia-divergencia",
        type=float,
        default=1.0,
        help="diferença entre fontes a partir da qual conta como discordância (default 1).",
    )
    args = ap.parse_args()
    cfg = DOMINIOS[args.dominio]
    silver, gold = cfg["schema_silver"], cfg["schema_gold"]
    chaves = {**cfg["chaves_silver"], **cfg["chaves_gold"]}
    schema_de = {m: silver for m in cfg["models_silver"]}
    schema_de.update({m: gold for m in cfg["models_gold"]})

    with conectar() as conn, conn.cursor() as cur:
        p = print
        p(f"# Diagnóstico de qualidade — domínio {args.dominio}\n")

        p("## 1. Mojibake residual (bronze)\n")
        for l in secao_mojibake(cur, args.schema_bronze or cfg["schema_bronze"], cfg["bronze"]):
            p(l)
        p(f"\n## 2a. Mojibake residual — silver ({silver})\n")
        for l in secao_mojibake(cur, silver, sorted(cfg["models_silver"])):
            p(l)
        p(f"\n## 2b. Mojibake residual — gold ({gold})\n")
        for l in secao_mojibake(cur, gold, sorted(cfg["models_gold"])):
            p(l)

        p(f"\n## 3. Colunas com >= {args.limiar_nulo:.0f}% de nulo/vazio\n")
        for l in secao_nulos(cur, silver, sorted(cfg["models_silver"]), args.limiar_nulo):
            p(l)
        for l in secao_nulos(cur, gold, sorted(cfg["models_gold"]), args.limiar_nulo)[2:]:
            p(l)

        p("\n## 4. Duplicidade na chave natural\n")
        for l in secao_duplicidade(cur, schema_de, chaves):
            p(l)

        p("\n## 5. Cobertura dos joins\n")
        for l in secao_joins(cur, silver, cfg["joins"]):
            p(l)

        p("\n## 6. Valores reais dos campos de corte\n")
        for l in secao_cortes(cur, schema_de, cfg["cortes"]):
            p(l)

        p("\n## 7. Divergência entre fontes da mesma medida\n")
        for l in secao_divergencia(cur, silver, args.tolerancia_divergencia):
            p(l)

        p("\n## 8. Atualidade e procedência das medições\n")
        for l in secao_atualidade(cur, silver):
            p(l)


if __name__ == "__main__":
    sys.exit(main())
