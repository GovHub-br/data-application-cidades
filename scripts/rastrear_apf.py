#!/usr/bin/env python3
"""Rastreia um APF por todas as camadas do domínio, para conciliar números divergentes.

Quando duas gold mostram valores diferentes para o mesmo empreendimento, a pergunta não é
"qual está certo" — é "de onde cada uma tirou o número". Este script responde isso: para
cada tabela de bronze, silver e gold, imprime as linhas daquele APF com as colunas não
nulas, e no fim compara lado a lado os indicadores que aparecem em mais de um lugar.

Não usa lista de colunas fixa: descobre pelo information_schema e faz `select *`. Assim não
quebra quando a origem muda de layout — que é justamente quando ele é mais necessário.

Uso:
    set -a; source .env; set +a
    python scripts/rastrear_apf.py --apf 63665048
    python scripts/rastrear_apf.py --apf 63665048 --so-divergencias
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List, Optional, Tuple

import psycopg2
from dotenv import load_dotenv

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]

SCHEMA = "empreendimento_rural"

# Ordem de leitura: a bronze primeiro, porque é o que a origem realmente mandou.
CAMADAS: Dict[str, List[str]] = {
    "bronze": [
        "bronze_prioritarios_snh",
        "bronze_prioritarios_caixa",
        "bronze_prioritarios_bb",
        "bronze_cadastro_pj",
        "bronze_obra_mensal",
        "bronze_financeiro_mensal",
        "bronze_pnhr_caixa",
        "bronze_pnhr_bb",
        "bronze_pnhr_liberacoes",
        "bronze_trabalho_social_caixa",
        "bronze_trabalho_social_bb",
    ],
    "silver": [
        "silver_prioritarios_snh",
        "silver_prioritarios_caixa",
        "silver_prioritarios_bb",
        "silver_cadastro_pj",
        "silver_cadastro_pf",
        "silver_obra_mensal",
        "silver_financeiro_mensal",
        "silver_pnhr_caixa",
        "silver_pnhr_bb",
        "silver_pnhr_liberacoes",
        "silver_trabalho_social_caixa",
        "silver_trabalho_social_bb",
        "silver_empreendimento",
    ],
    "gold": [
        "gold_ficha_empreendimento",
        "gold_evolucao_financeira",
        "gold_execucao_fisica_financeira",
        "gold_ficha_trabalho_social",
        "gold_perfil_beneficiarios",
        "gold_infraestrutura_agua_saneamento",
    ],
}

# Colunas de APF por tabela da bronze: lá o nome ainda é o da origem, e varia.
# A ordem importa: a primeira que existir na tabela é usada.
APF_BRONZE = [
    "nu_apf",
    "codigo_da_operacao_no_agente_financeiro",
    "co_operacao_agente_financeiro",
    "nu_contrato",
    "numero_do_contrato",
    "apf",
]

# Indicadores que aparecem em mais de uma tabela e por isso podem divergir.
# (rótulo, [(tabela, coluna), ...])
CONCILIAR: List[Tuple[str, List[Tuple[str, str]]]] = [
    (
        "execução física (%)",
        [
            ("silver_prioritarios_snh", "percentual_execucao_fisica"),
            ("silver_prioritarios_caixa", "percentual_execucao_fisica"),
            ("silver_prioritarios_bb", "percentual_execucao_fisica"),
            ("silver_cadastro_pj", "percentual_obra_realizada"),
            ("silver_obra_mensal", "percentual_obra_realizada"),
            ("silver_obra_mensal", "percentual_obra_prevista"),
            ("silver_empreendimento", "percentual_execucao_fisica"),
            ("gold_ficha_empreendimento", "percentual_execucao_fisica"),
            ("gold_execucao_fisica_financeira", "pct_obra_realizada"),
        ],
    ),
    (
        "valor contratado (R$)",
        [
            ("silver_prioritarios_snh", "valor_contratado"),
            ("silver_cadastro_pj", "vr_investimento_total"),
            ("silver_empreendimento", "valor_contratado"),
            ("gold_ficha_empreendimento", "valor_contratado"),
            ("gold_evolucao_financeira", "valor_contratado"),
        ],
    ),
    (
        "valor desembolsado (R$)",
        [
            ("silver_prioritarios_snh", "valor_desembolsado"),
            ("silver_cadastro_pj", "vr_liberado"),
            ("silver_empreendimento", "valor_desembolsado"),
            ("gold_ficha_empreendimento", "valor_desembolsado"),
            ("gold_evolucao_financeira", "vr_acumulado"),
        ],
    ),
    (
        "execução financeira (%)",
        [
            ("gold_ficha_empreendimento", "percentual_execucao_financeira"),
            ("gold_evolucao_financeira", "pct_executado_financeiro"),
            ("gold_execucao_fisica_financeira", "pct_executado_financeiro"),
        ],
    ),
    (
        "UH contratadas",
        [
            ("silver_prioritarios_snh", "uh_contratadas"),
            ("silver_cadastro_pj", "qt_uh_contratadas"),
            ("silver_empreendimento", "quantidade_uh_contratadas"),
            ("gold_ficha_empreendimento", "quantidade_uh_contratadas"),
        ],
    ),
    (
        "situação",
        [
            ("silver_prioritarios_snh", "situacao"),
            ("silver_empreendimento", "situacao_empreendimento"),
            ("gold_ficha_empreendimento", "situacao_empreendimento"),
            ("gold_ficha_empreendimento", "status_execucao_simplificado"),
        ],
    ),
]


def conectar():  # type: ignore[no-untyped-def]
    return psycopg2.connect(
        host=os.environ["DB_DW_HOST_MCID"],
        port=int(os.environ.get("DB_DW_PORT_MCID", 5432)),
        user=os.environ["DB_DW_USER_MCID"],
        password=os.environ["DB_DW_PASSWORD_MCID"],
        dbname=os.environ["DB_DW_DBNAME_MCID"],
    )


def colunas(cur, tabela: str) -> List[str]:
    cur.execute(
        """select column_name from information_schema.columns
           where table_schema = %s and table_name = %s order by ordinal_position""",
        (SCHEMA, tabela),
    )
    return [r[0] for r in cur.fetchall()]


def coluna_apf(cols: List[str]) -> Optional[str]:
    for c in APF_BRONZE:
        if c in cols:
            return c
    return None


def linhas_do_apf(cur, tabela: str, apf: str) -> Tuple[Optional[List[str]], List[tuple]]:
    """(colunas, linhas) do APF, ou (None, []) se a tabela não existe / não tem APF."""
    cols = colunas(cur, tabela)
    if not cols:
        return None, []
    col = coluna_apf(cols)
    if col is None:
        return cols, []
    # A bronze é toda text e o APF pode vir com zeros à esquerda ou pontuação; comparar
    # só os dígitos evita falso negativo, que aqui seria pior que uma linha a mais.
    try:
        cur.execute(
            f'select * from "{SCHEMA}"."{tabela}" '
            f"where regexp_replace(coalesce({col}::text, ''), '[^0-9]', '', 'g') "
            f"= regexp_replace(%s, '[^0-9]', '', 'g')",
            (apf,),
        )
        return cols, cur.fetchall()
    except psycopg2.Error as e:
        cur.connection.rollback()
        print(f"  ! {tabela}: {str(e).strip().splitlines()[0]}")
        return cols, []


def fmt(v: object) -> str:
    if v is None:
        return "—"
    s = str(v)
    return s if len(s) <= 120 else s[:117] + "..."


def despejar(cur, apf: str) -> Dict[Tuple[str, str], List[object]]:
    """Imprime as linhas por camada e devolve {(tabela, coluna): [valores]}."""
    coletado: Dict[Tuple[str, str], List[object]] = {}
    for camada, tabelas in CAMADAS.items():
        print(f"\n{'=' * 78}\n{camada.upper()}\n{'=' * 78}")
        for tabela in tabelas:
            cols, linhas = linhas_do_apf(cur, tabela, apf)
            if cols is None:
                print(f"\n### {tabela} — (não existe no schema {SCHEMA})")
                continue
            print(f"\n### {tabela} — {len(linhas)} linha(s)")
            if not linhas:
                continue
            for i, linha in enumerate(linhas, 1):
                if len(linhas) > 1:
                    print(f"  -- linha {i}/{len(linhas)}")
                for c, v in zip(cols, linha):
                    coletado.setdefault((tabela, c), []).append(v)
                    if v is not None and str(v).strip() != "":
                        print(f"  {c:<52} {fmt(v)}")
    return coletado


def conciliar(coletado: Dict[Tuple[str, str], List[object]]) -> None:
    print(f"\n{'=' * 78}\nCONCILIAÇÃO — o mesmo indicador, em cada lugar onde ele aparece\n{'=' * 78}")
    for rotulo, origens in CONCILIAR:
        print(f"\n{rotulo}")
        vistos = []
        for tabela, col in origens:
            if (tabela, col) not in coletado:
                continue
            vals = coletado[(tabela, col)]
            distintos = sorted({str(v) for v in vals if v is not None})
            if not distintos:
                print(f"  {tabela}.{col:<34} —  (nulo)")
                continue
            mostra = distintos[0] if len(distintos) == 1 else f"{len(distintos)} valores: {', '.join(distintos[:6])}"
            print(f"  {tabela}.{col:<34} {mostra}")
            vistos.extend(distintos)
        if len({v for v in vistos}) > 1:
            print("  >>> DIVERGE")


def main() -> None:
    global SCHEMA
    load_dotenv()
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apf", required=True, help="APF do empreendimento (só os dígitos)")
    ap.add_argument("--schema", default=SCHEMA)
    ap.add_argument(
        "--so-divergencias",
        action="store_true",
        help="pula o despejo por tabela e imprime apenas a conciliação",
    )
    args = ap.parse_args()

    SCHEMA = args.schema

    with conectar() as conn:
        with conn.cursor() as cur:
            if args.so_divergencias:
                import io
                real = sys.stdout
                sys.stdout = io.StringIO()
                coletado = despejar(cur, args.apf)
                sys.stdout = real
            else:
                print(f"APF {args.apf} — schema {SCHEMA}")
                coletado = despejar(cur, args.apf)
            conciliar(coletado)


if __name__ == "__main__":
    main()
