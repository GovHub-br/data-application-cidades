#!/usr/bin/env python3
"""Renomeia datasets do Superset pela API, seguindo um mapa (schema, tabela) -> (schema, tabela).

Por que existe: quando um model dbt muda de nome ou de schema, o dataset do Superset
continua apontando para a tabela antiga, que deixou de existir. Todo gráfico e métrica
construídos sobre ele quebram — mas o dataset em si guarda essas definições. Recriar o
dataset perde tudo; renomear em cima preserva.

O que ele NÃO faz: não mexe em gráfico, dashboard, métrica nem coluna calculada. Só troca
o par (schema, table_name) do dataset. Os gráficos seguem apontando para o mesmo dataset
por id, então continuam funcionando desde que as COLUNAS tenham os mesmos nomes.

Uso:
    set -a; source .env; set +a
    python scripts/superset_renomeia_datasets.py                 # dry-run (padrão)
    python scripts/superset_renomeia_datasets.py --apply
    python scripts/superset_renomeia_datasets.py --apply --sync-colunas

Variáveis de ambiente:
    SUPERSET_URL       ex.: http://10.0.0.60:8088
    SUPERSET_USER      usuário com permissão de editar dataset
    SUPERSET_PASSWORD
    SUPERSET_PROVIDER  'db' (padrão) ou 'ldap'
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]


# (schema_antigo, tabela_antiga) -> (schema_novo, tabela_nova)
#
# Rural: adoção do padrão do projeto (far_dbt / fds_dbt) — três schemas por camada
# (bronze/prata/ouro) e a linha no nome a partir da prata.
MAPA: Dict[Tuple[str, str], Tuple[str, str]] = {
    ("empreendimento_rural", "gold_ficha_empreendimento"):
        ("ouro", "ouro_rural_ficha_empreendimento"),
    ("empreendimento_rural", "gold_resumo_gerencial"):
        ("ouro", "ouro_rural_resumo_gerencial"),
    ("empreendimento_rural", "gold_panorama_estadual"):
        ("ouro", "ouro_rural_panorama_estadual"),
    ("empreendimento_rural", "gold_mapa_nacional"):
        ("ouro", "ouro_rural_mapa_nacional"),
    ("empreendimento_rural", "gold_evolucao_financeira"):
        ("ouro", "ouro_rural_evolucao_financeira"),
    ("empreendimento_rural", "gold_execucao_fisica_financeira"):
        ("ouro", "ouro_rural_execucao_fisica_financeira"),
    ("empreendimento_rural", "gold_ficha_trabalho_social"):
        ("ouro", "ouro_rural_ficha_trabalho_social"),
    ("empreendimento_rural", "gold_perfil_beneficiarios"):
        ("ouro", "ouro_rural_perfil_beneficiarios"),
    ("empreendimento_rural", "gold_infraestrutura_agua_saneamento"):
        ("ouro", "ouro_rural_infraestrutura_agua_saneamento"),
}

# Nomes que a rodada anterior pode ter deixado para trás. O dataset pode estar em
# qualquer um dos dois estados dependendo de quando o Superset foi ajustado pela última
# vez, então os dois são procurados e levam ao mesmo destino.
ALIASES_ANTIGOS: Dict[Tuple[str, str], Tuple[str, str]] = {
    ("empreendimento_rural_gold", "ficha_empreendimento_rural"):
        ("ouro", "ouro_rural_ficha_empreendimento"),
    ("empreendimento_rural_gold", "resumo_gerencial_rural"):
        ("ouro", "ouro_rural_resumo_gerencial"),
    ("empreendimento_rural_gold", "panorama_estadual_rural"):
        ("ouro", "ouro_rural_panorama_estadual"),
    ("empreendimento_rural_gold", "mapa_nacional_rural"):
        ("ouro", "ouro_rural_mapa_nacional"),
    ("empreendimento_rural_gold", "evolucao_financeira_rural"):
        ("ouro", "ouro_rural_evolucao_financeira"),
    ("empreendimento_rural_gold", "execucao_fisica_financeira_chart_rural"):
        ("ouro", "ouro_rural_execucao_fisica_financeira"),
    ("empreendimento_rural_gold", "ficha_trabalho_social"):
        ("ouro", "ouro_rural_ficha_trabalho_social"),
    ("empreendimento_rural_gold", "perfil_beneficiarios"):
        ("ouro", "ouro_rural_perfil_beneficiarios"),
    ("empreendimento_rural_gold", "infraestrutura_agua_saneamento"):
        ("ouro", "ouro_rural_infraestrutura_agua_saneamento"),
}


class Superset:
    """Cliente mínimo da API do Superset.

    Uma sessão só, de propósito: o PUT de dataset exige token Bearer, token CSRF e o
    cookie de sessão JUNTOS. Com requests avulsos o cookie se perde e o Superset devolve
    400 "Referrer checking failed" — que não parece erro de autenticação e custa tempo.
    """

    def __init__(self, url: str, user: str, senha: str, provider: str = "db") -> None:
        self.url = url.rstrip("/")
        self.s = requests.Session()

        r = self.s.post(
            f"{self.url}/api/v1/security/login",
            json={
                "username": user,
                "password": senha,
                "provider": provider,
                "refresh": True,
            },
            timeout=30,
        )
        r.raise_for_status()
        self.token = r.json()["access_token"]
        self.s.headers.update({"Authorization": f"Bearer {self.token}"})

        r = self.s.get(f"{self.url}/api/v1/security/csrf_token/", timeout=30)
        r.raise_for_status()
        self.csrf = r.json()["result"]
        self.s.headers.update({"X-CSRFToken": self.csrf, "Referer": self.url})

    def datasets(self) -> List[dict]:
        """Todos os datasets, paginando — a API devolve 100 por vez por padrão."""
        out: List[dict] = []
        pagina = 0
        while True:
            r = self.s.get(
                f"{self.url}/api/v1/dataset/",
                params={"q": f"(page:{pagina},page_size:100)"},
                timeout=60,
            )
            r.raise_for_status()
            lote = r.json()["result"]
            out.extend(lote)
            if len(lote) < 100:
                return out
            pagina += 1

    def renomeia(self, dataset_id: int, schema: str, tabela: str) -> None:
        r = self.s.put(
            f"{self.url}/api/v1/dataset/{dataset_id}",
            params={"override_columns": "false"},
            json={"schema": schema, "table_name": tabela},
            timeout=60,
        )
        if not r.ok:
            raise RuntimeError(f"PUT {dataset_id} -> {r.status_code}: {r.text[:400]}")

    def sincroniza_colunas(self, dataset_id: int) -> None:
        """Relê as colunas da tabela física.

        Só faz sentido quando a tabela nova tem colunas que a antiga não tinha. Se as
        colunas são as mesmas, NÃO rode: a sincronização recria as colunas do dataset e
        pode derrubar as calculadas.
        """
        r = self.s.put(
            f"{self.url}/api/v1/dataset/{dataset_id}/refresh",
            timeout=60,
        )
        if not r.ok:
            raise RuntimeError(f"refresh {dataset_id} -> {r.status_code}: {r.text[:400]}")


def main() -> int:
    load_dotenv()
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="grava; sem isso é só dry-run")
    ap.add_argument(
        "--procurar",
        metavar="TERMO",
        help="não renomeia nada: lista os datasets cujo schema ou nome de tabela contém "
        "o termo (sem diferenciar maiúsculas). Serve para descobrir sob que nome um "
        "dataset está registrado quando o mapa não o encontra.",
    )
    ap.add_argument(
        "--sync-colunas",
        action="store_true",
        help="após renomear, relê as colunas da tabela nova (cuidado: recria as colunas "
        "do dataset). Use só se a tabela nova ganhou colunas.",
    )
    args = ap.parse_args()

    faltando = [v for v in ("SUPERSET_URL", "SUPERSET_USER", "SUPERSET_PASSWORD")
                if not os.environ.get(v)]
    if faltando:
        print(f"Faltam variáveis de ambiente: {', '.join(faltando)}")
        print("Rode antes:  set -a; source .env; set +a")
        return 2

    sup = Superset(
        os.environ["SUPERSET_URL"],
        os.environ["SUPERSET_USER"],
        os.environ["SUPERSET_PASSWORD"],
        os.environ.get("SUPERSET_PROVIDER", "db"),
    )

    existentes = sup.datasets()

    if args.procurar:
        termo = args.procurar.lower()
        achados = [
            d for d in existentes
            if termo in (d.get("schema") or "").lower()
            or termo in d["table_name"].lower()
        ]
        print(f"{len(achados)} de {len(existentes)} datasets casam com {args.procurar!r}:\n")
        for d in sorted(achados, key=lambda x: ((x.get("schema") or ""), x["table_name"])):
            print(f"  [{d['id']:>4}] {d.get('schema')}.{d['table_name']}")
        return 0

    alvo = {**MAPA, **ALIASES_ANTIGOS}

    # LISTA por chave, não um dataset só. A mesma tabela pode ter mais de um dataset
    # registrado — acontece quando alguém cria um novo em vez de editar o existente, e os
    # dois continuam válidos porque os gráficos se ligam por id. Indexar num dict simples
    # fazia o segundo sobrescrever o primeiro e deixava o outro apontando para uma tabela
    # que vai deixar de existir, sem aviso no dry-run.
    por_chave: Dict[Tuple[str, str], List[dict]] = {}
    for d in existentes:
        por_chave.setdefault((d.get("schema") or "", d["table_name"]), []).append(d)

    print(f"{'APPLY' if args.apply else 'DRY-RUN'} · {sup.url} · "
          f"{len(existentes)} datasets no Superset\n")

    achados: List[Tuple[dict, Tuple[str, str]]] = []
    ausentes: List[Tuple[str, str]] = []
    ja_no_destino: List[Tuple[str, str]] = []

    for (schema_v, tab_v), (schema_n, tab_n) in alvo.items():
        if (schema_n, tab_n) in por_chave and (schema_v, tab_v) not in por_chave:
            ja_no_destino.append((schema_n, tab_n))
            continue
        lote = por_chave.get((schema_v, tab_v))
        if not lote:
            ausentes.append((schema_v, tab_v))
            continue
        for d in lote:
            achados.append((d, (schema_n, tab_n)))

    duplicadas = {
        chave: [d["id"] for d in lote]
        for chave, lote in por_chave.items()
        if len(lote) > 1 and chave in alvo
    }

    for d, (schema_n, tab_n) in achados:
        print(f"  [{d['id']:>4}] {d.get('schema')}.{d['table_name']}")
        print(f"         -> {schema_n}.{tab_n}")

    if duplicadas:
        print("\n  ATENÇÃO — tabela com mais de um dataset. Todos são renomeados, porque")
        print("  não há como saber daqui qual deles os gráficos usam:")
        for (sch, tab), ids in sorted(duplicadas.items()):
            print(f"    {sch}.{tab} -> ids {', '.join(str(i) for i in ids)}")

    if ja_no_destino:
        print(f"\n  já no destino ({len(set(ja_no_destino))}): "
              + ", ".join(f"{s}.{t}" for s, t in sorted(set(ja_no_destino))))
    if ausentes:
        print(f"\n  não encontrados ({len(ausentes)}) — provavelmente já renomeados "
              f"numa rodada anterior, ou nunca existiram:")
        for s, t in ausentes:
            print(f"    {s}.{t}")

    if not achados:
        print("\nNada a fazer.")
        return 0

    if not args.apply:
        print(f"\n{len(achados)} dataset(s) seriam renomeados "
              f"({len(set(k for _, k in achados))} tabelas). Rode com --apply.")
        return 0

    erros = 0
    print()
    for d, (schema_n, tab_n) in achados:
        try:
            sup.renomeia(d["id"], schema_n, tab_n)
            if args.sync_colunas:
                sup.sincroniza_colunas(d["id"])
            print(f"  ok   [{d['id']:>4}] -> {schema_n}.{tab_n}")
        except Exception as e:  # noqa: BLE001 — queremos seguir e reportar no fim
            erros += 1
            print(f"  ERRO [{d['id']:>4}] -> {schema_n}.{tab_n}: {e}")

    print(f"\n{len(achados) - erros} renomeado(s), {erros} com erro.")
    if erros == 0:
        print("Os gráficos seguem ligados aos mesmos datasets por id — nada a refazer, "
              "desde que os nomes de coluna não tenham mudado.")
    return 1 if erros else 0


if __name__ == "__main__":
    sys.exit(main())