# scripts/superset_reaponta_dbt.py

"""Reaponta datasets do Superset do dbt antigo para o novo, via API.

O FAR e o FDS saíram de um schema por domínio (`empreendimento_far`,
`empreendimentos_fds`) com a camada no prefixo da tabela (`silver_`, `gold_`) para três
schemas do projeto (`prata`, `ouro`) com a linha no nome (`prata_far_`, `ouro_fds_`).
Mudam schema E nome da tabela — por isso este script não é o
`superset_reaponta_schema.py`, que só trocava o schema.

Um chart não some quando a tabela por trás dele deixa de existir: ele continua na tela e
só para de carregar. Como o dataset é reaproveitado (mesmo id, campos trocados), os charts
e dashboards seguem apontando para ele e nada precisa ser recriado.

As colunas não mudaram entre o dbt antigo e o novo, então não há metadado a reconstruir.

Credenciais (mesmas do login da UI, provider `db`):

    SUPERSET_URL / SUPERSET_USER / SUPERSET_PASSWORD

Uso:

    python scripts/superset_reaponta_dbt.py            # dry-run
    python scripts/superset_reaponta_dbt.py --apply
"""

import argparse
import logging
import os
import sys
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s %(message)s", stream=sys.stderr
)
log = logging.getLogger(__name__)

# (schema_antigo, tabela_antiga) -> (schema_novo, tabela_nova).
# Escrito à mão, um por linha: é o contrato da migração e tem que ser conferível de olho.
MAPA: List[Tuple[Tuple[str, str], Tuple[str, str]]] = [
    (("empreendimento_far", f"silver_{n}"), ("prata", f"prata_far_{n}"))
    for n in (
        "cadastro_pj",
        "consolidado",
        "dados_prioritarios_caixa",
        "empreendimento",
        "evolucao_financeira",
        "financeiro_mensal",
        "obra_mensal",
    )
] + [
    (("empreendimento_far", f"gold_{n}"), ("ouro", f"ouro_far_{n}"))
    for n in (
        "evolucao_financeira_chart",
        "execucao_fisica_financeira_chart",
        "ficha_empreendimento",
        "mapa_nacional",
        "panorama_estadual",
        "resumo_gerencial",
    )
] + [
    (("empreendimentos_fds", f"silver_{n}"), ("prata", f"prata_fds_{n}"))
    for n in (
        "cadastro_pj",
        "dados_prioritarios_entregas",
        "empreendimento",
        "evolucao_financeira",
        "financeiro_mensal",
        "int_059_caixa_pj",
        "obra_mensal",
        "trabalho_social",
    )
] + [
    (("empreendimentos_fds", f"gold_{n}"), ("ouro", f"ouro_fds_{n}"))
    for n in (
        "evolucao_financeira_chart",
        "ficha_empreendimento",
        "panorama_entidade",
    )
]

SCHEMAS_ANTIGOS = ("empreendimento_far", "empreendimentos_fds")


class Superset:
    """Sessão autenticada na API do Superset.

    Um PUT exige três coisas juntas: o Bearer do /security/login, o token do
    /security/csrf_token/ E o cookie de sessão que veio com ele. Faltando qualquer uma a
    resposta é 400 "CSRF token missing" — por isso tudo passa por um requests.Session,
    que carrega o cookie sozinho.
    """

    def __init__(self, url: str, usuario: str, senha: str) -> None:
        self.url = url.rstrip("/")
        self.s = requests.Session()
        r = self.s.post(
            f"{self.url}/api/v1/security/login",
            json={
                "username": usuario,
                "password": senha,
                "provider": "db",
                "refresh": True,
            },
            timeout=30,
        )
        r.raise_for_status()
        self.s.headers.update({"Authorization": f"Bearer {r.json()['access_token']}"})
        r = self.s.get(f"{self.url}/api/v1/security/csrf_token/", timeout=30)
        r.raise_for_status()
        self.s.headers.update({"X-CSRFToken": r.json()["result"], "Referer": self.url})
        log.info("Autenticado em %s", self.url)

    def buscar(self, tabela: str, schema: str) -> Optional[Dict]:
        """Dataset por (table_name, schema), ou None.

        O par importa: `gold_ficha_empreendimento` existe no schema do FAR e no do FDS.
        """
        q = (
            "(filters:!("
            f"(col:table_name,opr:eq,value:'{tabela}'),"
            f"(col:schema,opr:eq,value:'{schema}')"
            "))"
        )
        r = self.s.get(f"{self.url}/api/v1/dataset/", params={"q": q}, timeout=30)
        r.raise_for_status()
        res = r.json().get("result", [])
        if len(res) > 1:
            raise RuntimeError(f"{schema}.{tabela}: {len(res)} datasets, esperava 1")
        return res[0] if res else None

    def reapontar(self, pk: int, schema: str, tabela: str) -> None:
        r = self.s.put(
            f"{self.url}/api/v1/dataset/{pk}",
            json={"schema": schema, "table_name": tabela},
            timeout=60,
        )
        if not r.ok:
            raise RuntimeError(f"PUT /dataset/{pk} -> {r.status_code}: {r.text[:400]}")

    def restantes(self) -> List[Dict]:
        """Datasets que ainda vivem nos schemas antigos — o que o mapa não cobriu."""
        sobra = []
        for schema in SCHEMAS_ANTIGOS:
            q = f"(filters:!((col:schema,opr:eq,value:'{schema}')),page_size:100)"
            r = self.s.get(f"{self.url}/api/v1/dataset/", params={"q": q}, timeout=30)
            r.raise_for_status()
            sobra += r.json().get("result", [])
        return sobra


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--apply", action="store_true", help="grava; sem a flag, roda em dry-run"
    )
    args = ap.parse_args()

    load_dotenv()
    faltando = [
        v
        for v in ("SUPERSET_URL", "SUPERSET_USER", "SUPERSET_PASSWORD")
        if not os.environ.get(v)
    ]
    if faltando:
        log.error("Faltam variáveis de ambiente: %s", ", ".join(faltando))
        return 2

    sup = Superset(
        os.environ["SUPERSET_URL"],
        os.environ["SUPERSET_USER"],
        os.environ["SUPERSET_PASSWORD"],
    )
    log.info(
        "%s | %d dataset(s) no mapa", "APPLY" if args.apply else "DRY-RUN", len(MAPA)
    )

    contagem = {"reapontado": 0, "dry_run": 0, "ja_no_destino": 0, "ausente": 0,
                "erro": 0}
    for (sc_de, tb_de), (sc_para, tb_para) in MAPA:
        origem = f"{sc_de}.{tb_de}"
        try:
            ds = sup.buscar(tb_de, sc_de)
            if ds is None:
                # idempotência: numa segunda execução já está no destino
                if sup.buscar(tb_para, sc_para):
                    log.info("· %-52s já em %s.%s", origem, sc_para, tb_para)
                    contagem["ja_no_destino"] += 1
                else:
                    log.warning("· %-52s sem dataset no Superset", origem)
                    contagem["ausente"] += 1
                continue
            if not args.apply:
                log.info("· %-52s id=%-5s -> %s.%s", origem, ds["id"], sc_para, tb_para)
                contagem["dry_run"] += 1
                continue
            sup.reapontar(ds["id"], sc_para, tb_para)
            log.info("✓ %-52s id=%-5s -> %s.%s", origem, ds["id"], sc_para, tb_para)
            contagem["reapontado"] += 1
        except Exception as e:  # noqa: BLE001 — o motivo do Superset vai no log
            log.error("✗ %-52s %s", origem, e)
            contagem["erro"] += 1

    for ds in sup.restantes():
        log.warning(
            "sobrou em schema antigo: %s.%s (id=%s, %s)",
            ds.get("schema"),
            ds.get("table_name"),
            ds.get("id"),
            ds.get("kind"),
        )

    log.info("Concluído: %s", {k: v for k, v in contagem.items() if v})
    return 1 if contagem["erro"] else 0


if __name__ == "__main__":
    sys.exit(main())
