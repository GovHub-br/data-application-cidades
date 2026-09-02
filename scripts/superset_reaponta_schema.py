# scripts/superset_reaponta_schema.py

"""Reaponta datasets do Superset para outro schema, via API.

Quando uma camada do dbt muda de schema — como o rural, que separou
`empreendimento_rural` em `empreendimento_rural_silver` e `empreendimento_rural_gold` —
os datasets do Superset continuam apontando para o schema antigo e os gráficos param de
achar a tabela. O nome da tabela e das colunas não muda, então não há nada a reconstruir:
é trocar o campo `schema` de cada dataset.

Na UI isso é um dataset por vez, e some sem deixar rastro. Aqui é uma execução,
idempotente, com dry-run por default e log do que mudou.

Credenciais (mesmas do login da UI, provider `db`):

    export SUPERSET_URL=https://superset.exemplo.gov.br
    export SUPERSET_USER=...
    export SUPERSET_PASSWORD=...

Uso (o default de --tabelas já são as 9 gold do rural):

    python scripts/superset_reaponta_schema.py \\
        --de empreendimento_rural --para empreendimento_rural_gold          # dry-run
    python scripts/superset_reaponta_schema.py \\
        --de empreendimento_rural --para empreendimento_rural_gold --apply
"""

import argparse
import logging
import os
import sys
from typing import Dict, List, Optional

import requests

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s %(message)s", stream=sys.stderr
)
log = logging.getLogger(__name__)

# As 9 gold do rural que o Superset consome. Alterar aqui quando a lista mudar.
GOLD_RURAL = [
    "ficha_empreendimento_rural",
    "resumo_gerencial_rural",
    "panorama_estadual_rural",
    "mapa_nacional_rural",
    "evolucao_financeira_rural",
    "execucao_fisica_financeira_chart_rural",
    "ficha_trabalho_social",
    "perfil_beneficiarios",
    "infraestrutura_agua_saneamento",
]


class Superset:
    """Sessão autenticada na API do Superset.

    O Superset exige três coisas juntas num PUT: o Bearer do /security/login, o token do
    /security/csrf_token/ E o cookie de sessão que veio junto com ele. Faltando qualquer
    uma, a resposta é 400 com "CSRF token missing" — por isso tudo passa por um
    requests.Session, que carrega o cookie sozinho.
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
        token = r.json()["access_token"]
        self.s.headers.update({"Authorization": f"Bearer {token}"})
        r = self.s.get(f"{self.url}/api/v1/security/csrf_token/", timeout=30)
        r.raise_for_status()
        self.s.headers.update(
            {"X-CSRFToken": r.json()["result"], "Referer": self.url}
        )
        log.info("Autenticado em %s", self.url)

    def buscar_dataset(self, tabela: str, schema: str) -> Optional[Dict]:
        """Dataset por (table_name, schema), ou None."""
        q = (
            "(filters:!("
            f"(col:table_name,opr:eq,value:'{tabela}'),"
            f"(col:schema,opr:eq,value:'{schema}')"
            "))"
        )
        r = self.s.get(f"{self.url}/api/v1/dataset/", params={"q": q}, timeout=30)
        r.raise_for_status()
        res = r.json().get("result", [])
        return res[0] if res else None

    def trocar_schema(self, pk: int, schema: str) -> None:
        r = self.s.put(
            f"{self.url}/api/v1/dataset/{pk}", json={"schema": schema}, timeout=60
        )
        if not r.ok:
            raise RuntimeError(f"PUT /dataset/{pk} -> {r.status_code}: {r.text[:400]}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--de", required=True, help="schema atual dos datasets")
    ap.add_argument("--para", required=True, help="schema novo")
    ap.add_argument(
        "--tabelas",
        default=",".join(GOLD_RURAL),
        help="nomes de tabela separados por vírgula (default: as 9 gold do rural)",
    )
    ap.add_argument(
        "--apply", action="store_true", help="grava; sem a flag, roda em dry-run"
    )
    args = ap.parse_args()

    faltando = [
        v for v in ("SUPERSET_URL", "SUPERSET_USER", "SUPERSET_PASSWORD")
        if not os.environ.get(v)
    ]
    if faltando:
        log.error("Faltam variáveis de ambiente: %s", ", ".join(faltando))
        return 2

    tabelas: List[str] = [t.strip() for t in args.tabelas.split(",") if t.strip()]
    sup = Superset(
        os.environ["SUPERSET_URL"],
        os.environ["SUPERSET_USER"],
        os.environ["SUPERSET_PASSWORD"],
    )

    log.info(
        "%s | %s -> %s | %d tabela(s)",
        "APPLY" if args.apply else "DRY-RUN",
        args.de,
        args.para,
        len(tabelas),
    )

    contagem = {"trocado": 0, "dry_run": 0, "ja_no_destino": 0, "nao_encontrado": 0,
                "erro": 0}
    for tabela in tabelas:
        ds = sup.buscar_dataset(tabela, args.de)
        if ds is None:
            # idempotência: numa segunda execução ele já está no destino
            if sup.buscar_dataset(tabela, args.para):
                log.info("· %-42s já no destino", tabela)
                contagem["ja_no_destino"] += 1
            else:
                log.warning("· %-42s NÃO ENCONTRADO em %s", tabela, args.de)
                contagem["nao_encontrado"] += 1
            continue
        if not args.apply:
            log.info("· %-42s dataset id=%s trocaria o schema", tabela, ds["id"])
            contagem["dry_run"] += 1
            continue
        try:
            sup.trocar_schema(ds["id"], args.para)
            log.info("✓ %-42s dataset id=%s -> %s", tabela, ds["id"], args.para)
            contagem["trocado"] += 1
        except Exception as e:  # noqa: BLE001 — o motivo do Superset vai no log
            log.error("✗ %-42s %s", tabela, e)
            contagem["erro"] += 1

    log.info("Concluído: %s", {k: v for k, v in contagem.items() if v})
    return 1 if contagem["erro"] or contagem["nao_encontrado"] else 0


if __name__ == "__main__":
    sys.exit(main())
