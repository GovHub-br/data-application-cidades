"""Reaponta os datasets do Superset do conjuntura v1 para a nova arquitetura.

Descartável: some depois que o reapontamento estiver conferido.

O que muda: cada dataset deixa de apontar para `conjuntura.<nome v1>` e passa a
apontar para `<bronze|prata|ouro>.<nome v2>`. Alterar o dataset EM SEU LUGAR
(mesmo `id`) preserva os charts pendurados nele — recriar o dataset os
desconectaria.

Uso:
    python scripts/superset_reaponta_conjuntura.py            # simulação
    python scripts/superset_reaponta_conjuntura.py --aplicar  # grava
"""

import argparse
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parents[1]
load_dotenv(RAIZ / ".env")

SCHEMA_ANTIGO = "conjuntura"

# Tabelas do schema `conjuntura` que NÃO são models e continuam onde estão:
# cargas manuais sem DAG e os snapshots que congelam as edições do boletim.
FICAM = ("bnz_manual_", "bnz_cbic_", "snap_", "boletim_gabarito")


def mapa_v1_para_v2() -> dict[str, tuple[str, str]]:
    """{nome v1: (schema v2, nome v2)}, lido do manifest do dbt."""
    import json

    manifest = json.loads((RAIZ / "dbt/mcid/target/manifest.json").read_text())
    v2 = {
        no["alias"]: no["schema"]
        for no in manifest["nodes"].values()
        if no["resource_type"] == "model" and no["path"].startswith("conjuntura_dbt/")
    }
    sys.path.insert(0, str(Path(__file__).parent))
    from _mapa_conjuntura import MAPA  # noqa: E402

    return {v1: (v2[nome], nome) for v1, nome in MAPA.items() if nome in v2}


class Superset:
    def __init__(self) -> None:
        self.url = os.environ["SUPERSET_URL"].rstrip("/")
        self.s = requests.Session()
        r = self.s.post(
            f"{self.url}/api/v1/security/login",
            json={
                "username": os.environ["SUPERSET_USER"],
                "password": os.environ["SUPERSET_PASSWORD"],
                "provider": "db",
                "refresh": True,
            },
            timeout=60,
        )
        r.raise_for_status()
        self.s.headers.update({"Authorization": f"Bearer {r.json()['access_token']}"})
        csrf = self.s.get(f"{self.url}/api/v1/security/csrf_token/", timeout=60).json()
        self.s.headers.update({"X-CSRFToken": csrf["result"], "Referer": self.url})

    def datasets(self) -> list[dict]:
        achados, pagina = [], 0
        while True:
            q = '{"page_size":100,"page":%d,"columns":["table_name","schema","id"]}' % pagina
            res = self.s.get(f"{self.url}/api/v1/dataset/", params={"q": q}, timeout=60)
            res.raise_for_status()
            lote = res.json().get("result", [])
            if not lote:
                return achados
            achados += lote
            pagina += 1

    def charts_de(self, pk: int) -> int:
        r = self.s.get(f"{self.url}/api/v1/dataset/{pk}/related_objects", timeout=60)
        return r.json().get("charts", {}).get("count", 0) if r.ok else -1

    def reapontar(self, pk: int, schema: str, tabela: str) -> None:
        r = self.s.put(
            f"{self.url}/api/v1/dataset/{pk}",
            json={"schema": schema, "table_name": tabela},
            timeout=60,
        )
        r.raise_for_status()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--aplicar", action="store_true")
    args = ap.parse_args()

    mapa = mapa_v1_para_v2()
    api = Superset()
    todos = api.datasets()

    alvos, sem_mapa = [], []
    for d in todos:
        if d.get("schema") != SCHEMA_ANTIGO:
            continue
        nome = d.get("table_name") or ""
        if nome.startswith(FICAM):
            continue
        if nome in mapa:
            alvos.append((d, *mapa[nome]))
        else:
            sem_mapa.append(nome)

    print(f"datasets no Superset: {len(todos)}")
    print(f"em `{SCHEMA_ANTIGO}` e com destino na v2: {len(alvos)}")
    print()
    for d, schema, tabela in sorted(alvos, key=lambda x: x[0]["table_name"]):
        n = api.charts_de(d["id"])
        print(f"  [{d['id']:4}] {SCHEMA_ANTIGO}.{d['table_name']}")
        print(f"         -> {schema}.{tabela}   ({n} charts)")
        if args.aplicar:
            api.reapontar(d["id"], schema, tabela)

    if sem_mapa:
        print()
        print(f"em `{SCHEMA_ANTIGO}` e SEM destino na v2 ({len(sem_mapa)}):")
        for n in sorted(sem_mapa):
            print(f"  {n}")

    print()
    print("APLICADO" if args.aplicar else "SIMULAÇÃO — nada foi gravado")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
