#!/usr/bin/env python3
"""Aplica no OpenMetadata a governança declarada no dbt.

O `sincronizar_openmetadata.py` já envia descrição de tabela e de coluna. Ele
não envia domínio, produto de dados, proprietário de produto, termo de
glossário nem certificação — e por isso o schema de referência tem as 20
tabelas etiquetadas e certificadas enquanto o nosso tem zero.

Este comando fecha essa lacuna lendo o que está declarado em:

    governance/dominios.yml     domínio, produto, proprietário, tier, etiqueta
    governance/termos_mcid.yml  termos do glossário e a que modelos se aplicam

Nada é criado pela interface. O que está nos arquivos é o que existe no
catálogo; quem editar na tela perde a alteração no próximo sync.

É idempotente e, por padrão, não escreve: sem `--confirmar` apenas relata o
que faria.

Uso:
    poetry run python scripts/governance/sincronizar_governanca.py
    poetry run python scripts/governance/sincronizar_governanca.py --confirmar
"""

from __future__ import annotations

import argparse
import os
import pathlib
import sys
from urllib.parse import quote

import requests
import yaml

RAIZ = pathlib.Path(__file__).resolve().parents[2]
GOV = RAIZ / "dbt" / "mcid" / "governance"


def ambiente() -> tuple[str, str, str, str]:
    for linha in (RAIZ / ".env").read_text(encoding="utf-8").splitlines():
        linha = linha.strip()
        if linha and not linha.startswith("#") and "=" in linha:
            chave, _, valor = linha.partition("=")
            os.environ.setdefault(chave.strip(), valor.strip().strip('"').strip("'"))
    faltando = [
        n
        for n in ("OPENMETADATA_URL", "OPENMETADATA_JWT_TOKEN",
                  "OPENMETADATA_DATABASE_SERVICE", "OPENMETADATA_DATABASE_NAME")
        if not os.environ.get(n)
    ]
    if faltando:
        raise SystemExit("Variáveis ausentes no .env: " + ", ".join(faltando))
    return (
        os.environ["OPENMETADATA_URL"].rstrip("/"),
        os.environ["OPENMETADATA_JWT_TOKEN"],
        os.environ["OPENMETADATA_DATABASE_SERVICE"],
        os.environ["OPENMETADATA_DATABASE_NAME"],
    )


class Om:
    def __init__(self, url: str, token: str, confirmar: bool) -> None:
        self.base = f"{url}/api/v1"
        self.confirmar = confirmar
        self.s = requests.Session()
        self.s.headers.update(
            {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        )
        self.criados, self.atualizados, self.pulados = 0, 0, 0

    def existe(self, rota: str, fqn: str) -> dict | None:
        r = self.s.get(f"{self.base}/{rota}/name/{quote(fqn, safe='')}", timeout=60)
        return r.json() if r.ok else None

    def criar(self, rota: str, corpo: dict, rotulo: str) -> dict | None:
        if not self.confirmar:
            print(f"  [simulação] criaria {rota}: {rotulo}")
            self.criados += 1
            return None
        r = self.s.put(f"{self.base}/{rota}", json=corpo, timeout=90)
        if not r.ok:
            print(f"  FALHA {rota} {rotulo}: HTTP {r.status_code} {r.text[:180]}")
            return None
        self.criados += 1
        return r.json()

    def patch(self, rota: str, ident: str, operacoes: list, rotulo: str) -> None:
        if not operacoes:
            self.pulados += 1
            return
        if not self.confirmar:
            print(f"  [simulação] ajustaria {rotulo}: {[o['path'] for o in operacoes]}")
            self.atualizados += 1
            return
        r = self.s.patch(
            f"{self.base}/{rota}/{ident}",
            headers={"Content-Type": "application/json-patch+json"},
            json=operacoes,
            timeout=90,
        )
        if not r.ok:
            print(f"  FALHA patch {rotulo}: HTTP {r.status_code} {r.text[:180]}")
            return
        self.atualizados += 1


def carregar() -> tuple[dict, dict]:
    dom = yaml.safe_load((GOV / "dominios.yml").read_text(encoding="utf-8"))
    termos = yaml.safe_load((GOV / "termos_mcid.yml").read_text(encoding="utf-8"))
    return dom, termos


def sincronizar_dominios(om: Om, dom: dict) -> None:
    print("Domínios")
    for d in dom["dominios"]:
        atual = om.existe("domains", d["name"])
        corpo = {
            "name": d["name"],
            "displayName": d.get("display_name", d["name"]),
            "description": " ".join((d.get("description") or "").split()),
            "domainType": d.get("domain_type", "Aggregate"),
        }
        if d.get("parent"):
            corpo["parent"] = d["parent"]
        if atual:
            om.patch(
                "domains", atual["id"],
                [{"op": "replace", "path": "/description", "value": corpo["description"]}],
                f"domínio {d['name']}",
            )
        else:
            om.criar("domains", corpo, d["name"])


def sincronizar_produtos(om: Om, dom: dict, servico: str, banco: str) -> None:
    print("Produtos de dados")
    times = {k: v["name"] for k, v in (dom.get("proprietarios") or {}).items()}
    for p in dom["produtos"]:
        time_nome = times.get(p["owner_key"])
        dono = om.existe("teams", time_nome) if time_nome else None
        corpo = {
            "name": p["name"],
            "displayName": p.get("display_name", p["name"]),
            "description": " ".join((p.get("description") or "").split()),
            "domains": [p["domain"]],
        }
        if dono:
            corpo["owners"] = [
                {"id": dono["id"], "type": "team",
                 "name": dono["name"], "fullyQualifiedName": dono["fullyQualifiedName"]}
            ]
        atual = om.existe("dataProducts", p["name"])
        if atual:
            om.patch(
                "dataProducts", atual["id"],
                [{"op": "replace", "path": "/description", "value": corpo["description"]}],
                f"produto {p['name']}",
            )
        else:
            om.criar("dataProducts", corpo, p["name"])


def sincronizar_termos(om: Om, termos: dict) -> None:
    print("Termos de glossário")
    for t in termos["termos"]:
        if om.existe("glossaryTerms", t["fqn"]):
            om.pulados += 1
            continue
        partes = t["fqn"].split(".")
        corpo = {
            "name": t["name"],
            "displayName": t.get("display_name", t["name"]),
            "description": " ".join((t.get("description") or "").split()),
            "glossary": partes[0],
        }
        pai = t.get("parent") or (".".join(partes[:-1]) if len(partes) > 2 else None)
        if pai:
            corpo["parent"] = pai
        om.criar("glossaryTerms", corpo, t["fqn"])


def _tags_da_tabela(dom: dict, produto: str, camada: str) -> list[str]:
    et = dom.get("etiquetas_automaticas") or {}
    saida = []
    if et.get("organizacao"):
        saida.append(et["organizacao"])
    if et.get("por_produto", {}).get(produto):
        saida.append(et["por_produto"][produto])
    if et.get("por_camada", {}).get(camada):
        saida.append(et["por_camada"][camada])
    return saida


def _camada_do_schema(schemas: list[dict], nome: str) -> str:
    for s in schemas:
        if s["name"] == nome:
            return s.get("layer") or "gold"
    return "gold"


def catalogar_tabelas(om: Om, dom: dict, termos: dict, servico: str, banco: str) -> None:
    """Aplica domínio, produto, etiqueta, certificação e glossário nas tabelas.

    A ordem importa: o OpenMetadata recusa vincular tabela a produto de dados
    se ela ainda não pertence ao domínio do produto — a regra chama-se
    `Data Product Domain Validation`. Domínio primeiro, produto depois.
    """
    print("Catalogação das tabelas")
    esquemas = yaml.safe_load((GOV / "schemas.yml").read_text(encoding="utf-8"))["schemas"]

    # termo de glossário por modelo, vindo das duas seções do arquivo
    por_modelo: dict[str, list[str]] = {}
    for termo in termos.get("termos", []):
        for modelo in termo.get("aplica_a") or []:
            por_modelo.setdefault(modelo, []).append(termo["fqn"])
    for fqn, modelos in (termos.get("aplicacao_de_termos_existentes") or {}).items():
        for modelo in modelos:
            por_modelo.setdefault(modelo, []).append(fqn)

    tiers = dom.get("certificacao_por_camada") or {}
    usos = dom.get("permissao_de_uso_por_camada") or {}
    curadorias = dom.get("certificacao_de_curadoria_por_camada") or {}

    for produto in dom["produtos"]:
        dominio = om.existe("domains", produto["domain"])
        prod = om.existe("dataProducts", produto["name"])
        if not (dominio and prod):
            print(f"  domínio ou produto ausente para {produto['name']}; pulando")
            continue
        for schema in produto["schemas"]:
            camada = _camada_do_schema(esquemas, schema)
            etiquetas = _tags_da_tabela(dom, produto["name"], camada)
            tier = tiers.get(camada)
            uso = usos.get(camada)
            curadoria = curadorias.get(camada)
            fqn_schema = f"{servico}.{banco}.{schema}"
            r = om.s.get(
                f"{om.base}/tables",
                params={"databaseSchema": fqn_schema, "limit": 100, "fields": "tags,domains"},
                timeout=90,
            )
            if not r.ok:
                print(f"  não consegui listar {schema}: HTTP {r.status_code}")
                continue
            tabelas = r.json().get("data", [])
            print(f"  {schema}: {len(tabelas)} tabelas · camada {camada} · {tier}")
            for tab in tabelas:
                rotulo = f"{schema}.{tab['name']}"
                if not om.confirmar:
                    om.atualizados += 1
                    continue
                om.patch("tables", tab["id"],
                         [{"op": "add", "path": "/domains",
                           "value": [{"id": dominio["id"], "type": "domain"}]}],
                         rotulo)
                om.patch("tables", tab["id"],
                         [{"op": "add", "path": "/dataProducts",
                           "value": [{"id": prod["id"], "type": "dataProduct"}]}],
                         rotulo)
                marcas = [{"tagFQN": e, "source": "Classification",
                           "labelType": "Automated", "state": "Confirmed"} for e in etiquetas]
                if tier:
                    marcas.append({"tagFQN": f"Tier.{tier}", "source": "Classification",
                                   "labelType": "Automated", "state": "Confirmed"})
                if uso:
                    marcas.append({"tagFQN": uso, "source": "Classification",
                                   "labelType": "Automated", "state": "Confirmed"})
                if curadoria:
                    marcas.append({"tagFQN": curadoria, "source": "Classification",
                                   "labelType": "Automated", "state": "Confirmed"})
                for termo_fqn in por_modelo.get(tab["name"], []):
                    marcas.append({"tagFQN": termo_fqn, "source": "Glossary",
                                   "labelType": "Automated", "state": "Confirmed"})
                if marcas:
                    om.patch("tables", tab["id"],
                             [{"op": "add", "path": "/tags", "value": marcas}], rotulo)
            if not om.confirmar:
                extras = [f"Tier.{tier}"] if tier else []
                if uso:
                    extras.append(uso)
                if curadoria:
                    extras.append(curadoria)
                print(f"    [simulação] aplicaria {etiquetas + extras} e os termos declarados")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--confirmar", action="store_true", help="escreve de fato")
    args = p.parse_args()

    url, token, servico, banco = ambiente()
    om = Om(url, token, args.confirmar)
    dom, termos = carregar()

    if not args.confirmar:
        print("MODO SIMULAÇÃO — nada é escrito. Use --confirmar para aplicar.\n")

    sincronizar_dominios(om, dom)
    sincronizar_produtos(om, dom, servico, banco)
    sincronizar_termos(om, termos)
    catalogar_tabelas(om, dom, termos, servico, banco)

    print(
        f"\ncriados={om.criados}  atualizados={om.atualizados}  "
        f"já conformes={om.pulados}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
