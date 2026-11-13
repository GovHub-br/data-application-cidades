#!/usr/bin/env python3
"""Baixa o centro (centroide) de cada município do IBGE e grava o seed
dbt/mcid/seeds/trabalho_social/ibge_municipio_centroide.csv.

Uso (na raiz do repositório, com internet):
    python scripts/ibge/baixar_centroides_municipios.py

Fontes (API de serviços de dados do IBGE):
- localidades: código, nome e UF de cada município
  https://servicodados.ibge.gov.br/api/v1/localidades/municipios
- malhas: metadados com o centroide de cada município
  https://servicodados.ibge.gov.br/api/v3/malhas/paises/BR/metadados?intrarregiao=municipio

Depois: dbt seed --target prod --select ibge_municipio_centroide
O painel do Trabalho Social usa o centro do município quando a SNH não traz coordenada válida.
"""
import csv
import gzip
import json
import sys
import urllib.request
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
DESTINO = RAIZ / "dbt" / "mcid" / "seeds" / "trabalho_social" / "ibge_municipio_centroide.csv"
URL_MUN = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
URL_MALHA = "https://servicodados.ibge.gov.br/api/v3/malhas/paises/BR/metadados?intrarregiao=municipio"


def baixar(url: str):
    req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "mcid-govhub"})
    with urllib.request.urlopen(req, timeout=180) as r:
        corpo = r.read()
    # A API do IBGE responde comprimida em gzip mesmo sem o cliente pedir.
    if corpo[:2] == b"\x1f\x8b":
        corpo = gzip.decompress(corpo)
    return json.loads(corpo.decode("utf-8"))


def uf_do_municipio(m: dict) -> str:
    try:
        return m["microrregiao"]["mesorregiao"]["UF"]["sigla"]
    except (KeyError, TypeError):
        try:
            return m["regiao-imediata"]["regiao-intermediaria"]["UF"]["sigla"]
        except (KeyError, TypeError):
            return ""


def main() -> None:
    print("Baixando municípios...")
    municipios = {str(m["id"]): m for m in baixar(URL_MUN)}
    print(f"  {len(municipios)} municípios")
    print("Baixando centroides (malhas)...")
    malha = baixar(URL_MALHA)
    centro = {}
    for item in malha:
        c = item.get("centroide") or {}
        if "latitude" in c and "longitude" in c:
            centro[str(item.get("id"))] = (c["latitude"], c["longitude"])
    print(f"  {len(centro)} centroides")

    linhas = []
    for cod, m in sorted(municipios.items()):
        lat, lon = centro.get(cod, (None, None))
        linhas.append({
            "cod_ibge": cod,
            "municipio": m["nome"],
            "uf": uf_do_municipio(m),
            "latitude": "" if lat is None else f"{float(lat):.6f}",
            "longitude": "" if lon is None else f"{float(lon):.6f}",
        })
    sem = sum(1 for l in linhas if not l["latitude"])
    with open(DESTINO, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["cod_ibge", "municipio", "uf", "latitude", "longitude"])
        w.writeheader()
        w.writerows(linhas)
    print(f"Gravado {DESTINO} ({len(linhas)} municípios, {sem} sem centroide)")
    if sem > 50:
        sys.exit("Muitos municípios sem centroide: confira a resposta da API de malhas.")


if __name__ == "__main__":
    main()
