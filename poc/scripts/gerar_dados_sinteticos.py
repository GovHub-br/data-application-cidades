#!/usr/bin/env python3
"""Gera o dataset sintético da POC em CSV/TXT/XLSX + variantes de encoding.

O conteúdo LÓGICO é idêntico nos três formatos principais — é o que permite comparar
"um model por formato" e medir tamanho de parquet de forma justa. As colunas foram
escolhidas para reproduzir as armadilhas reais do lake do MCid:

  - header com acento e maiúscula ("Código Cliente")   -> normalização de nomes
  - zeros à esquerda ("000123")                        -> cast para INTEGER
  - datas em 3 formatos (DD/MM/YYYY, YYYYMMDD, ISO)    -> parse_date_br
  - valores em 4 formatos ("1.234,56", "0000...,00")   -> parse_financial_value
  - S/N                                                 -> BOOLEAN
  - campo com o próprio delimitador e aspas dentro      -> quoting
  - CPF já tokenizado (como sai do mascarar_minio.py)

As linhas malformadas ficam num ARQUIVO SEPARADO (clientes_malformado.csv) de propósito:
misturá-las ao dataset principal estragaria a comparação de contagem de linhas entre
raw -> staging -> bronze -> silver.
"""

import csv
import random
from datetime import date, timedelta
from pathlib import Path

DEST = Path(__file__).resolve().parent.parent / "data" / "sintetico"
N_LINHAS = 50_000
SEED = 42

HEADER = [
    "Código Cliente",
    "Nome do Cliente",
    "Data de Cadastro",
    "Valor do Contrato",
    "Ativo",
    "CPF",
    "Observação",
]

PRIMEIROS = ["Ana", "João", "Maria", "José", "Antônio", "Luíza", "Camões", "Íris", "Ângela"]
ULTIMOS = ["da Silva", "Gonçalves", "Assunção", "Ferreira", "Müller", "Sá", "Ço-Testé"]

OBSERVACOES = [
    "sem observação",
    'campo com ; ponto-e-vírgula dentro',
    'campo com "aspas" no meio',
    "campo com | pipe dentro",
    "",
]


def _valor(rng: random.Random) -> str:
    """Os 4 formatos de valor que o parse_financial_value do mcid trata."""
    escolha = rng.random()
    centavos = rng.randint(0, 99)
    inteiro = rng.randint(1, 999_999)
    if escolha < 0.40:  # brasileiro com milhar: 123.456,78
        return f"{inteiro:,}".replace(",", ".") + f",{centavos:02d}"
    if escolha < 0.65:  # zero-padded, sem separador de milhar: 00000000123456,78
        return f"{inteiro:014d},{centavos:02d}"
    if escolha < 0.90:  # americano: 4300.75
        return f"{inteiro}.{centavos:02d}"
    return rng.choice(["", "None", "NaN"])  # nulos disfarçados


def _data(rng: random.Random) -> str:
    d = date(2020, 1, 1) + timedelta(days=rng.randint(0, 2000))
    escolha = rng.random()
    if escolha < 0.70:
        return d.strftime("%d/%m/%Y")
    if escolha < 0.85:
        return d.strftime("%Y%m%d")
    if escolha < 0.97:
        return d.strftime("%Y-%m-%d")
    return ""


def gerar_linhas() -> list:
    rng = random.Random(SEED)
    linhas = []
    for i in range(1, N_LINHAS + 1):
        linhas.append([
            f"{i:06d}",  # zeros à esquerda
            f"{rng.choice(PRIMEIROS)} {rng.choice(ULTIMOS)}",
            _data(rng),
            _valor(rng),
            rng.choice(["S", "N"]),
            f"{rng.getrandbits(64):016x}",  # token HMAC-like, como sai do mascaramento
            rng.choice(OBSERVACOES),
        ])
    return linhas


def escrever_delimitado(path: Path, linhas: list, delim: str, encoding: str) -> None:
    with open(path, "w", encoding=encoding, newline="", errors="replace") as f:
        w = csv.writer(f, delimiter=delim, quoting=csv.QUOTE_MINIMAL)
        w.writerow(HEADER)
        w.writerows(linhas)
    print(f"  {path.name:38s} {path.stat().st_size / 1e6:7.2f} MB  ({encoding}, delim={delim!r})")


def escrever_xlsx(path: Path, linhas: list) -> None:
    from openpyxl import Workbook

    wb = Workbook(write_only=True)
    ws = wb.create_sheet("clientes")
    ws.append(HEADER)
    for linha in linhas:
        ws.append(linha)
    wb.save(path)
    print(f"  {path.name:38s} {path.stat().st_size / 1e6:7.2f} MB  (xlsx, 1 aba)")


def escrever_cp1252_patologico(path: Path, linhas: list) -> None:
    """cp1252 com os 5 bytes INDEFINIDOS (0x81,0x8D,0x8F,0x90,0x9D).

    É exatamente o caso que faz o detectar_encoding do lake_utils desistir de cp1252 e
    cair para latin-1. Serve para verificar se o DuckDB preserva o byte como o Python.
    """
    bytes_indefinidos = bytes([0x81, 0x8D, 0x8F, 0x90, 0x9D])
    with open(path, "wb") as f:
        f.write((";".join(HEADER) + "\n").encode("cp1252"))
        for i, linha in enumerate(linhas[:1000]):
            campos = list(linha)
            campos[6] = "obs"  # sem delimitador dentro, para não precisar de quoting
            texto = ";".join(campos)
            dados = texto.encode("cp1252", errors="replace")
            if i % 100 == 0:  # a cada 100 linhas, injeta os bytes patológicos
                dados = dados[:-3] + bytes_indefinidos
            f.write(dados + b"\n")
    print(f"  {path.name:38s} {path.stat().st_size / 1e6:7.2f} MB  (cp1252 + bytes indefinidos)")


def escrever_malformado(path: Path, linhas: list) -> None:
    """Arquivo com 2 linhas quebradas, para exercitar store_rejects do DuckDB."""
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writerow(HEADER)
        w.writerows(linhas[:100])
        f.write("999997;coluna;a;menos\n")  # 4 campos em vez de 7
        f.write("999998;a;mais;1;2;3;4;5;6;7;8\n")  # 11 campos
    print(f"  {path.name:38s} {path.stat().st_size / 1e6:7.2f} MB  (2 linhas malformadas)")


def main() -> None:
    DEST.mkdir(parents=True, exist_ok=True)
    print(f"Gerando {N_LINHAS:,} linhas em {DEST} ...")
    linhas = gerar_linhas()

    escrever_delimitado(DEST / "clientes.csv", linhas, ";", "utf-8")
    escrever_delimitado(DEST / "clientes.txt", linhas, "|", "utf-8")
    escrever_xlsx(DEST / "clientes.xlsx", linhas)
    escrever_delimitado(DEST / "clientes_latin1.csv", linhas, ";", "latin-1")
    escrever_delimitado(DEST / "clientes_cp1252.csv", linhas, ";", "cp1252")
    escrever_cp1252_patologico(DEST / "clientes_cp1252_patologico.csv", linhas)
    escrever_malformado(DEST / "clientes_malformado.csv", linhas)
    print("OK")


if __name__ == "__main__":
    main()
