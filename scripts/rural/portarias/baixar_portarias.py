"""
baixar_portarias.py: baixa os PDFs das portarias do MCMV Rural listadas no manifesto.

Roda no Windows (ou em qualquer máquina que abra o gov.br no navegador). Usa só a
biblioteca padrão do Python, então não precisa instalar nada.

Uso (na raiz do repositório):
    python scripts/rural/portarias/baixar_portarias.py
    python scripts/rural/portarias/baixar_portarias.py --papel autorizacao
    python scripts/rural/portarias/baixar_portarias.py --so 1024_2024 354_2024

O que faz:
- Lê manifesto_portarias_rural.csv (mesma pasta deste script).
- Para cada portaria, baixa a versão original e, se existir, a compilada.
- Grava em pdf/<portaria_codigo>.pdf e pdf/<portaria_codigo>_compilada.pdf.
  Links do DOU (in.gov.br) são páginas, não PDF: gravados como .html.
- Pula o que já foi baixado (use --refazer para baixar de novo).
- Confere se o arquivo começa com %PDF; se não, marca como falha.
- No fim, grava pdf/_download_log.csv com o resultado de cada arquivo.
"""

import argparse
import csv
import time
import urllib.error
import urllib.request
from pathlib import Path

PASTA = Path(__file__).resolve().parent
MANIFESTO = PASTA / "manifesto_portarias_rural.csv"
DESTINO = PASTA / "pdf"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


def baixar(url: str, destino: Path, refazer: bool) -> tuple[str, str]:
    if destino.exists() and destino.stat().st_size > 0 and not refazer:
        return "ja_existia", ""
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            dados = r.read()
    except urllib.error.HTTPError as e:
        return "falha", f"HTTP {e.code}"
    except Exception as e:  # rede, timeout, SSL
        return "falha", f"{type(e).__name__}: {e}"
    if destino.suffix == ".pdf" and not dados.startswith(b"%PDF"):
        return "falha", "o servidor não devolveu um PDF (link quebrado ou página de erro)"
    destino.write_bytes(dados)
    return "ok", f"{len(dados) // 1024} KB"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--papel", choices=["selecao", "autorizacao"], help="baixa só esse papel")
    ap.add_argument("--so", nargs="*", help="códigos de portaria (ex.: 1024_2024)")
    ap.add_argument("--refazer", action="store_true", help="baixa de novo o que já existe")
    args = ap.parse_args()

    DESTINO.mkdir(exist_ok=True)
    with open(MANIFESTO, encoding="utf-8") as f:
        linhas = list(csv.DictReader(f))
    if args.papel:
        linhas = [l for l in linhas if l["papel"] == args.papel]
    if args.so:
        linhas = [l for l in linhas if l["portaria_codigo"] in set(args.so)]

    log = []
    for l in linhas:
        for tipo, url in (("original", l["url"]), ("compilada", l["url_compilada"])):
            if not url:
                continue
            ext = ".html" if "in.gov.br" in url else ".pdf"
            sufixo = "_compilada" if tipo == "compilada" else ""
            destino = DESTINO / f"{l['portaria_codigo']}{sufixo}{ext}"
            status, detalhe = baixar(url, destino, args.refazer)
            print(f"{status:11} {destino.name:40} {detalhe}")
            log.append({"portaria_codigo": l["portaria_codigo"], "tipo": tipo, "arquivo": destino.name,
                        "status": status, "detalhe": detalhe, "url": url})
            if status == "ok":
                time.sleep(1)  # não martelar o gov.br

    with open(DESTINO / "_download_log.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(log[0].keys()) if log else ["portaria_codigo"])
        w.writeheader()
        w.writerows(log)

    falhas = [x for x in log if x["status"] == "falha"]
    print(f"\n{len(log)} arquivos: {len(log) - len(falhas)} ok ou já existiam, {len(falhas)} falharam.")
    for x in falhas:
        print(f"  baixar à mão: {x['arquivo']} <- {x['url']}  ({x['detalhe']})")


if __name__ == "__main__":
    main()
