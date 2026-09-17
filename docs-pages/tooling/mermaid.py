"""
mermaid.py: gera e renderiza os diagramas de linhagem das tabelas gold.

O texto Mermaid sai da linhagem dbt ja coletada — os ref() e source() de cada
modelo — e nao de coordenadas calculadas: e o formato que se le em diff e que
acompanha o codigo.

A renderizacao para SVG usa mermaid-cli, que precisa de Node e de um Chrome.
Como o build do CI roda offline e sem Node, o SVG e guardado em cache por hash
do proprio texto do diagrama, versionado em src/_diagramas/. O diagrama so e
re-renderizado quando a linhagem muda de fato.
"""

import hashlib
import json
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from markupsafe import Markup

from tooling.common import SRC_DIR, log

DIAGRAMAS_DIR = SRC_DIR / "_diagramas"
CAMADAS_ORDEM = ("fonte", "bronze", "silver", "gold", "outros")
ROTULO_CAMADA = {
    "fonte": "Fontes",
    "bronze": "Bronze",
    "silver": "Silver",
    "gold": "Gold",
    "outros": "Outros",
}
# O classDef do Mermaid vira `class="..."` nos nos do SVG. Sem prefixo, nomes
# como `fonte` e `gold` colidem com classes do tema e o CSS do site passa a
# estilizar o diagrama — o sintoma sao rotulos em caixa alta e fora do lugar.
PREFIXO = "mm-"
ESTILOS = """  classDef mm-fonte fill:#fff7ed,stroke:#fed7aa,color:#7c2d12
  classDef mm-bronze fill:#fef3e2,stroke:#b45309,color:#7c2d12
  classDef mm-silver fill:#f1f5f9,stroke:#64748b,color:#1e293b
  classDef mm-gold fill:#f3ebff,stroke:#7a34f3,color:#4c1d95
  classDef mm-alvo fill:#7a34f3,stroke:#5b21b6,color:#ffffff,font-weight:bold"""


def _rotulo(nome: str, camada: str) -> str:
    """Rotulo do no, quebrado em duas linhas quando o nome e longo.

    Nome de fonte carrega o schema (`raw.tabela`), o que dobra a largura do no
    e, por consequencia, a do diagrama inteiro — a ponto de o texto ficar
    ilegivel quando o SVG e reduzido.
    """
    if camada == "fonte" and "." in nome:
        schema, tabela = nome.split(".", 1)
        return f"{schema}.<br/>{tabela}"
    if len(nome) > 26:
        meio = nome.rfind("_", 0, len(nome) // 2 + 6)
        if meio > 6:
            return f"{nome[:meio]}_<br/>{nome[meio + 1:]}"
    return nome


def _id(nome: str) -> str:
    """Identificador seguro para o Mermaid."""
    return "n_" + re.sub(r"[^0-9a-zA-Z_]", "_", nome)


def _ancestrais(
    alvo: str, por_nome: dict[str, dict[str, Any]]
) -> tuple[dict[str, str], list[tuple[str, str]]]:
    """Percorre a linhagem para tras a partir de uma gold.

    Devolve os nos alcancados, com sua camada, e as arestas entre eles.
    """
    nos: dict[str, str] = {}
    arestas: list[tuple[str, str]] = []
    fila = [alvo]
    while fila:
        nome = fila.pop()
        if nome in nos:
            continue
        modelo = por_nome.get(nome)
        if modelo is None:
            nos[nome] = "outros"
            continue
        nos[nome] = modelo["camada"]
        for ref in modelo["refs"]:
            arestas.append((ref, nome))
            fila.append(ref)
        for fonte in modelo["sources"]:
            nos.setdefault(fonte, "fonte")
            arestas.append((fonte, nome))
    return nos, arestas


def grafo_da_gold(alvo: str, por_nome: dict[str, dict[str, Any]]) -> str:
    """Texto Mermaid do fluxo que constroi uma tabela gold."""
    nos, arestas = _ancestrais(alvo, por_nome)

    # Sem subgraph por camada: o Mermaid ignora `direction` em subgraph com
    # aresta cruzando, e o resultado fica lado a lado — 2400px de largura para
    # o mesmo grafo que ocupa 1150px quando o dagre empilha por nivel sozinho.
    # A legenda das camadas vive no HTML, escrita uma vez em vez de repetida em
    # cada diagrama.
    linhas = ["graph LR"]
    for camada in CAMADAS_ORDEM:
        for nome in sorted(n for n, c in nos.items() if c == camada):
            linhas.append(f'  {_id(nome)}["{_rotulo(nome, camada)}"]')

    for origem, destino in sorted(set(arestas)):
        linhas.append(f"  {_id(origem)} --> {_id(destino)}")

    linhas.append(ESTILOS)
    for camada in CAMADAS_ORDEM:
        desta = sorted(n for n, c in nos.items() if c == camada and n != alvo)
        if desta:
            classes = ",".join(_id(n) for n in desta)
            linhas.append(f"  class {classes} {PREFIXO}{camada}")
    linhas.append(f"  class {_id(alvo)} {PREFIXO}alvo")

    return "\n".join(linhas)


def _tamanho_natural(svg: str) -> str:
    """Fixa a largura do SVG na do viewBox.

    O mermaid-cli emite `width="100%"` com um teto em px. Dentro de uma coluna
    estreita isso reduz o desenho ate o texto ficar ilegivel. Com a largura
    natural, o container rola na horizontal e o diagrama continua legivel.
    """
    caixa = re.search(r'viewBox="0 0 ([\d.]+) ([\d.]+)"', svg)
    if not caixa:
        return svg
    largura = float(caixa.group(1))
    svg = svg.replace('width="100%"', f'width="{largura:.0f}"', 1)
    return re.sub(r"max-width:\s*[\d.]+px;?", "", svg, count=1)


def _hash(texto: str) -> str:
    return hashlib.sha256(texto.encode("utf-8")).hexdigest()[:16]


def _renderizar(pendentes: list[tuple[str, str]]) -> dict[str, str]:
    """Renderiza os diagramas que faltam, numa unica chamada ao mermaid-cli.

    O mermaid-cli aceita um markdown com varios blocos e escreve um SVG por
    bloco, o que evita pagar o custo de subir o navegador N vezes.
    """
    if not shutil.which("npx"):
        log.warning("npx indisponivel: %d diagrama(s) sem SVG", len(pendentes))
        return {}

    chrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    with tempfile.TemporaryDirectory() as tmp:
        pasta = Path(tmp)
        entrada = pasta / "diagramas.md"
        entrada.write_text(
            "\n\n".join(f"```mermaid\n{texto}\n```" for _, texto in pendentes),
            encoding="utf-8",
        )
        comando = ["npx", "-y", "@mermaid-js/mermaid-cli", "-i", str(entrada)]
        comando += ["-o", str(pasta / "saida.md"), "--quiet"]
        if Path(chrome).exists():
            config = pasta / "puppeteer.json"
            config.write_text(
                json.dumps({"executablePath": chrome, "args": ["--no-sandbox"]}),
                encoding="utf-8",
            )
            comando += ["-p", str(config)]

        try:
            subprocess.run(comando, capture_output=True, text=True, check=True)
        except (subprocess.CalledProcessError, OSError) as erro:
            log.warning("mermaid-cli falhou, seguindo sem os SVG: %s", erro)
            return {}

        renderizados: dict[str, str] = {}
        for indice, (chave, _) in enumerate(pendentes, start=1):
            svg = pasta / f"saida-{indice}.svg"
            if svg.exists():
                renderizados[chave] = _tamanho_natural(svg.read_text(encoding="utf-8"))
        return renderizados


def preparar(dominios: list[dict[str, Any]], por_nome: dict[str, Any]) -> int:
    """Anexa a cada modelo gold o seu diagrama, em texto Mermaid e em SVG.

    Devolve quantos diagramas ficaram sem SVG — o template mostra o texto do
    diagrama nesses casos, em vez de um espaço vazio.
    """
    DIAGRAMAS_DIR.mkdir(parents=True, exist_ok=True)
    pendentes: list[tuple[str, str]] = []

    for dominio in dominios:
        for modelo in dominio["gold"]:
            texto = grafo_da_gold(modelo["nome"], por_nome)
            chave = _hash(texto)
            modelo["diagrama"] = texto
            modelo["diagrama_chave"] = chave
            if not (DIAGRAMAS_DIR / f"{chave}.svg").exists():
                pendentes.append((chave, texto))

    if pendentes:
        log.info("renderizando %d diagrama(s) novo(s)", len(pendentes))
        for chave, svg in _renderizar(pendentes).items():
            (DIAGRAMAS_DIR / f"{chave}.svg").write_text(svg, encoding="utf-8")

    sem_svg = 0
    for dominio in dominios:
        for modelo in dominio["gold"]:
            arquivo = DIAGRAMAS_DIR / f"{modelo['diagrama_chave']}.svg"
            if arquivo.exists():
                modelo["diagrama_svg"] = Markup(arquivo.read_text(encoding="utf-8"))
            else:
                modelo["diagrama_svg"] = None
                sem_svg += 1
    return sem_svg
