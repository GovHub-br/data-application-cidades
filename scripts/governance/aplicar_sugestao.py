#!/usr/bin/env python3
"""Aplica no YAML do dbt uma sugestão de descrição feita no OpenMetadata.

Chamado pelo workflow `om-sugestao.yml`, que recebe o `repository_dispatch` do
receptor (repositório de governança declarativa). Edita a descrição do model
ou da coluna no arquivo onde ela mora; o workflow abre o PR.

A edição troca só as linhas do campo, e não regrava o arquivo: os `schema.yml`
deste repositório têm indentação variada e muito comentário, e um dump
completo reescreveria centenas de linhas para mudar uma. Depois da troca o
arquivo é relido, e a edição só é aceita se a ÚNICA diferença semântica for o
campo sugerido.

Uso:
    python scripts/governance/aplicar_sugestao.py --carga carga.json
"""

from __future__ import annotations

import argparse
import copy
import json
import re
import sys
import textwrap
from pathlib import Path
from typing import Any

import yaml
from ruamel.yaml import YAML

RAIZ = Path(__file__).resolve().parents[2]
MODELS = RAIZ / "dbt" / "mcid" / "models"
LARGURA = 88
DOC_BLOCK = re.compile(r"^\s*\{\{\s*doc\(\s*['\"]([\w.]+)['\"]\s*\)\s*\}\}\s*$")


class SugestaoInvalida(Exception):
    pass


# ── onde a descrição mora ───────────────────────────────────────────────────
def nome_da_tabela(fqn: str) -> str:
    """`Servico.banco.schema.tabela` → `tabela` (nome entre aspas fica inteiro)."""
    partes = re.findall(r'"[^"]*"|[^.]+', fqn)
    if len(partes) != 4:
        raise SugestaoInvalida(f"FQN de tabela inesperado: {fqn}")
    return partes[3].strip('"')


def arquivo_do_model(modelo: str, raiz: Path = MODELS) -> Path | None:
    for caminho in sorted(raiz.rglob("*.yml")):
        documento = yaml.safe_load(caminho.read_text(encoding="utf-8")) or {}
        if any(m.get("name") == modelo for m in documento.get("models") or []):
            return caminho
    return None


def sql_do_model(modelo: str, raiz: Path = MODELS) -> Path | None:
    return next(iter(sorted(raiz.rglob(f"{modelo}.sql"))), None)


# ── renderização ────────────────────────────────────────────────────────────
def linhas_da_descricao(texto: str, indentacao: int) -> list[str]:
    """`description:` plano se couber e for seguro; senão bloco dobrado `>-`."""
    texto = " ".join(texto.split())
    prefixo = " " * indentacao + "description: "
    linha = prefixo + texto
    if len(linha) <= LARGURA and yaml.safe_load(linha.strip()) == {"description": texto}:
        return [linha]
    corpo = textwrap.wrap(texto, width=LARGURA - indentacao - 2)
    return [prefixo + ">-"] + [" " * (indentacao + 2) + parte for parte in corpo]


# ── localização por linha ───────────────────────────────────────────────────
def indentacao(linha: str) -> int:
    return len(linha) - len(linha.lstrip(" "))


ESCALAR_EM_BLOCO = re.compile(r":\s*[>|][+-]?\d*\s*(#.*)?$")


def fim_do_bloco(
    linhas: list[str], inicio: int, coluna: int, comentario_encerra: bool = False
) -> int:
    """Última linha (inclusive) do bloco que começa em `inicio`.

    O bloco segue enquanto a linha estiver vazia ou mais indentada que
    `coluna`. Linhas vazias no fim ficam de fora: pertencem ao que vem depois.
    Com `comentario_encerra`, um `#` encerra o bloco: fora de escalar em bloco
    ele é comentário de quem vem depois, e não texto da descrição.
    """
    fim = inicio
    for i in range(inicio + 1, len(linhas)):
        linha = linhas[i]
        if linha.strip() and indentacao(linha) <= coluna:
            break
        if comentario_encerra and linha.lstrip().startswith("#"):
            break
        if linha.strip():
            fim = i
    return fim


def trocar_ou_inserir_descricao(linhas: list[str], mapa: Any, nova: str) -> list[str]:
    """Troca a `description` do mapeamento (model ou coluna), ou insere após `name`."""
    if "description" in mapa:
        linha_chave, coluna_chave = mapa.lc.key("description")
        em_bloco = bool(ESCALAR_EM_BLOCO.search(linhas[linha_chave]))
        fim = fim_do_bloco(
            linhas, linha_chave, coluna_chave, comentario_encerra=not em_bloco
        )
        return (
            linhas[:linha_chave]
            + linhas_da_descricao(nova, coluna_chave)
            + linhas[fim + 1 :]
        )
    linha_nome, coluna_nome = mapa.lc.key("name")
    fim = fim_do_bloco(linhas, linha_nome, coluna_nome)
    return linhas[: fim + 1] + linhas_da_descricao(nova, coluna_nome) + linhas[fim + 1 :]


def acrescentar_coluna(
    linhas: list[str], model: Any, coluna: str, nova: str
) -> list[str]:
    nome = " ".join(coluna.split())
    if "columns" in model and model["columns"]:
        colunas = model["columns"]
        ultima = len(colunas) - 1
        linha_item, coluna_item = colunas.lc.item(ultima)
        fim = fim_do_bloco(linhas, linha_item, coluna_item - 1)
        traco = " " * (coluna_item - 2)
        novo = [f"{traco}- name: {nome}"] + linhas_da_descricao(nova, coluna_item)
        return linhas[: fim + 1] + novo + linhas[fim + 1 :]
    linha_nome, coluna_nome = model.lc.key("name")
    fim = fim_do_bloco(linhas, linha_nome, coluna_nome - 1)
    base = " " * coluna_nome
    novo = [f"{base}columns:", f"{base}  - name: {nome}"] + linhas_da_descricao(
        nova, coluna_nome + 4
    )
    return linhas[: fim + 1] + novo + linhas[fim + 1 :]


# ── aplicação ───────────────────────────────────────────────────────────────
def esperado_depois(original: dict, modelo: str, coluna: str | None, nova: str) -> dict:
    """O documento como deve ficar: o original com só o campo sugerido trocado."""
    documento = copy.deepcopy(original)
    model = next(m for m in documento["models"] if m.get("name") == modelo)
    texto = " ".join(nova.split())
    if coluna is None:
        model["description"] = texto
        return documento
    colunas = model.setdefault("columns", [])
    alvo = next((c for c in colunas if c.get("name") == coluna), None)
    if alvo is None:
        colunas.append({"name": coluna, "description": texto})
    else:
        alvo["description"] = texto
    return documento


def editar_bloco_doc(nome: str, nova: str, raiz: Path = MODELS) -> Path:
    padrao = re.compile(
        r"(\{%-?\s*docs\s+"
        + re.escape(nome)
        + r"\s*-?%\})(.*?)(\{%-?\s*enddocs\s*-?%\})",
        re.DOTALL,
    )
    for caminho in sorted(raiz.parent.rglob("*.md")):
        texto = caminho.read_text(encoding="utf-8")
        if padrao.search(texto):
            corpo = (
                "\n" + "\n".join(textwrap.wrap(" ".join(nova.split()), LARGURA)) + "\n"
            )
            caminho.write_text(
                padrao.sub(lambda m: m.group(1) + corpo + m.group(3), texto, count=1),
                encoding="utf-8",
            )
            return caminho
    raise SugestaoInvalida(f"bloco doc('{nome}') não encontrado")


def aplicar(modelo: str, coluna: str | None, nova: str, raiz: Path = MODELS) -> Path:
    """Aplica a sugestão e devolve o arquivo alterado."""
    if not nova.strip():
        raise SugestaoInvalida("sugestão vazia")
    caminho = arquivo_do_model(modelo, raiz)
    if caminho is None:
        sql = sql_do_model(modelo, raiz)
        if sql is None:
            raise SugestaoInvalida(f"model {modelo} não existe neste repositório")
        caminho = sql.parent / "schema.yml"
        if not caminho.exists():
            caminho.write_text("version: 2\n\nmodels:\n", encoding="utf-8")
        texto = caminho.read_text(encoding="utf-8")
        existentes = (YAML().load(texto) or {}).get("models") or []
        # o novo item segue a indentação da lista que já existe no arquivo
        traco = existentes.lc.item(0)[1] - 2 if existentes else 2
        caminho.write_text(
            texto.rstrip("\n") + f"\n\n{' ' * traco}- name: {modelo}\n", encoding="utf-8"
        )

    texto = caminho.read_text(encoding="utf-8")
    original = yaml.safe_load(texto)
    documento = YAML().load(texto)
    model = next(m for m in documento["models"] if m.get("name") == modelo)

    atual = (
        model.get("description")
        if coluna is None
        else next(
            (
                c.get("description")
                for c in model.get("columns") or []
                if c.get("name") == coluna
            ),
            None,
        )
    )
    doc = DOC_BLOCK.match(atual or "")
    if doc:
        return editar_bloco_doc(doc.group(1), nova, raiz)

    linhas = texto.splitlines()
    if coluna is None:
        linhas = trocar_ou_inserir_descricao(linhas, model, nova)
    else:
        alvo = next(
            (c for c in model.get("columns") or [] if c.get("name") == coluna), None
        )
        if alvo is None:
            linhas = acrescentar_coluna(linhas, model, coluna, nova)
        else:
            linhas = trocar_ou_inserir_descricao(linhas, alvo, nova)
    novo_texto = "\n".join(linhas) + ("\n" if texto.endswith("\n") else "")

    if yaml.safe_load(novo_texto) != esperado_depois(original, modelo, coluna, nova):
        raise SugestaoInvalida(
            f"a edição de {caminho} mudaria mais do que a descrição sugerida; abortado"
        )
    caminho.write_text(novo_texto, encoding="utf-8")
    return caminho


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--carga", type=Path, required=True, help="client_payload em JSON"
    )
    args = parser.parse_args()
    carga = json.loads(args.carga.read_text(encoding="utf-8"))
    if carga.get("tipo") != "table":
        print(f"tipo de ativo não suportado: {carga.get('tipo')}", file=sys.stderr)
        return 2
    try:
        caminho = aplicar(
            nome_da_tabela(carga["fqn"]), carga.get("coluna"), carga["sugestao"]
        )
    except SugestaoInvalida as erro:
        print(f"sugestão não aplicada: {erro}", file=sys.stderr)
        return 1
    print(caminho.relative_to(RAIZ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
