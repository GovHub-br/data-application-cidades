"""
assets.py: cataloga os documentos enviados pela equipe em docs-pages/src/acervo/.

Cada arquivo vira uma entrada com titulo, tipo e tamanho. Um acervo.yml opcional
na mesma pasta sobrescreve titulo, descricao e a que fase o documento pertence.

Saida: docs-pages/src/_data/acervo.json
"""

from datetime import datetime, timezone
from typing import Any

import yaml

from tooling.common import ACERVO_DIR, log

TIPOS = {
    ".pdf": "documento",
    ".docx": "documento",
    ".md": "documento",
    ".pptx": "apresentacao",
    ".key": "apresentacao",
    ".xlsx": "planilha",
    ".csv": "planilha",
    ".png": "imagem",
    ".jpg": "imagem",
    ".jpeg": "imagem",
    ".svg": "imagem",
}


def _metadados() -> dict[str, dict[str, Any]]:
    manifesto = ACERVO_DIR / "acervo.yml"
    if not manifesto.exists():
        return {}
    try:
        dados = yaml.safe_load(manifesto.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as erro:
        log.warning("acervo.yml invalido: %s", erro)
        return {}
    return {item["arquivo"]: item for item in dados.get("documentos") or []}


def coletar() -> dict[str, Any]:
    if not ACERVO_DIR.is_dir():
        log.info("docs-pages/src/acervo/ ainda nao existe, acervo vazio")
        return {
            "gerado_em": datetime.now(timezone.utc).isoformat(),
            "resumo": {"total_documentos": 0, "por_tipo": {}},
            "documentos": [],
        }

    extras = _metadados()
    documentos: list[dict[str, Any]] = []
    for arquivo in sorted(ACERVO_DIR.rglob("*")):
        if not arquivo.is_file() or arquivo.name == "acervo.yml":
            continue
        relativo = arquivo.relative_to(ACERVO_DIR).as_posix()
        meta = extras.get(relativo, {})
        documentos.append(
            {
                "arquivo": relativo,
                "titulo": meta.get("titulo") or arquivo.stem.replace("-", " ").strip(),
                "descricao": meta.get("descricao", ""),
                "fase": meta.get("fase"),
                "tipo": TIPOS.get(arquivo.suffix.lower(), "outro"),
                "tamanho_kb": round(arquivo.stat().st_size / 1024, 1),
            }
        )

    por_tipo: dict[str, int] = {}
    for doc in documentos:
        por_tipo[doc["tipo"]] = por_tipo.get(doc["tipo"], 0) + 1

    return {
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "resumo": {"total_documentos": len(documentos), "por_tipo": por_tipo},
        "documentos": documentos,
    }
