#!/usr/bin/env python3
"""Audita cobertura e segurança de descrições YAML do dbt.

O auditor examina apenas metadados versionados; nunca abre tabelas, parquets
ou resultados de execução. Por padrão ele é informativo. Com ``--strict``,
falha se houver descrição ausente ou padrão impróprio para publicação.
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MODELS = ROOT / "dbt" / "mcid" / "models"
#: snapshots e seeds ficam FORA de `models/`. Varrer só models/ fazia o
#: auditor não enxergar o YAML deles e reportá-los como sem documentação.
DEFAULT_EXTRAS = (
    ROOT / "dbt" / "mcid" / "snapshots",
    ROOT / "dbt" / "mcid" / "seeds",
)
DEFAULT_MANIFEST = ROOT / "dbt" / "mcid" / "target" / "manifest.json"

RULES = {
    "exemplo": re.compile(r"\b(?:ex\.?|exemplo|por exemplo)\b", re.IGNORECASE),
    "mapeamento_literal": re.compile(r"\b\d+\s*=\s*[^\s]"),
    "caminho_tecnico": re.compile(
        r"(?:s3://|\braw/|\bstaging/|manual_conjuntura\.)", re.IGNORECASE
    ),
    # O catálogo descreve o DADO, não como ele foi produzido. Se algo é manual
    # ou automatizado, de que model veio ou por qual DAG passou, isso é assunto
    # do time de desenvolvimento e não é metadado de consumo.
    "linguagem_de_processo": re.compile(
        r"(?:\[MANUAL\]|\[AUTOMATIZADO\]|automatizad|\bmanual\b|\bdbt\b|pipeline"
        r"|ingest[ãa]o|\bDAG\b|\bscript\b|planilha|\.xlsx|parquet|MinIO"
        r"|\b(?:bronze|silver|gold)_\w+|\bref\(|bate exato|validad[oa] (?:vs|contra))",
        re.IGNORECASE,
    ),
}

#: Frases produzidas por `semantic_descriptions.py`, que preenche o vazio por
#: casamento de prefixo enquanto a descrição curada não existe. Elas não
#: descrevem o dado — "Atributo do indicador no contexto descrito pela tabela"
#: vale para qualquer coluna de qualquer tabela. Contá-las como documentação
#: fazia a cobertura de coluna aparecer como 92% quando a real era 32%.
TEXTO_GERADO = re.compile(
    r"^(?:Atributo do indicador no contexto descrito pela tabela"
    r"|Varia[çc][ãa]o do indicador conforme o recorte definido pela tabela"
    r"|Quantidade do fen[ôo]meno medido no per[íi]odo de refer[êe]ncia"
    r"|Valor monet[áa]rio do indicador no per[íi]odo de refer[êe]ncia"
    r"|Participa[çc][ãa]o ou varia[çc][ãa]o percentual no recorte definido pela tabela"
    r"|Total agregado do indicador no per[íi]odo de refer[êe]ncia"
    r"|Saldo do indicador no per[íi]odo de refer[êe]ncia"
    r"|Taxa associada ao indicador no per[íi]odo de refer[êe]ncia"
    r"|Medida de pre[çc]o ou custo no per[íi]odo de refer[êe]ncia"
    r"|R[óo]tulo do per[íi]odo de refer[êe]ncia da observa[çc][ãa]o"
    r"|Data que identifica o per[íi]odo de refer[êe]ncia da observa[çc][ãa]o"
    r"|Momento de ingest[ãa]o do registro no pipeline"
    r"|Schema \w+: camada)\.?$",
    re.IGNORECASE,
)


def efetiva(texto: str) -> bool:
    """Descrição que de fato documenta — nem vazia, nem texto de preenchimento."""
    return bool(texto) and not TEXTO_GERADO.match(texto)


@dataclass(frozen=True)
class Finding:
    path: Path
    resource: str
    field: str
    rule: str


def nos_do_manifesto(manifest: Path) -> dict[str, str]:
    """Inventário de models/seeds/snapshots, do manifesto do dbt.

    O auditor sozinho só enxerga o que está DECLARADO em YAML, e por isso
    reportava 100% de cobertura ignorando os models sem nenhuma entrada. O
    manifesto dá o denominador verdadeiro. Ele lista nomes e caminhos — é
    metadado de projeto, não conteúdo de tabela.
    """
    import json

    if not manifest.exists():
        return {}
    dados = json.loads(manifest.read_text(encoding="utf-8"))
    return {
        no["name"]: no.get("path", "")
        for no in dados.get("nodes", {}).values()
        if no.get("resource_type") in ("model", "seed", "snapshot")
    }


def load_yaml(path: Path) -> dict:
    content = yaml.safe_load(path.read_text(encoding="utf-8"))
    return content if isinstance(content, dict) else {}


def description(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def audit(root: Path) -> tuple[list[Finding], int, int]:
    findings: list[Finding] = []
    documented_resources = 0
    documented_columns = 0
    total_resources = 0
    total_columns = 0

    for path in sorted(root.rglob("*.yml")):
        document = load_yaml(path)
        for kind in ("models", "sources", "seeds", "snapshots"):
            for resource in document.get(kind, []) or []:
                name = str(resource.get("name", "<sem_nome>"))
                resource_id = f"{kind}.{name}"
                total_resources += 1
                text = description(resource.get("description"))
                if efetiva(text):
                    documented_resources += 1
                elif text:
                    findings.append(
                        Finding(path, resource_id, "", "texto_de_preenchimento")
                    )
                else:
                    findings.append(Finding(path, resource_id, "", "descricao_ausente"))
                findings.extend(check_text(path, resource_id, "", text))

                for column in resource.get("columns", []) or []:
                    column_name = str(column.get("name", "<sem_nome>"))
                    total_columns += 1
                    column_text = description(column.get("description"))
                    if efetiva(column_text):
                        documented_columns += 1
                    elif column_text:
                        findings.append(
                            Finding(
                                path, resource_id, column_name, "texto_de_preenchimento"
                            )
                        )
                    else:
                        findings.append(
                            Finding(path, resource_id, column_name, "descricao_ausente")
                        )
                    findings.extend(
                        check_text(path, resource_id, column_name, column_text)
                    )

    return (
        findings,
        (documented_resources, total_resources),
        (documented_columns, total_columns),
    )


def check_text(path: Path, resource: str, field: str, text: str) -> list[Finding]:
    return [
        Finding(path, resource, field, rule)
        for rule, pattern in RULES.items()
        if text and pattern.search(text)
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--models-root", type=Path, default=DEFAULT_MODELS)
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--resumo", action="store_true", help="só os totais")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    args = parser.parse_args()

    findings, (res_ok, res_tot), (col_ok, col_tot) = audit(args.models_root)
    for extra in DEFAULT_EXTRAS:
        if extra.exists():
            f2, (r2, rt2), (c2, ct2) = audit(extra)
            findings += f2
            res_ok += r2
            res_tot += rt2
            col_ok += c2
            col_tot += ct2
    import collections

    # nós que existem no projeto mas não têm sequer entrada em YAML
    declarados = set()
    caminhos = list(args.models_root.rglob("*.yml"))
    for extra in DEFAULT_EXTRAS:
        if extra.exists():
            caminhos += list(extra.rglob("*.yml"))
    for caminho in sorted(caminhos):
        doc = load_yaml(caminho)
        for tipo in ("models", "sources", "seeds", "snapshots"):
            for recurso in doc.get(tipo, []) or []:
                declarados.add(str(recurso.get("name", "")))
    inventario = nos_do_manifesto(args.manifest)
    sem_yaml = sorted(n for n in inventario if n not in declarados)
    for nome in sem_yaml:
        findings.append(
            Finding(Path(inventario[nome]), f"model.{nome}", "", "sem_entrada_yaml")
        )
    if inventario:
        cobertos = len(inventario) - len(sem_yaml)
        print(
            f"Nós do projeto com entrada em YAML: {cobertos}/{len(inventario)} "
            f"({cobertos * 100 // len(inventario)}%)"
        )
    por_regra = collections.Counter(f.rule for f in findings)
    pct = lambda a, b: f"{a * 100 // b}%" if b else "—"
    print(
        f"Recursos com descrição efetiva: {res_ok}/{res_tot} ({pct(res_ok, res_tot)})  ·  "
        f"Colunas: {col_ok}/{col_tot} ({pct(col_ok, col_tot)})"
    )
    print(
        "Achados por regra: "
        + (", ".join(f"{k}={v}" for k, v in por_regra.most_common()) or "nenhum")
    )
    if args.resumo:
        return 1 if args.strict and findings else 0
    for finding in findings:
        location = finding.resource
        if finding.field:
            location += f".{finding.field}"
        # o caminho do manifesto já vem relativo ao projeto; só o dos YAMLs
        # é absoluto. Chamar relative_to nos dois quebrava a listagem inteira.
        try:
            caminho = finding.path.relative_to(ROOT)
        except ValueError:
            caminho = finding.path
        print(f"{caminho}: {location} — {finding.rule}")

    return 1 if args.strict and findings else 0


if __name__ == "__main__":
    raise SystemExit(main())
