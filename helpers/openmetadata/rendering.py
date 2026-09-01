"""Renderizacao das recipes YAML.

As recipes sao versionadas com marcadores `${NOME}` e recebem valores em
runtime. A substituicao e textual de proposito: evita depender de um motor de
template so para trocar meia duzia de valores num YAML.
"""

import logging
import re
from pathlib import Path

# Um marcador que sobreviveu a substituicao. Casa `${OM_HOST}`, nao `$OM_HOST`.
MARCADOR_NAO_SUBSTITUIDO = re.compile(r"\$\{([A-Z0-9_]+)\}")


def render_recipe(
    source_recipe_path: str,
    recipe_replacements: dict,
    output_dir: Path,
) -> Path:
    """Substitui os marcadores da recipe e grava a versao renderizada.

    Levanta se sobrar marcador. Sem essa checagem a recipe segue com o literal
    `${INGESTION_TOKEN}` no lugar do token, o OpenMetadata responde erro de
    autenticacao, e o rastro aponta para credencial invalida em vez de para a
    chave que faltou nos replacements.
    """
    recipe_file = Path(source_recipe_path)

    if not recipe_file.exists():
        raise FileNotFoundError(f"Recipe nao encontrada: {recipe_file}")

    rendered_recipe = recipe_file.read_text(encoding="utf-8")

    for key, value in recipe_replacements.items():
        if value is None:
            continue
        rendered_recipe = rendered_recipe.replace(f"${{{key}}}", str(value))

    faltando = sorted(set(MARCADOR_NAO_SUBSTITUIDO.findall(rendered_recipe)))
    if faltando:
        raise KeyError(
            f"{recipe_file.name}: sem valor para {', '.join(faltando)}. "
            f"Recebi: {', '.join(sorted(recipe_replacements))}."
        )

    rendered_recipe_path = output_dir / recipe_file.name
    rendered_recipe_path.write_text(rendered_recipe, encoding="utf-8")

    logging.info("[openmetadata] Recipe renderizada em %s", rendered_recipe_path)
    return rendered_recipe_path
