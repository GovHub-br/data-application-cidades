"""Guardas de segurança para as recipes do OpenMetadata."""

from pathlib import Path

import yaml

RECIPES_DIR = Path(__file__).resolve().parents[1] / "helpers/openmetadata/recipes"


def test_recipes_do_not_carry_hardcoded_credentials() -> None:
    """Toda credencial entra por replacement, nunca literal no YAML."""
    for recipe_path in sorted(RECIPES_DIR.glob("*.yaml")):
        raw = recipe_path.read_text(encoding="utf-8")
        config = yaml.safe_load(raw)

        token = config["workflowConfig"]["openMetadataServerConfig"]["securityConfig"][
            "jwtToken"
        ]
        assert token.startswith("${") and token.endswith(
            "}"
        ), f"{recipe_path.name} tem jwtToken literal"

        for secret_key in ("password", "jwtToken"):
            for line_number, line in enumerate(raw.splitlines(), start=1):
                stripped = line.strip()
                if not stripped.startswith(f"{secret_key}:"):
                    continue
                value = stripped.split(":", 1)[1].strip()
                assert value.startswith(
                    "${"
                ), f"{recipe_path.name}:{line_number} tem {secret_key} literal"
