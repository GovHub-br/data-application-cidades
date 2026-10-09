"""Preparo `MaskPii`: PII mascarada no worker, antes do arquivo pousar na raw."""

import dataclasses
import re
import shutil
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ingestion.extractors import RawFile, describe_file
from ingestion.masking import (
    MaskingKeys,
    mascarar_tabular,
    mascarar_xlsx,
    targets_por_posicao,
    verificar_roundtrip_tabular,
    xlsx_tem_alvo,
)
from ingestion.text import detectar_dialeto, detectar_encoding

SAMPLE_BYTES = 65536
TABULAR = {".csv", ".txt"}
EXCEL = {".xlsx"}
ACCESS = {".mdb", ".accdb"}


@dataclass(frozen=True)
class MaskPii:
    """Mascara as colunas de PII do arquivo e cede a versão mascarada no lugar dele.

    Mesmas regras e mesmos tokens do `scripts/mascarar_minio.py` (o código é o mesmo,
    em `ingestion.masking`): o dado novo continua juntando com o já mascarado. O
    lake nunca guarda o original.

    - CSV/TXT: encoding e dialeto detectados pela amostra; reescrita em fluxo,
      byte a byte em latin-1 (coluna que não é PII sai idêntica);
    - XLSX: só as planilhas com alvo são reescritas;
    - `.mdb`: não dá para reescrever; é erro (converta as tabelas antes);
    - outros formatos passam como estão.

    `positions`: regex no nome do arquivo → colunas por posição (`{2: "cpf"}`), para
    arquivo sem cabeçalho. O segredo do HMAC vem de `secret_env`. O que foi
    mascarado vai para o manifesto (`details["masking"]`).
    """

    positions: Mapping[str, Mapping[int, str]] = field(default_factory=dict)
    secret_env: str = "MASKING_HMAC_SECRET"

    def apply(self, part: RawFile, work_dir: Path) -> Iterator[RawFile]:
        keys = MaskingKeys.from_env(self.secret_env)
        ext = part.path.suffix.lower()
        if ext in ACCESS:
            raise ValueError(
                f"{part.name}: .mdb não é mascarável no lugar; converta as tabelas antes"
            )
        if ext in TABULAR:
            yield from self._tabular(part, work_dir, keys)
        elif ext in EXCEL:
            yield from self._xlsx(part, work_dir, keys)
        else:
            yield _with(part, {"status": "nao_suportado"})

    def _tabular(
        self, part: RawFile, work_dir: Path, keys: MaskingKeys
    ) -> Iterator[RawFile]:
        with part.path.open("rb") as source:
            sample = source.read(SAMPLE_BYTES)
        encoding = detectar_encoding(sample)
        dialect = detectar_dialeto(sample, encoding)
        if dialect is None:
            yield _with(part, {"status": "sem_cabecalho"})
            return
        delimiter, lineterm, fully_quoted = dialect
        fixed = self._positions(part.name)
        target = _target(part, work_dir)
        targets, _, total, changed = mascarar_tabular(
            str(part.path),
            str(target),
            delimiter,
            lineterm,
            fully_quoted,
            encoding,
            fixed,
            keys,
        )
        if not targets:
            target.unlink(missing_ok=True)
            yield _with(part, {"status": "sem_pii"})
            return
        verificar_roundtrip_tabular(
            str(part.path), str(target), delimiter, total, sem_header=fixed is not None
        )
        yield self._replace(part, target, targets, total, changed)

    def _xlsx(
        self, part: RawFile, work_dir: Path, keys: MaskingKeys
    ) -> Iterator[RawFile]:
        has_target, _ = xlsx_tem_alvo(str(part.path))
        if not has_target:
            yield _with(part, {"status": "sem_pii"})
            return
        target = _target(part, work_dir)
        targets, _, total, changed, _ = mascarar_xlsx(str(part.path), str(target), keys)
        yield self._replace(part, target, targets, total, changed)

    def _positions(self, name: str) -> list[dict[str, Any]] | None:
        for pattern, columns in self.positions.items():
            if re.search(pattern, name):
                return targets_por_posicao(columns)
        return None

    @staticmethod
    def _replace(
        part: RawFile,
        target: Path,
        targets: list[dict[str, Any]],
        total: int,
        changed: int,
    ) -> RawFile:
        part.path.unlink()
        final = target.parent.parent / part.name
        shutil.move(target, final)
        target.parent.rmdir()
        masked = describe_file(final, part.name)
        return _with(
            dataclasses.replace(masked, source_id=part.source_id, details=part.details),
            {
                "status": "mascarado",
                "columns": [
                    {k: t[k] for k in ("column", "category", "action")} for t in targets
                ],
                "rows": total,
                "changed": changed,
            },
        )


def _target(part: RawFile, work_dir: Path) -> Path:
    folder = work_dir / f"mascarando-{part.sha256[:12]}"
    folder.mkdir(parents=True, exist_ok=True)
    return folder / part.name


def _with(part: RawFile, masking: dict[str, Any]) -> RawFile:
    return dataclasses.replace(part, details={**part.details, "masking": masking})
