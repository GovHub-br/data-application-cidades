"""Zip: cada membro é extraído para disco e convertido pelo formato dele."""

import dataclasses
import re
import shutil
import tempfile
import zipfile
from collections.abc import Iterator
from pathlib import Path, PurePosixPath

from ingestion.converters.base_converter import FileConverter, Source
from ingestion.converters.converter_errors import ConversionError
from ingestion.converters.converter_registry import ConverterFactory


@ConverterFactory.register("zip", extensions=(".zip",))
class ZipConverter(FileConverter):
    """Um membro por vez: extrai em stream para um diretório temporário ao lado do
    zip, delega ao conversor da extensão do membro e apaga antes do próximo.

    A saída é `<zip>__<membro>` (com a aba ou tabela do membro como sufixo extra).
    `config.include` escolhe os membros; o resto da configuração (encoding,
    delimitador, preâmbulo) vale para eles, como no relatório do Tesouro: um TSV
    UTF-16 dentro do zip. Zip dentro de zip funciona do mesmo jeito.
    """

    def _read(self, path: Path) -> Iterator[Source]:
        member_config = dataclasses.replace(self.config, format=None, include=None)
        found = False
        with zipfile.ZipFile(path) as archive:
            for member in self._members(archive):
                found = True
                with tempfile.TemporaryDirectory(prefix="zip-", dir=path.parent) as tmp:
                    local = Path(tmp) / PurePosixPath(member.filename).name
                    with archive.open(member) as stream, local.open("wb") as target:
                        shutil.copyfileobj(stream, target, 1 << 20)
                    inner = ConverterFactory.for_file(local, member_config)
                    inner.memory_pool = self.memory_pool
                    stem = PurePosixPath(member.filename).with_suffix("").as_posix()
                    for table in inner._read(local):
                        suffix = f"{stem}__{table.suffix}" if table.suffix else stem
                        yield dataclasses.replace(table, suffix=suffix)
        if not found:
            raise ConversionError(f"{path.name}: nenhum membro a converter")

    def _members(self, archive: zipfile.ZipFile) -> Iterator[zipfile.ZipInfo]:
        pattern = re.compile(self.config.include) if self.config.include else None
        for member in archive.infolist():
            if member.is_dir():
                continue
            if pattern and not pattern.search(member.filename):
                continue
            yield member
