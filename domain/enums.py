from enum import Enum


class FileFormat(str, Enum):
    """Valores batem com o `formato` já usado em BaseFileParser/ParserFactory.

    XML/MDB não têm parser registrado ainda — ficam aqui prontos pra quando
    surgir uma fonte real nesses formatos (evita parser sem uso real).
    """

    CSV = "csv"
    TXT = "txt"
    JSON = "json"
    EXCEL = "xlsx"
    XML = "xml"
    MDB = "mdb"
