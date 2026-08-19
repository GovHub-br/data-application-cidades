class ParserError(Exception):
    """Erro base do parser."""


class UnsupportedFileFormatError(ParserError):
    pass


class InvalidSourceFileError(ParserError):
    pass


class SchemaValidationError(ParserError):
    pass


class TransformationError(ParserError):
    pass


class ParquetWriteError(ParserError):
    pass
