"""Perfis de fonte: a mecânica de cada fonte, sem escolha de pipeline.

Um módulo por fonte, com constantes e funções puras: URL base, como montar a
requisição de uma entrada da Variable, formato da resposta, fluxo de login. A DAG
declara o `DatasetSpec` (extrator, conversor, modo de carga) e usa o perfil para
o que é da fonte:

    INCC_M = DatasetSpec(
        domain="fgv",
        dataset="incc_m",
        extractor=http_file(base_url=fgv.SINDUSCON_PR, requests=(fgv.INCC_M,)),
        converter=ConverterConfig(header_row=3),
        load_mode=LoadMode.OVERWRITE,
    )

Os extratores e conversores continuam genéricos: o perfil só os configura.
"""
