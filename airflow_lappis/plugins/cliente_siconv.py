import zipfile
import csv
import io
import logging
import requests


class ClienteSiconv:
    URL_ZIP = "https://repositorio.dados.gov.br/seges/detru/siconv.zip"
    ZIP_PATH = "/tmp/siconv.zip"

    def baixar_zip(self) -> None:
        logging.info("[cliente_siconv.py] Baixando arquivo SICONV...")
        response = requests.get(self.URL_ZIP, stream=True)
        with open(self.ZIP_PATH, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logging.info("[cliente_siconv.py] Download concluído")

    def ler_csv(self, nome_csv: str, skip_rows: int = 0) -> list[dict]:
        logging.info(f"[cliente_siconv.py] Lendo {nome_csv}...")
        registros = []

        with zipfile.ZipFile(self.ZIP_PATH, "r") as z:
            with z.open(nome_csv) as f:
                conteudo = io.TextIOWrapper(f, encoding="utf-8")
                reader = csv.DictReader(conteudo, delimiter=";")

                for i, row in enumerate(reader):
                    if i < skip_rows:
                        continue
                    registros.append({k.lower(): v for k, v in row.items()})

        logging.info(f"[cliente_siconv.py] {len(registros)} registros lidos de {nome_csv}")
        return registros