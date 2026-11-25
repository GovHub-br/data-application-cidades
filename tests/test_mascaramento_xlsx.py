"""Mascaramento de XLSX reescrevendo o XML da planilha."""

import io
import re
import zipfile
from xml.sax.saxutils import escape

import openpyxl
import pytest

from tests.conftest import fresh_import

# the package copies the env at import; nothing else reads it, so it can be undone
with pytest.MonkeyPatch.context() as _mp:
    fresh_import(_mp, "mascarar_minio", "mascaramento", MASKING_HMAC_SECRET="segredo-de-teste")

from mascaramento import regras, xlsx  # noqa: E402

_X14AC = "http://schemas.microsoft.com/office/spreadsheetml/2009/9/ac"
_MC = "http://schemas.openxmlformats.org/markup-compatibility/2006"
_SST_TIPO = "application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"
_SST_REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings"


def _como_o_excel(texto: str, compart: list) -> str:
    """Prefixo x14ac declarado só na raiz e usado em cada <row>; texto em sharedStrings."""
    texto = texto.replace(
        "<worksheet ",
        f'<worksheet xmlns:mc="{_MC}" xmlns:x14ac="{_X14AC}" mc:Ignorable="x14ac" ',
        1,
    )
    texto = re.sub(r"<row ([^>]*)>", r'<row \1 x14ac:dyDescent="0.25">', texto)

    def _compartilhar(m: re.Match) -> str:
        if m.group(2) not in compart:
            compart.append(m.group(2))
        return f'<c r="{m.group(1)}" t="s"><v>{compart.index(m.group(2))}</v></c>'

    return re.sub(r'<c r="(\w+)" t="inlineStr"><is><t>([^<]*)</t></is></c>', _compartilhar, texto)


def _planilha_do_excel(tmp_path, linhas):
    wb = openpyxl.Workbook()
    for linha in linhas:
        wb.active.append(linha)
    buf = io.BytesIO()
    wb.save(buf)

    src, compart = tmp_path / "excel.xlsx", []
    with zipfile.ZipFile(buf) as zin, zipfile.ZipFile(src, "w") as zout:
        for info in zin.infolist():
            dados = zin.read(info).decode()
            if info.filename == "xl/worksheets/sheet1.xml":
                dados = _como_o_excel(dados, compart)
            elif info.filename == "[Content_Types].xml":
                dados = dados.replace(
                    "</Types>",
                    f'<Override PartName="/xl/sharedStrings.xml" ContentType="{_SST_TIPO}"/></Types>',
                )
            elif info.filename == "xl/_rels/workbook.xml.rels":
                dados = dados.replace(
                    "</Relationships>",
                    f'<Relationship Id="rIdSst" Type="{_SST_REL}" Target="sharedStrings.xml"/>'
                    "</Relationships>",
                )
            zout.writestr(info, dados)
        sst = "".join(f"<si><t>{escape(s)}</t></si>" for s in compart)
        zout.writestr(
            "xl/sharedStrings.xml",
            f'<sst xmlns="{xlsx._XL_NS}" count="{len(compart)}" uniqueCount="{len(compart)}">'
            f"{sst}</sst>",
        )
    return src


def _mascarar(tmp_path, linhas):
    src, dst = _planilha_do_excel(tmp_path, linhas), tmp_path / "saida.xlsx"
    resultado = xlsx._mascarar_xlsx(str(src), str(dst))
    return resultado, dst


def test_linha_com_prefixo_declarado_na_raiz_e_mascarada(tmp_path):
    linhas = [["cpf_beneficiario_titular", "uf"], ["12345678909", "GO"], ["98765432100", "DF"]]

    (targets, _, total, alterados, _), dst = _mascarar(tmp_path, linhas)

    assert [t["column"] for t in targets] == ["cpf_beneficiario_titular"]
    assert (total, alterados) == (2, 2)
    lidas = list(openpyxl.load_workbook(dst).active.iter_rows(values_only=True))
    assert lidas[1] == (regras._hmac_token("12345678909"), "GO")
    assert lidas[2] == (regras._hmac_token("98765432100"), "DF")
    # o atributo do Excel continua lá, com o prefixo original
    with zipfile.ZipFile(dst) as z:
        sheet = z.read("xl/worksheets/sheet1.xml").decode()
    assert sheet.count('x14ac:dyDescent="0.25"') == 3


def test_cabecalho_da_coluna_alvo_sobrevive_em_shared_strings(tmp_path):
    # o nome da coluna só aparece na coluna-alvo: apagá-lo do sharedStrings deixava a
    # coluna sem nome
    linhas = [["cpf_beneficiario_titular", "uf"], ["12345678909", "GO"]]

    _, dst = _mascarar(tmp_path, linhas)

    cabecalho = next(openpyxl.load_workbook(dst).active.iter_rows(values_only=True))
    assert cabecalho == ("cpf_beneficiario_titular", "uf")
    with zipfile.ZipFile(dst) as z:
        assert "12345678909" not in z.read("xl/sharedStrings.xml").decode()
