"""Mascaramento de XLSX em fluxo: reescreve só as planilhas com PII, entrada a entrada."""

import re
import shutil
import xml.etree.ElementTree as ET
import zipfile
from typing import IO, Dict, List, Optional, Tuple

from ingestion.masking.keys import MaskingKeys
from ingestion.masking.rules import classificar


# Processamento XLSX
def xlsx_tem_alvo(src_path: str) -> Tuple[bool, bool]:
    """Pré-scan barato dos headers em modo read_only (streaming, sem carregar o DOM).

    load_workbook completo materializa TODAS as células como objetos na RAM (~0,5-1 KB
    por célula); fazer isso só para descobrir que o arquivo não tem PII é desperdício —
    e a maioria dos xlsx do lake não tem. Retorna (tem_alvo, has_pf).
    """
    import openpyxl

    wb = openpyxl.load_workbook(src_path, read_only=True)
    try:
        tem_alvo = False
        has_pf_any = False
        for ws in wb.worksheets:
            first = next(ws.iter_rows(values_only=True), None)
            if first is None:
                continue
            header = [str(c) if c is not None else "" for c in first]
            # None: openpyxl entrega str Unicode; re-decodificar destruiria acentos.
            targets, has_pf = classificar(header, None)
            has_pf_any = has_pf_any or has_pf
            if targets:
                tem_alvo = True
        return tem_alvo, has_pf_any
    finally:
        wb.close()


# --- Reescrita do xlsx em streaming -----------------------------------------------
#
# Um xlsx é um zip de XMLs. Em vez de carregar o workbook (o openpyxl materializa toda
# célula como objeto e estoura a memória da task em planilhas grandes), copiamos cada
# entrada do zip byte a byte e transformamos linha a linha só as planilhas com alvo.
# Efeito colateral bom: o que não é tocado sai idêntico, inclusive modelo PowerPivot,
# calcChain e o valor em cache das fórmulas.

_XL_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
# atenção: este é o namespace do atributo `r:id` em workbook.xml, diferente do
# `package/2006` que nomeia os elementos dentro do .rels
_REL_ID_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_Q = f"{{{_XL_NS}}}"
_RE_COL = re.compile(r"([A-Z]+)")
# sem isto cada <row> reescrita sai com prefixo ns0: e uma declaração de namespace própria
ET.register_namespace("", _XL_NS)


def _col_de_ref(ref: str) -> int:
    """Índice 0-based da coluna a partir da referência da célula ('AB12' -> 27)."""
    m = _RE_COL.match(ref or "")
    if not m:
        return -1
    n = 0
    for ch in m.group(1):
        n = n * 26 + (ord(ch) - 64)
    return n - 1


def _sheets_do_zip(zin: zipfile.ZipFile) -> List[Tuple[str, str]]:
    """[(nome da aba, caminho do xml no zip)], na ordem do workbook.

    A ordem de `xl/worksheets/sheetN.xml` NÃO corresponde à ordem das abas, e o nome do
    arquivo não tem relação com o nome da aba — a ligação é workbook.xml -> rels.
    """
    rels: Dict[str, str] = {}
    with zin.open("xl/_rels/workbook.xml.rels") as f:
        for el in ET.parse(f).getroot():
            destino = el.get("Target", "")
            if destino.startswith("/"):
                destino = destino[1:]
            elif not destino.startswith("xl/"):
                destino = "xl/" + destino
            rels[el.get("Id", "")] = destino.replace("/./", "/")

    saida: List[Tuple[str, str]] = []
    with zin.open("xl/workbook.xml") as f:
        raiz = ET.parse(f).getroot()
        for sheet in raiz.iter(f"{_Q}sheet"):
            rid = sheet.get(f"{{{_REL_ID_NS}}}id", "")
            if rid in rels:
                saida.append((sheet.get("name", ""), rels[rid]))
    return saida


def _ler_shared_strings(zin: zipfile.ZipFile) -> List[str]:
    if "xl/sharedStrings.xml" not in zin.namelist():
        return []
    valores: List[str] = []
    with zin.open("xl/sharedStrings.xml") as f:
        for _, el in ET.iterparse(f, events=("end",)):
            if el.tag == f"{_Q}si":
                valores.append("".join(t.text or "" for t in el.iter(f"{_Q}t")))
                el.clear()
    return valores


def _header_da_sheet(zin: zipfile.ZipFile, caminho: str, compart: List[str]) -> List[str]:
    """Primeira linha da planilha, respeitando buracos (célula ausente = coluna vazia)."""
    with zin.open(caminho) as f:
        for _, el in ET.iterparse(f, events=("end",)):
            if el.tag != f"{_Q}row":
                continue
            celulas: Dict[int, str] = {}
            for c in el.findall(f"{_Q}c"):
                v = c.find(f"{_Q}v")
                if v is None or v.text is None:
                    inline = c.find(f"{_Q}is")
                    texto = (
                        "".join(t.text or "" for t in inline.iter(f"{_Q}t"))
                        if inline is not None
                        else ""
                    )
                else:
                    texto = compart[int(v.text)] if c.get("t") == "s" else (v.text or "")
                celulas[_col_de_ref(c.get("r", ""))] = texto
            el.clear()
            if not celulas:
                return []
            return [celulas.get(i, "") for i in range(max(celulas) + 1)]
    return []


def _indices_compartilhados(zin: zipfile.ZipFile, sheets: List[Tuple[str, set]]) -> set:
    """Índices de sharedStrings que podem ser apagados com segurança.

    A mesma string pode ser referenciada por várias células: apagar uma usada fora de
    coluna-alvo destrói dado legítimo, e manter uma usada só por célula-alvo vaza o valor
    original, que continua no sharedStrings.xml depois de a célula virar `***`. Por isso a
    varredura cobre todas as planilhas, inclusive as sem alvo.
    """
    de_alvo: set = set()
    de_fora: set = set()
    for caminho, alvos in sheets:
        with zin.open(caminho) as f:
            for _, el in ET.iterparse(f, events=("end",)):
                if el.tag != f"{_Q}row":
                    continue
                for c in el.findall(f"{_Q}c"):
                    if c.get("t") != "s":
                        continue
                    v = c.find(f"{_Q}v")
                    if v is None or v.text is None:
                        continue
                    destino = de_alvo if _col_de_ref(c.get("r", "")) in alvos else de_fora
                    destino.add(int(v.text))
                el.clear()
    return de_alvo - de_fora


def _reescrever_shared_strings(
    fin: IO[bytes], fout: IO[bytes], apagar: set, keys: MaskingKeys
) -> None:
    fout.write(b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>')
    fout.write(f'<sst xmlns="{_XL_NS}">'.encode())
    i = 0
    for _, el in ET.iterparse(fin, events=("end",)):
        if el.tag != f"{_Q}si":
            continue
        if i in apagar:
            fout.write(f"<si><t>{keys.redaction}</t></si>".encode())
        else:
            fout.write(ET.tostring(el, encoding="utf-8"))
        i += 1
        el.clear()
    fout.write(b"</sst>")


def _transformar_row(
    bruto: bytes, acoes: Dict[int, str], compart: List[str], keys: MaskingKeys
) -> Tuple[bytes, bool]:
    """Recebe UMA <row> como bytes, devolve (bytes reescritos, alterou?).

    A row vem sem declaração de namespace (ela mora no default do <worksheet>), então é
    embrulhada antes do parse e desembrulhada depois.
    """
    raiz = ET.fromstring(b'<w xmlns="' + _XL_NS.encode() + b'">' + bruto + b"</w>")
    row = raiz[0]
    mudou = _mascarar_linha(row, acoes, compart, keys)
    return ET.tostring(row, encoding="utf-8"), mudou


class _RecorteSheet:
    """Máquina de estados do recorte de <sheetData> no XML da planilha.

    Três estados: PRÓLOGO (antes de <sheetData>), DADOS (entre as <row>) e EPÍLOGO (depois
    de </sheetData>). Prólogo e epílogo são copiados byte a byte; nos dados, cada <row> é
    isolada, transformada e devolvida. A margem de 64 bytes que fica retida no buffer
    garante que uma marcação partida entre dois blocos de leitura não passe despercebida.
    """

    MARGEM = 64
    FIM_ROW = b"</row>"
    FIM_DADOS = b"</sheetData>"

    def __init__(
        self,
        fout: IO[bytes],
        acoes: Dict[int, str],
        compart: List[str],
        keys: MaskingKeys,
    ) -> None:
        self.keys = keys
        self.fout = fout
        self.acoes = acoes
        self.compart = compart
        self.buf = b""
        self.total = 0
        self.alterados = 0
        self.primeira = True
        self.em_dados = False
        self.terminou = False

    def alimentar(self, bloco: bytes) -> None:
        self.buf += bloco
        while self._passo():
            pass

    def finalizar(self) -> None:
        while self._passo():
            pass
        self.fout.write(self.buf)
        self.buf = b""

    def _reter(self) -> bool:
        """Escoa o buffer deixando a margem de segurança. Sempre encerra a rodada."""
        if len(self.buf) > self.MARGEM:
            self.fout.write(self.buf[: -self.MARGEM])
            self.buf = self.buf[-self.MARGEM :]
        return False

    def _passo(self) -> bool:
        if self.terminou:
            self.fout.write(self.buf)
            self.buf = b""
            return False
        if not self.em_dados:
            return self._passo_prologo()
        return self._passo_dados()

    def _passo_prologo(self) -> bool:
        i = self.buf.find(b"<sheetData")
        if i < 0:
            return self._reter()
        j = self.buf.find(b">", i)
        if j < 0:
            return False
        self.fout.write(self.buf[: j + 1])
        # <sheetData/> = planilha sem linhas: já é epílogo
        self.em_dados = self.buf[j - 1 : j] != b"/"
        self.terminou = not self.em_dados
        self.buf = self.buf[j + 1 :]
        return True

    def _passo_dados(self) -> bool:
        i = self.buf.find(b"<row")
        f = self.buf.find(self.FIM_DADOS)
        if i < 0 or (0 <= f < i):
            if f < 0:
                return self._reter()
            self.fout.write(self.buf[: f + len(self.FIM_DADOS)])
            self.buf = self.buf[f + len(self.FIM_DADOS) :]
            self.terminou = True
            return True

        fim_tag = self.buf.find(b">", i)
        j = self.buf.find(self.FIM_ROW, i)
        if fim_tag < 0 or (j < 0 and self.buf[fim_tag - 1 : fim_tag] != b"/"):
            # <row> incompleta: escoa só o que vem antes dela e espera o resto. Cortar
            # pela margem comeria bytes da linha maior que o bloco de leitura.
            if i > 0:
                self.fout.write(self.buf[:i])
                self.buf = self.buf[i:]
            return False
        if self.buf[fim_tag - 1 : fim_tag] == b"/":  # <row .../> vazia
            self.fout.write(self.buf[: fim_tag + 1])
            self.buf = self.buf[fim_tag + 1 :]
            return True

        self.fout.write(self.buf[:i])
        bruto = self.buf[i : j + len(self.FIM_ROW)]
        self.buf = self.buf[j + len(self.FIM_ROW) :]
        if self.primeira:  # cabeçalho: nunca mascarado
            self.primeira = False
            self.fout.write(bruto)
            return True
        self.total += 1
        saida, mudou = _transformar_row(bruto, self.acoes, self.compart, self.keys)
        self.alterados += 1 if mudou else 0
        self.fout.write(saida)
        return True


def _reescrever_sheet(
    fin: IO[bytes],
    fout: IO[bytes],
    acoes: Dict[int, str],
    compart: List[str],
    keys: MaskingKeys,
) -> Tuple[int, int]:
    """Copia a planilha trocando as células-alvo. Retorna (linhas, linhas alteradas).

    Recorte byte a byte: tudo fora de <sheetData> é copiado sem passar por parser e só as
    <row> são materializadas, uma por vez — a memória fica proporcional à maior linha.
    Reconstruir o XML pelo ElementTree seria mais simples, mas descarta silenciosamente os
    irmãos de <sheetData> e a planilha sai sem formatação nenhuma.

    O valor mascarado vai como `inlineStr`, sem inserir entradas em sharedStrings.xml.
    """
    rec = _RecorteSheet(fout, acoes, compart, keys)
    while True:
        bloco = fin.read(1 << 20)
        if not bloco:
            rec.finalizar()
            break
        rec.alimentar(bloco)
    return rec.total, rec.alterados


def _mascarar_linha(
    row: ET.Element, acoes: Dict[int, str], compart: List[str], keys: MaskingKeys
) -> bool:
    """Substitui in-place as células-alvo de uma <row>. Retorna se algo mudou."""
    mudou = False
    for c in row.findall(f"{_Q}c"):
        acao = acoes.get(_col_de_ref(c.get("r", "")))
        if acao is None:
            continue
        formula = c.find(f"{_Q}f")
        v = c.find(f"{_Q}v")
        atual = ""
        if v is not None and v.text is not None:
            atual = compart[int(v.text)] if c.get("t") == "s" else v.text
        elif formula is None:
            inline = c.find(f"{_Q}is")
            if inline is None:
                continue
            atual = "".join(t.text or "" for t in inline.iter(f"{_Q}t"))
        # Célula de fórmula em coluna-alvo: a fórmula é removida junto com o valor em
        # cache. Preservá-la deixaria o Excel recalcular a PII no próximo open.
        if formula is None and not str(atual).strip():
            continue
        for filho in list(c):
            c.remove(filho)
        c.set("t", "inlineStr")
        alvo = ET.SubElement(ET.SubElement(c, f"{_Q}is"), f"{_Q}t")
        alvo.text = keys.token(str(atual)) if acao == "hmac" else keys.redact(str(atual))
        mudou = True
    return mudou


def mascarar_xlsx(
    src_path: str, dst_path: str, keys: Optional[MaskingKeys] = None
) -> Tuple[List[dict], bool, int, int, bool]:
    """Retorna (targets, has_pf, registros_total, registros_alterados, has_formulas).

    Reescrita em streaming (ver bloco acima): a memória é proporcional à maior linha, não
    ao arquivo. `has_formulas` hoje é sempre False — fórmulas fora de coluna-alvo saem
    byte-idênticas, e o campo só continua existindo pelo contrato com a auditoria.
    """
    keys = keys or MaskingKeys.from_env()
    all_targets: List[dict] = []
    has_pf_any = False
    total = alterados = 0

    with zipfile.ZipFile(src_path) as zin:
        compart = _ler_shared_strings(zin)
        por_sheet: Dict[str, Dict[int, str]] = {}
        for nome_aba, caminho in _sheets_do_zip(zin):
            header = _header_da_sheet(zin, caminho, compart)
            if not header:
                continue
            targets, has_pf = classificar(header, None)  # o XML já entrega str
            has_pf_any = has_pf_any or has_pf
            if targets:
                all_targets.extend({**t, "sheet": nome_aba} for t in targets)
                por_sheet[caminho] = {t["idx"]: t["action"] for t in targets}

        if not all_targets:
            shutil.copyfile(src_path, dst_path)
            return all_targets, has_pf_any, 0, 0, False

        todas = [(c, set(por_sheet.get(c, {}))) for _, c in _sheets_do_zip(zin)]
        apagar = _indices_compartilhados(zin, todas)

        with zipfile.ZipFile(dst_path, "w", zipfile.ZIP_DEFLATED) as zout:
            for info in zin.infolist():
                if info.filename in por_sheet:
                    with zin.open(info) as fin, zout.open(info.filename, "w") as fout:
                        n, a = _reescrever_sheet(
                            fin, fout, por_sheet[info.filename], compart, keys
                        )
                        total += n
                        alterados += a
                elif info.filename == "xl/sharedStrings.xml" and apagar:
                    with zin.open(info) as fin, zout.open(info.filename, "w") as fout:
                        _reescrever_shared_strings(fin, fout, apagar, keys)
                else:
                    with zin.open(info) as fin, zout.open(info, "w") as fout:
                        shutil.copyfileobj(fin, fout, 1 << 18)

    return all_targets, has_pf_any, total, alterados, False
