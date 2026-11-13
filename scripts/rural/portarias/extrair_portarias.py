"""
extrair_portarias.py: extrai as tabelas de propostas dos anexos das portarias do MCMV Rural
(seleção e autorização de contratação) e grava um CSV por linha de proposta.

Roda onde houver pdfplumber (a VM do Cowork já tem). Lê os arquivos baixados pelo
baixar_portarias.py, na pasta pdf/.

Uso (na raiz do repositório):
    python scripts/rural/portarias/extrair_portarias.py
    python scripts/rural/portarias/extrair_portarias.py --so 1024_2024

Também lê PDFs avulsos, de qualquer linha (FDS, FAR...), sem passar pelo manifesto:
    python scripts/rural/portarias/extrair_portarias.py caminho/da/portaria.pdf [outro.pdf ...]
    python scripts/rural/portarias/extrair_portarias.py caminho/da/portaria.pdf --saida pasta/
Nesse modo o código da portaria sai do nome do arquivo (ex.: ..._PORTARIAMCIDN698_2026_... -> 698_2026)
e as saídas vão para --saida (padrão: a pasta do primeiro PDF), sem o cruzamento com a 354/2024.

Saídas (pasta saida/):
- propostas_portarias_rural.csv: uma linha por proposta em cada portaria.
- conferencia_portarias_rural.csv: uma linha por portaria, com contagens e alertas.
- autorizadas_x_selecionadas.csv: cada proposta autorizada e se ela aparece no anexo da 354/2024.

Como lê cada arquivo:
1. Usa a versão compilada quando existe (ela já traz as alterações).
2. Procura tabelas com pdfplumber e acha o cabeçalho pelas palavras (UF, Município,
   Identificação/Proposta, Entidade/EO, CNPJ, Unidades/UH).
3. Se não achar tabela, lê o texto linha a linha e pega as linhas que têm o número da
   proposta (código no formato 8-4-4-4-12).
4. HTML do DOU: lê a tabela da página com o mesmo critério de cabeçalho.
A chave da proposta é o "Número de identificação da proposta". Ele é normalizado para
minúsculas e sem espaços, porque a quebra de linha do PDF às vezes corta o código.
"""

import argparse
import csv
import html
import re
from collections import Counter, defaultdict
from pathlib import Path

import pdfplumber

PASTA = Path(__file__).resolve().parent
MANIFESTO = PASTA / "manifesto_portarias_rural.csv"
PDFS = PASTA / "pdf"
SAIDA = PASTA / "saida"

RE_ID = re.compile(r"[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}", re.I)
RE_CNPJ = re.compile(r"\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}")
UFS = set("AC AL AP AM BA CE DF ES GO MA MT MS MG PA PB PR PE PI RJ RN RS RO RR SC SP SE TO".split())

# palavra do cabeçalho -> campo
CABECALHO = [
    ("id_proposta", ("identifica", "proposta", "código", "codigo")),
    ("eo_cnpj", ("cnpj",)),
    ("eo_nome", ("entidade", "organizadora", " eo")),
    ("empreendimento", ("empreendimento",)),
    ("municipio", ("munic",)),
    ("uf", ("uf",)),
    ("qt_uh", ("unidades", "uh", "quantidade", "qtd")),
]


def limpa(x) -> str:
    return re.sub(r"\s+", " ", str(x or "")).strip()


def norm_id(x: str) -> str:
    s = re.sub(r"\s+", "", str(x or "")).lower()
    m = RE_ID.search(s)
    if not m:
        return ""
    h = re.sub(r"[^0-9a-f]", "", m.group(0))
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"


def norm_cnpj(x: str) -> str:
    d = re.sub(r"\D", "", str(x or ""))
    return d.zfill(14) if 11 < len(d) <= 14 else ""


def norm_int(x: str):
    d = re.sub(r"[^\d]", "", str(x or ""))
    return int(d) if d else None


def mapear_cabecalho(linha: list) -> dict:
    """Devolve {campo: índice da coluna} se a linha parece cabeçalho de proposta."""
    cel = [limpa(c).lower() for c in linha]
    achados = {}
    for campo, chaves in CABECALHO:
        for i, c in enumerate(cel):
            if i in achados.values():
                continue
            if campo == "uf":
                ok = c in ("uf", "u.f.", "estado")
            else:
                ok = any(k.strip() in c for k in chaves)
            if ok:
                achados[campo] = i
                break
    return achados if {"id_proposta", "qt_uh"} <= set(achados) or len(achados) >= 4 else {}


RE_ID_PEDACO = re.compile(r"^[0-9a-f]{4,}(-[0-9a-f]*)*-?$", re.I)


def juntar_linhas(linhas: list, col_id) -> list:
    """Junta, célula a célula, as linhas de uma mesma proposta quebrada em várias linhas da tabela.
    Na coluna do código a junção é sem espaço (o código é cortado no meio)."""
    largura = max(len(l) for l in linhas)
    out = []
    for i in range(largura):
        partes = [limpa(l[i]) for l in linhas if i < len(l) and limpa(l[i])]
        out.append(("" if i == col_id else " ").join(partes))
    return out


def linhas_de_tabelas(tabelas: list, pagina: int, estado: dict) -> list:
    """Lê as tabelas de uma página. Uma proposta pode ocupar uma linha (layout do Rural) ou várias
    (FDS 698/2026: o código da proposta é cortado em duas linhas e os demais campos ficam na do meio).
    As linhas são acumuladas até o código da proposta ficar completo."""
    saida = []
    for t in tabelas:
        for linha in t:
            if not linha or all(not limpa(c) for c in linha):
                continue
            m = mapear_cabecalho(linha)
            if m:
                estado["mapa"] = m
                estado["buffer"] = []
                continue
            mapa = estado.get("mapa")
            if not mapa:
                continue
            # sem coluna de código no cabeçalho: a coluna é a que traz pedaço de código (ex.: "cf27cf01-6c03-")
            if "id_proposta" not in mapa:
                for i, c in enumerate(linha):
                    if RE_ID_PEDACO.match(limpa(c).replace(" ", "")):
                        mapa["id_proposta"] = i
                        break
            col_id = mapa.get("id_proposta")
            buffer = estado.setdefault("buffer", [])
            buffer.append(list(linha))
            if len(buffer) > 4:
                del buffer[0]
            junta = juntar_linhas(buffer, col_id)
            get = lambda campo: junta[mapa[campo]] if campo in mapa and mapa[campo] < len(junta) else ""
            pid = norm_id(get("id_proposta"))
            if not pid and len(buffer) == 1:
                # layout de uma linha só, sem coluna de código mapeada: procura o código célula a célula
                # (juntar as células grudaria o começo do código ao CNPJ da coluna vizinha)
                pid = next((norm_id(c) for c in linha if norm_id(c)), "")
            texto_linha = " ".join(junta).upper()
            if not pid:
                compacto = re.sub(r"\s", "", "".join(limpa(c) for c in linha)).upper()
                if compacto.startswith("TOTAL"):
                    estado["total_declarado"] = norm_int(get("qt_uh")) or estado.get("total_declarado")
                    estado["buffer"] = []
                continue
            estado["buffer"] = []
            uf = get("uf").upper()
            if uf in UFS:
                estado["uf"] = uf
            else:
                uf = estado.get("uf", "")  # célula de UF mesclada: herda a anterior
            saida.append({
                "uf": uf,
                "municipio": get("municipio").upper(),
                "municipio_original": get("municipio"),
                "id_proposta": pid,
                "eo_nome": get("eo_nome"),
                "eo_cnpj": norm_cnpj(get("eo_cnpj")) or norm_cnpj(texto_linha),
                "empreendimento": get("empreendimento"),
                "qt_uh": norm_int(get("qt_uh")),
                "pagina": pagina,
                "metodo": "tabela",
            })
    return saida


def linhas_de_texto(texto: str, pagina: int) -> list:
    """Plano B: uma proposta por linha de texto que tenha o código da proposta."""
    saida = []
    for ln in texto.splitlines():
        pid = norm_id(ln)
        if not pid:
            continue
        m_id = RE_ID.search(ln.replace(" ", "")) or RE_ID.search(ln)
        cnpj_m = RE_CNPJ.search(ln)
        partes = ln.split()
        uf = partes[0].upper() if partes and partes[0].upper() in UFS else ""
        # município: entre a UF e o código; EO: entre o código e o CNPJ; UH: último número
        idx_id = ln.find(m_id.group(0)) if m_id and m_id.group(0) in ln else -1
        antes = ln[:idx_id] if idx_id >= 0 else ""
        depois = ln[idx_id + len(m_id.group(0)):] if idx_id >= 0 else ln
        municipio = limpa(antes[len(uf):] if uf and antes.lstrip().upper().startswith(uf) else antes).upper()
        eo = limpa(depois[: depois.find(cnpj_m.group(0))] if cnpj_m and cnpj_m.group(0) in depois else "")
        nums = re.findall(r"\b\d{1,4}\b", ln[cnpj_m.end():] if cnpj_m else ln)
        saida.append({
            "uf": uf, "municipio": municipio, "id_proposta": pid, "eo_nome": eo,
            "eo_cnpj": norm_cnpj(cnpj_m.group(0)) if cnpj_m else "",
            "qt_uh": int(nums[-1]) if nums else None, "pagina": pagina, "metodo": "texto",
        })
    return saida


def ler_pdf(caminho: Path):
    estado = {}
    linhas = []
    with pdfplumber.open(caminho) as pdf:
        for n, pg in enumerate(pdf.pages, start=1):
            tabs = pg.extract_tables() or []
            achou = linhas_de_tabelas(tabs, n, estado)
            if not achou:
                achou = linhas_de_texto(pg.extract_text() or "", n)
            linhas += achou
        texto_total = "\n".join((p.extract_text() or "") for p in pdf.pages[:2])
    return linhas, estado.get("total_declarado"), texto_total


def ler_html(caminho: Path):
    bruto = caminho.read_text(encoding="utf-8", errors="ignore")
    estado = {}
    tabelas = []
    for t in re.findall(r"<table.*?</table>", bruto, flags=re.S | re.I):
        linhas = []
        for tr in re.findall(r"<tr.*?</tr>", t, flags=re.S | re.I):
            cels = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, flags=re.S | re.I)
            linhas.append([html.unescape(re.sub(r"<[^>]+>", " ", c)) for c in cels])
        tabelas.append(linhas)
    linhas = linhas_de_tabelas(tabelas, 1, estado)
    if not linhas:
        texto = html.unescape(re.sub(r"<[^>]+>", "\n", bruto))
        linhas = linhas_de_texto(texto, 1)
    return linhas, estado.get("total_declarado"), ""


def escolher_arquivo(cod: str):
    for nome in (f"{cod}_compilada.pdf", f"{cod}.pdf", f"{cod}.html"):
        p = PDFS / nome
        if p.exists() and p.stat().st_size > 0:
            return p
    return None


def codigo_do_nome(caminho: Path) -> tuple:
    """Número e ano da portaria a partir do nome do arquivo (PORTARIAMCIDN698_2026, 698_2026, N698DE...2026)."""
    nome = caminho.stem.upper()
    m = (re.search(r"N[ºO°]?\s*(\d{1,4})[_\s-]+(20\d{2})", nome)
         or re.search(r"(?<!\d)(\d{1,4})[_-](20\d{2})(?!\d)", nome)
         or re.search(r"N[ºO°]?(\d{1,4})DE.*?(20\d{2})", nome))
    return (m.group(1).lstrip("0") or "0", m.group(2)) if m else ("", "")


def manifesto_avulso(arquivos: list) -> list:
    itens = []
    for a in arquivos:
        numero, ano = codigo_do_nome(a)
        itens.append({"portaria_codigo": f"{numero}_{ano}" if numero else a.stem, "numero": numero, "ano": ano,
                      "dt_portaria": "", "papel": "", "selecao": "", "_arquivo": a})
    return itens


def main() -> None:
    global SAIDA
    ap = argparse.ArgumentParser()
    ap.add_argument("arquivos", nargs="*", type=Path,
                    help="PDFs ou HTML avulsos para ler direto, sem o manifesto do Rural")
    ap.add_argument("--so", nargs="*", help="códigos de portaria do manifesto (ex.: 1024_2024)")
    ap.add_argument("--tabela", action="store_true",
                    help="grava também a tabela do anexo como está no PDF: <nome do pdf>.csv, separado por ';' (abre direto no Excel)")
    ap.add_argument("--saida", type=Path, help="pasta das saídas (padrão: saida/ ou a pasta do primeiro PDF)")
    args = ap.parse_args()

    if args.arquivos:
        faltando = [str(a) for a in args.arquivos if not a.exists()]
        if faltando:
            raise SystemExit("Arquivo não encontrado: " + ", ".join(faltando))
        manifesto = manifesto_avulso([a.resolve() for a in args.arquivos])
        SAIDA = args.saida or args.arquivos[0].resolve().parent
    else:
        with open(MANIFESTO, encoding="utf-8") as f:
            manifesto = list(csv.DictReader(f))
        if args.so:
            manifesto = [m for m in manifesto if m["portaria_codigo"] in set(args.so)]
        SAIDA = args.saida or SAIDA
    SAIDA.mkdir(parents=True, exist_ok=True)

    todas, conf = [], []
    for m in manifesto:
        cod = m["portaria_codigo"]
        arq = m.get("_arquivo") or escolher_arquivo(cod)
        base = {k: m[k] for k in ("portaria_codigo", "numero", "ano", "dt_portaria", "papel", "selecao")}
        if not arq:
            conf.append({**base, "arquivo": "", "situacao": "sem arquivo", "propostas": 0, "uh": 0,
                         "total_uh_declarado": "", "alertas": "baixar o PDF"})
            continue
        try:
            linhas, total_decl, _ = ler_html(arq) if arq.suffix == ".html" else ler_pdf(arq)
        except Exception as e:
            conf.append({**base, "arquivo": arq.name, "situacao": "erro", "propostas": 0, "uh": 0,
                         "total_uh_declarado": "", "alertas": f"{type(e).__name__}: {e}"})
            continue
        for l in linhas:
            todas.append({**base, "arquivo": arq.name, **l})
        if args.tabela and linhas:
            destino = SAIDA / f"{arq.stem}.csv"
            with open(destino, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.writer(f, delimiter=";")
                w.writerow(["IDENTIFICACAO DA PROPOSTA", "UF", "MUNICÍPIO", "CNPJ ENTIDADE",
                            "NOME DO EMPREENDIMENTO", "UH"])
                for l in linhas:
                    w.writerow([l["id_proposta"], l["uf"], l.get("municipio_original") or l["municipio"],
                                l["eo_cnpj"], l.get("empreendimento", ""), l["qt_uh"] if l["qt_uh"] is not None else ""])
            print(f"  tabela do anexo: {destino}")
        ids = Counter(l["id_proposta"] for l in linhas)
        uh = sum(l["qt_uh"] or 0 for l in linhas)
        alertas = []
        if not linhas:
            alertas.append("nenhuma proposta encontrada: abrir o PDF (pode ser imagem ou layout novo)")
        if dup := [k for k, v in ids.items() if v > 1]:
            alertas.append(f"{len(dup)} id(s) repetido(s)")
        if n := sum(1 for l in linhas if not l["eo_cnpj"]):
            alertas.append(f"{n} sem CNPJ")
        if n := sum(1 for l in linhas if l["qt_uh"] is None):
            alertas.append(f"{n} sem UH")
        if n := sum(1 for l in linhas if not l["uf"]):
            alertas.append(f"{n} sem UF")
        if total_decl and total_decl != uh:
            alertas.append(f"soma das UH ({uh}) diferente do total do anexo ({total_decl})")
        if any(l["metodo"] == "texto" for l in linhas):
            alertas.append("parte lida pelo texto (conferir)")
        conf.append({**base, "arquivo": arq.name, "situacao": "lido", "propostas": len(linhas), "uh": uh,
                     "total_uh_declarado": total_decl or "", "alertas": "; ".join(alertas)})
        print(f"{cod:10} {arq.name:34} {len(linhas):5} propostas {uh:7} UH  {'; '.join(alertas)}")

    campos = ["portaria_codigo", "numero", "ano", "dt_portaria", "papel", "selecao", "arquivo", "pagina",
              "uf", "municipio", "id_proposta", "eo_nome", "eo_cnpj", "empreendimento", "qt_uh", "metodo"]
    sufixo = "rural" if not args.arquivos else "_".join(m["portaria_codigo"] for m in manifesto)[:80]
    with open(SAIDA / f"propostas_portarias_{sufixo}.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=campos, extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(todas)
    with open(SAIDA / f"conferencia_portarias_{sufixo}.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(conf[0].keys()))
        w.writeheader()
        w.writerows(conf)

    # Autorizadas x selecionadas (seleção 2023)
    sel = {l["id_proposta"]: l for l in todas if l["portaria_codigo"] == "354_2024"}
    aut = [l for l in todas if l["papel"] == "autorizacao"] if not args.arquivos else []
    if aut:
        vezes = Counter(l["id_proposta"] for l in aut)
        cruz = []
        for l in aut:
            s = sel.get(l["id_proposta"])
            cruz.append({
                "id_proposta": l["id_proposta"], "portaria_autorizacao": l["portaria_codigo"],
                "uf": l["uf"], "municipio": l["municipio"], "eo_cnpj": l["eo_cnpj"], "qt_uh_autorizadas": l["qt_uh"],
                "esta_na_354": "sim" if s else ("354 não lida" if not sel else "não"),
                "qt_uh_selecionadas": s["qt_uh"] if s else "",
                "mesma_eo": ("sim" if s["eo_cnpj"] == l["eo_cnpj"] else "não") if s else "",
                "autorizada_em_quantas_portarias": vezes[l["id_proposta"]],
            })
        with open(SAIDA / "autorizadas_x_selecionadas.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(cruz[0].keys()))
            w.writeheader()
            w.writerows(cruz)
        ids_aut = set(vezes)
        print(f"\nSeleção 2023: {len(sel)} selecionadas na 354; {len(ids_aut)} propostas distintas autorizadas"
              + (f"; {len(ids_aut - set(sel))} autorizadas que não estão na 354" if sel else ""))

    print(f"\nSaídas em {SAIDA}:")
    for nome in (f"propostas_portarias_{sufixo}.csv", f"conferencia_portarias_{sufixo}.csv"):
        print(f"  {SAIDA / nome}")


if __name__ == "__main__":
    main()
