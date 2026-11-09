"""Regras de detecção das colunas sensíveis e transformação dos valores."""

import hashlib
import hmac
import logging
import re
from typing import Dict, List, Optional, Tuple

from lake_utils import norm_header

from .config import (
    HMAC_SECRET,
    REDACTION,
    TOKEN_LEN,
)

log = logging.getLogger(__name__)


# Padrões de detecção de colunas sensíveis (do mapeamento do schema sftp)
P_CPF = re.compile(r"cpf")
# NIS/PIS/PASEP/NIT são o mesmo número de identificação do trabalhador (identificador de
# PF)
P_NIS = re.compile(r"(^|_)nis(_|$)|nu_nis|num_nis|(^|_)pis(_|$)|pasep|(^|_)nit(_|$)")
# RG, título de eleitor, CTPS e certidão do CadÚnico. Zona, seção, UF e datas ficam.
P_DOCUMENTO = re.compile(
    r"identidade|nu_titulo_eleitor|(cart|serie)_trab_prev|(matricula|livro|folha)_certidao"
)
P_CEP = re.compile(r"cep")
P_ENDER = re.compile(
    r"endereco|logradouro|(^|_)rua(_|$)|bairro|complemento|"
    r"num_?casa|numero_?casa|(^|_)quadra(_|$)|(^|_)lote(_|$)"
)
# não-endereços que casariam por acidente: "objetivo_complemento" (rótulo de programa),
# "ic_benef_sit_rua" e o bloco "*_rua_memb" do CadÚnico (situação de rua)
P_ENDER_EXC = re.compile(r"objetivo|sit_rua|rua_memb|(^|_)ic(_|$)")
# Texto livre que costuma carregar dado da pessoa: observação do entrevistador e ponto de
# referência do domicílio no CadÚnico. Como o endereço, só é redigido com indicador de PF.
P_TEXTO_LIVRE = re.compile(r"(^|_)obs(_|$)|observac|referencia_local")
P_NASC = re.compile(r"nascimento|dt_?nasc|data_?nasc|dat_nasc")
# Atributo de perfil (raça, deficiência). Não é mascarado, mas instituição não tem raça
# nem deficiência, então a coluna prova que o arquivo trata de pessoa física.
P_SENSIVEL = re.compile(r"cor_raca|(^|_)raca(_|$)|etnia|deficiencia|(^|_)pcd(_|$)")

# Papéis que sempre denotam pessoa física. Mascarados incondicionalmente.
P_NOME_PESSOA = re.compile(
    r"comprador|conjuge|dependente|completo|(^|_)no_pessoa$|apelido_pessoa|entrevistador"
)
# Papéis que tanto podem ser pessoa quanto instituição: no FAR o "proponente" é a
# prefeitura. Só viram PII com indicador forte no arquivo.
P_NOME_AMBIGUO = re.compile(r"titular|proponente|responsavel|mutuario|beneficiario")

P_NOME_EXC = re.compile(
    r"empreendimento|municipio|(^|_)uf(_|$)|agente|banco|entidade|orgao|"
    r"logradouro|bairro|arquivo|razao|social|programa|modalidade|situacao|"
    r"fantasia|projeto|obra|construtora|incorporadora|"
    # instituição explícita: ente público não é pessoa
    r"ente_publico|(^|_)publico(_|$)|prefeitura|estado|uniao|governo|"
    # "titularidade" é o REGIME do imóvel (próprio/cedido), não o nome de alguém
    r"titularidade|"
    # metadado: em catálogo de dados "nome" descreve uma COLUNA, não uma pessoa
    r"coluna|campo|atributo|conjunto|(^|_)tabela|dicionario|metadado|"
    # colunas com papel (mutuario/beneficiario/titular...) mas que não são NOME:
    # identificadores PJ, códigos, valores, flags e datas
    r"cnpj|cpf|sexo|(^|_)tipo(_|$)|(^|_)vr(_|$)|valor|prest|parcela|"
    r"(^|_)qt(_|$)|(^|_)ic(_|$)|(^|_)dt(_|$)|(^|_)mulher(_|$)|pdc|pcd|objetivo"
)
# Prefixo de código: o conteúdo é um identificador, não texto de nome
# (`co_ente_publico_proponente` guarda '1'). Vale só para a categoria "nome".
P_CODIGO = re.compile(r"^(co|cod|nu|num|qtd?|id)_")

# Indicadores de que o arquivo contém pessoa física. FORTE é estrutural (não existe CPF de
# prefeitura); FRACO é inferido por palavra-chave, e é onde moram os falsos positivos.
# Só o FORTE destrava CEP/endereço e os papéis ambíguos — como o mascaramento reescreve o
# raw/ no lugar, um falso positivo apaga dado público em definitivo.
_PF_INDICATOR_FORTE = {"cpf", "nis", "documento", "nascimento", "sensivel"}
_PF_INDICATOR_FRACO = {"nome"}
_PF_INDICATOR_CATS = _PF_INDICATOR_FORTE | _PF_INDICATOR_FRACO

# Categorias decididas por um único padrão, na ordem de precedência.
_CATEGORIAS_DIRETAS = [
    (P_CPF, "cpf"),
    (P_NIS, "nis"),
    (P_DOCUMENTO, "documento"),
    (P_NASC, "nascimento"),
    (P_SENSIVEL, "sensivel"),
]

# Arquivos SEM cabeçalho, onde o matching por nome não teria o que casar: a posição das
# colunas é declarada à mão, por key exata, depois de conferir o conteúdo. Estar aqui
# também significa que a linha 0 é dado, não cabeçalho (ver `_mascarar_tabular`).
COLUNAS_POR_POSICAO: Dict[str, Dict[int, str]] = {
    "raw/sftp/fabrica/GEFUS/ANTERIORES/CAIXA_AF_GEHIS_ALIENACAO_IMOVEL_M202112.TXT": {
        2: "cpf",
        3: "nis",
    },
}

# Ação por categoria, igual à que `classificar()` aplica no caminho por nome de coluna.
_ACAO_POR_CATEGORIA = {
    "cpf": "hmac",
    "nis": "hmac",
    "documento": "redact",
    "nome": "redact",
    "cep": "redact",
    "endereco": "redact",
    "texto_livre": "redact",
}


def _avisar_mascaramento_sem_prova(key: str, rec: dict) -> None:
    """Avisa quando um arquivo é mascarado sem prova estrutural de pessoa física.

    Como a reescrita é no lugar, o valor não volta sem reingerir da origem — todo
    mascaramento sem CPF/NIS merece conferência.
    """
    if rec.get("status") not in ("masked", "dry_run"):
        return
    cats = {m["category"] for m in (rec.get("masked_columns") or [])}
    if cats and not (cats & _PF_INDICATOR_FORTE):
        log.warning(
            "%s — mascarado SEM indicador forte de PF (só %s). Confira se não é dado "
            "institucional/de empreendimento antes de aplicar.",
            key,
            ", ".join(sorted(cats)),
        )


def targets_por_posicao(key: str) -> Optional[List[dict]]:
    """Alvos declarados para uma key sem cabeçalho. None se a key não está no mapa."""
    mapa = COLUNAS_POR_POSICAO.get(key)
    if not mapa:
        return None
    targets = []
    for idx, categoria in sorted(mapa.items()):
        acao = _ACAO_POR_CATEGORIA.get(categoria)
        if acao is None:
            raise ValueError(
                f"COLUNAS_POR_POSICAO[{key!r}]: categoria desconhecida {categoria!r}"
            )
        targets.append(
            {
                "idx": idx,
                "column": f"(posição {idx})",
                "category": categoria,
                "action": acao,
            }
        )
    return targets


# Mascaramento de valores
def _canonico(valor: str) -> str:
    """Só dígitos e sem zeros à esquerda: cada origem formata o CPF de um jeito.

    É a forma que CadÚnico e INT039 já mandam, então os tokens delas não mudam.
    """
    digitos = re.sub(r"\D", "", valor)
    if not digitos:
        return valor.strip()
    return digitos.lstrip("0") or "0"


def _hmac_token(valor: str) -> str:
    if valor is None or valor.strip() == "":
        return valor
    canonico = _canonico(valor).encode("utf-8")
    return hmac.new(HMAC_SECRET, canonico, hashlib.sha256).hexdigest()[:TOKEN_LEN]


def _redigir(valor: str) -> str:
    if valor is None or valor.strip() == "":
        return valor
    return REDACTION


# Detecção de header / colunas sensíveis
def _categoria(norm: str) -> Optional[str]:
    """Categoria base da coluna (sem aplicar a regra condicional de CEP/endereço).

    A ordem importa: identificador estrutural (CPF/NIS/nascimento/sensível) vence papel,
    e papel vence CEP/endereço.
    """
    for padrao, categoria in _CATEGORIAS_DIRETAS:
        if padrao.search(norm):
            return categoria
    if P_TEXTO_LIVRE.search(norm):
        return "texto_livre"
    if not P_NOME_EXC.search(norm) and not P_CODIGO.search(norm):
        if P_NOME_PESSOA.search(norm):
            return "nome"
        if P_NOME_AMBIGUO.search(norm):
            return "nome_ambiguo"
    if norm == "nome":
        return "nome_bare"
    if P_CEP.search(norm):
        return "cep"
    if P_ENDER.search(norm) and not P_ENDER_EXC.search(norm):
        return "endereco"
    return None


def classificar(  # noqa: C901
    header: List[str], real_encoding: Optional[str]
) -> Tuple[List[dict], bool]:
    """
    Retorna (targets, has_pf_indicator).
    targets: [{idx, column, category, action}] já com a regra condicional aplicada.

    `real_encoding` vale só para header lido como latin-1 sobre bytes de outro encoding
    (CSV/TXT), que é re-decodificado antes do matching. Passe None quando o header já é
    Unicode correto (xlsx, mdb): o round-trip por latin-1 destrói os acentos e
    'Beneficiário' deixa de casar com "beneficiario".
    """
    normed: List[Tuple[int, str, str]] = []  # (idx, original_header, norm)
    for idx, cell in enumerate(header):
        texto = cell
        if real_encoding is not None:
            try:
                texto = cell.encode("latin-1", "surrogateescape").decode(
                    real_encoding, "replace"
                )
            except Exception:  # noqa: BLE001
                texto = cell
        normed.append((idx, cell, norm_header(texto)))

    cats = {idx: _categoria(n) for idx, _, n in normed}
    has_pf_forte = any(c in _PF_INDICATOR_FORTE for c in cats.values())
    has_pf = any(c in _PF_INDICATOR_CATS for c in cats.values())

    targets: List[dict] = []
    for idx, original, norm in normed:
        cat = cats[idx]
        if cat is None:
            continue
        if cat in ("cpf", "nis"):
            action = "hmac"
        elif cat in ("nascimento", "sensivel"):
            # só indicador de PF: o valor é preservado
            continue
        elif cat in ("nome", "documento"):
            action = "redact"
        elif cat == "nome_ambiguo":
            # papel que pode ser instituição: só mascara com prova de PF no arquivo
            if not has_pf_forte:
                continue
            cat, action = "nome", "redact"
        elif cat == "nome_bare":
            if not has_pf:
                continue
            cat, action = "nome", "redact"
        elif cat in ("cep", "endereco", "texto_livre"):
            # basta o indicador fraco: lista de mutuários sem CPF ainda é endereço
            # residencial. Os papéis que davam falso positivo hoje são "nome_ambiguo".
            if not has_pf:  # PJ/empreendimento/obra pública -> preserva
                continue
            action = "redact"
        else:
            continue
        targets.append(
            {"idx": idx, "column": original, "category": cat, "action": action}
        )
    return targets, has_pf
