"""Quais colunas têm PII, pelo nome (do mapeamento do schema do SFTP).

Vieram de `scripts/mascarar_minio.py`, que hoje os importa daqui.
"""

import csv
import re
from collections.abc import Mapping
from typing import List, Optional, Tuple

from ingestion.text import norm_header

# csv pode ter campos grandes (linhas longas de bases bancárias)
csv.field_size_limit(2**31 - 1)

# Padrões de detecção de colunas sensíveis (do mapeamento do schema sftp)
P_CPF = re.compile(r"cpf")
# NIS/PIS/PASEP/NIT são o mesmo número de identificação do trabalhador (identificador de
# PF)
P_NIS = re.compile(r"(^|_)nis(_|$)|nu_nis|num_nis|(^|_)pis(_|$)|pasep|(^|_)nit(_|$)")
P_CEP = re.compile(r"cep")
P_ENDER = re.compile(
    r"endereco|logradouro|(^|_)rua(_|$)|bairro|complemento|"
    r"num_?casa|numero_?casa|(^|_)quadra(_|$)|(^|_)lote(_|$)"
)
# não-endereços que casariam por acidente: "objetivo_complemento" (rótulo de programa),
# "ic_benef_sit_rua" (flag indicadora de situação de rua)
P_ENDER_EXC = re.compile(r"objetivo|sit_rua|(^|_)ic(_|$)")
P_NASC = re.compile(r"nascimento|dt_?nasc|data_?nasc|dat_nasc")
# Atributo sensível (LGPD art. 5º II). Instituição não tem raça nem deficiência, então a
# coluna também serve de prova de que o arquivo trata de pessoa física.
P_SENSIVEL = re.compile(r"cor_raca|(^|_)raca(_|$)|etnia|deficiencia|(^|_)pcd(_|$)")
# Códigos categóricos necessários para análise de equidade e acessibilidade. Eles não
# identificam alguém sozinhos e permanecem apenas nas camadas restritas, ligados a
# CPF/NIS já pseudonimizados. Nomes/textos livres continuam redigidos.
P_SENSIVEL_ANALITICO = re.compile(r"^co_raca_cor_pessoa$|^co_deficiencia_memb$")

# Papéis que sempre denotam pessoa física. Mascarados incondicionalmente.
P_NOME_PESSOA = re.compile(
    r"comprador|conjuge|dependente|completo|(^|_)no_pessoa$|apelido_pessoa"
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
_PF_INDICATOR_FORTE = {"cpf", "nis", "nascimento", "sensivel"}
_PF_INDICATOR_FRACO = {"nome"}
_PF_INDICATOR_CATS = _PF_INDICATOR_FORTE | _PF_INDICATOR_FRACO

# Categorias decididas por um único padrão, na ordem de precedência.
_CATEGORIAS_DIRETAS = [
    (P_CPF, "cpf"),
    (P_NIS, "nis"),
    (P_NASC, "nascimento"),
    (P_SENSIVEL, "sensivel"),
]


# Ação por categoria, igual à que `classificar()` aplica no caminho por nome de coluna.
_ACAO_POR_CATEGORIA = {
    "cpf": "hmac",
    "nis": "hmac",
    "nascimento": "redact",
    "nome": "redact",
    "sensivel": "redact",
    "cep": "redact",
    "endereco": "redact",
}


# Detecção de header / colunas sensíveis
def _categoria(norm: str) -> Optional[str]:
    """Categoria base da coluna (sem aplicar a regra condicional de CEP/endereço).

    A ordem importa: identificador estrutural (CPF/NIS/nascimento/sensível) vence papel,
    e papel vence CEP/endereço.
    """
    for padrao, categoria in _CATEGORIAS_DIRETAS:
        if padrao.search(norm):
            return categoria
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
        elif cat == "sensivel" and P_SENSIVEL_ANALITICO.search(norm):
            # Preserva só códigos analíticos categóricos. O arquivo continua sendo
            # reconhecido como PF e os identificadores diretos seguem protegidos.
            continue
        elif cat in ("nascimento", "nome", "sensivel"):
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
        elif cat in ("cep", "endereco"):
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


def targets_por_posicao(mapa: Mapping[int, str]) -> List[dict]:
    """Alvos declarados por posição, para arquivo SEM cabeçalho (`{2: "cpf"}`).

    Estar aqui significa que a linha 0 é dado, não cabeçalho.
    """
    targets = []
    for idx, categoria in sorted(mapa.items()):
        acao = _ACAO_POR_CATEGORIA.get(categoria)
        if acao is None:
            raise ValueError(f"categoria de PII desconhecida: {categoria!r}")
        targets.append(
            {
                "idx": idx,
                "column": f"(posição {idx})",
                "category": categoria,
                "action": acao,
            }
        )
    return targets
