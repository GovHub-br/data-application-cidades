#!/usr/bin/env python3
"""Gera inventario executivo de campos e perguntas do cruzamento RCB x CadUnico."""

from pathlib import Path
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Font, PatternFill, Border, Side
from openpyxl.utils import get_column_letter


PROJECT = Path(__file__).resolve().parents[1]
WORKSPACE = PROJECT.parent
FAMILIA = WORKSPACE / "sharepoint/Cadastro Único/ARQ_FAMILIA_PBF_12122025_SB8.TXT"
PESSOA = WORKSPACE / "reforma-cadunico-mask-GpOSSR/minio-download-b0b43f06fb36b6d0f718.txt.part"
OUT = PROJECT / "docs/levantamento_cadunico_reforma_casa_brasil.xlsx"


def header(path: Path):
    with path.open("r", encoding="utf-8", errors="replace") as f:
        return f.readline().rstrip("\r\n").split(";")


def tema(col):
    c = col.upper()
    grupos = [
        (("CPF", "NIS", "PIS", "IDENTIDADE", "CERTIDAO", "CART_TRAB", "TITULO_ELEITOR"), "identificação/documentos"),
        (("LOGRADOURO", "LOCALIDADE", "CEP", "ENDERE", "REFERENCIA_LOCAL", "UNIDADE_TERRITORIAL"), "endereço/território"),
        (("TRAB", "RENDA", "DINH_", "AGRICULTURA"), "trabalho/renda"),
        (("DOMIC", "COMODO", "PISO", "AGUA", "BANHEIRO", "SANITARIO", "LIXO", "ILUMINACAO", "CALCAMENTO"), "domicílio/inadequação"),
        (("ESCOLA", "CURSO", "SERIE", "LER_ESCREVER"), "educação"),
        (("DEF_", "DEFICIENCIA", "SAUDE", "MEDICAMENTO"), "saúde/deficiência"),
        (("INDIGENA", "QUILOMBOLA"), "grupos tradicionais"),
        (("DESP_",), "despesas"),
        (("RUA_", "DORMIR_", "ALBERGUE", "CRAS", "CREAS"), "situação de rua/assistência"),
        (("SEXO", "RACA", "NASC", "PARENTESCO", "PESSOA", "FAMIL"), "perfil sociodemográfico"),
        (("CADAST", "ATUALIZ", "EST_CADASTRAL", "MARC_PBF", "BPC"), "cadastro/benefícios"),
    ]
    for termos, nome in grupos:
        if any(t in c for t in termos):
            return nome
    return "outros"


def privacidade(col):
    c = col.upper()
    if any(x in c for x in ("CPF", "NIS", "NO_PESSOA", "APELIDO", "MAE", "PAI", "IDENTIDADE", "CERTIDAO", "CART_TRAB", "TITULO_ELEITOR", "ENTREVISTADOR")):
        return "dado pessoal direto/restrito"
    if any(x in c for x in ("LOGRADOURO", "LOCALIDADE", "CEP", "REFERENCIA_LOCAL", "UNIDADE_TERRITORIAL", "ESCOLA", "ESTAB_ASSIST")):
        return "dado pessoal indireto/restrito"
    if any(x in c for x in ("RENDA", "DESP_", "DEF_", "SAUDE", "BPC", "RUA_", "ALBERGUE")):
        return "sensível ou socioeconômico/restrito"
    return "restrito na base individual; agregável"


def uso(col):
    t = tema(col)
    usos = {
        "identificação/documentos": "vínculo determinístico; nunca publicar valor original",
        "endereço/território": "endereço completo, geocodificação e cruzamento espacial, em ambiente restrito",
        "trabalho/renda": "ocupação, atividade laboral, renda formal/informal e capacidade de pagamento",
        "domicílio/inadequação": "condição física, adensamento e proxy de inadequação antes da reforma",
        "educação": "escolaridade e frequência escolar",
        "saúde/deficiência": "perfil de deficiência, apoio e acesso a serviços",
        "grupos tradicionais": "recortes indígena e quilombola",
        "despesas": "orçamento familiar e comprometimento de renda",
        "situação de rua/assistência": "vulnerabilidade e acesso à assistência social",
        "perfil sociodemográfico": "perfil de beneficiários e composição familiar",
        "cadastro/benefícios": "validade cadastral, PBF/BPC e temporalidade do vínculo",
        "outros": "avaliar com dicionário oficial do CadÚnico",
    }
    return usos[t]


QUESTOES = [
    ["Percentual no CadÚnico", "Parcial: modelo pronto; número não materializado localmente", "RCB + CadÚnico Pessoa", "CPF HMAC; fallback NIS HMAC", "100 * contratos com indicador_encontrado_cadunico / total de contratos RCB", "Deduplicar por contrato e por identificador; informar data-base. A evidência local registra 100.859 contratos RCB, mas não contém o resultado materializado do join."],
    ["Serviços/ocupações das pessoas", "Disponível parcialmente", "CadÚnico Pessoa", "CO_PRINCIPAL_TRAB_MEMB, CO_AGRICULTURA_TRAB_MEMB, CO_TRABALHOU_SEMANA_MEMB, CO_CART_ASSINADA_MEMB e IN_DINH_*", "Distribuir pessoas vinculadas por códigos de ocupação/atividade; aplicar dicionário oficial", "A Prata atual preserva apenas parte dos campos. Não confundir ocupação da pessoa com serviço da reforma."],
    ["Serviços executados na reforma", "Não disponível", "Fonte ausente", "—", "Exige item de orçamento/serviço, projeto, medição, vistoria ou nota fiscal", "Modalidade e tipo de imóvel não identificam telhado, banheiro, elétrica, placa solar etc."],
    ["Endereço exato/completo", "Existe somente na origem restrita", "CadÚnico Família", "NO_LOCALIDADE_FAM + NO_TIP_LOGRADOURO_FAM + NO_TIT_LOGRADOURO_FAM + NO_LOGRADOURO_FAM + NU_LOGRADOURO_FAM + DS_COMPLEMENTO_FAM + DS_COMPLEMENTO_ADIC_FAM + NU_CEP_LOGRADOURO_FAM + DS_REFERENCIA_LOCAL_FAM", "Concatenar componentes após o vínculo por código familiar", "A staging atual redige endereço. Não publicar linha a linha; usar para geocodificação controlada e divulgar apenas agregados."],
    ["Unidades em favela/comunidade urbana", "Não calculável só com as bases atuais", "RCB/CadÚnico + IBGE Favelas e Comunidades Urbanas 2022", "endereço geocodificado (lat/lon) + polígono IBGE", "ST_Within(ponto_imóvel, polígono_FCU); contar contratos distintos", "Código municipal/CEP não basta. Registrar precisão e taxa de geocodificação; resultado deve ter categoria indeterminada."],
    ["Reformas de pessoas já contempladas no MCMV", "Requer base nominal histórica de beneficiários", "RCB + beneficiários MCMV FAR/FDS/FGTS/Rural/Oferta Pública", "CPF/NIS HMAC compatível; secundariamente chave familiar", "Existência de benefício anterior à data da contratação RCB; contar pessoas e contratos distintos", "As bases geográficas de empreendimentos disponíveis não provam contemplação individual. Evitar falso positivo por nome/endereço."],
    ["Casas já financiadas", "Parcial e ambíguo", "RCB + CadÚnico + registro de financiamento/garantia", "NU_CONTRATO, NU_TIPO_GARANTIA, SITUACAO_GARANTIA; eventual base registral/financeira", "Separar: (a) reforma financiada pelo RCB; (b) imóvel com financiamento habitacional preexistente", "Todo registro RCB é contrato de financiamento da reforma. Isso não prova que a aquisição do imóvel já estava financiada."],
]

ADICIONAIS = [
    ["qualidade do vínculo", "metodo_vinculo_cadunico, indicador_encontrado_cadunico, data-base", "medir cobertura e possíveis vieses do cruzamento", "alta"],
    ["vulnerabilidade habitacional", "material de piso/parede, água, banheiro, esgoto, lixo, iluminação, calçamento, cômodos/dormitórios", "priorização e linha de base da inadequação", "alta"],
    ["capacidade de pagamento", "renda CadÚnico, renda comprovada RCB, despesas, prestação, prazo, juros, atraso", "comprometimento de renda e risco financeiro", "alta"],
    ["composição familiar", "membros, faixas etárias, parentesco, pessoas por dormitório", "adensamento e perfil familiar", "alta"],
    ["equidade", "sexo, raça/cor, deficiência, indígena, quilombola", "alcance e desigualdades", "alta"],
    ["território", "IBGE município, UF, unidade territorial, endereço geocodificado", "mapas, áreas vulneráveis e distância de serviços", "alta"],
    ["educação e trabalho", "alfabetização, escola, curso, trabalho, ocupação, renda informal", "perfil socioeconômico", "média"],
    ["assistência e saúde", "CRAS/CREAS, estabelecimento de saúde, BPC, deficiência", "rede de proteção associada", "média"],
    ["escopo físico da obra", "item, quantidade, unidade, custo, fornecedor, ART/RRT", "responder o que foi reformado e custo por item", "fonte nova essencial"],
    ["execução e resultado", "medição, vistoria, início/fim, aceite, fotos antes/depois", "conclusão, prazo, qualidade e melhoria", "fonte nova essencial"],
    ["histórico de política habitacional", "programa, modalidade, empreendimento, data de entrega, CPF/NIS HMAC", "identificar contemplação anterior no MCMV", "fonte nova essencial"],
    ["geografia de comunidades urbanas", "latitude, longitude, precisão, polígono e versão IBGE", "contar reformas dentro de FCU", "fonte nova essencial"],
]

RCB = [
    ("nu_contrato", "contrato", "identificador do contrato"), ("cpf_hmac", "vínculo", "CPF tokenizado"),
    ("nis_hmac", "vínculo", "NIS/PIS tokenizado"), ("dt_referencia", "tempo", "data da fotografia"),
    ("dt_contratacao", "tempo", "data do evento/contratação"), ("uf", "território", "UF do imóvel"),
    ("municipio", "território", "município do imóvel"), ("codigo_ibge", "território", "código IBGE"),
    ("modalidade", "produto", "modalidade administrativa"), ("faixa_renda", "perfil", "faixa de renda"),
    ("tipo_imovel", "produto", "somente código 5; dicionário não recebido"), ("tipo_garantia", "financiamento", "tipo de garantia"),
    ("situacao_garantia", "financiamento", "situação da garantia"), ("renda_familiar_comprovada", "renda", "renda comprovada"),
    ("percentual_renda_informal", "renda", "percentual de renda informal"), ("valor_financiado", "financiamento", "valor do evento financiado"),
    ("valor_recurso_proprio", "financiamento", "recursos próprios"), ("valor_fgts_utilizado", "financiamento", "FGTS utilizado"),
    ("taxa_juros_nominal", "financiamento", "taxa nominal inicial"), ("prazo_financiamento_meses", "financiamento", "prazo em meses"),
    ("valor_prestacao_inicial", "financiamento", "prestação inicial"), ("dias_atraso", "adimplência", "dias em atraso"),
    ("valor_investimento", "financiamento", "valor de investimento"), ("area_imovel", "imóvel", "área do imóvel"),
]


def add_sheet(wb, title, rows, widths=None):
    ws = wb.create_sheet(title)
    for row in rows:
        ws.append(row)
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    blue = PatternFill("solid", fgColor="173F5F")
    thin = Side(style="thin", color="D9E2F3")
    for cell in ws[1]:
        cell.fill = blue
        cell.font = Font(color="FFFFFF", bold=True)
        cell.alignment = Alignment(wrap_text=True, vertical="center")
    for row in ws.iter_rows():
        for cell in row:
            cell.alignment = Alignment(wrap_text=True, vertical="top")
            cell.border = Border(bottom=thin)
    if widths:
        for i, width in enumerate(widths, 1):
            ws.column_dimensions[get_column_letter(i)].width = width
    else:
        for col in range(1, ws.max_column + 1):
            ws.column_dimensions[get_column_letter(col)].width = min(55, max(14, max(len(str(ws.cell(r, col).value or "")) for r in range(1, min(ws.max_row, 100) + 1)) + 2))
    return ws


def main():
    pessoa, familia = header(PESSOA), header(FAMILIA)
    wb = Workbook()
    wb.remove(wb.active)
    resumo = [
        ["Item", "Resultado"],
        ["Objetivo", "Inventário de campos e plano de cruzamento Reforma Casa Brasil × CadÚnico"],
        ["Data do levantamento", "14/10/2026"],
        ["CadÚnico Pessoa", f"{len(pessoa)} colunas; 2.199.185 registros observados no arquivo local (sem o cabeçalho)"],
        ["CadÚnico Família", f"{len(familia)} colunas; 9.304.758 registros observados no arquivo local (sem o cabeçalho)"],
        ["RCB", "Evidência local: 100.859 contratos; colunas analíticas mapeadas no dbt"],
        ["Percentual no CadÚnico", "Ainda não calculado: a base RCB materializada e o resultado do join não estão disponíveis localmente. O modelo e a fórmula existem."],
        ["Alerta de privacidade", "Endereço, CPF, NIS e documentos são restritos. O arquivo contém somente nomes de campos e regras; nenhum dado pessoal."],
        ["Conclusão", "O cruzamento atual responde cobertura CadÚnico, perfil e linha de base domiciliar/financeira. Serviços de obra, FCU, contemplação MCMV anterior e financiamento preexistente precisam de fontes adicionais."],
    ]
    add_sheet(wb, "LEIA-ME", resumo, [28, 110])
    add_sheet(wb, "perguntas_prioritarias", [["Pergunta", "Situação agora", "Fonte", "Campos/chave", "Regra proposta", "Limitação"]] + QUESTOES, [35, 28, 30, 55, 65, 75])
    rows = [["Origem", "Coluna original", "Tema", "Uso no cruzamento", "Classificação/controle", "Observação"]]
    for origem, cols in (("CadÚnico Pessoa", pessoa), ("CadÚnico Família", familia)):
        for col in cols:
            rows.append([origem, col, tema(col), uso(col), privacidade(col), "Descrição funcional inferida do nome; validar domínio/códigos no dicionário oficial."])
    add_sheet(wb, "todas_colunas_cadunico", rows, [22, 42, 28, 65, 36, 58])
    add_sheet(wb, "campos_rcb_relevantes", [["Campo tratado", "Tema", "Uso/limitação"]] + [list(x) for x in RCB], [36, 24, 70])
    add_sheet(wb, "colunas_adicionais", [["Bloco", "Campos", "Resposta que habilita", "Prioridade"]] + ADICIONAIS, [34, 70, 65, 22])
    plano = [
        ["Etapa", "Ação", "Saída/controle"],
        [1, "Materializar snapshot RCB e cortes CadÚnico protegidos na mesma data lógica", "contagens por fonte e data-base"],
        [2, "Deduplicar RCB por contrato e CadÚnico Pessoa por CPF/NIS HMAC", "taxas de duplicidade e chaves nulas"],
        [3, "Vincular por CPF HMAC; usar NIS HMAC como fallback", "método do vínculo e indicador encontrado"],
        [4, "Calcular cobertura global e por UF/município/faixa", "numerador, denominador, percentual e intervalo de data"],
        [5, "Trazer Família por CO_FAMILIAR_FAM", "perfil, renda, despesas e domicílio"],
        [6, "Geocodificar endereço em ambiente restrito e cruzar com polígonos FCU do IBGE", "dentro/fora/indeterminado + precisão"],
        [7, "Pseudonimizar bases nominais históricas MCMV com a mesma chave HMAC", "contemplação anterior à contratação RCB"],
        [8, "Incorporar escopo/medição/vistoria da obra", "serviços executados, conclusão e resultado"],
        [9, "Publicar apenas agregados com supressão de células pequenas", "painel sem CPF/NIS/endereço"],
    ]
    add_sheet(wb, "plano_execucao", plano, [10, 90, 65])
    OUT.parent.mkdir(parents=True, exist_ok=True)
    wb.save(OUT)
    print(f"Gerado: {OUT}")
    print(f"Colunas: pessoa={len(pessoa)}, familia={len(familia)}, total={len(pessoa)+len(familia)}")


if __name__ == "__main__":
    main()
