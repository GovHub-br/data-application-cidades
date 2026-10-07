#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from minio import Minio


@dataclass(frozen=True)
class Family:
    inventario_id: str
    fonte: str
    pattern: str
    linha_mcmv: str
    natureza_linha: str
    entidade_basica: str
    granularidade: str
    campos_chave: str
    campos_status: str
    campos_empreendimento: str
    campos_financeiros: str
    campos_temporais: str
    campos_geograficos: str
    dados_pessoais: str
    classificacao: str
    confianca: str
    observacao: str


def load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        raw = raw.strip()
        if raw and not raw.startswith("#") and "=" in raw:
            key, value = raw.split("=", 1)
            values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def extract_periods(names: list[str]) -> tuple[str, str]:
    periods: list[str] = []
    for name in names:
        for match in re.findall(r"(?<!\d)(20\d{2})[_-]?(0[1-9]|1[0-2])(?:[_-]?[0-3]\d)?(?!\d)", name):
            periods.append("-".join(match))
    return (min(periods), max(periods)) if periods else ("", "")


def make_client(env: dict[str, str]) -> tuple[Minio, str]:
    client = Minio(
        env["MINIO_ENDPOINT"],
        access_key=env["MINIO_ACCESS_KEY"],
        secret_key=env["MINIO_SECRET_KEY"],
        secure=env.get("MINIO_SECURE", "false").lower() in {"1", "true", "yes", "sim"},
    )
    return client, env["MINIO_BUCKET"]


def families() -> list[Family]:
    pf_fields = dict(
        campos_chave="NU_APF; NU_CONTRATO; NU_CPF_CGC_MUTUARIO",
        campos_status="ORIGEM; TP_ORCAMENTO; FAIXA; COTISTA; TPIMOVEL; SISTAMORT",
        campos_empreendimento="não possui nome/status de empreendimento; operação individual",
        campos_financeiros="VLR_COMPRA; VLR_RENDA_FAMILIAR_COMPROVADA; VLR_EMPRESTIMO; descontos/equilíbrio FGTS e OGU; TXJRSMUT",
        campos_temporais="DT_ASSINATURA; ANO_ORC; DT_MOVIMENTO; DT_NASCIMENTO",
        campos_geograficos="COD_MUN_IBGE; CEP; CO_AGENTE_FINANCEIRO",
        dados_pessoais="sim: nome, CPF/CNPJ, nascimento, sexo, estado civil e CEP; acesso deve ser restrito/LGPD",
    )
    pj_fields = dict(
        campos_chave="NU_APF; CGC; COD_MUNICIPIO_IBGE",
        campos_status="SITUACAO_OBRA; SITUACAO_CONTRATO; PERCENTUAL_OBRA_REALIZADO",
        campos_empreendimento="NO_EMPREENDIMENTO; RZ_SOCIAL; unidades financiadas/concluídas/entregues; início/término da obra",
        campos_financeiros="VI; VE; valores de financiamento/empreendimento conforme versão",
        campos_temporais="DT_MOVIMENTO; DT_ASSINATURA; DT_INICIO_OBRA; DT_TERMINO_OBRA; ANO_ORC",
        campos_geograficos="COD_MUNICIPIO_IBGE; endereço; CEP; latitude; longitude; município; UF; região",
        dados_pessoais="não na amostra; contém dados de pessoa jurídica e endereço do empreendimento",
    )
    return [
        Family("SP_PF_FGTS_20250807", "sharepoint_local", r"^Super Recentes/Base_PF_FGTS_20250807\.txt$", "classe_media", "financiada", "contrato_pessoa_fisica_fgts", "uma operação/contrato PF", **pf_fields, classificacao="base operacional PF FGTS", confianca="alta", observacao="Arquivo solicitado; pipe, Latin-1, 27 campos; ainda não confirmado no MinIO raw/sharepoint."),
        Family("SP_PF_FGTS_20250907", "sharepoint_local", r"^Super Recentes/Base_PF_FGTS_20250907\.txt$", "classe_media", "financiada", "contrato_pessoa_fisica_fgts", "uma operação/contrato PF", **pf_fields, classificacao="base operacional PF FGTS", confianca="alta", observacao="Arquivo solicitado; inclui exemplo TP_ORCAMENTO=Contrapartida Classe Media - SBPE; ainda não confirmado no MinIO raw/sharepoint."),
        Family("SFTP_PF_FGTS", "minio_sftp", r"raw/sftp/.*/Base_PF_FGTS_.*\.txt$", "classe_media", "financiada", "contrato_pessoa_fisica_fgts", "uma operação/contrato PF; snapshots", **pf_fields, classificacao="série histórica e corrente PF FGTS", confianca="alta", observacao="Fonte SFTP já presente no MinIO; contém versões mais atuais que os dois arquivos locais."),
        Family("SP_PJ_FGTS", "sharepoint_local", r"^FGTS - PJ \(Apoio PJ\)/base_pj_fgts_.*\.xlsx$", "classe_media", "financiada", "empreendimento_pessoa_juridica_fgts", "um empreendimento/APF por posição", **pj_fields, classificacao="base operacional PJ/empreendimentos FGTS", confianca="alta", observacao="Fonte central para APF, empreendimento, obra, situação e entregas."),
        Family("SFTP_PJ_FGTS", "minio_sftp", r"raw/sftp/.*/Base_PJ_FGTS_.*\.txt$", "classe_media", "financiada", "empreendimento_pessoa_juridica_fgts", "um empreendimento/APF por posição; snapshots", **pj_fields, classificacao="série histórica e corrente PJ/empreendimentos FGTS", confianca="alta", observacao="Fonte SFTP já presente no MinIO; preferível para atualização recorrente."),
        Family("SP_FGTS_ANALITICO", "sharepoint_local", r"^(Dados Abertos na internet|Exportados do Banco/Dados Abertos FGTS)/.*FGTS.*ANALITICO.*\.csv$", "classe_media", "financiada", "financiamento_fgts_analitico", "um financiamento/registro analítico", "cod_ibge; data_assinatura_financiamento", "programa FGTS; faixa; tipo de imóvel; cotista; amortização", "txt_nome_empreendimento (preenchimento variável)", "financiamento; compra; renda; subsídios/descontos/equilíbrio FGTS e OGU; juros", "data_referencia; data_assinatura_financiamento; nascimento", "cod_ibge; município; UF; região", "sim: nascimento, sexo e renda; versão aberta não expôs CPF/nome na amostra", "dados abertos analíticos FGTS", "alta", "Boa base analítica; validar proteção contra reidentificação e versões repetidas."),
        Family("SP_FGTS_SINTETICO", "sharepoint_local", r"^(Dados Abertos na internet|Exportados do Banco/Dados Abertos FGTS)/.*FGTS.*SINTETICO.*\.csv$", "classe_media", "financiada", "agregado_municipal_fgts", "município e ano", "cod_ibge; num_ano_financiamento", "não possui status individual", "não possui empreendimento individual", "qtd_uh_financiadas; vlr_financiamento; vlr_subsidio", "data_referencia; num_ano_financiamento", "cod_ibge; município; UF", "não", "agregado sintético FGTS", "alta", "Adequado para indicadores municipais; não substitui APF/empreendimento."),
        Family("SP_FGTS_CONTRATACAO_DIARIA", "sharepoint_local", r"^FGTS - Contratação diária por Município/fgts_contratacao_diaria_.*\.csv$", "classe_media", "financiada", "contratacao_municipal_diaria_fgts", "município/data", "município/IBGE; data de posição", "contratação/posição; sem status de obra", "não possui empreendimento individual", "quantidade e valor contratado/financiado conforme versão", "data de contratação/posição", "município; IBGE; UF", "não", "série operacional diária agregada", "média", "Útil para acompanhamento recente; há snapshots vazios/pequenos e versões repetidas a validar."),
        Family("SP_MCMV_CIDADES_EMENDAS", "sharepoint_local", r"^Canal FGTS/(?:Arquivados/)?MCMV_CIDADES_EMENDAS_.*\.xlsx$", "mcmv_cidades", "financiada", "contrato_mcmv_cidades_emendas", "um contrato/operação", "Operacao; NumeroDoContrato; TomadorCodigo", "PMCMV; PF; Cotista; Modalidade; Linha; Programa; SubPrograma", "modalidade/tipo de imóvel; sem status físico de obra na amostra", "financiamento bruto/líquido; FGTS; garantia; compra; renda; descontos FGTS/OGU; contrapartida/parceria", "DataDaContratacao; AnoDoOrcamento; data de referência do arquivo", "município; IBGE; UF; região", "possível dado de contratante conforme colunas adicionais; revisar antes de publicar", "contratação MCMV Cidades/Emendas", "alta", "Planilha com 56 colunas e dicionário; atualização declarada semanal."),
        Family("SFTP_MCMV_CIDADES_EMENDAS", "minio_sftp", r"raw/sftp/.*/MCMV_CIDADES_EMENDAS_.*\.xlsx$", "mcmv_cidades", "financiada", "contrato_mcmv_cidades_emendas", "um contrato/operação; snapshots semanais", "Operacao; NumeroDoContrato; TomadorCodigo", "PMCMV; PF; Cotista; Modalidade; Linha; Programa; SubPrograma", "modalidade/tipo de imóvel", "financiamento; descontos; contrapartida/parceria", "DataDaContratacao; AnoDoOrcamento; data do snapshot", "município; IBGE; UF; região", "possível dado de contratante; revisar", "série SFTP de contratação MCMV Cidades/Emendas", "alta", "Já disponível no MinIO e mais atual que parte do SharePoint local."),
        Family("SFTP_PMCMV_CIDADES_MCID", "minio_sftp", r"raw/sftp/.*/PMCMV_CIDADES_MCID_.*\.(?:csv|xlsx)$", "mcmv_cidades", "financiada", "contrato_ente_publico_mcmv_cidades", "um contrato/ente público", "nu_contrato; nu_ibge", "no_programa", "objeto/programa; sem status físico explícito no inventário estrutural", "valor", "dt_referencia; dt_contratacao; dt_lancamento; dt_posicao; dt_remessa", "município; ente público; UF; IBGE", "não; dados institucionais", "contratos MCMV Cidades MCID", "alta", "Complementa a planilha Emendas com visão de contratos/entes públicos."),
        Family("SP_SUB50_PAINEL", "sharepoint_local", r"^Novo MCMV - FNHIS Sub 50/(?:Arquivados/)?FNHIS SUB 50 Painel_TG_.*\.(?:csv|xlsx)$", "sub50_fnhis", "subsidiada", "proposta_instrumento_fnhis_sub50", "uma proposta/instrumento", "Código Programa; Nº Proposta; Nº Reservado PAC; CNPJ", "Sit. Contratação; Situação Proposta; Situação Instrumento", "Objeto; modalidade; proponente", "VL Repasse Proposta; empenhado; repasse; contrapartida; empenhado acumulado", "Data Consulta; Data Assinatura", "UF; município; região", "CNPJ do proponente; dado institucional", "monitoramento de propostas e instrumentos SUB50/FNHIS", "alta", "Fonte principal para status e valores da linha subsidiada SUB50/FNHIS."),
        Family("SP_SUB50_PROPOSTAS", "sharepoint_local", r"^Novo MCMV - FNHIS Sub 50/todas_propostas_FNHIS_.*\.xlsx$", "sub50_fnhis", "subsidiada", "proposta_fnhis_sub50", "uma proposta", "Número da Proposta; Cod_IBGE", "Situação da Proposta; justificativa de não enquadramento", "proponente; total de UH", "não possui valor na versão amostrada", "data do snapshot no nome", "município; Cod_IBGE", "não; proponente institucional", "cadastro/seleção de propostas SUB50/FNHIS", "alta", "Inclui selecionadas, não enquadradas e justificativas."),
        Family("SP_SUB50_CONSOLIDADO_MINIO", "minio_sharepoint", r"raw/sharepoint/novo_mcmv_fnhis_sub_50_propostas_(?:apresentadas|selecionadas)\.csv$", "sub50_fnhis", "subsidiada", "proposta_fnhis_sub50", "uma proposta", "número da proposta; IBGE", "situação/seleção da proposta", "objeto; proponente; UH", "valores conforme consolidado", "datas da proposta/seleção quando disponíveis", "município; IBGE; UF", "não; dados institucionais", "consolidado de propostas SUB50/FNHIS", "alta", "Objetos consolidados já publicados em raw/sharepoint."),
        Family("DUMP_BB_CONTRATOS_PF_FGTS", "minio_dump", r"raw/dados_historicos/bb_2013_06_junho_pmcmv_18062013_tab_contratos_pf_fgts\.csv$", "classe_media", "financiada", "contrato_pessoa_fisica_fgts", "um contrato PF histórico", "cod_contrato_alien_pf_fgts; cod_operacao; cod_mun_ibge", "cod_sit_contrato_pf; cod_subprograma; cod_linha_aplicacao; faixa", "tipologia; data de entrega", "financiamento; subsídios FGTS/OGU; recursos próprios; avaliação; juros; parcelas", "assinatura; entrega; ano orçamento", "município/IBGE; agente financeiro", "não nesta tabela; relacionável à tabela de beneficiários", "dump histórico de contratos PF FGTS", "alta", "Cobertura pontual de 2013; útil para histórico e validação de layout."),
        Family("DUMP_BB_BENEFICIARIOS_FGTS", "minio_dump", r"raw/dados_historicos/bb_2013_06_junho_pmcmv_18062013_tab_beneficiarios_fgts\.csv$", "classe_media", "financiada", "beneficiario_fgts", "uma pessoa/contrato", "cod_cpf; cod_contrato_alien_pf_fgts; num_cadmut", "titularidade; sexo; estado civil; raça/cor", "endereço da unidade", "renda familiar mensal", "nascimento", "endereço", "sim: CPF, nome, nascimento, endereço e atributos pessoais; acesso estritamente restrito/LGPD", "dump histórico de beneficiários FGTS", "alta", "Não deve ser publicado em camada aberta; relaciona-se aos contratos por contrato."),
        Family("DUMP_BASE_PF_PJ_HISTORICA", "minio_dump", r"raw/dados_historicos/caixa_001_2012_08_agosto_base_pf_e_pj_.*\.csv$", "classe_media", "financiada", "empreendimento_contrato_fgts_historico", "um empreendimento/APF", "codapf; idregistro; CNPJ; IBGE", "percentual de obra; unidades em obra/concluídas/entregues", "nome/endereço; construtora; tipologia; unidades; cronograma", "empréstimo; contrapartida pública; investimento; subsídios FGTS/OGU; liberado", "datas previstas; cronograma; ano", "UF; município; IBGE", "CNPJ e endereço PJ; sem PF na amostra estrutural", "dump histórico PF/PJ com execução de empreendimento", "alta", "Fonte histórica rara com APF, obra, progresso e finanças no mesmo conjunto."),
        Family("GAP_PRO_MORADIA", "lacuna", r"a^", "pro_moradia", "financiada", "não_identificada", "não disponível", "não identificado", "não identificado", "não identificado", "não identificado", "não identificado", "não identificado", "não identificado", "lacuna de fonte", "alta", "Nenhum arquivo/objeto nominalmente identificável como Pró-Moradia foi localizado no SharePoint, MinIO raw/sftp ou dump inventariado."),
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", type=Path, default=Path("local.env"))
    parser.add_argument("--sharepoint-root", type=Path, default=Path("/home/juan-pablo/CIDADES/sharepoint"))
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data-science/dados-historicos-tratamento/docs/evidencias/inventario-linhas-financiadas-mcmv/inventario_dados_basicos_linhas_mcmv.csv"),
    )
    args = parser.parse_args()
    env = load_env(args.env_file)
    client, bucket = make_client(env)
    remote = [(obj.object_name, obj.size or 0) for obj in client.list_objects(bucket, prefix="raw/", recursive=True)]
    local = [
        (path.relative_to(args.sharepoint_root).as_posix(), path.stat().st_size)
        for path in args.sharepoint_root.rglob("*")
        if path.is_file()
    ]

    rows: list[dict[str, object]] = []
    for family in families():
        candidates = local if family.fonte == "sharepoint_local" else remote
        matches = [(name, size) for name, size in candidates if re.search(family.pattern, name, re.IGNORECASE)]
        names = [name for name, _ in matches]
        start, end = extract_periods(names)
        rows.append(
            {
                "inventario_id": family.inventario_id,
                "fonte_origem": family.fonte,
                "bucket_ou_raiz": str(args.sharepoint_root) if family.fonte == "sharepoint_local" else (bucket if family.fonte != "lacuna" else ""),
                "prefixo_ou_filtro": family.pattern,
                "linha_mcmv": family.linha_mcmv,
                "natureza_linha": family.natureza_linha,
                "entidade_basica": family.entidade_basica,
                "granularidade": family.granularidade,
                "classificacao_dado": family.classificacao,
                "qtd_arquivos_ou_objetos": len(matches),
                "tamanho_total_bytes": sum(size for _, size in matches),
                "periodo_snapshot_inicio": start,
                "periodo_snapshot_fim": end,
                "exemplo_caminho": names[-1] if names else "",
                "campos_chave": family.campos_chave,
                "campos_status": family.campos_status,
                "campos_empreendimento": family.campos_empreendimento,
                "campos_financeiros": family.campos_financeiros,
                "campos_temporais": family.campos_temporais,
                "campos_geograficos": family.campos_geograficos,
                "dados_pessoais_lgpd": family.dados_pessoais,
                "disponibilidade": "local" if family.fonte == "sharepoint_local" else ("minio" if matches else "lacuna"),
                "confianca_classificacao": family.confianca,
                "observacao": family.observacao,
            }
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"CSV salvo em: {args.output}")
    print(f"Famílias inventariadas: {len(df)}")
    print(df.groupby(["linha_mcmv", "fonte_origem"], dropna=False).size().to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
