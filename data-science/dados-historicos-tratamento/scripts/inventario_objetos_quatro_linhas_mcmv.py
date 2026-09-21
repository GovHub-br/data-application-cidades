#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd
from minio import Minio


LOCAL_EXTRA = (
    "Super Recentes/Base_PF_FGTS_20250807.txt",
    "Super Recentes/Base_PF_FGTS_20250907.txt",
)


def load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        raw = raw.strip()
        if raw and not raw.startswith("#") and "=" in raw:
            key, value = raw.split("=", 1)
            values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def period_from_name(name: str) -> str:
    matches = re.findall(r"(?<!\d)(20\d{2})[_-]?(0[1-9]|1[0-2])(?:[_-]?([0-3]\d))?(?!\d)", name)
    if not matches:
        return ""
    year, month, day = matches[-1]
    return f"{year}-{month}-{day}" if day else f"{year}-{month}"


def source_from_object(name: str) -> str:
    if name.startswith("raw/sftp/"):
        return "minio_sftp"
    if name.startswith("raw/dados_historicos/"):
        return "minio_dump"
    if name.startswith("raw/sharepoint/"):
        return "minio_sharepoint"
    return "minio_outro"


def classify(name: str) -> dict[str, str] | None:
    low = name.lower()
    common_pf = {
        "linha_principal": "mcmv_financiada",
        "linhas_aplicaveis": "mcmv_financiada;classe_media",
        "natureza": "financiada",
        "entidade_basica": "contrato_pessoa_fisica",
        "granularidade": "contrato/operação PF por snapshot",
        "campos_chave_esperados": "NU_APF; NU_CONTRATO; CPF/CNPJ mutuário; IBGE",
        "campos_status_esperados": "FAIXA; ORIGEM; TP_ORCAMENTO; COTISTA; tipo de imóvel; amortização",
        "campos_basicos_esperados": "assinatura; movimento; agente; município; faixa; compra; renda; empréstimo; descontos/equilíbrio FGTS e OGU; juros",
        "dados_pessoais_lgpd": "sim: nome, CPF/CNPJ, nascimento, sexo, estado civil e CEP",
        "fundamentacao": "Base PF FGTS; Classe Média é identificada em TP_ORCAMENTO e/ou faixa, não apenas pelo nome do arquivo.",
    }
    if re.search(r"raw/sharepoint/canal fgts/mc\d{8}\.zip$", low):
        return {
            "familia_dado": "cip_canal_fgts",
            "classificacao_dado": "snapshot CIP Canal FGTS com bancos Access",
            "linha_principal": "mcmv_financiada",
            "linhas_aplicaveis": "mcmv_financiada;mcmv_cidades;pro_moradia;classe_media",
            "natureza": "financiada",
            "entidade_basica": "pacote_bancos_access_fgts",
            "granularidade": "pacote de referência com tabelas relacionais",
            "campos_chave_esperados": "cod_linha; cod_contrato; cod_empreendimento",
            "campos_status_esperados": "situação do contrato; execução; posição; término",
            "campos_basicos_esperados": "domínios; contratos; empreendimentos; obras; desembolsos; paralisações",
            "dados_pessoais_lgpd": "variável por tabela; restringir tabelas analíticas PF",
            "fundamentacao": "MC20260306.zip contém MCidades_AO_1, AO_2, AO_3 e CCI_CCA; linha 26 = HAB / PRO-MORADIA.",
        }
    if re.search(r"base_pf_fgts", low):
        return {"familia_dado": "base_pf_fgts", "classificacao_dado": "financiamentos PF FGTS", **common_pf}
    if re.search(r"base_pj_fgts", low):
        return {
            "familia_dado": "base_pj_fgts",
            "classificacao_dado": "empreendimentos/obras PJ FGTS",
            "linha_principal": "mcmv_financiada",
            "linhas_aplicaveis": "mcmv_financiada",
            "natureza": "financiada",
            "entidade_basica": "empreendimento_apf",
            "granularidade": "empreendimento/APF por snapshot",
            "campos_chave_esperados": "NU_APF; CGC/CNPJ; agente financeiro; IBGE",
            "campos_status_esperados": "SITUACAO_OBRA; SITUACAO_CONTRATO; PERCENTUAL_OBRA_REALIZADO",
            "campos_basicos_esperados": "nome empreendimento; construtora; unidades financiadas/concluídas/entregues; início/término; valores; endereço; geolocalização",
            "dados_pessoais_lgpd": "não na estrutura amostrada; contém dados de pessoa jurídica",
            "fundamentacao": "Base PJ FGTS contém APF, empreendimento, execução e status físico/contratual.",
        }
    if re.search(r"(?:p?mcmv)[ _-]?cidades", low) or "novo_mcmv_cidades" in low:
        return {
            "familia_dado": "mcmv_cidades",
            "classificacao_dado": "contratos/operações MCMV Cidades",
            "linha_principal": "mcmv_cidades",
            "linhas_aplicaveis": "mcmv_cidades",
            "natureza": "financiada",
            "entidade_basica": "contrato_operacao_ente_publico",
            "granularidade": "contrato/operação ou agregado municipal por snapshot",
            "campos_chave_esperados": "número contrato/operação; tomador/ente público; IBGE",
            "campos_status_esperados": "programa; subprograma; modalidade; linha; PF/PMCMV/cotista",
            "campos_basicos_esperados": "contratação; município; ente; valores de financiamento, FGTS, descontos e contrapartidas",
            "dados_pessoais_lgpd": "predominantemente institucional; revisar colunas adicionais",
            "fundamentacao": "Nome e dicionário identificam MCMV Cidades/Emendas e contratos com entes públicos.",
        }
    if re.search(r"pro[ _-]?moradia|promoradia", low):
        return {
            "familia_dado": "pro_moradia",
            "classificacao_dado": "operações Pró-Moradia",
            "linha_principal": "pro_moradia",
            "linhas_aplicaveis": "pro_moradia",
            "natureza": "financiada",
            "entidade_basica": "operacao_ente_publico",
            "granularidade": "operação/contrato",
            "campos_chave_esperados": "contrato/operação; ente; IBGE",
            "campos_status_esperados": "situação contratual e execução",
            "campos_basicos_esperados": "empreendimento/intervenção; valores; datas; município; status",
            "dados_pessoais_lgpd": "esperado institucional",
            "fundamentacao": "Identificação nominal Pró-Moradia no caminho/arquivo.",
        }
    if re.search(r"sub[ _-]?50|fnhis", low):
        return {
            "familia_dado": "fnhis_sub50",
            "classificacao_dado": "propostas/instrumentos FNHIS SUB50",
            "linha_principal": "sub50_fnhis",
            "linhas_aplicaveis": "sub50_fnhis",
            "natureza": "subsidiada",
            "entidade_basica": "proposta_instrumento_fnhis",
            "granularidade": "proposta/instrumento ou snapshot de acompanhamento",
            "campos_chave_esperados": "código programa; número proposta; instrumento/PAC; CNPJ; IBGE",
            "campos_status_esperados": "situação contratação; situação proposta; situação instrumento; enquadramento/seleção",
            "campos_basicos_esperados": "proposta; objeto; proponente; UH; repasse; empenho; contrapartida; assinatura; município; UF",
            "dados_pessoais_lgpd": "predominantemente institucional; CNPJ de proponente",
            "fundamentacao": "Nome/caminho identifica FNHIS ou SUB50, linha subsidiada do MCMV.",
        }
    if re.search(r"fgts.*analitico|dados_abertos.*fgts.*analitico", low):
        return {"familia_dado": "fgts_analitico", "classificacao_dado": "dados abertos analíticos FGTS", **common_pf}
    if re.search(r"fgts.*sintetico|dados_abertos.*fgts.*sintetico", low):
        return {
            "familia_dado": "fgts_sintetico",
            "classificacao_dado": "agregado municipal/anual FGTS",
            "linha_principal": "mcmv_financiada",
            "linhas_aplicaveis": "mcmv_financiada;classe_media",
            "natureza": "financiada",
            "entidade_basica": "agregado_municipal_fgts",
            "granularidade": "município/ano",
            "campos_chave_esperados": "IBGE; ano financiamento; data referência",
            "campos_status_esperados": "não possui status individual",
            "campos_basicos_esperados": "UH financiadas; valor financiamento; valor subsídio; município; UF",
            "dados_pessoais_lgpd": "não",
            "fundamentacao": "Arquivo sintético FGTS; serve a indicadores agregados das linhas financiadas.",
        }
    if re.search(r"fgts.*contrat|contrat.*fgts|fgts_canal_|fgts_site_", low):
        return {
            "familia_dado": "fgts_contratacao",
            "classificacao_dado": "contratação/status FGTS",
            "linha_principal": "mcmv_financiada",
            "linhas_aplicaveis": "mcmv_financiada;classe_media",
            "natureza": "financiada",
            "entidade_basica": "contrato_ou_agregado_contratacao_fgts",
            "granularidade": "contrato ou município/data conforme arquivo",
            "campos_chave_esperados": "contrato/APF ou IBGE/data",
            "campos_status_esperados": "situação do contrato; contratação/posição",
            "campos_basicos_esperados": "contratação; agente; município; unidades; valores; situação",
            "dados_pessoais_lgpd": "variável; revisar antes de disponibilizar",
            "fundamentacao": "Nome identifica contratação ou situação contratual FGTS.",
        }
    if name.startswith("raw/dados_historicos/") and re.search(r"beneficiarios_fgts|contratos_pf_fgts|base_pf_e_pj", low):
        info = {"familia_dado": "dump_fgts_historico", "classificacao_dado": "dump histórico FGTS", **common_pf}
        if "base_pf_e_pj" in low:
            info.update(
                entidade_basica="empreendimento_apf_historico",
                granularidade="empreendimento/APF",
                campos_status_esperados="percentual de obra; unidades em obra/concluídas/entregues",
                campos_basicos_esperados="APF; empreendimento; construtora; município; cronograma; valores; subsídios; progresso",
            )
        return info
    return None


def make_row(location: str, size: int, origin: str) -> dict[str, object] | None:
    info = classify(location)
    if info is None:
        return None
    return {
        "registro_tipo": "dado",
        "origem_fisica": origin,
        "localizacao": location,
        "nome_arquivo": Path(location).name,
        "extensao": Path(location).suffix.lower(),
        "tamanho_bytes": size,
        "periodo_snapshot": period_from_name(location),
        "disponibilidade": "minio" if origin.startswith("minio_") else "somente_local_pendente_minio",
        "arquivo_container": "",
        "tabela_interna": "",
        "linhas_registros": "",
        "filtro_linha_validado": "",
        **info,
        "confianca_classificacao": "alta" if info["familia_dado"] in {"base_pf_fgts", "base_pj_fgts", "mcmv_cidades", "pro_moradia"} else "média",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", type=Path, default=Path("local.env"))
    parser.add_argument("--sharepoint-root", type=Path, default=Path("/home/juan-pablo/CIDADES/sharepoint"))
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data-science/dados-historicos-tratamento/docs/evidencias/inventario-linhas-mcmv-com-sub50/inventario_objetos_mcmv_financiada_cidades_pro_moradia_classe_media_sub50_fnhis.csv"),
    )
    args = parser.parse_args()
    env = load_env(args.env_file)
    client = Minio(
        env["MINIO_ENDPOINT"],
        access_key=env["MINIO_ACCESS_KEY"],
        secret_key=env["MINIO_SECRET_KEY"],
        secure=env.get("MINIO_SECURE", "false").lower() in {"1", "true", "yes", "sim"},
    )
    bucket = env["MINIO_BUCKET"]
    rows: list[dict[str, object]] = []
    for obj in client.list_objects(bucket, prefix="raw/", recursive=True):
        row = make_row(obj.object_name, obj.size or 0, source_from_object(obj.object_name))
        if row:
            rows.append(row)

    cip_inventory = args.output.parent / "cip_mc20260306_inventario_tabelas.csv"
    if cip_inventory.is_file():
        cip = pd.read_csv(cip_inventory, dtype=str, keep_default_na=False)
        relevant = {
            "Linha": "domínio de linhas; código 26 = HAB / PRO-MORADIA",
            "tab_contratos_fgts": "contratos filtráveis por cod_linha=26",
            "tab_empreendimentos": "empreendimento ligado ao contrato por cod_empreendimento",
            "tab_Empreendimentos_Construtor": "construtor ligado por cod_empreendimento",
            "tab_empreendimentos_GPS": "características/localização ligadas por cod_empreendimento",
            "tab_empreendimentos_posicoes": "posição física ligada por cod_empreendimento",
            "OperaçõesPJ_PF": "objetivo/tipo ligado por cod_linha e cod_objetivo",
            "SituaçãoDoContrato": "domínio ligado por cod_situacao_contrato",
            "Modalidade": "domínio ligado por cod_modalidade",
            "Municípios": "município/IBGE ligado por cod_municipio",
            "Entidades": "ente/agente ligado pelos códigos da entidade",
            "tab_execucoes_obras": "execução ligada por cod_contrato",
            "operações_paralisadas_FGTS_setorpublico": "paralisação ligada por cod_contrato",
            "tab_desembolsos_fgts": "desembolso ligado por cod_contrato",
            "acompanhamento_termino_obra": "término/entrega ligado pelo número do contrato",
        }
        for item in cip.to_dict("records"):
            table = item["tabela_access"]
            if table not in relevant:
                continue
            rows.append({
                "registro_tipo": "tabela_interna_zip",
                "origem_fisica": "minio_sharepoint_zip",
                "localizacao": f"{item['objeto_minio']}::{item['arquivo_interno']}::{table}",
                "nome_arquivo": table,
                "extensao": ".mdb/tabela",
                "tamanho_bytes": 0,
                "periodo_snapshot": "2026-03-06",
                "disponibilidade": "minio_interno_zip_validado",
                "arquivo_container": item["arquivo_interno"],
                "tabela_interna": table,
                "linhas_registros": item["linhas_total"],
                "filtro_linha_validado": "cod_linha=26" if table in {"Linha", "tab_contratos_fgts", "OperaçõesPJ_PF"} else "join a partir dos 686 contratos cod_linha=26",
                "familia_dado": "cip_pro_moradia",
                "classificacao_dado": relevant[table],
                "linha_principal": "pro_moradia",
                "linhas_aplicaveis": "pro_moradia",
                "natureza": "financiada",
                "entidade_basica": "contrato/empreendimento/evento/domínio",
                "granularidade": "conforme tabela Access",
                "campos_chave_esperados": item["chave_pro_moradia"],
                "campos_status_esperados": "situação contratual; posição; execução; paralisação; término",
                "campos_basicos_esperados": item["colunas"],
                "dados_pessoais_lgpd": "predominantemente institucional; verificar tabela antes de publicação",
                "fundamentacao": "Tabela extraída e inspecionada no CIP MC20260306; 686 contratos têm cod_linha=26.",
                "confianca_classificacao": "alta",
            })
    for relative in LOCAL_EXTRA:
        path = args.sharepoint_root / relative
        if not path.is_file():
            raise SystemExit(f"Arquivo local adicional não encontrado: {path}")
        row = make_row(relative, path.stat().st_size, "sharepoint_local_extra")
        assert row is not None
        row["linhas_registros"] = 10_250_828 if "20250807" in relative else 10_305_632
        row["filtro_linha_validado"] = "classificar por TP_ORCAMENTO/FAIXA; arquivo PF não possui cod_linha"
        rows.append(row)
    if not any(row["linha_principal"] == "pro_moradia" for row in rows):
        rows.append(
            {
                "registro_tipo": "lacuna",
                "origem_fisica": "minio_consultado",
                "localizacao": "",
                "nome_arquivo": "",
                "extensao": "",
                "tamanho_bytes": 0,
                "periodo_snapshot": "",
                "disponibilidade": "não_localizado",
                "familia_dado": "pro_moradia",
                "classificacao_dado": "lacuna de fonte Pró-Moradia",
                "linha_principal": "pro_moradia",
                "linhas_aplicaveis": "pro_moradia",
                "natureza": "financiada",
                "entidade_basica": "não identificada",
                "granularidade": "não disponível",
                "campos_chave_esperados": "contrato/operação; ente; IBGE",
                "campos_status_esperados": "situação contratual e execução",
                "campos_basicos_esperados": "empreendimento/intervenção; valores; datas; município; status",
                "dados_pessoais_lgpd": "não avaliado",
                "fundamentacao": "Nenhum objeto nominalmente identificável como Pró-Moradia foi localizado em raw/ no MinIO.",
                "confianca_classificacao": "alta",
            }
        )
    df = pd.DataFrame(rows)
    for line in ("mcmv_financiada", "mcmv_cidades", "pro_moradia", "classe_media", "sub50_fnhis"):
        df[f"usa_{line}"] = df["linhas_aplicaveis"].str.split(";").map(lambda values: line in values)
    df = df.sort_values(["linha_principal", "familia_dado", "origem_fisica", "localizacao"])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"CSV salvo em: {args.output}")
    print(f"Registros: {len(df)}")
    print(df.groupby(["linha_principal", "origem_fisica"], dropna=False).size().to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
