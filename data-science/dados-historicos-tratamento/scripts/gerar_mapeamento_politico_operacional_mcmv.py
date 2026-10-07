#!/usr/bin/env python3
"""Gera o mapa político-operacional das linhas MCMV a partir das evidências locais."""

from __future__ import annotations

import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs/evidencias/mapeamento-politico-operacional-mcmv"

COLS = [
    "linha", "natureza", "fase_ordem", "fase", "decisao_evento", "ator_responsavel",
    "sistema_canal", "fonte_recurso", "tabela_dado", "papel_do_dado",
    "chave_integracao_candidata", "disponibilidade_verificada", "confianca", "lacuna_observacao",
]


def r(linha, natureza, ordem, fase, evento, ator, sistema, recurso, tabela, papel,
      chave, disponibilidade, confianca="media", lacuna=""):
    return dict(zip(COLS, [linha, natureza, ordem, fase, evento, ator, sistema, recurso,
                           tabela, papel, chave, disponibilidade, confianca, lacuna]))


ROWS = [
    r("MCMV Financiada", "financiada", 1, "Planejamento", "Define metas, regras, renda, juros, tetos e subsídios", "MCID / CCFGTS", "Normativos; PPA; orçamento FGTS", "FGTS + subvenção OGU/Fundo Social quando aplicável", "tpc_2024_fgts_canal_tab_dbo_pc_2024_02_orcamento_final", "Alocação anual/macro do FGTS", "programa; exercício; região", "tabela lógica citada; objeto raw não confirmado", "media", "A planilha orçamentária não prova execução financeira por contrato."),
    r("MCMV Financiada", "financiada", 2, "Oferta", "Construtora/incorporadora estrutura empreendimento", "Construtora / incorporadora", "Sistemas da IF", "Capital de produção + FGTS", "fgts_canal_tab_ao_1_tab_empreendimentos_construtor", "Vínculo empreendimento-construtor", "cod_empreendimento; CNPJ", "tabela lógica citada; objeto raw não confirmado"),
    r("MCMV Financiada", "financiada", 3, "Proposta", "Família escolhe imóvel e apresenta documentação", "Família / correspondente / IF", "SIOPI e sistemas da IF", "Crédito FGTS", "tpc_2024_fgts_canal_tab_dbo_pc_2024_01_financiamento_pf_pj", "Financiamento PF/PJ e enquadramento", "cod_contrato; CPF/CNPJ", "tabela lógica citada; objeto raw não confirmado"),
    r("MCMV Financiada", "financiada", 4, "Enquadramento", "Cruza renda e requisitos da faixa", "Instituição financeira", "SIOPI", "FGTS + desconto elegível", "tpc_2024_fgts_canal_tdom_faixa_renda", "Domínio das faixas", "cod_faixa; vigência", "tabela lógica citada; objeto raw não confirmado"),
    r("MCMV Financiada", "financiada", 5, "Crédito e engenharia", "Avalia capacidade de pagamento e imóvel", "Instituição financeira / engenharia", "SIOPI; sistemas de engenharia", "Crédito FGTS", "fgts_canal_tab_ao_1_operacoes_pj_pf", "Operação PF/PJ", "cod_contrato; cod_empreendimento", "tabela lógica citada; objeto raw não confirmado"),
    r("MCMV Financiada", "financiada", 6, "Contratação", "Partes assinam e registram contrato", "Família / vendedor ou construtora / IF / cartório", "Sistemas contratuais da IF", "FGTS + recursos próprios + subsídio", "fgts_canal_tab_ao_1_contratos_fgts", "Contrato, valor, prazo e situação", "cod_contrato", "tabela lógica citada; objeto raw não confirmado", "alta"),
    r("MCMV Financiada", "financiada", 7, "Desembolso", "IF libera recursos após condições contratuais", "Instituição financeira", "Canal FGTS", "FGTS", "fgts_canal_tab_ao_2_tab_desembolsos_fgts", "Eventos de desembolso", "cod_contrato; data_desembolso", "tabela lógica citada; objeto raw não confirmado", "alta"),
    r("MCMV Financiada", "financiada", 8, "Obra", "Acompanha evolução, paralisação e término", "IF / construtora / MCID", "Canal FGTS", "FGTS", "fgts_canal_tab_ao_2_tab_execucoes_obras | fgts_canal_tab_ao_2_operacoes_paralisadas_fgts_setorpublico | fgts_canal_tab_ao_3_acompanhamento_termino_obra", "Execução, exceções e conclusão", "cod_contrato; cod_empreendimento; posição", "tabelas lógicas citadas; objetos raw não confirmados", "alta"),
    r("MCMV Financiada", "financiada", 9, "Entrega", "Registra UH financiada/entregue", "IF / MCID", "Dados abertos MCMV", "FGTS + subsídio aplicável", "dados_abertos_mcmv_fgts_analitico", "Analítico por UH/contrato", "cod_contrato; cod_empreendimento", "tabela lógica citada; objeto raw não confirmado", "alta", "Confirmar granularidade e a coluna data_entrega_para_pf no esquema real."),
    r("MCMV Financiada", "financiada", 10, "Qualidade", "Monitora qualificação de construtor e materiais", "MCID / PBQP-H", "PBQP-H", "Não é fonte financeira", "dados_abertos_pbqp_h_siac | dados_abertos_pbqp_h_simac", "Indicadores de qualidade", "CNPJ; certificado; validade", "tabelas lógicas citadas; objetos raw não confirmados"),

    r("MCMV Cidades", "financiada com aporte público", 1, "Habilitação", "Ente adere e comprova regularidade no SNHIS", "Estado / município / DF; MCID", "SNHIS", "Ainda sem desembolso", "dados_abertos_snhis_regularidade_entes", "Situação do termo de adesão", "CNPJ do ente; IBGE", "4 objetos raw/sharepoint confirmados", "alta"),
    r("MCMV Cidades", "financiada com aporte público", 2, "Decisão local", "Define público e aprova contrapartida/terreno", "Executivo e Legislativo local", "Lei e atos locais", "Tesouro local, terreno público ou emenda", "—", "Marco decisório político local", "CNPJ/IBGE do ente; lei", "lacuna", "alta", "Legislação autorizativa municipal/estadual não está no acervo federal mapeado."),
    r("MCMV Cidades", "financiada com aporte público", 3, "Emenda/aporte", "Vincula emenda ao benefício habitacional", "Parlamentar / ente / MCID", "Orçamento federal", "OGU por emenda", "orcamento_resultado_emendas | dados_abertos_orcamento_emendas_parlamentares_2024", "Emenda e execução orçamentária", "numero_da_emenda/codigo_da_emenda", "tabelas lógicas citadas; objeto raw individual não confirmado"),
    r("MCMV Cidades", "financiada com aporte público", 4, "Seleção", "Ente indica público elegível ao aporte", "Ente público", "Processo local + Caixa", "Aporte complementar", "novo_mcmv_cidades_emendas", "UH/benefício contratado via emenda", "numero_do_contrato; numero_da_emenda", "tabela lógica citada; 107 snapshots MCMV_CIDADES_EMENDAS confirmados no SFTP", "alta"),
    r("MCMV Cidades", "financiada com aporte público", 5, "Crédito", "Caixa cruza benefício local com capacidade de pagamento", "Caixa", "SIOPI/sistemas Caixa", "FGTS + aporte público", "fgts_canal_tab_ao_1_operacoes_pj_pf", "Operação financiada", "cod_contrato; CPF", "tabela lógica citada; objeto raw não confirmado"),
    r("MCMV Cidades", "financiada com aporte público", 6, "Contratação", "Formaliza contrato e aplica contrapartida", "Caixa / família / ente", "Canal FGTS", "FGTS + emenda/contrapartida/terreno", "dados_abertos_orcamento_uh_emendas_parlamentares | fgts_canal_tab_ao_1_contratos_fgts", "Valor de contrapartida e contrato", "cod_contrato; codigo_da_emenda", "tabelas lógicas citadas; snapshots SFTP confirmados", "alta"),
    r("MCMV Cidades", "financiada com aporte público", 7, "Execução e entrega", "Desembolsa, acompanha obra e entrega", "Caixa / construtora / ente", "Canal FGTS", "FGTS + aporte público", "fgts_canal_tab_ao_2_tab_desembolsos_fgts | fgts_canal_tab_ao_2_tab_execucoes_obras | fgts_canal_tab_ao_3_acompanhamento_termino_obra", "Desembolso, posição e entrega", "cod_contrato; cod_empreendimento", "tabelas lógicas citadas; objetos raw não confirmados"),

    r("Pró-Moradia", "financiamento ao setor público", 1, "Regulamentação", "CCFGTS/MCID definem modalidades e condições", "CCFGTS / MCID", "Normativos Pró-Moradia", "FGTS", "tpc_2024_fgts_canal_tab_dbo_pc_2024_02_orcamento_final", "Limite macro do FGTS", "programa; exercício; região", "tabela lógica citada; objeto raw não confirmado"),
    r("Pró-Moradia", "financiamento ao setor público", 2, "Carta-consulta", "Ente preenche projeto, engenharia e resumo financeiro", "Estado / município / DF", "Planilha SeleHab / correio institucional", "FGTS + contrapartida mínima do ente", "Carta-consulta por modalidade (documento)", "Origem da proposta e orçamento calculado", "identificador carta-consulta; CNPJ; IBGE", "arquivos/documentos citados; estrutura a inventariar", "media", "O orçamento automático é fórmula da planilha bloqueada, não uma tabela transacional."),
    r("Pró-Moradia", "financiamento ao setor público", 3, "Validação", "Verifica faixa, contrapartida, limites e vedações", "Caixa / MCID", "Análise técnica", "FGTS + >=5% ente, conforme modalidade/regra", "fgts_canal_tdom_ao_1_justificativa_de_obra", "Domínio/justificativa de exceções", "cod_justificativa; cod_empreendimento", "tabela lógica citada; objeto raw não confirmado", "media"),
    r("Pró-Moradia", "financiamento ao setor público", 4, "Contratação", "Assina financiamento com ente público", "Agente financeiro / ente", "Canal FGTS", "FGTS", "fgts_site_contratacao_diaria | fgts_canal_tab_ao_1_contratos_fgts", "Contrato e contratação diária", "cod_contrato; CNPJ do tomador", "tabelas lógicas citadas; objeto raw não confirmado", "alta"),
    r("Pró-Moradia", "financiamento ao setor público", 5, "Empreendimento", "Identifica obra, construtor, localização e modalidade", "Ente / agente financeiro", "Canal FGTS", "FGTS + contrapartida", "fgts_canal_tab_ao_1_tab_empreendimentos | fgts_canal_tab_ao_1_tab_empreendimentos_construtor | dados_prioritarios_disponibilizados_snh_empreendimentos", "Cadastro do empreendimento", "cod_empreendimento; cod_contrato", "um objeto histórico tab_empreendimentos confirmado; demais nomes lógicos não confirmados", "alta"),
    r("Pró-Moradia", "financiamento ao setor público", 6, "Acompanhamento", "Registra percentual, desembolso, situação e justificativa", "Caixa / ente / MCID", "Canal FGTS / SFTP", "FGTS", "fgts_canal_tab_ao_1_tab_empreendimentos_posicoes | fgts_sftp_empreendimentos_base_pj | fgts_canal_tab_ao_2_tab_desembolsos_fgts", "Snapshot físico-financeiro", "cod_empreendimento; cod_contrato; data_posicao", "tabelas lógicas citadas; objetos raw individuais não confirmados", "alta", "A PK deve ser testada; provável chave composta empreendimento/contrato/data de posição."),
    r("Pró-Moradia", "financiamento ao setor público", 7, "Beneficiários", "Seleciona famílias antes de 50% da obra quando a modalidade prevê UH/lote", "Ente público", "Cadastro local / mecanismos definidos em norma", "Não altera a fonte do financiamento", "—", "Lista de famílias", "CPF/NIS + empreendimento", "lacuna", "media", "Não foi identificada tabela federal estruturada específica da seleção Pró-Moradia."),

    r("Classe Média", "financiada", 1, "Regra financeira", "Define renda, condições, funding e limites", "MCID / CCFGTS / agente financeiro", "Normativos; orçamento FGTS", "FGTS + recursos da instituição financeira", "tpc_2024_fgts_canal_tab_dbo_pc_2024_02_orcamento_final | tpc_2024_fgts_canal_tdom_faixa_renda", "Orçamento e faixa", "programa; exercício; faixa; vigência", "tabelas lógicas citadas; objetos raw não confirmados", "media", "Validar no normativo vigente a proporção exata de funding; não fixar 50/50 só pela nota."),
    r("Classe Média", "financiada", 2, "Proposta", "Família simula, escolhe imóvel e envia documentos", "Família / Caixa", "SIOPI", "Crédito habitacional", "tpc_2024_fgts_canal_tab_dbo_pc_2024_01_financiamento_pf_pj", "Proposta/financiamento PF", "CPF; cod_proposta/cod_contrato", "tabela lógica citada; objeto raw não confirmado"),
    r("Classe Média", "financiada", 3, "Enquadramento", "Verifica renda e compatibilidade da faixa", "Caixa", "SIOPI", "FGTS + funding do agente", "fgts_sftp_empreendimentos_base_pj", "Compatibilidade de faixa e assinatura", "cod_contrato; cod_empreendimento", "tabela lógica citada; objeto raw individual não confirmado", "alta", "Confirmar coluna txt_compatibilidade_faixa_renda em amostra real."),
    r("Classe Média", "financiada", 4, "Crédito e engenharia", "Analisa capacidade e avalia imóvel/obra", "Caixa / engenharia", "SIOPI", "Crédito", "fgts_canal_tab_ao_1_operacoes_pj_pf", "Operação e vínculo PF/PJ", "cod_contrato; cod_empreendimento", "tabela lógica citada; objeto raw não confirmado"),
    r("Classe Média", "financiada", 5, "Contrato e registro", "Assina e registra contrato", "Família / vendedor/construtora / Caixa / cartório", "Sistemas Caixa", "FGTS + funding do agente + recursos próprios", "fgts_canal_tab_ao_1_contratos_fgts", "Condições contratuais", "cod_contrato", "tabela lógica citada; objeto raw não confirmado", "alta"),
    r("Classe Média", "financiada", 6, "Desembolso", "Banco recebe registro e libera valor", "Caixa", "Canal FGTS", "FGTS + funding do agente", "fgts_canal_tab_ao_2_tab_desembolsos_fgts", "Evento financeiro", "cod_contrato; data_desembolso", "tabela lógica citada; objeto raw não confirmado", "alta"),
    r("Classe Média", "financiada", 7, "Resultado", "Consolida contrato/UH e eventual entrega", "Caixa / MCID", "Dados abertos MCMV", "FGTS", "dados_abertos_mcmv_fgts_analitico", "Analítico MCMV-FGTS", "cod_contrato; cod_empreendimento", "tabela lógica citada; objeto raw não confirmado", "alta"),

    r("FNHIS SUB50", "subsidiada — anexo de fronteira", 1, "Planejamento e seleção", "Publica regras, limites e municípios/propostas selecionados", "MCID", "TransfereGov + atos oficiais", "OGU/FNHIS", "novo_mcmv_fnhis_sub_50_propostas_apresentadas | novo_mcmv_fnhis_sub_50_propostas_selecionadas", "Propostas e seleção", "numero_proposta TransfereGov; IBGE", "família SUB50 confirmada no inventário MinIO", "alta"),
    r("FNHIS SUB50", "subsidiada — anexo de fronteira", 2, "Instrumento", "MCID e prefeitura formalizam instrumento de repasse", "MCID / prefeitura", "TransfereGov", "OGU/FNHIS + contrapartida quando houver", "FNHIS_SEMANAL_MCID", "Situação de proposta/instrumento", "numero_proposta; instrumento; CNPJ; IBGE", "snapshots raw/sharepoint confirmados", "alta"),
    r("FNHIS SUB50", "subsidiada — anexo de fronteira", 3, "Obra e pagamento", "Autoriza início, mede, valida e libera parcelas", "Prefeitura / Caixa / MCID / construtora", "TransfereGov + vistoria Caixa", "OGU/FNHIS", "—", "Boletim, evidência, parecer e liberação", "numero_proposta; instrumento; medição", "lacuna estruturada", "alta", "O fluxo é centrado no TransfereGov, mas engenharia/vistoria e beneficiários usam outros componentes."),
    r("FNHIS SUB50", "subsidiada — anexo de fronteira", 4, "Beneficiários e pós-obra", "Indica, valida, entrega, ocupa e acompanha famílias", "Prefeitura / MCID / assistência social", "SIGDH + CadÚnico", "OGU/FNHIS", "cadunico_tab_familia | cadunico_tab_pessoa", "Elegibilidade e atualização familiar", "NIS/CPF; codigo_familia; IBGE", "tabelas lógicas citadas; objetos raw não confirmados", "media", "Dados pessoais protegidos; usar apenas ambiente autorizado e saída agregada."),
    r("Entidades/FDS", "subsidiada — anexo de fronteira", 1, "Seleção e contratação", "Seleciona proposta da entidade e converte seleção em contrato", "MCID / entidade / Caixa", "Processos MCID/Caixa; atos oficiais", "FDS + OGU quando previsto", "orcamento_resultado_ogu | dados_abertos_mcmv_ogu_beneficiario", "Orçamento/beneficiário contratado", "contrato; empreendimento; CPF/NIS", "tabelas lógicas citadas; objetos raw não confirmados", "media", "Filtrar a ação/modalidade correta; o nome OGU isolado não identifica Entidades."),
    r("Rural/PNHR", "subsidiada/financiada — anexo de fronteira", 1, "Empreendimento e beneficiário", "Consolida projeto e famílias atendidas por agente", "BB / Caixa / MCID", "Sistemas dos agentes", "Conforme grupo/modalidade rural", "int_empreendimentos_int_057_pnhr_bb_pj | int_empreendimentos_int_065_pnhr_caixa_pj | int_beneficiarios_int_058_pnhr_bb_pf | int_beneficiarios_int_064_pnhr_caixa_pf", "Projeto, status e beneficiário", "cod_empreendimento; contrato; CPF/NIS", "tabelas lógicas citadas; objetos raw não confirmados", "media", "Validar coluna de status e vínculo com entrega no esquema real."),
]


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    csv_path = OUT / "mapeamento_politico_operacional_linhas_mcmv.csv"
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLS)
        w.writeheader(); w.writerows(ROWS)

    md = OUT / "mapeamento_politico_operacional_linhas_mcmv.md"
    counts = {}
    for row in ROWS:
        counts[row["linha"]] = counts.get(row["linha"], 0) + 1
    lines = [
        "# Mapeamento político-operacional das linhas MCMV", "",
        "## Conclusão executiva", "",
        "O fluxo do **FNHIS SUB50** é fortemente centrado no **TransfereGov** para proposta, instrumento, medição, comprovação e liberação. Ele não é exclusivamente TransfereGov: a validação/seleção de beneficiários passa pelo SIGDH e CadÚnico, e a verificação física envolve Caixa, prefeitura e assistência social.", "",
        "As quatro linhas financiadas foram mantidas separadas: MCMV Financiada, MCMV Cidades, Pró-Moradia e Classe Média. Entidades/FDS, FNHIS SUB50 e Rural/PNHR aparecem somente no anexo de fronteira.", "",
        "## Como identificar a fonte do dinheiro", "",
        "Não existe uma única tabela suficiente para todos os níveis. Use a combinação:", "",
        "- **Macro/alocação:** `tpc_2024_fgts_canal_tab_dbo_pc_2024_02_orcamento_final` para FGTS; `orcamento_resultado_ogu`/`dados_abertos_orcamento_2024` para ações OGU; `orcamento_resultado_fundo_social` para Fundo Social; tabelas de emendas para MCMV Cidades.",
        "- **Operação/contrato:** `fgts_canal_tab_ao_1_contratos_fgts`, operações PF/PJ e desembolsos. É onde a fonte planejada precisa ser reconciliada com o contrato e o dinheiro efetivamente liberado.",
        "- **Aporte MCMV Cidades:** campos como `vlr_contrapartida_parceria`, `vlr_capital_proprio`, `valor_contrapartida`, emenda e terreno. Terreno é aporte não financeiro e não deve ser somado como desembolso sem regra de valoração.", "",
        "Portanto, `orcamento_resultado_ogu` responde **autorização/dotação**; não responde sozinho **quanto foi gasto**. Para gasto, procure empenhado/liquidado/pago na execução orçamentária e desembolso/liberação no nível contratual.", "",
        "## Chaves de integração recomendadas", "",
        "1. `cod_contrato` como eixo do contrato, preservado como texto.",
        "2. `cod_empreendimento` como eixo físico da obra.",
        "3. `numero_proposta`/instrumento para TransfereGov.",
        "4. `codigo_da_emenda`/`numero_da_emenda` para MCMV Cidades.",
        "5. CNPJ/IBGE para ente; CPF/NIS apenas em ambiente restrito.",
        "6. Posições e desembolsos exigem chave composta com data/snapshot; não declarar PK antes de teste de unicidade.", "",
        "## Disponibilidade observada", "",
        "A consulta de leitura ao bucket `data-lake-mcid/raw` confirmou 107 objetos cujo nome contém MCMV Cidades e quatro arquivos de regularidade SNHIS em `raw/sharepoint`. A maioria dos nomes `fgts_canal_*`, `tpc_*`, `orcamento_resultado_*` e `int_*` é uma referência lógica do banco/dump e não apareceu como objeto individual com o mesmo nome no raw. Isso está explicitado linha a linha no CSV.", "",
        "## Quantidade de etapas", "",
    ]
    lines += [f"- {k}: {v}" for k, v in counts.items()]
    lines += ["", "## Lacunas prioritárias", "",
              "- Confirmar os esquemas reais das tabelas lógicas no dump e testar chaves/PKs.",
              "- Obter execução orçamentária com empenhado, liquidado e pago para não confundir dotação com gasto.",
              "- Inventariar as fórmulas e versões das cartas-consulta Pró-Moradia; `documento de origem` deve guardar arquivo, versão, aba e célula/faixa que gerou o registro.",
              "- Materializar indicadores PPA somente com fórmula, período, fonte e data de corte documentados.",
              "- Tratar CPF, NIS e CadÚnico sob LGPD; produtos de gestão devem ser agregados.", "",
              "## Arquivo detalhado", "",
              "O CSV contém uma linha por etapa, com ator, sistema, fonte de recurso, tabela, chave candidata, evidência de disponibilidade, confiança e lacuna.", ""]
    md.write_text("\n".join(lines), encoding="utf-8")
    print(csv_path)
    print(md)


if __name__ == "__main__":
    main()
