# Fontes do conjuntura

Estado em 26/11/2026. Quando uma fonte mudar, atualize esta tabela junto com o código.

| Indicador (pág.) | Tipo | Fonte | Entrada | Tabela ouro | Calendário |
|---|---|---|---|---|---|
| PIB construção (p1) | JSON, API SIDRA | `servicodados.ibge.gov.br/api/v3/agregados/5932` | `ibge_ingest_dag` | `ouro_conjuntura_pib_construcao_civil_pct` | trimestral; o 3T sai no início de dezembro |
| CBIC lançamentos/vendas (p1) | PDF da coletiva | cbic.org.br/hubdedados | manual: `scripts/database` → `bronze_cbic_lancamentos_vendas` | `ouro_conjuntura_boletim_p1_*` | ~8 semanas após o trimestre |
| Construtoras (p2) | PDF/XLSX de RI | RI da MRV, Cury, Tenda, Direcional, Pacaembu e Plano & Plano | MRV: `lancamentos/vendas_ingest_dag`, mas o ouro lê o manual. Demais: manual | `ouro_conjuntura_balancos_empresas` | prévias ~2 semanas após o trimestre |
| Crédito PF/PJ (p2) | JSON, API SGS | `api.bcb.gov.br/dados/serie/bcdata.sgs.<cód>/dados` (BACEN_SERIES) | `bacen_sgs_ingest_dag` | `ouro_conjuntura_financiamentos_imobiliarios_pf_pj` | mensal, ~2 meses de defasagem |
| Crédito/PIB (p4) | JSON, OData Olinda | `olinda.bcb.gov.br/.../MercadoImobiliario/...` | `bacen_credito_pib_ingest_dag` | `ouro_conjuntura_credito_pib` | mensal, ~3 meses |
| FGTS-PJ (p2) | ZIP/TXT, SFTP `sftp_geavo` | `/GEAVO/Base_PJ_FGTS_AAAAMMDD.zip` | `sftp_ingest_dag` + caminho fixo no sources.yml | `ouro_conjuntura_financiamentos_habitacionais` | mensal (dia 7) |
| FGTS-PF, novo/usado (p4) | ZIP/TXT, SFTP | `/GEAVO/Base_PF_FGTS_AAAAMMDD.zip` | idem | `ouro_conjuntura_uh_condicao_uso` | mensal |
| PF MCMV por faixa, Pró-Cotista (p5) | ZIP com .mdb, SFTP | `/GEAVO/MCAAAAMMDD.zip` (MCidades_CCI_CCA_AAAA.mdb) | idem | `ouro_conjuntura_financiamento_pf_faixa` | semanal |
| Fundo Social (p5) | CSV, SFTP `sftp_mcid` | `/home/fabrica/GEFUS/PMCMV_FAIXA3_MCID_AAAA_MM_DD.csv` | idem | `ouro_conjuntura_fundo_social` | semanal |
| SBPE unidades (p2/p5) | XLSX | abecip.org.br/credito-imobiliario/indicadores/financiamento | `abecip_financiamentos_ingest_dag` | `ouro_conjuntura_uh_condicao_uso`, `..._financiamentos_habitacionais` | mensal |
| Poupança SBPE (p5) | XLSX | abecip.org.br/.../caderneta-de-poupanca | `abecip_poupanca_trimestral_ingest_dag` | `ouro_conjuntura_saldo_poupanca` | mensal |
| SBPE por banco, novo × usado (p3/p4) | PDF em imagem (OCR de outro time) | `abecip.org.br/admin/assets/uploads/anexos/data-abecip-AAAA-MM.pdf` | JSON em `snh/raw/abecip/<AAAA-MM>/` → `abecip_instituicoes_ingest_dag` | `ouro_conjuntura_financiamentos_instituicao` | mensal; um mês basta por trimestre (é acumulado no ano) |
| Novo CAGED (p3) | 7z de microdados, FTP | `ftp.mtps.gov.br/pdet/microdados/NOVO CAGED/<ano>/<ano><mês>/CAGED{MOV,FOR,EXC}*.7z` | `novo_caged_pdet_ingest_dag` | `ouro_conjuntura_empregos_caged` | mensal, ~5 semanas |
| PNAD, PIM-PF, PMC (p3) | JSON, SIDRA | agregados 6323, 6391 e outros (Variable IBGE_CONFIGURACOES) | `ibge_ingest_dag` | `ouro_conjuntura_pnad_*`, `..._producao_fisica` | mensal/trimestral |
| OGU (p6) | ZIP/CSV anexo de e-mail (Tesouro Gerencial) | relatórios agendados, conexão IMAP `tesouro_imap` | `dotacao_execucao_outras_fontes_mcid_ingest_dag` e `orcamento_mcid_por_acao_com_data_ingest_dag` | `ouro_conjuntura_ogu` | diário (quando o agendamento está ativo) |
| SINAPI (p6) | JSON, SIDRA 2296 | IBGE | `ibge_ingest_dag` | `ouro_conjuntura_sinapi` | mensal |
| INCC-M (p6) | JSON, SGS 7456 (variação %) + XLSX do Sinduscon-PR (índice até 07/26) | api.bcb.gov.br · sindusconpr.com.br | `bacen_sgs_ingest_dag` (+ `incc_m_ingest_dag`) | `ouro_conjuntura_incc_m` (índice encadeado na prata) | mensal, fim do mês |
| ICST (p7) | CSV, FGV Dados com login | autenticacao-ibre.fgv.br | `icst_ingest_dag` | `ouro_conjuntura_icst` | mensal, fim do mês |
| FipeZap locação (p7) | XLSX | downloads.fipe.org.br/indices/fipezap/fipezap-serieshistoricas.xlsx | `fipezap_trimestral_ingest_dag` | `ouro_conjuntura_fipezap` | mensal |
| IMOB (p7) | JSON, Alpha Vantage | alphavantage.co TIME_SERIES_DAILY IMOB.SA | `infomoney_imob` | `ouro_conjuntura_indice_imob` | diário |
| Canal FGTS, funding, valor médio, ABRAMAT | planilha ou PDF | manual | `scripts/database` → `bronze_manual_*` | `ouro_conjuntura_canal_fgts`, `_funding`, `_fgts_valor_medio` | — |

Inserção manual: só rode os scripts de `scripts/database` quando o Lucas pedir, e em lote.
