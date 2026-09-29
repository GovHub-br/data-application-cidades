// Parâmetros esperados no Power BI:
// ServidorPostgres (texto), BancoPostgres (texto)

let
    Fonte = PostgreSQL.Database(ServidorPostgres, BancoPostgres),
    ResumoMensal = Fonte{[Schema="ouro", Item="ouro_linha_financiada_resumo_mensal"]}[Data],
    RelatorioSemanal = Fonte{[Schema="ouro", Item="ouro_linha_financiada_relatorio_semanal"]}[Data],
    MapaExecucao = Fonte{[Schema="ouro", Item="ouro_linha_financiada_mapa_execucao"]}[Data],
    Empreendimentos = Fonte{[Schema="ouro", Item="ouro_linha_financiada_empreendimentos"]}[Data],
    Contrapartidas = Fonte{[Schema="ouro", Item="ouro_linha_financiada_contrapartidas"]}[Data],
    ExecucaoOrcamentaria = Fonte{[Schema="ouro", Item="ouro_linha_financiada_execucao_orcamentaria"]}[Data],
    FeaturesPreditivas = Fonte{[Schema="ouro", Item="ouro_linha_financiada_features_preditivas"]}[Data],
    BasePfFundoSocial = Fonte{[Schema="ouro", Item="ouro_linha_financiada_base_pf_fundo_social"]}[Data],
    CoberturaPerguntas = Fonte{[Schema="ouro", Item="ouro_linha_financiada_cobertura_perguntas"]}[Data]
in
    [
        ResumoMensal = ResumoMensal,
        RelatorioSemanal = RelatorioSemanal,
        MapaExecucao = MapaExecucao,
        Empreendimentos = Empreendimentos,
        Contrapartidas = Contrapartidas,
        ExecucaoOrcamentaria = ExecucaoOrcamentaria,
        FeaturesPreditivas = FeaturesPreditivas,
        BasePfFundoSocial = BasePfFundoSocial,
        CoberturaPerguntas = CoberturaPerguntas
    ]
