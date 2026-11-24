"""Tratamento dos dados históricos do MCMV (dump do banco antigo), da raw ao cleansed.

Funções puras: recebem DataFrame ou caminho local e não sabem de S3. Quem lê e grava no
lake é a `dados_historicos_dag`, pelo S3Hook."""
