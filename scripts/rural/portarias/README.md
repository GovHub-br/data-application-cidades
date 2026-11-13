# Portarias do MCMV Rural: baixar e extrair os anexos

Extrai as listas de propostas das portarias de **seleção** (354/2024 e 697/2026) e das **46 portarias de autorização de contratação** da Seleção 2023. O resultado é um CSV com uma linha por proposta, que permite contar selecionadas e autorizadas por portaria e por UF.

## Por que em dois passos

O gov.br não abre a partir da VM do Cowork nem da nuvem do Claude (o acesso é bloqueado). Então:

1. **Baixar** roda no seu Windows, onde o gov.br abre normalmente.
2. **Extrair** roda em qualquer lugar com `pdfplumber` (a VM do Cowork já tem).

## Passo 1: baixar (no Windows, na raiz do repositório)

```
python scripts\rural\portarias\baixar_portarias.py
```

- Baixa as 48 portarias do `manifesto_portarias_rural.csv` para `pdf/`, a original e a compilada quando existe.
- Pula o que já foi baixado. Para baixar só uma: `--so 1024_2024`. Só as autorizações: `--papel autorizacao`.
- No fim, lista o que falhou. Esperado:
  - **550/2025:** o link do site aponta para um arquivo `.crdownload`. Baixe à mão e salve como `pdf/550_2025.pdf`.
  - **649, 650 e 1.036 de 2025:** só existem como página do DOU. O script salva o `.html`. Se o DOU bloquear, abra no navegador e use "Salvar como" `pdf/649_2025.html` (ou imprima como PDF e salve `pdf/649_2025.pdf`).

## Passo 2: extrair

```
python scripts/rural/portarias/extrair_portarias.py
```

Usa a compilada quando existe. Acha a tabela pelo cabeçalho (UF, Município, Número de identificação da proposta, EO, CNPJ, UH). Se a página não tiver tabela, lê o texto linha a linha.

Saídas em `saida/`:

| Arquivo | Grão | Para quê |
|---|---|---|
| `propostas_portarias_rural.csv` | proposta × portaria | contar propostas e UH por portaria e por UF |
| `conferencia_portarias_rural.csv` | portaria | ver o que foi lido e os alertas |
| `autorizadas_x_selecionadas.csv` | proposta autorizada | ver se cada autorizada está na 354, com a mesma EO e as mesmas UH |

## Como conferir

Abra `conferencia_portarias_rural.csv` e olhe a coluna `alertas`:

- **nenhuma proposta encontrada:** o PDF pode ser imagem ou ter outro layout. Abra e me mande.
- **soma das UH diferente do total do anexo:** faltou ou sobrou linha.
- **parte lida pelo texto:** confira umas linhas contra o PDF.
- **sem UF / sem CNPJ / sem UH:** a coluna não foi reconhecida.

Depois, compare com uma contagem à mão em pelo menos uma portaria.

## Limites conhecidos

- O extrator foi testado com PDFs de exemplo no mesmo formato do anexo da 354 e da 697, e ainda não com as portarias reais de autorização. Rode primeiro com `--so 354_2024 1024_2024` e confira.
- A chave entre seleção e autorização é o número de identificação da proposta. Ainda falta confirmar que as autorizações trazem esse número.
- As retificações de nomes de municípios (707/2025 e 1.281/2025) não são aplicadas; elas não mudam o id da proposta.
