# Pipeline de Dados com Pandas

![Python](https://img.shields.io/badge/Python-3.9-blue) ![Pandas](https://img.shields.io/badge/Pandas-1.3.5-brightgreen)

## 📄 Descrição

Este projeto implementa um pipeline de dados que:
- Importa um arquivo CSV.
- Limpa os dados.
- Gera duas tabelas de resultado.
- Cria um relatório de qualidade dos dados.

## 🎯 Objetivo

1. **Importação:** Carregar um arquivo CSV para processamento.
2. **Limpeza:** Tratar valores faltantes e inconsistências.
3. **Resultados:**
   - **Tabela 1:** `location_region` ordenadas pela média de `risk score`.
   - **Tabela 2:** Top 3 `receiving address` com maior `amount` em transações recentes do tipo `sale`.

## 🛠 Tecnologias

- **Linguagem:** Python 3.9
- **Bibliotecas:** Pandas
- **Ambiente:** Jupyter Notebook
