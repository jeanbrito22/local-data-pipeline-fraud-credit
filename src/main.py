import pandas as pd
import logging
from utils import (
    MANDATORY_COLUMNS,
    OPTIONAL_COLUMNS,
    clean_data,
    quality_report,
    calculate_avg_risk_score_by_region,
    find_top_receiving_addresses,
    summarize_data,
)

# Configuração de logging
log_file = "logs/pipeline_execution.log"  # Caminho para o arquivo de logs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Exibe no console
        logging.FileHandler(log_file, mode="a")  # Salva no arquivo
    ]
)

def main():
    """
    Função principal para executar o pipeline de validação,
    análise de qualidade e criação das tabelas.
    """
    # Caminho do arquivo CSV
    
    # Dados brutos a serem processados.
    input_file = "data/input/df_fraud_credit.csv" 
    # Registros válidos após as validações obrigatórias.
    output_cleaned = "data/output/cleaned_data.csv"
    # Registros inválidos com erros em colunas obrigatórias.
    output_invalid = "logs/invalid_mandatory_data.csv"
    # Registros válidos nas obrigatórias, mas com problemas em colunas opcionais.
    output_optional_issues = "logs/optional_issues.csv" 
    # Relatório com métricas de qualidade de dados.
    quality_report_file = "data/output/quality_report.csv"
    # Tabela com a média de risk_score agrupada por região, em ordem decrescente.
    avg_risk_score_by_region_file = "data/output/avg_risk_score_by_region.csv"
    # Tabela com os 3 principais endereços receptores (receiving_address) por valor.
    top_receiving_addresses_file = "data/output/top_receiving_addresses.csv"

    try:
        # Carregar os dados
        logging.info("Carregando os dados...")
        data = pd.read_csv(input_file, sep=",", on_bad_lines="warn")

        # # Limpar os dados
        logging.info("Limpando e validando os dados...")
        (
            valid_data,
            invalid_data,
            optional_issues
        ) = clean_data(data, MANDATORY_COLUMNS, OPTIONAL_COLUMNS)

        # Salvar os dados limpos e inválidos
        logging.info("Salvando os resultados...")
        valid_data.to_csv(output_cleaned, index=False)
        invalid_data.to_csv(output_invalid, index=False)
        optional_issues.to_csv(output_optional_issues, index=False)

        # Gerar relatório de qualidade
        logging.info("Gerando relatório de qualidade...")
        report = quality_report(data, MANDATORY_COLUMNS, OPTIONAL_COLUMNS)
        report.to_csv(quality_report_file, index=False)

        logging.info("Criando tabelas de resultados...")
        # Criar tabelas
        avg_risk_score_by_region = calculate_avg_risk_score_by_region(valid_data)
        # Salvar tabelas
        avg_risk_score_by_region.to_csv(avg_risk_score_by_region_file, index=False)
        # Criar tabelas
        top_receiving_addresses = find_top_receiving_addresses(valid_data)
        # Salvar tabelas
        top_receiving_addresses.to_csv(top_receiving_addresses_file, index=False)

        logging.info("Processamento concluído com sucesso!")
        logging.info(f"Dados limpos salvos em: {output_cleaned}")
        logging.info(f"Relatório de qualidade salvo em: {quality_report_file}")
        logging.info(f"Tabela de média de risco por região salva em: {avg_risk_score_by_region_file}")
        logging.info(f"Tabela de top receiving addresses salva em: {top_receiving_addresses_file}")

        # Obter métricas
        summary = summarize_data(data, valid_data, invalid_data)

        # Exibir resumo
        logging.info("Resumo do processamento:")
        for key, value in summary.items():
            logging.info(f"{key}: {value}")

    except Exception as e:
        logging.error(f"Erro durante o processamento: {e}")

if __name__ == "__main__":
    main()
