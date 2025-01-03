import pandas as pd
from validations import (
    validate_timestamp,
    validate_address,
    validate_amount,
    validate_transaction_type,
    validate_location_region,
    validate_ip_prefix,
    validate_login_frequency,
    validate_session_duration,
    validate_purchase_pattern,
    validate_age_group,
    validate_anomaly,
    validate_risk_score
)
# Dicionários de colunas obrigatórias e opcionais
MANDATORY_COLUMNS = {
    'timestamp': validate_timestamp,
    'amount': validate_amount,
    'sending_address': validate_address,
    'receiving_address': validate_address,
    'transaction_type': validate_transaction_type,
    'location_region': validate_location_region,
    'risk_score': validate_risk_score
}

OPTIONAL_COLUMNS = {
    'ip_prefix': validate_ip_prefix,
    'login_frequency': validate_login_frequency,
    'session_duration': validate_session_duration,
    'purchase_pattern': validate_purchase_pattern,
    'age_group': validate_age_group,
    'anomaly': validate_anomaly
}

def clean_data(data, mandatory_validations, optional_validations):
    """
    Limpa e valida um DataFrame baseado em colunas obrigatórias e opcionais.
    
    Args:
        data (pd.DataFrame): O DataFrame de entrada contendo os dados.
        mandatory_validations (dict): Dicionário onde as chaves são colunas obrigatórias e os valores são funções de validação.
        optional_validations (dict): Dicionário onde as chaves são colunas opcionais e os valores são funções de validação.
    
    Returns:
        valid_data (pd.DataFrame): Registros válidos para todas as colunas obrigatórias.
        invalid_data (pd.DataFrame): Registros com problemas nas colunas obrigatórias.
        optional_issues (pd.DataFrame): Registros válidos nas obrigatórias, mas com problemas nas opcionais.
    """
    # Validar colunas obrigatórias
    valid_rows = pd.Series(True, index=data.index)
    for column, validation_fn in mandatory_validations.items():
        if column in data:
            valid_rows &= data[column].apply(validation_fn)
        else:
            raise KeyError(f"Coluna obrigatória '{column}' não encontrada no DataFrame.")

    # Separar registros válidos e inválidos baseados nas obrigatórias
    valid_data = data[valid_rows].copy()
    invalid_data = data[~valid_rows].copy()

    # Validar colunas opcionais
    optional_issues = pd.DataFrame(columns=data.columns)
    for column, validation_fn in optional_validations.items():
        if column in data:
            invalid_optional = valid_data[~valid_data[column].apply(validation_fn)]
            optional_issues = pd.concat([optional_issues, invalid_optional], ignore_index=True)

    # Remover duplicatas de optional_issues
    optional_issues = optional_issues.drop_duplicates()

    return valid_data, invalid_data, optional_issues


def quality_report(data, mandatory_validations, optional_validations):
    """
    Gera um relatório de qualidade para um DataFrame com base nas dimensões de qualidade.

    Completude: Percentual de valores preenchidos em cada coluna.
    Conformidade: Percentual de valores válidos em relação ao total.
    Consistência: Identificação de discrepâncias em relação a padrões esperados.
    Acurácia: Relacionada à conformidade, mas validando valores em um contexto lógico.
    Unicidade: Número de valores únicos em cada coluna.
    
    Args:
        data (pd.DataFrame): O DataFrame a ser analisado.
        mandatory_validations (dict): Dicionário de validações para colunas obrigatórias.
        optional_validations (dict): Dicionário de validações para colunas opcionais.
    
    Returns:
        pd.DataFrame: Relatório de qualidade contendo métricas para cada coluna.
    """
    report = []

    # Combinar todas as validações
    all_validations = {**mandatory_validations, **optional_validations}

    for column, validation_fn in all_validations.items():
        if column in data:
            # Total de registros
            total_records = len(data)

            # Valores preenchidos
            filled_values = data[column].notna().sum()
            completeness = (filled_values / total_records) * 100

            # Valores válidos
            valid_values = data[column].apply(validation_fn).sum()
            conformity = (valid_values / total_records) * 100

            # Valores únicos
            unique_values = data[column].nunique()

            # Adicionar ao relatório
            report.append({
                "Column": column,
                "Total Records": total_records,
                "Filled Values": filled_values,
                "Completeness (%)": round(completeness, 2),
                "Valid Values": valid_values,
                "Conformity (%)": round(conformity, 2),
                "Unique Values": unique_values,
            })
        else:
            report.append({
                "Column": column,
                "Total Records": len(data),
                "Filled Values": 0,
                "Completeness (%)": 0.0,
                "Valid Values": 0,
                "Conformity (%)": 0.0,
                "Unique Values": 0,
            })

    # Converter para DataFrame
    return pd.DataFrame(report)

def calculate_avg_risk_score_by_region(data):
    """
    Calcula a média do 'risk_score' por 'location_region' em ordem decrescente.
    """
    data['risk_score'] = pd.to_numeric(data['risk_score'], errors='coerce')
    return (
        data.groupby("location_region", as_index=False)["risk_score"]
        .mean()
        .sort_values("risk_score", ascending=False)
    )

def find_top_receiving_addresses(data):
    """
    Lista os 3 principais 'receiving_address' com maior 'amount'.
    """
    data["amount"] = pd.to_numeric(data["amount"], errors="coerce")
    data["timestamp"] = pd.to_datetime(data["timestamp"], errors="coerce")

    filtered_data = data[data["transaction_type"] == "sale"].dropna(subset=["amount", "timestamp"])
    grouped = filtered_data.groupby("receiving_address", as_index=False).agg(
        max_amount=("amount", "max"), latest_timestamp=("timestamp", "max")
    )
    return grouped.nlargest(3, "max_amount")

def summarize_data(data, valid_data, invalid_data):
    """
    Gera um resumo simplificado do processamento de dados.
    """
    return {
        "Total Records": len(data),
        "Inserted Records": len(valid_data),
        "Error Records": len(invalid_data),
    }
