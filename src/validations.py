import re
from datetime import datetime

def validate_timestamp(timestamp):
    """
    Valida se o timestamp é:
    - Um número inteiro.
    - Maior ou igual a 0 (não negativo).
    - A partir de 1º de janeiro de 1970 (UNIX epoch).
    """
    try:
        timestamp = int(timestamp)  # Garantir que é um número inteiro
        if timestamp < 0:
            return False  # Não permitir valores negativos
        return True
    except (ValueError, OverflowError, TypeError):
        return False

def validate_address(address):
            """
            Verifica se o endereço segue o padrão esperado:
            - Começa com '0x'
            - Contém 40 caracteres hexadecimais após '0x'
            """
            pattern = r"^0x[a-fA-F0-9]{40}$"
            return bool(re.match(pattern, address))

def validate_amount(amount):
    """
    Valida se o valor da coluna 'amount' é:
    - Um número (float ou int).
    - Maior que 0 (positivo).
    """
    try:
        # Converter o valor para float
        value = float(amount)
        return value > 0  # Garantir que o valor é positivo
    except (ValueError, TypeError):
        return False

def validate_transaction_type(transaction_type):
    """
    Valida se o valor de 'transaction_type' está na lista de valores permitidos.
    """
    allowed_values = ['transfer', 'purchase', 'sale', 'phishing', 'scam']
    return transaction_type in allowed_values


def validate_location_region(location_region):
    """
    Valida se o valor de 'location_region' está na lista de valores permitidos.
    """
    allowed_values = ['Europe', 'South America', 'Asia', 'Africa', 'North America', 'Oceania']
    return location_region in allowed_values


def validate_ip_prefix(ip):
    """
    Valida a coluna ip_prefix, considerando os critérios definidos.
    
    Args:
        ip_prefix_list (list): Lista de valores para validação.
    
    Returns:
        list: Lista de valores válidos.
    """
    # Regex para validar prefixos IP nos formatos especificados
    regex = r'^(\d+|\d+\.\d+|\d+\.\d+\.\d+)$'
    
    # Filtra os valores válidos
    
    return bool(re.match(regex, str(ip)))


def validate_login_frequency(login_frequency):
    """
    Valida se o valor de 'login_frequency' é um número inteiro.
    """
    try:
        return int(login_frequency) == float(login_frequency)
    except (ValueError, TypeError):
        return False


def validate_session_duration(session_duration):
    """
    Valida se o valor de 'session_duration' é um número inteiro.
    """
    try:
        return int(session_duration) == float(session_duration)
    except (ValueError, TypeError):
        return False

def validate_purchase_pattern(purchase_pattern):
    """
    Valida se o valor de 'purchase_pattern' está na lista de valores permitidos.
    """
    allowed_values = ['focused', 'high_value', 'random']
    return purchase_pattern in allowed_values

def validate_age_group(age_group):
    """
    Valida se o valor de 'age_group' está na lista de valores permitidos.
    """
    allowed_values = ['established', 'veteran', 'new']
    return age_group in allowed_values

def validate_anomaly(anomaly):
    """
    Valida se o valor de 'anomaly' está na lista de valores permitidos.
    """
    allowed_values = ['low_risk', 'moderate_risk', 'high_risk']
    return anomaly in allowed_values

def validate_risk_score(risk_score):
    """
    Valida se o valor de 'risk_score' é um número float e está no intervalo [0, 100].
    """
    try:
        # Tenta converter para float e verifica o intervalo
        risk_score = float(risk_score)
        return 0 <= risk_score <= 100
    except (ValueError, TypeError):
        return False