import pandas as pd
import logging
from typing import Optional
import requests

logger = logging.getLogger(__name__)


def parse_currency(league: str) -> Optional[pd.DataFrame]:
    """
    Парсит данные валюты с poe.ninja.
    
    Args:
        league: Имя лиги Path of Exile
        
    Returns:
        DataFrame с данными валюты или None при ошибке получения
    """
    url = f"https://poe.ninja/api/data/currencyoverview?league={league}&type=Currency"
    
    try:
        logger.info(f"Fetching currency data for league: {league}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        lines = data.get('lines', [])
        
        result = []
        for line in lines:
            raw_pay_value = line.get('pay', {}).get('value')
            inverse_pay_value = None
            if raw_pay_value is not None and raw_pay_value != 0:
                inverse_pay_value = 1 / raw_pay_value
            result.append({
                'currency_name': line.get('currencyTypeName'),
                'details_id': line.get('detailsId'),
                'chaos_equivalent': line.get('chaosEquivalent'),
                'pay_value': inverse_pay_value,
                'receive_value': line.get('receive', {}).get('value') if line.get('receive') else None,
                'trade_count': line.get('pay', {}).get('count') if line.get('pay') else 0
            })
        
        df = pd.DataFrame(result)
        logger.info(f"Successfully parsed {len(df)} currency entries for league: {league}")
        return df
        
    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching currency data for league: {league}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error fetching currency data for league {league}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing currency for league {league}: {e}", exc_info=True)
        return None
