import pandas as pd
import logging
from typing import Optional
import requests

logger = logging.getLogger(__name__)


def parse_cards(league: str) -> Optional[pd.DataFrame]:
    """
    Парсит данные карт гаданий с poe.ninja.
    
    Args:
        league: Имя лиги Path of Exile
        
    Returns:
        DataFrame с данными карт или None при ошибке получения
    """
    url = f"https://poe.ninja/poe1/api/economy/stash/current/item/overview?league={league}&type=DivinationCard"
    
    try:
        logger.info(f"Fetching divination cards data for league: {league}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        lines = data.get('lines', [])
        
        result = []
        for line in lines:
            result.append({
                'league_name': league,
                'card_name': line.get('name'),
                'stack_size': line.get('stackSize'),
                'chaos_value': line.get('chaosValue'),
                'trade_count': line.get('tradeInfo', {}).get('count') if line.get('tradeInfo') else 0,
                'details_id': line.get('detailsId')
            })
        
        df = pd.DataFrame(result)
        logger.info(f"Successfully parsed {len(df)} divination card entries for league: {league}")
        return df
        
    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching divination cards data for league: {league}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error fetching divination cards for league {league}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing divination cards for league {league}: {e}", exc_info=True)
        return None
