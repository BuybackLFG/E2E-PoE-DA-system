import pandas as pd
import logging
from typing import Optional
import requests

logger = logging.getLogger(__name__)


def parse_items(league: str) -> Optional[pd.DataFrame]:
    """
    Парсит данные уникальных предметов с poe.ninja.
    
    Args:
        league: Имя лиги Path of Exile
        
    Returns:
        DataFrame с данными предметов или None при ошибке получения
    """
    url = f"https://poe.ninja/api/data/itemoverview?league={league}&type=UniqueWeapon"
    
    try:
        logger.info(f"Fetching unique items data for league: {league}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        lines = data.get('lines', [])
        
        result = []
        for line in lines:
            result.append({
                'item_name': line.get('name'),
                'base_type': line.get('baseType'),
                'item_type': line.get('itemType'),
                'level_required': line.get('levelRequired'),
                'chaos_value': line.get('chaosValue'),
                'links': line.get('links'),
                'details_id': line.get('detailsId')
            })
        
        df = pd.DataFrame(result)
        logger.info(f"Successfully parsed {len(df)} unique item entries for league: {league}")
        return df
        
    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching unique items data for league: {league}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error fetching unique items for league {league}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing unique items for league {league}: {e}", exc_info=True)
        return None
