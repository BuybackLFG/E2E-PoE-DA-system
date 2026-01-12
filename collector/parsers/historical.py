import pandas as pd
import logging
from typing import Optional, Dict, List
import requests
import io
import zipfile
from datetime import datetime

logger = logging.getLogger(__name__)


def parse_historical_currency(league: str, dump_date: Optional[str] = None) -> Optional[pd.DataFrame]:
    """
    Парсит дампы валюты из poe.ninja (ZIP архив с CSV).
    
    Args:
        league: Имя лиги
        dump_date: format: YYYY-MM-DD. Если None, получает последнюю доступную
        
    Returns:
        DataFrame с данными валюты или None если ошибка
    """
    url = f"https://poe.ninja/poe1/api/data/dumps/dump?name={league}"
    
    try:
        logger.info(f"Fetching historical currency dump for league: {league}")
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        
        # Read ZIP file from response
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
            # Look for currency.csv
            if 'currency.csv' not in zip_ref.namelist():
                logger.warning(f"currency.csv not found in dump for {league}")
                return None
            
            # Read CSV with semicolon separator
            with zip_ref.open('currency.csv') as csv_file:
                df = pd.read_csv(csv_file, sep=';')
                
                # Transform data to match database schema
                result = []
                for _, row in df.iterrows():
                    result.append({
                        'league_name': league,
                        'currency_name': row.get('currencyTypeName'),
                        'details_id': row.get('detailsId'),
                        'chaos_equivalent': row.get('chaosEquivalent'),
                        'pay_value': row.get('pay', {}).get('value') if isinstance(row.get('pay'), dict) else None,
                        'receive_value': row.get('receive', {}).get('value') if isinstance(row.get('receive'), dict) else None,
                        'trade_count': row.get('pay', {}).get('count') if isinstance(row.get('pay'), dict) else 0
                    })
                
                result_df = pd.DataFrame(result)
                logger.info(f"Successfully parsed {len(result_df)} historical currency entries for {league}")
                return result_df
                
    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching historical currency dump for {league}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error fetching historical currency dump for {league}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing historical currency for {league}: {e}", exc_info=True)
        return None


def parse_historical_items(league: str, dump_date: Optional[str] = None) -> Optional[pd.DataFrame]:
    """
    Парсит дампы предметов из poe.ninja (ZIP архив с CSV).
    
    Args:
        league: Имя лиги
        dump_date: format: YYYY-MM-DD. Если None, получает последнюю доступную
        
    Returns:
        DataFrame с данными предметов или None если ошибка
    """
    url = f"https://poe.ninja/poe1/api/data/dumps/dump?name={league}"
    
    try:
        logger.info(f"Fetching historical items dump for league: {league}")
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
            if 'items.csv' not in zip_ref.namelist():
                logger.warning(f"items.csv not found in dump for {league}")
                return None
            
            with zip_ref.open('items.csv') as csv_file:
                df = pd.read_csv(csv_file, sep=';')
                
                result = []
                for _, row in df.iterrows():
                    result.append({
                        'league_name': league,
                        'item_name': row.get('name'),
                        'base_type': row.get('baseType'),
                        'item_type': row.get('itemType'),
                        'level_required': row.get('levelRequired'),
                        'chaos_value': row.get('chaosValue'),
                        'links': row.get('links'),
                        'details_id': row.get('detailsId')
                    })
                
                result_df = pd.DataFrame(result)
                logger.info(f"Successfully parsed {len(result_df)} historical item entries for {league}")
                return result_df
                
    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching historical items dump for {league}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error fetching historical items dump for {league}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing historical items for {league}: {e}", exc_info=True)
        return None


def parse_historical_cards(league: str, dump_date: Optional[str] = None) -> Optional[pd.DataFrame]:
    """
    Парсит дампы карт с poe.ninja (ZIP архив с CSV).
    
    Args:
        league: Name of the league
        dump_date: format: YYYY-MM-DD. Если None, получает последнюю доступную
        
    Returns:
        DataFrame с данными карт или None если ошибка
    """
    url = f"https://poe.ninja/poe1/api/data/dumps/dump?name={league}"
    
    try:
        logger.info(f"Fetching historical divination cards dump for league: {league}")
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        
        # Read ZIP file from response
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
            # Look for items.csv (divination cards are in items.csv)
            if 'items.csv' not in zip_ref.namelist():
                logger.warning(f"items.csv not found in dump for {league}")
                return None
            
            # Read CSV with semicolon separator
            with zip_ref.open('items.csv') as csv_file:
                df = pd.read_csv(csv_file, sep=';')
                
                # Filter for divination cards only
                df_cards = df[df.get('itemType') == 'DivinationCard']
                
                # Transform data to match database schema
                result = []
                for _, row in df_cards.iterrows():
                    result.append({
                        'league_name': league,
                        'card_name': row.get('name'),
                        'stack_size': row.get('stackSize'),
                        'chaos_value': row.get('chaosValue'),
                        'trade_count': row.get('tradeInfo', {}).get('count') if isinstance(row.get('tradeInfo'), dict) else 0,
                        'details_id': row.get('detailsId')
                    })
                
                result_df = pd.DataFrame(result)
                logger.info(f"Successfully parsed {len(result_df)} historical divination card entries for {league}")
                return result_df
                
    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching historical divination cards dump for {league}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error fetching historical divination cards dump for {league}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing historical divination cards for {league}: {e}", exc_info=True)
        return None


