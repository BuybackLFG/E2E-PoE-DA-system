import logging
from typing import Optional
from datetime import datetime
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


def get_latest_league() -> Optional[str]:
    """
    Получает название последней лиги Path of Exile с poewiki.net.
    
    Returns:
        Последнее название лиги Path of Exile или None если не удалось получить.
    """
    url = 'https://www.poewiki.net/wiki/League'
    
    try:
        logger.info(f"Fetching latest league from {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'class': 'cargoTable'})
        
        if table is None:
            logger.error("Could not find league table on poewiki.net")
            return None
        
        rows = table.find_all('tr')

        leagues = []
        for row in rows[1:]:
            cells = row.find_all('td')
            if cells:
                league_name = cells[0].text.strip()
                release_date = cells[1].text.strip()
                leagues.append({'League': league_name, 'Release Date': release_date})

        def parse_date(date_str):
            for fmt in ('%Y-%m-%d %I:%M:%S %p', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            return datetime.min

        if leagues:
            latest = max(leagues, key=lambda x: parse_date(x['Release Date']))
            league_name = latest['League'].split()[0]
            logger.info(f"Successfully retrieved latest league: {league_name}")
            return league_name
        
        logger.warning("No leagues found in poewiki.net table")
        return None
        
    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching league from {url}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error fetching league: {e}")
        return None
    except Exception as e:
        logger.error(f"Error fetching latest league: {e}", exc_info=True)
        return None
