import logging
from typing import Optional, List, Dict, Tuple
from datetime import datetime
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


# Вспомогательная функция для парсинга дат
def _parse_date(date_str: str) -> datetime:
    """
    Парсит строку даты из poewiki.net в объект datetime.
    Обрабатывает несколько возможных форматов.
    """
    for fmt in ('%Y-%m-%d %I:%M:%S %p', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    # Если ни один формат не подошел, возвращаем очень старую дату
    # Это позволит таким записям быть в конце при сортировке
    return datetime.min


def get_recent_leagues_from_wiki(num_leagues: int = 5) -> List[str]:
    """
    Получает названия последних N лиг Path of Exile с poewiki.net.

    Args:
        num_leagues: Количество последних лиг для получения.
            По умолчанию 5.

    Returns:
        Список названий последних N лиг (отсортированных от новой к старой)
        или пустой список, если не удалось получить.
    """
    url = 'https://www.poewiki.net/wiki/League'

    try:
        logger.info(f"Fetching recent leagues (top {num_leagues}) from {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'class': 'cargoTable'})

        if table is None:
            logger.error("Could not find league table with class 'cargoTable' on poewiki.net")
            return []

        rows = table.find_all('tr')

        all_leagues_data = []
        # Пропускаем заголовочную строку (rows[0])
        for row in rows[1:]:
            cells = row.find_all('td')
            if cells and len(cells) > 1:  # Убедимся, что есть как минимум имя и дата
                league_name_raw = cells[0].text.strip()
                # Извлекаем только основное имя лиги (например, "Keepers" из "Keepers (Hardcore)")
                league_name = league_name_raw.split(' ')[0].split('(')[0].strip()
                release_date_str = cells[1].text.strip()

                all_leagues_data.append({
                    'League': league_name,
                    'Release Date': _parse_date(release_date_str)
                })

        if not all_leagues_data:
            logger.warning("No league data found in poewiki.net table")
            return []

        # Сортируем лиги по дате выпуска в убывающем порядке (самые новые первыми)
        all_leagues_data.sort(key=lambda x: x['Release Date'], reverse=True)

        # Извлекаем уникальные имена лиг и берем N самых новых
        # Это нужно, потому что wiki может перечислять "Standard" или "Hardcore" как отдельные записи
        recent_unique_leagues = []
        seen_league_names = set()
        for league_data in all_leagues_data:
            name = league_data['League']
            if name and name not in seen_league_names:  # Проверяем, что имя не пустое
                recent_unique_leagues.append(name)
                seen_league_names.add(name)
            if len(recent_unique_leagues) >= num_leagues:
                break

        logger.info(f"Successfully retrieved {len(recent_unique_leagues)} recent leagues: {recent_unique_leagues}")
        return recent_unique_leagues

    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching leagues from {url}")
        return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error fetching leagues from {url}: {e}")
        return []
    except Exception as e:
        logger.error(f"Error fetching recent leagues from {url}: {e}", exc_info=True)
        return []


def get_latest_league() -> Optional[str]:
    """
    Получает название самой последней лиги Path of Exile с poewiki.net.
    Это обертка для get_recent_leagues_from_wiki(num_leagues=1).

    Returns:
        Название последней лиги или None, если не удалось получить.
    """
    recent_leagues = get_recent_leagues_from_wiki(num_leagues=1)
    return recent_leagues[0] if recent_leagues else None

