import pandas as pd
import logging
from typing import Optional, Dict, List, Set
import requests
import io
import zipfile
from datetime import datetime
import os  # Импортируем модуль os для работы с файловой системой

logger = logging.getLogger(__name__)

# Общий заголовок User-Agent для всех запросов
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36'
}


# --- НОВАЯ ФУНКЦИЯ ДЛЯ ПОИСКА ПРАВИЛЬНОГО ИМЕНИ ФАЙЛА ВНУТРИ ZIP ---
def _find_csv_filename_in_zip(zip_namelist: List[str], league_name: str, file_type: str) -> Optional[str]:
    """
    Ищет наиболее подходящее имя CSV-файла внутри ZIP-архива.
    Приоритет:
    1. Точное совпадение: "{league_name}.{file_type}.csv"
    2. Если league_name - это "Standard" или "Hardcore", то просто "{file_type}.csv"
    3. Если league_name содержит "Hardcore", то "Hardcore {league_name}.{file_type}.csv" (для случаев типа "Hardcore Settlers")
    """
    # 1. Поиск точного совпадения
    exact_match = f"{league_name}.{file_type}.csv"
    if exact_match in zip_namelist:
        return exact_match

    # 2. Обработка "Standard" и "Hardcore" лиг, которые могут быть без префикса
    if league_name.lower() == "standard" and f"{file_type}.csv" in zip_namelist:
        return f"{file_type}.csv"
    if league_name.lower() == "hardcore" and f"{file_type}.csv" in zip_namelist:
        return f"{file_type}.csv"

    # 3. Поиск для лиг типа "Hardcore X"
    if "hardcore" in league_name.lower():
        hc_match = f"Hardcore {league_name.replace('Hardcore ', '')}.{file_type}.csv"
        if hc_match in zip_namelist:
            return hc_match

    # 4. Если ничего не найдено, возвращаем None
    return None


def parse_historical_currency(league: str, dump_date: Optional[str] = None) -> Optional[pd.DataFrame]:
    """
    Парсит дампы валюты из poe.ninja (ZIP архив с CSV).
    """

    url = f"https://poe.ninja/poe1/api/data/dumps/dump?name={league}"

    try:
        logger.info(f"Fetching historical currency dump for league: {league} from {url}")
        response = requests.get(url, headers=HEADERS, timeout=60)
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
            # --- ИЗМЕНЕНИЕ ЗДЕСЬ: ИСПОЛЬЗУЕМ НОВУЮ ФУНКЦИЮ ДЛЯ ПОИСКА ИМЕНИ ФАЙЛА ---
            currency_csv_filename = _find_csv_filename_in_zip(zip_ref.namelist(), league, 'currency')

            if not currency_csv_filename:
                logger.warning(
                    f"No suitable currency CSV found in dump for {league}. Available files: {zip_ref.namelist()}")
                return None

            with zip_ref.open(currency_csv_filename) as csv_file:  # Используем найденное имя файла
                df = pd.read_csv(csv_file, sep=';')

                result = []
                for _, row in df.iterrows():
                    if row['Pay'] == 'Chaos Orb' and row['Get'] != 'Chaos Orb':
                        currency_name = row['Get']

                    # Случай 2: получают хаосы
                    elif row['Get'] == 'Chaos Orb' and row['Pay'] != 'Chaos Orb':
                        currency_name = row['Pay']

                    # Все остальные случаи не интересуют
                    else:
                        continue

                    # Защита от деления на ноль / NaN
                    if pd.isna(row['Value']) or row['Value'] == 0:
                        continue

                    chaos_equivalent = (1 / row['Value'])

                    result.append({
                        'league_name': league,
                        'currency_name': currency_name,
                        'chaos_equivalent': chaos_equivalent,
                        'timestamp': row['Date']
                    })

                result_df = pd.DataFrame(result)
                logger.info(
                    f"Successfully parsed {len(result_df)} historical currency entries for {league} from {currency_csv_filename}")
                return result_df

    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching historical currency dump for {league} from {url}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error fetching historical currency dump for {league} from {url}: {e}")
        return None
    except zipfile.BadZipFile:
        logger.error(f"Downloaded file for {league} is not a valid ZIP archive from {url}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Error parsing historical currency for {league} from {url}: {e}", exc_info=True)
        return None


def parse_historical_items(
        league: str,
        allowed_types: Optional[Set[str]] = None,
        dump_date: Optional[str] = None
) -> Optional[pd.DataFrame]:
    """
    Парсит items.csv, возвращая только записи с указанными типами.

    Args:
        allowed_types: если None — возвращает все типы кроме DivinationCard
                       если передан set — возвращает только эти типы
    """
    url = f"https://poe.ninja/poe1/api/data/dumps/dump?name={league}"

    try:
        logger.info(f"Fetching items dump for {league}")
        response = requests.get(url, headers=HEADERS, timeout=60)
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
            filename = _find_csv_filename_in_zip(zip_ref.namelist(), league, 'items')
            if not filename:
                logger.warning(f"items.csv not found for {league}")
                return None

            with zip_ref.open(filename) as csv_file:
                df = pd.read_csv(csv_file, sep=';')

                # ── Фильтрация ───────────────────────────────────────────────
                if allowed_types is None:
                    allowed_types={"UniqueAccessory", "UniqueJewel", "UniqueWeapon", "UniqueArmour"}
                mask = df['Type'].isin(allowed_types)

                df_filtered = df[mask].copy()


                result = []
                for _, row in df_filtered.iterrows():
                    result.append({
                        'league_name': league,
                        'item_name': row.get('Name'),
                        'base_type': row.get('Type'),
                        'item_type': row.get('BaseType'),
                        'chaos_value': row.get('Value'),
                        'timestamp': row.get('Date')
                    })

                result_df = pd.DataFrame(result)

                logger.info(f"Parsed {len(result_df)} items for {league} "
                            f"(allowed types: {allowed_types or 'all except DivCard'})")

                return result_df

    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching historical items dump for {league} from {url}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error fetching historical items dump for {league} from {url}: {e}")
        return None
    except zipfile.BadZipFile:
        logger.error(f"Downloaded file for {league} is not a valid ZIP archive from {url}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Error parsing historical items for {league} from {url}: {e}", exc_info=True)
        return None


def parse_historical_cards(league: str, dump_date: Optional[str] = None) -> Optional[pd.DataFrame]:
    """
    Парсит дампы карт с poe.ninja (ZIP архив с CSV).
    """

    url = f"https://poe.ninja/poe1/api/data/dumps/dump?name={league}"  # Убедитесь, что здесь тоже правильный URL

    try:
        logger.info(f"Fetching historical divination cards dump for league: {league} from {url}")
        response = requests.get(url, headers=HEADERS, timeout=60)
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
            # --- ИЗМЕНЕНИЕ ЗДЕСЬ: ИСПОЛЬЗУЕМ НОВУЮ ФУНКЦИЮ ДЛЯ ПОИСКА ИМЕНИ ФАЙЛА ---
            items_csv_filename = _find_csv_filename_in_zip(zip_ref.namelist(), league,
                                                           'items')  # Карты тоже в items.csv

            if not items_csv_filename:
                logger.warning(
                    f"No suitable items CSV found for divination cards in dump for {league}. Available files: {zip_ref.namelist()}")
                return None

            with zip_ref.open(items_csv_filename) as csv_file:  # Используем найденное имя файла
                df = pd.read_csv(csv_file, sep=';')

                df_cards = df[df.get('Type') == 'DivinationCard']

                result = []
                for _, row in df_cards.iterrows():
                    result.append({
                        'league_name': league,
                        'card_name': row.get('Name'),
                        'chaos_value': row.get('Value'),
                        'timestamp': row.get('Date')
                    })

                result_df = pd.DataFrame(result)
                logger.info(
                    f"Successfully parsed {len(result_df)} historical divination card entries for {league} from {items_csv_filename}")
                return result_df

    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching historical divination cards dump for {league} from {url}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error fetching historical divination cards dump for {league} from {url}: {e}")
        return None
    except zipfile.BadZipFile:
        logger.error(f"Downloaded file for {league} is not a valid ZIP archive from {url}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Error parsing historical divination cards for {league} from {url}: {e}", exc_info=True)
        return None


