# collector.py
import os
import time
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Tuple, Optional, Dict, Any  # Добавил Dict, Any

from parsers.currency import parse_currency
from parsers.cards import parse_cards
from parsers.items import parse_items
from parsers.league_finder import get_latest_league, get_recent_leagues_from_wiki
from parsers.historical import parse_historical_currency, parse_historical_items, parse_historical_cards
from parsers.historical_backfill import HistoricalBackfiller
from league_manager import LeagueManager

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_HOST = os.getenv('DB_HOST', 'db')
DB_PORT = os.getenv('DB_PORT', '5432')

DB_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = None
league_manager = None


def get_current_active_league() -> Optional[str]:
    """
    Получает актуальную активную лигу из poewiki.net.
    """
    try:
        latest = get_latest_league()
        if latest:
            logger.info(f"Latest active league from wiki: {latest}")
            return latest
    except Exception as e:
        logger.warning(f"Error fetching latest active league from wiki: {e}")

    logger.warning("Using default league: Settlers")
    return "Settlers"


# --- ИЗМЕНЕНА ФУНКЦИЯ get_leagues_to_collect ---
def get_leagues_to_collect(specific_league_name: Optional[str] = None, collect_historical_flag: bool = False) -> List[
    Dict[str, Any]]:  # Изменен тип возвращаемого значения
    """
    Получаем список лиг для которых потом будем собирать данные.

    Args:
        specific_league_name: Определенное имя лиги (если  None, используется последние лиги).
        collect_historical_flag: Включать ли дампы старых лиг из wiki.

    Returns:
        Список словарей с информацией о лигах (name, is_historical, start_date, status)
    """
    leagues_to_process: List[Dict[str, Any]] = []

    current_active_league_name = get_current_active_league()

    # Для текущей активной лиги мы не знаем точную start_date и status из wiki,
    # поэтому будем использовать значения по умолчанию при создании.
    # Но если она есть в recent_wiki_leagues, мы возьмем данные оттуда.

    if specific_league_name:
        # Если указана конкретная лига, обрабатываем только ее.
        # Для нее мы не знаем start_date и status, поэтому используем дефолты.
        leagues_to_process.append({
            'name': specific_league_name,
            'is_historical': False,
            'start_date': None,  # Будет CURRENT_DATE в league_manager
            'status': 'Active'  # Будет Active в league_manager
        })
    elif current_active_league_name:
        leagues_to_process.append({
            'name': current_active_league_name,
            'is_historical': False,
            'start_date': None,
            'status': 'Active'
        })
    else:
        logger.error("No current active league found and no specific league provided. Cannot collect data.")
        return []

    if collect_historical_flag:
        all_recent_wiki_leagues_info = get_recent_leagues_from_wiki(num_leagues=5)

        current_league_names_in_queue = {l['name'] for l in leagues_to_process}
        for wiki_league_info in all_recent_wiki_leagues_info:
            if wiki_league_info['name'] not in current_league_names_in_queue:
                leagues_to_process.append({
                    'name': wiki_league_info['name'],
                    'is_historical': True,  # Эти лиги будут обрабатываться как исторические
                    'start_date': wiki_league_info['start_date'],
                    'status': wiki_league_info['status']
                })
            else:
                # Если текущая активная лига уже была добавлена с дефолтными значениями,
                # но теперь мы нашли ее в wiki, обновим ее данные.
                for i, league_data in enumerate(leagues_to_process):
                    if league_data['name'] == wiki_league_info['name']:
                        leagues_to_process[i]['start_date'] = wiki_league_info['start_date']
                        leagues_to_process[i]['status'] = wiki_league_info['status']
                        # Если это была текущая лига, но она найдена в исторических,
                        # то она все равно обрабатывается как текущая (is_historical=False)
                        # но с правильными датой и статусом.
                        break

    # Удаляем дубликаты, сохраняя порядок.
    # Теперь каждый элемент - это словарь, поэтому `seen` будет хранить имена.
    final_leagues_to_process = []
    seen_names = set()
    for league_data in leagues_to_process:
        if league_data['name'] not in seen_names:
            final_leagues_to_process.append(league_data)
            seen_names.add(league_data['name'])

    return final_leagues_to_process


def _check_if_data_exists_for_league_and_table(league_id: int, table_name: str) -> bool:
    """
    Проверяет, существуют ли какие-либо записи для данного league_id в указанной таблице.
    """
    global engine
    try:
        with engine.connect() as conn:
            query = text(f"SELECT EXISTS(SELECT 1 FROM {table_name} WHERE league_id = :league_id)")
            result = conn.execute(query, {'league_id': league_id}).scalar()
            return bool(result)
    except SQLAlchemyError as e:
        logger.error(f"Database error checking existing data for league_id {league_id} in {table_name}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error in _check_if_data_exists_for_league_and_table: {e}", exc_info=True)
        return False


def save_to_database(df, table_name, league_id: int):
    """
    Сохранить DataFrame в базу данных с правильным league_id.

    Args:
        df: Pandas DataFrame для сохранения
        table_name: Имя целевой таблицы
        league_id: ID лиги

    Returns:
        True при успехе, False в противном случае
    """
    global engine

    if df is None or df.empty:
        logger.warning(f"Empty DataFrame provided for table {table_name} for league ID {league_id}")
        return False

    try:
        df['league_id'] = league_id

        if 'league_name' in df.columns:
            df = df.drop('league_name', axis=1)

        df.to_sql(table_name, engine, if_exists='append', index=False)
        logger.info(f"Successfully saved {len(df)} rows to {table_name} for league ID {league_id}")
        return True
    except SQLAlchemyError as e:
        logger.error(f"Database error saving to {table_name} for league ID {league_id}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error saving to {table_name} for league ID {league_id}: {e}")
        return False


def initialize_database():
    """Инициализирование соединение с базой данных и league manager."""
    global engine, league_manager

    try:
        logger.info(f"Initializing database connection: postgresql://{DB_USER}:***@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        engine = create_engine(DB_URL)

        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        logger.info("Database connection established successfully")

        league_manager = LeagueManager(engine)
        logger.info("League manager initialized successfully")

        return True
    except Exception as e:
        logger.error(f"Error initializing database: {e}", exc_info=True)
        return False


# --- ИЗМЕНЕНА ФУНКЦИЯ collect_data_for_source ---
def collect_data_for_source(source_name, parser_func, league_info: Dict[str, Any], table_name):  # Изменен тип league
    """
    Получает данные для указанного источника и сохраняет их в базу данных

    Args:
        source_name: имя источника
        parser_func: функция парсинга (current API)
        league_info: Словарь с информацией о лиге (name, is_historical, start_date, status)
        table_name: имя целевой таблицы

    Returns:
        Кортеж (success: bool, records_count: int)
    """
    global engine, league_manager

    league_name_str = league_info['name']
    use_historical = league_info['is_historical']
    league_start_date = league_info['start_date']
    league_status = league_info['status']

    logger.info(
        f"--- Starting {source_name} collection for {league_name_str} league (historical={use_historical}, status={league_status}, start_date={league_start_date}) ---")

    # Получаем league_id, передавая все доступные метаданные
    league_id = league_manager.get_or_create_league(
        league_name_str,
        status=league_status,
        start_date=league_start_date
    )
    if not league_id:
        logger.error(f"Failed to get/create league: {league_name_str}")
        return (False, 0)

    if use_historical:
        if _check_if_data_exists_for_league_and_table(league_id, table_name):
            logger.info(
                f"Historical data for {source_name} in league {league_name_str} (ID: {league_id}) already exists. Skipping.")
            return (True, 0)

    try:
        if use_historical:
            if source_name == 'Currency':
                df = parse_historical_currency(league_name_str)
            elif source_name == 'Divination Cards':
                df = parse_historical_cards(league_name_str)
            elif source_name == 'Unique Items':
                df = parse_historical_items(league_name_str)
            else:
                logger.warning(f"Unknown source for historical parsing: {source_name}")
                return (False, 0)
        else:
            df = parser_func(league_name_str)

        if df is None or df.empty:
            logger.warning(f"No data received from {source_name} for league {league_name_str}")
            return (False, 0)

        success = save_to_database(df, table_name, league_id)

        if success:
            logger.info(f"{source_name} updated successfully ({len(df)} records) for league {league_name_str}")
            return (True, len(df))
        else:
            logger.error(f"Failed to save {source_name} data to database for league {league_name_str}")
            return (False, 0)

    except Exception as e:
        logger.error(f"Error collecting {source_name} for league {league_name_str}: {e}", exc_info=True)
        return (False, 0)



def run_backfill_on_start():
    """
    Запустить заполнение исторических данных при старте контейнера.

    Выполняется один раз при запуске, если переменная RUN_BACKFILL_ON_START=true
    """
    global engine, league_manager

    RUN_BACKFILL_ON_START = os.getenv('RUN_BACKFILL_ON_START', 'false').lower() == 'true'

    if not RUN_BACKFILL_ON_START:
        logger.info("Заполнение исторических данных при старте отключено (RUN_BACKFILL_ON_START=false)")
        return

    logger.info("=" * 70)
    logger.info("ЗАПУСК ЗАПОЛНЕНИЯ ИСТОРИЧЕСКИХ ДАННЫХ ПРИ СТАРТЕ")
    logger.info("=" * 70)

    try:
        # Получить лигу для заполнения
        SPECIFIC_LEAGUE = os.getenv('SPECIFIC_LEAGUE', None)
        if SPECIFIC_LEAGUE:
            league_name = SPECIFIC_LEAGUE
        else:
            league_name = get_current_active_league()  # Используем новую функцию

        logger.info(f"Лига для заполнения: {league_name}")

        # Инициализировать механизм заполнения
        backfiller = HistoricalBackfiller(engine, league_name)

        # Заполнить все типы данных (последние 90 дней по умолчанию)
        results = backfiller.backfill_all(max_days_back=90)

        total_items = sum(r[0] for r in results.values())
        total_records = sum(r[1] for r in results.values())

        logger.info(
            f"Заполнение при старте завершено:\n"
            f"  Всего обработано предметов: {total_items}\n"
            f"  Всего вставлено записей: {total_records}\n"
            f"  Валюты: {results['currency'][0]} предметов, {results['currency'][1]} записей\n"
            f"  Карты гаданий: {results['divination_cards'][0]} предметов, {results['divination_cards'][1]} записей\n"
            f"  Уникальные предметы: {results['unique_items'][0]} предметов, {results['unique_items'][1]} записей"
        )

        logger.info("=" * 70)

    except Exception as e:
        logger.error(f"Ошибка при заполнении исторических данных при старте: {e}", exc_info=True)
        # Не прерываем работу коллектора при ошибке backfill


def main():
    """Главный цикл коллектора."""
    global engine, league_manager

    logger.info(f"=== COLLECTOR STARTED === PID: {os.getpid()}")

    if not initialize_database():
        logger.error("Failed to initialize database. Exiting.")
        return

    run_backfill_on_start()

    cycle_count = 0

    COLLECT_HISTORICAL = os.getenv('COLLECT_HISTORICAL', 'false').lower() == 'true'
    SPECIFIC_LEAGUE = os.getenv('SPECIFIC_LEAGUE', None)

    while True:
        cycle_count += 1
        logger.info(f"=== Starting collection cycle #{cycle_count} ===")

        try:
            leagues_to_process = get_leagues_to_collect(SPECIFIC_LEAGUE, COLLECT_HISTORICAL)

            # Логируем только имена лиг для читаемости
            logger.info(f"Leagues to collect: {[l['name'] for l in leagues_to_process]}")

            if not leagues_to_process:
                logger.warning("No leagues to process in this cycle. Sleeping.")

            for league_info in leagues_to_process:  # Теперь league_info - это словарь
                league_name = league_info['name']
                is_historical_for_this_league = league_info['is_historical']

                logger.info(f"\n{'=' * 60}")
                logger.info(f"Processing league: {league_name} (historical={is_historical_for_this_league})")
                logger.info(f"{'=' * 60}")

                results = {
                    'currency': collect_data_for_source(
                        'Currency', parse_currency, league_info, 'currency_prices'  # Передаем весь словарь league_info
                    ),
                    'cards': collect_data_for_source(
                        'Divination Cards', parse_cards, league_info, 'divination_cards'
                    ),
                    'items': collect_data_for_source(
                        'Unique Items', parse_items, league_info, 'unique_items'
                    )
                }

                successful_sources = sum(1 for success, _ in results.values() if success)
                total_records = sum(count for _, count in results.values())

                logger.info(
                    f"\n=== {league_name} Summary ===\n"
                    f"  Successful sources: {successful_sources}/3\n"
                    f"  Total records collected: {total_records}\n"
                    f"  Currency: {'✓' if results['currency'][0] else '✗'} ({results['currency'][1]} records)\n"
                    f"  Cards: {'✓' if results['cards'][0] else '✗'} ({results['cards'][1]} records)\n"
                    f"  Items: {'✓' if results['items'][0] else '✗'} ({results['items'][1]} records)"
                )

        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt. Shutting down gracefully...")
            break

        except Exception as e:
            logger.error(f"Unexpected error in collection cycle: {e}", exc_info=True)

        logger.info("\n=== Collection cycle finished. Sleeping for 30 minutes ===")
        time.sleep(1800)

    logger.info("Shutting down collector...")
    if engine:
        engine.dispose()
    logger.info("Collector stopped.")


if __name__ == "__main__":
    main()

