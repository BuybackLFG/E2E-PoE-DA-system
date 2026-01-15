import os
import time
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Tuple, Optional

from parsers.currency import parse_currency
from parsers.cards import parse_cards
from parsers.items import parse_items
# Импортируем обе функции из league_finder
from parsers.league_finder import get_latest_league, get_recent_leagues_from_wiki
from parsers.historical import parse_historical_currency, parse_historical_items, parse_historical_cards
from parsers.historical_backfill import HistoricalBackfiller
from league_manager import LeagueManager  # fetch_available_leagues_from_ninja больше не нужен здесь

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Настройки базы из .env
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_HOST = os.getenv('DB_HOST', 'db')
DB_PORT = os.getenv('DB_PORT', '5432')

# Initialize database engine
DB_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = None
league_manager = None


def get_current_active_league() -> Optional[str]:
    """
    Получает актуальную активную лигу из poewiki.net.
    """
    try:
        latest = get_latest_league()  # Используем новую обертку из league_finder
        if latest:
            logger.info(f"Latest active league from wiki: {latest}")
            return latest
    except Exception as e:
        logger.warning(f"Error fetching latest active league from wiki: {e}")

    logger.warning("Using default league: Settlers")
    return "Settlers"


def get_leagues_to_collect(specific_league_name: Optional[str] = None, collect_historical_flag: bool = False) -> List[
    Tuple[str, bool]]:
    """
    Получаем список лиг для которых потом будем собирать данные.

    Args:
        specific_league_name: Определенное имя лиги (если  None, используется последние лиги).
        collect_historical_flag: Включать ли дампы старых лиг из wiki.

    Returns:
        Список кортежей (имя_лиги, является_ли_исторической: bool)
    """
    leagues_to_process: List[Tuple[str, bool]] = []

    current_active_league = get_current_active_league()

    if specific_league_name:
        # Если указана конкретная лига, обрабатываем только ее.
        # Считаем ее текущей для целей парсинга (не исторический API).
        leagues_to_process.append((specific_league_name, False))
    elif current_active_league:
        # Всегда добавляем текущую активную лигу как неисторическую
        leagues_to_process.append((current_active_league, False))
    else:
        logger.error("No current active league found and no specific league provided. Cannot collect data.")
        return []

    if collect_historical_flag:
        # Получаем последние N лиг из wiki (включая текущую)
        # Определите, сколько исторических лиг вы хотите собирать. Например, 5.
        # Это будет включать текущую лигу, если она входит в топ N.
        all_recent_wiki_leagues = get_recent_leagues_from_wiki(num_leagues=5)

        # Добавляем лиги из этого списка, которые еще не были добавлены (как текущая)
        current_league_names_in_queue = {l[0] for l in leagues_to_process}
        for wiki_league_name in all_recent_wiki_leagues:
            if wiki_league_name not in current_league_names_in_queue:
                leagues_to_process.append((wiki_league_name, True))  # Эти лиги будут обрабатываться как исторические

    # Удаляем дубликаты, сохраняя порядок и флаг
    # Это нужно, если current_active_league уже был в списке all_recent_wiki_leagues
    final_leagues_to_process = []
    seen = set()
    for league_name, is_hist in leagues_to_process:
        if league_name not in seen:
            final_leagues_to_process.append((league_name, is_hist))
            seen.add(league_name)

    return final_leagues_to_process


def save_to_database(df, table_name, league_name: str):
    """
    Сохранить DataFrame в базу данных с правильным league_id.

    Args:
        df: Pandas DataFrame для сохранения
        table_name: Имя целевой таблицы
        league_name: Имя лиги для получения league_id

    Returns:
        True при успехе, False в противном случае
    """
    global engine, league_manager

    if df is None or df.empty:
        logger.warning(f"Empty DataFrame provided for table {table_name} for league {league_name}")
        return False

    try:
        # создает или получет лигу
        league_id = league_manager.get_or_create_league(league_name)
        if not league_id:
            logger.error(f"Failed to get/create league: {league_name}")
            return False

        df['league_id'] = league_id

        # убирает столбец league_name (если есть... deprecated так как теперь все по ID) #TODO переделать позже
        if 'league_name' in df.columns:
            df = df.drop('league_name', axis=1)

        # сохрание в дб
        df.to_sql(table_name, engine, if_exists='append', index=False)
        logger.info(f"Successfully saved {len(df)} rows to {table_name} for league {league_name}")
        return True
    except SQLAlchemyError as e:
        logger.error(f"Database error saving to {table_name} for league {league_name}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error saving to {table_name} for league {league_name}: {e}")
        return False


def initialize_database():
    """Инициализирование соединение с базой данных и league manager."""
    global engine, league_manager

    try:
        logger.info(f"Initializing database connection: postgresql://{DB_USER}:***@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        engine = create_engine(DB_URL)

        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        logger.info("Database connection established successfully")

        # Initialize league manager
        league_manager = LeagueManager(engine)
        logger.info("League manager initialized successfully")

        return True
    except Exception as e:
        logger.error(f"Error initializing database: {e}", exc_info=True)
        return False


def collect_data_for_source(source_name, parser_func, league, table_name, use_historical=False):
    """
    Получает данные для указанного источника и сохраняет их в базу данных

    Args:
        source_name: имя источника
        parser_func: функция парсинга (current API)
        league: лига
        table_name: имя целевой таблицы
        use_historical: использовать исторические данные из dumps

    Returns:
        Кортеж (success: bool, records_count: int)
    """
    logger.info(f"--- Starting {source_name} collection for {league} league (historical={use_historical}) ---")

    try:
        if use_historical:
            # Для исторических лиг всегда используем парсеры из historical.py
            if source_name == 'Currency':
                df = parse_historical_currency(league)
            elif source_name == 'Divination Cards':
                df = parse_historical_cards(league)
            elif source_name == 'Unique Items':
                df = parse_historical_items(league)
            else:
                logger.warning(f"Unknown source for historical parsing: {source_name}")
                return (False, 0)
        else:
            # Для текущей лиги используем обычные парсеры
            df = parser_func(league)

        if df is None or df.empty:
            logger.warning(f"No data received from {source_name} for league {league}")
            return (False, 0)

        success = save_to_database(df, table_name, league)

        if success:
            logger.info(f"{source_name} updated successfully ({len(df)} records) for league {league}")
            return (True, len(df))
        else:
            logger.error(f"Failed to save {source_name} data to database for league {league}")
            return (False, 0)

    except Exception as e:
        logger.error(f"Error collecting {source_name} for league {league}: {e}", exc_info=True)
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

    # Запустить заполнение исторических данных при старте (если включено)
    run_backfill_on_start()

    cycle_count = 0

    COLLECT_HISTORICAL = os.getenv('COLLECT_HISTORICAL', 'false').lower() == 'true'
    SPECIFIC_LEAGUE = os.getenv('SPECIFIC_LEAGUE', None)

    while True:
        cycle_count += 1
        logger.info(f"=== Starting collection cycle #{cycle_count} ===")

        try:
            # получает лиги для сбора (теперь возвращает (имя_лиги, флаг_историчности))
            leagues_to_process = get_leagues_to_collect(SPECIFIC_LEAGUE, COLLECT_HISTORICAL)

            # Логируем только имена лиг для читаемости
            logger.info(f"Leagues to collect: {[l[0] for l in leagues_to_process]}")

            if not leagues_to_process:
                logger.warning("No leagues to process in this cycle. Sleeping.")

            for league_name, is_historical_for_this_league in leagues_to_process:
                logger.info(f"\n{'=' * 60}")
                logger.info(f"Processing league: {league_name} (historical={is_historical_for_this_league})")
                logger.info(f"{'=' * 60}")

                results = {
                    'currency': collect_data_for_source(
                        'Currency', parse_currency, league_name, 'currency_prices',
                        use_historical=is_historical_for_this_league
                    ),
                    'cards': collect_data_for_source(
                        'Divination Cards', parse_cards, league_name, 'divination_cards',
                        use_historical=is_historical_for_this_league
                    ),
                    'items': collect_data_for_source(
                        'Unique Items', parse_items, league_name, 'unique_items',
                        use_historical=is_historical_for_this_league
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

