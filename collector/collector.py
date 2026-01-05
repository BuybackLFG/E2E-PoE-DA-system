import os
import time
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from parsers.currency import parse_currency
from parsers.cards import parse_cards
from parsers.items import parse_items
from parsers.league_finder import get_latest_league

# Load environment variables
load_dotenv()

# Configure logging
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

LEAGUE_CACHE_FILE = ".last_league"
LEAGUE_UPDATE_INTERVAL = timedelta(days=1)

# Initialize database engine
DB_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = None


def load_cached_league():
    """Загрузить последнюю сохранённую лигу из файла"""
    if os.path.exists(LEAGUE_CACHE_FILE):
        try:
            with open(LEAGUE_CACHE_FILE, "r", encoding="utf-8") as f:
                return f.read().strip()
        except Exception as e:
            logger.error(f"Error loading cached league: {e}")
    return None


def save_cached_league(league):
    """Сохранить текущую лигу в файл"""
    try:
        with open(LEAGUE_CACHE_FILE, "w", encoding="utf-8") as f:
            f.write(league)
    except Exception as e:
        logger.error(f"Error saving cached league: {e}")


def get_league():
    """
    Получаем актуальную лигу (с кешем и проверкой раз в день).
    """
    last_update_file = ".last_league_update"
    need_update = True

    if os.path.exists(last_update_file):
        try:
            with open(last_update_file, "r") as f:
                last_update_str = f.read().strip()
                try:
                    last_update = datetime.fromisoformat(last_update_str)
                    if datetime.now() - last_update < LEAGUE_UPDATE_INTERVAL:
                        need_update = False
                except ValueError:
                    pass
        except Exception as e:
            logger.warning(f"Error reading last update file: {e}")

    cached = load_cached_league()

    if need_update:
        try:
            latest = get_latest_league()
            if latest:
                save_cached_league(latest)
                try:
                    with open(last_update_file, "w") as f:
                        f.write(datetime.now().isoformat())
                except Exception as e:
                    logger.warning(f"Error saving last update time: {e}")
                return latest
        except Exception as e:
            logger.warning(f"Ошибка при получении актуальной лиги: {e}")

    # Если апдейт не нужен или произошла ошибка — используем кэш
    return cached or "Settlers"  # дефолт на случай полного фейла


def save_to_database(df, table_name):
    """
    Сохранить DataFrame в базу данных.
    
    Args:
        df: Pandas DataFrame для сохранения
        table_name: Имя целевой таблицы
        
    Returns:
        True при успехе, False в противном случае
    """
    global engine
    
    if df is None or df.empty:
        logger.warning(f"Empty DataFrame provided for table {table_name}")
        return False
    
    try:
        df.to_sql(table_name, engine, if_exists='append', index=False)
        logger.info(f"Successfully saved {len(df)} rows to {table_name}")
        return True
    except SQLAlchemyError as e:
        logger.error(f"Database error saving to {table_name}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error saving to {table_name}: {e}")
        return False


def initialize_database():
    """Инициализирование соединение с базой данных."""
    global engine
    
    try:
        logger.info(f"Initializing database connection: postgresql://{DB_USER}:***@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        engine = create_engine(DB_URL)
        
        # Test connection
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        
        logger.info("Database connection established successfully")
        return True
    except Exception as e:
        logger.error(f"Error initializing database: {e}", exc_info=True)
        return False


def collect_data_for_source(source_name, parser_func, league, table_name):
    """
    Получает данные для указанного источника и сохраняет их в базу данных
    
    Args:
        source_name: имя источника
        parser_func: функция парсинга
        league: лига
        table_name: имя целевой таблицы
        
    Returns:
        Tuple of (success: bool, records_count: int)
    """
    logger.info(f"--- Starting {source_name} collection for {league} league ---")
    
    try:
        # Parse data
        df = parser_func(league)
        
        if df is None or df.empty:
            logger.warning(f"No data received from {source_name}")
            return (False, 0)
        
        # Save to database with retry
        success = save_to_database(df, table_name)
        
        if success:
            logger.info(f"{source_name} updated successfully ({len(df)} records)")
            return (True, len(df))
        else:
            logger.error(f"Failed to save {source_name} data to database")
            return (False, 0)
            
    except Exception as e:
        logger.error(f"Error collecting {source_name}: {e}", exc_info=True)
        return (False, 0)


def main():
    """Главный цикл коллектора."""
    global engine
    
    logger.info(f"=== COLLECTOR STARTED === PID: {os.getpid()}")
    
    # Initialize database connection
    if not initialize_database():
        logger.error("Failed to initialize database. Exiting.")
        return
    
    cycle_count = 0
    
    while True:
        cycle_count += 1
        logger.info(f"=== Starting collection cycle #{cycle_count} ===")
        
        try:
            # Get current league
            LEAGUE = get_league()
            logger.info(f"Using league: {LEAGUE}")
            
            # Track collection results
            results = {
                'currency': collect_data_for_source(
                    'Currency', parse_currency, LEAGUE, 'currency_prices'
                ),
                'cards': collect_data_for_source(
                    'Divination Cards', parse_cards, LEAGUE, 'divination_cards'
                ),
                'items': collect_data_for_source(
                    'Unique Items', parse_items, LEAGUE, 'unique_items'
                )
            }
            
            # Summary of collection cycle
            successful_sources = sum(1 for success, _ in results.values() if success)
            total_records = sum(count for _, count in results.values())
            
            logger.info(
                f"=== Cycle #{cycle_count} Summary ===\n"
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
        
        # Wait before next cycle
        logger.info("=== Collection cycle finished. Sleeping for 30 minutes ===")
        time.sleep(1800)
    
    # Cleanup
    logger.info("Shutting down collector...")
    if engine:
        engine.dispose()
    logger.info("Collector stopped.")


if __name__ == "__main__":
    main()
