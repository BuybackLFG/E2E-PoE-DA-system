import os
import time
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from parsers.currency import parse_currency
from parsers.cards import parse_cards
from parsers.items import parse_items
from parsers.league_finder import get_latest_league
from parsers.historical import parse_historical_currency, parse_historical_items, parse_historical_cards
from league_manager import LeagueManager, fetch_available_leagues_from_ninja, get_latest_league_from_wiki

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


def get_league():
    """
    Получаем актуальную лигу из базы данных или API.
    Больше не использует файловое кеширование. #TODO убрать пояснение в комментариях позже 
    """
    global league_manager
    
    try:
        latest = get_latest_league_from_wiki()
        if latest:
            logger.info(f"Latest league from wiki: {latest}")
            return latest
    except Exception as e:
        logger.warning(f"Error fetching latest league from wiki: {e}")
    
    logger.warning("Using default league: Settlers")
    return "Settlers"


def get_leagues_to_collect(league_name: str = None, include_historical: bool = False) -> list:
    """
    Получаем список лиг для которых потом будем собирать данные
    
    Args:
        league_name: Определенное имя лиги (если  None, используется последняя лига)
        include_historical: включать ли дампы старых лиг
        
    Returns:
        Список лиг 
    """
    global league_manager
    
    leagues = []
    
    if league_name:
        # Собирает определенную лигу
        leagues.append(league_name)
    else:
        # Собирает последнюю лигу 
        leagues.append(get_league())
    
    if include_historical:
        # Fполучет лиги из дампов
        try:
            historical_leagues = fetch_available_leagues_from_ninja()
            if historical_leagues:
                leagues.extend([l for l in historical_leagues if l not in leagues])
                logger.info(f"Added {len(historical_leagues)} historical leagues to collection queue")
        except Exception as e:
            logger.warning(f"Error fetching historical leagues: {e}")
    
    return leagues


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
        logger.warning(f"Empty DataFrame provided for table {table_name}")
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
        logger.error(f"Database error saving to {table_name}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error saving to {table_name}: {e}")
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
            # Use current API parser
            df = parser_func(league)
        
        if df is None or df.empty:
            logger.warning(f"No data received from {source_name}")
            return (False, 0)
        
        success = save_to_database(df, table_name, league)
        
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
    global engine, league_manager
    
    logger.info(f"=== COLLECTOR STARTED === PID: {os.getpid()}")
    
    if not initialize_database():
        logger.error("Failed to initialize database. Exiting.")
        return
    
    cycle_count = 0
    
    COLLECT_HISTORICAL = os.getenv('COLLECT_HISTORICAL', 'false').lower() == 'true'
    SPECIFIC_LEAGUE = os.getenv('SPECIFIC_LEAGUE', None)
    
    while True:
        cycle_count += 1
        logger.info(f"=== Starting collection cycle #{cycle_count} ===")
        
        try:
            # получает лиги для сбора 
            leagues = get_leagues_to_collect(SPECIFIC_LEAGUE, COLLECT_HISTORICAL)
            logger.info(f"Leagues to collect: {leagues}")
            
            
            for league in leagues:
                logger.info(f"\n{'='*60}")
                logger.info(f"Processing league: {league}")
                logger.info(f"{'='*60}")
                
                is_historical = (league != get_league()) or COLLECT_HISTORICAL
                
                results = {
                    'currency': collect_data_for_source(
                        'Currency', parse_currency, league, 'currency_prices', use_historical=is_historical
                    ),
                    'cards': collect_data_for_source(
                        'Divination Cards', parse_cards, league, 'divination_cards', use_historical=is_historical
                    ),
                    'items': collect_data_for_source(
                        'Unique Items', parse_items, league, 'unique_items', use_historical=is_historical
                    )
                }
                
                successful_sources = sum(1 for success, _ in results.values() if success)
                total_records = sum(count for _, count in results.values())
                
                logger.info(
                    f"\n=== {league} Summary ===\n"
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
        
        #
        logger.info("\n=== Collection cycle finished. Sleeping for 30 minutes ===")
        time.sleep(1800)
    
    logger.info("Shutting down collector...")
    if engine:
        engine.dispose()
    logger.info("Collector stopped.")


if __name__ == "__main__":
    main()
