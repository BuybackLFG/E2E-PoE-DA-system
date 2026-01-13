"""
Автономный скрипт для заполнения исторических данных PoE.

Этот скрипт получает исторические данные из poe.ninja API для валют,
карт гаданий и уникальных предметов, заполняя пробелы в базе данных
за отсутствующие даты.

Использование:
    python backfill_historical.py --league Keepers --days 90
    python backfill_historical.py --league Keepers --type currency --days 30
    python backfill_historical.py --league Keepers --all
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from parsers.historical_backfill import HistoricalBackfiller

load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('backfill.log')
    ]
)
logger = logging.getLogger(__name__)


def initialize_database():
    """
    Инициализировать соединение с базой данных.
    
    Returns:
        SQLAlchemy движок или None при ошибке
    """
    try:
        DB_USER = os.getenv('DB_USER')
        DB_PASSWORD = os.getenv('DB_PASSWORD')
        DB_NAME = os.getenv('DB_NAME')
        DB_HOST = os.getenv('DB_HOST', 'db')
        DB_PORT = os.getenv('DB_PORT', '5432')
        
        if not all([DB_USER, DB_PASSWORD, DB_NAME]):
            logger.error("Отсутствуют необходимые учетные данные базы данных в файле .env")
            return None
        
        DB_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        
        logger.info(f"Подключение к базе данных: postgresql://{DB_USER}:***@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        engine = create_engine(DB_URL)
        
        # Проверить соединение
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        logger.info("Соединение с базой данных установлено успешно")
        return engine
        
    except Exception as e:
        logger.error(f"Ошибка инициализации базы данных: {e}", exc_info=True)
        return None


def get_available_leagues(engine) -> list:
    """
    Получить список доступных лиг из базы данных.
    
    Args:
        engine: SQLAlchemy движок
        
    Returns:
        Список названий лиг
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT league_name FROM leagues ORDER BY start_date DESC")
            )
            leagues = [row[0] for row in result]
            return leagues
    except Exception as e:
        logger.error(f"Ошибка получения списка лиг: {e}", exc_info=True)
        return []


def main():
    """Главная функция выполнения."""
    parser = argparse.ArgumentParser(
        description='Заполнение исторических данных PoE из poe.ninja API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  # Заполнить все типы данных для лиги Keepers (последние 90 дней)
  python backfill_historical.py --league Keepers --all
  
  # Заполнить только валюту для лиги Settlers (последние 30 дней)
  python backfill_historical.py --league Settlers --type currency --days 30
  
  # Заполнить только карты гаданий
  python backfill_historical.py --league Settlers --type divination_cards
  
  # Список доступных лиг
  python backfill_historical.py --list-leagues
        """
    )
    
    parser.add_argument(
        '--league',
        type=str,
        help='Название лиги для заполнения (например, Keepers, Settlers)'
    )
    
    parser.add_argument(
        '--type',
        type=str,
        choices=['currency', 'divination_cards', 'unique_items', 'all'],
        default='all',
        help='Тип данных для заполнения (по умолчанию: all)'
    )
    
    parser.add_argument(
        '--days',
        type=int,
        default=90,
        help='Максимальное количество дней для просмотра назад (по умолчанию: 90)'
    )
    
    parser.add_argument(
        '--list-leagues',
        action='store_true',
        help='Показать доступные лиги и выйти'
    )
    
    args = parser.parse_args()
    
    # Инициализировать базу данных
    engine = initialize_database()
    if not engine:
        logger.error("Не удалось инициализировать базу данных. Выход.")
        sys.exit(1)
    
    # Показать лиги если запрошено
    if args.list_leagues:
        leagues = get_available_leagues(engine)
        if leagues:
            print("\nДоступные лиги:")
            for i, league in enumerate(leagues, 1):
                print(f"  {i}. {league}")
        else:
            print("Лиги не найдены в базе данных.")
        engine.dispose()
        sys.exit(0)
    
    # Проверить аргумент лиги
    if not args.league:
        logger.error("Аргумент --league обязателен (если не используется --list-leagues)")
        parser.print_help()
        engine.dispose()
        sys.exit(1)
    
    # Проверить аргумент дней
    if args.days <= 0:
        logger.error("--days должно быть положительным целым числом")
        engine.dispose()
        sys.exit(1)
    
    logger.info("="*70)
    logger.info("СКРИПТ ЗАПОЛНЕНИЯ ИСТОРИЧЕСКИХ ДАННЫХ")
    logger.info("="*70)
    logger.info(f"Лига: {args.league}")
    logger.info(f"Тип данных: {args.type}")
    logger.info(f"Дней для просмотра назад: {args.days}")
    logger.info(f"Начато в: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*70)
    
    try:
        # Инициализировать механизм заполнения
        backfiller = HistoricalBackfiller(engine, args.league)
        
        # Выполнить заполнение в зависимости от типа
        if args.type == 'currency':
            items, records = backfiller.backfill_currency(args.days)
            logger.info(f"\nЗаполнение валют завершено: {items} предметов, {records} записей")
            
        elif args.type == 'divination_cards':
            items, records = backfiller.backfill_divination_cards(args.days)
            logger.info(f"\nЗаполнение карт гаданий завершено: {items} предметов, {records} записей")
            
        elif args.type == 'unique_items':
            items, records = backfiller.backfill_unique_items(args.days)
            logger.info(f"\nЗаполнение уникальных предметов завершено: {items} предметов, {records} записей")
            
        elif args.type == 'all':
            results = backfiller.backfill_all(args.days)
            total_items = sum(r[0] for r in results.values())
            total_records = sum(r[1] for r in results.values())
            logger.info(f"\nПолное заполнение завершено:")
            logger.info(f"  Всего обработано предметов: {total_items}")
            logger.info(f"  Всего вставлено записей: {total_records}")
            logger.info(f"  Валюты: {results['currency'][0]} предметов, {results['currency'][1]} записей")
            logger.info(f"  Карты гаданий: {results['divination_cards'][0]} предметов, {results['divination_cards'][1]} записей")
            logger.info(f"  Уникальные предметы: {results['unique_items'][0]} предметов, {results['unique_items'][1]} записей")
        
        logger.info("="*70)
        logger.info(f"Заполнение успешно завершено в: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*70)
        
    except ValueError as e:
        logger.error(f"Ошибка конфигурации: {e}")
        engine.dispose()
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("\nЗаполнение прервано пользователем")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при заполнении: {e}", exc_info=True)
        engine.dispose()
        sys.exit(1)
    finally:
        if engine:
            engine.dispose()
            logger.info("Соединение с базой данных закрыто")


if __name__ == "__main__":
    main()