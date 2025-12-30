import os
import time
import psycopg2
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()

from parsers.currency import parse_currency
from parsers.cards import parse_cards
from parsers.items import parse_items
from parsers.league_finder import get_latest_league

# Настройки базы из .env
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_HOST = 'localhost'
DB_PORT = '5433'

LEAGUE_CACHE_FILE = ".last_league"
LEAGUE_UPDATE_INTERVAL = timedelta(days=1)  # Проверяем новую лигу раз в сутки


def load_cached_league():
    """Загрузить последнюю сохранённую лигу из файла"""
    if os.path.exists(LEAGUE_CACHE_FILE):
        with open(LEAGUE_CACHE_FILE, "r", encoding="utf-8") as f:
            return f.read().strip()
    return None


def save_cached_league(league):
    """Сохранить текущую лигу в файл"""
    with open(LEAGUE_CACHE_FILE, "w", encoding="utf-8") as f:
        f.write(league)


def get_league():
    """Получить актуальную лигу (с кешем и проверкой раз в день)"""
    last_update_file = ".last_league_update"
    need_update = True

    if os.path.exists(last_update_file):
        with open(last_update_file, "r") as f:
            last_update_str = f.read().strip()
            try:
                last_update = datetime.fromisoformat(last_update_str)
                if datetime.now() - last_update < LEAGUE_UPDATE_INTERVAL:
                    need_update = False
            except ValueError:
                pass

    cached = load_cached_league()

    if need_update:
        try:
            latest = get_latest_league()
            if latest:
                save_cached_league(latest)
                with open(last_update_file, "w") as f:
                    f.write(datetime.now().isoformat())
                return latest
        except Exception as e:
            print(f"[WARN] Ошибка при получении актуальной лиги: {e}")

    # Если апдейт не нужен или произошла ошибка — используем кэш
    return cached or "Settlers"  # дефолт на случай полного фейла


engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')


def main():
    while True:
        LEAGUE = get_league()
        print(f"--- Starting data collection for {LEAGUE} league ---")

        # 1. Валюта
        try:
            df_curr = parse_currency(LEAGUE)
            if df_curr is not None:
                df_curr.to_sql('currency_prices', engine, if_exists='append', index=False)
                print("Currency prices updated.")
        except Exception as e:
            print(f"Error parsing currency: {e}")

            # 2. Карточки
        try:
            df_cards = parse_cards(LEAGUE)
            if df_cards is not None:
                df_cards.to_sql('divination_cards', engine, if_exists='append', index=False)
                print("Divination cards updated.")
        except Exception as e:
            print(f"Error parsing cards: {e}")

            # 3. Уники
        try:
            df_items = parse_items(LEAGUE)
            if df_items is not None:
                df_items.to_sql('unique_items', engine, if_exists='append', index=False)
                print("Unique items updated.")
        except Exception as e:
            print(f"Error parsing items: {e}")

        print("--- Collection cycle finished. Sleeping for 30 minutes ---")
        time.sleep(1800)


if __name__ == "__main__":
    main()
