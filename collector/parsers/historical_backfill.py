"""
Модуль для заполнения исторических данных PoE

Этот модуль обрабатывает получение и парсинг исторических данных из poe.ninja API
для валют, карт гаданий и уникальных предметов. Он заполняет пробелы в базе данных,
получая данные за отсутствующие даты.

Основные возможности:
- Сопоставляет имена предметов из базы данных с ID из API
- Обрабатывает сложные расчеты валют (значения pay/receive)
- Вычисляет правильные временные метки из "daysAgo"
- Пропускает Chaos Orb (id=1), так как это базовая валюта
- Обнаруживает и заполняет пробелы в базе данных
"""

import logging
import requests
import pandas as pd
from typing import Optional, Dict, List, Tuple
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


class HistoricalBackfiller:
    """
    Основной класс для обработки заполнения исторических данных.
    
    Координирует запросы к базе данных, вызовы API и преобразование данных
    для заполнения пробелов в исторических данных.
    """
    
    def __init__(self, engine: Engine, league_name: str):
        """
        Инициализация механизма заполнения.
        
        Args:
            engine: SQLAlchemy движок базы данных
            league_name: Название лиги для заполнения
        """
        self.engine = engine
        self.league_name = league_name
        self.league_id = self._get_league_id()
        
        if not self.league_id:
            raise ValueError(f"Лига {league_name} не найдена в базе данных")
    
    def _get_league_id(self) -> Optional[int]:
        """Получить ID лиги из базы данных."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("SELECT id FROM leagues WHERE league_name = :league_name"),
                    {"league_name": self.league_name}
                )
                row = result.fetchone()
                return row[0] if row else None
        except Exception as e:
            logger.error(f"Ошибка при получении ID лиги: {e}", exc_info=True)
            return None
    
    def backfill_currency(self, max_days_back: int = 90) -> Tuple[int, int]:
        """
        Заполнить исторические данные валют.
        
        Args:
            max_days_back: Максимальное количество дней для просмотра назад
            
        Returns:
            Кортеж (обработано_предметов, вставлено_записей)
        """
        logger.info(f"Начало заполнения валют для лиги: {self.league_name}")
        
        # Шаг 1: Получить детали валют из текущего API
        currency_details = self._fetch_currency_details()
        if not currency_details:
            logger.error("Не удалось получить детали валют")
            return (0, 0)
        
        # Шаг 2: Получить существующие названия валют из базы данных
        existing_currencies = self._get_existing_currency_names()
        
        # Шаг 3: Сопоставить названия из базы с ID из API
        name_to_id_map = self._map_currency_names_to_ids(currency_details, existing_currencies)
        
        if not name_to_id_map:
            logger.warning("Не найдено соответствий валют")
            return (0, 0)
        
        logger.info(f"Найдено {len(name_to_id_map)} соответствий валют")
        
        # Шаг 4: Для каждой валюты получить исторические данные и заполнить пробелы
        total_records = 0
        items_processed = 0
        
        for currency_name, api_id in name_to_id_map.items():
            try:
                # Пропустить Chaos Orb (id=1), так как это базовая валюта
                if api_id == 1:
                    logger.debug(f"Пропуск Chaos Orb (id=1) - базовая валюта")
                    continue
                
                records = self._backfill_single_currency(currency_name, api_id, max_days_back)
                if records > 0:
                    total_records += records
                    items_processed += 1
                    
            except Exception as e:
                logger.error(f"Ошибка при заполнении валюты {currency_name}: {e}", exc_info=True)
                continue
        
        logger.info(f"Заполнение валют завершено: {items_processed} предметов, {total_records} записей")
        return (items_processed, total_records)
    
    def _fetch_currency_details(self) -> Optional[List[Dict]]:
        """
        Получить детали валют из текущего API.
        
        Returns:
            Список словарей с деталями валют или None
        """
        url = f"https://poe.ninja/poe1/api/economy/stash/current/currency/overview?league={self.league_name}&type=Currency"
        
        try:
            logger.info(f"Получение деталей валют из API")
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            currency_details = data.get('currencyDetails', [])
            
            logger.info(f"Получено {len(currency_details)} деталей валют")
            return currency_details
            
        except requests.exceptions.Timeout:
            logger.error("Тайм-аут при получении деталей валют")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка запроса при получении деталей валют: {e}")
            return None
        except Exception as e:
            logger.error(f"Ошибка при получении деталей валют: {e}", exc_info=True)
            return None
    
    def _get_existing_currency_names(self) -> List[str]:
        """
        Получить список существующих названий валют из базы данных.
        
        Returns:
            Список названий валют
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("""
                        SELECT DISTINCT currency_name 
                        FROM currency_prices 
                        WHERE league_id = :league_id
                    """),
                    {"league_id": self.league_id}
                )
                names = [row[0] for row in result]
                logger.info(f"Найдено {len(names)} существующих валют в базе данных")
                return names
        except Exception as e:
            logger.error(f"Ошибка при получении существующих названий валют: {e}", exc_info=True)
            return []
    
    def _map_currency_names_to_ids(self, currency_details: List[Dict], 
                                   existing_names: List[str]) -> Dict[str, int]:
        """
        Сопоставить названия валют из базы с ID из API.
        
        Args:
            currency_details: Список деталей валют из API
            existing_names: Список названий валют из базы данных
            
        Returns:
            Словарь соответствий currency_name -> api_id
        """
        name_to_id_map = {}
        
        for detail in currency_details:
            api_name = detail.get('name')
            trade_id = detail.get('tradeId')
            api_id = detail.get('id')
            
            if not api_id:
                continue
            
            # Проверить, существует ли эта валюта в базе данных (точное совпадение)
            if api_name in existing_names:
                name_to_id_map[api_name] = api_id
                logger.debug(f"Сопоставлено {api_name} -> id={api_id}")
        
        return name_to_id_map

    def _backfill_single_currency(self, currency_name: str, api_id: int, max_days_back: int) -> int:
        historical_data = self._fetch_currency_history(api_id)
        if not historical_data or not isinstance(historical_data, dict):
            return 0

        receive_graph = historical_data.get("receiveCurrencyGraphData", [])
        pay_graph = historical_data.get("payCurrencyGraphData", [])

        # Делаем словарь {daysAgo → данные} для быстрого доступа
        receive_by_day = {e['daysAgo']: e for e in receive_graph if 'daysAgo' in e}
        pay_by_day = {e['daysAgo']: e for e in pay_graph if 'daysAgo' in e}

        records_to_insert = []
        today = datetime.now().date()

        # Берём все возможные дни из обоих источников
        all_days = set(receive_by_day.keys()) | set(pay_by_day.keys())

        for days_ago in sorted(all_days):
            if days_ago > max_days_back:
                continue

            entry_date = today - timedelta(days=days_ago)

            if entry_date in self._get_existing_currency_dates(currency_name):
                continue

            pay_entry = pay_by_day.get(days_ago)
            receive_entry = receive_by_day.get(days_ago)

            record = self._process_currency_entry_both(pay_entry, receive_entry, currency_name, entry_date)
            if record:
                records_to_insert.append(record)

        if records_to_insert:
            self._insert_currency_records(records_to_insert)
            logger.info(f"Вставлено {len(records_to_insert)} записей для {currency_name}")

        return len(records_to_insert)
    
    def _get_existing_currency_dates(self, currency_name: str) -> set:
        """
        Получить набор существующих дат для валюты.
        
        Args:
            currency_name: Название валюты
            
        Returns:
            Набор дат
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("""
                        SELECT DISTINCT DATE(timestamp) as date
                        FROM currency_prices
                        WHERE league_id = :league_id AND currency_name = :currency_name
                    """),
                    {"league_id": self.league_id, "currency_name": currency_name}
                )
                dates = {row[0] for row in result}
                return dates
        except Exception as e:
            logger.error(f"Ошибка при получении существующих дат для {currency_name}: {e}", exc_info=True)
            return set()
    
    def _fetch_currency_history(self, api_id: int) -> Optional[List[Dict]]:
        """
        Получить исторические данные для валюты из API.
        
        Args:
            api_id: API ID валюты
            
        Returns:
            Список исторических записей или None
        """
        url = f"https://poe.ninja/poe1/api/economy/stash/current/currency/history?league={self.league_name}&type=Currency&id={api_id}"
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            return data
            
        except requests.exceptions.Timeout:
            logger.error(f"Тайм-аут при получении истории валюты для id={api_id}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка запроса при получении истории валюты для id={api_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Ошибка при получении истории валюты: {e}", exc_info=True)
            return None

    def _process_currency_entry_both(self, pay_entry: Dict | None, receive_entry: Dict | None,
                                     currency_name: str, entry_date: datetime.date) -> Optional[Dict]:
        if not pay_entry and not receive_entry:
            return None

        pay_value = pay_entry.get('value', 0) if pay_entry else 0
        pay_count = pay_entry.get('count', 0) if pay_entry else 0

        receive_value = receive_entry.get('value', 0) if receive_entry else 0
        receive_count = receive_entry.get('count', 0) if receive_entry else 0

        values = []
        weights = []

        # Покупаем (pay) → сколько хаоса просят за 1 шт
        if pay_value > 0:
            values.append(1 / pay_value)
            weights.append(pay_count or 1)

        # Продаём (receive) → сколько хаоса дают за 1 шт
        if receive_value > 0 and receive_count > 0:
            values.append(receive_value)
            weights.append(receive_count)

        if not values:
            return None

        # Самый простой и популярный вариант — просто среднее
        chaos_equivalent = sum(values) / len(values)

        # Или взвешенное (часто лучше)
        # chaos_equivalent = sum(v * w for v, w in zip(values, weights)) / sum(weights)

        return {
            'timestamp': datetime.combine(entry_date, datetime.min.time()),
            'league_id': self.league_id,
            'currency_name': currency_name,
            'details_id': None,
            'chaos_equivalent': round(chaos_equivalent, 6),
            'pay_value': round(1 / pay_value, 6) if pay_value > 0 else None,
            'receive_value': round(receive_value,
                                   6) if receive_value > 0 and receive_count > 0 else None,
            'trade_count': max(pay_count, receive_count) if pay_count or receive_count else None
        }
    
    def _insert_currency_records(self, records: List[Dict]):
        """
        Вставить записи валют в базу данных.
        
        Args:
            records: Список словарей записей
        """
        try:
            df = pd.DataFrame(records)
            df.to_sql('currency_prices', self.engine, if_exists='append', index=False)
        except Exception as e:
            logger.error(f"Ошибка при вставке записей валют: {e}", exc_info=True)
            raise
    
    def backfill_divination_cards(self, max_days_back: int = 90) -> Tuple[int, int]:
        """
        Заполнить исторические данные карт гаданий.
        
        Args:
            max_days_back: Максимальное количество дней для просмотра назад
            
        Returns:
            Кортеж (обработано_предметов, вставлено_записей)
        """
        logger.info(f"Начало заполнения карт гаданий для лиги: {self.league_name}")
        
        # Шаг 1: Получить детали карт из текущего API
        card_details = self._fetch_card_details()
        if not card_details:
            logger.error("Не удалось получить детали карт")
            return (0, 0)
        
        # Шаг 2: Получить существующие названия карт из базы данных
        existing_cards = self._get_existing_card_names()
        
        # Шаг 3: Сопоставить названия из базы с ID из API
        name_to_id_map = self._map_card_names_to_ids(card_details, existing_cards)
        
        if not name_to_id_map:
            logger.warning("Не найдено соответствий карт")
            return (0, 0)
        
        logger.info(f"Найдено {len(name_to_id_map)} соответствий карт")
        
        # Шаг 4: Для каждой карты получить исторические данные и заполнить пробелы
        total_records = 0
        items_processed = 0
        
        for card_name, api_id in name_to_id_map.items():
            try:
                records = self._backfill_single_card(card_name, api_id, max_days_back)
                if records > 0:
                    total_records += records
                    items_processed += 1
                    
            except Exception as e:
                logger.error(f"Ошибка при заполнении карты {card_name}: {e}", exc_info=True)
                continue
        
        logger.info(f"Заполнение карт гаданий завершено: {items_processed} предметов, {total_records} записей")
        return (items_processed, total_records)
    
    def _fetch_card_details(self) -> Optional[List[Dict]]:
        """
        Получить детали карт гаданий из текущего API.
        
        Returns:
            Список словарей с деталями карт или None
        """
        url = f"https://poe.ninja/poe1/api/economy/stash/current/item/overview?league={self.league_name}&type=DivinationCard"
        
        try:
            logger.info(f"Получение деталей карт гаданий из API")
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            card_details = data.get('lines', [])
            
            logger.info(f"Получено {len(card_details)} деталей карт")
            return card_details
            
        except requests.exceptions.Timeout:
            logger.error("Тайм-аут при получении деталей карт")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка запроса при получении деталей карт: {e}")
            return None
        except Exception as e:
            logger.error(f"Ошибка при получении деталей карт: {e}", exc_info=True)
            return None
    
    def _get_existing_card_names(self) -> List[str]:
        """
        Получить список существующих названий карт из базы данных.
        
        Returns:
            Список названий карт
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("""
                        SELECT DISTINCT card_name 
                        FROM divination_cards 
                        WHERE league_id = :league_id
                    """),
                    {"league_id": self.league_id}
                )
                names = [row[0] for row in result]
                logger.info(f"Найдено {len(names)} существующих карт в базе данных")
                return names
        except Exception as e:
            logger.error(f"Ошибка при получении существующих названий карт: {e}", exc_info=True)
            return []
    
    def _map_card_names_to_ids(self, card_details: List[Dict], 
                               existing_names: List[str]) -> Dict[str, int]:
        """
        Сопоставить названия карт из базы с ID из API.
        
        Args:
            card_details: Список деталей карт из API
            existing_names: Список названий карт из базы данных
            
        Returns:
            Словарь соответствий card_name -> api_id
        """
        name_to_id_map = {}
        
        for detail in card_details:
            api_name = detail.get('name')
            api_id = detail.get('id')
            
            if not api_id:
                continue
            
            # Проверить, существует ли эта карта в базе данных (точное совпадение)
            if api_name in existing_names:
                name_to_id_map[api_name] = api_id
                logger.debug(f"Сопоставлено {api_name} -> id={api_id}")
        
        return name_to_id_map
    
    def _backfill_single_card(self, card_name: str, api_id: int, 
                              max_days_back: int) -> int:
        """
        Заполнить исторические данные для одной карты гаданий.
        
        Args:
            card_name: Название карты
            api_id: API ID карты
            max_days_back: Максимальное количество дней для просмотра назад
            
        Returns:
            Количество вставленных записей
        """
        # Получить существующие даты для этой карты
        existing_dates = self._get_existing_card_dates(card_name)
        
        # Получить исторические данные из API
        historical_data = self._fetch_card_history(api_id)
        if not historical_data:
            return 0
        
        # Обработать и отфильтровать данные
        records_to_insert = []
        today = datetime.now().date()
        
        for entry in historical_data:
            days_ago = entry.get('daysAgo')
            if days_ago is None or days_ago > max_days_back:
                continue
            
            # Вычислить дату для этой записи
            entry_date = today - timedelta(days=days_ago)
            
            # Проверить, есть ли уже данные за эту дату
            if entry_date in existing_dates:
                continue
            
            # Вычислить значения
            record = self._process_card_entry(entry, card_name, entry_date)
            if record:
                records_to_insert.append(record)
        
        # Вставить записи в базу данных
        if records_to_insert:
            self._insert_card_records(records_to_insert)
            logger.info(f"Вставлено {len(records_to_insert)} записей для {card_name}")
        
        return len(records_to_insert)
    
    def _get_existing_card_dates(self, card_name: str) -> set:
        """
        Получить набор существующих дат для карты.
        
        Args:
            card_name: Название карты
            
        Returns:
            Набор дат
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("""
                        SELECT DISTINCT DATE(timestamp) as date
                        FROM divination_cards
                        WHERE league_id = :league_id AND card_name = :card_name
                    """),
                    {"league_id": self.league_id, "card_name": card_name}
                )
                dates = {row[0] for row in result}
                return dates
        except Exception as e:
            logger.error(f"Ошибка при получении существующих дат для {card_name}: {e}", exc_info=True)
            return set()
    
    def _fetch_card_history(self, api_id: int) -> Optional[List[Dict]]:
        """
        Получить исторические данные для карты гаданий из API.
        
        Args:
            api_id: API ID карты
            
        Returns:
            Список исторических записей или None
        """
        url = f"https://poe.ninja/poe1/api/economy/stash/current/item/history?league={self.league_name}&type=DivinationCard&id={api_id}"
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            return data
            
        except requests.exceptions.Timeout:
            logger.error(f"Тайм-аут при получении истории карты для id={api_id}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка запроса при получении истории карты для id={api_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Ошибка при получении истории карты: {e}", exc_info=True)
            return None
    
    def _process_card_entry(self, entry: Dict, card_name: str, 
                            entry_date: datetime.date) -> Optional[Dict]:
        """
        Обработать одну историческую запись карты гаданий.
        
        Args:
            entry: Необработанная запись из API
            card_name: Название карты
            entry_date: Дата для этой записи
            
        Returns:
            Обработанный словарь записи или None
        """
        try:
            count = entry.get('count', 0)
            value = entry.get('value', 0)
            
            # Создать запись
            record = {
                'timestamp': datetime.combine(entry_date, datetime.min.time()),
                'league_id': self.league_id,
                'card_name': card_name,
                'stack_size': None,  # Недоступно в историческом API
                'chaos_value': value,
                'trade_count': count,
                'details_id': None  # Недоступно в историческом API
            }
            
            return record
            
        except Exception as e:
            logger.error(f"Ошибка при обработке записи карты: {e}", exc_info=True)
            return None
    
    def _insert_card_records(self, records: List[Dict]):
        """
        Вставить записи карт гаданий в базу данных.
        
        Args:
            records: Список словарей записей
        """
        try:
            df = pd.DataFrame(records)
            df.to_sql('divination_cards', self.engine, if_exists='append', index=False)
        except Exception as e:
            logger.error(f"Ошибка при вставке записей карт: {e}", exc_info=True)
            raise
    
    def backfill_unique_items(self, max_days_back: int = 90) -> Tuple[int, int]:
        """
        Заполнить исторические данные уникальных предметов.
        
        Args:
            max_days_back: Максимальное количество дней для просмотра назад
            
        Returns:
            Кортеж (обработано_предметов, вставлено_записей)
        """
        logger.info(f"Начало заполнения уникальных предметов для лиги: {self.league_name}")
        
        # Шаг 1: Получить детали предметов из текущего API
        item_details = self._fetch_item_details()
        if not item_details:
            logger.error("Не удалось получить детали предметов")
            return (0, 0)
        
        # Шаг 2: Получить существующие названия предметов из базы данных
        existing_items = self._get_existing_item_names()
        
        # Шаг 3: Сопоставить названия из базы с ID из API
        name_to_id_map = self._map_item_names_to_ids(item_details, existing_items)
        
        if not name_to_id_map:
            logger.warning("Не найдено соответствий предметов")
            return (0, 0)
        
        logger.info(f"Найдено {len(name_to_id_map)} соответствий предметов")
        
        # Шаг 4: Для каждого предмета получить исторические данные и заполнить пробелы
        total_records = 0
        items_processed = 0
        
        for item_name, api_id in name_to_id_map.items():
            try:
                records = self._backfill_single_item(item_name, api_id, max_days_back)
                if records > 0:
                    total_records += records
                    items_processed += 1
                    
            except Exception as e:
                logger.error(f"Ошибка при заполнении предмета {item_name}: {e}", exc_info=True)
                continue
        
        logger.info(f"Заполнение уникальных предметов завершено: {items_processed} предметов, {total_records} записей")
        return (items_processed, total_records)
    
    def _fetch_item_details(self) -> Optional[List[Dict]]:
        """
        Получить детали уникальных предметов из текущего API.
        
        Returns:
            Список словарей с деталями предметов или None
        """
        url = f"https://poe.ninja/poe1/api/economy/stash/current/item/overview?league={self.league_name}&type=UniqueWeapon"
        
        try:
            logger.info(f"Получение деталей уникальных предметов из API")
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            item_details = data.get('lines', [])
            
            logger.info(f"Получено {len(item_details)} деталей предметов")
            return item_details
            
        except requests.exceptions.Timeout:
            logger.error("Тайм-аут при получении деталей предметов")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка запроса при получении деталей предметов: {e}")
            return None
        except Exception as e:
            logger.error(f"Ошибка при получении деталей предметов: {e}", exc_info=True)
            return None
    
    def _get_existing_item_names(self) -> List[str]:
        """
        Получить список существующих названий предметов из базы данных.
        
        Returns:
            Список названий предметов
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("""
                        SELECT DISTINCT item_name 
                        FROM unique_items 
                        WHERE league_id = :league_id
                    """),
                    {"league_id": self.league_id}
                )
                names = [row[0] for row in result]
                logger.info(f"Найдено {len(names)} существующих предметов в базе данных")
                return names
        except Exception as e:
            logger.error(f"Ошибка при получении существующих названий предметов: {e}", exc_info=True)
            return []
    
    def _map_item_names_to_ids(self, item_details: List[Dict], 
                               existing_names: List[str]) -> Dict[str, int]:
        """
        Сопоставить названия предметов из базы с ID из API.
        
        Args:
            item_details: Список деталей предметов из API
            existing_names: Список названий предметов из базы данных
            
        Returns:
            Словарь соответствий item_name -> api_id
        """
        name_to_id_map = {}
        
        for detail in item_details:
            api_name = detail.get('name')
            api_id = detail.get('id')
            
            if not api_id:
                continue
            
            # Проверить, существует ли этот предмет в базе данных (точное совпадение)
            if api_name in existing_names:
                name_to_id_map[api_name] = api_id
                logger.debug(f"Сопоставлено {api_name} -> id={api_id}")
        
        return name_to_id_map
    
    def _backfill_single_item(self, item_name: str, api_id: int, 
                              max_days_back: int) -> int:
        """
        Заполнить исторические данные для одного уникального предмета.
        
        Args:
            item_name: Название предмета
            api_id: API ID предмета
            max_days_back: Максимальное количество дней для просмотра назад
            
        Returns:
            Количество вставленных записей
        """
        # Получить существующие даты для этого предмета
        existing_dates = self._get_existing_item_dates(item_name)
        
        # Получить исторические данные из API
        historical_data = self._fetch_item_history(api_id)
        if not historical_data:
            return 0
        
        # Обработать и отфильтровать данные
        records_to_insert = []
        today = datetime.now().date()
        
        for entry in historical_data:
            days_ago = entry.get('daysAgo')
            if days_ago is None or days_ago > max_days_back:
                continue
            
            # Вычислить дату для этой записи
            entry_date = today - timedelta(days=days_ago)
            
            # Проверить, есть ли уже данные за эту дату
            if entry_date in existing_dates:
                continue
            
            # Вычислить значения
            record = self._process_item_entry(entry, item_name, entry_date)
            if record:
                records_to_insert.append(record)
        
        # Вставить записи в базу данных
        if records_to_insert:
            self._insert_item_records(records_to_insert)
            logger.info(f"Вставлено {len(records_to_insert)} записей для {item_name}")
        
        return len(records_to_insert)
    
    def _get_existing_item_dates(self, item_name: str) -> set:
        """
        Получить набор существующих дат для предмета.
        
        Args:
            item_name: Название предмета
            
        Returns:
            Набор дат
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("""
                        SELECT DISTINCT DATE(timestamp) as date
                        FROM unique_items
                        WHERE league_id = :league_id AND item_name = :item_name
                    """),
                    {"league_id": self.league_id, "item_name": item_name}
                )
                dates = {row[0] for row in result}
                return dates
        except Exception as e:
            logger.error(f"Ошибка при получении существующих дат для {item_name}: {e}", exc_info=True)
            return set()
    
    def _fetch_item_history(self, api_id: int) -> Optional[List[Dict]]:
        """
        Получить исторические данные для уникального предмета из API.
        
        Args:
            api_id: API ID предмета
            
        Returns:
            Список исторических записей или None
        """
        url = f"https://poe.ninja/poe1/api/economy/stash/current/item/history?league={self.league_name}&type=UniqueWeapon&id={api_id}"
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            return data
            
        except requests.exceptions.Timeout:
            logger.error(f"Тайм-аут при получении истории предмета для id={api_id}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка запроса при получении истории предмета для id={api_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Ошибка при получении истории предмета: {e}", exc_info=True)
            return None
    
    def _process_item_entry(self, entry: Dict, item_name: str, 
                            entry_date: datetime.date) -> Optional[Dict]:
        """
        Обработать одну историческую запись уникального предмета.
        
        Args:
            entry: Необработанная запись из API
            item_name: Название предмета
            entry_date: Дата для этой записи
            
        Returns:
            Обработанный словарь записи или None
        """
        try:
            value = entry.get('value', 0)
            
            # Создать запись
            record = {
                'timestamp': datetime.combine(entry_date, datetime.min.time()),
                'league_id': self.league_id,
                'item_name': item_name,
                'base_type': None,  # Недоступно в историческом API
                'item_type': None,  # Недоступно в историческом API
                'level_required': None,  # Недоступно в историческом API
                'chaos_value': value,
                'links': None,  # Недоступно в историческом API
                'details_id': None  # Недоступно в историческом API
            }
            
            return record
            
        except Exception as e:
            logger.error(f"Ошибка при обработке записи предмета: {e}", exc_info=True)
            return None
    
    def _insert_item_records(self, records: List[Dict]):
        """
        Вставить записи уникальных предметов в базу данных.
        
        Args:
            records: Список словарей записей
        """
        try:
            df = pd.DataFrame(records)
            df.to_sql('unique_items', self.engine, if_exists='append', index=False)
        except Exception as e:
            logger.error(f"Ошибка при вставке записей предметов: {e}", exc_info=True)
            raise
    
    def backfill_all(self, max_days_back: int = 90) -> Dict[str, Tuple[int, int]]:
        """
        Заполнить все типы данных для лиги.
        
        Args:
            max_days_back: Максимальное количество дней для просмотра назад
            
        Returns:
            Словарь с результатами для каждого типа данных
        """
        logger.info(f"Начало полного заполнения для лиги: {self.league_name}")
        
        results = {
            'currency': self.backfill_currency(max_days_back),
            'divination_cards': self.backfill_divination_cards(max_days_back),
            'unique_items': self.backfill_unique_items(max_days_back)
        }
        
        total_items = sum(r[0] for r in results.values())
        total_records = sum(r[1] for r in results.values())
        
        logger.info(
            f"Полное заполнение завершено:\n"
            f"  Всего обработано предметов: {total_items}\n"
            f"  Всего вставлено записей: {total_records}\n"
            f"  Валюты: {results['currency'][0]} предметов, {results['currency'][1]} записей\n"
            f"  Карты гаданий: {results['divination_cards'][0]} предметов, {results['divination_cards'][1]} записей\n"
            f"  Уникальные предметы: {results['unique_items'][0]} предметов, {results['unique_items'][1]} записей"
        )
        
        return results