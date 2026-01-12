"""
Модуль управления соединениями

Предоставляет надежный HTTP-клиент и управление соединениями с базой данных:
- Пул соединений и их повторное использование
- Экспоненциальная задержка с рандомизацией
- Автоматическая логика повторных попыток
- Паттерн автоматического выключателя (circuit breaker)
- Комплексное логирование
- Graceful error handling
"""

import time
import random
import logging
from typing import Optional, Dict, Any, Callable
from functools import wraps
from contextlib import contextmanager

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from sqlalchemy import create_engine, Engine
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy.pool import QueuePool

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HTTPClient:
    """
    HTTP-клиент с пулом соединений, логикой повторных попыток и отказоустойчивостью.
    
    Возможности:
    - Пул соединений через requests.Session
    - Экспоненциальная задержка с рандомизацией
    - Настраиваемое количество повторных попыток и таймауты
    - Автоматический повтор при временных сбоях
    - Circuit breaker для предотвращения перегрузки неработающих endpoints
    - Комплексное логирование
    """
    
    def __init__(
        self,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        connect_timeout: float = 10.0,
        read_timeout: float = 30.0,
        pool_connections: int = 10,
        pool_maxsize: int = 10,
        enable_circuit_breaker: bool = True,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_timeout: float = 60.0
    ):
        """
        Инициализация HTTP-клиента.
        
        Args:
            max_retries: Максимальное количество попыток повтора
            backoff_factor: Множитель для экспоненциальной задержки (секунды)
            connect_timeout: Таймаут соединения в секундах
            read_timeout: Таймаут чтения в секундах
            pool_connections: Количество пулов соединений для кеширования
            pool_maxsize: Максимальное количество соединений в пуле
            enable_circuit_breaker: Включить паттерн circuit breaker
            circuit_breaker_threshold: Сбоев перед открытием цепи
            circuit_breaker_timeout: Секунд ожидания перед повтором после открытия цепи
        """
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.enable_circuit_breaker = enable_circuit_breaker
        self.circuit_breaker_threshold = circuit_breaker_threshold
        self.circuit_breaker_timeout = circuit_breaker_timeout
        
        # Circuit breaker state
        self._failure_counts: Dict[str, int] = {}
        self._circuit_open_until: Dict[str, float] = {}
        
        # Create session with connection pooling
        self.session = self._create_session(pool_connections, pool_maxsize)
        
        logger.info(
            f"HTTPClient initialized: max_retries={max_retries}, "
            f"backoff_factor={backoff_factor}, timeout=({connect_timeout}s, {read_timeout}s)"
        )
    
    def _create_session(self, pool_connections: int, pool_maxsize: int) -> requests.Session:
        """Создать сессию requests со стратегией повтора и пулом соединений."""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "PUT", "DELETE"],
            raise_on_status=False
        )
        
        # Mount adapters with retry strategy and connection pooling
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=pool_connections,
            pool_maxsize=pool_maxsize,
            pool_block=False
        )
        
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set default headers
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
        
        return session
    
    def _check_circuit_breaker(self, url: str) -> bool:
        """Проверить, открыт ли circuit breaker для этого URL."""
        if not self.enable_circuit_breaker:
            return False
        
        circuit_key = self._get_circuit_key(url)
        
        # Check if circuit is open
        if circuit_key in self._circuit_open_until:
            if time.time() < self._circuit_open_until[circuit_key]:
                logger.warning(
                    f"Circuit breaker OPEN for {url}. "
                    f"Retry after {self._circuit_open_until[circuit_key] - time.time():.1f}s"
                )
                return True
            else:
                # Circuit closed, reset failure count
                self._failure_counts[circuit_key] = 0
                del self._circuit_open_until[circuit_key]
                logger.info(f"Circuit breaker CLOSED for {url}")
        
        return False
    
    def _get_circuit_key(self, url: str) -> str:
        """Извлечь ключ circuit breaker из URL (домен + путь)."""
        from urllib.parse import urlparse
        parsed = urlparse(url)
        return f"{parsed.netloc}{parsed.path}"
    
    def _record_failure(self, url: str):
        """Записать сбой и потенциально открыть circuit breaker."""
        if not self.enable_circuit_breaker:
            return
        
        circuit_key = self._get_circuit_key(url)
        self._failure_counts[circuit_key] = self._failure_counts.get(circuit_key, 0) + 1
        
        if self._failure_counts[circuit_key] >= self.circuit_breaker_threshold:
            self._circuit_open_until[circuit_key] = time.time() + self.circuit_breaker_timeout
            logger.error(
                f"Circuit breaker OPENED for {url} after {self._failure_counts[circuit_key]} failures. "
                f"Will retry after {self.circuit_breaker_timeout}s"
            )
    
    def _record_success(self, url: str):
        """Записать успех и сбросить circuit breaker при необходимости."""
        if not self.enable_circuit_breaker:
            return
        
        circuit_key = self._get_circuit_key(url)
        if circuit_key in self._failure_counts:
            self._failure_counts[circuit_key] = 0
            logger.debug(f"Circuit breaker reset for {url}")
    
    def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[tuple] = None,
        allow_redirects: bool = True
    ) -> Optional[requests.Response]:
        """
        Выполнить GET-запрос с логикой повторных попыток и отказоустойчивостью.
        
        Args:
            url: Целевой URL
            params: Параметры запроса
            headers: Дополнительные заголовки
            timeout: Кортеж таймаутов (connect_timeout, read_timeout)
            allow_redirects: Следовать ли перенаправлениям
            
        Returns:
            Объект Response при успехе, None при неудаче после всех попыток
        """
        # Check circuit breaker
        if self._check_circuit_breaker(url):
            return None
        
        # Use default timeout if not provided
        if timeout is None:
            timeout = (self.connect_timeout, self.read_timeout)
        
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                logger.debug(f"GET {url} (attempt {attempt + 1}/{self.max_retries + 1})")
                
                response = self.session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=timeout,
                    allow_redirects=allow_redirects
                )
                
                # Check for HTTP errors
                if response.status_code >= 400:
                    logger.warning(
                        f"HTTP {response.status_code} for {url} (attempt {attempt + 1})"
                    )
                    
                    # Don't retry on 4xx errors (except 429)
                    if 400 <= response.status_code < 500 and response.status_code != 429:
                        self._record_failure(url)
                        return None
                    
                    raise requests.HTTPError(f"HTTP {response.status_code}", response=response)
                
                # Success
                self._record_success(url)
                logger.info(f"Successfully fetched {url} (status={response.status_code})")
                return response
                
            except requests.exceptions.Timeout as e:
                last_exception = e
                logger.warning(f"Timeout for {url} (attempt {attempt + 1}): {e}")
                
            except requests.exceptions.ConnectionError as e:
                last_exception = e
                logger.warning(f"Connection error for {url} (attempt {attempt + 1}): {e}")
                
            except requests.exceptions.HTTPError as e:
                last_exception = e
                logger.warning(f"HTTP error for {url} (attempt {attempt + 1}): {e}")
                
            except requests.exceptions.RequestException as e:
                last_exception = e
                logger.warning(f"Request exception for {url} (attempt {attempt + 1}): {e}")
            
            # Don't sleep after last attempt
            if attempt < self.max_retries:
                # Exponential backoff with jitter
                sleep_time = self.backoff_factor * (2 ** attempt) + random.uniform(0, 1)
                logger.debug(f"Sleeping {sleep_time:.2f}s before retry")
                time.sleep(sleep_time)
        
        # All retries failed
        self._record_failure(url)
        logger.error(
            f"All {self.max_retries + 1} attempts failed for {url}. "
            f"Last error: {last_exception}"
        )
        return None
    
    def close(self):
        """Close the session and cleanup resources."""
        self.session.close()
        logger.info("HTTPClient session closed")


class DatabaseConnectionManager:
    """
    Менеджер соединений с базой данных с проверкой здоровья и автоматическим переподключением.
    
    Возможности:
    - Пул соединений
    - Проверка здоровья с ping
    - Автоматическое переподключение при сбоях
    - Логика повторных попыток для SQL-операций
    - Управление транзакциями
    """
    
    def __init__(
        self,
        db_url: str,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: int = 30,
        pool_recycle: int = 3600,
        pool_pre_ping: bool = True
    ):
        """
        Инициализация менеджера соединений с базой данных.
        
        Args:
            db_url: URL соединения с базой данных
            pool_size: Размер пула соединений
            max_overflow: Максимальное количество дополнительных соединений
            pool_timeout: Таймаут получения соединения из пула
            pool_recycle: Перерабатывать соединения через это количество секунд
            pool_pre_ping: Проверять соединения перед использованием
        """
        self.db_url = db_url
        self.engine = self._create_engine(
            pool_size, max_overflow, pool_timeout, pool_recycle, pool_pre_ping
        )
        
        logger.info(
            f"DatabaseConnectionManager initialized: pool_size={pool_size}, "
            f"max_overflow={max_overflow}, pool_recycle={pool_recycle}s"
        )
    
    def _create_engine(
        self,
        pool_size: int,
        max_overflow: int,
        pool_timeout: int,
        pool_recycle: int,
        pool_pre_ping: bool
    ) -> Engine:
        """Создать SQLAlchemy engine с оптимизированным пулом соединений."""
        return create_engine(
            self.db_url,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle,
            pool_pre_ping=pool_pre_ping,
            echo=False
        )
    
    def check_connection(self) -> bool:
        """
        Проверить, здорово ли соединение с базой данных.
        
        Returns:
            True если соединение здорово, False в противном случае
        """
        try:
            with self.engine.connect() as conn:
                conn.execute("SELECT 1")
            logger.debug("Database connection check passed")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Database connection check failed: {e}")
            return False
    
    def get_engine(self) -> Engine:
        """Получить SQLAlchemy engine."""
        return self.engine
    
    def dispose(self):
        """Освободить пул соединений."""
        self.engine.dispose()
        logger.info("Database connection pool disposed")


# Global instances for reuse
_http_client: Optional[HTTPClient] = None
_db_manager: Optional[DatabaseConnectionManager] = None


def get_http_client() -> HTTPClient:
    """Получить или создать глобальный экземпляр HTTP-клиента."""
    global _http_client
    if _http_client is None:
        _http_client = HTTPClient()
    return _http_client


def get_db_manager(db_url: Optional[str] = None) -> DatabaseConnectionManager:
    """Получить или создать глобальный экземпляр менеджера базы данных."""
    global _db_manager
    if _db_manager is None:
        if db_url is None:
            raise ValueError("db_url must be provided for first initialization")
        _db_manager = DatabaseConnectionManager(db_url)
    return _db_manager


def with_retry(max_retries: int = 3, backoff_factor: float = 1.0):
    """
    Декоратор для добавления логики повторных попыток к любой функции.
    
    Args:
        max_retries: Максимальное количество попыток повтора
        backoff_factor: Множитель для экспоненциальной задержки
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    logger.warning(
                        f"{func.__name__} failed (attempt {attempt + 1}/{max_retries + 1}): {e}"
                    )
                    
                    if attempt < max_retries:
                        sleep_time = backoff_factor * (2 ** attempt) + random.uniform(0, 1)
                        logger.debug(f"Sleeping {sleep_time:.2f}s before retry")
                        time.sleep(sleep_time)
            
            logger.error(
                f"{func.__name__} failed after {max_retries + 1} attempts. "
                f"Last error: {last_exception}"
            )
            raise last_exception
        
        return wrapper
    return decorator


@contextmanager
def managed_transaction(engine: Engine):
    """
    Контекстный менеджер для транзакций базы данных с автоматическим откатом при ошибке.
    
    Args:
        engine: SQLAlchemy engine
        
    Yields:
        Объект соединения
    """
    conn = None
    try:
        conn = engine.connect()
        trans = conn.begin()
        yield conn
        trans.commit()
        logger.debug("Transaction committed successfully")
    except Exception as e:
        if conn:
            trans.rollback()
            logger.error(f"Transaction rolled back due to error: {e}")
        raise
    finally:
        if conn:
            conn.close()