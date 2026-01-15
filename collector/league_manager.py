import logging
from datetime import datetime
from typing import Optional, List, Dict
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


class LeagueManager:
    """Управляет лигами Path of Exile. Позволяет получать и создавать лиги в базе данных."""

    def __init__(self, engine: Engine):
        """
        Инициализирует LeagueManager c базой данных

        Args:
            engine: SQLAlchemy database engine
        """
        self.engine = engine

    def get_league_id(self, league_name: str) -> Optional[int]:
        """
        Получает ID лиги из базы данных

        Args:
            league_name: Имя лиги

        Returns:
            ID лиги или None
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("SELECT id FROM leagues WHERE league_name = :league_name"),
                    {"league_name": league_name}
                )
                row = result.fetchone()
                if row:
                    return row[0]
                return None
        except Exception as e:
            logger.error(f"Error fetching league ID for {league_name}: {e}", exc_info=True)
            return None
    def get_league_name(self, league_id: int) -> Optional[str]:
        """
        Получает имя лиги по её ID.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("SELECT league_name FROM leagues WHERE id = :league_id"),
                    {"league_id": league_id}
                )
                row = result.fetchone()
                return row[0] if row else None
        except Exception as e:
            logger.error(f"Error getting league name for ID {league_id}: {e}", exc_info=True)
            return None
    def get_or_create_league(self, league_name: str, status: str = 'Active', start_date: Optional[datetime] = None) -> \
    Optional[int]:
        """
        Получет текущую лигу или создает новую

        Args:
            league_name: Имя лиги
            status: Статус лиги ('Active' или 'Expired')
            start_date: Дата начала лиги. Если None, используется CURRENT_DATE.

        Returns:
            ID лиги или None
        """
        try:
            league_id = self.get_league_id(league_name)
            if league_id:
                logger.debug(f"Found existing league: {league_name} (ID: {league_id})")
                return league_id

            # Если лига не найдена, создаем ее
            with self.engine.connect() as conn:
                # Используем переданную start_date или CURRENT_DATE
                insert_start_date = start_date if start_date else datetime.now()  # Используем datetime.now() для CURRENT_DATE

                result = conn.execute(
                    text("""
                           INSERT INTO leagues (league_name, status, start_date)
                           VALUES (:league_name, :status, :start_date)
                           RETURNING id
                       """),
                    {"league_name": league_name, "status": status, "start_date": insert_start_date}
                )
                league_id = result.fetchone()[0]
                conn.commit()
                logger.info(
                    f"Created new league: {league_name} (ID: {league_id}, Status: {status}, Start Date: {insert_start_date.strftime('%Y-%m-%d')})")
                return league_id

        except Exception as e:
            logger.error(f"Error creating league {league_name}: {e}", exc_info=True)
            return None

    def get_all_leagues(self, status: Optional[str] = None) -> List[Dict]:
        """
        Получает список всех лиг.

        Args:
            status: Статус лиг ('Active' or 'Expired')

        Returns:
            Лист словарей с лигами
        """
        try:
            with self.engine.connect() as conn:
                if status:
                    result = conn.execute(
                        text(
                            "SELECT id, league_name, status, start_date FROM leagues WHERE status = :status ORDER BY start_date DESC"),
                        {"status": status}
                    )
                else:
                    result = conn.execute(
                        text("SELECT id, league_name, status, start_date FROM leagues ORDER BY start_date DESC")
                    )

                leagues = []
                for row in result:
                    leagues.append({
                        'id': row[0],
                        'league_name': row[1],
                        'status': row[2],
                        'start_date': row[3]
                    })
                return leagues

        except Exception as e:
            logger.error(f"Error fetching leagues: {e}", exc_info=True)
            return []

    def update_league_status(self, league_name: str, status: str) -> bool:
        """
        Обновлялет статус лиги

        Args:
            league_name: Имя лиги
            status: Новый статус ('Active' или 'Expired')

        Returns:
            True если успешно, False если не удалось
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(
                    text("UPDATE leagues SET status = :status WHERE league_name = :league_name"),
                    {"status": status, "league_name": league_name}
                )
                conn.commit()
                logger.info(f"Updated league {league_name} status to {status}")
                return True
        except Exception as e:
            logger.error(f"Error updating league status: {e}", exc_info=True)
            return False

