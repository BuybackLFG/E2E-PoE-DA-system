import logging
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

    def get_or_create_league(self, league_name: str, status: str = 'Active') -> Optional[int]:
        """
        Получет текущую лигу или создает новую

        Args:
            league_name: Имя лиги
            status: Статус лиги ('Active' или 'Expired')

        Returns:
            ID лиги или None
        """
        try:

            league_id = self.get_league_id(league_name)
            if league_id:
                logger.debug(f"Found existing league: {league_name} (ID: {league_id})")  # Изменено на debug
                return league_id

            with self.engine.connect() as conn:
                result = conn.execute(
                    text("""
                        INSERT INTO leagues (league_name, status, start_date)
                        VALUES (:league_name, :status, CURRENT_DATE)
                        RETURNING id
                    """),
                    {"league_name": league_name, "status": status}
                )
                league_id = result.fetchone()[0]
                conn.commit()
                logger.info(f"Created new league: {league_name} (ID: {league_id})")
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

