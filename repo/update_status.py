from sqlalchemy import Table, MetaData, update, Column, Integer, Boolean, String
from sqlalchemy.orm import Session
from config.logger import get_logger

logger = get_logger(__name__)

class UpdateStatusRepository:
    def __init__(self, session: Session):
        self.session = session
        self.engine = session.get_bind()
        self.metadata = MetaData()

    def update_downloaded_status(self, table_name: str, condition_column: str, condition_value: any, new_status: bool) -> int:
        """
        Оновлює булеве поле 'downloaded' у зазначеній таблиці за певною умовою.

        Args:
            table_name: Назва таблиці для оновлення.
            condition_column: Назва стовпця для умови WHERE.
            condition_value: Значення для умови WHERE.
            new_status: Новий булевий статус (True або False).

        Returns:
            Кількість оновлених рядків.
        """
        try:
            target_table = Table(
                table_name,
                self.metadata,
                Column('id', Integer, primary_key=True),
                Column('downloaded', Boolean),
                Column(condition_column, String)
            )

            stmt = (
                update(target_table)
                .where(getattr(target_table.c, condition_column) == condition_value)
                .values(downloaded=new_status)
            )

            trans = self.session.begin()
            try:
                result = self.session.execute(stmt)
                trans.commit()
                logger.info(f"У таблиці '{table_name}' оновлено {result.rowcount} рядків. Статус 'downloaded' змінено на {new_status}.")
                return result.rowcount
            except Exception:
                trans.rollback()
                raise
        except Exception as e:
            logger.error(f"Помилка під час оновлення статусу в таблиці '{table_name}': {e}", exc_info=True)
            return 0