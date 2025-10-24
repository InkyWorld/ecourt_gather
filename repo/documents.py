import json
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote_plus

from sqlalchemy.orm import Session
from sqlalchemy import create_engine, MetaData, Table, select

from config.logger import get_logger
from database.models import Document_data, Document_party_docs
from config.config import (
    GOV_REG_DB_NAME,
    GOV_REG_DB_PASSWORD,
    GOV_REG_DB_SERVER,
    GOV_REG_DB_USER,
    DB_DRIVER,
)

logger = get_logger(__name__)


class DocumentRepository:
    def __init__(self, session: Session, folder, company: str):
        self.session = session
        self.folder = Path(folder)
        self.company = company

    def save_document(
        self,
        original_url: str,
        file_content: bytes,
        file_name: str,
        size_in_bytes: int,
        doc_type: str,
        doc_id: str
    ):
        """
        Зберігає новий документ у базі даних.
        """

        try:
            (self.folder / "data_docs").mkdir(parents=True, exist_ok=True)
            (self.folder / "party_docs").mkdir(parents=True, exist_ok=True)
            if doc_type == "data":
                file_path = self.folder / "data_docs" / file_name
            else:
                file_path = self.folder / "party_docs" / file_name

            with open(file_path, "wb") as f:
                f.write(file_content)

            if doc_type == "data":
                db_document = Document_data(
                    original_url=original_url,
                    local_path=file_name,
                    size=size_in_bytes,
                    doc_id=doc_id,
                )
            elif doc_type == "party":
                db_document = Document_party_docs(
                    original_url=original_url,
                    local_path=file_name,
                    size=size_in_bytes,
                    doc_id=doc_id,
                )
            self.session.add(db_document)
            self.session.commit()
        except Exception as e:
            logger.error(f"Помилка збереження файлу в БД: {e}", exc_info=True)
            self.session.rollback()
            raise

    def find_by_file_link(self, original_url: str, doc_type: str):
        """
        Знаходить документ за полем file_link.

        Args:
            file_link: Посилання на файл для пошуку.

        Returns:
            Об'єкт Document, якщо знайдено, інакше None.
        """
        try:
            if doc_type == "data":
                model = Document_data
            elif doc_type == "party":
                model = Document_party_docs
            return (
                self.session.query(model)
                .filter(model.original_url == original_url)
                .first()
            )
        except Exception as e:
            logger.error(
                f"Помилка пошуку документа за посиланням '{original_url}': {e}",
                exc_info=True,
            )
            return None

    def _fetch_data_from_db_by_date_range(
        self, start_date: str, end_date: str, doc_type
    ) -> Optional[List[Dict]]:
        try:
            metadata = MetaData()
            password_encoded = quote_plus(GOV_REG_DB_PASSWORD)
            driver_encoded = quote_plus(DB_DRIVER)
            db_url = (
                f"mssql+pyodbc://{GOV_REG_DB_USER}:{password_encoded}@{GOV_REG_DB_SERVER}/{GOV_REG_DB_NAME}?"
                f"driver={driver_encoded}&TrustServerCertificate=yes"
            )
            if doc_type == "data":
                table_name = "document" + self.company
                updated_at_c = "UpdatedAt" if GOV_REG_DB_NAME == "Ace" else "updatedAt"
            else:
                table_name = "partyDocs" + self.company
                updated_at_c = "updatedAt"

            engine = create_engine(db_url)
            table = Table(table_name, metadata, autoload_with=engine)

            date_column = table.c[updated_at_c]

            query = select(table).where(date_column.between(start_date, end_date))

            results_list = []
            with engine.connect() as connection:
                result = connection.execute(query)
                for row in result:
                    results_list.append(row._asdict())
            logger.info(f"Для цього проміжку знайдено {len(results_list)} записів")
            return results_list if results_list else None

        except Exception as e:
            print(f"Сталася помилка: {e}")
