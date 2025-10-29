import asyncio
import json
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote_plus

import aiofiles
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.exc import IntegrityError

from config.logger import get_logger
# from database.models import Document_data, Document_party_docs
from database.models import Documents
from config.config import (
    GOV_REG_DB_NAME,
    GOV_REG_DB_PASSWORD,
    GOV_REG_DB_SERVER,
    GOV_REG_DB_USER,
    DB_DRIVER,
)
from utils.file_hashing import calculate_file_hash_from_bytes
from database.database import SessionLocal
logger = get_logger(__name__)


class DocumentRepository:
    def __init__(self, session: Session, folder, company: str):
        self.session = session
        self.folder = Path(folder)
        self.company = company
        self.SessionLocal = SessionLocal

    def save_document(
        self,
        original_url: str,
        file_content: bytes,
        file_name: str,
        size_in_bytes: int,
    ):
        """
        Зберігає новий документ у базі даних.
        """
        new_doc = Documents(
            original_url=original_url,
            local_path=file_name,
            size=size_in_bytes,
            file_hash=calculate_file_hash_from_bytes(file_content)
        )
        self.session.add(new_doc)
        try:
            self.session.flush()
            try:
                self.folder.mkdir(parents=True, exist_ok=True)
                with open(self.folder / file_name, "wb") as f:
                    f.write(file_content)
            except IOError as e:
                logger.error(f"! Помилка збереження файлу на диск: {e}.")
                self.session.rollback()
                raise 
        except IntegrityError:
            self.session.rollback()

    @staticmethod
    def _save_document_task_sync(
        new_doc: Documents,
        folder: Path,
        file_name: str,
        file_content: bytes
    ) -> bool:
        """
        Повністю ізольована, потоко-безпечна задача для збереження одного документа.
        Створює власну сесію, виконує операції та закриває її.
        Повертає True при успіху, False при помилці.
        """
        session = SessionLocal()
        try:
            session.add(new_doc)
            session.flush()

            try:
                folder.mkdir(parents=True, exist_ok=True)
                with open(folder / file_name, "wb") as f:
                    f.write(file_content)
            except IOError:
                session.rollback()
                return False

            session.commit()
            return True

        except IntegrityError:
            session.rollback()
            return False
        except Exception as e:
            logger.error(f"Неочікувана помилка БД в _save_document_task_sync: {e}", exc_info=True)
            session.rollback()
            return False
        finally:
            session.close()
        
    async def save_document_async(
        self,
        original_url: str,
        file_content: bytes,
        file_name: str,
        size_in_bytes: int,
    ) -> bool:
        """
        Асинхронно зберігає новий документ, запускаючи
        повністю ізольовану задачу в окремому потоці.
        """
        new_doc = Documents(
            original_url=original_url,
            local_path=file_name,
            size=size_in_bytes,
            file_hash=calculate_file_hash_from_bytes(file_content)
        )

        try:
            success = await asyncio.to_thread(
                self._save_document_task_sync,
                new_doc,
                self.folder,
                file_name,
                file_content
            )
            return success
        except Exception as e:
            logger.error(
                f"Критична помилка в save_document_async (to_thread): {e}",
                exc_info=True
            )
            return False

    def find_by_file_link(self, original_url: str, doc_type: str):
        """
        Знаходить документ за полем file_link.

        Args:
            file_link: Посилання на файл для пошуку.

        Returns:
            Об'єкт Document, якщо знайдено, інакше None.
        """
        try:
            return (
                self.session.query(Documents)
                .filter(Documents.original_url == original_url)
                .all()
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
            else:
                table_name = "partyDocs" + self.company

            engine = create_engine(db_url)
            table = Table(table_name, metadata, autoload_with=engine)

            date_column = table.c["updatedAt"]

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
    
    def get_existing_links_set(self) -> set:
        """
        Завантажує множину всіх існуючих original_url з таблиці Documents.
        """
        try:
            query = select(Documents.original_url)
            results = self.session.execute(query).scalars().all()
            return set(results)
        except Exception as e:
            logger.error(f"Помилка отримання існуючих посилань: {e}", exc_info=True)
            return set()
