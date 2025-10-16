# ecourt_gather/repo/document_repository.py
from typing import Optional
from sqlalchemy.orm import Session
from database.models import Document
from config.logger import get_logger

logger = get_logger(__name__)

class DocumentRepository:
    def __init__(self, session: Session):
        self.session = session

    def save_document(self, file_link: str, file_content: bytes):
        """
        Зберігає новий документ у базі даних.
        """
        try:
            db_document = Document(
                file_link=file_link,
                file_content=file_content
            )
            self.session.add(db_document)
            self.session.commit()
            logger.info(f"Файл '{file_link}' збережено в БД.")
        except Exception as e:
            logger.error(f"Помилка збереження файлу в БД: {e}", exc_info=True)
            self.session.rollback()
            
    def find_by_file_link(self, file_link: str) -> Optional[Document]:
        """
        Знаходить документ за полем file_link.

        Args:
            file_link: Посилання на файл для пошуку.

        Returns:
            Об'єкт Document, якщо знайдено, інакше None.
        """
        try:
            return self.session.query(Document).filter(Document.file_link == file_link).first()
        except Exception as e:
            logger.error(f"Помилка пошуку документа за посиланням '{file_link}': {e}", exc_info=True)
            return None