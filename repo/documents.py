# ecourt_gather/repo/document_repository.py
from sqlalchemy.orm import Session
from database.models import Document
from config.logger import get_logger

logger = get_logger(__name__)

class DocumentRepository:
    def __init__(self, session: Session):
        self.session = session

    def save_document(self, file_name: str, file_content: bytes):
        """
        Зберігає новий документ у базі даних.
        """
        try:
            db_document = Document(
                file_name=file_name,
                file_content=file_content
            )
            self.session.add(db_document)
            self.session.commit()
            logger.info(f"Файл '{file_name}' збережено в БД.")
        except Exception as e:
            logger.error(f"Помилка збереження файлу в БД: {e}", exc_info=True)
            self.session.rollback()