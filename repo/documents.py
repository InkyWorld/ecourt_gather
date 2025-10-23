from sqlalchemy.orm import Session

from config.logger import get_logger
from database.models import Document_data, Document_party_docs

logger = get_logger(__name__)


class DocumentRepository:
    def __init__(self, session: Session, folder):
        self.session = session
        self.folder = folder

    def save_document(
        self,
        original_url: str,
        file_content: bytes,
        file_name: str,
        size_in_bytes: int,
        doc_type: str,
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
                )
            elif doc_type == "party":
                db_document = Document_party_docs(
                    original_url=original_url,
                    local_path=file_name,
                    size=size_in_bytes,
                )
            self.session.add(db_document)
            self.session.commit()
        except Exception as e:
            logger.error(f"Помилка збереження файлу в БД: {e}", exc_info=True)
            self.session.rollback()

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
