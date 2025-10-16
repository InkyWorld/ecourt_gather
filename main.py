# ecourt_gather/main.py
from config.logger import get_logger
from database.database import initialize_database, get_db_session
from repo.documents import DocumentRepository
from services.documents import DocumentService

logger = get_logger(__name__)

def main():
    """Головна функція для запуску процесу збору даних."""
    initialize_database()
    with get_db_session() as db_session:
        try:
            doc_repo = DocumentRepository(session=db_session)
            doc_service = DocumentService(document_repo=doc_repo)
            
            base_link = "https://stage-api-corp.court.gov.ua/api/v1/"
            
            doc_service.gather_documents(
                base_link,
                start_date="2023-08-25",
                end_date="2023-08-27",
                doc_type="data",
            )
            doc_service.gather_documents(
                base_link,
                start_date="2023-08-25",
                end_date="2025-08-27",
                doc_type="party",
            )
        except Exception as e:
            logger.critical(f"Критична помилка в main: {e}", exc_info=True)


if __name__ == "__main__":
    main()