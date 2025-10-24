from datetime import datetime, timedelta, timezone

from config.config import BASE_LINK_AND_API_VERSION, TOKENS_FOLDERS_COMPANIES
from config.logger import get_logger
from database.database import get_db_session, initialize_database
from repo.documents import DocumentRepository
from services.documents import DocumentService

logger = get_logger(__name__)


def main():
    """Головна функція для запуску процесу збору даних."""
    initialize_database()
    with get_db_session() as db_session:
        try:
            for token, (folder, company) in TOKENS_FOLDERS_COMPANIES.items():
                doc_repo = DocumentRepository(
                    session=db_session, folder=folder, company=company
                )
                data_doc_service = DocumentService(
                    document_repo=doc_repo,
                    doc_type="data",
                    token=token,
                    company=company,
                )
                party_doc_service = DocumentService(
                    document_repo=doc_repo,
                    doc_type="party",
                    token=token,
                    company=company,
                )
                now = datetime.now(timezone.utc)
                start_date = (now - timedelta(weeks=5000, hours=24, seconds=10)).isoformat()
                end_date = (now + timedelta(seconds=10)).isoformat()

                # start_date = "2025-10-15T01:01:37.000Z"
                # end_date = "2025-10-17T20:29:37.000Z"
                logger.info(f"Період збору документів data_doc: {start_date} по {end_date}")

                data_doc_service.gather_documents(
                    BASE_LINK_AND_API_VERSION,
                    start_date=start_date,
                    end_date=end_date,
                )

                logger.info(f"Період збору документів party_doc: {start_date} по {end_date}")
                party_doc_service.gather_documents(
                    BASE_LINK_AND_API_VERSION,
                    start_date=start_date,
                    end_date=end_date,
                )
        except Exception as e:
            logger.critical(f"Критична помилка в main: {e}", exc_info=True)


if __name__ == "__main__":
    main()
