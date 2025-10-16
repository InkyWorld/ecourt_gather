import json
import os
import sys
from datetime import datetime, time, timezone
from typing import Dict, List, Optional
import requests
from dotenv import load_dotenv

from config.config import BASE_DIR
from config.logger import get_logger
from repo.documents import DocumentRepository

logger = get_logger(__name__)
load_dotenv()


class DocumentService:
    def __init__(self, document_repo: DocumentRepository):
        self.document_repo = document_repo

    def _fetch_data(
        self, url: str, query_params: Optional[Dict] = None
    ) -> Optional[List[Dict]]:
        token = os.getenv("API_BEARER_TOKEN")
        if not token:
            logger.critical("Не знайдено API_BEARER_TOKEN. Роботу зупинено.")
            sys.exit(1)

        headers = {"Authorization": f"Bearer {token}"}

        try:
            logger.info(f"Надсилаємо запит до {url}")
            response = requests.get(
                url, headers=headers, params=query_params, timeout=360
            )
            response.raise_for_status()
            api_data = response.json()
            data_list = api_data.get("data")

            if data_list is None:
                logger.warning("У відповіді відсутній ключ 'data'.")
            else:
                logger.info(
                    f"Дані успішно отримано. Кількість елементів: {len(data_list) if isinstance(data_list, list) else 'N/A'}"
                )

            return data_list

        except requests.exceptions.RequestException as e:
            logger.error(f"Помилка запиту до API: {e}", exc_info=True)
            return None

    def _fetch_data_by_date_range(
        self, url: str, start_date: str, end_date: str
    ) -> Optional[List[Dict]]:
        start_dt = datetime.combine(
            datetime.strptime(start_date, "%Y-%m-%d"), time.min
        ).replace(tzinfo=timezone.utc)
        end_dt = datetime.combine(
            datetime.strptime(end_date, "%Y-%m-%d"), time.max
        ).replace(tzinfo=timezone.utc)

        logger.info(
            f"Отримання даних за період з {start_dt.isoformat()} по {end_dt.isoformat()}."
        )

        filters = [
            f"createdAt||$gte||{start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}",
            f"createdAt||$lte||{end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}",
        ]

        return self._fetch_data(url, query_params={"filter": filters})

    def _download_and_save_to_db(self, base_url: str, file_link: str):
        token = os.getenv("API_BEARER_TOKEN")
        if not token:
            logger.critical("Не знайдено API_BEARER_TOKEN.")
            return

        headers = {"Authorization": f"Bearer {token}"}
        try:
            response = requests.get(base_url, headers=headers, stream=True, timeout=360)
            response.raise_for_status()
            file_content = response.content
            self.document_repo.save_document(file_link, file_content)

        except requests.exceptions.RequestException as e:
            logger.error(f"Помилка завантаження файлу {base_url}: {e}", exc_info=True)

    def gather_documents(
        self, base_link: str, doc_type: str, start_date: str = "2000-01-01", end_date: str = "3000-01-01", update_today_only: bool = False
    ):
        endpoint = "data/document" if doc_type == "data" else "party-docs/document"
        documents = self._fetch_data_by_date_range(
            f"{base_link}{endpoint}", start_date, end_date
        )

        if not documents:
            logger.warning(
                f"Не знайдено документів типу '{doc_type}' за вказаний період."
            )
            return

        for doc in documents:
            if update_today_only:
                try:
                    updatedAt: str | None = doc.get("updatedAt")
                    updated_at_datetime = datetime.fromisoformat(
                        updatedAt.replace("Z", "+00:00")
                    )
                    today_utc_date = datetime.now(timezone.utc).date()
                    if updated_at_datetime.date() != today_utc_date:
                        continue
                except (ValueError, TypeError):
                    logger.warning(f"Некоректний формат дати 'updatedAt': {updatedAt}")
                    continue
            doc_id = doc.get("id")
            if not doc_id:
                continue

            if doc_type == "data":
                attachments = [doc.get("original", {})]
            else:
                attachments = doc.get("attachments", [])

            links = []
            for attachment in attachments:
                link = attachment.get("link")
                if not link:
                    continue
                if self.document_repo.find_by_file_link(link):
                    continue
                links.append(link)
                self._download_and_save_to_db(
                    f"{base_link}storage/file/{link}",
                    link,
                )
