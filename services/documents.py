import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

import requests

from config.logger import get_logger
from repo.documents import DocumentRepository

logger = get_logger(__name__)


class DocumentService:
    def __init__(self, document_repo: DocumentRepository, doc_type: str, token):
        self.document_repo = document_repo
        self.doc_type = doc_type
        self.token = token

    def _fetch_data(self, url: str, query_params: Optional[Dict] = None):
        if not self.token:
            logger.critical("Не знайдено API_BEARER_TOKEN. Роботу зупинено.")
            sys.exit(1)

        headers = {"Authorization": f"Bearer {self.token}"}

        try:
            logger.info(f"Надсилаємо запит до {url}")
            response = requests.get(
                url, headers=headers, params=query_params, timeout=360
            )
            response.raise_for_status()
            api_data = response.json()

            if api_data.get("data") is None:
                logger.warning("У відповіді відсутній ключ 'data'.")

            return api_data

        except requests.exceptions.RequestException as e:
            logger.error(f"Помилка запиту до API: {e}", exc_info=True)
            return None

    def _fetch_data_by_date_range(
        self, url: str, start_date: str, end_date: str
    ) -> Optional[List[Dict]]:
        filters = [
            f"updatedAt||$gte||{start_date}",
            f"updatedAt||$lte||{end_date}",
        ]

        offset = 0
        documents = []
        page = 0
        pageCount = 0
        while pageCount >= page:
            new_docs = self._fetch_data(
                url, query_params={"filter": filters, "limit": 100, "offset": offset}
            )
            offset += 100
            documents.extend(new_docs.get("data"))
            if not new_docs:
                break
            if pageCount == 0:
                pageCount = new_docs["pageCount"]
                page = new_docs.get("page")
            page += 1

        return documents

    def _download_and_save_to_db(
        self, base_url: str, original_url: str, file_name: str, doc_type: str
    ):
        if not self.token:
            logger.critical("Не знайдено API_BEARER_TOKEN.")
            return

        headers = {"Authorization": f"Bearer {self.token}"}
        try:
            response = requests.get(base_url, headers=headers, stream=True, timeout=360)
            response.raise_for_status()
            file_content = response.content
            size_in_bytes = len(file_content)
            self.document_repo.save_document(
                original_url, file_content, file_name, size_in_bytes, doc_type
            )

        except requests.exceptions.RequestException as e:
            logger.error(f"Помилка завантаження файлу {base_url}: {e}", exc_info=True)

    def gather_documents(self, base_link: str, start_date: str, end_date: str):
        # endpoint = "data/document" if self.doc_type == "data" else "party-docs/document"

        documents = self.document_repo._fetch_data_from_db_by_date_range(
            start_date, end_date, self.doc_type
        )
        # documents = self._fetch_data_by_date_range(
        #     f"{base_link}{endpoint}", start_date, end_date
        # )

        if not documents:
            logger.warning(
                f"Не знайдено документів типу '{self.doc_type}' за вказаний період."
            )
            return

        for doc in documents:
            doc_id = doc.get("DocumentId")
            if not doc_id:
                continue

            if self.doc_type == "data":
                attachments = [json.loads(doc.get("OriginalText"))]
            else:
                attachments = doc.get("attachments", [])

            for attachment in attachments:
                link = attachment.get("link")
                if not link:
                    continue
                ext = Path(link).suffix
                if self.doc_type != "data":
                    file_name = f"{doc_id}-{attachment.get('attachNum')}{ext}"
                else:
                    file_name = f"{doc_id}{ext}"
                if not link:
                    continue
                if self.document_repo.find_by_file_link(link, self.doc_type):
                    continue
                self._download_and_save_to_db(
                    f"{base_link}storage/file/{link}", link, file_name, self.doc_type
                )
