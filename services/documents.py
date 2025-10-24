import json
import os
import sys
from pathlib import Path
from time import time
from typing import Dict, List, Optional

import requests

from config.logger import get_logger
from repo.documents import DocumentRepository

logger = get_logger(__name__)


class DocumentService:
    def __init__(self, document_repo: DocumentRepository, doc_type: str, token, company: str):
        self.document_repo = document_repo
        self.doc_type = doc_type
        self.token = token
        self.company = company

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
        self, base_url: str, original_url: str, file_name: str, doc_type: str, doc_id: str
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
                original_url, file_content, file_name, size_in_bytes, doc_type, doc_id
            )

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"Файл не знайдено за посиланням (404): {base_url}")
            elif e.response.status_code == 403:
                logger.warning(f"Доступ заборонено (403) до файлу: {base_url}")
            else:
                logger.error(f"HTTP помилка завантаження файлу {base_url}: {e}", exc_info=True)
            raise

        except requests.exceptions.RequestException as e:
            logger.error(f"Загальна помилка завантаження файлу {base_url}: {e}", exc_info=True)
            raise

    def gather_documents(self, base_link: str, start_date: str, end_date: str):
        documents = self.document_repo._fetch_data_from_db_by_date_range(
            start_date, end_date, self.doc_type
        )

        if not documents:
            logger.warning(
                f"Не знайдено документів типу '{self.doc_type}' за вказаний період."
            )
            return
        files_not_saved = 0
        files_not_saved_404 = 0
        all_files = 0
        start_time = time()
        files_in_db = 0
        all_attachments_count = 0
        for doc in documents:
            if self.doc_type == "data":
                doc_id = doc.get("DocumentId")
            else:
                doc_id = doc.get("id")
            if not doc_id:
                continue

            if self.doc_type == "data":
                attachments = [json.loads(doc.get("OriginalText" if self.company == "Ace" else "originalText"))]
            else:
                attachments = json.loads(doc.get("attachments"))
            all_attachments_count += len(attachments)
            if attachments:
                for attachment in attachments:
                    link = attachment.get("link")
                    if not link:
                        print("no link")
                        continue
                    else:
                        all_files += 1
                    ext = Path(link).suffix
                    if self.doc_type != "data":
                        file_name = f"{doc_id}-{attachment.get('attachNum')}{ext}"
                    else:
                        file_name = f"{doc_id}{ext}"
                    already_in_db = self.document_repo.find_by_file_link(link, self.doc_type)
                    is_duplicate = False
                    if already_in_db:
                        files_in_db += len(already_in_db)
                        is_duplicate = any(db_doc.doc_id == doc_id for db_doc in already_in_db)
                        if is_duplicate:
                            continue
                    try:
                        self._download_and_save_to_db(
                            f"{base_link}storage/file/{link}", link, file_name, self.doc_type, doc_id
                        )
                    except requests.exceptions.HTTPError as e:
                        if e.response.status_code == 404:
                            files_not_saved += 1
                            files_not_saved_404 += 1
                    except Exception:
                        files_not_saved += 1
        logger.info(f"Зайняло часу {time() - start_time:.2f}")
        logger.info(f"Всього посилань на файли {all_files}")
        logger.info(f"Всього attachments {all_attachments_count}")
        logger.info(f"Всього знайдено в бд {files_in_db}")
        logger.info(f"Не збережених файлів {files_not_saved} усього")
        logger.info(f"Не збережених файлів через {files_not_saved} 404")
