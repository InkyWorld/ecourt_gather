import asyncio
import json
import os
import sys
from pathlib import Path
from time import time
from typing import Dict, List, Optional, Set

import httpx
import requests
from sqlalchemy import Tuple
from tqdm.asyncio import tqdm

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
            if new_docs and new_docs.get("data"):
                documents.extend(new_docs.get("data"))
            if not new_docs:
                break
            if pageCount == 0:
                pageCount = new_docs["pageCount"]
                page = new_docs.get("page")
            page += 1

        return documents

    def _download_and_save_to_db(
        self, base_url: str, original_url: str, file_name: str
    ):
        if not self.token:
            logger.critical("Не знайдено API_BEARER_TOKEN.")
            return

        headers = {"Authorization": f"Bearer {self.token}",
                   "Accept": f"application/{original_url.split('.')[-1]}"}
        try:
            response = requests.get(base_url, headers=headers, stream=True, timeout=360)
            response.raise_for_status()
            file_content = response.content
            size_in_bytes = len(file_content)
            self.document_repo.save_document(
                original_url, file_content, file_name, size_in_bytes
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

    async def _download_and_save_to_db_async(self, base_url: str, original_url: str, file_name: str):
        if not self.token:
            logger.critical("Не знайдено API_BEARER_TOKEN.")
            return "failed"

        headers = {"Authorization": f"Bearer {self.token}"}

        for attempt in range(1, 10):
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(600.0)) as client:
                    async with client.stream("GET", base_url, headers=headers, follow_redirects=True) as response:
                        response.raise_for_status()

                        chunks = []
                        async for chunk in response.aiter_bytes():
                            chunks.append(chunk)
                        file_content = b"".join(chunks)

                size_in_bytes = len(file_content)
                db_success = await self.document_repo.save_document_async(
                    original_url, file_content, file_name, size_in_bytes
                )
                return "success" if db_success else "failed"

            except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ReadError):
                await asyncio.sleep(10)
                if attempt == 9:
                    logger.error(f"Не вдалося завантажити після 9 спроб: {base_url}")
                    return "failed"

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    logger.warning(f"Файл не знайдено (404): {base_url}")
                    return "404"
                elif e.response.status_code == 403:
                    logger.warning(f"Доступ заборонено (403): {base_url}")
                    return "failed"
                else:
                    logger.error(f"HTTP помилка завантаження файлу {base_url}: {e}", exc_info=True)
                    return "failed"

            except Exception as e:
                logger.error(f"Загальна помилка завантаження файлу {base_url}: {e}", exc_info=True)
                return "failed"

    async def _run_all_downloads_async(
        self, files_to_download: Set[Tuple], concurrency_limit: int = 10
    ) -> Dict[str, int]:
        """
        Асинхронно запускає всі завдання на завантаження з обмеженням паралелізму.
        """
        semaphore = asyncio.Semaphore(concurrency_limit)
        stats = {"success": 0, "not_found": 0, "failed": 0}

        async def semaphore_task_wrapper(base_url, original_url, file_name):
            async with semaphore:
                return await self._download_and_save_to_db_async(
                    base_url, original_url, file_name
                )

        tasks = []
        for base_url, original_url, file_name in files_to_download:
            tasks.append(semaphore_task_wrapper(base_url, original_url, file_name))

        logger.info(
            f"Запускаємо {len(tasks)} паралельних завдань "
            f"з обмеженням {concurrency_limit} одночасних завантажень..."
        )

        for future in tqdm(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc="Завантаження файлів",
        ):
            try:
                result = await future
                if result == "success":
                    stats["success"] += 1
                elif result == "404":
                    stats["not_found"] += 1
                else:
                    stats["failed"] += 1
            except Exception as e:
                logger.error(f"Помилка у виконанні завдання: {e}", exc_info=True)
                stats["failed"] += 1
        
        return stats
    
    def gather_documents(self, base_link: str, start_date: str, end_date: str):
        documents = self.document_repo._fetch_data_from_db_by_date_range(
            start_date, end_date, self.doc_type
        )

        if not documents:
            logger.warning(
                f"Не знайдено документів типу '{self.doc_type}' за вказаний період."
            )
            return
        
        logger.info("Завантажуємо існуючі посилання з бази даних...")
        existing_links_set = self.document_repo.get_existing_links_set()
        logger.info(f"Завантажено {len(existing_links_set)} існуючих унікальних посилань.")

        files_in_db_count = 0
        all_attachments_count = 0
        files_to_download = set()
        
        logger.info(f"Знайдено {len(documents)} документів. Починаємо обробку...")

        for doc in tqdm(documents, desc="Обробка документів"):
            if self.doc_type == "data":
                doc_id = doc.get("DocumentId")
            else:
                doc_id = doc.get("id")
            if not doc_id:
                continue

            if self.doc_type == "data":
                attachments = [json.loads(doc.get("originalText"))]
            else:
                attachments = json.loads(doc.get("attachments"))

            if attachments:
                all_attachments_count += len(attachments)
                for attachment in attachments:
                    link = attachment.get("link")
                    if not link:
                        attachNum_log = attachment.get("attachNum", "N/A")
                        logger.warning(f"no link in {doc_id=} {attachNum_log=} found, skipping...")
                        continue

                    if link in existing_links_set:
                        files_in_db_count += 1
                        continue
                    
                    existing_links_set.add(link)
                    ext = Path(link).suffix
                    attachNum = ""
                    
                    if self.doc_type != "data":
                        attachNum = attachment.get("attachNum")
                        file_name = f"{doc_id}-{attachNum}"
                    else:
                        file_name = f"{doc_id}"
                    file_name += ext
                    files_to_download.add(
                        (f"{base_link}storage/file/{link}", link, file_name)
                    )
            else:
                logger.warning(f"No attachments found for document ID {doc_id}.")

        logger.info(f"Всього attachments {all_attachments_count}")
        logger.info(f"Всього знайдено в бд {files_in_db_count} по посиланнях")
        logger.info(f"Всього посилань на файли {len(files_to_download)} для завантаження")

        if not files_to_download:
            logger.info("Немає нових файлів для завантаження.")
            return

        stats = asyncio.run(self._run_all_downloads_async(files_to_download, concurrency_limit=10))

        # --- Логування результатів ---
        files_not_saved = stats["failed"] + stats["not_found"]
        files_not_saved_404 = stats["not_found"]

        logger.info("--- Результати завантаження ---")
        logger.info(f"Успішно завантажено: {stats['success']}")
        logger.info(f"Не збережених файлів {files_not_saved} усього")
        logger.info(f"  - з них не знайдено (404): {files_not_saved_404}")
        logger.info(f"  - з них інші помилки: {stats['failed']}")
        logger.info("---------------------------------")
