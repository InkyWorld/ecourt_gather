from datetime import datetime, time, timezone
import json
import os
import sys
from typing import Dict, Optional

import requests
from dotenv import load_dotenv

from config.config import BASE_DIR
from config.logger import get_logger

logger = get_logger(__name__)


def fetch_data(url, query_params=None):
    """
    Отримує дані з API, логує процес та зберігає результат.

    Args:
        query_params: Словник з query-параметрами для API-запиту.
    """
    load_dotenv()
    token = os.getenv("API_BEARER_TOKEN")

    if not token:
        logger.critical("Не знайдено API_BEARER_TOKEN у файлі .env. Роботу зупинено.")
        sys.exit(1)

    headers = {"Authorization": f"Bearer {token}"}

    try:
        logger.info(f"Надсилаємо запит до {url}")
        response = requests.get(url, headers=headers, params=query_params)
        response.raise_for_status()

        api_data = response.json()
        data_list = api_data.get("data") if isinstance(api_data, dict) else None
        if data_list is None:
            logger.warning(
                "У відповіді відсутній ключ 'data' або відповідь не є словником."
            )
        else:
            logger.info("Дані успішно отримано.")
            if isinstance(data_list, list):
                logger.info(
                    f"Кількість елементів у вкладеному списку: {len(data_list)}"
                )
            else:
                logger.warning("Не вдалося знайти список елементів у відповіді.")

        return data_list

    except requests.exceptions.HTTPError as err:
        logger.error(
            f"HTTP помилка: {err.response.status_code}. Відповідь: {err.response.text}"
        )
    except requests.exceptions.Timeout:
        logger.error("Запит перевищив час очікування.")
    except Exception:
        logger.error("Сталася непередбачувана помилка.", exc_info=True)


def fetch_data_by_date_range(url, start_date: str, end_date: str):
    """
    Формує запит для отримання даних за діапазон дат по полю 'createdAt'.

    Args:
        start_date (str): Початкова дата у форматі 'YYYY-MM-DD'.
        end_date (str): Кінцева дата у форматі 'YYYY-MM-DD'.
    """
    start_datetime_utc = datetime.combine(
        datetime.strptime(start_date, "%Y-%m-%d"), time.min
    ).replace(tzinfo=timezone.utc)
    end_datetime_utc = datetime.combine(
        datetime.strptime(end_date, "%Y-%m-%d"), time.max
    ).replace(tzinfo=timezone.utc)

    start_datetime_str = start_datetime_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_datetime_str = end_datetime_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info(
        f"Запускаємо отримання даних за період з {start_datetime_utc} по {end_datetime_utc}."
    )
    params = {}

    filters = [
        f"createdAt||$gte||{start_datetime_str}",
        f"createdAt||$lte||{end_datetime_str}",
    ]

    params["filter"] = filters
    return fetch_data(url, query_params=params)


def fetch_data_by_date(url, date: str):
    """
    Формує запит для отримання даних за одну конкретну дату по полю 'createdAt'.
    Args:
        date (str): Конкретна дата у форматі 'YYYY-MM-DD'.
    """
    return fetch_data_by_date_range(url, start_date=date, end_date=date)


def download_file(url: str, save_path: str):
    """
    Завантажує файл з вказаного URL та зберігає його за вказаним шляхом.

    Args:
        url (str): URL файлу для завантаження.
        save_path (str): Шлях для збереження завантаженого файлу.
    """
    load_dotenv()
    token = os.getenv("API_BEARER_TOKEN")

    if not token:
        logger.critical("Не знайдено API_BEARER_TOKEN для завантаження файлу.")
        return None
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()

        with open(save_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        return save_path

    except requests.exceptions.HTTPError as err:
        logger.error(
            f"HTTP помилка при завантаженні файлу {url}: {err.response.status_code}. Відповідь: {err.response.text}"
        )
    except requests.exceptions.Timeout:
        logger.error(f"Запит на завантаження файлу {url} перевищив час очікування.")
    except Exception:
        logger.error(
            f"Сталася непередбачувана помилка при завантаженні файлу {url}.",
            exc_info=True,
        )

    return None

def gather_urls_data_documents(base_link: str, start_date: str, end_date: str):
    """
    Збирає URL-адреси документів за вказаний діапазон дат.

    Args:
        base_link (str): Базовий URL для формування посилань на документи.
        start_date (str): Початкова дата у форматі 'YYYY-MM-DD'.
        end_date (str): Кінцева дата у форматі 'YYYY-MM-DD'.

    Returns:
        Optional[Dict]: Словник з URL-адресами документів або None у разі помилки.
    """
    documents = fetch_data_by_date_range(
        f"{base_link}data/document",
        start_date=start_date,
        end_date=end_date,
    )
    with open(BASE_DIR / "output" / "documents.json", "w", encoding="utf-8") as f:
        json.dump(documents, f, ensure_ascii=False, indent=4)
    
    for doc in documents:
        name = doc.get("original", {}).get("link")
        if not name:
            continue
        ending = name.split(".")[-1]
        download_file(
            f"{base_link}storage/file/{name}",
            str(output_dir / f"{doc['id']}.{ending}"),
        )

def gather_urls_party_documents(base_link: str, start_date: str, end_date: str):
    """
    Збирає URL-адреси документів за вказаний діапазон дат.

    Args:
        base_link (str): Базовий URL для формування посилань на документи.
        start_date (str): Початкова дата у форматі 'YYYY-MM-DD'.
        end_date (str): Кінцева дата у форматі 'YYYY-MM-DD'.

    Returns:
        Optional[Dict]: Словник з URL-адресами документів або None у разі помилки.
    """
    documents = fetch_data_by_date_range(
        f"{base_link}party-docs/document",
        start_date=start_date,
        end_date=end_date,
    )
    with open(BASE_DIR / "output" / "party_documents.json", "w", encoding="utf-8") as f:
        json.dump(documents, f, ensure_ascii=False, indent=4)
    
    for doc in documents:
        attachments = doc.get("attachments", [])
        for attachment in attachments:
            name = attachment.get("link")
            if not name:
                continue
            ending = name.split(".")[-1]
            download_file(
                f"{base_link}storage/file/{name}",
                str(output_dir / f"{doc['id']}.{ending}"),
            )

if __name__ == "__main__":
    base_link = "https://stage-api-corp.court.gov.ua/api/v1/"
    output_dir = BASE_DIR / "output"
    output_dir.mkdir(exist_ok=True)
    documents_urls = gather_urls_data_documents(
        base_link,
        start_date="2023-08-25",
        end_date="2023-08-27",
    )
    party_documents_urls = gather_urls_party_documents(
        base_link,
        start_date="2023-08-25",
        end_date="2025-08-27",
    )
