import json
import os
import sys
from typing import Dict, Optional

import requests
from dotenv import load_dotenv

from config.config import BASE_DIR
from config.logger import get_logger

logger = get_logger(__name__)


def fetch_and_save_data(url, query_params = None):
    """
    Отримує дані з API, логує процес та зберігає результат.

    Args:
        query_params: Словник з query-параметрами для API-запиту.
    """
    load_dotenv()
    token = os.getenv("API_BEARER_TOKEN")

    if not token:
        logger.critical(
            "Не знайдено API_BEARER_TOKEN у файлі .env. Роботу зупинено."
        )
        sys.exit(1)

    output_file = BASE_DIR / "output" / "data.json"
    output_file.parent.mkdir(exist_ok=True)

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

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(api_data, f, ensure_ascii=False, indent=4)
        logger.info(f"Результат збережено у файл: {output_file}")

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
        other_params: Інші параметри запиту (напр., limit, sort).
    """
    logger.info(f"Запускаємо отримання даних за період з {start_date} по {end_date}.")
    params = {}

    filters = [
        f"createdAt||$gte||{start_date}",
        f"createdAt||$lte||{end_date}"
    ]

    params['filter'] = filters
    fetch_and_save_data(url, query_params=params)


# --- Нова функція для фільтрації за конкретною датою ---
def fetch_data_by_specific_date(url, date: str):
    """
    Формує запит для отримання даних за одну конкретну дату по полю 'createdAt'.
    
    Примітка: цей метод знайде записи, де `createdAt` точно дорівнює 'YYYY-MM-DD 00:00:00'.
    Якщо потрібно знайти всі записи за день, краще використовувати fetch_data_by_date_range
    з однаковими start_date та end_date.

    Args:
        date (str): Конкретна дата у форматі 'YYYY-MM-DD'.
        other_params: Інші параметри запиту (напр., limit, sort).
    """
    logger.info(f"Запускаємо отримання даних за конкретну дату: {date}.")
    params = {}
    date_filter = f"createdAt||$eq||{date}"
    params['filter'] = [date_filter]

    fetch_and_save_data(url, query_params=params)

if __name__ == "__main__":
    fetch_and_save_data(
        "https://stage-api-corp.court.gov.ua/api/v1/data/case")
    # fetch_data_by_date_range(
    #     "https://stage-api-corp.court.gov.ua/api/v1/data/case",
    #     start_date="2023-08-25",
    #     end_date="2023-08-27",
    # )
