import os
import sys
import json
import requests
from dotenv import load_dotenv

from config.logger import get_logger
from config.config import BASE_DIR

logger = get_logger(__name__)


def fetch_and_save_data():
    """Отримує дані з API, логує процес та зберігає результат."""
    load_dotenv()
    token = os.getenv("API_BEARER_TOKEN")
    api_url = os.getenv("API_URL")

    if not token or not api_url:
        logger.critical("Не знайдено API_BEARER_TOKEN або API_URL у файлі .env. Роботу зупинено.")
        sys.exit(1)

    output_file = BASE_DIR / "output" / "data.json"
    output_file.parent.mkdir(exist_ok=True)

    headers = {"Authorization": f"Bearer {token}"}

    try:
        logger.info(f"Надсилаємо запит до {api_url}")
        response = requests.get(api_url, headers=headers, timeout=10)
        response.raise_for_status()

        api_data = response.json()
        data_list = api_data.get('data') if isinstance(api_data, dict) else None
        if data_list is None:
            logger.warning("У відповіді відсутній ключ 'data' або відповідь не є словником.")
        else:
            logger.info("Дані успішно отримано.")
            if isinstance(data_list, list):
                logger.info(f"Кількість елементів у вкладеному списку: {len(data_list)}")
            else:
                logger.warning("Не вдалося знайти список елементів у відповіді.")


        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(api_data, f, ensure_ascii=False, indent=4)
        logger.info(f"Результат збережено у файл: {output_file}")

    except requests.exceptions.HTTPError as err:
        logger.error(f"HTTP помилка: {err.response.status_code}. Відповідь: {err.response.text}")
    except requests.exceptions.Timeout:
        logger.error("Запит перевищив час очікування.")
    except Exception as err:
        logger.error(f"Сталася непередбачувана помилка.", exc_info=True)


if __name__ == "__main__":
    fetch_and_save_data()