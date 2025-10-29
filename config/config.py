import json
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_LINK = os.getenv("BASE_LINK")
API_VERSION = os.getenv("API_VERSION")
BASE_LINK_AND_API_VERSION = BASE_LINK + API_VERSION

DB_SERVER = os.environ.get("DB_SERVER")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_DRIVER = os.environ.get("DB_DRIVER")

GOV_REG_DB_SERVER = os.environ.get("GOV_REG_DB_SERVER")
GOV_REG_DB_NAME = os.environ.get("GOV_REG_DB_NAME")
GOV_REG_DB_USER = os.environ.get("GOV_REG_DB_USER")
GOV_REG_DB_PASSWORD = os.environ.get("GOV_REG_DB_PASSWORD")

TABLE_NAME = os.environ.get("TABLE_NAME")

TOKENS_FOLDERS_COMPANIES_STR = os.environ.get("TOKENS_FOLDERS_COMPANIES")
TOKENS_FOLDERS_COMPANIES = json.loads(TOKENS_FOLDERS_COMPANIES_STR)
