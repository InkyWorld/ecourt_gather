import json
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_LINK = os.getenv("BASE_LINK")
API_VERSION = os.getenv("API_VERSION")
BASE_LINK_AND_API_VERSION = BASE_LINK + API_VERSION

TOKENS_AND_FOLDERS = json.loads(os.environ.get("TOKEN_AND_FOLDERS", "[]"))
