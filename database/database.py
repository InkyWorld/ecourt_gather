import os
from contextlib import contextmanager
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from config.logger import get_logger

logger = get_logger(__name__)
load_dotenv()

server = os.environ.get("DB_SERVER")
database = os.environ.get("DB_NAME")
user = os.environ.get("DB_USER")
password = os.environ.get("DB_PASSWORD")
driver = os.environ.get("DB_DRIVER")
password_encoded = quote_plus(password)
driver_encoded = quote_plus(driver)
db_url = (
    f"mssql+pyodbc://{user}:{password_encoded}@{server}/{database}?"
    f"driver={driver_encoded}&TrustServerCertificate=yes"
)
engine = create_engine(db_url)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def initialize_database():
    """
    Створює всі таблиці в базі даних, визначені в моделях SQLAlchemy.
    Цей скрипт потрібно запускати лише один раз для налаштування БД.
    """
    try:
        logger.info("Спроба створення таблиць бази даних...")
        Base.metadata.create_all(bind=engine)
        logger.info("Таблиці бази даних успішно створено (або вони вже існували).")
    except Exception as e:
        logger.error(f"Не вдалося створити таблиці: {e}", exc_info=True)


@contextmanager
def get_db_session():
    """
    Контекстний менеджер для створення та закриття сесії бази даних.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
        logger.info("Сесію бази даних закрито.")


def get_db():
    """Створює сесію бази даних."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
