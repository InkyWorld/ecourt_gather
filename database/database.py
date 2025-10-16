import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
from contextlib import contextmanager 

from config.config import BASE_DIR
from config.logger import get_logger

logger = get_logger(__name__)
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{BASE_DIR / 'ecourt_gather.db'}")

engine = create_engine(DATABASE_URL)
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