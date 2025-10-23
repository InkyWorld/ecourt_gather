import os

from dotenv import load_dotenv
from sqlalchemy import BigInteger, Column, DateTime, Integer, String, func

from .database import Base

load_dotenv()

DATA_DOCS_TABLE_NAME = os.environ.get("DATA_DOCS_TABLE_NAME")
PARTY_DOCS_TABLE_NAME = os.environ.get("PARTY_DOCS_TABLE_NAME")


class Document_data(Base):
    __tablename__ = DATA_DOCS_TABLE_NAME
    __table_args__ = {"schema": "dbo"}

    id = Column(Integer, primary_key=True, index=True)
    original_url = Column(String(2048), nullable=False, index=True)
    local_path = Column(String(2048), nullable=False)
    size = Column(BigInteger, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Document_party_docs(Base):
    __tablename__ = PARTY_DOCS_TABLE_NAME
    __table_args__ = {"schema": "dbo"}

    id = Column(Integer, primary_key=True, index=True)
    original_url = Column(String(2048), nullable=False, index=True)
    local_path = Column(String(2048), nullable=False)
    size = Column(BigInteger, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
