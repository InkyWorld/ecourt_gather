import uuid
from sqlalchemy import BigInteger, Column, DateTime, Integer, String, func
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER

from .database import Base
from config.config import DATA_DOCS_TABLE_NAME, PARTY_DOCS_TABLE_NAME


class Document_data(Base):
    __tablename__ = DATA_DOCS_TABLE_NAME
    __table_args__ = {"schema": "dbo"}

    id = Column(
        UNIQUEIDENTIFIER(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True
    )
    doc_id = Column(String(255), index=True, nullable=False)
    original_url = Column(String(2048), nullable=True, index=True)
    local_path = Column(String(2048), nullable=True)
    size = Column(BigInteger, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Document_party_docs(Base):
    __tablename__ = PARTY_DOCS_TABLE_NAME
    __table_args__ = {"schema": "dbo"}

    id = Column(
        UNIQUEIDENTIFIER(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True
    )
    doc_id = Column(String(255), index=True, nullable=False)
    original_url = Column(String(2048), nullable=True, index=True)
    local_path = Column(String(2048), nullable=True)
    size = Column(BigInteger, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
