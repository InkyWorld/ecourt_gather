import uuid
from sqlalchemy import BigInteger, Column, DateTime, Integer, String, func
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER

from .database import Base
from config.config import TABLE_NAME
# from config.config import DATA_DOCS_TABLE_NAME, PARTY_DOCS_TABLE_NAME


class Documents(Base):
    __tablename__ = TABLE_NAME
    # __tablename__ = DATA_DOCS_TABLE_NAME
    __table_args__ = {"schema": "dbo"}

    id = Column(
        UNIQUEIDENTIFIER(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True
    )

    original_url = Column(String(2048), nullable=True, index=True)
    local_path = Column(String(2048), nullable=True)
    size = Column(BigInteger, nullable=True)
    file_hash = Column(String(64), nullable=True, index=True, unique=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
