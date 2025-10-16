from sqlalchemy import Column, Integer, String, BLOB, DateTime, func
from .database import Base

class Document(Base):
    """Модель SQLAlchemy для зберігання документів."""
    __tablename__ = "file_storage"

    id = Column(Integer, primary_key=True, index=True)
    file_name = Column(String, nullable=False, index=True)
    file_content = Column(BLOB, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())