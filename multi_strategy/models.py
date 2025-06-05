from sqlalchemy import Column, Date, DateTime, Float, PrimaryKeyConstraint, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class StockDaily(Base):
    __tablename__ = "stock_daily"

    ts_code = Column(String(10), nullable=False)
    trade_date = Column(Date, nullable=False)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    pre_close = Column(Float)
    vol = Column(Float)
    amount = Column(Float)
    update_time = Column(DateTime)  # ✅ 添加这一行

    __table_args__ = (PrimaryKeyConstraint("ts_code", "trade_date"),)  # 设为联合主键，确保唯一性
