from sqlalchemy import Column, Date, Float, Integer, String, create_engine
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class StockDaily(Base):
    __tablename__ = "stock_daily"
    ts_code = Column(String(10), primary_key=True)
    trade_date = Column(Date, primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    pre_close = Column(Float)
    vol = Column(Float)
    amount = Column(Float)
