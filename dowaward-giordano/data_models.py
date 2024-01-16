from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class StockPrice(Base):
    __tablename__ = 'stock_prices'

    id = Column(Integer)
    ticker = Column(String, nullable=False, primary_key=True)
    price_date = Column(DateTime, nullable=False, primary_key=True)
    open = Column(Float)
    close = Column(Float)
    adjusted_close = Column(Float)
    high = Column(Float)
    low = Column(Float)
    volume = Column(Integer)
    data_source = Column(String, nullable=False, primary_key=True)
    update_date = Column(DateTime, nullable=False, primary_key=True)

    def __repr__(self):
        return f"<StockPrice(symbol={self.ticker}, date={self.price_date}, close_price={self.close_price})>"
