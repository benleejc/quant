from data.data_models import Base, StockPrice
from data.data_utils import calculate_returns, calculate_garch, get_current
from data.data_utils import calculate_correlation, calculate_ATR

from sqlalchemy import create_engine, select, insert
from sqlalchemy.orm import Session

import yfinance as yf
import pandas as pd
import datetime as dt


def get_engine():
    return create_engine('sqlite:///raw.db', echo=False)


def create_all():
    return Base.metadata.create_all(get_engine())


def ticker_exists(ticker):
    with Session(get_engine()) as session:
        row = session.execute(
                select(StockPrice).where(StockPrice.ticker == ticker)).first()
    return True if row else False


def etl_base():
    return


def etl_yfinance(tickers):

    new_tickers = []
    existing_tickers = []

    for ticker in tickers:
        if ticker_exists(ticker):
            existing_tickers.append(ticker)
        else:
            new_tickers.append(ticker)

    dfs = []
    if new_tickers:
        new_df = yf.download(' '.join(new_tickers), period='max')
        if len(new_df) > 1:
            new_df = new_df\
                    .stack(level=1)\
                    .reset_index()\
                    .rename(columns={'level_1': 'ticker'})
        else:
            new_df = new_df.reset_index()
        dfs.append(new_df)

    if existing_tickers:
        existing_df = yf.download(' '.join(existing_tickers), period='3mo')
        if len(existing_tickers) > 1:
            existing_df = existing_df.stack(level=1)\
                    .reset_index()\
                    .rename(columns={'level_1': 'ticker'})
        else:
            existing_df = existing_df.reset_index()

        dfs.append(existing_df)
    if dfs:
        df = pd.concat(dfs)
    else:
        return None

    df['update_date'] = dt.datetime.now()
    df['data_source'] = 'yfinance'
    df = df.rename(columns={'Date': 'price_date',
                            'Adj Close': 'adjusted_close'})
    df.columns = [col.lower() for col in df.columns]

    recs = df.to_dict(orient='records')
    print(f'{len(recs)} rows being inserted into StockPrice')

    with Session(get_engine()) as session:
        session.execute(
                insert(StockPrice).returning(
                    StockPrice.id,
                    sort_by_parameter_order=True
                    ), recs)
        session.commit()

    return


def etl_fmp():
    return


def etl_dowaward_giordano():
    with Session(get_engine()) as session:
         print('querying database.. ')
         stmt = session.query(StockPrice)
         data = pd.read_sql(stmt.statement, con=(get_engine()))

    df = get_current(data)
    print('calculating returns..')
    df = calculate_returns(df)
    print('calculating garch..')
    df = calculate_garch(df, 1, 1, 10, 'adjusted_daily_returns')
    print('calculating correlations..')
    df = calculate_correlation(df)
    print('calculating ATR..')
    df = calculate_ATR(df)

    return df


