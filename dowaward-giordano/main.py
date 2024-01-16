from config import tickers
from data_pipelines import etl_yfinance, create_all


def get_yfinance_data():
    return etl_yfinance(tickers)


def main():
    create_all()
    get_yfinance_data()
    return


if __name__ == '__main__':
    main()
