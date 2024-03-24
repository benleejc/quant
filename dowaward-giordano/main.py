from config import tickers
from data.data_pipelines import etl_yfinance, create_all, etl_dowaward_giordano

def get_yfinance_data():
    return etl_yfinance(tickers)


def run_yf_scrape():
    create_all()
    get_yfinance_data()


def main():
    #run_yf_scrape()
    data = etl_dowaward_giordano()


    return data


if __name__ == '__main__':
    print(main())
    
