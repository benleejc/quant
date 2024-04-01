from config import tickers
from data.data_pipelines import etl_yfinance, create_all, etl_dowaward_giordano

def get_yfinance_data():
    return etl_yfinance(tickers)


def run_yf_scrape():
    create_all()
    get_yfinance_data()


def main():
    run_yf_scrape()
    data = etl_dowaward_giordano()
    print(data.columns)
    model_cols = ['MMODEL', 'CMODEL', 'VMODEL', 'TMODEL']
    group_cols = ['ticker', 'data_source']
    data[model_cols] = data.groupby(group_cols)[model_cols].shift(1)
    data.dropna(subset=model_cols, inplace=True)
    source = 'yfinance'
    model_df = data.loc[data['data_source'] == source][group_cols + model_cols]
    # resample data to monthly
    ranking_model = {
            'MMODEL': 1,
            'CMODEL': -1,
            'VMODEL': -1
            }
    df_monthly = model_df.loc[model_df.index == model_df.index.to_period('M').to_timestamp('M')]
    for k, v in ranking_model.items():
        model_df[k] *= v
    rank_cols = {k: k + '_RANK' for k, _ in ranking_model.items()}
    rank_df = df_monthly.groupby('price_date')[ranking_model.keys()]\
            .rank()\
            .rename(columns=rank_cols)
    # run backtrader/lean test

    return rank_df


if __name__ == '__main__':
    print(main())

