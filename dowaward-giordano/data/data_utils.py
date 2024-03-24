import pandas as pd
from arch import arch_model


def resample_returns(df: pd.DataFrame, periods: int, period_type: str = 'D') -> pd.DataFrame:
    """calculate returns based on period

    :param period:
    :param period_type:
    :return:
    """

    return df.merge((
        df.groupby(['ticker', 'data_source'])
            .rolling(periods, periods)[['adjusted_daily_returns', 'unadjusted_daily_returns']]
            .agg(lambda x: (x + 1).prod())
            .reset_index()
            .rename(columns={
                'adjusted_daily_returns': f'adjusted_{str(periods) + period_type.lower()}_returns',
                'unadjusted_daily_returns': f'unadjusted_{str(periods) + period_type.lower()}_returns'
            })
    ), on=['ticker', 'price_date', 'data_source'])


def calculate_returns(df: pd.DataFrame, periods: int = 120 ,period_type: str = 'D') -> pd.DataFrame:
    """calculate returns based on period
    
    :param period: 
    :param period_type: 
    :return: 
    """
    # TODO this is slow
    df['adjusted_daily_returns'] = (df[df['current']==1]
                                      .sort_values(by='price_date', ascending=True)
                                      .groupby(['ticker', 'data_source'])['adjusted_close']
                                      .pct_change())
    df['unadjusted_daily_returns'] = (df[df['current']==1]
                                        .sort_values(by='price_date', ascending=True)
                                        .groupby(['ticker', 'data_source'])['close']
                                        .pct_change())
    
    cols = ['price_date', 'ticker', 'open', 'high', 'low', 'close', 'adjusted_close', 'unadjusted_daily_returns', 'adjusted_daily_returns', 'data_source']
    df = df[df['current'] == 1][cols].set_index('price_date')
    
    if periods == 1 and period_type == 'D':
        return df
    
    df = resample_returns(df, periods, period_type)
    
    return df


def get_current(df):
    df['current'] = (df.groupby(['ticker', 'price_date', 'data_source'])['update_date']
                   .rank()
                   .apply(lambda x: x if x == 1 else 0))
    return df


def calculate_garch(df, p, q, freq, return_column):
    # note that this isn't actually plotting forecasts
    tickers = df.ticker.unique()
    vol_dfs = []
    for ticker in tickers:
        returns_data = df[df.ticker == ticker][['price_date', return_column]].set_index('price_date')
        returns_data = (returns_data.dropna())*1000

        model = arch_model(returns_data, vol='Garch', p=p, q=q)
        res = model.fit(update_freq=freq, disp='off')
        vol_df = pd.DataFrame(res.conditional_volatility)
        vol_df['ticker'] = ticker
        vol_dfs.append(vol_df)
    vol_df = pd.concat(vol_dfs).reset_index()
# vol_df = pd.DataFrame(returns_data.rolling(window=10, min_periods=10).std()) # normal variance calculation
    # vol_df = pd.DataFrame(returns_data.rolling(window=10, min_periods=10).std()) # normal variance calculation
    return df.merge(vol_df, on=['price_date', 'ticker'], how='left')
