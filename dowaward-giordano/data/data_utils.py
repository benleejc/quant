import pandas as pd
import numpy as np
from arch import arch_model


def resample_returns(df: pd.DataFrame,
                     periods: int,
                     period_type: str = 'D') -> pd.DataFrame:
    """calculate returns based on period

    :param period:
    :param period_type:
    :return:
    """
    s = str(periods) + period_type.lower()

    return df.merge((
        df.groupby(['ticker', 'data_source'])
        .rolling(periods, periods)[
                ['adjusted_daily_returns',
                 'unadjusted_daily_returns']
                ]
        .agg(lambda x: (x + 1).prod())
        .reset_index()
        .rename(columns={
            'adjusted_daily_returns': f'adjusted_{s}_returns',
            'unadjusted_daily_returns': f'unadjusted_{s}_returns'
            })
    ), on=['ticker', 'price_date', 'data_source'])


def calculate_returns(df: pd.DataFrame,
                      periods: int = 120,
                      period_type: str = 'D') -> pd.DataFrame:
    """calculate returns based on period

    :param period:
    :param period_type:
    :return:
    """
    # TODO this is slow
    df['adjusted_daily_returns'] = (df[df['current']==1]
                                    .sort_values(
                                          by='price_date',
                                          ascending=True
                                          )
                                    .groupby([
                                          'ticker',
                                          'data_source'
                                          ])['adjusted_close']
                                    .pct_change())
    df['unadjusted_daily_returns'] = (df[df['current']==1]
                                      .sort_values(by='price_date',
                                                   ascending=True)
                                      .groupby([
                                          'ticker',
                                          'data_source'
                                          ])['close']
                                      .pct_change()
                                      )

    cols = ['price_date', 'ticker', 'open', 'high', 'low', 'close',
            'adjusted_close', 'unadjusted_daily_returns',
            'adjusted_daily_returns', 'data_source']
    df = df[df['current'] == 1][cols].set_index('price_date')

    if periods == 1 and period_type == 'D':
        return df

    df = resample_returns(df, periods, period_type)
    df.rename(columns={
        'adjusted_120d_returns': 'MMODEL'
        }, inplace=True)

    return df


def get_current(df):
    df['current'] = (df.groupby([
        'ticker',
        'price_date',
        'data_source'
        ])['update_date']
          .rank()
          .apply(lambda x: x if x == 1 else 0))
    return df


def calculate_garch(df, p, q, freq, return_column):
    # note that this isn't actually plotting forecasts
    tickers = df.ticker.unique()
    vol_dfs = []
    for ticker in tickers:
        returns_data = df[df.ticker == ticker][['price_date', return_column]]\
                .set_index('price_date')
        returns_data = (returns_data.dropna())*1000

        model = arch_model(returns_data, vol='Garch', p=p, q=q)
        res = model.fit(update_freq=freq, disp='off')
        vol_df = pd.DataFrame(res.conditional_volatility)
        vol_df['ticker'] = ticker
        vol_dfs.append(vol_df)
    vol_df = pd.concat(vol_dfs).reset_index().rename(columns={
        'cond_vol': 'VMODEL'
        })
    return df.merge(vol_df, on=['price_date', 'ticker'], how='left')


def calculate_correlation(df):
    start_date = df.groupby(['ticker'])['price_date'].min().max()
    working_df = df[df['price_date'] >= start_date].copy()
    working_df['average_adjusted_daily_returns'] = working_df[
            ['price_date', 'adjusted_daily_returns']]\
        .groupby('price_date')['adjusted_daily_returns']\
        .transform('mean')

    correlations = working_df.set_index('price_date')\
        .groupby('ticker')[
                ['average_adjusted_daily_returns', 'adjusted_daily_returns']
                ]\
        .rolling(80, 80)\
        .corr()\
        .reset_index()
    cols = ['ticker', 'price_date', 'adjusted_daily_returns']
    correlations = correlations[
            correlations['level_2'] == 'average_adjusted_daily_returns'
            ][cols]
    correlations = correlations.rename(columns={
        'adjusted_daily_returns': 'CMODEL'
        })
    working_df = working_df.merge(correlations, on=['ticker', 'price_date'])
    return working_df


def calculate_ATR(df):
    df.set_index('price_date', inplace=True)
    df['prev_close'] = df.groupby('ticker')['close'].shift(1)
    tr_components = pd.concat([
            (df['high'] - df['low']),
            (df['high'] - df['prev_close']).abs(),
            (df['low'] - df['prev_close']).abs()
            ], axis=1)

    df['tr'] = tr_components.max(axis=1)
    # 42 period atr + highest close of 63 period
    # 42 period atr + lowest low of 105 period
    atr_period = 42
    high_period = 63
    low_period = 105
    df['rolling_tr'] = df.groupby('ticker')['tr']\
        .transform(lambda x: x.rolling(atr_period).sum())
    df['atr'] = df['rolling_tr'] / atr_period

    df['upper_band'] = df.groupby('ticker')['close']\
        .transform(lambda x: x.rolling(high_period).max().shift(1)) + df['atr']
    df['lower_band'] = df.groupby('ticker')['low']\
        .transform(lambda x: x.rolling(low_period).min().shift(1)) + df['atr']

    conds = [
        (df['close'] >= df['upper_band']),
        (df['low'] < df['lower_band'])
    ]

    choices = [2, -2]
    df['TMODEL'] = np.select(conds, choices, default=np.nan)
    df['TMODEL'] = df.groupby('ticker')['TMODEL'].ffill()
    return df

