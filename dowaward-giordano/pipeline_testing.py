from data.data_pipelines import etl_dowaward_giordano
import pandas as pd
import numpy as np

def display_options():
    display = pd.options.display
    display.max_columns = 10
    display.max_rows = None
    display.max_colwidth = 222
    display.width = None
    return None


def calculate_ATR(df):
    df.set_index('price_date', inplace=True)
    df['prev_close'] = df.groupby('ticker')['close'].shift(1)
    tr_components =  pd.concat([
            (df['high'] - df['low']),
            (df['high'] - df['prev_close']).abs(),
            (df['low'] - df['prev_close']).abs()
            ], axis=1)

    df['tr'] = tr_components.max(axis=1)
    # 42 period atr + highest close of 63 period
    # 42 period atr - lowest low of 105 period
    atr_period = 42
    high_period = 63
    low_period = 105
    df['atr'] = df.groupby('ticker')['tr'].transform(lambda x: x.rolling(atr_period).sum()) / 42
    df['upper_band'] = df.groupby('ticker')['prev_close'].transform(lambda x: x.rolling(high_period).max())
    df['lower_band'] = df.groupby('ticker')['low'].transform(lambda x: x.rolling(low_period).min())

    #conds = [
    #    (df['close'] >= df['atr_upper']),
    #    (df['low'] < df['atr_lower'])
    #]

    #choices = [2,-2]
    #df['atr'] = np.select(conds, choices)
    print(df[df.ticker == 'SHY'][['close', 'high', 'upper_band', 'atr', 'tr']].iloc[0:100,:].to_csv('test.csv'))
    #print(df[(df['close'] >= df['atr_upper'])])
    #print(df[(df['low'] < df['atr_lower'])])
            
    return df 

def rank_signals(df, columns_to_rank):
    pass

def main():
    df = etl_dowaward_giordano()
    df = calculate_ATR(df)
    return 

if __name__ == '__main__':
    display_options()
    main()
