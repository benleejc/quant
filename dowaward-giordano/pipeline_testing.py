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


def rank_signals(df, columns_to_rank):
    pass

def main():
    df = etl_dowaward_giordano()
    df = calculate_ATR(df)
    return 

if __name__ == '__main__':
    display_options()
    main()
