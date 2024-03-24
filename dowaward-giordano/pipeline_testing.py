# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: hydrogen
#       format_version: '1.3'
#       jupytext_version: 1.16.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
from data.data_pipelines import etl_dowaward_giordano

# %%
%load_ext autoreload
%autoreload 2

# %%
df = etl_dowaward_giordano()


# %%
def calculate_correlation(df):
    # TODO aveage relative correlation calculation
    pass

def calculate_ATR(df):
    pass

def rank_signals(df, columns_to_rank):
    pass
