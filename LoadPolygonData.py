import alpaca_trade_api as tradeapi
import sqlite3 as db
import pandas as pd
from statsmodels.tsa.arima_model import ARIMA
import numpy as np
import statsmodels.tsa.stattools as ts

cnx = db.connect('ETF.db')
c = cnx.cursor()

api = tradeapi.REST('<key_id>', '<secret_key_id>', 'https://paper-api.alpaca.markets')
account = api.get_account()
api.list_positions()

c.execute("SELECT * FROM BONDS")
bond_tickers = pd.DataFrame(c.fetchall())
ticker_close_list = []

# c.execute("SELECT * FROM FUNDS")
# oil_tickers = pd.DataFrame(c.fetchall())
# print(oil_tickers)

for i in range(len(bond_tickers)):
    ticker_close_list.append('close' + bond_tickers.loc[i][0])
    if i == 0:
        data = api.polygon.historic_agg('day', bond_tickers.loc[i][0], limit=100).df
        bonds_price_data = pd.DataFrame({bond_tickers.loc[i][0]: data.close})
        bonds_price_data = bonds_price_data.reset_index(drop=True)
    else:
        data = api.polygon.historic_agg('day', bond_tickers.loc[i][0], limit=100).df
        data_df = pd.DataFrame({bond_tickers.loc[i][0]: data.close})
        data_df = data_df.reset_index(drop=True)
        bonds_price_data = pd.merge(bonds_price_data, data_df, how='inner', left_index=True, right_index=True)

print(bonds_price_data)
bonds_price_data.to_sql('Polygon_Bonds', con=cnx, if_exists='replace')





