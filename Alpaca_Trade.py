import alpaca_trade_api as tradeapi
import sqlite3 as db
import pandas as pd
from statsmodels.tsa.arima_model import ARIMA
import numpy as np
import statsmodels.tsa.stattools as ts

cnx = db.connect('ETF.db')
c = cnx.cursor()

api = tradeapi.REST('<key_id>', '<secret_key_id>', 'https://api.alpaca.markets')
account = api.get_account()
api.list_positions()

orders = []

pairs = pd.read_sql("SELECT * FROM Bonds_Forecast", cnx)
print(pairs)

print(api.list_positions())

for i in range(len(pairs)):
    quote1 = api.get_quote(pairs.loc[i].etf1).last
    quote2 = api.get_quote(pairs.loc[i].etf2).last
    if pairs.loc[i].endog == pairs.loc[i].etf1:
        spread = quote1 + (quote2 * -pairs.loc[i].BETA)
    elif pairs.loc[i].endog == pairs.loc[i].etf2:
        spread = quote2 + (quote1 * -pairs.loc[i].BETA)
    else:
        print("Warning: problem with pairs data frame.")
        exit()
    zscore = (spread - pairs.loc[i].SpreadMEAN)/pairs.loc[i].SpreadSTD
    print(zscore)
    if abs(zscore) > 1.96:
        if quote1 < pairs.loc[i].forecast1 and quote2 > pairs.loc[i].forecast2:
            orders.append(api.submit_order(pairs.loc[i].etf1, 1, 'buy', 'market', 'day'))
        elif quote1 > pairs.loc[i].forecast1 and quote2 < pairs.loc[i].forecast2:
            orders.append(api.submit_order(pairs.loc[i].etf2, 1, 'buy', 'market', 'day'))
        else:
            continue

