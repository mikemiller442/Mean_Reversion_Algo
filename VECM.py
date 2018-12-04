import alpaca_trade_api as tradeapi
import sqlite3 as db
import pandas as pd
from statsmodels.tsa.vector_ar import vecm
from statsmodels.tsa.arima_model import ARMA
import numpy as np
import itertools
import statsmodels.tsa.stattools as ts
import statsmodels.api as sm
import matplotlib.pyplot as plt
from Johansen import testCointegration
from ARIMA import predict


class FUNDS:
    def __init__(self, pairs, testJohansen):
        self.pairs = pairs
        self.etfs = []
        self.cointegratedPairs = []
        self.vecm_Models = []
        self.testJohansen = testJohansen

    def fitModels(self):
        for i in range(len(self.pairs)):
            self.prepareTimeSeries(self.pairs[i])
        if self.testJohansen is True:
            testCointegration(self.cointegratedPairs)

    def prepareTimeSeries(self, pair):
        symbList = [pair[0], pair[1]]
        tableNameX = symbList[0] + '_historicalDATA'
        tableNameY = symbList[1] + '_historicalDATA'
        commandX = "SELECT * FROM " + tableNameX
        commandY = "SELECT * FROM " + tableNameY
        c.execute(commandX)
        days = c.fetchall()
        closeX = []

        for j in range(len(days)):
            closeX.append(days[j][5])

        X_DATA = pd.DataFrame({'CLOSE': closeX[::-1]})
        c.execute(commandY)
        days = c.fetchall()
        closeY = []

        for j in range(len(days)):
            closeY.append((days[j][5]))

        Y_DATA = pd.DataFrame({'CLOSE': closeY[::-1]})
        X = X_DATA['CLOSE'].astype(float)
        Y = Y_DATA['CLOSE'].astype(float)
        etf1 = ETF(symbList[0], X)
        if etf1 not in self.etfs:
            self.etfs.append(etf1)
        etf2 = ETF(symbList[1], Y)
        if etf2 not in self.etfs:
            self.etfs.append(etf2)
        etfPair = PAIR(etf1, etf2)
        etfPair.CADF(self)


class PAIR:
    def __init__(self, etf1, etf2):
        self.etf1 = etf1
        self.etf2 = etf2

    def bestOLS(self, Y, X, Y_Ticker, X_Ticker):
        model1 = sm.OLS(Y, X).fit()
        BETA1 = model1.params[0]
        df1 = pd.DataFrame({'Y': Y, 'X': X})
        df1['spread'] = df1.Y + (df1.X * -BETA1)
        model2 = sm.OLS(X, Y).fit()
        BETA2 = model2.params[0]
        df2 = pd.DataFrame({'Y': Y, 'X': X})
        df2['spread'] = df2.X + (df2.Y * -BETA2)

        comb1 = ts.adfuller(df1.spread)
        comb2 = ts.adfuller(df2.spread)

        if comb1[1] < comb2[1]:
            if comb1[1] < 0.05:
                std = np.std(df1.spread[-500:])
                mean = np.mean(df1.spread[-500:])
                return {'std': std, 'mean': mean, 'BETA': BETA1, 'endog': Y_Ticker}
            else:
                return 0
        else:
            if comb2[1] < 0.05:
                std = np.std(df2.spread[-500:])
                mean = np.mean(df2.spread[-500:])
                return {'std': std, 'mean': mean, 'BETA': BETA2, 'endog': X_Ticker}
            else:
                return 0

    def CADF(self, funds):
        X = self.etf1.data
        Y = self.etf2.data
        if len(X.index) != len(Y.index):
            if len(X.index) < len(Y.index):
                Y = Y[-len(X.index):]
                Y = Y.reset_index(drop=True)

            else:
                X = X[-len(Y.index):]
                X = X.reset_index(drop=True)

        results = self.bestOLS(Y, X, self.etf2.ticker, self.etf1.ticker)
        if results is not 0:
            price_data = pd.DataFrame({self.etf1.ticker: X, self.etf2.ticker: Y})
            selected_orders = vecm.select_order(price_data, maxlags=4)
            print(selected_orders)
            if selected_orders.aic > 0:
                model = vecm.VECM(price_data, k_ar_diff=selected_orders.aic)
                model_fit = model.fit()
                if model_fit.pvalues_alpha[0] < 0.05 and model_fit.pvalues_alpha[1] < 0.05:
                    if self.etf1 not in self.etf2.cointegrated:
                        self.etf2.cointegrated.append(self.etf1)
                    if self.etf2 not in self.etf1.cointegrated:
                        self.etf1.cointegrated.append(self.etf2)
                    funds.cointegratedPairs.append(self)
                    print(model_fit.alpha)
                    print(model_fit.beta)
                    print(model_fit.gamma)
                    print(model_fit.predict())
                    p = model_fit.predict()
                    print(p)
                    print(p[0][0])
                    print(p[0][1])
                    print(model_fit.summary())
                    dict1 = {'etf1': self.etf1.ticker, 'forecast1': p[0][0],
                             'etf2': self.etf2.ticker, 'forecast2': p[0][1],
                             'SpreadSTD': results['std'], 'SpreadMEAN': results['mean'],
                             'BETA': results['BETA'], 'endog': results['endog']}
                    funds.vecm_Models.append(dict1)


class ETF:
    def __init__(self, ticker, data):
        self.ticker = ticker
        self.data = data
        self.prediction = 0
        self.cointegrated = []

    def ARMA(self):
        df = pd.DataFrame({'price': self.data})
        df['log_ret'] = np.log(df.price) - np.log(df.price.shift(1))
        df = df.drop(df.index[0])
        dftest = ts.adfuller(df.log_ret)
        if dftest[1] < 0.05:
            results = ts.arma_order_select_ic(df.log_ret, ic=['aic', 'bic'], trend='nc')
            model = ARMA(df.log_ret, order=results.aic_min_order)
            model_fit = model.fit(disp=0)
            transformed_predictions = model_fit.forecast()
            print(transformed_predictions[0][0])
            prediction = df.price.loc[-1:] * np.exp(transformed_predictions[0][0])
            self.prediction = prediction


if __name__ == "__main__":
    api = tradeapi.REST('<key_id>', '<secret_key_id>',
                        'https://api.alpaca.markets')
    account = api.get_account()
    positions = []
    position_info = api.list_positions()

    for i in range(len(position_info)):
        positions.append(position_info[i].symbol)

    cnx = db.connect('ETF.db')
    cnx.row_factory = db.Row
    c = cnx.cursor()

    # c.execute("SELECT * FROM POSITIONS")
    # positions = c.fetchall()

    etfLIST = []

    # c.execute("SELECT * FROM FUNDS")
    c.execute("SELECT * FROM BONDS")
    table = c.fetchall()

    # c.execute("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
    # names = c.fetchall()
    # print(names[0][0])

    for i in range(len(table)):
        try:
            commandETF = "SELECT 1 FROM " + table[i][0] + "_historicalDATA LIMIT 1;"
            c.execute(commandETF)
            etfLIST.append(table[i][0])
        except db.OperationalError as e:
            print(table[i][0] + " didn't have any historical data for some reason.")
            print(e.args)
        except Exception as e:
            print(e.args)

    pairs = list(itertools.combinations(etfLIST, 2))
    funds = FUNDS(pairs, False)
    funds.fitModels()

    arma_predictions = []

    for j in range(len(funds.etfs)):
        if funds.etfs[i].ticker in positions:
            funds.etfs[i].ARMA()
        arma_predictions.append(funds.etfs[i].prediction)

    # print(arma_predictions)
    # arma_df = pd.DataFrame(arma_predictions)
    # print(arma_df)
    # arma_df.to_sql('Univariate_Bonds_Forecast', con=cnx, if_exists='replace', index=False)

    arma_prediction_df = pd.DataFrame({'etf1': etfLIST, 'univariate1': arma_predictions})
    funds.vecm_Models.merge(arma_prediction_df, on='etf1')
    arma_prediction_df = pd.DataFrame({'etf2': etfLIST, 'univariate2': arma_predictions})
    funds.vecm_Models.merge(arma_prediction_df, on='etf2')

    forecast_df = pd.DataFrame(funds.vecm_Models)
    print(forecast_df)
    forecast_df.to_sql('Bonds_Forecast', con=cnx, if_exists='replace', index=False)




