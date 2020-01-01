import alpaca_trade_api as tradeapi
import sqlite3 as db
import pandas as pd
import numpy as np
import math
import itertools
import statsmodels.tsa.stattools as ts
import statsmodels.api as sm
from pandas import ExcelWriter

class FUNDS:
    def __init__(self, pairs):
        self.pairs = pairs
        self.etfs = []

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

        X_DATA = pd.DataFrame({'CLOSE': closeX})
        c.execute(commandY)
        days = c.fetchall()
        closeY = []

        for j in range(len(days)):
            closeY.append((days[j][5]))

        closeX = closeX[1:]
        closeY = closeY[1:]

        Y_DATA = pd.DataFrame({'CLOSE': closeY})
        # print(X_DATA)
        # print(Y_DATA)
        # print(symbList[0])
        # print(symbList[1])
        # print(closeX[0])
        # print(closeY[0])
        X = X_DATA['CLOSE'].astype(float)
        Y = Y_DATA['CLOSE'].astype(float)
        etf1 = ETF(symbList[0], X)
        if etf1 not in self.etfs:
            self.etfs.append(etf1)
        etf2 = ETF(symbList[1], Y)
        if etf2 not in self.etfs:
            self.etfs.append(etf2)
        etfPair = PAIR(etf1, etf2)
        etfPair.CADF()


class PAIR:
    def __init__(self, etf1, etf2):
        self.etf1 = etf1
        self.etf2 = etf2
        self.endog = None
        self.exog = None
        self.delta = 1e-4
        self.wt = self.delta / (1 - self.delta) * np.eye(2)
        self.vt = 1e-3
        self.theta = np.zeros(2)
        self.R = None
        self.invested = None
        self.daily_ret = np.zeros(len(self.etf1.data))
        self.sharpe = 0
        self.CAGR = 0
        self.price_data = None

    def bestOLS(self, Y, X, Y_Ticker, X_Ticker):
        model1 = sm.OLS(Y, X).fit()
        BETA1 = model1.params[0]
        print(BETA1)
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

    def CADF(self):
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
            if results['endog'] == self.etf1.ticker:
                self.BuildKalman(self.etf2, self.etf1,results['BETA'])
            else:
                self.BuildKalman(self.etf1, self.etf2,results['BETA'])

    def BuildKalman(self, exog, endog, beta):
        self.endog = endog
        print(endog.ticker)
        self.exog = exog
        print(exog.ticker)
        print("test3")
        print(beta)
        self.theta = np.asarray([beta, 0.0])
        print(self.theta)

        print("test1")
        q_List = []

        for d in range(500):
            F = np.asarray([exog.data[d], 1.0])
            print(F)
            y = endog.data[d]
            print(y)

            if self.R is not None:
                self.R = self.C + self.wt
            else:
                self.R = np.zeros((2, 2))

            yhat = F.dot(self.theta)
            print(yhat)
            et = y - yhat
            print(et)

            Qt = F.dot(self.R).dot(F.T) + self.vt
            print("Qt")
            print(Qt)
            q_List.append(Qt)

            At = self.R.dot(F.T) / Qt
            print(At)
            self.theta = self.theta + At.flatten()*et
            print(self.theta)
            self.C = self.R - At * F.dot(self.R)
            print(self.C)

        for d in range(500, len(self.etf1.data)-1):
            F = np.asarray([exog.data[d], 1.0])
            print("F")
            print(F)
            y = endog.data[d]
            print("y")
            print(y)

            if self.R is not None:
                self.R = self.C + self.wt
            else:
                self.R = np.zeros((2, 2))

            print("theta")
            print(self.theta)
            yhat = F.dot(self.theta)
            print("yhat")
            print(y)
            et = y - yhat
            print("et ")
            print(et)

            Qt = F.dot(self.R).dot(F.T) + self.vt
            sqrt_Qt = np.sqrt(Qt)
            print("sqrt_Qt ")
            print(sqrt_Qt)

            q_List.append(Qt)

            At = self.R.dot(F.T) / Qt
            self.theta = self.theta + At.flatten() * et
            self.C = self.R - At * F.dot(self.R)

            if self.invested is None:
                self.daily_ret[d-500] = 0
                print("test")
                print(self.theta)
                print(self.theta[0])
                self.cur_hedge_qty = int(math.floor(100 * self.theta[0]))
                print(self.cur_hedge_qty)
                print("et")
                print(et)
                print(sqrt_Qt)
                if et < -sqrt_Qt:
                    self.invested = "long"
                elif et > sqrt_Qt:
                    self.invested = "short"
            if self.invested is not None:
                if self.invested == "long":
                    long_ret = (endog.data[d-501]-endog.data[d-500])/endog.data[d-500]
                    print("long_ret")
                    print(d)
                    print(long_ret)
                    short_ret = (exog.data[d-501]-exog.data[d-500])/exog.data[d-500]
                    print("short_ret")
                    print(d)
                    print(short_ret)
                    self.daily_ret[d-500] = (long_ret-short_ret)/2
                    print(self.daily_ret[d-500])
                    if et > -sqrt_Qt:
                        self.invested = None
                else:
                    long_ret = (exog.data[d - 501] - exog.data[d - 500]) / exog.data[d - 500]
                    print("long_ret")
                    print(d)
                    print(long_ret)
                    short_ret = (endog.data[d - 501] - endog.data[d - 500]) / endog.data[d - 500]
                    print("short_ret")
                    print(d)
                    print(short_ret)
                    self.daily_ret[d - 500] = (long_ret - short_ret) / 2
                    self.invested = None
                    if et < sqrt_Qt:
                        self.invested = None

        q_frame = pd.DataFrame({"Qt": q_List})
        Q_writer = pd.ExcelWriter('QtData.xlsx', engine='xlsxwriter')
        q_frame.to_excel(Q_writer, 'Sheet1')
        Q_writer.save()

        self.price_data = pd.DataFrame({'endog': self.endog, 'exog': self.exog, 'endog_data': self.endog.data,
                                        'exog_data': self.exog.data, 'daily_ret': self.daily_ret})
        self.price_data['excess_daily_ret'] = self.price_data['daily_ret'] - 0.05/252
        print(self.price_data['excess_daily_ret'].std())
        # for k in range(len(self.price_data['excess_daily_ret'])):
        #     print(self.price_data['daily_ret'][k])
        #     print(self.price_data['excess_daily_ret'][k])

        print(endog.ticker)
        print(exog.ticker)
        self.sharpe = np.sqrt(252)*self.price_data['excess_daily_ret'].mean()/self.price_data['excess_daily_ret'].std()
        cum_returns = self.price_data['daily_ret'].sum()
        self.CAGR = (cum_returns ** (252/len(self.etf1.data))) - 1

        ##shitty code
        self.price_data['CAGR'] = self.CAGR
        self.price_data['sharpe'] = self.sharpe

        writer = pd.ExcelWriter('TestETF_Pair.xlsx', engine='xlsxwriter')
        self.price_data.to_excel(writer, 'Sheet1')
        writer.save()

        exit(0)


class ETF:
    def __init__(self, ticker, data):
        self.ticker = ticker
        self.data = data


if __name__ == "__main__":
    # api = tradeapi.REST('AKUIBA33DB4AC9N8OO5N', 'RxTrUVJ35hCscaSYI3jPAWx2UqoUPYogxZlSr3M9',
    #                     'https://api.alpaca.markets')
    # account = api.get_account()
    # positions = []
    # position_info = api.list_positions()
    #
    # for i in range(len(position_info)):
    #     positions.append(position_info[i].symbol)

    cnx = db.connect('ETF.db')
    cnx.row_factory = db.Row
    c = cnx.cursor()

    # c.execute("SELECT * FROM POSITIONS")
    # positions = c.fetchall()

    etfLIST = []

    # c.execute("SELECT * FROM FUNDS")
    c.execute("SELECT * FROM BONDS")
    table = c.fetchall()

    for i in range(len(table)):
        try:
            commandETF = "SELECT 1 FROM " + table[i][1] + "_historicalDATA LIMIT 1;"
            c.execute(commandETF)
            etfLIST.append(table[i][1])
        except db.OperationalError as e:
            print(table[i][1] + " didn't have any historical data for some reason.")
            print(e.args)
        except Exception as e:
            print(e.args)

    print("test2")

    pairs = list(itertools.combinations(etfLIST, 2))
    funds = FUNDS(pairs)
    for p in range(len(funds.pairs)):
        funds.prepareTimeSeries(funds.pairs[p])


