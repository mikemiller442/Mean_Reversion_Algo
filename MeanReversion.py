import sqlite3 as db
import itertools
from datetime import datetime
import pandas as pd
from pandas import ExcelWriter
import numpy as np
from numpy import log, polyfit, sqrt, std, subtract
import statsmodels.tsa.stattools as ts
import statsmodels.api as sm
import matplotlib.pyplot as plt
import seaborn as sns
import pprint

# ETF.db is a SQLite database. If it is in the same directory as this code, the
# program should be able to connect to it no problem. The database was populated
# in ScrapeNASDAQ.py, and has a table of historical data for each ETF plus a table
# of general data about all the ETF's. If you're curious why database are structured
# like this, look up relational databases.

cnx = db.connect('ETF.db')
cnx.row_factory = db.Row
c = cnx.cursor()

class PAIR:
    def __init__(self, numDAYS, ETF_X, ETF_Y, CAGR, SHARPE, HALF_LIFE, BETA, HURST, ADF_PVALUE):
        self.num = numDAYS
        self.X = ETF_X
        self.Y = ETF_Y
        self.CAGR = CAGR
        self.SHARPE = SHARPE
        self.hl = HALF_LIFE
        self.BETA = BETA
        self.HURST = HURST
        self.p = ADF_PVALUE


def hurst(ts):
    # Returns the Hurst Exponent of the time series vector ts
    # Create the range of lag values
    lags = range(2, 100)

    # Calculate the array of the variances of the lagged differences
    tau = [sqrt(std(subtract(ts[lag:], ts[:-lag]))) for lag in lags]

    # Use a linear fit to estimate the Hurst Exponent
    poly = polyfit(log(lags), log(tau), 1)

    # Return the Hurst exponent from the polyfit output
    return poly[0]*2.0


ETF = []
pairsLIST = []

# The SELECT * FROM is SQL code, and the c.execute is Python running code in order to run
# SQL code through the sqlite3 library.

c.execute("SELECT * FROM 'FUNDS'")
table = c.fetchall()

for i in range(len(table)):
    try:
        commandETF = "SELECT 1 FROM " + table[i][1] + "_historicalDATA LIMIT 1;"
        c.execute(commandETF)
        ETF.append(table[i][1])
    except db.OperationalError as e:
        print(table[i][1] + " didn't have any historical data for some reason.")
        print(e.args)
    except Exception as e:
        print(e.args)

pairs = list(itertools.combinations(ETF, 2))

for i in range(len(pairs)):
    symbList = [pairs[i][0], pairs[i][1]]

    tableNameX = symbList[0] + '_historicalDATA'
    tableNameY = symbList[1] + '_historicalDATA'

    commandX = "SELECT * FROM " + tableNameX

    commandY = "SELECT * FROM " + tableNameY

    c.execute(commandX)

    # fetches every row in the table

    days = c.fetchall()

    closeX = []

    # The column at index 5 is the closing price.

    for j in range(len(days)):
        closeX.append(days[j][5])

    # Code makes a pandas dataframe, which is easy to plot and do regression analysis on.

    X_DATA = pd.DataFrame({'CLOSE': closeX})

    c.execute(commandY)

    days = c.fetchall()

    closeY = []

    for j in range(len(days)):
        closeY.append((days[j][5]))

    Y_DATA = pd.DataFrame({'CLOSE': closeY})

    # A float is a data type that can have decimals, so not just integers.

    X = X_DATA['CLOSE'].astype(float)
    Y = Y_DATA['CLOSE'].astype(float)

    # Code below makes sure that the dataframes are the same length, and truncates
    # the longer one to have the same length as the shorter one.

    if len(X.index) != len(Y.index):
        if len(X.index) < len(Y.index):
            rowsToDelete = len(Y.index) - len(X.index)
            Y = Y[:-rowsToDelete]
        else:
            rowsToDelete = len(X.index) - len(Y.index)
            X = X[:-rowsToDelete]

    # Plots both of the time series

    plt.plot(Y, label=symbList[1])
    plt.plot(X, label=symbList[0])
    plt.ylabel('Price')
    plt.xlabel('Time')
    plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
    # plt.show()

    # Linear regression on the time series... Check out the model.summary, because
    # I definitely don't understand all of the data in that table. Especially the
    # "coef" term, which I think is the beta coefficient.

    try:
        model = sm.OLS(Y, X).fit()

        # print(model.summary())

        # I believe model.params[0] is the beta coefficient.

        BETA = model.params[0]

        # Takes the first two data frames and merges it into one data frame.

        df1 = pd.DataFrame({'Y': Y, 'X': X})

        # I don't know what hr stands for.

        # Note the negative sign below

        df1['hr'] = -BETA

        # My understanding of the code below: let's say we ran a regression of a time series against
        # itself (which I did on accident lol), then the beta coefficent would be 1, because the change
        # in standard deviation of one variable has exactly the same change in standard devation for the
        # other variable. When you run the equation below, you are basically adding y to X times -1,
        # so you would get zero -> no meaningful spread series. .78 is a pretty good correlation, but
        # there is probably something better. But the equation is basically subtracting a fraction of
        # X (depending on the beta coefficient) from y, which gives the spread series.

        df1['spread'] = Y + (X * df1.hr)

        plt.plot(df1.spread)
        # plt.show()

        cadf = ts.adfuller(df1.spread)
        adfPVALUE = cadf[1]
        print('Augmented Dickey Fuller test statistic =', cadf[0])
        print('Augmented Dickey Fuller p-value =', adfPVALUE)
        print('Augmented Dickey Fuller 1%, 5% and 10% test statistics =', cadf[4])

        hurstEXP = hurst(df1.spread)

        print("Hurst Exponent =", round(hurstEXP, 2))

        # Run OLS regression on spread series and lagged version of itself
        spread_lag = df1.spread.shift(1)
        spread_lag.loc[0] = spread_lag.loc[1]
        spread_ret = df1.spread - spread_lag
        spread_ret.loc[0] = spread_ret.loc[1]
        spread_lag2 = sm.add_constant(spread_lag)

        model = sm.OLS(spread_ret, spread_lag2)
        res = model.fit()

        print(res.params[0])
        print(res.params[1])

        halflife = round(-np.log(2) / res.params[1], 0)

        print('Halflife = ', halflife)

        try:
            meanSpread = df1.spread.rolling(window=int(np.floor(halflife))).mean()
            stdSpread = df1.spread.rolling(window=int(np.floor(halflife))).std()

            df1['zScore'] = (df1.spread - meanSpread) / stdSpread

            # print(df1)

            df1['zScore'].plot()
            # plt.show()

            entryZscore = 2
            exitZscore = 0

            # set up num units long
            df1['long entry'] = ((df1.zScore < - entryZscore) & (df1.zScore.shift(1) > - entryZscore))
            df1['long exit'] = ((df1.zScore > - exitZscore) & (df1.zScore.shift(1) < - exitZscore))
            df1['num units long'] = np.nan
            df1.loc[df1['long entry'], 'num units long'] = 1
            df1.loc[df1['long exit'], 'num units long'] = 0
            df1['num units long'].iloc[0] = 0
            df1['num units long'] = df1['num units long'].fillna(method='pad')

            # set up num units short
            df1['short entry'] = ((df1.zScore > entryZscore) & (df1.zScore.shift(1) < entryZscore))
            df1['short exit'] = ((df1.zScore < exitZscore) & (df1.zScore.shift(1) > exitZscore))
            df1.loc[df1['short entry'], 'num units short'] = -1
            df1.loc[df1['short exit'], 'num units short'] = 0
            df1['num units short'].iloc[0] = 0
            df1['num units short'] = df1['num units short'].fillna(method='pad')

            df1['numUnits'] = df1['num units long'] + df1['num units short']
            df1['spread pct ch'] = (df1['spread'] - df1['spread'].shift(1)) / ((df1['X'] * abs(df1['hr'])) + df1['Y'])
            df1['port rets'] = df1['spread pct ch'] * df1['numUnits'].shift(1)

            df1['cum rets'] = df1['port rets'].cumsum()
            df1['cum rets'] = df1['cum rets'] + 1

            plt.plot(df1['cum rets'])
            plt.xlabel(symbList[1])
            plt.ylabel(symbList[0])
            # plt.show()

            sharpe = ((df1['port rets'].mean() / df1['port rets'].std()) * sqrt(252))

            start_val = 1
            end_val = df1['cum rets'].iat[-1]

            days = len(X.index)

            CAGR = round(((float(end_val) / float(start_val)) ** (252.0 / days)) - 1, 4)

            print("CAGR = {}%".format(CAGR * 100))
            print("Sharpe Ratio = {}".format(round(sharpe, 2)))

            pairsLIST.append(PAIR(len(Y.index), pairs[i][0], pairs[i][1], CAGR, sharpe, halflife, BETA, hurstEXP, adfPVALUE))
        except ValueError as e:
            print(symbList[0] + " and " + symbList[0] + " failed to work.")
            print(e.args)
        except Exception as e:
            print(e.args)
    except ValueError as e:
        print("ETFs in the pair don't have the same length of historical data.")
        print(e.args)
    except Exception as e:
        print(e.args)

# sorts the list of PAIR objects by their CAGR attribute
pairsLIST.sort(key=lambda x: x.CAGR, reverse=True)

numDAYS = []
ETF_X = []
ETF_Y = []
CAGR = []
SHARPE = []
HALF_LIFE = []
BETA = []
HURST = []
ADF_PVALUE = []

for i in range(len(pairsLIST)):
    numDAYS.append(pairsLIST[i].num)
    ETF_X.append(pairsLIST[i].X)
    ETF_Y.append(pairsLIST[i].Y)
    CAGR.append(pairsLIST[i].CAGR)
    SHARPE.append(pairsLIST[i].SHARPE)
    HALF_LIFE.append(pairsLIST[i].hl)
    BETA.append(pairsLIST[i].BETA)
    HURST.append(pairsLIST[i].HURST)
    ADF_PVALUE.append(pairsLIST[i].p)

DATA_DF = pd.DataFrame({'numDAYS': numDAYS,
                        'X': ETF_X,
                        'Y': ETF_Y,
                        'CAGR': CAGR,
                        'SHARPE': SHARPE,
                        'HALF_LIFE': HALF_LIFE,
                        'BETA': BETA,
                        'HURST': HURST,
                        'ADF_P_VALUE': ADF_PVALUE})

writer = pd.ExcelWriter('ETF_pairs.xlsx', engine='xlsxwriter')
DATA_DF.to_excel(writer, sheet_name='Sheet1')
writer.save()

DATA_DF.to_sql('ETF_pairs', con=cnx, if_exists='replace')




