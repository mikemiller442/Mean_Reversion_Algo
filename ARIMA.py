import sqlite3 as db
import pandas as pd
from statsmodels.tsa.arima_model import ARMA
import statsmodels.tsa.stattools as ts
from statsmodels.graphics.tsaplots import plot_pacf, plot_acf
import numpy as np
import statsmodels.tsa.stattools as ts
import matplotlib.pyplot as plt

cnx = db.connect('ETF.db')
cnx.row_factory = db.Row
c = cnx.cursor()

c.execute("SELECT * FROM XOP_historicalDATA")
days = c.fetchall()

close = []

for j in range(len(days)):
    close.append(days[j][5])

DATA = pd.DataFrame({'CLOSE': close[::-1]})

d = DATA['CLOSE'].astype(float)

df = pd.DataFrame({'price': d})

df['log_ret'] = np.log(df.price) - np.log(df.price.shift(1))

print(df)

WTI = pd.read_csv('WTI_Futures.csv', skiprows=[1])
WTI['WTI_log'] = np.log(WTI.Price) - np.log(WTI.Price.shift(1))
WTI = WTI[:1906]

print(WTI)

df['WTI'] = WTI.Price
#df['WTI'] = df.WTI.shift(-1)

print(df.WTI)

plt.plot(df.price, label='XOP')
plt.plot(df.WTI, label='WTI')
plt.ylabel('Price')
plt.xlabel('Time')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
plt.show()

plt.plot(df.log_ret, label='XOP')
plt.ylabel('Price')
plt.xlabel('Time')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
plt.show()

df = df.drop(df.index[0])

print(df)

# Perform Dickey-Fuller test:
print('Results of Dickey-Fuller Test:')
dftest = ts.adfuller(df.log_ret, autolag='AIC')
dfoutput = pd.Series(dftest[0:4], index=['Test Statistic', 'p-value', '#Lags Used', 'Number of Observations Used'])
for key, value in dftest[4].items():
    dfoutput['Critical Value (%s)' % key] = value
print(dfoutput)

plot_acf(df.log_ret, lags=50)
plt.show()

plot_pacf(df.log_ret, lags=50)
plt.show()

recent_df = df[1800:]


results = ts.arma_order_select_ic(df.log_ret, ic=['aic', 'bic'], trend='nc')

print(results)

model = ARMA(df.log_ret, order=results.aic_min_order)
model_fit = model.fit(disp=0)
print(model_fit.summary())
# plot residual errors
residuals = pd.DataFrame(model_fit.resid)

plot_acf(residuals, lags=50)
plt.show()

plot_pacf(residuals, lags=50)
plt.show()

model2 = ARMA(df.log_ret, order=results.aic_min_order, exog=df.WTI)
model2_fit = model2.fit(trend='nc', disp=0)
print(model2_fit.summary())

recent_residuals = residuals[1800:]
recent_residuals.plot(kind='kde')
plt.show()

recent_residuals.plot()
plt.plot(recent_df.log_ret)
plt.show()
print(residuals.describe())

trans_predicted_values = model_fit.predict()
print(recent_df)
print(df)
print(trans_predicted_values)
df['trans_predicted'] = trans_predicted_values
#predict_df = pd.DataFrame({'original':df.price, 'trans_predicted':trans_predicted_values})
df['predicted'] = df.price.shift(1)*np.exp(df.trans_predicted)
print(df)
recent_predicted_values = df.predicted[1800:]
plt.plot(recent_predicted_values, label='predicted')
plt.plot(recent_df.price, label='observed')
plt.legend(bbox_to_anchor=(0, 0), loc=3)
plt.show()

print(df.predicted[1800:])




