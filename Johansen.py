import sqlite3 as db
import pandas as pd
from statsmodels.tsa.arima_model import ARMA
from statsmodels.tsa.vector_ar import vecm
import numpy as np
import statsmodels.tsa.stattools as ts


def johansenSignificance(lr1, lr2, cvt, cvm):
    for i in range(3):
        if lr1[i] > cvt[0][i]:
            return False
    for i in range(3):
        if lr2[i] > cvm[0][i]:
            return False
    return True


def testCointegration(cointegratedPairs):
    for i in range(len(cointegratedPairs)):
        mutualCointegrated = cointegratedPairs[i].etf1.cointegrated
        for j in range(len(cointegratedPairs[i].etf2.cointegrated)):
            if cointegratedPairs[i].etf2.cointegrated[j] not in mutualCointegrated:
                mutualCointegrated.append(cointegratedPairs[j].etf2.cointegrated[j])
        for j in range(len(mutualCointegrated)):
            if mutualCointegrated[j].ticker == cointegratedPairs[i].etf1.ticker or mutualCointegrated[j].ticker == cointegratedPairs[i].etf2.ticker:
                continue
            else:
                endog1 = cointegratedPairs[i].etf1.data
                endog2 = cointegratedPairs[i].etf2.data
                endog3 = mutualCointegrated[j].data
                min_length = min(len(endog1), len(endog2), len(endog3))
                if min_length < len(endog1):
                    rowsToDelete = len(endog1) - min_length
                    endog1 = endog1[:-rowsToDelete]
                if min_length < len(endog2):
                    rowsToDelete = len(endog2) - min_length
                    endog2 = endog2[:-rowsToDelete]
                if min_length < len(endog3):
                    rowsToDelete = len(endog3) - min_length
                    endog3 = endog3[:-rowsToDelete]
                coint_data = pd.DataFrame({cointegratedPairs[i].etf1.ticker: endog1,
                                           cointegratedPairs[i].etf2.ticker: endog2,
                                           mutualCointegrated[j].ticker: endog3})
                try:
                    johansen_result = vecm.coint_johansen(coint_data, 1, 2)
                    if johansenSignificance(johansen_result.lr1, johansen_result.lr2, johansen_result.cvt, johansen_result.cvm):
                        selected_orders_VEC = vecm.select_order(coint_data, maxlags=4)
                        print(selected_orders_VEC)
                        if selected_orders_VEC.aic > 0:
                            model_VEC = vecm.VECM(coint_data, k_ar_diff=selected_orders_VEC.aic)
                            model_VEC_fit = model_VEC.fit()
                            print(model_VEC_fit.summary())
                except np.linalg.linalg.LinAlgError as e:
                    print("SVD did not converge.")
                    print(e.args)
                except Exception as e:
                    print(e.args)

