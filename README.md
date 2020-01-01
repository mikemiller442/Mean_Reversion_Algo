# Mean_Reversion_Algo
This project employs a variety of models to execute a mean reversion trading strategy. In general mean reversion strategies depend on trading collections of securities that are cointegrated, meaning that a linear combination of the securities forms a stationary time series, even if the individual securities are not stationary themselves. Stationary time series have fixed mean and fixed variance, allowing us to profitably trade them because they eventually return to their fixed mean. For example, if two bond ETF's are cointegrated, you can enter a long position on the underperforming ETF and a short position on the overperforming ETF, hedging your position and presumably making a profit when the spread between them reverts to the mean.

The augmented Dicky-Fuller test (DFT) is a significance test with the null hypothesis that a time series has a unit root, meaning that stochastic shocks have permanent effects on the time series, making it non-stationary. This project extensively uses the DFT to select pairs of ETFs that are cointegrated and that can be profitably traded. For baskets of more than two ETFs, the Johansen test can assess stationarity in a linear combination of several ETFs.

MeanReversion.py backtests a simple pairwise trading strategy that assumes a fixed hedge ratio between the two securities. This hedge ratio is gained by regressing an ETF on another cointegrated ETF and using the regression coefficient as the hedge ratio if the residuals are stationary by the DFT. Then you can track the spread using this hedge ratio in real time and enter a long-short position when the residual exceeds 1.96 standard deviations.

KalmanFilter.py improves this by updating this hedge ratio and the variance of the residuals in a Bayesian manner as evidence is collected. This is definitely superior because the hedge ratio will most likely change in time, especially if the ETFs appreciate in value or if their volatility increases.

VECM.py is a different way to improve the original strategy by using vector error correction models to predict the short term deviations in cointegrated ETFs. This gives a more precise view of the short term movements because it relies on autoregressive models that utilize autocorrelation across both securities. Alpaca_Trade.py executes trades on alpaca markets using indicators from these VECM models.

Different data sets are scraped from NASDAQ and collected from Polygon.
