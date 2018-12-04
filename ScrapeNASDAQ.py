import sqlite3 as db
import pandas as pd
import requests
import json
from bs4 import BeautifulSoup
from time import time
from time import sleep
from random import randint
from warnings import warn

cnx = db.connect('ETF.db')

# Code uses the requests library to retrieve data from etf.com. Normally beautiful soup on its own
# works, but the full data table only loads once an HTTP request is made once the user clicks to request
# the full table. The headers are the information that make that specific request.

# NOTE: the below headers will NOT work for you. To get the headers specific to your computer, go to
# the url in the referer key-value pair below, go into the network tab/XHR requests of your developer
# tools, and then click on the 'show 100' button on the table. Copy this command as a cURL command
# and google how to convert cURL commands to Python requests. Then delete the cookies.

headers = {
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9,es-US;q=0.8,es;q=0.7',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
    'accept': '*/*',
    'referer': 'https://www.etf.com/channels/oil-etfs',
    'authority': 'www.etf.com',
    'x-requested-with': 'XMLHttpRequest',
}

response = requests.get('https://www.etf.com/etf-finder-channel-tag/80065/-aum/0/100/1', headers=headers)

content = response.content

# lxml is a Python library for parsing XML data and HTML code.

soup = BeautifulSoup(content, 'html5lib')

myJSON = json.loads(soup.body.text)

tickerNames = []
expenseRatios = []
aum = []
spreadPercentages = []
priceTrAsOf = []
priceTr1Mo = []
priceTr3Mo = []
priceTr1Yr = []
priceTr3YrAnnualized = []
priceTr5YrAnnualized = []
priceTr10YrAnnualized = []
efficiencyScores = []
tradabilityScores = []
fitScores = []
avgDailyDollarVolumes = []
avgDailyShareVolumes = []
spreads = []
dividendYields = []
pe = []
pb = []

start_time = time()
retrievals = 0

for i in range(len(myJSON)):
    tickerNames.append(myJSON[i]['ticker'])
    expenseRatios.append(myJSON[i]['fundBasics']['expenseRatio']['value'])
    aum.append(myJSON[i]['fundBasics']['aum']['value'])
    spreadPercentages.append(myJSON[i]['fundBasics']['spreadPct']['value'])
    priceTrAsOf.append(myJSON[i]['performance']['priceTrAsOf'])
    priceTr1Mo.append(myJSON[i]['performance']['priceTr1Mo']['value'])
    priceTr3Mo.append(myJSON[i]['performance']['priceTr3Mo']['value'])
    priceTr1Yr.append(myJSON[i]['performance']['priceTr1Yr']['value'])
    priceTr3YrAnnualized.append(myJSON[i]['performance']['priceTr3YrAnnualized']['value'])
    priceTr5YrAnnualized.append(myJSON[i]['performance']['priceTr5YrAnnualized']['value'])
    priceTr10YrAnnualized.append(myJSON[i]['performance']['priceTr10YrAnnualized']['value'])
    efficiencyScores.append(myJSON[i]['analysis']['efficiencyScore'])
    tradabilityScores.append(myJSON[i]['analysis']['tradabilityScore'])
    fitScores.append(myJSON[i]['analysis']['fitScore'])
    avgDailyDollarVolumes.append(myJSON[i]['analysis']['avgDailyDollarVolume'])
    avgDailyShareVolumes.append(myJSON[i]['analysis']['avgDailyShareVolume'])
    spreads.append(myJSON[i]['analysis']['spread']['value'])
    dividendYields.append(myJSON[i]['fundamentals']['dividendYield']['value'])
    pe.append(myJSON[i]['fundamentals']['equity']['pe'])
    pb.append(myJSON[i]['fundamentals']['equity']['pb'])

    # The first request was a GET request. This request is a POST request, meaning that there is
    # another data type that we are sending to the server. While the POST request returns HTML code
    # that we can parse with beautiful soup, the get request returns a JSON object. For the GET
    # request, we use the json library in Python, which turns the JSON object into an array of arrays,
    # hence the syntax above.

    # For the POST request, you will have to do a similar thing as above to get the request
    # equivalent of the cURL command. The data string tells the server that we want 10 years
    # of historical data for this list of tickers that we are iterating through.

    # NOTE: I changed the value on the referer key so that the url can change for each
    # ETF in the list we are iterating through. Don't ask me why the ticker needs to be
    # in lower case... just don't change it.

    url = 'https://www.nasdaq.com/symbol/' + myJSON[i]['ticker'].lower() + '/historical'

    headers = {
        'origin': 'https://www.nasdaq.com',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9,es-US;q=0.8,es;q=0.7',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
        'content-type': 'application/json',
        'accept': '*/*',
        'referer': url,
        'authority': 'www.nasdaq.com',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = '10y|false|' + myJSON[i]['ticker']

    response = requests.post(url, headers=headers, data=data)

    # The code below is very important. If you don't space your requests out and spam the server,
    # your IP address will be banned.

    sleep(randint(8, 15))
    retrievals += 1
    elapsed_time = time() - start_time
    print('Request:{}; Frequency: {} requests/s'.format(retrievals, retrievals / elapsed_time))
    if response.status_code != 200:
        warn('Request: {}; Status code: {}'.format(retrievals, response.status_code))
    if retrievals > 37:
        warn('Number of requests was greater than expected.')
        break

    date = []
    open = []
    high = []
    low = []
    close = []
    volume = []

    content = response.content

    soup = BeautifulSoup(content, 'html5lib')

    try:
        info = soup.tbody.find_all('tr')

        days = []

        for j in range(len(info)):
            days.append(info[j].find_all('td'))

        for j in range(len(days)):
            date.append(days[j][0].text.strip())
            open.append(days[j][1].text.strip())
            high.append(days[j][2].text.strip())
            low.append(days[j][3].text.strip())
            close.append(days[j][4].text.strip())
            volume.append(days[j][5].text.strip())

        # The pandas library of Python is extraordinarily powerful. We will use it for
        # visuals, transferring data between SQL databases and excel sheets, and statistical
        # analysis. Code creates a pandas dataframe for the historical data for the ETF,
        # writes it to an excel sheet, and then creates a sqlite table and commits it
        # to the database.

        print(date[0])

        del date[0]
        del open[0]
        del high[0]
        del low[0]
        del close[0]
        del volume[0]

        DATA_DF = pd.DataFrame({'DATE': date,
                                'OPEN': open,
                                'HIGH': high,
                                'LOW': low,
                                'CLOSE': close,
                                'VOLUME': volume})

        writer = pd.ExcelWriter(myJSON[i]['ticker'] + '_historical' + '.xlsx', engine='xlsxwriter')
        DATA_DF.to_excel(writer, sheet_name='Sheet1')
        writer.save()

        DATA_DF.to_sql(myJSON[i]['ticker'] + '_historicalDATA', con=cnx, if_exists='replace')

        cnx.commit()
    except AttributeError as e:
        print(myJSON[i]['ticker'] + " didn't have data on NASDAQ for some reason.")
        print(e.args)
    except Exception as e:
        print("Something else went wrong. Kill all of your web browsing activity and restart the browser.")
        print(e.args)

# Code creates a dataframe that gives on overview for each ETF for which
# we have historical data. Once the final commit is made, you will have all
# of the tables we have created in your ETF.db file in the same directory
# as this python code (I think). The pandas dataframes die, but their
# legacy lives in the SQL database (and the excel files if they are
# helpful at all.

fundInfo = pd.DataFrame({'TICKER': tickerNames,
                         'EXPENSE_RATIO': expenseRatios,
                         'AUM': aum,
                         'SPREAD_PERCENTAGE': spreadPercentages,
                         'priceTrAsOf': priceTrAsOf,
                         'priceTr1Mo': priceTr1Mo,
                         'priceTr3Mo': priceTr3Mo,
                         'priceTr1Yr': priceTr1Yr,
                         'priceTr3YrAnnualized': priceTr3YrAnnualized,
                         'priceTr5YrAnnualized': priceTr5YrAnnualized,
                         'priceTr10YrAnnualized': priceTr10YrAnnualized,
                         'EFFICIENCY_SCORE': efficiencyScores,
                         'TRADABILITY_SCORE': tradabilityScores,
                         'FITSCORE': fitScores,
                         'avgDailyDollarVolume': avgDailyDollarVolumes,
                         'avgDailyShareVolume': avgDailyShareVolumes,
                         'spread': spreads,
                         'DIVIDEND_YIELD': dividendYields,
                         'PE': pe,
                         'PB': pb})

writer = pd.ExcelWriter('funds.xlsx', engine='xlsxwriter')
fundInfo.to_excel(writer, sheet_name='Sheet1')
writer.save()

fundInfo.to_sql('FUNDS', con=cnx, if_exists='replace')

cnx.commit()





