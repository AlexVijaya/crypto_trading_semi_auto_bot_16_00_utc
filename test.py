import ccxt
import datetime

exchange = ccxt.binance()
symbol = 'BETA/BUSD'
timeframe = '1d'
ohlcv = exchange.fetch_ohlcv(symbol, timeframe)

print('Last 5 rows of OHLCV data with date and time:')
for i in range(len(ohlcv)-5, len(ohlcv)):
    row = ohlcv[i]
    timestamp = row[0]
    date_time = datetime.datetime.fromtimestamp(timestamp/1000.0).strftime('%Y-%m-%d %H:%M:%S')
    print(row + [date_time])
