import pandas as pd
from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager
import time
from datetime import datetime

inicio = time.time()

client = Client('y9myIjcbGKgDEfnhvD1h5fG9H7xuZYxU7sYSDShpjOWbxagY0px463iSKuYl9i8m',
                'IvjYIP84Y9fcOFRpdLh3v9RaeY6rUojAvfaj8je1Bcl2l1sZJqTJ0Dubd0PcXMXc')

info = client.get_all_tickers()
df_info = pd.DataFrame(info)
df_trades_full = pd.DataFrame()

for index, row in df_info.iterrows():
    print(row['symbol'])
    trades = client.get_my_trades(symbol=row['symbol'])
    print(trades)
    df_trades = pd.DataFrame(trades)
    df_trades["symbol"] = row['symbol']
    df_trades_full = df_trades_full.append(df_trades)
    time.sleep(0.2)

fim = time.time()

df_trades_full['datetime'] = pd.to_datetime(df_trades_full['time'], unit='ms')

df_trades_full['price'] =df_trades_full['price'].astype(float)
df_trades_full['qty'] =df_trades_full['qty'].astype(float)
df_trades_full['quoteQty'] =df_trades_full['quoteQty'].astype(float)
df_trades_full['commission'] =df_trades_full['commission'].astype(float)
df_trades_full.to_csv('df_trades_full.csv', sep=';',index=True)
print(fim - inicio)
