#imports
import MetaTrader5 as mt5
import time
import datetime
import pytz
timezone = pytz.timezone("Etc/UTC")

#variaveis
global user_login
user_login = 4179727
global user_pw
user_pw = "01@Cssmb3ao"
global server
server="XPMT5-PRD"
import pandas as pd
import json
import conn_firebase
db = conn_firebase.conn_firebase()


def initialize_mt5(user_login,user_pw,server):# conecte-se ao MetaTrader 5

    mt5.shutdown()

    init = "nok"

    if not mt5.initialize(login=user_login, server=server, password=user_pw):
        while init == "nok":
            if not mt5.initialize(login=user_login, server=server, password=user_pw):
                print("initialize() failed")
                mt5.shutdown()
                init = "nok"
            else:
                init = "ok"
                pass
    else:

        pass


def get_ticks(symbol, utc_from):
    initialize_mt5(user_login, user_pw, server)
    ticks = mt5.copy_ticks_from(symbol, utc_from, 10000, mt5.COPY_TICKS_INFO)
    ticks_frame = pd.DataFrame(ticks)
    ticks_frame = ticks_frame.drop_duplicates(subset = 'time_msc', keep= 'first')
    last_frame = pd.to_numeric(ticks_frame['time_msc']).max()
    ticks_frame.set_index('time_msc',inplace=True)
    json_ticks = json.loads(ticks_frame.to_json(orient="index"))
    return json_ticks,last_frame

def symbol_verify(ACAO):
    initialize_mt5(user_login, user_pw, server)
    selected = mt5.symbol_info(ACAO)
    if selected == None:
         result = 0
    else:
        result = 1
    return result


###Converter para leitura array de symbols oriundos do firebase
symbol_list = pd.read_csv('symbols_full.csv', delimiter=';')
symbol_list['symbol_verify'] = symbol_list.apply(lambda x: symbol_verify(x['ACAO']), axis=1)
symbols_df = symbol_list[symbol_list['symbol_verify'] == 1]
symbol_list = symbols_df['ACAO'].values

###
a=1
while a == 1:
    start =time.time()

    for symbol in symbol_list:
        query = db.child("ticks").child(symbol).order_by_key().limit_to_last(1).get().val()
        if query is None:
            utc_from = datetime.datetime.fromisoformat('2021-08-01')
            max = 0
        else:
            max = next(reversed(query))
            timestamp = int(max) / 1000
            utc_from = datetime.datetime.fromtimestamp(timestamp)

        ##

        try:
            ticks, last_tick = get_ticks(symbol, utc_from)
        except:
            pass
        else:
            #Insert Ticks no Firebase
            if int(max)<last_tick:
                db.child("ticks").child(symbol).update(ticks)
            else:
                pass
        ##

    fim = time.time()
    print((fim-start)*1000)


