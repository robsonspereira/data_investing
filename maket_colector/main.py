#primeira versao utilizando pandas df como database

#imports
import MetaTrader5 as mt5
import pandas as pd
import time
import datetime
import pytz
#variaveis
global user_login
user_login = 4179727
global user_pw
user_pw = "01@Cssmb3ao"
global server
server="XPMT5-PRD"
import pandas_gbq
from google.oauth2 import service_account

def save_gbq(df, table, path_json_key):
    credentials = service_account.Credentials.from_service_account_file(path_json_key)
    pandas_gbq.to_gbq(dataframe = df,
                      destination_table = table,
                      project_id = "serious-conduit-216417",
                      credentials = credentials,
                      if_exists = 'append')





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


#funcao de coleta baseada em 1 dia

class dados:

    def __init__(self,symbol,timeframe,date,hour):
        dic_timeframe ={'M1': mt5.TIMEFRAME_M1, 'M2': mt5.TIMEFRAME_M2, 'M3': mt5.TIMEFRAME_M3,
                         'M4': mt5.TIMEFRAME_M4, 'M5': mt5.TIMEFRAME_M5,
                         'M6': mt5.TIMEFRAME_M6, 'M10': mt5.TIMEFRAME_M10, 'M12': mt5.TIMEFRAME_M12,
                        'M15': mt5.TIMEFRAME_M15,'M20': mt5.TIMEFRAME_M20, 'M30': mt5.TIMEFRAME_M30,
                         'H1': mt5.TIMEFRAME_H1, 'H2': mt5.TIMEFRAME_H2, 'H3': mt5.TIMEFRAME_H3, 'H4': mt5.TIMEFRAME_H4,
                         'H6': mt5.TIMEFRAME_H6,
                         'H8': mt5.TIMEFRAME_H8, 'H12': mt5.TIMEFRAME_H12, 'D1': mt5.TIMEFRAME_D1,
                         'W1': mt5.TIMEFRAME_W1}
        self.timeframe = dic_timeframe[timeframe]
        initialize_mt5(user_login,user_pw,server)
        self.symbol = symbol
        timezone = pytz.timezone("Etc/UTC")
        year = date.year
        month = date.month
        day = date.day
        self.hour_start= hour
        self.date_start = datetime.datetime(year, month, day, hour= hour_start, tzinfo=timezone)
        self.date_end = datetime.datetime(year, month, day, hour = 23, tzinfo=timezone)
        self.rates = self.get_data()
        self.rates_frame= self.get_df()

    def get_data(self):
        rates = mt5.copy_rates_range(self.symbol,self.timeframe,self.date_start,self.date_end) #alterar para data
        self.rates = rates
        mt5.shutdown()
        return self.rates

    def get_df(self):
        # a partir dos dados recebidos criamos o DataFrame
        rates_frame = pd.DataFrame(self.rates)
        # convertemos o tempo em segundos no formato datetime
        rates_frame['time']=pd.to_datetime(rates_frame['time'], unit='s')
        return rates_frame

#coleta historica


inicio = time.time()

current_date = datetime.date.today()

try:
    pd.read_csv('df.csv', index_col=[0])
except:
    df = pd.DataFrame()
    print("Novo DF")
    date_start = "2021-01-01"
    date_end = datetime.date.today()
else:
    df=pd.read_csv('df.csv', index_col= [0])
    df.reset_index(inplace=True)
    date_start = max(df['time'])
    date_end = datetime.date.today()


try:
    pd.read_csv('symbols.csv', delimiter=';')
except:
    symbol_list=[]
    date_end = datetime.date.today()
else:
    def symbol_verify(ACAO):
        initialize_mt5(user_login, user_pw, server)
        selected = mt5.symbol_info(ACAO)
        if selected == None:
            result = 0
        else:
            result = 1
        return result
    symbol_list = pd.read_csv('symbols.csv', delimiter=';')
    symbol_list['symbol_verify'] = symbol_list.apply(lambda x: symbol_verify(x['ACAO']), axis=1)
    symbols_df = symbol_list[symbol_list['symbol_verify'] == 1]
    symbol_list = symbols_df['ACAO'].values


for symbol in symbol_list:
    timeframe_list = ['M1', 'M5','M10', 'M15','M30', 'H1' ,'D1']
    for timeframe in timeframe_list:

        try:
            df[df['symbol'] == symbol]
        except:
            date_start ="2021-01-01"
            hour_start = 0
        else:
            df_symbol = df.loc[df['symbol'] == symbol]
            df_symbol = df_symbol.loc[df_symbol['timeframe']==timeframe]
            if df_symbol.empty:
                date_start = "2021-01-01"
                hour_start =0
            else:
                date_start = max(df_symbol['time'])
                hour_start = datetime.datetime.fromisoformat(date_start).hour

        datelist = pd.date_range(start=date_start,end=date_end)
        for current_date in datelist:
            df_cd = dados(symbol,timeframe,current_date,hour_start).rates_frame
            df_cd["current_date"] = current_date
            df_cd['symbol'] = symbol
            df_cd['timeframe'] = timeframe
            df = df.append(df_cd,ignore_index=True)
            print(symbol ,'_', timeframe )


fim = time.time()
df.drop_duplicates(['time', 'symbol','timeframe'], keep='last')
df.to_csv('df.csv', index=False)
json_key = 'C:/Users/robson.pereira_beyou/Documents/DataExploration/maket_colector/credentials_service_account_bq.json'
save_gbq(df, 'rates.rates_full', json_key)
print(fim - inicio)
