
import conn_firebase
import pandas as pd


db = conn_firebase.conn_firebase().child("ticks").child("ABEV3").get().val()

df = pd.DataFrame.from_dict(db, orient='index')
df['time'] = pd.to_datetime(df['time'], unit='s')

