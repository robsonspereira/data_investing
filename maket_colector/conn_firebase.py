import pyrebase


def conn_firebase():
    firebaseConfig = {
    "apiKey": "AIzaSyBSRz-W_85T47pe72sAIkYvfq9_3OpmlOA",
    "authDomain": "data-market-5adeb.firebaseapp.com",
    "databaseURL": "https://data-market-5adeb-default-rtdb.firebaseio.com",
    "projectId": "data-market-5adeb",
    "storageBucket": "data-market-5adeb.appspot.com",
    "messagingSenderId": "405012665002",
    "appId": "1:405012665002:web:85f5b651f8c55ea0d476d3",
    "measurementId": "G-845GDBT0CP"
    }

    firebase =  pyrebase.initialize_app(firebaseConfig)
    db = firebase.database()
    return db

