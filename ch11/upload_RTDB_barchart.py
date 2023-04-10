import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
import pandas as pd

cred = credentials.Certificate('healthcarebigdataplaybook-firebase-adminsdk-c3jxd-3b5240908d.json')
default_app = firebase_admin.initialize_app(cred, {
    'databaseURL' : 'https://healthcarebigdataplaybook-default-rtdb.asia-southeast1.firebasedatabase.app/'
})

dbRef = db.reference()

df_ratio = pd.read_csv('stacked_ratio_en.csv', encoding="CP949")
updates = df_ratio.to_dict(orient='records')

# device 노드 찾기
dbDevice = dbRef.child('stackedbar')
dbDevice.set( updates )