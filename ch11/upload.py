import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

cred = credentials.Certificate('healthcarebigdataplaybook-firebase-adminsdk-c3jxd-3b5240908d.json')
default_app = firebase_admin.initialize_app(cred, {
    'databaseURL' : 'https://healthcarebigdataplaybook-default-rtdb.asia-southeast1.firebasedatabase.app/'
})

dbRef = db.reference()
print(dbRef.get())
