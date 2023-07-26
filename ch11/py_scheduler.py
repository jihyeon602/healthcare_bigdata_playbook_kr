#-*- coding: utf-8 -*-

import schedule
import time

import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
import pandas as pd

cred = credentials.Certificate('healthcarebigdataplaybook-firebase-adminsdk-c3jxd-3b5240908d.json')
default_app = firebase_admin.initialize_app(cred, {
    'databaseURL' : 'https://healthcarebigdataplaybook-default-rtdb.asia-southeast1.firebasedatabase.app/'
})


def doUpload_barchart():
  print("do doUpload_barchart")

  dbRef = db.reference()

  df_ratio = pd.read_csv('stacked_ratio_en.csv', encoding="CP949")
  updates = df_ratio.to_dict(orient='records')

  # device 노드 찾기
  dbDevice = dbRef.child('stackedbar')
  dbDevice.set( updates )

  print('done doUpload_barchart')


# 매일 오전 0시 10분에 한번 실행
schedule.every().day.at("00:10").do(doUpload_barchart)


def main():

  while True:
    schedule.run_pending()
    time.sleep(1)


if __name__ == '__main__':
  main()

