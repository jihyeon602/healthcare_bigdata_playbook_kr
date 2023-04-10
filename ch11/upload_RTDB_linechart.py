import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
import numpy as np
import pandas as pd
from datetime import datetime

cred = credentials.Certificate('healthcarebigdataplaybook-firebase-adminsdk-c3jxd-3b5240908d.json')
default_app = firebase_admin.initialize_app(cred, {
    'databaseURL' : 'https://healthcarebigdataplaybook-default-rtdb.asia-southeast1.firebasedatabase.app/'
})


def calc_dist(df):
  """
  calculation of distance
  """
  x_d = df['X'] - df['p_x']
  y_d = df['Y'] - df['p_y']
  z_d = df['Z'] - df['p_z']
  return np.sqrt(x_d**2 + y_d**2 + z_d**2)

def calc_timediff(df):
  """
  The time difference is calculated and then stored in the d_time column.
  """
  d1 = datetime.strptime(str(df['timestamp']), "%Y-%m-%d %H:%M:%S.%f")
  d2 = datetime.strptime(str(df['p_timestamp']), "%Y-%m-%d %H:%M:%S.%f")
  return (d1 - d2).total_seconds()

def calc_act(df):
  """
  Calculate the velocity and store it in the calc_act column.
  """
  if df['d_time'] != 0:
    return np.abs(df['dist'] / df['d_time'])
  else:
    # 정상적인 수가 아님.
    return 0


dbRef = db.reference()

act = pd.read_csv('OTg6QzA_activities.csv')

#22년1월1일 데이터만 추출해서, 가시화를 해보자
data220101 = act[ (act['timestamp'] >= '2022-01-01 00:00:00.000')  & (act['timestamp'] < '2022-01-02 00:00:00.000') ].copy()

data220101['p_timestamp'] = data220101.timestamp.shift(1)
data220101['p_x'] = data220101['X'].shift(1)
data220101['p_y'] = data220101['Y'].shift(1)
data220101['p_z'] = data220101['Z'].shift(1)

# NaN 값이 있는 행을 삭제하자. NaN가 있으면 거리 및 속도 계산할 수 없다.
# 첫번째 행이 삭제될 것이다.
tenmin = data220101.dropna().copy()

tenmin['dist'] = tenmin[["X","p_x", "Y", "p_y", "Z", "p_z"]].apply(calc_dist, axis=1)
tenmin['d_time'] = tenmin[["timestamp", "p_timestamp"]].apply(calc_timediff, axis=1)
tenmin['calc_act'] = tenmin[["dist", "d_time"]].apply(calc_act, axis=1)
tenmin['timestamp'] = tenmin['timestamp'].astype('datetime64[ns]')

cleaned = tenmin[['timestamp', 'calc_act']]
cleaned = cleaned.set_index('timestamp')

# 10분단위로 리샘플링
tenmin = cleaned.resample('10min')
result = tenmin['calc_act'].agg(['mean'])
# print(result)
# print(result.dtypes)

# 칼럼 이름 변경하기
result['hm'] = result.index.values
result['hm'] = result['hm'].dt.strftime('%H:%M')

# NaN값을 zero로 채우기
result.fillna(0, inplace=True)

updates = result.to_dict(orient='records')

print(updates)

# # device 노드 찾기
dbDevice = dbRef.child('OTg6QzA')
dbDevice.child('2022-01-01').set( updates )






# hm으로 그룹핑하기
# grouped = tenmin.groupby(['hm'])
# grouped_calc_act = grouped['calc_act']

# print(tenmin)

# newtenmin = pd.DataFrame(np.empty((144,3)), columns=['utime', 'hm','mean'])

# newtenmin[:] = np.nan

# newtenmin['utime'] = np.arange(2734938,2735082,1)
# newtenmin['hm'] = newtenmin[["utime"]].apply(hour_min2, axis=1)

# # hm으로 그룹핑하기
# grouped = tenmin.groupby(['hm'])
# grouped_calc_act = grouped['calc_act']
# result = grouped_calc_act.agg(['mean'])

# # result에서 평균값이 0를 넘는 값만 newtenmin로 결합해보자
# newData = pd.merge(newtenmin, result [ result['mean'] > 0], on="hm", how="left")

# # mean_x가 결측치면 mean_y값을 new_mean로 대입하라
# newData['new_mean'] = newData['mean_x'].fillna(newData['mean_y'])

# # NaN값을 zero로 채우기
# newData = newData.fillna(0)

# # 필요없는 컬럼 삭제하기
# newData = newData.drop(['utime','mean_x', 'mean_y'], axis=1)

# # 칼럼 이름 변경하기
# newData.columns = ['hm','mean']

# updates = newData.to_dict(orient='records')

# # device 노드 찾기
# dbDevice = dbRef.child('OTg6QzA')
# dbDevice.child('2022-01-01').set( updates )
