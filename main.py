import json
import sys
import csv
import os
from datetime import date
from datetime import timedelta
#import secrets
#import getpass
#import ssl
#import cryptography
#from pbkdf2 import PBKDF2
import cx_Oracle as oracledb

print(__file__)
print()
print(os.path.abspath(__file__))
today = date.today()
diff = (today.weekday() - 6) % 7
last_sun = today - timedelta(days=diff)
sun = today - timedelta(days=diff) - timedelta(7)
last_sat = today - timedelta(days=diff) - timedelta(1)
print(sun)
print(last_sun)

#oracledb.init_oracle_client(lib_dir=r"C:\instantclient_19_14")
if getattr(sys, 'frozen', False):
  #test.exe로 실행한 경우,test.exe를 보관한 디렉토리의 full path를 취득
  program_directory = os.path.dirname(os.path.abspath(sys.executable))
else:
  program_directory = os.path.dirname(os.path.abspath(__file__))

oracledb.init_oracle_client()
with open(os.path.join(program_directory, 'config.json')) as f:
    config = json.load(f)
#print(config['db_info'])
#if config['db_info']['service_name']:
#  dsn = cx_Oracle.makedsn(config['db_info']['ip'], config['db_info']['port'], service_name = config['db_info']['service_name'] ) # 오라클 주소
#else:
#  dsn = cx_Oracle.makedsn(config['db_info']['ip'], config['db_info']['port'], sid = config['db_info']['sid'] ) # 오라클 주소

connection = oracledb.connect(user=config['db_info']['user'], password=config['db_info']['password'], dsn=config['db_info']['dsn'])
print("SELECT * FROM COLS WHERE TABLE_NAME = '%s'" % config['table_info']['table_name'])
cur = connection.cursor() # 실행 결과 데이터를 담을 메모리 객체
columns = config['table_info']['columns']
cols = []
temp_cols=None

if not columns:
  temp_cols = cur.execute("SELECT COLUMN_NAME FROM COLS WHERE TABLE_NAME = '%s'" % config['table_info']['table_name'])
else:
  temp_cols = columns

for col in temp_cols:
  colname = ''.join(col)
  columns.append(colname)
  
  #cols.append("\"" + ','.join(col) + "\"")
  cols.append('\"'+colname +'\"')
print("###################")
print(columns)
print("###################")
with open(os.path.join(program_directory, str(sun) + '~' + str(last_sat) + '.csv') ,'w',encoding=config['table_info']['encoding']) as file :
  write = csv.writer(file, delimiter=',', lineterminator='', quotechar = "'")
  write.writerow(cols)

#connection = cx_Oracle.connect(config['db_info']['id'], config['db_info']['pw'], dsn=dsn, encoding=config['db_info']['encoding']) # 오라클 접속

#for row in cur.execute("select * from wb_anal_point"):
#    print(row)

with open(os.path.join(program_directory, str(sun) + '~' + str(last_sat) + '.csv'),'a',encoding=config['table_info']['encoding']) as file :
  write = csv.writer(file, lineterminator='\n')
  write.writerow('')
results = []


query = ""
if columns:
  query = "select " + ','.join(columns) + " from %s where anal_date >= to_date('%s', 'yyyy-mm-dd') and anal_date < to_date('%s', 'yyyy-mm-dd') and rownum <= 1000"%(config['table_info']['table_name'], sun, last_sun)
else:
  query = "select * from %s where anal_date >= to_date('%s', 'yyyy-mm-dd') and anal_date < to_date('%s', 'yyyy-mm-dd') and rownum <= 1000"%(config['table_info']['table_name'], sun, last_sun)
#print(query)
cursor = cur.execute(query)
#columns = [column[0] for column in cursor.description]
##print(columns)

#for row in cursor.fetchall():
#  results.append(dict(zip(columns, row)))
##print(results)

#cols.clear()
#for result in results:
#  row = ""
#  row += str(result["ANAL_POINT_CODE"])
#  row += "," + '"' + result["ANAL_INSTRU_CODE"] + '"'
#  row += "," + '"' + result["ITEM_CODE"] + '"'
#  row += "," + '"' + result["ITEM_STATUS"] + '"'
#  row += "," + str(result["ANAL_DATE"])
#  row += "," + str(result["ITEM_VAL"])
#  row += "," + str(result["UPLOAD_FLAG"])
#  cols.append(row)

for c in cursor:
  for column in columns:
    pass


#print(cols)

with open(os.path.join(program_directory, str(sun) + '~' + str(last_sat) + '.csv'),'a', encoding=config['table_info']['encoding']) as file :
    write = csv.writer(file, delimiter='\n', quotechar="'")
    write.writerow(cols)

#for col in cur.execute("select ANAL_POINT_CODE,ANAL_INSTRU_CODE,ITEM_CODE,ITEM_STATUS, TO_CHAR(ANAL_DATE, 'YYYY/MM/DD HH24:MI:SS') ANAL_DATE,ITEM_VAL,UPLOAD_FLAG from %s"% config['table_info']['table_name']):
#  colname = ''.join(str(col))
#  colname = colname.replace("'", '"').replace("(", "").replace(")", "")
#  print(colname)
#  cols.append(colname)
  

#with open(os.path.join(os.path.dirname(__file__), sun + '~' + last_sat + '.csv','a', encoding=config['table_info']['encoding']) as file :
#    write = csv.writer(file, delimiter=',', lineterminator='\r\n', quotechar = '"')
#    write.writerow(cols)