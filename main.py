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
columns = {}
cols = []

temp_cols = cur.execute("SELECT COLUMN_NAME, DATA_TYPE FROM COLS WHERE TABLE_NAME = '%s'" % config['table_info']['table_name'])
for row in temp_cols.fetchall():
  columns[row[0]] = row[1]
#print(columns)

#if not columns:
#  temp_cols = cur.execute("SELECT COLUMN_NAME FROM COLS WHERE TABLE_NAME = '%s'" % config['table_info']['table_name'])
#else:
#  temp_cols = columns

for column in columns:
  colname = ''.join(column)
  #print(colname)
  cols.append('\"'+colname +'\"')
#print(cols)
# 헤더 만들기
with open(os.path.join(program_directory, str(sun) + '~' + str(last_sat) + '.csv') ,'w',encoding=config['table_info']['encoding']) as file :
  write = csv.writer(file, delimiter=',', lineterminator='', quotechar = "'")
  write.writerow(cols)

#connection = cx_Oracle.connect(config[0]['id'], config[0]['pw'], dsn=dsn, encoding=config[0]['encoding']) # 오라클 접속

#for row in cur.execute("select * from wb_anal_point"):
#    print(row)

#한줄 추가
with open(os.path.join(program_directory, str(sun) + '~' + str(last_sat) + '.csv'),'a',encoding=config['table_info']['encoding']) as file :
  write = csv.writer(file, lineterminator='\n')
  write.writerow('')


count_query = "select count(*) as cnt from %s"%(config['table_info']['table_name'])
count_query += " where " + config['table_info']['where'] if config['table_info']['where'] else ""
#query = "select * from %s"%(config['table_info']['table_name'])
#if columns:
#  query = "select " + ','.join(columns) + " from %s where anal_date >= to_date('%s', 'yyyy-mm-dd') and anal_date < to_date('%s', 'yyyy-mm-dd') and rownum <= 1000"%(config['table_info']['table_name'], sun, last_sun)
#else:
#  query = "select * from %s where anal_date >= to_date('%s', 'yyyy-mm-dd') and anal_date < to_date('%s', 'yyyy-mm-dd') and rownum <= 1000"%(config['table_info']['table_name'], sun, last_sun)
#print(query)
print("실행된 쿼리 : " + count_query)
cursor = cur.execute(count_query)
row_cnt = 0
for row in cursor:
  row_cnt = row[0]
fetch_count = config['table_info']['fetch_count']
if not fetch_count:
  fetch_count = 1000
start_index = 1

print("total_count : " + str(row_cnt))
print("fetch_count : " + str(fetch_count))
index_name = "/* INDEX(T " + config['table_info']['index_name'] + ") */"  if config['table_info']['index_name'] else ''
index_keys = ''
if index_name:
  for index_key in cur.execute("SELECT COLUMN_NAME FROM all_ind_columns WHERE INDEX_NAME = '%s'"%(index_name)):
    if index_keys:
      index_keys = "A.%s = B.%s"%index_key
    else:
      index_keys += "and A.%s = B.%s"%index_key

while True:
  print("start_index : " + str(start_index))
  if index_name:
    query = """
      SELECT /*+ USE_NL(A B) */ 
          B.* 
      FROM (     
          SELECT  
              ROWNUM AS RNUM 
              , TT.* 
          FROM (     
              SELECT  %s
                  *
              FROM %s
              where %s
          )TT 
          WHERE ROWNUM <= %s
      ) A 
      INNER JOIN %s B ON A.anal_point_code = B.anal_point_code AND A.anal_date = B.anal_date AND A.item_code = B.item_code
      WHERE RNUM >= %s
    """ %(index_name, config['table_info']['table_name'], config['table_info']['where'], start_index, config['table_info']['table_name'], start_index + fetch_count)
  else:
    query = "select A.*, rownum as r_num from %s A %s "%(config['table_info']['table_name'], " where " +config['table_info']['where'] if config['table_info']['where'] else "")
    query = "select * from (%s) where r_num >= %s and r_num < %s "%(query, start_index, start_index + fetch_count)
  
  print(query)
  cursor = cur.execute(query)
  #columns = [column[0] for column in cursor.description]
  ##print(columns)

  results = []
  for row in cursor.fetchall():
    results.append(dict(zip(columns, row)))

  data = []
  for result in results:
    row = ""
    for key in result:
      temp = ""
      if columns[key] == 'NUMBER' or columns[key] == 'FLOAT' or columns[key] == 'BINARY_FLOAT' or columns[key] == 'BINARY_DOUBLE':
        temp = str(result[key] if result[key] is not None else '')
      else:
        temp = '"' + str(result[key]) + '"' if result[key] is not None else '""'

      if row:
        row += "," + temp
      else:
        row = temp
    data.append(row)

    #row = ""
    #row += str(result["ANAL_POINT_CODE"])
    #row += "," + '"' + result["ANAL_INSTRU_CODE"] + '"'
    #row += "," + '"' + result["ITEM_CODE"] + '"'
    #row += "," + '"' + result["ITEM_STATUS"] + '"'
    #row += "," + str(result["ANAL_DATE"])
    #row += "," + str(result["ITEM_VAL"])
    #row += "," + str(result["UPLOAD_FLAG"])
    #cols.append(row)
  #print("################")
  #for c in cursor:
  #  print(c)
  #  for column in columns:
  #    print(column)


  #print(cols)

  with open(os.path.join(program_directory, str(sun) + '~' + str(last_sat) + '.csv'),'a', encoding=config['table_info']['encoding']) as file :
    write = csv.writer(file, delimiter='\n', quotechar="'", lineterminator='')
    write.writerow(data)
    
  start_index += fetch_count

  if start_index > row_cnt:
    break

  #한줄 추가
  with open(os.path.join(program_directory, str(sun) + '~' + str(last_sat) + '.csv'),'a',encoding=config['table_info']['encoding']) as file :
    write = csv.writer(file, lineterminator='\n')
    write.writerow('')
  
  

  #for col in cur.execute("select ANAL_POINT_CODE,ANAL_INSTRU_CODE,ITEM_CODE,ITEM_STATUS, TO_CHAR(ANAL_DATE, 'YYYY/MM/DD HH24:MI:SS') ANAL_DATE,ITEM_VAL,UPLOAD_FLAG from %s"% config['table_info']['table_name']):
  #  colname = ''.join(str(col))
  #  colname = colname.replace("'", '"').replace("(", "").replace(")", "")
  #  print(colname)
  #  cols.append(colname)
    

  #with open(os.path.join(os.path.dirname(__file__), sun + '~' + last_sat + '.csv','a', encoding=config['table_info']['encoding']) as file :
  #    write = csv.writer(file, delimiter=',', lineterminator='\r\n', quotechar = '"')
  #    write.writerow(cols)