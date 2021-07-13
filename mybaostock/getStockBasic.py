import baostock as bs
import pandas as pd
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()

#### 创建连接 ####
engine_ts = create_engine('mysql://root:123456@127.0.0.1:3306/stock?charset=utf8&use_unicode=1')

#### 登陆系统 ####
lg = bs.login()

#### 获取证券信息 ####

data_list = pd.DataFrame()
rs_stock = bs.query_all_stock(day="2021-06-18")
df_stock = rs_stock.get_data()

for code in df_stock["code"]:
    print(code)
    rs_stock_basic = bs.query_stock_basic(code=code)
    data_list = data_list.append(rs_stock_basic.get_data())

res = data_list.to_sql("stock_basic", engine_ts, if_exists="append", index=True, chunksize=5000)
with engine_ts.begin() as cn:
    sql = """INSERT INTO stock_basic ( code, code_name, ipoDate, outDate, type, status)
            SELECT  t.code, t.code_name, t.ipoDate, t.outDate, t.type, t.status
            FROM temp_stock_basic t
            WHERE NOT EXISTS
                (SELECT 1 FROM stock_basic f
                 WHERE t.code = f.code)"""
    cn.execute(sql)

#### 登出系统 ####
bs.logout()