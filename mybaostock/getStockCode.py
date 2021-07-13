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
rs_stock = bs.query_all_stock(day="2021-06-18")

#### MySQL保存结果集 ####
data_list = []
while (rs_stock.error_code == '0') & rs_stock.next():
    # 获取一条记录，将记录合并在一起
    data_list.append(rs_stock.get_row_data())
result = pd.DataFrame(data_list, columns=rs_stock.fields)
res = result.to_sql("stock_code", engine_ts, if_exists="append", index=True, chunksize=5000)

with engine_ts.begin() as cn:
    sql = """INSERT INTO stock_code ( code, tradeStatus, code_name)
            SELECT  t.code, t.tradeStatus, t.code_name
            FROM temp_stock_code t
            WHERE NOT EXISTS
                (SELECT 1 FROM stock_code f
                 WHERE t.code = f.code)"""
    cn.execute(sql)

print(result)

#### 登出系统 ####
bs.logout()