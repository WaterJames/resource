import pandas as pd
import tushare as ts
from sqlalchemy import create_engine
import pymysql

pymysql.install_as_MySQLdb()
def read_data():
    sql = """SELECT str_to_date(cal_date, '%%Y%%m%%d') FROM stock.trade_calendar where is_open = 1;"""
    df = pd.read_sql_query(sql, engine_ts)
    return df

def write_data(df):
    res = df.to_sql("trade_calendar", engine_ts, if_exists="replace", index=False, chunksize=5000)
    return res

if __name__ == '__main__':
    engine_ts = create_engine('mysql://root:123456@127.0.0.1:3306/stock?charset=utf8&use_unicode=1')
    pro = ts.pro_api('187c1b4cfa6e1863857627ae4918d024da78c72c321217d6dd3031cb')

    df = pro.trade_cal(exchange='', start_date='20180101', end_date='20251231')
    write_data(df)
    print(read_data())