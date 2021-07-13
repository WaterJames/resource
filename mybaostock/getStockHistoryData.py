import baostock as bs
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()

#### 创建连接 ####
engine_ts = create_engine('mysql://root:123456@127.0.0.1:3306/stock?charset=utf8&use_unicode=1')

def download_data():
    bs.login()
    # 获取指定日期的指数、股票数据
    stock_rs = bs.query_all_stock('2021-06-21')
    stock_df = stock_rs.get_data()
    data_df = pd.DataFrame()
    # for code in stock_df["code"]:
    #     print("Downloading :" + code)
    #     k_rs = bs.query_history_k_data_plus(code, "date,code,open,high,low,close,preclose, volume, amount, adjustflag, turn, tradestatus, pctChg, peTTM, psTTM, pcfNcfTTM, pbMRQ, isST",
    #                                         '2021-01-01', '2021-06-22')
    #     data_df = data_df.append(k_rs.get_data())
    #     table_name = code.replace('.', '_')[0:4]
    #     # k_rs.get_data().to_sql(table_name, engine_ts, if_exists="append", index=True, chunksize=5000)
    #     data_df['average_num'] = data_df['amount'].astype(float)/data_df['volume'].astype(float)

    k_rs = bs.query_history_k_data_plus('sz.002762',
                                        "date,code,open,high,low,close,preclose, volume, amount, adjustflag, turn, tradestatus, pctChg, peTTM, psTTM, pcfNcfTTM, pbMRQ, isST",
                                                                                 '2021-01-01', '2021-06-22')
    data_df = data_df.append(k_rs.get_data())

    # data_df.to_sql(table_name, engine_ts, if_exists="append", index=True, chunksize=5000)

    data_df['average_num'] = data_df.apply(get_average, axis=1)
    # data_df.to_csv("D:\\demo_assignDayData.csv", encoding="gbk", index=False)
    data_df.to_csv("D:\\demo_assignDayData.csv", encoding="gbk", index=False)
    bs.logout()
    return

def get_average(data_frame):
    volume = data_frame['volume']
    amount = data_frame['amount']
    print(amount)


    if (amount == '' or float(amount) == 0):
        print('----------------')
        return 0
    else:
        print('amount')
        # return float(volume) / float(amount)

if __name__ == '__main__':
    # 获取指定日期全部股票的日K线数据
    download_data()

