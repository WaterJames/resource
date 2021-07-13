import baostock as bs
import pandas as pd
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
from datetime import datetime

engine_ts = create_engine('mysql://root:123456@127.0.0.1:3306/stock?charset=utf8&use_unicode=1')


def download_data(date):

    bs.login()

    # 获取指定日期的指数、股票数据
    stock_rs = bs.query_all_stock(date)
    print(stock_rs)
    stock_df = stock_rs.get_data()
    print(stock_df)
    data_df = pd.DataFrame()
    for code in stock_df["code"]:
        print("Downloading :" + code)
        k_rs = bs.query_history_k_data_plus(code, "date,code,open,high,low,close,preclose, volume, amount, adjustflag, turn, tradestatus, pctChg, peTTM, psTTM, pcfNcfTTM, pbMRQ, isST", start_date=date, end_date=date)
        data_df = data_df.append(k_rs.get_data())
    bs.logout()

    res = data_df.to_sql("stockdetail_2021", engine_ts, if_exists="append", index=True, chunksize=5000)
    # with engine_ts.begin() as cn:
    #     sql = """INSERT INTO stockDaily ( ts_code, trade_date, open, high, low, close, pre_close, t_change, pct_chg, vol)
    #             SELECT  t.ts_code, t.trade_date, t.open, t.high, t.low, t.close, t.pre_close, t.change, t.pct_chg, t.vol
    #             FROM temp_stockDaily t
    #             WHERE NOT EXISTS
    #                 (SELECT 1 FROM stockDaily f
    #                  WHERE t.ts_code = f.ts_code
    #                  AND t.trade_date = f.trade_date)"""
    #     cn.execute(sql)

    return res


# 获取交易日历
def get_date():
    sql = """SELECT 
    STR_TO_DATE(cal_date, '%%Y%%m%%d') cal_date
FROM
    stock.trade_calendar
WHERE
    is_open = 1
        AND cal_date > DATE_FORMAT('2021-06-16', '%%Y%%m%%d')
        AND cal_date < DATE_FORMAT('2022-06-18', '%%Y%%m%%d');"""
    df = pd.read_sql_query(sql=sql, con=engine_ts)
    return df

if __name__ == '__main__':
    # 获取指定日期全部股票的日K线数据
    for row in get_date()["cal_date"]:
        print(row)
        download_data(row.strftime('%Y-%m-%d'))
