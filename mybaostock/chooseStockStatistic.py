from sqlalchemy import create_engine
import pandas as pd
from mybaostock.my_queue import MyQueue
import pymysql

pymysql.install_as_MySQLdb()
engine_ts = create_engine('mysql://root:123456@127.0.0.1:3306/stock?charset=utf8&use_unicode=1')

# 获取交易日历
def get_date():
    sql = """SELECT 
    STR_TO_DATE(cal_date, '%%Y%%m%%d') cal_date
FROM
    stock.trade_calendar
WHERE
    is_open = 1
        AND cal_date > DATE_FORMAT('2019-01-01', '%%Y%%m%%d')
        AND cal_date < DATE_FORMAT('2020-01-01', '%%Y%%m%%d')
        order by cal_date desc;"""
    df = pd.read_sql_query(sql, engine_ts)
    return df

def search_code(every_date, code):
    # code = 'sz.000930'
    sql = """    SELECT * FROM stock.stockdetail_2019 where code = '"""
    my_sql = sql + code + '\' and date=\'' + every_date.strftime('%Y-%m-%d') +'\';'
    # my_sql = sql + code + '\' and date=\'' + every_date +'\';'
    df = pd.read_sql_query(my_sql, engine_ts)
    return df

def get_code():
    sql = """  select distinct(code) from stock.stockdetail_2019; """
    df = pd.read_sql_query(sql, engine_ts)
    return df

def choose_data(code):
    my_date = MyQueue()
    my_volume = MyQueue()
    my_amount = MyQueue()
    my_turn = MyQueue()

    for every_date in get_date()["cal_date"]:

        my_date.push(search_code(every_date, code)["date"])
        my_volume.push(search_code(every_date, code)["volume"])
        my_amount.push(search_code(every_date, code)["amount"])
        my_turn.push(search_code(every_date, code)["turn"])

        if(my_turn.sum() > 1):
            average_price = my_amount.sum() / my_volume.sum() * 0.95
            if(average_price > float(search_code(every_date, code)["close"])):
                print('**************************************************')
                print(average_price)
                print(my_date.items)
                print(my_volume.items)
                print(my_amount.items)
                print(my_turn.items)

                my_date.pop()
                my_volume.pop()
                my_amount.pop()
                my_turn.pop()
        else:
            continue

if __name__ == '__main__':

    for every_code in get_code()["code"]:
        choose_data(every_code)