import numpy as np
from sqlalchemy import create_engine
import pandas as pd
from mybaostock.my_queue import MyQueue
import mysql.connector
import xlwt
import datetime
import json
mydb = mysql.connector.connect(host='localhost', user='root', passwd='123456', database='stock', auth_plugin='mysql_native_password')
mycursor = mydb.cursor()
line = 0

# 获取交易日历
def get_date():
    sql = """SELECT
    DATE_FORMAT(cal_date, '%Y-%m-%d')
    FROM
    stock.trade_calendar
    WHERE
    is_open = 1
        AND cal_date > DATE_FORMAT('2021-01-01', '%Y%m%d')
        AND cal_date < DATE_FORMAT('2021-06-19', '%Y%m%d')
        order by cal_date desc;"""
    mycursor.execute(sql)
    res = mycursor.fetchall()
    myresult = pd.DataFrame(list(res), columns=['cal_date'])
    return myresult

def search_code(every_date, code):
    sql = """    SELECT * FROM stock.stockdetail_2021 where code = '"""
    my_sql = sql + code + '\' and date=\'' + every_date + '\';'
    mycursor.execute(my_sql)

    res = mycursor.fetchall()
    myresult = pd.DataFrame(list(res), columns=['index', 'date', 'code', 'open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'adjustflag', 'turn', 'tradestatus', 'pctChg', 'peTTM', 'psTTM', 'pcfNcfTTM', 'pbMRQ', 'isST'])
    return myresult

def get_code():
    sql = """  select distinct(code) from stock.stockdetail_2021 where code like 'sz%%'; """
    # df = pd.read_sql_query(sql, mycursor)
    mycursor.execute(sql)
    res = mycursor.fetchall()
    myresult = pd.DataFrame(list(res), columns=['code'])
    return myresult

def choose_data(code):
    my_date = MyQueue()
    my_volume = MyQueue()
    my_amount = MyQueue()
    my_turn = MyQueue()

    workbook = xlwt.Workbook()
    worksheet = workbook.add_sheet('stock')

    for every_date in get_date()['cal_date']:
        # if(len(search_code(every_date, code)["turn"][0]) == 0) or (len(search_code(every_date, code)["date"][0]) == 0) or (len(search_code(every_date, code)["amount"][0]) == 0) or (len(search_code(every_date, code)["volume"][0]) == 0):
        if (search_code(every_date, code).empty == True):
            continue
        else:
            my_date.push(search_code(every_date, code)["date"])
            my_volume.push(search_code(every_date, code)["volume"])
            my_amount.push(search_code(every_date, code)["amount"])
            my_turn.push(search_code(every_date, code)["turn"])

            if(my_turn.sum() > 1):
                average_price = my_amount.sum() / my_volume.sum()
                if(average_price > float(search_code(every_date, code)["close"][0])):
                    print('**************************************************')
                    print(code)
                    print(average_price)
                    print(search_code(every_date, code)["close"][0])
                    global line
                    worksheet.write(line, 0, code)
                    worksheet.write(line, 1, average_price)
                    worksheet.write(line, 2, search_code(every_date, code)["close"][0])

                    line = line + 1
                    my_date.pop()
                    my_volume.pop()
                    my_amount.pop()
                    my_turn.pop()
                break
            else:
                continue
    workbook.save('stock_workbook.xls')

if __name__ == '__main__':

    # choose_data('sz.000661')
    for every_code in get_code()['code']:
        choose_data(every_code)
