import mysql.connector

mydb = mysql.connector.connect(host='rm-2ze8vw8i4x251u9156o.mysql.rds.aliyuncs.com', user='jack', passwd='852013@aA', port='3306', database='stock', auth_plugin='mysql_native_password')
mycursor = mydb.cursor()