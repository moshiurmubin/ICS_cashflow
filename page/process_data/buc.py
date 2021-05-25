# Copyright (c) 2017, Frappe Technologies Pvt. Ltd. and Contributors
# MIT License. See license.txt
from __future__ import unicode_literals, print_function
import frappe
import pandas as pd
import datetime 
from datetime import date, timedelta
import time
import pymysql
from calendar import monthrange

def get_weekend(d):    
    # d = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
    n = int(d.strftime("%d"))
    m = int(d.strftime("%m"))
    y = int(d.strftime("%Y"))
    if ((n/7)<=1):
        return  datetime.datetime(y, m, 7) 
    elif ((n/7)<=2):
        return  datetime.datetime(y, m, 7*2) 
    elif ((n/7)<=3):
        return  datetime.datetime(y, m, 7*3) 
    elif ((n/7)<=4):
        return  datetime.datetime(y, m, 7*4) 
    elif ((n/7)<=5):
        r = monthrange(y, m)
        return  datetime.datetime(y, m, r[1]) 


def get_dbr():	
	connection = pymysql.connect(host='localhost',
							user='root',
							password='frappe',
							database='_1bd3e0294da19198',
							cursorclass=pymysql.cursors.DictCursor)
	with connection:
		with connection.cursor() as cursor:
			sql = "SELECT * FROM `v_buc`"
			cursor.execute(sql)
			result = cursor.fetchall()
	return result



def get_shipment_forecast():
    df = pd.DataFrame.from_records(get_dbr()).dropna()
    df["total"] = df["total"].replace({'\$':''}, regex = True)
    df["realised_day"] = pd.to_datetime(df["realised_day"])
    df["weekend"] = df["realised_day"].apply(lambda x:  get_weekend(x))

    df["margin"] = (df["amount"].astype(float) / df["margin"].astype(float))  * 100
    df["bildup_unifill"] = (df["amount"].astype(float) / df["bildup_unifill"].astype(float))  * 100
    df["erq"] = (df["amount"].astype(float) / df["erq"].astype(float))  * 100
    df["bildup_machine"] = (df["amount"].astype(float) / df["bildup_machine"].astype(float))  * 100
    df["bildup_regular"] = (df["amount"].astype(float) / df["bildup_regular"].astype(float))  * 100
    df["w"] = df["weekend"].astype(str)
    df.to_excel("shipment.xlsx")
    df = df.groupby(by=["w"])["margin","bildup_unifill","erq","bildup_machine","bildup_regular"].sum()
    df.to_excel("sum_shipment.xlsx")
    return df

print(get_shipment_forecast())
