# Copyright (c) 2017, Frappe Technologies Pvt. Ltd. and Contributors
# MIT License. See license.txt
import pandas as pd
import datetime 
from datetime import date, timedelta
import time
import pymysql
from sqlalchemy import create_engine
from calendar import monthrange
import pymysql.cursors
import numpy as np
import frappe

pvt_path ="site1.local/private/files/"

engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root",pw="frappe",db="_1bd3e0294da19198"))

connection = pymysql.connect(host='localhost',
							user='root',
							password='frappe',
							database='_1bd3e0294da19198',
							cursorclass=pymysql.cursors.DictCursor)




def get_weekend(d):    
    try:
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
    except:
        print("An exception occurred")


def get_sql_data_shipment():	
	with connection:
		with connection.cursor() as cursor:
			sql = """SELECT fob_price,order_qty,r_day,margin,bildup_unifill, bildup_machine,bildup_regular,erq FROM 
            `v_shipment_forecast` """
			# sql = "SELECT fob_price,order_qty,r_day,margin,bildup_unifill, bildup_machine,bildup_regular,erq FROM `v_shipment_forecast` where r_day >= (select value from `tabSingles` where doctype= 'Cashflow Settings'  and field = 'cf_date')"
			cursor.execute(sql)
			result = cursor.fetchall()
	return result


def get_shipment_forecast():
    df = pd.DataFrame.from_records(get_sql_data_shipment())
    # df["fob_price"] = df["fob_price"].replace({'\$':''}, regex = True)
    df["amount"] = round(df["fob_price"].astype(float) * df["order_qty"].astype(float) -  (df["fob_price"].astype(float) * df["order_qty"].astype(float) * .001))
    # df["fb_charge"] = (round(df["fob_price"].astype(float) * df["order_qty"].astype(float),3) * .001) 
    df["r_day"] = pd.to_datetime(df["r_day"])
    df["weekend"] = df["r_day"].apply(lambda x:  get_weekend(x))

    df["margin"] = (df["amount"].astype(float) / df["margin"].astype(float))  * 100
    df["bildup_unifill"] = (df["amount"].astype(float) / df["bildup_unifill"].astype(float))  * 100
    df["erq"] = (df["amount"].astype(float) / df["erq"].astype(float))  * 100
    df["bildup_machine"] = (df["amount"].astype(float) / df["bildup_machine"].astype(float))  * 100
    df["bildup_regular"] = (df["amount"].astype(float) / df["bildup_regular"].astype(float))  * 100
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.to_excel("shipment forecas.xlsx")
    df.to_sql(name="a_shipment_forecast", con=engine, if_exists = 'replace', index=False)

    df = pd.read_sql_query("""
    SELECT weekend as  'Description', 'USD' as Unit, sum(margin) as 'Sales Proceeds - Margin A/C',
    sum(bildup_unifill) as 'Sales Proceeds - Build up (Unifil payment)', 
    sum(erq)  as 'Sales Proceeds - ERQ A/C', sum(bildup_machine) as 
    'Sales Proceeds - Build Up A/C (Machinery payment)', 
    sum(bildup_regular)  as 'Sales Proceeds - Build Up A/C (Regular)'  
    FROM a_shipment_forecast GROUP BY weekend
    """, con=engine)

    df = pd.melt(df, id_vars=['Description','Unit'], value_vars={'Sales Proceeds - Margin A/C','Sales Proceeds - Build up (Unifil payment)','Sales Proceeds - ERQ A/C','Sales Proceeds - Build Up A/C (Machinery payment)','Sales Proceeds - Build Up A/C (Regular)'})
    df.rename( columns={'variable':'Descriptio'}, inplace=True )
    df.rename( columns={'Description':'variable'}, inplace=True )
    df.to_sql(name="a_shipment_forecast_sum", con=engine, if_exists = 'replace', index=False)
    df.to_excel("shipment forecas SUM by week.xlsx")
    print(df)
    return df

def get_sql_data_buc():	
	with connection:
		with connection.cursor() as cursor:
			sql = "SELECT  r_day, ex_fty_date, `total_fob_income`, margin,bildup_unifill, bildup_machine,bildup_regular,erq FROM `v_bill_under_collection` where status not like '%REALIZED%'"
			cursor.execute(sql)
			result = cursor.fetchall()
	return result

def get_buc_forecast():
    df = pd.DataFrame.from_records(get_sql_data_buc())
    # df["amount"] = df["total__fob_income"].replace({'\$':''}, regex = True)
    df["amount"] = df["total_fob_income"]
    # df["amount"] = round(df["fob_price"].astype(float) * df["order_qty"].astype(float) -  (df["fob_price"].astype(float) * df["order_qty"].astype(float) * .001))
    df["amount"] = (df['amount'].astype(float) - (df["amount"].astype(float) * .001)) 
    df["r_day"] = pd.to_datetime(df["r_day"])
    df["weekend"] = df["r_day"].apply(lambda x:  get_weekend(x))

    df["margin"] = (df["amount"].astype(float) / df["margin"].astype(float))  * 100
    df["bildup_unifill"] = (df["amount"].astype(float) / df["bildup_unifill"].astype(float))  * 100
    df["erq"] = (df["amount"].astype(float) / df["erq"].astype(float))  * 100
    df["bildup_machine"] = (df["amount"].astype(float) / df["bildup_machine"].astype(float))  * 100
    df["bildup_regular"] = (df["amount"].astype(float) / df["bildup_regular"].astype(float))  * 100
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.to_excel("shipment forecas.xlsx")
    df.to_sql(name="a_buc_forecast", con=engine, if_exists = 'replace', index=False)

    df = pd.read_sql_query("""
    SELECT weekend as  'Description', 'USD' as Unit, sum(margin) as 'Sales Proceeds - Margin A/C',
    sum(bildup_unifill) as 'Sales Proceeds - Build up (Unifil payment)', 
    sum(erq)  as 'Sales Proceeds - ERQ A/C', sum(bildup_machine) as 
    'Sales Proceeds - Build Up A/C (Machinery payment)', 
    sum(bildup_regular)  as 'Sales Proceeds - Build Up A/C (Regular)'  
    FROM a_buc_forecast GROUP BY weekend
    """, con=engine)

    df = pd.melt(df, id_vars=['Description','Unit'], value_vars={'Sales Proceeds - Margin A/C','Sales Proceeds - Build up (Unifil payment)','Sales Proceeds - ERQ A/C','Sales Proceeds - Build Up A/C (Machinery payment)','Sales Proceeds - Build Up A/C (Regular)'})
    df.rename( columns={'variable':'Descriptio'}, inplace=True )
    df.rename( columns={'Description':'variable'}, inplace=True )
    df.to_sql(name="a_buc_forecast_sum", con=engine, if_exists = 'replace', index=False)
    # df.to_excel("shipment forecas SUM by week.xlsx")
    print(df)
    return df


# get_shipment_forecast()
get_buc_forecast()