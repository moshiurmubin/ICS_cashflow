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
	with connection:
		with connection.cursor() as cursor:
			sql = "SELECT * FROM `v_shipment_forecast` where considerable_handover <> 'BUC' "
			cursor.execute(sql)
			result = cursor.fetchall()
	return result



def create_rp_data():
    start = ''
    end = ''
    selected_members = ''
    # query = f"""
    # SELECT member_id, yearmonth FROM queried_table 
    # WHERE yearmonth BETWEEN {start} AND {end}
    # AND member_id IN {selected_members}
    # """
    query_tabcashflow_report = f"""
    SELECT report_line, report_col, ex_date, amount from tabcashflow_report
    """
    query_report_data = f"""
    INSERT INTO tabcashflow_report (report_line, report_col, ex_date, amount) 
    VALUES  (%s, %s, %s, %s)
    """
    dfrl = pd.read_sql('tabReport Lines', con=engine)
    dfrc = pd.read_sql('tabCF Columns', con=engine)

    row_name = ''
    col_name = ''
    for index, row in dfrl.iterrows():
        print('xxxxxxxxxxxx')
        print(row['name'])  
        for index, row in dfrc.iterrows():
            print(row['name'], row['date1']) 
            # frappe.db.sql("INSERT INTO tabcashflow_report (report_line) VALUES ('XXXX')", as_dict=True)
            # todo = frappe.get_doc({"doctype":"ToDo", "description": "test"})
            # cursor = connection.cursor()
            # cursor.execute(query_report_data, ('1008','Kabir','01-02-2001','100'))
            # connection.commit()

            
            # print(df)

    # print(dfrc)

# create_rp_data()

def get_conn():
    connection = pymysql.connect(host='localhost',
							user='root',
							password='frappe',
							database='_1bd3e0294da19198')
    return connection

def run_sql(sql):	
	connection = pymysql.connect(host='localhost',
							user='root',
							password='frappe',
							database='_1bd3e0294da19198',
							cursorclass=pymysql.cursors.DictCursor)
	with connection:
		with connection.cursor() as cursor:
			# sql = "SELECT * FROM `v_shipment_forecast` where considerable_handover <> 'BUC' "
			cursor.execute(sql)
			result = cursor.fetchall()
    # connection.commit()
	return result

def get_sql_data_shipment():	
	with connection:
		with connection.cursor() as cursor:
			sql = "SELECT fob_price,order_qty,r_day,margin,bildup_unifill, bildup_machine,bildup_regular,erq FROM `v_shipment_forecast` where r_day >= (select value from `tabSingles` where doctype= 'Cashflow Settings'  and field = 'cf_date')"
			cursor.execute(sql)
			result = cursor.fetchall()
	return result


def process_shipment_forecast():
    with connection:
        with connection.cursor() as cursor:
            sql = """UPDATE    `tabShipment Forecast` SET 
            ex_date = exfty_date,
            fob_price = fobpc,
            rate = fobpc,
            quantity = po_qty,
            order_qty = po_qty,
            total = fobpc * po_qty,
            original_shipment_date = oc"""
            cursor.execute(sql)
    df = pd.DataFrame.from_records(get_sql_data_shipment())
            # result = cursor.fetchall()
            # for row in result:
                # print(row["name"])

def get_shipment_forecast():
    df = pd.DataFrame.from_records(get_sql_data_shipment())
    df["fob_price"] = df["fob_price"].replace({'\$':''}, regex = True)
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
			sql = "SELECT  r_day, ex_fty_date, `total__fob_income`, margin,bildup_unifill, bildup_machine,bildup_regular,erq FROM `v_bill_under_collection` where status not like '%REALIZED%'"
			cursor.execute(sql)
			result = cursor.fetchall()
	return result

def get_buc_forecast():
    df = pd.DataFrame.from_records(get_sql_data_buc())
    # df["amount"] = df["total__fob_income"].replace({'\$':''}, regex = True)
    df["amount"] = df["total__fob_income"]
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

def get_sql_data_buc_act():	
	with connection:
		with connection.cursor() as cursor:
			sql = "SELECT  r_day, ex_fty_date, `total__fob_income`, margin,bildup_unifill, bildup_machine,bildup_regular,erq FROM `v_bill_under_collection` where status like '%REALIZED%'"
			cursor.execute(sql)
			result = cursor.fetchall()
	return result

def get_buc_actual():
    df = pd.DataFrame.from_records(get_sql_data_buc_act())
    if not df.empty:
        df["amount"] = df["total__fob_income"].replace({'\$':''}, regex = True)
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
        df.to_excel("Actual shipment.xlsx")
        df.to_sql(name="a_buc_a", con=engine, if_exists = 'replace', index=False)

        df = pd.read_sql_query("""
        SELECT weekend as  'Description', 'USD' as Unit, sum(margin) as 'Sales Proceeds - Margin A/C',
        sum(bildup_unifill) as 'Sales Proceeds - Build up (Unifil payment)', 
        sum(erq)  as 'Sales Proceeds - ERQ A/C', sum(bildup_machine) as 
        'Sales Proceeds - Build Up A/C (Machinery payment)', 
        sum(bildup_regular)  as 'Sales Proceeds - Build Up A/C (Regular)'  
        FROM a_buc_a GROUP BY weekend
        """, con=engine)

        df = pd.melt(df, id_vars=['Description','Unit'], value_vars={'Sales Proceeds - Margin A/C','Sales Proceeds - Build up (Unifil payment)','Sales Proceeds - ERQ A/C','Sales Proceeds - Build Up A/C (Machinery payment)','Sales Proceeds - Build Up A/C (Regular)'})
        df.rename( columns={'variable':'Descriptio'}, inplace=True )
        df.rename( columns={'Description':'variable'}, inplace=True )
        df.to_sql(name="a_buc_a_sum", con=engine, if_exists = 'replace', index=False)
        # df.to_excel("shipment forecas SUM by week.xlsx")
        print(df)
    return df


#usecols=range(10)
def ActExpensesIncome():

    df = pd.read_excel('~/frappe-bench/sites/site1.local/private/files/ICS-CF-Form 04.04.21.xlsx', index_col=None, header=67, skiprows=0, sheet_name='Act Expenses')
    # df.drop([0], inplace=True)
    # df.drop(columns=['Apr', 'May','Jun'],inplace=True)
    df.rename( columns={'Summary':'Description'}, inplace=True )
    col_len =len(df.columns)
    dfr = df.iloc[:12] # number of rows 
    dfr = dfr.iloc[:, 2:col_len] # set columns dfc.columns[[5,len(dfc.columns)-1]]
    dfr.columns = [str(col)[:10] + '' for col in dfr.columns]
    dfc = dfr.iloc[:, 3:col_len] 
    dfr.to_excel("Actual Expenses.xlsx")
    dfr = pd.melt(dfr, id_vars=['Descriptio','Unit'], value_vars=dfc.columns)
    # dfr.dropna(inplace=True)
    dfr.to_sql(name="actual_expenses", con=engine, if_exists = 'replace', index=False)

    df = pd.read_excel('~/frappe-bench/sites/site1.local/private/files/ICS-CF-Form 04.04.21.xlsx', index_col=None, header=95, skiprows=0, sheet_name='Act Expenses')
    # df.drop([0], inplace=True)
    df.rename( columns={'Summary':'Description'}, inplace=True )
    df.drop(columns=['Apr', 'May','Jun'],inplace=True)
    col_len =len(df.columns)
    dfr = df.iloc[:25] # number of rows 
    dfr = dfr.iloc[:, 2:col_len] # set columns dfc.columns[[5,len(dfc.columns)-1]]
    dfr.columns = [str(col)[:10] + '' for col in dfr.columns]
    dfc = dfr.iloc[:, 3:col_len] 
    dfr.to_excel("Actual_Expense_SUMMARY_ERQ_MARGIN.xlsx")
    dfr = pd.melt(dfr, id_vars=['Descriptio','Unit'], value_vars=dfc.columns)
    # dfr.dropna(inplace=True)
    dfr.to_sql(name="actual_expense_SUMMARY_ERQ_MARGIN", con=engine, if_exists = 'replace', index=False)


    #Budgeted Expenses
    # df = pd.read_excel('~/frappe-bench/sites/site1.local/private/files/ICS-CF-Form 04.04.21.xlsx', index_col=None, header=1,skiprows=0, sheet_name='Proj. Expenses income')
    # # df = pd.read_excel('~/frappe-bench/sites/site1.local/private/files/ICS-CF-Form 04.04.21.xlsx', index_col=0, dtype={'Name': str, 'Value': float})
    # # df.columns = df.columns.str.replace('Unnamed.*', '') + df.iloc[0].fillna('') 
    # # df.drop([0,1], inplace=True)
    # # print(df.head(5))
    # df.rename( columns={'Unnamed: 0':'SLNO'}, inplace=True )
    # df.rename( columns={'Unnamed: 1':'ACC'}, inplace=True )
    # df.rename( columns={'Unnamed: 2':'Description'}, inplace=True )
    # df.rename( columns={'Unnamed: 3':'Unit'}, inplace=True )
    # df.rename( columns={'Unnamed: 4':'new column name'}, inplace=True )
    # df.drop([0], inplace=True)
    # print(df.columns)
    # print(df[:62])
    # df.to_excel("Monthly Budgeted Expenses.xlsx")


def is_date_valid(year, month, day):
    this_date = '%d/%d/%d' % (month, day, year)
    try:
        time.strptime(this_date, '%m/%d/%Y')
    except ValueError:
        return False
    else:
        return True

def ProjExpensesIncome():
    df = pd.read_excel('~/frappe-bench/sites/site1.local/private/files/ICS-CF-Form 04.04.21.xlsx', index_col=None, header=0, skiprows=64, sheet_name='Proj. Expenses income')
    # df.drop([0], inplace=True)
    df.drop(columns=['Apr', 'May','Jun'],inplace=True)
    df.rename( columns={'Summary':'Description'}, inplace=True )
    col_len =len(df.columns)
    dfr = df.iloc[0:25] # number of rows 
    dfr = dfr.iloc[:, 2:col_len] # set columns dfc.columns[[5,len(dfc.columns)-1]]
    dfr.columns = [str(col)[:10] + '' for col in dfr.columns]
    dfc = dfr.iloc[:, 3:col_len] 
    dfr.to_excel("Expense Summary CD SOD.xlsx")
    dfr = pd.melt(dfr, id_vars=['Descriptio','Unit'], value_vars=dfc.columns)
    # dfr.dropna(inplace=True)
    dfr.to_sql(name="Expense_Summary_CD_SOD", con=engine, if_exists = 'replace', index=False)

    df = pd.read_excel('~/frappe-bench/sites/site1.local/private/files/ICS-CF-Form 04.04.21.xlsx', index_col=None, header=0,skiprows=93, sheet_name='Proj. Expenses income')
    df.drop(columns=['Apr', 'May','Jun'],inplace=True)
    df.rename( columns={'Summary':'Description'}, inplace=True )
    col_len =len(df.columns)
    dfr = df.iloc[0:11] # number of rows 
    dfr = dfr.iloc[:, 2:col_len] # set columns dfc.columns[[5,len(dfc.columns)-1]]
    dfr.columns = [str(col)[:10] + '' for col in dfr.columns]
    dfc = dfr.iloc[:, 3:col_len] 
    # print(dfr)
    dfr.to_excel("Expense Summary ERQ MARGIN.xlsx")
    dfr = pd.melt(dfr, id_vars=['Descriptio','Unit'], value_vars=dfc.columns)
    # dfr.dropna(inplace=True)
    dfr.to_sql(name="Expense_Summary_ERQ_MARGIN", con=engine, if_exists = 'replace', index=False)

    df = pd.read_excel('~/frappe-bench/sites/site1.local/private/files/ICS-CF-Form 04.04.21.xlsx', index_col=None, header=0,skiprows=108, sheet_name='Proj. Expenses income')
    df.drop(columns=['Apr', 'May','Jun'],inplace=True)
    df.rename( columns={'Unnamed: 2':'Description'}, inplace=True )
    df.rename( columns={'Unnamed: 3':'Unit'}, inplace=True )
    # df.drop([0,1], inplace=True)
    df.drop([0], inplace=True)
    col_len =len(df.columns)    
    dfr = df.iloc[0:19] # number of rows 
    dfr = dfr.iloc[:, 2:col_len] # set columns dfc.columns[[5,len(dfc.columns)-1]]
    dfr.columns = [str(col)[:10] + '' for col in dfr.columns]
    dfc = dfr.iloc[:, 3:col_len] 
    # # print(dfr)
    dfr.to_excel("Budget_exp.xlsx")
    dfr = pd.melt(dfr, id_vars=['Descriptio','Unit'], value_vars=dfc.columns)
    # dfr.dropna(inplace=True)
    dfr.to_sql(name="budget_exp", con=engine, if_exists = 'replace', index=False)

# ProjExpensesIncome()

def bank_balance():
    #Bank Balance 
    df = pd.read_excel('~/frappe-bench/sites/site1.local/private/files/ICS-CF-Form 04.04.21.xlsx', index_col=None, header=2, skiprows=0, sheet_name='Bank Balance & Others')
    # df.rename( columns={'Unnamed: 0':'Description'}, inplace=True )
    df.rename( columns={'Unnamed: 2':'Description'}, inplace=True )
    df.rename( columns={'Unnamed: 3':'Unit'}, inplace=True )
    # df.drop([0,1], inplace=True)
    # df.drop([0], inplace=True)
    df.drop(columns=['Apr', 'May','Jun'],inplace=True)
    col_len =len(df.columns)
    dfr = df.iloc[2:6] # number of rows 
    # dfr = dfr.iloc[:, 2:col_len] # set columns dfc.columns[[5,len(dfc.columns)-1]]
    dfr.columns = [str(col)[:10] + '' for col in dfr.columns]
    dfc = dfr.iloc[:, 4:col_len] 
    dfr.to_excel("Bank Balance.xlsx")
    dfr = pd.melt(dfr, id_vars=['Descriptio','Unit'], value_vars=dfc.columns)
    # dfr.dropna(inplace=True)
    dfr.to_sql(name="bank_balance", con=engine, if_exists = 'replace', index=False)

    # print(dfr)
 
    #Cash Inflow projection From Export
    df = pd.read_excel('~/frappe-bench/sites/site1.local/private/files/ICS-CF-Form 04.04.21.xlsx', index_col=None, header=2, skiprows=0, sheet_name='Bank Balance & Others')
    # df.rename( columns={'Unnamed: 0':'Description'}, inplace=True )
    df.rename( columns={'Unnamed: 2':'Description'}, inplace=True )
    df.rename( columns={'Unnamed: 3':'Unit'}, inplace=True )
    # df.drop([0,1], inplace=True)
    # df.drop([0], inplace=True)
    df.drop(columns=['Apr', 'May','Jun'],inplace=True)
    col_len =len(df.columns)
    dfr = df.iloc[12:22] # number of rows 
    # dfr = dfr.iloc[:, 2:col_len] # set columns dfc.columns[[5,len(dfc.columns)-1]]
    dfr.columns = [str(col)[:10] + '' for col in dfr.columns]
    dfc = dfr.iloc[:, 4:col_len] 
    dfr.to_excel("Cash Inflow projection From Export.xlsx")
    dfr = pd.melt(dfr, id_vars=['Descriptio','Unit'], value_vars=dfc.columns)
    # dfr.dropna(inplace=True)
    dfr.to_sql(name="cash_inflow_projection_from_export", con=engine, if_exists = 'replace', index=False)

    # print(dfr)


    #Actual Export 
    df = pd.read_excel('~/frappe-bench/sites/site1.local/private/files/ICS-CF-Form 04.04.21.xlsx', index_col=None, header=2, skiprows=0, sheet_name='Bank Balance & Others')
    # df.rename( columns={'Unnamed: 0':'SLNO'}, inplace=True )
    df.rename( columns={'Unnamed: 2':'Description'}, inplace=True )
    df.rename( columns={'Unnamed: 3':'Unit'}, inplace=True )
    # df.drop([0,1], inplace=True)
    # df.drop([0], inplace=True)
    df.drop(columns=['Apr', 'May','Jun'],inplace=True)
    col_len =len(df.columns)
    dfr = df.iloc[85:88] # number of rows 
    # dfr = dfr.iloc[:, 2:col_len] # set columns dfc.columns[[5,len(dfc.columns)-1]]
    dfr.columns = [str(col)[:10] + '' for col in dfr.columns]
    dfc = dfr.iloc[:, 4:col_len] 
    dfr.to_excel("Actual Export.xlsx")
    dfr = pd.melt(dfr, id_vars=['Descriptio','Unit'], value_vars=dfc.columns)
    # dfr.dropna(inplace=True)
    dfr.to_sql(name="actual_export", con=engine, if_exists = 'replace', index=False)

    # print(dfr[:20])

    df = pd.read_excel('~/frappe-bench/sites/site1.local/private/files/ICS-CF-Form 04.04.21.xlsx', index_col=None, header=0, skiprows=98, sheet_name='Bank Balance & Others')
    # df.rename( columns={'Unnamed: 0':'SLNO'}, inplace=True )
    df.rename( columns={'Unnamed: 2':'Description'}, inplace=True )
    df.rename( columns={'Unnamed: 3':'Unit'}, inplace=True )
    # df.drop([0,1], inplace=True)
    df.drop([0], inplace=True)
    # df.drop(columns=['Apr', 'May','Jun'],inplace=True)
    col_len =len(df.columns)
    dfr = df.iloc[:18] # number of rows 
    # dfr = dfr.iloc[:, 2:col_len] # set columns dfc.columns[[5,len(dfc.columns)-1]]
    dfr.columns = [str(col)[:10] + '' for col in dfr.columns]
    dfc = dfr.iloc[:, 4:col_len] 
    dfr.to_excel("Cash Inflow from Other Sources.xlsx")
    dfr = pd.melt(dfr, id_vars=['Descriptio','Unit'], value_vars=dfc.columns)
    # dfr.dropna(inplace=True)
    dfr.to_sql(name="cash_inflow_from_other_sources", con=engine, if_exists = 'replace', index=False)

    # print(dfr)


def run_sql(sql):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)

def check_is_modi():
    with connection:
        with connection.cursor() as cursor:
            sql = "SELECT  `expected_fund_realisation_date_revised_1` as ex1, `expected_fund_realisation_date_revised_2` as ex2, `expected_fund_realisation_date_revised_3` as ex3   FROM `tabBills Under Collection` where `expected_fund_realisation_date_revised_1` <> '' or `expected_fund_realisation_date_revised_2` <> '' or `expected_fund_realisation_date_revised_3` <> ''"
            cursor.execute(sql)
            result = cursor.fetchall()

    for r in result:
        if r["ex3"] is not None:
            date_time_obj = datetime.datetime.strptime(r["ex3"], '%Y-%m-%d %H:%M:%S')
            weekend = get_weekend(date_time_obj)
            days = datetime.timedelta(7)
            weeks = weekend - days
            run_sql("select * from `tabcashflow_report`  where ex_date between '"+weeks.strftime('%Y-%m-%d %H:%M:%S')+"' and '"+weekend.strftime('%Y-%m-%d %H:%M:%S')+"'  and line_name = 'USD Sales from build up account'")
            print(weekend.strftime('%Y-%m-%d %H:%M:%S') )
        elif r["ex2"] is not None:
            date_time_obj = datetime.datetime.strptime(r["ex2"], '%Y-%m-%d %H:%M:%S')
            weekend = get_weekend(date_time_obj)
            days = datetime.timedelta(7)
            weeks = weekend - days
            run_sql("select * from `tabcashflow_report`  where ex_date between '"+weeks.strftime('%Y-%m-%d %H:%M:%S')+"' and '"+weekend.strftime('%Y-%m-%d %H:%M:%S')+"'  and line_name = 'USD Sales from build up account'")
            print(weekend)
        elif r["ex1"] is not None:
            date_time_obj = datetime.datetime.strptime(r["ex1"], '%Y-%m-%d %H:%M:%S')
            weeks = get_weekend(date_time_obj)
            days = datetime.timedelta(7)
            weekend = weekend - days
            run_sql("select * from `tabcashflow_report`  where ex_date between '"+weeks.strftime('%Y-%m-%d %H:%M:%S')+"' and '"+weekend.strftime('%Y-%m-%d %H:%M:%S')+"'  and line_name = 'USD Sales from build up account'")
            print(weekend)
        
    
    # df = pd.DataFrame.from_records(result)
    # if not df["ex3"].empty:
    #     df["ex3"] = pd.to_datetime(df["ex3"])
    #     print(df["ex3"])
    #     df["weekend"] = df["ex3"].apply(lambda x:  get_weekend(x))




def valid_ship_forecast():
    with connection:
        with connection.cursor() as cursor:
            sql = "SELECT if(sum(order_qty) < 8400000, 'Check File','') as chkfile FROM v_shipment_forecast"
            cursor.execute(sql)
            result = cursor.fetchall()
    
    # Monthly QTY
    # check by FOB - if lowar then 4.25 show allert (waited average)  AKL 
    # BUC check date 


    for r in result:
        print(r["chkfile"])



def valid_ship_forecast():
    with connection:
        with connection.cursor() as cursor:
            sql = "SELECT if(sum(order_qty) < 8400000, 'Check File','') as chkfile FROM v_shipment_forecast"
            cursor.execute(sql)
            result = cursor.fetchall()
    # Ex Re Date 
    # Revice Date 
    # Confirmation -- Bank notify + 7 
    # Weekly Varience Report - 
    # Fund Flow (summerize file)


    for r in result:
        print(r["chkfile"])


def budget_exp_test():
    df = pd.read_excel('~/frappe-bench/sites/site1.local/private/files/ICS-CF-Form 04.04.21.xlsx', index_col=None, header=0,skiprows=108, sheet_name='Proj. Expenses income')
    df.drop(columns=['Apr', 'May','Jun'],inplace=True)
    df.rename( columns={'Unnamed: 2':'Description'}, inplace=True )
    df.rename( columns={'Unnamed: 3':'Unit'}, inplace=True )
    # df.drop([0,1], inplace=True)
    df.drop([0], inplace=True)
    col_len =len(df.columns)    
    dfr = df.iloc[0:19] # number of rows 
    dfr = dfr.iloc[:, 2:col_len] # set columns dfc.columns[[5,len(dfc.columns)-1]]
    dfr.columns = [str(col)[:10] + '' for col in dfr.columns]
    dfc = dfr.iloc[:, 3:col_len] 
    print(dfc.columns)
    dfr.to_excel("Budget_exp.xlsx")
    # dfr = pd.melt(dfr, id_vars=['Descriptio'], value_vars='2021-03-06')
    dfr = pd.melt(dfr, id_vars=['Descriptio','Unit'], value_vars=dfc.columns)
    # dfr.dropna(inplace=True)
    print(dfr)
    # dfr.to_sql(name="budget_exp", con=engine, if_exists = 'replace', index=False)

def test():
    df = pd.DataFrame({'A': {0: 'a', 1: 'b', 2: 'c'},
                   'B': {0: 1, 1: 3, 2: 5},
                   'C': {0: 2, 1: 4, 2: 6}})
    df = pd.melt(df, id_vars=['A'], value_vars=['B'])
    print(df)
   
    # dfr.to_excel("Expense Summary CD SOD.xlsx")


def fund_flow():
    dfsf = pd.read_sql('a_shipment_forecast_sum', con=engine)
    dfsf = dfsf.pivot(index='variable', columns='Descriptio', values='value')

    dfbuc = pd.read_sql('a_buc_forecast_sum', con=engine)
    dfbuc = dfbuc.pivot(index='variable', columns='Descriptio', values='value')

    result = pd.merge(dfsf, dfbuc, on="variable", how="outer")
    result.to_excel("fund_flow.xlsx")
    print(result)

fund_flow()
# if valid_ship_forecast() == "":
# process_shipment_forecast()
# get_shipment_forecast()
# get_buc_forecast()
# get_buc_actual()
# # budget_exp_test()
# ProjExpensesIncome()
# bank_balance()
# ActExpensesIncome()
# check_is_modi()
