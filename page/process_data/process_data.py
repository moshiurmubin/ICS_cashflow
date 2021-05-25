	# Copyright (c) 2017, Frappe Technologies Pvt. Ltd. and Contributors
# MIT License. See license.txt

from __future__ import print_function, unicode_literals

import datetime
import math
import time
from datetime import date, timedelta

import cashflowpro.cashflowpro.page.process_data.read_cf
import frappe
from frappe import _, scrub
from frappe.utils import flt, nowdate
from frappe.model.document import Document
import pandas as pd
import pymysql
from frappe import enqueue
from pymysql.times import TimeDelta
from sqlalchemy import create_engine
import time


engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root",pw="frappe",db="_1bd3e0294da19198"))



usd_rate = 0







def run_sql(sql):
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='frappe',
                                 db='_1bd3e0294da19198',
                                 cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
    # connection.commit()
    finally:
        connection.close()


def truncate_table():
    engine.execute("truncate `tabcashflow_report`")


def get_line_sum_by_date(rep_dat1, rep_dat2, _line_name, tab_name):
    # print(_line_name, tab_name)
    # print(_line_name, tab_name)
    # if _line_name == 'Staff salaries':
    filters = {'line_name': _line_name, 'start_date': rep_dat1.strftime('%Y-%m-%d') , 'end_date': rep_dat2.strftime('%Y-%m-%d') }
    # print("""SELECT sum(value) as amount FROM `"""+tab_name+"""` where `Descriptio` = '"""+_line_name+"""' and (`variable` between '"""+rep_dat1.strftime('%Y-%m-%d')+"""' and '"""+rep_dat2.strftime('%Y-%m-%d')+"""' )""")
    amount =frappe.db.sql("""SELECT sum(amount) as amount FROM `"""+tab_name+"""` where TRIM(`Descriptio`) = '"""+_line_name+"""' and (`variable` between '"""+rep_dat1.strftime('%Y-%m-%d')+"""' and '"""+rep_dat2.strftime('%Y-%m-%d')+"""' )""", as_dict=0)
    if amount[0][0] is not None:
        return amount[0][0]
    else:
        return 0

def get_line_summary_account(rep_dat1, rep_dat2, _line_name, tab_name):
    filters = {'line_name': _line_name, 'start_date': rep_dat1.strftime('%Y-%m-%d') , 'end_date': rep_dat2.strftime('%Y-%m-%d') }
    # print("""SELECT sum(value) as amount FROM `"""+tab_name+"""` where `Descriptio` = '"""+_line_name+"""' and (`variable` between '"""+rep_dat1.strftime('%Y-%m-%d')+"""' and '"""+rep_dat2.strftime('%Y-%m-%d')+"""' )""")
    amount =frappe.db.sql("""SELECT sum(amount) as amount FROM `"""+tab_name+"""` where TRIM(`account`) = '"""+_line_name+"""' and (`variable` between '"""+rep_dat1.strftime('%Y-%m-%d')+"""' and '"""+rep_dat2.strftime('%Y-%m-%d')+"""' )""", as_dict=0)
    if amount[0][0] is not None:
        return amount[0][0]
    else:
        return 0  

def get_line_summary(rep_dat1, rep_dat2, _line_name, tab_name):
    filters = {'line_name': _line_name, 'start_date': rep_dat1.strftime('%Y-%m-%d') , 'end_date': rep_dat2.strftime('%Y-%m-%d') }
    # print("""SELECT sum(value) as amount FROM `"""+tab_name+"""` where `Descriptio` = '"""+_line_name+"""' and (`variable` between '"""+rep_dat1.strftime('%Y-%m-%d')+"""' and '"""+rep_dat2.strftime('%Y-%m-%d')+"""' )""")
    amount =frappe.db.sql("""SELECT sum(value) as amount FROM `"""+tab_name+"""` where TRIM(`Descriptio`) = '"""+_line_name+"""' and (`variable` between '"""+rep_dat1.strftime('%Y-%m-%d')+"""' and '"""+rep_dat2.strftime('%Y-%m-%d')+"""' )""", as_dict=0)
    if amount[0][0] is not None:
        return amount[0][0]
    else:
        return 0


    # # print(_line_name, tab_name)
    # filters = {'line_name': _line_name, 'start_date': rep_dat1.strftime('%Y-%m-%d') , 'end_date': rep_dat2.strftime('%Y-%m-%d') }
    # # print(("""SELECT sum(value) as amount FROM """+tab_name+""" where `Descriptio` = %(line_name)s and (`variable` between %(start_date)s and %(end_date)s )""", filters))
    # amount =frappe.db.sql("""SELECT sum(value) as amount FROM """+tab_name+""" where `Descriptio` = TRIM(%(line_name)s) and (`variable` between %(start_date)s and %(end_date)s )""", filters, as_dict=0)
    # if amount[0][0] is not None:
    #     return amount[0][0]
    # else:
    #     return 0


@frappe.whitelist()
def make_data_g1():
    usd_rate = float(frappe.db.get_single_value('Cashflow Settings','usd_rate'))
    truncate_table()
    dfrl = pd.read_sql('tabReport Lines', con=engine)
    dfrc = pd.read_sql('tabCF Columns', con=engine)
    i = 0
    for index, row in dfrl.iterrows():
        _line_name = row['line_name']
        _group_no = row['group_no']
        rep_line = row['name']
        for index, row in dfrc.iterrows():
            rep_col = row['name']
            col_idx = row['col_no']
            _month = row['month']
            rep_dat1 = row['date1']
            rep_dat2 = row['date2']
            _amount = 0
            # print(row['name'], row['date2']) 
            ####### BDT Inflow #########
            if row['col_status'] == 'Projected':
                if _line_name == 'Transfer in from ERQ/Margin/Buildup': 
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name, 'v_Income_Forecast')
                if _line_name == 'USD Sales from build up account': 
                    _amount = (usd_rate * get_line_summary(rep_dat1, rep_dat2, 'Sales Proceeds - Build Up A/C (Regular)', 'final_proj_income')) / 1000
                if _line_name == 'Cash Incentive':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name, 'v_Income_Forecast')
                if _line_name == 'Other Inflow':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name, 'v_Income_Forecast')
                if _line_name == 'Received in L/O Account Bank':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name, 'v_Income_Forecast')
                if _line_name == 'Sub-Contract Income':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name, 'v_Income_Forecast')
                if _line_name == 'Received in CD A/C as STL/LTL':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name, 'v_Income_Forecast')
                if _line_name == 'Received in CD as OD':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name, 'v_Income_Forecast')
                if _line_name == 'Received in CD A/C as PSF/PC Loan':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name, 'v_Income_Forecast')
                if _line_name == 'Received from AKL (Inter Co.)':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name, 'v_Income_Forecast')
                if _line_name == 'Received from AKL (STL)':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name, 'v_Income_Forecast')
                if _line_name == 'Received in Mother Account':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name, 'v_Income_Forecast')
                if _line_name == 'Received in Welfare Account':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name, 'v_Income_Forecast')
                if _line_name == 'Received in Salary Account':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name, 'v_Income_Forecast')
                if _line_name == 'Received in Zakat Account':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name, 'v_Income_Forecast')
                if _line_name == 'Increased OD Account':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name, 'v_Income_Forecast')
                if _line_name == 'Bank Charge/Interest Expenses (OD A/C)':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name, 'v_Income_Forecast')
                if _line_name == 'Received in L/O Account Cash':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name, 'v_Income_Forecast')
            ####### BDT Outflow #########
                if _line_name == 'Direct/Indirect Wages':
                    _amount = get_line_summary_account(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Staff salaries':
                    _amount = get_line_summary_account(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'EOT':
                    _amount = get_line_summary_account(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Advance Salaries/ Wages':
                    _amount = get_line_summary_account(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Bonus':
                    _amount = get_line_summary_account(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Air Freight':
                    _amount = get_line_summary_account(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Sub Con/Reprocessing payment':
                    _amount = get_line_summary_account(rep_dat1, rep_dat2, 'Sub Con Payment' , 'v_Expenses_Forecast')
                if _line_name == 'Capital Expenditure':
                    _amount = get_line_summary_account(rep_dat1, rep_dat2, 'Capex' , 'v_Expenses_Forecast')
                if _line_name == 'Bank Charge (Mother A/C)':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, 'Bank Charge(Commercial)' , 'v_Expenses_Forecast')
                if _line_name == 'Finance/Interest Expenses (Mother A/C)':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Other Expenses':
                    _amount = get_line_summary_account(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Payment to AKL (Inter Co.)':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Payment to AKL (STL)':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Lock Investment from CD':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Loan Settlement (STL/LTL)':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Loan Settlement (QSF Loan)':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Loan Settlement (PSF / PC)':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Settlement of O/D loan from CD':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Payment from Salary Account':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Payment from Welfare Account':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Payment from Zakat Account':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Payment from L/O Bank Account':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Payment from L/O Cash Account':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Transfer out as contra Salary A/c from Mother A/C':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Decreased OD Account':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')

            ######### USD inflow ###########
                if _line_name == 'Sales Proceeds - Margin A/C':
                    _amount = get_line_summary(rep_dat1, rep_dat2, _line_name , 'final_proj_income')
                if _line_name == 'Sales Proceeds - Build up (Unifil payment)':
                    _amount = get_line_summary(rep_dat1, rep_dat2, _line_name , 'final_proj_income')
                if _line_name == 'Sales Proceeds - ERQ A/C':
                    _amount = get_line_summary(rep_dat1, rep_dat2, _line_name , 'final_proj_income')
                if _line_name == 'Sales Proceeds - Build Up A/C (Machinery payment)':
                    _amount = get_line_summary(rep_dat1, rep_dat2, _line_name , 'final_proj_income')           
                if _line_name == 'Sales Proceeds - Build Up A/C (Regular)':
                    _amount = get_line_summary(rep_dat1, rep_dat2, _line_name , 'final_proj_income')
                if _line_name == 'Other Inflow - Margin A/C':
                    _amount = get_line_summary(rep_dat1, rep_dat2, _line_name , 'final_proj_income')
                if _line_name == 'Transfer in ERQ A/C from build up/others':
                    _amount = get_line_summary(rep_dat1, rep_dat2, _line_name , 'final_proj_income')

           ######### USD out flow ###########
           
                if _line_name == 'LC Payment from margin A/C':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Transfer to CD for extra event from Margin A/C':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Transfer to CD for extra event from build up A/C':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'LC Payment for Unifill from build up A/C':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Early Settlement(EDF-LC) from margin A/C':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'TT Payment from ERQ A/C':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Foreign Bank Charges from sales proceed':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Source Tax & CRF':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'USD encashment for CD from build up A/C':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'LC Payment for Machinery from build up A/C ':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')
                if _line_name == 'Transfer out from build up/others to ERQ':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Forecast')

            if row['col_status'] == 'Actual':
                if _line_name == 'Shipped Qty':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, 'Shipped Qty', 'v_Shipments_qty')
                if _line_name == 'Shipped FOB':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, 'Shipped FOB', 'v_Shipments_fob')
                    
                ####### BDT Inflow #########
                if _line_name == 'Transfer in from ERQ/Margin/Buildup':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')
                if _line_name == 'USD Sales from build up account':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')
                if _line_name == 'Cash Incentive':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')
                if _line_name == 'Other Inflow':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')
                if _line_name == 'Sub-Contract Income':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')
                if _line_name == 'Received in CD A/C as STL/LTL':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')
                if _line_name == 'Received in CD as OD':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')
                if _line_name == 'Received in CD A/C as PSF/PC Loan':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')
                if _line_name == 'Received from AKL (Inter Co.)':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')
                if _line_name == 'Received from AKL (STL)':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')
                if _line_name == 'Received in Mother Account':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')
                if _line_name == 'Received in Welfare Account':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')
                if _line_name == 'Received in Salary Account':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')
                if _line_name == 'Received in  Zakat Account':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')
                if _line_name == 'Increased OD Account':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')
                if _line_name == 'Bank Charge/Interest Expenses (OD A/C)':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')
                if _line_name == 'Received in L/O Account Cash':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')
            
            
            
            ####### BDT Outflow #########
                if _line_name == 'Direct/Indirect Wages':
                    _amount = get_line_summary_account(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Staff salaries':
                    _amount = get_line_summary_account(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'EOT':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Advance Salaries/ Wages':
                    _amount = get_line_summary_account(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Bonus':
                    _amount = get_line_summary_account(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Air Freight':
                    _amount = get_line_summary_account(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Sub Con/Reprocessing payment':
                    _amount = get_line_summary_account(rep_dat1, rep_dat2, 'Sub Con Payment' , 'v_Expenses_Actual')
                if _line_name == 'Capital Expenditure':
                    _amount = get_line_summary_account(rep_dat1, rep_dat2, 'Capex' , 'v_Expenses_Actual')
                if _line_name == 'Bank Charge (Mother A/C)':
                    _amount = get_line_summary_account(rep_dat1, rep_dat2, 'Bank Charge(Commercial)' , 'v_Expenses_Actual')
                if _line_name == 'Finance/Interest Expenses (Mother A/C)':
                    _amount = get_line_summary_account(rep_dat1, rep_dat2, 'Bank Interest' , 'v_Expenses_Actual')
                if _line_name == 'Other Expenses':
                    _amount = get_line_summary_account(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Payment to AKL (Inter Co.)':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Payment to AKL (STL)':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Lock Investment from CD':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Loan Settlement (STL/LTL)':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Loan Settlement (QSF Loan)':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Loan Settlement (PSF / PC)':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Settlement of O/D loan from CD':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Payment from Salary Account':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Payment from Welfare Account':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Payment from Zakat Account':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Payment from L/O Bank Account':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Payment from L/O Cash Account':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Transfer out as contra Salary A/c from Mother A/C':
                    _amount = get_line_summary_account(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Decreased OD Account':
                    _amount = get_line_summary_account(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
            
            
                ######### USD inflow ###########
                if _line_name == 'Sales Proceeds - Margin A/C':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')
                if _line_name == 'Sales Proceeds - Build up (Unifil payment)':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')
                if _line_name == 'Sales Proceeds - ERQ A/C':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')
                if _line_name == 'Sales Proceeds - Build Up A/C (Machinery payment)':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')           
                if _line_name == 'Sales Proceeds - Build Up A/C (Regular)':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')
                if _line_name == 'Other Inflow - Margin A/C':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')
                if _line_name == 'Transfer in ERQ A/C from build up/others':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Income_Actual')
                
                ######### USD out flow ###########
                if _line_name == 'LC Payment from margin A/C':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Transfer to CD for extra event from Margin A/C':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Transfer to CD for extra event from build up A/C':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'LC Payment for Unifill from build up A/C':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Early Settlement(EDF-LC) from margin A/C':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'TT Payment from ERQ A/C':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Foreign Bank Charges from sales proceed':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Source Tax & CRF':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'USD encashment for CD from build up A/C':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'LC Payment for Machinery from build up A/C ':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
                if _line_name == 'Transfer out from build up/others to ERQ':
                    _amount = get_line_sum_by_date(rep_dat1, rep_dat2, _line_name , 'v_Expenses_Actual')
            
            
            
            ########### cash out flow #####################

            # print(rep_dat1, rep_dat2)
            # if _line_name == 'Total inflow - Export':
            #     _amount = get_line_total_by_line(rep_dat1, rep_dat2, '4')
            # print(_amount)
            # # if _line_name == 'Total inflow - Bank Loan':
            #     _amount = 0  #get_line_sum_by_date(rep_dat1, rep_dat2, 5)
            # if _line_name == 'Total inflow - Others':
            #     _amount = get_line_total_by_line(rep_dat1, rep_dat2, '5')
            # if _line_name == 'Total inflow - Contra':
            #     _amount = get_line_total_by_line(rep_dat1, rep_dat2, '6')
            # print("aaaaaaaaaa")
            # print(_amount)             
            
            if (_amount is not None) and (_amount > 0):
                data = frappe.get_doc({"doctype":"cashflow_report"})
                data.report_line = rep_line
                data.group_no = _group_no
                data.report_col = rep_col
                data.col_index = col_idx
                data.ex_date = rep_dat2
                data.month = _month
                data.line_name = _line_name
                data.amount = round(_amount,3)
                # print(_amount)   
                data.insert()
                frappe.db.commit()
    return 0





def get_total_by_lineid(rep_dat1, rep_dat2, rept_col ,sList):
    sql = """select sum(amount) as amount from tabcashflow_report where report_col ='"""+ rept_col +"""' and (`ex_date` between '"""+rep_dat1.strftime('%Y-%m-%d, %H:%M:%S')+"""' and '"""+rep_dat2.strftime('%Y-%m-%d, %H:%M:%S')+"""') and report_line in (""" + sList + """)"""
    print(sql)
    amount = frappe.db.sql(sql, as_dict=0)
    frappe.db.commit()
    if amount[0][0] is not None:
        return amount[0][0]
    else:
        return 0


def get_opening_by_lineid(rep_dat1, rep_dat2, rept_col ,sList):
    sql = """select sum(amount) as amount from tabcashflow_report where  col_index = """+ str(rept_col -2) +""" and (`ex_date` between '"""+rep_dat1.strftime('%Y-%m-%d, %H:%M:%S')+"""' and '"""+rep_dat2.strftime('%Y-%m-%d, %H:%M:%S')+"""') and report_line in (""" + sList + """)"""
    print(sql)
    amount = frappe.db.sql(sql, as_dict=0)
    frappe.db.commit()
    if amount[0][0] is not None:
        return amount[0][0]
    else:
        return 0

        
    


# get_total_by_lineid('2021-04-01','2021-04-01', "'CFRL0229','CFRL0229','CFRL0229','CFRL0229'")




def get_line_total_by_line(rep_dat1, rep_dat2, _param, _line_name, _col_index):
    # print(rep_dat1, rep_dat2, _param)
    filters = {'group_no': _param, 'start_date': rep_dat1.strftime('%Y-%m-%d') , 'end_date': rep_dat2.strftime('%Y-%m-%d') }
    # print(("""SELECT sum(amount) as amount FROM tabcashflow_report where  `group_no` = %(group_no)s and (`ex_date` between %(start_date)s and %(end_date)s )"""), filters)
    frappe.db.sql("""DELETE FROM tabcashflow_report where line_name = '"""+_line_name+"""' and col_index = '"""+ str(_col_index)+"""' """)
    frappe.db.commit()
    # print("""SELECT sum(amount) as amount FROM tabcashflow_report where  `group_no` = '"""+_param+"""' and col_index = '"""+ str(_col_index)+"""' and (`ex_date` between '"""+rep_dat1.strftime('%Y-%m-%d, %H:%M:%S')+"""' and '"""+rep_dat2.strftime('%Y-%m-%d, %H:%M:%S')+"""' )""")
    amount =frappe.db.sql("""SELECT sum(amount) as amount FROM tabcashflow_report where  `group_no` = '"""+_param+"""' and col_index = '"""+ str(_col_index)+"""' and (`ex_date` between '"""+rep_dat1.strftime('%Y-%m-%d, %H:%M:%S')+"""' and '"""+rep_dat2.strftime('%Y-%m-%d, %H:%M:%S')+"""' )""", as_dict=0)
    frappe.db.commit()
    if amount is not None:
        return amount[0][0]
        # print(amount[0][0])

@frappe.whitelist()
def make_data_gtotal():
    # truncate_table()
    dfrl = pd.read_sql('tabReport Lines', con=engine)
    dfrc = pd.read_sql('tabCF Columns', con=engine)
    for index, row in dfrl.iterrows():
        _line_name = row['line_name']
        _group_no = row['group_no']
        rep_line = row['name']
        for index, row in dfrc.iterrows():
            rep_col = row['name']
            col_idx = row['col_no']
            _month = row['month']
            rep_dat1 = row['date1']
            rep_dat2 = row['date2']
            

            _amount = 0
#Cash Inflow Total BDT
            if _line_name == 'Total inflow - Export':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0129'")

            if _line_name == 'Total inflow - Bank Loan':
                 _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CCFRL0134','CFRL0135','CFRL0136'")

            if _line_name == 'Total inflow - Others':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CCFRL0128','CFRL0130','CFRL0131','CFRL0132','CFRL0133','CFRL0137','CFRL0138','CFRL0145'")
            if _line_name == 'Total inflow - Contra':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CCFRL0139','CFRL0140','CFRL0141','CFRL0142','CFRL0143'")


            if _line_name == 'Total Opening Balance':
                 _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CCFRL0120','CCFRL0121','CCFRL0122','CCFRL0123','CCFRL0124','CCFRL0125','CCFRL0126'")

#Cash Out flow                             
            if _line_name == 'Total Out-flow - Operation':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0168','CFRL0169','CFRL0170','CFRL0171','CFRL0172','CFRL0160','CFRL0161','CFRL0158'")
                

            if _line_name == 'Total Out-flow - Salary & wages':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0153','CFRL0154','CFRL0150','CFRL0151'")
            if _line_name == 'Total Out-flow - Loan':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0164','CFRL0165','CFRL0166','CFRL0167'")
            if _line_name == 'Total Out-flow - Investment':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0163','CFRL0157'")
            if _line_name == 'Total Out-flow - bank Interest':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0159'")
            if _line_name == 'Total Out-flow - EOT':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0152'")
            if _line_name == 'Total Out-flow - Air Freight':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0155'")
            if _line_name == 'Total Out-flow - Sub Con/Reprocessing charge':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0156'")
            if _line_name == 'Total cash outflow (Salary & Operation)':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0175','CFRL0176','CFRL0179'")
            if _line_name == 'Total Out-flow - Contra':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0174'")



            if _line_name == 'Total Closing Balance':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ," 'CCFRL0185','CCFRL0186','CCFRL0187','CCFRL0188','CCFRL0189','CCFRL0190','CCFRL0191'")


            if _line_name == 'Total Opening Balance':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ," 'CCFRL0193','CCFRL0194','CCFRL0195','CCFRL0196','CCFRL0197'")


            if _line_name == 'Total In-flow':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ," 'CCFRL0199','CCFRL0200','CCFRL0201','CCFRL0202','CCFRL0203','CCFRL0204','CCFRL0205'")

            if _line_name == 'Total In-flow - Contra':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CCFRL0205' ")



            if _line_name == 'Total out-flow from margin - LC / EDF':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CCFRL0208','CCFRL0209','CCFRL0212' ")

            if _line_name == 'Total out-flow from ERQ - TT':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CCFRL0213' ")

            if _line_name == 'Total out-flow from build up A/C':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CCFRL0209','CCFRL0210','CCFRL0216','CCFRL0217'")
            if _line_name == 'Total bank charges':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CCFRL0214' ")
            if _line_name == 'Total out-flow - Contra':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CCFRL0218' ")

#Closing Balance
            if _line_name == 'Closing Balance Left Over (Bank A/C)':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0132','CFRL0133','CFRL0120'") - get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0171'")
            if _line_name == 'Closing Balance Left Over (Cash A/C)':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0121','CFRL0145'") - get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0172'") 
            if _line_name == 'Closing Balance Welfare A/C':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0122','CFRL0140'") - get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0169'")
            if _line_name == 'Closing Balance Zakat Fund':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0123','CFRL0142'") -  get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0170'") 
            if _line_name == 'Closing Balance Salary A/C':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0124','CFRL0141'") -  get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0150','CFRL0152','CFRL0154','CFRL0168'") 
            if _line_name == 'Closing Balance Mother A/C':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0125','CFRL0128','CFRL0129','CFRL0130','CFRL0131','CFRL0134','CFRL0135','CFRL0136','CFRL0137','CFRL0138','CFRL0139'") - get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0151','CFRL0153','CFRL0155','CFRL0156','CFRL0157','CFRL0158','CFRL0159','CFRL0160','CFRL0161','CFRL0162','CFRL0163','CFRL0164','CFRL0165','CFRL0166','CFRL0167','CFRL0173'")
            if _line_name == 'Closing Balance OD':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0126'") -  get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0144','CFRL0174'")   


            if _line_name == 'Total Closing Balance':
                _amount = get_total_by_lineid(rep_dat1, rep_dat2, rep_col ,"'CFRL0224','CFRL0225','CFRL0226','CFRL0227','CFRL0228'")


                
            if (_amount is not None) and (_amount > 0):
                data = frappe.get_doc({"doctype":"cashflow_report"})
                data.report_line = rep_line
                # data.group_no = _group_no
                data.report_col = rep_col
                data.col_index = col_idx
                data.ex_date = rep_dat2
                data.month = _month
                data.line_name = _line_name
                data.amount = round(_amount)
                # print(_amount)   
                data.insert()
                # frappe.db.commit()

@frappe.whitelist()
def make_data_open_bal():
    # truncate_table()
    dfrl = pd.read_sql('tabReport Lines', con=engine)
    dfrc = pd.read_sql('tabCF Columns', con=engine)
    for index, row in dfrl.iterrows():
        _line_name = row['line_name']
        _group_no = row['group_no']
        rep_line = row['name']
        for index, row in dfrc.iterrows():
            rep_col = row['name']
            col_idx = row['col_no']
            _month = row['month']
            rep_dat1 = row['date1']
            rep_dat2 = row['date2']
            

            _amount = 0

# Opening Balance BDT 
            
            if _line_name == 'Opening Balance Left Over Bank A/C':
                _amount = get_opening_by_lineid(rep_dat1 - timedelta(days=7), rep_dat2 - timedelta(days=7), col_idx ,"'CFRL0185'")
            if _line_name == 'Opening Balance Left Over Cash A/C':
                _amount = get_opening_by_lineid(rep_dat1 - timedelta(days=7), rep_dat2 - timedelta(days=7), col_idx ,"'CFRL0186'")
            if _line_name == 'Opening Balance Welfare A/C':
                _amount = get_opening_by_lineid(rep_dat1 - timedelta(days=7), rep_dat2 - timedelta(days=7), col_idx ,"'CFRL0187'")
            if _line_name == 'Opening Balance Zakat Fund':
                _amount = get_opening_by_lineid(rep_dat1 - timedelta(days=7), rep_dat2 - timedelta(days=7), col_idx ,"'CFRL0188'")
            if _line_name == 'Opening Balance Salary A/C':
                _amount = get_opening_by_lineid(rep_dat1 - timedelta(days=7), rep_dat2 - timedelta(days=7), col_idx ,"'CFRL0189'")
            if _line_name == 'Opening Balance Mother Accounts':
                _amount = get_opening_by_lineid(rep_dat1 - timedelta(days=7), rep_dat2 - timedelta(days=7), col_idx ,"'CFRL0190'")
            if _line_name == 'Opening Balance OD Accounts':
                _amount = get_opening_by_lineid(rep_dat1 - timedelta(days=7), rep_dat2 - timedelta(days=7), col_idx ,"'CFRL0191'")
            
            if _line_name == 'Total Opening Balance':
                _amount = get_total_by_lineid(rep_dat1 - timedelta(days=7), rep_dat2 - timedelta(days=7), col_idx ,"'CFRL0192'")




                
            if (_amount is not None) and (_amount > 0):
                data = frappe.get_doc({"doctype":"cashflow_report"})
                data.report_line = rep_line
                # data.group_no = _group_no
                data.report_col = rep_col
                data.col_index = col_idx
                data.ex_date = rep_dat2
                data.month = _month
                data.line_name = _line_name
                data.amount = round(_amount)
                # print(_amount)   
                data.insert()
                # frappe.db.commit()


# make_data_gtotal()

 
def create_rep_data():
    make_data_g1()
    # make_data_gtotal()
    
    

def get_right_col():
	Task = frappe.db.get_list('Task')
	return Task


@frappe.whitelist()
def get_buyer():	
	connection = pymysql.connect(host='localhost',
							user='root',
							password='frappe',
							database='_1bd3e0294da19198',
							cursorclass=pymysql.cursors.DictCursor)
	with connection:
		with connection.cursor() as cursor:
			sql = "SELECT `bank`, `buyer_name` FROM `tabBuyers` WHERE `buyer_name`=%s"
			cursor.execute(sql, ('Target',))
			result = cursor.fetchone()
	return result

def valuation_formula(x):
    return x * 0.5

@frappe.whitelist()
def get_logged_user():
    create_rep_data()
    return frappe.session.user

def get_weekend(xdate):    
    print(xdate)
    return(100)
    # n = int(d.strftime("%d"))
    # m = int(d.strftime("%m"))
    # y = int(d.strftime("%Y"))
    # if ((n/7)<=1):
    #     return  datetime.datetime(y, m, 7) 
    # elif ((n/7)<=2):
    #     return  datetime.datetime(y, m, 7*2) 
    # elif ((n/7)<=3):
    #     return  datetime.datetime(y, m, 7*3) 
    # elif ((n/7)<=4):
    #     return  datetime.datetime(y, m, 7*4) 
    # elif ((n/7)<=5):
    #     return  datetime.datetime(y, m, 7*5) 
    
@frappe.whitelist()
def get_leaderboard_config():
	leaderboard_config = frappe._dict()
	leaderboard_hooks = frappe.get_hooks('leaderboards')
	for hook in leaderboard_hooks:
		leaderboard_config.update(frappe.get_attr(hook)())
	return leaderboard_config

def weeknum(dayname):
    if dayname == 'Monday':   return 0
    if dayname == 'Tuesday':  return 1
    if dayname == 'Wednesday':return 2
    if dayname == 'Thursday': return 3
    if dayname == 'Friday':   return 4
    if dayname == 'Saturday': return 5
    if dayname == 'Sunday':   return 6

def alldays(year, whichDayYouWant):
    d = date(year, 1, 1)
    d += timedelta(days = (weeknum(whichDayYouWant) - d.weekday()) % 7)
    while d.year == year:
        yield d
        d += timedelta(days = 7)

def weeknumber(year, month, day, weekstartsonthisday):
    specificdays = [d for d in alldays(year, weekstartsonthisday)]
    return len([specificday for specificday in specificdays if specificday <= date(year,month,day)])
