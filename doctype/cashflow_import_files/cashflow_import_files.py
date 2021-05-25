# -*- coding: utf-8 -*-
# Copyright (c) 2021, ics and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
import pandas as pd
from frappe.model.document import Document


@frappe.whitelist()
def get_sum_qty_ship_fore(file_type, file_name):


	if file_type=="Shipment forecast":
		yearly_shipment_qty_akl = float(frappe.db.get_single_value('Cashflow Settings','yearly_shipment_qty_akl'))
		df = pd.read_excel('~/frappe-bench/sites/site1.local'+file_name, index_col=None, header=0, skiprows=0, sheet_name='Sheet1')
		df = df.fillna(0)
		Total = df['PO Qty'].sum()
		if Total < yearly_shipment_qty_akl:
			res = 0
			msg = "Shipment Tergate is not Fullfill, do you wnat to import?"
		else:
			res = 1
			msg = "OK, do you want to import?"
		print(Total)
		return {"Total": str(Total), "MSG": msg, "res": str(res) }
		

	if file_type=="Bills Under Collection":
		yearly_shipment_qty_akl = float(frappe.db.get_single_value('Cashflow Settings','yearly_shipment_qty_akl'))
		df = pd.read_excel('~/frappe-bench/sites/site1.local'+file_name, index_col=None, header=0, skiprows=0, sheet_name='Sheet1')
		df = df.fillna(0)
		Total = df['QTY-PCS'].sum()
		if Total < yearly_shipment_qty_akl:
			res = 0
			msg = "Shipment Tergate is not Fullfill, do you wnat to import?"
		else:
			res = 1
			msg = "OK, do you want to import?"
		print(Total)
		return {"Total": str(Total), "MSG": msg, "res": str(res) }
		





@frappe.whitelist()
def import_data(file_type, file_name):   
	if file_type == 'Shipment forecast':
		try:
			frappe.db.sql("""UPDATE `tabShipment Forecast`
			SET status = 'Inactive'  """)
			df = pd.read_excel('~/frappe-bench/sites/site1.local'+file_name, index_col=None, header=0, skiprows=0, sheet_name='Sheet1')
			df = df.fillna(0)
			for index, row in df.iterrows():
				data = frappe.get_doc({"doctype":"Shipment Forecast"})
				data.buyer = row['Buyer']
				data.style = row['Style']
				data.order_qty = row['PO Qty']
				data.fob_price = row['FOB/PC']
				data.team = row['Team']
				data.oc = row['OC']
				data.po_no = row['PO No']
				data.color = row['Color']
				data.po_qty = row['PO Qty']
				data.shipped_qty = row['Shipped Qty']
				data.exfty_date = row['Ex-Fty Date']
				data.insert()
		except:
			print("File not correct")
			return {'MSG': 'File is not correct, please check file and try again.'}
		finally:
			print("Import Done")
			return {'MSG': 'File Import Done'}

	if file_type == 'Bills Under Collection':
		try:
			frappe.db.sql("""UPDATE `tabBills Under Collection`
			SET status = 'Inactive'  """)
			df = pd.read_excel('~/frappe-bench/sites/site1.local'+file_name, index_col=None, header=0, skiprows=0, sheet_name='Sheet1')
			print(df)
			df = df.fillna(0)
			for index, row in df.iterrows():
				data = frappe.get_doc({"doctype":"Bills Under Collection"})
				data.buyer = row['Buyer']
				# print(row['Invoice'])
				data.style_no = row['STYLE NO']
				data.qty_pcs = row['QTY-PCS']
				print(row['UNIT PRICE'],row['QTY-PCS'] )
				# data.value_po_wise = row['FOB/PC']
				data.unit_price = row['UNIT PRICE']
				data.invoice= row['Invoice']
				# # data.team = row['Team']
				# data.oc = row['OC']
				data.po_no = row['PO NO']
				# # data.color = row['Color']
				# # data.po_qty = row['PO Qty']
				# data.shipped_qty = row['Shipped Qty']
				data.ex_fty_date = row['EX-FTY DATE']
				data.insert()
		except:
			print("File not correct")
			return {'MSG': 'File is not correct, please check file and try again.'}
		finally:
			print("Import Done")
			return {'MSG': 'BUC Import Done'}

		


class CashflowImportFiles(Document):
	pass
