# Copyright (c) 2013, ics and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
# import frappe

def execute(filters=None):
	columns, data = [], []

	# filters = {'company': 'Frappe Technologies Inc'}
	# data = frappe.db.sql("""
	# 	SELECT
	# 		acc.account_number
	# 		gl.debit
	# 		gl.credit
	# 	FROM `tabGL Entry` gl
	# 		LEFT JOIN `tabAccount` acc 
	# 		ON gl.account = acc.name
	# 	WHERE gl.company = %(company)s
	# """, filters=filters, as_dict=0)

	# data = frappe.db.sql("""
	# 	SELECT
	# 		acc.account_number
	# 		gl.debit
	# 		gl.credit
	# 	FROM `tabGL Entry` gl
	# 		LEFT JOIN `tabAccount` acc 
	# 		ON gl.account = acc.name
	# """,  as_dict=0)

	print('Hello')
	# frappe.log_error(frappe.get_traceback(), 'Query Failed')

	return  data
