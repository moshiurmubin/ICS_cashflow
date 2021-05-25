# Copyright (c) 2013, ics and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe import _
from erpnext.accounts.utils import get_balance_on

def execute(filters=None):
	filters = frappe._dict(filters or {})
	columns = get_columns(filters)
	data = get_data(filters)
	return columns, data

def get_columns(filters):
	columns = [
				{
			"label": _("Name"),
		},
		{
			"label": _("Buyer Name"),
		},
		{
			"label": _("Bank"),
		}
	]

	return columns

def get_conditions(filters):
	conditions = {}

	if filters.account_type:
		conditions["account_type"] = filters.account_type
		return conditions

	if filters.company:
		conditions["company"] = filters.company

	if filters.root_type:
		conditions["root_type"] = filters.root_type

	return conditions

def get_data(filters):
	data = []
	# conditions = get_conditions(filters)

	# buyer = frappe.db.get_all("Buyers", fields=["name","buyer_name"])
	emp = frappe.db.sql("""select name, buyer_name,bank from tabBuyers  """ , as_list=1)
	# buyer  = frappe.db.sql_list("select name, buyer_name from tabBuyers")
	# for d in emp:
	# 	# balance = get_balance_on(d.name, date=filters.report_date)
	# 	row = {"buyer_name": d.buyer_name}
	# 	data.append(row)
	return emp