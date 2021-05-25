// Copyright (c) 2021, ics and contributors
// For license information, please see license.txt

frappe.ui.form.on('Cashflow Import Files', {
	refresh: function(frm) {
		frm.add_custom_button('Start Import', () => 
			{ 
				if (frm.doc.file_type == 'Shipment forecast'){
					// msgprint("Shipment forecast")
					frappe.call({
						method: 'cashflowpro.cashflowpro.doctype.cashflow_import_files.cashflow_import_files.get_sum_qty_ship_fore',
						args: {
							'file_type': frm.doc.file_type,
							'file_name': frm.doc.file
						},
						callback: function(r) {
							if (!r.exc) {
								// code snippet
								frappe.confirm(
									'Total Order Quantity is:' +r.message.Total+'. '+r.message.MSG,
									function(){		
										frappe.call({
											method: "cashflowpro.cashflowpro.doctype.cashflow_import_files.cashflow_import_files.import_data", //dotted path to server method
											args: {
												'file_type': frm.doc.file_type,
												'file_name': frm.doc.file
											},
											callback: function(r) {
												msgprint(r.message.MSG)
											}
											// ,
											// error: (r) => {
											// 	// on error
											// 	// msgprint(r.message.MSG)
											// }
										})											
									},
									function(){
										msgprint('No data imported.')
									}
								)
								msgprint(r.message.Total)
							}
						}
					
					}) // end of frappe call 
				}

				else if (frm.doc.file_type == 'Bills Under Collection'){
					// msgprint("Shipment forecast")
					frappe.call({
						method: 'cashflowpro.cashflowpro.doctype.cashflow_import_files.cashflow_import_files.get_sum_qty_ship_fore',
						args: {
							'file_type': frm.doc.file_type,
							'file_name': frm.doc.file
						},
						callback: function(r) {
							if (!r.exc) {
								// code snippet
								frappe.confirm(
									'Total Order Quantity is:' +r.message.Total+'. '+r.message.MSG,
									function(){		
										frappe.call({
											method: "cashflowpro.cashflowpro.doctype.cashflow_import_files.cashflow_import_files.import_data", //dotted path to server method
											args: {
												'file_type': frm.doc.file_type,
												'file_name': frm.doc.file
											},
											callback: function(r) {
												msgprint(r.message.MSG)
											}
											// ,
											// error: (r) => {
											// 	// on error
											// 	// msgprint(r.message.MSG)
											// }
										})											
									},
									function(){
										msgprint('No data imported.')
									}
								)
								msgprint(r.message.Total)
							}
						}
					
					}) // end of frappe call 
				}


			}
		) // end of add button
	}
});
