frappe.pages['processdata'].on_page_load = function(wrapper) {
	var page = frappe.ui.make_app_page({
		parent: wrapper,
		title: 'Process Data',
		single_column: true
	});
}