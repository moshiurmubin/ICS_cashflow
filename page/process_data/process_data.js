frappe.pages['Process data'].on_page_load = function(wrapper) {
	var page = frappe.ui.make_app_page({
		parent: wrapper,
		title: 'Process Data',
		single_column: true
	});
	// frappe.msgprint("Hello")
	this.page.set_title_sub('Subtitle');
	this.page.add_menu_item('Send Email', () => open_email_dialog())
	let $btn = this.page.set_primary_action('Show Report', () => create_new(), 'octicon octicon-plus')
	this.page.add_inner_button('New Post', () => new_post(), 'Make');
	let field = this.page.add_field({
		label: 'Status',
		fieldtype: 'Select',
		fieldname: 'status',
		options: [
			'Open',
			'Closed',
			'Cancelled'
		],
		change() {
			console.log(field.get_value());
		}
	});
	this.page.sidebar.html(`<ul class="module-sidebar-nav overlay-sidebar nav nav-pills nav-stacked">
	<li class="strong module-sidebar-item">Coffee</li>
	<li class="strong module-sidebar-item">Tea</li>
	<li class="strong module-sidebar-item">Milk</li>
	</ul>`);
	// this.page.main.html(`Hello`)
	
	frappe.call({
		method: "cashflowpro.cashflowpro.page.process_data.process_data.get_logged_user", //dotted path to server method
		callback: function(r) {
			// code snippet
			console.log(r);
		}
	});
}