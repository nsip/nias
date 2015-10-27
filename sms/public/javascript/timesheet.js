// timesheet.js


	var db1;
	var db2;
	var db3;
	var xbyx;
	var time_series;
	var workstream;

	var last_dashboard_request = {};
	var last_xbyx_request = {};
	var last_time_series_request = {};
	var last_workstream_request = {};


    d3.json( "/graph_data?include_workstreams=true", function (data) {
    	db1 = createDashboardGraph( 
    		dimple.newSvg("#chartContainer_customer", "100%", 350), data, 'Customer' );	
    	db2 = createDashboardGraph( 
    		dimple.newSvg("#chartContainer_wa", "100%", 350), data, 'Work Area' );	
    	db3 = createDashboardGraph( 
    		dimple.newSvg("#chartContainer_activity", "100%", 350), data, 'Activity' );	
    	xbyx = createXbyXGraph(
    		dimple.newSvg("#chartContainer_x_by_x", "100%", 400), data, ['Customer'], ['Activity','Work Area']);
    	time_series = createTimeSeriesGraph(
    		dimple.newSvg("#chartContainer_time_series", "100%", 400), data, ['Work Area','Activity']);
    	workstream = createWorkstreamGraph(
    		dimple.newSvg("#chartContainer_workstreams", "100%", 400), data, ['Work Area','Activity']);

    });
    
    function createDashboardGraph( svg, data, series_name ) {
	    var chart = new dimple.chart(svg, data);
	    chart.setBounds(0, 0, '100%', '100%')
	    chart.addMeasureAxis("p", "Time (Enter Decimal of 1 hour)");
	    var ring = chart.addSeries([series_name], dimple.plot.pie);
	    ring.innerRadius = "50%";
	    chart.draw();
	    return chart;
    }

    function createXbyXGraph( svg, data, category_name, series_names) {
	    var chart = new dimple.chart( svg, data );
	    chart.setBounds( 50, 20, '85%', '80%' )
	    var x = chart.addCategoryAxis( "x", category_name );
	    chart.addMeasureAxis( "y", "Time (Enter Decimal of 1 hour)" );  
	    chart.addSeries( series_names, dimple.plot.bar );
	    chart.draw();
	    return chart;
    }

    function createTimeSeriesGraph( svg, data, series_names ) {
	    var chart = new dimple.chart( svg, data );
	    chart.setBounds( 50, 20, '85%', '80%')
	    var x = chart.addCategoryAxis("x", ["Day (Enter Date)"]);
	    x.timeField = "Day (Enter Date)";
	    x.dateParseFormat = "%d/%m/%Y";
	    x.tickFormat = "%d %B";
	    x.addOrderRule("Day (Enter Date)");
	    chart.addMeasureAxis("y", "Time (Enter Decimal of 1 hour)");
	    var s = chart.addSeries( series_names, dimple.plot.bar );
	    s.interpolation = 'step'; //'cardinal';
	    chart.draw();
	    return chart;
    }

    function createWorkstreamGraph( svg, data, series_names) {
	    var chart = new dimple.chart( svg, data );
	    chart.setBounds( 50, 20, '85%', '80%' )
	    var x = chart.addCategoryAxis( "x", ["WS_WorkStream"] );
	    chart.addMeasureAxis( "y", "Time (Enter Decimal of 1 hour)" );
	    chart.addSeries( series_names, dimple.plot.bar );
	    chart.draw();
	    return chart;
    }



    function update_dashboard_graphs( ) {

    	var customers = [];    
	    $("#dashboard_form #customer_select :selected").each(function(){
	        customers.push($(this).val()); 
	    });

	    var people =[];
	    $("#dashboard_form #people_select :selected").each(function(){
	        people.push($(this).val()); 
	    });

	    var activities =[];
	    $("#dashboard_form #activities_select :selected").each(function(){
	        activities.push($(this).val()); 
	    });

	    var work_areas =[];
	    $("#dashboard_form #work_areas_select :selected").each(function(){
	        work_areas.push($(this).val()); 
	    });

	    var months = [];
	    $("#dashboard_form #date_select :selected").each(function(){
	        months.push( $(this).val() );
	    });

	    var ytd = $("#dashboard_form #ytd").is( ':checked' );


	    // console.log( activities );
	    // console.log( work_areas );
	    // console.log( people );
	    // console.log( customers );
	    // console.log( months );
	    // console.log( ytd );

	    var request = {
	    	date_options: { months: months, ytd: ytd },
	    	activities: activities,
	    	work_areas: work_areas,
	    	people: people,
	    	customers: customers
	    };

	    var start_date = $("#dashboard_form #start_date").val();
		if (start_date.length == 10) {
		    request.date_options.start_date = start_date;
		}

	    var end_date = $("#dashboard_form #end_date").val();
		if (end_date.length == 10) {
		    request.date_options.end_date = end_date;
		}


	    last_dashboard_request = request;

	    // console.log( $.param( request ) )


	    $.getJSON( "/graph_data", request, function( data ) {

	    	// console.log( data );

	    	db1.data = data;
	    	db1.draw();

	    	db2.data = data;
	    	db2.draw();

	    	db3.data = data;
	    	db3.draw();

	    });

    }


    function update_xbyx_graphs( ) {

    	var customers = [];    
	    $("#xbyx_form #customer_select :selected").each(function(){
	        customers.push($(this).val()); 
	    });

	    var people =[];
	    $("#xbyx_form #people_select :selected").each(function(){
	        people.push($(this).val()); 
	    });

	    var activities =[];
	    $("#xbyx_form #activities_select :selected").each(function(){
	        activities.push($(this).val()); 
	    });

	    var work_areas =[];
	    $("#xbyx_form #work_areas_select :selected").each(function(){
	        work_areas.push($(this).val()); 
	    });

	    var months = [];
	    $("#xbyx_form #date_select :selected").each(function(){
	        months.push( $(this).val() );
	    });

	    var ytd = $("#xbyx_form #ytd").is( ':checked' );


	    // console.log( activities );
	    // console.log( work_areas );
	    // console.log( people );
	    // console.log( customers );
	    // console.log( months );
	    // console.log( ytd );

	    var request = {
	    	date_options: { months: months, ytd: ytd },
	    	activities: activities,
	    	work_areas: work_areas,
	    	people: people,
	    	customers: customers
	    };

	    var start_date = $("#xbyx_form #start_date").val();
		if (start_date.length == 10) {
		    request.date_options.start_date = start_date;
		}

	    var end_date = $("#xbyx_form #end_date").val();
		if (end_date.length == 10) {
		    request.date_options.end_date = end_date;
		}


	    last_xbyx_request = request;

	    // console.log( $.param( request ) )

	    $.getJSON( "/graph_data", request, function( data ) {

	    	// console.log( data );

	    	xbyx.data = data;
	    	xbyx.draw();

	    });

    }

    function update_time_graphs( ) {

    	var customers = [];    
	    $("#time_form #customer_select :selected").each(function(){
	        customers.push($(this).val()); 
	    });

	    var people =[];
	    $("#time_form #people_select :selected").each(function(){
	        people.push($(this).val()); 
	    });

	    var activities =[];
	    $("#time_form #activities_select :selected").each(function(){
	        activities.push($(this).val()); 
	    });

	    var work_areas =[];
	    $("#time_form #work_areas_select :selected").each(function(){
	        work_areas.push($(this).val()); 
	    });

	    var months = [];
	    $("#time_form #date_select :selected").each(function(){
	        months.push( $(this).val() );
	    });

	    var ytd = $("#time_form #ytd").is( ':checked' );

	    // console.log( activities );
	    // console.log( work_areas );
	    // console.log( people );
	    // console.log( customers );
	    // console.log( months );
	    // console.log( ytd );

	    var request = {
	    	date_options: { months: months, ytd: ytd },
	    	activities: activities,
	    	work_areas: work_areas,
	    	people: people,
	    	customers: customers
	    };

	    var start_date = $("#time_form #start_date").val();
		if (start_date.length == 10) {
		    request.date_options.start_date = start_date;
		}

	    var end_date = $("#time_form #end_date").val();
		if (end_date.length == 10) {
		    request.date_options.end_date = end_date;
		}


	    last_time_series_request = request;

	    // console.log( $.param( request ) )

	    $.getJSON( "/graph_data", request, function( data ) {

	    	// console.log( data );

	    	time_series.data = data;
	    	time_series.draw();

	    });

    }

    function update_workstream_graphs( ) {

    	var customers = [];    
	    $("#workstream_form #customer_select :selected").each(function(){
	        customers.push($(this).val()); 
	    });

	    var people =[];
	    $("#workstream_form #people_select :selected").each(function(){
	        people.push($(this).val()); 
	    });

	    var activities =[];
	    $("#workstream_form #activities_select :selected").each(function(){
	        activities.push($(this).val()); 
	    });

	    var work_areas =[];
	    $("#workstream_form #work_areas_select :selected").each(function(){
	        work_areas.push($(this).val()); 
	    });

	    var months = [];
	    $("#workstream_form #date_select :selected").each(function(){
	        months.push( $(this).val() );
	    });

	    var ytd = $("#workstream_form #ytd").is( ':checked' );

	    // console.log( activities );
	    // console.log( work_areas );
	    // console.log( people );
	    // console.log( customers );
	    // console.log( months );
	    // console.log( ytd );

	    var request = {
	    	date_options: { months: months, ytd: ytd },
	    	activities: activities,
	    	work_areas: work_areas,
	    	people: people,
	    	customers: customers,
	    	include_workstreams: true
	    };

	    var start_date = $("#workstream_form #start_date").val();
		if (start_date.length == 10) {
		    request.date_options.start_date = start_date;
		}

	    var end_date = $("#workstream_form #end_date").val();
		if (end_date.length == 10) {
		    request.date_options.end_date = end_date;
		}


	    last_workstream_request = request;

	    // console.log( request );
	    // console.log( $.param( request ) )

	    $.getJSON( "/graph_data", request, function( data ) {

	    	// console.log( data );

	    	workstream.data = data;
	    	workstream.draw();

	    });

    }

    function update_xbyx_categories( ) {

		var category = "";
		var selected = $("#xbyx_category_form input[type='radio']:checked");
		if (selected.length > 0) {
		    category = selected.val();
		}

		// console.log( category );

	    $.getJSON( "/graph_data", last_xbyx_request, function( data ) {

	    	$( "#chartContainer_x_by_x" ).empty();

	    	xbyx = createXbyXGraph(
    			dimple.newSvg("#chartContainer_x_by_x", "100%", 400), data, category, ['Activity','Work Area']);
	    	xbyx.draw();
    	});


    }
































