function new_poolchart(canvas) {
	let labels = [];
	let load_series = [];
	let running_series = [];
	let starting_and_running_series = [];
	let desired_series = [];

	let load_ds = {
		label: 'Load',
		data: load_series,
		steppedLine: 'after',
		fill: false,
		showLine: true,
		//
		pointRadius: 3,
		borderWidth: 0,
		borderColor: 'pink',
		backgroundColor: 'pink',
	};

	// Running: darker green;
	// starting: lighter green on top of that
	let running_ds = {
		label: 'Running',
		data: running_series,
		steppedLine: 'before',
		fill: true,
		//
		pointRadius: 0,
		borderWidth: 0,
		borderColor: '#afc',
		backgroundColor: '#afc',
	};
	let starting_and_running_ds = {
		label: 'Starting + Running',
		data: starting_and_running_series,
		steppedLine: 'before',
		fill: true,
		//
		pointRadius: 0,
		borderWidth: 0,
		borderColor: '#dfe',
		backgroundColor: '#dfe'
	};
	// Desired: dotted line
	let desired_ds = {
		label: 'Desired',
		data: desired_series,
		steppedLine: 'before',
		fill: false,
		//
		pointRadius: 0,
		borderWidth: 1,
		borderDash: [5, 5],
		borderColor: 'black',
		backgroundColor: 'black',
	};

	let options = {
		responsive: false,
		animation: {
			duration: 0,
		},
		legend: {
			display: false,
		},
		scales: {
			xAxes: [{
				type: 'linear',
				display: true,
				scaleLabel: {
					display: true,
					labelString: 'minutes ago',
				},
				ticks: {
					suggestedMin: -5,
					suggestedMax: 0,
				},
			}],
			yAxes: [
				{
					type: 'linear',
					ticks: {
						stepSize: 0.5,
						beginAtZero: true,
						suggestedMin: 0,
						suggestedMax: 4,
					},
					// display: true,
					// scaleLabel: {
					// 	display: true,
					// 	labelString: 'pear',
					// }
				},
			],
		}
	}

	let config = {
		type: 'line',
		options: options,
		data: {
			labels: labels,
			datasets: [
				running_ds,
				starting_and_running_ds,
				desired_ds,
				load_ds,
			]
		}
	};

	let ctx = canvas.getContext('2d');
	let chart = new Chart(ctx, config);

	return {
		canvas: canvas,
		chart: chart,
		raw_stats: [],
		labels: labels,
		load_series: load_series,
		running_series: running_series,
		starting_and_running_series: starting_and_running_series,
		desired_series: desired_series,
	};
}

function add_to_poolchart(poolchart, now, stats) {
	poolchart.raw_stats.push({
		timestamp: now,
		...stats
	})
}

function refresh_poolchart(poolchart, now) {
	function process(name, arr, f, labels=null) {
		arr.length = 0;
		if (labels != null) {
			labels.length = 0;
		}
		for (let entry of poolchart.raw_stats) {
			let t = (entry.timestamp - now) / 60000;
			arr.push({
				x: t,
				y: f(entry)
			})
			if (labels != null) {
				labels.push(t);
			}
		}
		console.log(name + " " + JSON.stringify(arr));
	}
	process("load",
		poolchart.load_series,
		entry => entry.load,
		poolchart.labels);
	process("running",
		poolchart.running_series,
		entry => entry.up
	);
	process("starting",
		poolchart.starting_and_running_series,
		entry => entry.up + entry.starting
	);
	process("desired",
		poolchart.desired_series,
		entry => entry.desired
	);
	poolchart.chart.update();
	console.log(".")
}