function longpoll_status(url, cb, id = null, seen = null) {
	let opts = {
		method: 'POST',
		headers: {
			'Content-Type': 'application/json',
		},
		body: {},
	};
	let body = {};
	if (id) {
		body.id = id;
	}
	if (seen) {
		body.seen = seen;
	}
	opts.body = JSON.stringify(body);
	fetch(url, opts)
		.then(function (response) {
			// First check the response itself. Fetch the result as json or
			// turn it into an an error if not ok.
			if (response.ok) {
				return response.json(); // this is promise, not the object
			} else {
				return response.text().then(t => Promise.reject(t))
			}
		})
		.then(function (response_body) {
			// schedule the next poll
			setTimeout(longpoll_status, 1000, url, cb, response_body.id, response_body.seen);

			// pass the current response down the line
			return Promise.resolve(response_body);
		})
		.then(function (response_body) {
			cb(response_body);
		})
		// .catch(function (err) {
		// 	console.log("ERROR: " + JSON.stringify(err));
		// })
}