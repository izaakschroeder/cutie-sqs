
'use strict';

var argv = require('yargs'),
	path = require('path'),
	through2 = require('through2'),
	sqs = require(path.join(__dirname, '..'));

function work(job, encoding, callback) {
	console.log('workin on', job);
	var self = this;
	// Finish the job after 1 second.
	setTimeout(function done() {
		self.push(job);
		callback();
	}, 1000);
}


// Specify SQS queue url as --queue=https://...
var jobs = sqs(argv.queue);
var worker = through2({ objectMode: true, highWaterMark: 1 }, work);
jobs.pipe(worker);


var count = 0;
// Inject jobs into the queue every 3 seconds.
setInterval(function job() {
	jobs.write({ type: 'test', data: { message: 'hello ' + count } });
}, 3000);
