
var path = require('path'),
	through2 = require('through2'),
	sqs = require(path.join(__dirname, '..'));

function work(job, encoding, callback) {
	console.log('workin on', job);
	var self = this;
	setTimeout(function() {
		self.push(job);
		callback();
	}, 1000);
}


var jobs = sqs('https://sqs.us-west-2.amazonaws.com/534692210857/test');
var worker = through2({ objectMode: true, highWaterMark: 1 }, work);
jobs.pipe(worker);


var count = 0;

setInterval(function job() {
	jobs.write({ type: 'test', data: { message: 'hello '+count } });
}, 3000);
