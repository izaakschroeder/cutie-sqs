
/**
 * @constructor
 * @param {Object} opts
 *
 */
function SQSQueue(opts) {
	this.queues = opts.queues;
	this.wait = 60*5;
	this.sqs = opts.sqs;
}

SQSQueue.prototype.dequeue = function(opts, callback) {

	opts = _.assign({
		amount: 1,
		timeout: 100,
		wait: this.wait
	}, opts);

	var queue = this.queues[opts.type];

	if (!queue) {
		return callback(null, []);
	}

	sqs.receiveMessage({
		QueueUrl: queue,
		MaxNumberOfMessages: opts.amount,
		VisibilityTimeout: opts.timeout,
		WaitTimeSeconds: opts.wait,
		AttributeNames: [ 'ApproximateReceiveCount' ]
	}, function(err, data) {

		if (err) {
			return callback(err);
		}

		var jobs = _.map(data.Messages, function(message) {
			return {
				attempts: message.Attributes.ApproximateReceiveCount,
				data: JSON.parse(message.Body),
				qqq: { receipt: message.ReceiptHandle }
			}
		});

		return callback(null, jobs);
	});
}

SQSQueue.prototype.delete = function(job, callback) {
	sqs.deleteMessage({
		QueueUrl: job.qqq.queue,
		ReceiptHandle: job.qqq.receipt
	}, callback);
}

SQSQueue.prototype.progress = function(job, callback) {
	sqs.changeMessageVisibility({
		QueueUrl: job.qqq.queue,
		ReceiptHandle: job.qqq.ReceiptHandle,
		VisibilityTimeout: 444
	}, callback);
}

return function processChunk(done) {

}
