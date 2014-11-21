
'use strict';

var _ = require('lodash'),
	AWS = require('aws-sdk'),
	Stream = require('stream'),
	through2 = require('through2'),
	url = require('url'),
	util = require('util');

/**
* @constructor
* @param {Object} opts Configuration options.
*/
function SQSStream(opts) {
	if (this instanceof SQSStream === false) {
		return new SQSStream(opts);
	}

	if (_.isString(opts)) {
		opts = { queue: opts };
	}

	if (!_.isObject(opts)) {
		throw new TypeError();
	}

	if (!_.has(opts, 'queue')) {
		throw new TypeError();
	}

	// We can only request chunks 10 at a time as per the AWS docs
	if (_.has(opts, 'highWaterMark') && opts.highWaterMark > 10) {
		throw new TypeError();
	}

	Stream.Duplex.call(this, {
		objectMode: true,
		highWaterMark: opts.highWaterMark || 1
	});
	this.queue = opts.queue;
	this.wait = opts.wait || 20;
	this.timeout = opts.timeout || 100;

	if (!_.has(opts, 'sqs')) {
		var parts = url.parse(opts.queue).host.split('.');
		this.sqs = new AWS.SQS({ region: parts[1] });
	} else {
		this.sqs = opts.sqs;
	}
}
util.inherits(SQSStream, Stream.Duplex);

/**
 * Create a stream that captures job event information from a worker including
 * job completion and progress. It inspects any jobs that pass through it and
 * transparently pipes along anything else.
 *
 * @returns {Object} Stream.
 */
SQSStream.prototype.receiver = function receiver() {
	var self = this;

	var stream = through2.obj(function jobFinished(job, encoding, callback) {

		// If it's not a job then just pass it along
		if (!job.receipt) {
			return callback(null, job);
		}

		self._delete(job, function jobDeleted(err) {
			if (err) {
				return callback(err);
			}
			return callback(null, job);
		});
	});

	function progress(job, amount) {
		self._progress(job, amount, function progressUpdated(err) {
			if (err) {
				stream.emit('error', err);
			}
		});
	}

	stream.on('pipe', function piped(src) {
		src.on('progress', progress);
	}).on('unpipe', function unpiped(src) {
		src.removeListener('progress', progress);
	});

	return stream;
};

/**
 * Override the default stream pipe mechanism as a way of re-capturing the
 * finished jobs. When a worker stream is done with the job, it puts the job
 * back into the stream and we spy on that with our receiver so that when a
 * job is marked as done we can remove it from the SQS queue. This also lets
 * use capture "progress" events if they're emitted.
 *
 * @returns {Object} The injected stream.
 *
 * @see Stream.Readable.pipe
 */
SQSStream.prototype.pipe = function pipe() {
	return Stream.Duplex.prototype.pipe.apply(this, arguments)
		.pipe(this.receiver());
};


/**
* Remove a job from the queue.
*
* @param {Object} job The job to remove.
* @param {Function} callback Called when operation is finished.
* @returns {void}
*/
SQSStream.prototype._delete = function _delete(job, callback) {
	this.sqs.deleteMessage({
		QueueUrl: this.queue,
		ReceiptHandle: job.receipt
	}, callback);
};

/**
* Delay the expiration of the job for some time when the job has been marked
* as making progress.
*
* @param {Object} job The job to delay.
* @param {Number} amount How close the job is to being done.
* @param {Function} callback Called when operation is finished.
* @returns {void}
*/
SQSStream.prototype._progress = function _progress(job, amount, callback) {
	this.sqs.changeMessageVisibility({
		QueueUrl: this.queue,
		ReceiptHandle: job.receipt,
		VisibilityTimeout: 300
	}, callback);
};

/**
* Remove jobs from the queue.
*
* @param {Number} amount How many jobs to read.
* @returns {void}
*
* @see Stream.Readable._read
*/
SQSStream.prototype._read = function _read(amount) {
	var self = this;
	this.sqs.receiveMessage({
		QueueUrl: this.queue,
		MaxNumberOfMessages: amount,
		VisibilityTimeout: this.timeout,
		WaitTimeSeconds: this.wait,
		AttributeNames: [ 'ApproximateReceiveCount' ]
	}, function gotMessages(err, data) {

		// If there's an error then broadcast it.
		if (err) {
			self.emit('error', err);
			return;
		}

		// If we didn't get anything from the queue, but we're not over yet,
		// then keep trying to read more data from the queue.
		if (!_.has(data, 'Messages') && !self._readableState.ended) {
			process.nextTick(function getMore() {
				self._read(amount);
			});
			return;
		}

		// Deserialize the incoming messages and send them off.
		_.forEach(data.Messages, function processMessage(message) {
			try {
				var job = _.assign(JSON.parse(message.Body), {
					attempts: message.Attributes.ApproximateReceiveCount,
					receipt: message.ReceiptHandle
				});

			} catch(e) {
				self.emit('error', e);
				return;
			}
			self.push(job);
		});
	});
};

/**
 * Shutdown the reading component of the SQS stream so that no more jobs are
 * read from the queue.
 *
 * @returns {void}
 */
SQSStream.prototype.close = function close() {
	this.push(null);
};

/**
* Put a job into the queue.
*
* @param {Object} job The job to put into the queue.
* @param {String} encoding Unused for object streams.
* @param {Function} callback Called when the job has been put into the queue.
* @returns {void}
*
* @see Stream.Writable._write
*/
SQSStream.prototype._write = function _write(job, encoding, callback) {
	var body;

	// Check for things like recursive structures, etc.
	try {
		body = JSON.stringify(job);
	} catch(e) {
		callback(e);
		return;
	}

	// Dump the message into the queue
	this.sqs.sendMessage({
		MessageBody: body,
		QueueUrl: this.queue,
		DelaySeconds: job.delay || 0
	}, callback);
};


module.exports = SQSStream;
