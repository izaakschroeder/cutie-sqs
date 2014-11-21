
'use strict';

var _ = require('lodash'),
	SQS = require('sqs');

describe('SQS', function() {

	beforeEach(function() {
		this.sandbox = sinon.sandbox.create();
		this.sqs = {
			deleteMessage: this.sandbox.stub(),
			changeMessageVisibility: this.sandbox.stub(),
			sendMessage: this.sandbox.stub(),
			receiveMessage: this.sandbox.stub()
		};
	});

	afterEach(function() {
		this.sandbox.restore();
	});

	describe('#constructor', function() {
		it('should error if given no options', function() {
			expect(SQS).to.throw(TypeError);
		});

		it('should error if given no SQS object', function() {
			expect(_.partial(SQS, { sqs: null })).to.throw(TypeError);
		});
	});

	describe('#_write', function() {
		it('should dump message into the queue', function() {
			this.sqs.sendMessage.callsArgWith(1, null, { });
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			this.sandbox.spy(stream, 'emit');
			stream.end({ data: { 'hello': 'world' }});
			expect(this.sqs.sendMessage).to.be.calledOnce;
			expect(stream.emit).to.be.calledWith('finish');
		});

		it('should check recursive structure', function() {
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			this.sandbox.spy(stream, 'emit');
			var foo = { };
			foo.a = foo;
			stream.once('error', _.noop);
			stream.end(foo);
			expect(stream.emit).to.be.calledWith('error');
		});

		it('should return an error', function() {
			this.sqs.sendMessage.callsArgWith(1, 'mistake');
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			this.sandbox.spy(stream, 'emit');
			stream.once('error', _.noop);
			stream.end({ data: { 'hello': 'world' }});
			expect(this.sqs.sendMessage).to.be.calledOnce;
			expect(stream.emit).to.be.calledWith('error', 'mistake');
		});
	});

	describe('#_delete', function() {
		it('should delete the job', function() {
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			var callback = this.sandbox.stub();
			this.sqs.deleteMessage.callsArgWith(1, null, { });
			stream._delete({ receipt: 'garbage'}, callback);
			expect(this.sqs.deleteMessage).to.be.calledOnce;
			expect(callback).to.be.calledOnce;
		});

		it('should return an error', function() {
			this.sqs.deleteMessage.callsArgWith(1, 'mistake');
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			var callback = this.sandbox.stub();
			stream._delete({ receipt: 'garbage'}, callback);
			expect(this.sqs.deleteMessage).to.be.calledOnce;
			expect(callback).to.be.calledWith('mistake');
		});
	});

	describe('#progress', function() {
		it('should continue working', function() {
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			var callback = this.sandbox.stub();
			this.sqs.changeMessageVisibility.callsArgWith(1, null, { });
			stream._progress({ receipt: 'garbage'}, null, callback);
			expect(this.sqs.changeMessageVisibility).to.be.calledOnce;
			expect(callback).to.be.calledOnce;
		});
		it('should return an error', function() {
			this.sqs.changeMessageVisibility.callsArgWith(1, 'mistake');
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			var callback = this.sandbox.stub();
			stream._progress({ receipt: 'garbage'}, null, callback);
			expect(this.sqs.changeMessageVisibility).to.be.calledOnce;
			expect(callback).to.be.calledWith('mistake');
		});
	});

	describe('#_read', function() {
		it('should return an error', function() {
			this.sqs.receiveMessage.callsArgWith(1, 'mistake');
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test', maxMessages: 0});
			this.sandbox.spy(stream, 'emit');
			stream.once('error', _.noop);
			stream.read(null);
			expect(this.sqs.receiveMessage).to.be.calledOnce;
			expect(stream.emit).to.be.calledWith('error', 'mistake');
		});

		it('should read job until maxMessage read count has been reached', function() {
			this.sqs.receiveMessage.callsArgWith(1, null, { });
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test', maxMessages: 3});
			stream.read(null);
			expect(stream.readMessages === 3);
		});

		it('should push job into queue', function() {
			this.sqs.receiveMessage.callsArgWith(1, null, {Messages: [{
				receipt: 'hello',
				Body: '{ }',
				receiptHandle: 'nig',
				Attributes: {
					ApproximateReceiveCount: 1
				}
			}]});
			var stream = new SQS({ sqs: this.sqs, queue: 'https//test'});
			stream.read(1);
		});
	});

	describe('receiver', function() {

		it('should return the stream', function() {
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			var r = stream.receiver();
			r.end();
		});

		it('should pass jobs with no receipt', function() {
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			var stub = this.sandbox.stub(stream, '_delete');
			var r = stream.receiver();
			r.end({noReceipt: 'potato'});
			expect(stub).to.not.be.called;
		});

		it('should delete jobs with receipts', function() {
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			var stub = this.sandbox.stub(stream, '_delete');
			var r = stream.receiver();
			r.end({receipt: 'true'});
			expect(stub).to.be.called;
		});

		it('should return after deleting job with receipt', function() {
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			var r = stream.receiver();
			r.end({receipt: 'true'});
		});

	});

	describe('pipe', function() {

	});

	describe('#pop', function() {

	});

	describe('#push', function() {

	});
});
