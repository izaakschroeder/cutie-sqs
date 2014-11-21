
'use strict';

var _ = require('lodash'),
	through2 = require('through2'),
	AWS = require('aws-sdk'),
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
		it('should accept string URLs as the only argument', function() {
			this.sandbox.stub(AWS, 'SQS');
			expect(_.partial(SQS, 'http://www.google.ca')).to.not.throw(Error);
		});

		it('should error if given no options', function() {
			expect(SQS).to.throw(TypeError);
		});

		it('should error if given no SQS object', function() {
			expect(_.partial(SQS, { sqs: null })).to.throw(TypeError);
		});

		it('should url.parse', function() {
			var stream = new SQS({queue: 'https://test'});
			stream.end();
		});

		it('should error if highwatermark', function() {
			expect(_.partial(SQS, {
				sqs: this.sqs,
				queue: 'https://test',
				highWaterMark: 11
			})).to.throw(TypeError);
		});
	});

	describe('#_write', function() {
		it('should post message to SQS', function() {
			this.sqs.sendMessage.callsArgWith(1, null, { });
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			this.sandbox.spy(stream, 'emit');
			stream.end({ data: { 'hello': 'world' }});
			expect(this.sqs.sendMessage).to.be.calledOnce;
		});

		it('should error on recursive structures', function() {
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			this.sandbox.spy(stream, 'emit');
			var foo = { };
			foo.a = foo;
			stream.once('error', _.noop);
			stream.end(foo);
			expect(stream.emit).to.be.calledWith('error');
		});

		it('should pass up errors from SQS', function() {
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
		it('should delete the job from SQS', function() {
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			var callback = this.sandbox.stub();
			this.sqs.deleteMessage.callsArgWith(1, null, { });
			stream._delete({ receipt: 'garbage'}, callback);
			expect(this.sqs.deleteMessage).to.be.calledOnce;
			expect(callback).to.be.calledOnce;
		});

		it('should return pass up errors from SQS', function() {
			this.sqs.deleteMessage.callsArgWith(1, 'mistake');
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			var callback = this.sandbox.stub();
			stream._delete({ receipt: 'garbage'}, callback);
			expect(this.sqs.deleteMessage).to.be.calledOnce;
			expect(callback).to.be.calledWith('mistake');
		});
	});

	describe('#_progress', function() {
		it('should update message visibility in SQS', function() {
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			var callback = this.sandbox.stub();
			this.sqs.changeMessageVisibility.callsArgWith(1, null, { });
			stream._progress({ receipt: 'garbage'}, null, callback);
			expect(this.sqs.changeMessageVisibility).to.be.calledOnce;
			expect(callback).to.be.calledOnce;
		});

		it('should return pass up errors from SQS', function() {
			this.sqs.changeMessageVisibility.callsArgWith(1, 'mistake');
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			var callback = this.sandbox.stub();
			stream._progress({ receipt: 'garbage'}, null, callback);
			expect(this.sqs.changeMessageVisibility).to.be.calledOnce;
			expect(callback).to.be.calledWith('mistake');
		});
	});

	describe('#_read', function() {
		it('should pass up errors from SQS', function() {
			this.sqs.receiveMessage.callsArgWith(1, 'mistake');
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test', maxMessages: 0});
			this.sandbox.spy(stream, 'emit');
			stream.once('error', _.noop);
			stream.read();
			stream.close();
			expect(this.sqs.receiveMessage).to.be.calledOnce;
			expect(stream.emit).to.be.calledWith('error', 'mistake');
		});

		it('should put jobs from SQS into the stream', function() {
			this.sqs.receiveMessage.callsArgWith(1, null, {Messages: [{
				receipt: 'hello',
				Body: '{ "message": "hello" }',
				receiptHandle: 'nig',
				Attributes: {
					ApproximateReceiveCount: 1
				}
			}]});
			var stream = new SQS({ sqs: this.sqs, queue: 'https//test'});
			this.sandbox.spy(stream, 'push');
			expect(stream.read()).to.deep.contain({ message: 'hello' });
			expect(stream.push).to.be.calledOnce;
			stream.close();
		});

		it('should return error from if unable to process message', function() {
			this.sqs.receiveMessage.callsArgWith(1, null, {Messages: [{
				receipt: 'hello',
				Body: 'crap',
				receiptHandle: 'nig',
				Attributes: {
					ApproximateReceiveCount: 1
				}
			}]});
			var stream = new SQS({ sqs: this.sqs, queue: 'https//test'});
			stream.once('error', _.noop);
			this.sandbox.spy(stream, 'emit');
			stream.read();
			expect(stream.emit).to.be.calledWith('error');
		});

		it('should keep reading more messages until close', function(done) {
			this.sqs.receiveMessage.callsArgWith(1, null, { });
			var stream = new SQS({ sqs: this.sqs, queue: 'https//test'});

			stream.read();
			this.sandbox.spy(stream, '_read');

			process.nextTick(function() {
				expect(stream._read).to.be.calledOnce;
				stream.close();
				done();
			});

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
			this.sandbox.stub(stream, '_delete');
			var r = stream.receiver();
			var job = {noReceipt: 'potato'};
			r.end(job);
			expect(stream._delete).to.not.be.called;
			expect(r.read()).to.deep.equal(job);
		});

		it('should delete jobs with receipts', function() {
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			this.sandbox.stub(stream, '_delete').callsArgWith(1, null);
			var r = stream.receiver();
			r.end({receipt: 'true'});
			expect(stream._delete).to.be.calledOnce;
		});

		it('should pass through jobs with receipts', function() {
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			var stub = this.sandbox.stub(stream, '_delete');
			var r = stream.receiver();
			var job = {receipt: 'true'};
			stub.callsArgWith(1, null);
			r.write(job);
			expect(r.read()).to.deep.equal(job);
		});


		it('should pass up errors from _progress', function() {
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			this.sandbox.stub(stream, '_progress').callsArgWith(2, 'error');
			var r = stream.receiver();
			this.sandbox.spy(r, 'emit');
			var src = through2.obj();
			r.once('error', _.noop);
			src.pipe(r);
			src.emit('progress');
			expect(r.emit).to.be.calledWith('error', 'error');
		});

		it('should pass up errors from _delete', function() {
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			this.sandbox.stub(stream, '_delete').callsArgWith(1, 'error');
			var r = stream.receiver();
			this.sandbox.spy(r, 'emit');
			r.once('error', _.noop);
			r.end({receipt: 'true'});
			expect(r.emit).to.be.calledWith('error', 'error');
		});

		it('should watch for `progress` events from children', function() {
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			this.sandbox.stub(stream, '_progress').callsArgWith(2, null);
			var r = stream.receiver();
			var src = through2.obj();
			src.pipe(r);
			src.emit('progress');
			expect(stream._progress).to.be.calledOnce;
		});

		it('should watch for `unpipe` events from children', function() {
			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			var r = stream.receiver();
			var src = through2.obj();
			src.pipe(r);
			src.unpipe(r);
		});

	});

	describe('pipe', function() {
		it('should inject the receiver', function(done) {
			this.sqs.receiveMessage.callsArgWith(1, null, {Messages: [{
				receipt: 'hello',
				Body: '{ "message": "hello" }',
				receiptHandle: 'nig',
				Attributes: {
					ApproximateReceiveCount: 1
				}
			}]});

			var stream = new SQS({ sqs: this.sqs, queue: 'https://test' });
			var herp = this.sandbox.stub();
			var dest = through2.obj(herp);
			stream.pipe(dest);

			process.nextTick(function() {
				expect(herp).to.be.calledOnce.and.calledWithMatch({
					message: 'hello'
				});
				stream.close();
				done();
			});

		});
	});

});
