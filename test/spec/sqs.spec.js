
'use strict';

var _ = require('lodash'),
	SQS = require('sqs');

describe('SQS', function() {

	beforeEach(function() {

	});

	describe('#constructor', function() {
		it('should error if given no options', function() {
			expect(SQS).to.throw(TypeError);
		});

		it('should error if given no SQS object', function() {
			expect(_.partial(SQS, { sqs: null })).to.throw(TypeError);
		});
	});

	describe('#pop', function() {

	});

	describe('#push', function() {

	});
});
