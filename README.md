# cutie-sqs

SQS backend for [cutie](http://www.github.com/izaakschroeder/cutie).

![build status](http://img.shields.io/travis/izaakschroeder/cutie-sqs.svg?style=flat)
![coverage](http://img.shields.io/coveralls/izaakschroeder/cutie-sqs.svg?style=flat)
![license](http://img.shields.io/npm/l/cutie-sqs.svg?style=flat)
![version](http://img.shields.io/npm/v/cutie-sqs.svg?style=flat)
![downloads](http://img.shields.io/npm/dm/cutie-sqs.svg?style=flat)

Got jobs you need to run from an SQS queue? Now processing them is easy.

```javascript
var through2 = require('through2'),
	sqs = require('cutie-sqs');

function work(job, encoding, callback) {
	// Do work here
	...

	// Job is done
	callback(null, job);
}

// Create the job queue
var jobs = sqs('https://my-queue-url');

// Process jobs
jobs.pipe(through2.obj(work));

// Send a job
jobs.write({ message: 'hello world' });
```
