/*jshint node:true */

"use strict";

var eos = require('end-of-stream');
var Writable = require('stream').Writable;
var Readable = require('stream').Readable;

module.exports = function (task, done) {
	var that = this, finish, cb, isDone = false, start, r;

	finish = function (err, runMethod) {
		var hrDuration = process.hrtime(start);

		if (isDone && !err) {
			err = new Error('task completion callback called too many times');
		}
		isDone = true;

		var duration = hrDuration[0] + (hrDuration[1] / 1e9); // seconds

		done.call(that, err, {
			duration: duration, // seconds
			hrDuration: hrDuration, // [seconds,nanoseconds]
			runMethod: runMethod
		});
	};

	cb = function (err) {
		finish(err, 'callback');
	};

	try {
		start = process.hrtime();
		r = task(cb);
	} catch (err) {
		return finish(err, 'catch');
	}

	if (r && typeof r.then === 'function') {
		// wait for promise to resolve
		// FRAGILE: ASSUME: Promises/A+, see http://promises-aplus.github.io/promises-spec/
		r.then(function () {
			finish(null, 'promise');
		}, function(err) {
			finish(err, 'promise');
		});

	} else if (r && typeof r.pipe === 'function') {
		// wait for stream to end

		eos(r, { error: true, readable: r.readable, writable: r.writable && !r.readable }, function(err){
			finish(err, 'stream');
		});
		// If it's a readable stream, consume it
		if (r._readableState) {
			// Modern stream - pipe it to a Writable
			var drainer = Writable({objectMode: r._readableState.objectMode});
			drainer._write = function(chunk, enc, next) {
				next();
			};
			r.pipe(drainer);
		} else if (r.readable) {
			// Legacy stream - just add a 'data' handler to keep consume
			r.on("data", function() {});
		}

	} else if (task.length === 0) {
		// synchronous, function took in args.length parameters, and the callback was extra
		finish(null, 'sync');

	//} else {
		// FRAGILE: ASSUME: callback

	}
};
