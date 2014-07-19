/*jshint node:true */
/*global describe:false, it:false */
"use strict";

var Orchestrator = require('../');
var Readable = require('stream').Readable;
var Writable = require('stream').Writable;
var Duplex = require('stream').Duplex;
var Q = require('q');
var fs = require('fs');
var should = require('should');
require('mocha');

describe('orchestrator', function() {
	describe('given a stream', function() {

		it('should consume a Readable stream to relieve backpressure, in objectMode', function(done) {
			var orchestrator, a;

			// Arrange
			orchestrator = new Orchestrator();
			a = 0;
			orchestrator.add('test', function() {
                // Create a Readable stream with a small buffer...
                var rs = Readable({objectMode: true, highWaterMark: 2});
                rs._read = function() {
                    // ...and generate more chunks than fit in that buffer
                    if (a++ < 100) {
                        rs.push(a);
                    } else {
                        rs.push(null);
                    }
                };
                return rs;
			});

			// Act
			orchestrator.start('test', function(err) {
				// Assert
                // Simple completion of the task is the main criterion here, but check a few things:
				a.should.be.above(99);
				should.not.exist(err);
				orchestrator.isRunning.should.equal(false);
				done();
			});
		});

		it('should consume a Readable stream to relieve backpressure', function(done) {
			var orchestrator, a;

			// Arrange
			orchestrator = new Orchestrator();
			a = 0;
			orchestrator.add('test', function() {
                // Create a Readable stream with a small buffer...
                var rs = Readable({highWaterMark: 2});
                rs._read = function() {
                    // ...and generate more chunks than fit in that buffer
                    if (a++ < 100) {
                        rs.push(".");
                    } else {
                        rs.push(null);
                    }
                };
                return rs;
			});

			// Act
			orchestrator.start('test', function(err) {
				// Assert
                // Simple completion of the task is the main criterion here, but check a few things:
				a.should.be.above(99);
				should.not.exist(err);
				orchestrator.isRunning.should.equal(false);
				done();
			});
		});

		it('should detect completion of a Writable stream', function(done) {
			var orchestrator, a, lengthRead;

			// Arrange
			orchestrator = new Orchestrator();
			a = 0;
            lengthRead = 0;
			orchestrator.add('test', function() {
                // Create a Readable stream...
                var rs = Readable({highWaterMark: 2});
                rs._read = function() {
                    if (a++ < 100) {
                        rs.push(".");
                    } else {
                        rs.push(null);
                    }
                };

                // ...and consume it
                var ws = Writable();
                ws._write = function(chunk, enc, next) {
                    lengthRead += chunk.length;
                    next();
                };
                rs.pipe(ws);

                // Return the Writable
                return ws;
			});

			// Act
			orchestrator.start('test', function(err) {
				// Assert
				a.should.be.above(99);
                lengthRead.should.equal(100);
				should.not.exist(err);
				orchestrator.isRunning.should.equal(false);
				done();
			});
		});

		it('should detect completion of a Writable stream, in objectMode', function(done) {
			var orchestrator, a, lengthRead;

			// Arrange
			orchestrator = new Orchestrator();
			a = 0;
            lengthRead = 0;
			orchestrator.add('test', function() {
                // Create a Readable stream...
                var rs = Readable({objectMode: true, highWaterMark: 2});
                rs._read = function() {
                    if (a++ < 100) {
                        rs.push(a);
                    } else {
                        rs.push(null);
                    }
                };

                // ...and consume it
                var ws = Writable({objectMode: true});
                ws._write = function(chunk, enc, next) {
                    lengthRead++;
                    next();
                };
                rs.pipe(ws);

                // Return the Writable
                return ws;
			});

			// Act
			orchestrator.start('test', function(err) {
				// Assert
				a.should.be.above(99);
                lengthRead.should.equal(100);
				should.not.exist(err);
				orchestrator.isRunning.should.equal(false);
				done();
			});
		});

		it('should handle an intermediate Readable stream being returned', function(done) {
			var orchestrator, a, lengthRead;

			// Arrange
			orchestrator = new Orchestrator();
			a = 0;
            lengthRead = 0;
			orchestrator.add('test', function() {
                // Create a Readable stream...
                var rs = Readable({highWaterMark: 2});
                rs._read = function() {
                    if (a++ < 100) {
                        rs.push(".");
                    } else {
                        rs.push(null);
                    }
                };

                // ...and consume it
                var ws = Writable();
                ws._write = function(chunk, enc, next) {
                    lengthRead += chunk.length;
                    next();
                };
                rs.pipe(ws);

                // Return the Readable
                return rs;
			});

			// Act
			orchestrator.start('test', function(err) {
				// Assert
				a.should.be.above(99);
                // Ensure all data was received by the Writable
                lengthRead.should.equal(100);
				should.not.exist(err);
				orchestrator.isRunning.should.equal(false);
				done();
			});
		});

		it('should handle an intermediate Readable stream being returned, in objectMode', function(done) {
			var orchestrator, a, lengthRead;

			// Arrange
			orchestrator = new Orchestrator();
			a = 0;
            lengthRead = 0;
			orchestrator.add('test', function() {
                // Create a Readable stream...
                var rs = Readable({objectMode: true, highWaterMark: 2});
                rs._read = function() {
                    if (a++ < 100) {
                        rs.push(a);
                    } else {
                        rs.push(null);
                    }
                };

                // ...and consume it
                var ws = Writable({objectMode: true});
                ws._write = function(chunk, enc, next) {
                    lengthRead++;
                    next();
                };
                rs.pipe(ws);

                // Return the Readable
                return rs;
			});

			// Act
			orchestrator.start('test', function(err) {
				// Assert
				a.should.be.above(99);
                // Ensure all data was received by the Writable
                lengthRead.should.equal(100);
				should.not.exist(err);
				orchestrator.isRunning.should.equal(false);
				done();
			});
		});

	});
});
