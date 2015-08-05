var Readable = require('stream').Readable;
var StringDecoder = require('string_decoder').StringDecoder;
var url = require('url');
var util = require('util');
var qs = require('querystring');
var debug = require('debug')('changes-stream');
var http = require('http-https');
var back = require('back');

var extend = util._extend;

module.exports = ChangesStream;

util.inherits(ChangesStream, Readable);

//
// @ChangeStream
// ## Constructor to initialize the changes stream
//
function ChangesStream (options) {
  if (!(this instanceof ChangesStream)) { return new ChangesStream(options) }
  options = options || {};

  var hwm = options.highWaterMark || 16;
  Readable.call(this, { objectMode: true, highWaterMark: hwm });
  //
  // PARSE ALL THE OPTIONS OMG
  //
  this._feedParams = ['heartbeat', 'feed', 'filter', 'include_docs', 'view', 'style', 'since'];
  // Bit of a buffer for aggregating data
  this._buffer = '';
  this._decoder = new StringDecoder('utf8');

  this.timeout = options.timeout || 2 * 60 * 1000;
  // Time to wait for a new change before we jsut retry a brand new request
  this.inactivity_ms = options.inactivity_ms || 60 * 60 * 1000;
  this.reconnect = options.reconnect || { minDelay: 100, maxDelay: 30 * 1000, retries: 5 };
  this.db = typeof options === 'string'
    ? options
    : options.db;

  // http option
  this.rejectUnauthorized = options.strictSSL || options.rejectUnauthorized || true;

  if (!this.db) {
    throw new Error('DB is required');
  }
  // Setup all query options and defaults
  this.feed = options.feed || 'continuous';
  this.since = options.since || 0;
  // Allow couch heartbeat to be used but we can just manage that timeout
  this.heartbeat = options.heartbeat || 30 * 1000;
  this.style = options.style || 'main_only';

  this.filterIds = Array.isArray(options.filter)
    ? options.filter
    : false;

  this.filter = !this.filterIds
    ? (options.filter || false)
    : '_doc_ids';

  this.clientFilter = typeof this.filter === 'function';
  // If we are doing a client side filter we need the actual document
  this.include_docs = !this.clientFilter
    ? (options.include_docs || false)
    : true;

  this.paused = false;
  this.request();
}

//
// Setup all the _changes query options
//
ChangesStream.prototype.preRequest = function () {
  // We want to actually reform this every time in case something has changed
  this.query = this._feedParams.reduce(function (acc, key) {
    if (this[key]) {
      acc[key] = this[key];
    }
    return acc;
  }.bind(this), {});

  // Remove filter from query parameters since we have confirmed it as
  // a function
  if (this.clientFilter) {
    delete this.query.filter;
  }
};

//
// Make the changes request and start listening on the feed
//
ChangesStream.prototype.request = function () {
  // Setup possible query string options
  this.preRequest();
  var opts = url.parse(this.db + '/_changes?' + qs.stringify(this.query)),
      payload;
  //
  // Handle both cases of POST and GET
  //
  opts.method = this.filterIds ? 'POST' : 'GET';
  opts.timeout = this.timeout;
  opts.rejectUnauthorized = this.rejectUnauthorized;
  opts.headers = {
    'accept': 'application/json'
  };

  //
  // When we are a post we need to create a payload;
  //
  if (this.filterIds) {
    opts.headers['content-type'] = 'application/json';
    payload = new Buffer(JSON.stringify(this.filterIds), 'utf8');
  }

  //
  // Set a timer for the initial request with some extra magic number
  //
  this.timer = setTimeout(this.onTimeout.bind(this), (this.heartbeat || 30 * 1000) + 5000)

  this.req = http.request(opts);
  this.req.setSocketKeepAlive(true);
  this.req.once('error', this._onError.bind(this));
  this.req.once('response', this._onResponse.bind(this));
  if (payload) {
    this.req.write(payload);
  }
  this.req.end();

};


//
// Handle the response from a new request
// Remark: Should we use on('data') and just self buffer any events we get
// when a proper pause is called? This may be more intuitive behavior that is
// compatible with how streams3 will work anyway. This just makes the _read
// function essentially useless as it is on most cases
//
ChangesStream.prototype._onResponse = function (res) {
  clearTimeout(this.timer);
  this.timer = null;
  if (res.statusCode !== 200) {
    var err = new Error('Received a ' + res.statusCode + ' from couch');
    err.statusCode = res.statusCode;
    return this.emit('error', err);
  }
  this.source = res;
  //
  // Set a timer so that we know we are actually getting some changes from the
  // socket
  //
  this.timer = setTimeout(this.onTimeout.bind(this), this.inactivity_ms);
  this.source.on('data', this._readData.bind(this));
  this.source.on('end', this._onEnd.bind(this));
};

//
// Little wrapper around retry for our self set timeouts
//
ChangesStream.prototype.onTimeout = function () {
  clearTimeout(this.timer);
  this.timer = null
  debug('request timed out or is inactive, lets retry');
  this.retry();
};

//
// Parse and read the data that we get from _changes
//
ChangesStream.prototype._readData = function (data) {
  debug('data event fired from the underlying _changes response');

  this._buffer += this._decoder.write(data);

  var lines = this._buffer.split('\n');
  this._buffer = lines.pop();

  for (var i=0; i<lines.length; i++) {
    var line = lines[i];

    try { line = JSON.parse(line) }
    catch (ex) { return; }
    //
    // Process each change
    //
    this._onChange(line);
  }
};

//
// Process each change request
//
ChangesStream.prototype._onChange = function (change) {
  var query, doc;
  if (this.timer) {
    clearTimeout(this.timer);
    this.timer = null;
    this.timer = setTimeout(this.onTimeout.bind(this), this.inactivity_ms);
  }

  if (change === '') {
    return this.emit('heartbeat');
  }

  //
  // Update the since value internally as we will need that to
  // be up to date for proper retries
  //
  this.since = change.seq || change.last_seq || this.since;

  //
  // This is ugly but replicates the correct behavior
  // for running a client side filter function
  //
  if (this.clientFilter) {
    doc = JDUP(change.doc);
    query = JDUP({ query: this.query });
    if (!this.filter(doc, query)) {
      return;
    }
  }

  //
  // If we are ever going to have backpressure issues
  // we would want to see if push returned false/null
  // and then stop reading from underlying source.
  //
  if (!this.push(change)) {
    debug('paused feed due to highWatermark and backpressure purposes');
    this.pause();
  }

  //
  // End the stream if we are on teh last change
  //
  if (change.last_seq) this.push(null);
};

//
// On error be set for retrying the underlying request
//
ChangesStream.prototype._onError = function (err) {
  this.attempt = this.attempt || extend({}, this.reconnect);
  return back(function (fail, opts) {
    if (fail) {
      this.attempt = null;
      return this.emit('error', err);
    }
    debug('retry # %d', opts.attempt);

    this.retry();
  }.bind(this), this.attempt);
};

//
// When response ends (for example. CouchDB shuts down gracefully), create an
// artificial error to let the user know what happened.
//
ChangesStream.prototype._onEnd = function () {
  this._onError(new Error('CouchDB disconnected gracefully'));
};

//
// Cleanup and flush any data and retry the request
//
ChangesStream.prototype.retry = function () {
  debug('retry request');
  this.emit('retry');
  this.cleanup();
  this.request();
};

//
// Pause the underlying socket if we want manually handle that backpressure
// and buffering
//
ChangesStream.prototype.pause = function () {
  if (!this.paused) {
    debug('paused the source request');
    this.emit('pause');
    this.source && this.source.pause();
    this.paused = true;
  }
};

//
// Resume the underlying socket so we continue to push changes onto the
// internal buffer
//
ChangesStream.prototype.resume = function () {
  if (this.paused) {
    debug('resumed the source request');
    this.emit('resume');
    this.source.resume();
    this.paused = false;
  }
};

ChangesStream.prototype.preCleanup = function () {
  var rem = this._buffer.trim();
  debug('precleanup: do we have remaining data?')
  if (rem) {
    debug('attempting to parse remaining data');
    try { rem = JSON.parse(rem) }
    catch (ex) { return }

    this.push(rem);
  }
};

//
// Cleanup the valuable internals, great for before a retry
//
ChangesStream.prototype.cleanup = function () {
  debug('cleanup: flushing any possible buffer and killing underlying request');
  if (this.timer) {
    clearTimeout(this.timer);
    this.timer = null;
  }
  if (this.req && this.req.socket) {
    this.req.abort();
    this.req.removeAllListeners();
    this.req = null;
  }
  this.preCleanup();
  if (this.source && this.source.socket) {
    this.source.removeAllListeners();
    this.source.destroy();
    this.source = null
  }
};

//
// Complete destroy the internals and end the stream
//
ChangesStream.prototype.destroy = function () {
  debug('destroy the instance and end the stream')
  this.cleanup();
  this._decoder.end();
  this._decoder = null;
  this.removeAllListeners();
  this.push(null);
};

//
// Lol @_read
//
ChangesStream.prototype._read = function (n) {
  this.resume();
};

function JDUP (obj) {
  return JSON.parse(JSON.stringify(obj));
}
