var Readable = require('stream').Readable;
var StringDecoder = require('string_decoder').StringDecoder;
var url = require('url');
var util = require('util');
var qs = require('querystring');
var debug = require('debug')('changes-stream');
var http = require('http-https');
var back = require('back');

module.exports = ChangesStream;

util.inherits(ChangesStream, Readable);

//
// @ChangeStream
// ## Constructor to initialize the changes stream
//
function ChangesStream (options) {
  if (!(this instanceof ChangesStream)) { return new ChangesStream(options) }
  Readable.call(this, { objectMode: true });
  options = options || {};

  //
  // PARSE ALL THE OPTIONS OMG
  //
  this._feedParams = ['heartbeat', 'feed', 'filter', 'include_docs', 'view', 'style'];
  // Bit of a buffer for aggregating data
  this._buffer = '';
  this._decoder = new StringDecoder('utf8');

  this.timeout = options.timeout || 2 * 60 * 1000;
  this.reconnect = options.reconnect || {};
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
  this.heartbeat = options.heartbeat || 30 * 1000; // default 30 seconds
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

  this.req = http.request(opts);
  this.req.setSocketKeepAlive(true);
  this.req.on('error', this.retry.bind(this));
  this.req.on('response', this.response.bind(this));
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
ChangesStream.prototype.response = function (res) {
  if (res.statusCode !== 200) {
    return this.emit('error', new Error('Received a ' + res.statusCode + ' from couch'));
  }
  this.source = res;
  this.source.on('data', this.readData.bind(this));
};

//
// Parse and read the data that we get from _changes
//
ChangesStream.prototype.readData = function (data) {
  this._buffer += this._decoder.write(data);

  var lines = this._buffer.split('\n');
  this._buffer = lines.pop();

  for (var i=0; i<lines.length; i++) {
    var line = lines[i];
    try { line = JSON.parse(line) }
    catch (ex) { return; }

    //
    // If we are ever going to have backpressure issues
    // we would want to see if push returned false/null
    // and then stop reading from underlying source
    //
    if (line === '') {
      return this.emit('heartbeat');
    }
    //
    // This is ugly but replicates the correct behavior
    // for running a client side filter function
    //
    if (this.clientFilter) {
      var doc = JDUP(line.doc);
      var query = JDUP({ query: this.query });
      if (!this.filter(doc, query)) {
        return;
      }
      return this.push(line);
    }
    this.push(line);
  }
};

ChangesStream.prototype.retry = function (err) {
  return back(function (fail, attempt) {
    if (fail) {
      return this.emit('error', err);
    }
    debug('retry number %d', attempt.attempt);
    //
    // Allows us to set the DB to something different if we want to
    //
    this.emit('retry');
    this.req.removeAllListeners();
    this.request();
  }, this.reconnect);
};

ChangesStream.prototype.pause = function () {
  if (!this.paused) {
    debug('paused the source request');
    this.source.pause();
    this.paused = true;
    this.emit('pause');
  }
};

ChangesStream.prototype.resume = function () {
  if (this.paused) {
    debug('resumed the source request');
    this.source.resume();
    this.paused = false;
    this.emit('resume');
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
// Useful for cases where the request is still runnning and we haven't errored
//
ChangesStream.prototype.destroy =
ChangesStream.prototype.cleanup = function () {
  debug('cleanup/destroy: flushing any possible buffer and killing underlying request');
  this.preCleanup();
  this.req.removeAllListeners();
  this.req.abort();
  delete this.req;
  if (this.source) {
    this.source.removeAllListeners();
    delete this.source;
  }
  this._decoder.end();
  delete this._decoder;
  this.removeAllListeners();
  this.push(null);
};

//
// Lol @_read
//
ChangesStream.prototype._read = function (n) {};

function JDUP (obj) {
  return JSON.parse(JSON.stringify(obj));
}
