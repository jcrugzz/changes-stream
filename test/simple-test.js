var test = require('tap').test;
var http = require('http-https');
var url = require('url');
var qs = require('querystring');
var ChangesStream = require('../');
var createMockChangesStream = require('./mock');

var db = process.env.TEST_COUCH || 'http://localhost:5984/changes_stream_db'

var simpleDoc = {
  _id: 'whatever',
  hello: 'there',
  how: 'are',
  you: 'today'
};

var cleanup = function (req) {
  req.abort();
  delete req;
};

var deleteDb = function (callback) {
  var opts = url.parse(db);
  opts.method = 'DELETE';
  opts.headers = {
    'content-type': 'application/json',
    'accept': 'application/json'
  };
  var req = http.request(opts);
  req.on('error', callback);
  req.on('response', function (res) {
    if (res.statusCode !== 200 && res.statusCode !== 404) {
      cleanup(req);
      return callback(new Error('bad status code on delete'));
    }
    callback();
    cleanup(req);
  });
  req.end();
};

var createDb = function (callback) {
  var opts = url.parse(db);
  opts.method = 'PUT';
  opts.headers = {
    'content-type': 'application/json',
    'accept': 'application/json'
  };
  var req = http.request(opts);
  req.on('error', callback);
  req.on('response', function (res) {
    if (res.statusCode != 201) {
      cleanup(req);
      return callback(new Error('bad status code on create'));
    }
    callback();
    cleanup(req);
  });
  req.end();
};

var insertDoc = function (callback) {
  var opts = url.parse(db + '/whatever');
  opts.method = 'PUT';
  opts.headers = {
    'content-type': 'application/json',
    'accept': 'application/json'
  };
  var payload = new Buffer(JSON.stringify(simpleDoc), 'utf8');
  var req = http.request(opts);
  req.on('error', callback);
  req.on('response', function (res) {
    if (res.statusCode !== 201) {
      cleanup(req);
      return callback(new Error('bad status code ' + res.statusCode + ' inserting doc'));
    }
    cleanup(req);
    callback();
  });
  req.write(payload);
  req.end();
};

test('Initial Change test', function (t) {
  t.plan(5);
  var changes;

  function fail (err) {
    t.fail(err.message);
    t.end();
  }
  deleteDb(function (err) {
    if (err) return fail(err);
    t.ok(true, 'Database deleted')
    createDb(function (err) {
      if (err) return fail(err);
      t.ok(true, 'Database created');
      insertDoc(function (err) {
        if (err) return fail(err);
        t.ok(true, 'Doc inserted')

        changes = new ChangesStream({
          db: db,
          include_docs: true
        });
        changes.on('readable', function () {
          var change = changes.read();
          t.equals(1, change.seq);
          delete change.doc._rev;
          t.deepEquals(simpleDoc, change.doc);
          changes.destroy();
        });
      })
    });
  });

});

test('Respects query_params', function (t) {
  var urlObj;

  // Create mock that extends ChangesStream and overrides the `request` method
  var Mock = createMockChangesStream({
    request: function () {
      this.preRequest();
      urlObj = url.parse(url.resolve(url.resolve(this.db, '_changes'), '?' + qs.stringify(this.query)));
    }
  });

  var all = {
    heartbeat: 30 * 1000, // default
    feed: 'continuous',   // default
    style: 'all_docs',
    since: 1000,
    limit: 5,
    hello: 'world',
    secret: 'shh don\'t tell'
  };
  var custom = {
    hello: all.hello,
    secret: all.secret
  };
  var changes = new Mock({
    db: db,
    style: all.style,
    since: all.since,
    limit: all.limit,
    query_params: custom
  });

  t.same(changes.query, all);
  t.equal(urlObj.query, 'hello=world&secret=shh%20don%27t%20tell&heartbeat=30000&feed=continuous&style=all_docs&since=1000&limit=5');
  t.equal(changes.query_params, custom);
  t.end();
});
