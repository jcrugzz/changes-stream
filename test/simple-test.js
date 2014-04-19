var test = require('tap').test;
var http = require('http-https');
var url = require('url');
var ChangesStream = require('../');

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

