# changes-stream

[![build status](https://secure.travis-ci.org/jcrugzz/changes-stream.png)](http://travis-ci.org/jcrugzz/changes-stream)

A fault tolerant changes stream with builtin retry HEAVILY inspired by
[`follow`][follow]. This module is a [`Readable` Stream][readable] with all of
the fun stream methods that you would expect.

## install

```sh
npm install changes-stream --save
```

## Options

So `changes-stream` can take a fair bit of options in order so that you can
fully customize your `_changes` request. They include the following:

```js
{
  db: 'http://localhost:5984/my_db', // full database URL
  feed: 'continuous', // Can also be longpoll technically but not currently implemented
  filter: 'docs/whatever', // Can be a defined couchdb view, a local filter function or an array of IDs
  inactivity_ms: 60 * 60 * 1000, // time allow inactivity before retrying request
  timeout: undefined, // How long couchdb should wait for a change to show up before closing the feed. in milliseconds
  requestTimeout: 2 * 60 * 1000, // http timeout
  agent: undefined, // http agent
  since: 0, // update sequence to start from, 'now' will start it from latest
  heartbeat: 30 * 1000, // how often we want couchdb to send us a heartbeat message
  style: 'main_only', // specifies how many revisions returned all_docs would return leaf revs
  include_docs: false, // whether or not we want to return the full document as a property
  query_params: {}, // custom arbitrary params to send in request e.g. { hello: 'world' }
  use_post: false // switch the default HTTP method to POST (cannot be used with a filter array)
}
```

## Example

```js
var ChangesStream = require('changes-stream');

var changes = new ChangesStream('http://localhost:5984/my_database');

changes.on('readable', function () {
  var change = changes.read();
});

```
[follow]: https://github.com/iriscouch/follow
[readable]: http://nodejs.org/api/stream.html#stream_class_stream_readable
