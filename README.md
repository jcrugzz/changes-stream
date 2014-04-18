# changes-stream

[![build status](https://secure.travis-ci.org/jcrugzz/changes-stream.png)](http://travis-ci.org/jcrugzz/changes-stream)

A fault tolerant changes stream with builtin retry HEAVILY inspired by
[`follow`][follow]. This module is a [`Readable` Stream][readable] with all of
the fun stream methods that you would expect.

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
