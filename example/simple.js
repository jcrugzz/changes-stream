var ChangesStream = require('../');

var db = process.env.TEST_COUCH;

var changes = new ChangesStream({
  db: db,
  include_docs: true
});

changes.on('readable', function () {
  var change = changes.read();
  console.dir(change);
  changes.destroy();
});
