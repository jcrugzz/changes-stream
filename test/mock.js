var util = require('util');
var ChangesStream = require('../');

module.exports = createMockChangesStream;

function createMockChangesStream (overrides) {

  function MockChangesStream (opts) {
    ChangesStream.call(this, opts);
  }

  util.inherits(MockChangesStream, ChangesStream);

  Object.keys(overrides).forEach(function (methodName) {
    MockChangesStream.prototype[methodName] = overrides[methodName];
  });

  return MockChangesStream;
}
