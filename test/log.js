
var test = require('tape')
var memdown = require('memdown')
var Log = require('../log')
var Entry = require('../entry')

test('basic', function (t) {
  var log = new Log('log.db', {
    db: memdown,
    valueEncoding: 'json'
  })

  var liveStream = log.createReadStream({ live: true })
  liveStream.once('data', function (data) {
    t.ok(data instanceof Entry)
    entry.id(data.id()) // id should be the only thing missing
    t.deepEqual(data.toJSON(), entry.toJSON())
    t.end()
  })

  var entry = new Entry()
    .tag('tx')
    .set({
      hey: 'ho',
      blah: {
        blah: {
          blah: 1
        }
      }
    })

  t.throws(log.append.bind(log, entry.toJSON()), /Expected Entry/i)
  log.append(entry)
})
