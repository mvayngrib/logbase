
var test = require('tape')
var memdown = require('memdown')
var Log = require('../log')
var Entry = require('../entry')

test('log, basic', function (t) {
  t.plan(4)

  var log = new Log('log.db', {
    db: memdown
  })

  var liveStream = log.createReadStream({ live: true })
  liveStream.once('data', function (data) {
    t.ok(data instanceof Entry)
    entry.id(data.id()) // id should be the only thing missing
    t.deepEqual(data.toJSON(), entry.toJSON())
  })

  var entry = new Entry({
    hey: 'ho',
    blah: {
      blah: {
        blah: 1
      }
    }
  })

  t.throws(log.append.bind(log, entry.toJSON()), /Expected Entry/i)
  log.append(entry, function (err) {
    if (err) throw err

    log.get(1, function (err, stored) {
      if (err) throw err

      entry.id(stored.id())
      t.deepEqual(stored.toJSON(), entry.toJSON())
    })
  })
})
