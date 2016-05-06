
var test = require('tape')
var memdown = require('memdown')
var Log = require('../log')
// var Entry = require('../entry')

test('log, basic', function (t) {
  t.plan(2)

  var log = new Log('log.db', {
    db: memdown
  })

  var liveStream = log.createReadStream({ live: true })
  liveStream.once('data', function (data) {
    delete data.id
    t.deepEqual(data, entry)
  })

  var entry = {
    hey: 'ho',
    blah: {
      blah: {
        blah: 1
      }
    }
  }

  log.append(entry, function (err) {
    if (err) throw err

    log.get(1, function (err, stored) {
      if (err) throw err

      delete stored.id
      t.deepEqual(stored, entry)
    })
  })
})
