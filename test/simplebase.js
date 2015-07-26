
var test = require('tape')
var levelup = require('levelup')
var leveldown = require('memdown')
var levelQuery = require('level-queryengine')
var jsonQuery = require('jsonquery-engine')
var SimpleBase = require('../simplebase')
var Log = require('../log')
var Entry = require('../entry')

test('basic', function (t) {
  t.timeoutAfter(5000)

  var log = new Log('log', {
    db: leveldown,
    valueEncoding: 'json'
  })

  log.setMaxListeners(0)

  var numRead = 0
  var numDead = 10
  var numLive = 10
  var numEntries = numDead + numLive
  log.createReadStream({ live: true })
    .on('data', function () {
      if (++numRead === numDead) {
        restart()
      }
    })

  var expectedIds = []
  for (var i = 1; i <= numEntries; i++) {
    expectedIds.push(i)
  }

  // pre-add
  addEntries(log, numDead)

  var ldb
  var base
  var processedId = 1

  function restart (cb) {
    if (ldb) {
      return ldb.close(function (err) {
        if (err) throw err

        ldb = null
        restart(cb)
      })
    }

    ldb = levelQuery(levelup('ldb', {
      db: leveldown,
      valueEncoding: 'json'
    }))

    ldb.query.use(jsonQuery())
    ldb.ensureIndex('length')

    base = SimpleBase(ldb, log, processEntry)

    base.on('change', function (id) {
      t.equal(id, processedId++)
    })

    base.once('live', function () {
      addEntries(log, numLive, numRead)
      base.once('live', function () {
        cleanup()
        t.end()
      })
    })

    if (cb) cb()
  }

  var passed = 0
  var live

  function processEntry (entry, add, cb) {
    live = processedId > numDead
    if (!live && passed++ === 3) {
      // die a few times
      passed = 0
      return restart()
    }

    base.get('ids', function (err, ids) {
      if (err) {
        if (err.notFound) ids = []
        else throw err
      }

      ids.push(entry.id())
      add({ type: 'put', key: 'ids', value: ids })
      cb()
    })
  }

  function cleanup () {
    if (ldb) ldb.close()
    if (log) log.close()
  }
})

function addEntries (log, num, offset) {
  offset = offset || 0
  for (var i = 1; i <= num; i++) {
    var entry = new Entry()
      .set('count', i + offset)

    log.append(entry)
  }
}
