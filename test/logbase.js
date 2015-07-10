
var path = require('path')
var test = require('tape')
var levelup = require('levelup')
var leveldown = require('leveldown')
var rimraf = require('rimraf')
var LogBasedDB = require('../logbase')
var Log = require('../log')
var LogEntry = require('../entry')

test('basic', function (t) {
  t.timeoutAfter(5000)

  var paths = {
    db: path.resolve(__dirname, 'db.db'),
    log: path.resolve(__dirname, 'log.db')
  }

  cleanup()

  var log = new Log(paths.log, {
    db: leveldown,
    valueEncoding: 'json'
  })

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
  var processedId = 1

  function restart (cb) {
    if (ldb) {
      return ldb.destroy(function (err) {
        if (err) throw err

        ldb = null
        restart(cb)
      })
    }

    ldb = new LogBasedDB({
      log: log,
      db: levelup(paths.db, {
        db: leveldown,
        valueEncoding: 'json'
      })
    })

    ldb._process = process

    ldb.on('change', function (id) {
      t.equal(id, processedId++)
      if (processedId === numEntries) {
        cleanup()
        t.end()
      }
    })

    if (cb) cb()
  }

  var passed = 0
  var live

  function process (entry, cb) {
    var self = this
    live = processedId > numDead
    if (!live && passed++ === 3) {
      // die a few times
      passed = 0
      return restart()
    }

    if (processedId === numDead) {
      // add live
      addEntries(log, numLive, numRead)
    }

    this.get('ids', function (err, ids) {
      if (err) {
        if (err.notFound) ids = []
        else throw err
      }

      ids.push(entry.id())
      self.batch([{ type: 'put', key: 'ids', value: ids }], cb)
    })
  }

  function cleanup () {
    for (var p in paths) {
      clear(paths[p])
    }
  }
})

function clear (dbPath) {
  rimraf.sync(dbPath)
}

function addEntries (log, num, offset) {
  offset = offset || 0
  for (var i = 1; i <= num; i++) {
    var entry = new LogEntry()
      .set('count', i + offset)

    log.append(entry)
  }
}
