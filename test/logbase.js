
var path = require('path')
var test = require('tape')
var levelup = require('levelup')
var leveldown = require('leveldown')
var rimraf = require('rimraf')
var LogBasedDB = require('../logbase')
var Log = require('../log')
var LogEntry = require('../entry')

test('basic', function (t) {
  var paths = {
    db: path.resolve(__dirname, 'db.db'),
    log: path.resolve(__dirname, 'log.db')
  }

  cleanup()

  var db = levelup(paths.db, {
    db: leveldown,
    valueEncoding: 'json'
  })

  var log = new Log(paths.log, {
    db: leveldown,
    valueEncoding: 'json'
  })

  var numRead = 0
  var numEntries = 20
  log.createReadStream({ live: true })
    .on('data', function () {
      if (++numRead === numEntries) {
        restart()
      }
    })

  var expectedIds = []
  for (var i = 1; i <= numEntries; i++) {
    var entry = new LogEntry()
      .set('count', i)

    log.append(entry)
    expectedIds.push(i)
  }

  var ldb
  var processedId = 1

  function restart () {
    if (ldb) {
      return ldb.destroy(function (err) {
        if (err) throw err

        ldb = null
        restart()
      })
    }

    ldb = new LogBasedDB({
      log: log,
      db: db
    })

    ldb._process = process

    ldb.on('change', function (id) {
      t.equal(id, processedId++)
      if (processedId === numEntries) {
        cleanup()
        t.end()
      }
    })
  }

  var passed = 0

  function process (entry, cb) {
    var self = this
    if (passed++ === 3) {
      // die a few times
      passed = 0
      return restart()
    }

    this._db.get('ids', function (err, ids) {
      if (err) {
        if (err.notFound) ids = []
        else throw err
      }

      ids.push(entry.id())
      self._db.batch([{ type: 'put', key: 'ids', value: ids }], cb)
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
